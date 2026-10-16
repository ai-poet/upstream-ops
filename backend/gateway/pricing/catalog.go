package pricing

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/gateway/internal/cfgcache"
	"github.com/bejix/upstream-ops/backend/storage"
)

//go:embed default_prices.json
var defaultPricesJSON []byte

// overrideCacheTTL 价格覆盖整表缓存的兜底过期时间（本进程写入时随网关配置版本立即失效）。
const overrideCacheTTL = 10 * time.Second

// LiteLLM 原始条目（字段可选；与 sub2api model_prices_and_context_window.json 一致）。
type defaultPriceEntry struct {
	Mode                                string   `json:"mode"`
	InputCostPerToken                   *float64 `json:"input_cost_per_token"`
	InputCostPerTokenPriority           *float64 `json:"input_cost_per_token_priority"`
	OutputCostPerToken                  *float64 `json:"output_cost_per_token"`
	OutputCostPerTokenPriority          *float64 `json:"output_cost_per_token_priority"`
	CacheCreationInputTokenCost         *float64 `json:"cache_creation_input_token_cost"`
	CacheCreationInputTokenCostPriority *float64 `json:"cache_creation_input_token_cost_priority"`
	CacheCreationInputTokenCostAbove1hr *float64 `json:"cache_creation_input_token_cost_above_1hr"`
	CacheReadInputTokenCost             *float64 `json:"cache_read_input_token_cost"`
	CacheReadInputTokenCostPriority     *float64 `json:"cache_read_input_token_cost_priority"`
	InputCostPerImageToken              *float64 `json:"input_cost_per_image_token"`
	OutputCostPerImageToken             *float64 `json:"output_cost_per_image_token"`
	OutputCostPerImage                  *float64 `json:"output_cost_per_image"`
	LongContextInputTokenThreshold      *int     `json:"long_context_input_token_threshold"`
	LongContextInputCostMultiplier      *float64 `json:"long_context_input_cost_multiplier"`
	LongContextOutputCostMultiplier     *float64 `json:"long_context_output_cost_multiplier"`
	// TieredPricing 按输入长度分档的价格（LiteLLM 里部分国产模型只有这个，没有顶层单价）
	TieredPricing []tieredPriceEntry `json:"tiered_pricing"`
}

type tieredPriceEntry struct {
	InputCostPerToken       *float64  `json:"input_cost_per_token"`
	OutputCostPerToken      *float64  `json:"output_cost_per_token"`
	CacheReadInputTokenCost *float64  `json:"cache_read_input_token_cost"`
	Range                   []float64 `json:"range"`
}

// firstTier 起点最低的一档；没有分档时返回 nil。
func (e defaultPriceEntry) firstTier() *tieredPriceEntry {
	var first *tieredPriceEntry
	for i := range e.TieredPricing {
		t := &e.TieredPricing[i]
		if t.InputCostPerToken == nil && t.OutputCostPerToken == nil {
			continue
		}
		if first == nil || tierStart(t) < tierStart(first) {
			first = t
		}
	}
	return first
}

func tierStart(t *tieredPriceEntry) float64 {
	if len(t.Range) == 0 {
		return 0
	}
	return t.Range[0]
}

// Catalog 内置价 + DB 覆盖 + 硬编码家族回退（对齐 sub2api BillingService）。
type Catalog struct {
	mu       sync.RWMutex
	defaults map[string]ModelPricing
	// prefixed 价目表里带渠道前缀的条目按裸模型名索引（moonshot/kimi-k2 → kimi-k2），见 prefixedKeys
	prefixed  map[string]string
	fallbacks map[string]ModelPricing
	overrides *storage.ModelPriceOverrides
	// overrideCache 价格覆盖整表缓存（计费每个请求都要查，表很小）；写入后随网关配置版本失效
	overrideCacheOnce sync.Once
	overrideCache     *cfgcache.Cache[string, priceOverrideSnapshot]

	// firstTierFallback 只有分档定价的条目取最低一档（参考价目表用；网关计费不用，免得长上下文少算）
	firstTierFallback bool

	runMu sync.Mutex
	// syncMu 后台定时同步与管理端手动同步（SyncNow）不同时下载
	syncMu sync.Mutex
	// ref 参考价目表（见 reference.go）
	ref    referenceTable
	stopCh chan struct{}
	wg     sync.WaitGroup
	cfg    config.PricingConfig
	client *http.Client
	log    *slog.Logger
	hash   string
}

func NewCatalog(overrides *storage.ModelPriceOverrides) *Catalog {
	c := &Catalog{
		defaults:  map[string]ModelPricing{},
		fallbacks: map[string]ModelPricing{},
		overrides: overrides,
	}
	c.loadDefaults(defaultPricesJSON)
	c.seedFallbackPrices()
	return c
}

func (c *Catalog) loadDefaults(body []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return err
	}
	defaults := make(map[string]ModelPricing, len(raw))
	for name, rawEntry := range raw {
		if name == "sample_spec" {
			continue
		}
		var e defaultPriceEntry
		if err := json.Unmarshal(rawEntry, &e); err != nil {
			continue
		}
		firstTierUpTo := 0
		if e.InputCostPerToken == nil && e.OutputCostPerToken == nil && c.firstTierFallback {
			if t := e.firstTier(); t != nil {
				e.InputCostPerToken, e.OutputCostPerToken = t.InputCostPerToken, t.OutputCostPerToken
				if e.CacheReadInputTokenCost == nil {
					e.CacheReadInputTokenCost = t.CacheReadInputTokenCost
				}
				if len(t.Range) > 1 {
					firstTierUpTo = int(t.Range[1])
				}
			}
		}
		// 没有 token 或按图价格的条目跳过。仅有图片价的模型仍需保留，
		// 但 Resolve 的 token 路径不会把它当成 token 价使用。
		if e.InputCostPerToken == nil && e.OutputCostPerToken == nil && e.OutputCostPerImage == nil {
			continue
		}
		p := ModelPricing{BillingMode: normalizeBillingMode(e.Mode), FirstTierUpTo: firstTierUpTo}
		if e.InputCostPerToken != nil {
			p.InputPricePerToken = *e.InputCostPerToken
		}
		if e.InputCostPerTokenPriority != nil {
			p.InputPricePerTokenPriority = *e.InputCostPerTokenPriority
		}
		if e.OutputCostPerToken != nil {
			p.OutputPricePerToken = *e.OutputCostPerToken
		}
		if e.OutputCostPerTokenPriority != nil {
			p.OutputPricePerTokenPriority = *e.OutputCostPerTokenPriority
		}
		if e.CacheCreationInputTokenCost != nil {
			p.CacheCreationPricePerToken = *e.CacheCreationInputTokenCost
		}
		if e.CacheCreationInputTokenCostPriority != nil {
			p.CacheCreationPricePerTokenPriority = *e.CacheCreationInputTokenCostPriority
		}
		if e.CacheCreationInputTokenCostAbove1hr != nil {
			p.CacheCreation1hPricePerToken = *e.CacheCreationInputTokenCostAbove1hr
		}
		if e.CacheReadInputTokenCost != nil {
			p.CacheReadPricePerToken = *e.CacheReadInputTokenCost
		}
		if e.CacheReadInputTokenCostPriority != nil {
			p.CacheReadPricePerTokenPriority = *e.CacheReadInputTokenCostPriority
		}
		if e.InputCostPerImageToken != nil {
			p.ImageInputPricePerToken = *e.InputCostPerImageToken
		}
		if e.OutputCostPerImageToken != nil {
			p.ImageOutputPricePerToken = *e.OutputCostPerImageToken
		}
		if e.OutputCostPerImage != nil {
			p.OutputPricePerImage = *e.OutputCostPerImage
		}
		if e.LongContextInputTokenThreshold != nil {
			p.LongContextInputThreshold = *e.LongContextInputTokenThreshold
		}
		if e.LongContextInputCostMultiplier != nil {
			p.LongContextInputMultiplier = *e.LongContextInputCostMultiplier
		}
		if e.LongContextOutputCostMultiplier != nil {
			p.LongContextOutputMultiplier = *e.LongContextOutputCostMultiplier
		}
		key := strings.ToLower(strings.TrimSpace(name))
		if key == "" {
			continue
		}
		p.ModelName = key
		defaults[key] = p
	}
	prefixed := prefixedKeys(defaults)
	c.mu.Lock()
	c.defaults = defaults
	c.prefixed = prefixed
	c.mu.Unlock()
	return nil
}

// officialProviders 模型厂商自己的渠道前缀。同一个模型挂在多个渠道下时优先用这些：
// 转售渠道（azure_ai、openrouter、fireworks_ai……）的价格常和官方不同。
var officialProviders = map[string]bool{
	"openai": true, "anthropic": true, "gemini": true, "xai": true, "deepseek": true,
	"moonshot": true, "zai": true, "dashscope": true, "mistral": true, "minimax": true,
	"cohere": true, "perplexity": true, "volcengine": true, "tencent": true,
}

// prefixedKeys 价目表（LiteLLM 格式）里很多模型只以「渠道/模型」出现（如 zai/glm-4.5），
// 网关收到的是裸模型名。为这些条目建「裸名 → 完整键」索引；裸名本身就有条目的不收（精确匹配优先）。
// 同一个裸名出现在多个渠道下时：官方渠道优先，其次层级少的（moonshot/x 优先于 openrouter/moonshotai/x），
// 最后按字典序，保证结果稳定。
func prefixedKeys(defaults map[string]ModelPricing) map[string]string {
	out := map[string]string{}
	for key := range defaults {
		i := strings.LastIndex(key, "/")
		if i < 0 || i+1 >= len(key) {
			continue
		}
		bare := key[i+1:]
		if _, ok := defaults[bare]; ok {
			continue
		}
		if cur, ok := out[bare]; !ok || prefixedKeyLess(key, cur) {
			out[bare] = key
		}
	}
	return out
}

// prefixedKeyLess a 是否比 b 更适合作为裸名的价格来源。
func prefixedKeyLess(a, b string) bool {
	oa, ob := officialProviders[strings.SplitN(a, "/", 2)[0]], officialProviders[strings.SplitN(b, "/", 2)[0]]
	if oa != ob {
		return oa
	}
	if na, nb := strings.Count(a, "/"), strings.Count(b, "/"); na != nb {
		return na < nb
	}
	return a < b
}

// Start 启动远程价目同步。启动失败不会影响内置价目表；后续定时任务会继续重试。
func (c *Catalog) Start(cfg config.PricingConfig, log *slog.Logger) {
	if c == nil {
		return
	}
	c.Stop()
	cfg = cfg.WithDefaults()
	c.runMu.Lock()
	c.cfg = cfg
	c.log = log
	c.client = &http.Client{Timeout: 30 * time.Second}
	c.stopCh = make(chan struct{})
	stopCh := c.stopCh
	c.runMu.Unlock()

	if body, err := os.ReadFile(cfg.FallbackFile); err == nil {
		if err := c.loadDefaults(body); err != nil {
			c.logError("load fallback pricing failed", err)
		}
	}
	if err := c.loadCached(); err != nil {
		c.logError("load cached pricing failed", err)
	}
	if !cfg.Enabled {
		return
	}
	if err := c.sync(); err != nil {
		c.logError("initial pricing sync failed; using cached or embedded prices", err)
	}

	interval := time.Duration(cfg.HashCheckIntervalMinutes) * time.Minute
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := c.sync(); err != nil {
					c.logError("pricing sync failed", err)
				}
			case <-stopCh:
				return
			}
		}
	}()
}

// Stop 停止远程同步任务；可安全重复调用。
func (c *Catalog) Stop() {
	if c == nil {
		return
	}
	c.runMu.Lock()
	stopCh := c.stopCh
	c.stopCh = nil
	c.runMu.Unlock()
	if stopCh != nil {
		close(stopCh)
		c.wg.Wait()
	}
}

func (c *Catalog) pricingFilePath() string {
	return filepath.Join(c.cfg.DataDir, "model_prices_and_context_window.json")
}

func (c *Catalog) hashFilePath() string {
	return filepath.Join(c.cfg.DataDir, "model_prices_and_context_window.sha256")
}

func (c *Catalog) loadCached() error {
	c.runMu.Lock()
	cfg := c.cfg
	c.runMu.Unlock()
	body, err := os.ReadFile(filepath.Join(cfg.DataDir, "model_prices_and_context_window.json"))
	if err != nil {
		return err
	}
	if err := c.loadDefaults(body); err != nil {
		return fmt.Errorf("parse cached pricing: %w", err)
	}
	hash, err := os.ReadFile(filepath.Join(cfg.DataDir, "model_prices_and_context_window.sha256"))
	if err != nil {
		sum := sha256.Sum256(body)
		c.mu.Lock()
		c.hash = fmt.Sprintf("%x", sum)
		c.mu.Unlock()
		return nil
	}
	c.mu.Lock()
	c.hash = firstField(string(hash))
	c.mu.Unlock()
	return nil
}

func (c *Catalog) sync() error {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	c.runMu.Lock()
	cfg, client := c.cfg, c.client
	c.runMu.Unlock()
	if !cfg.Enabled || strings.TrimSpace(cfg.RemoteURL) == "" || client == nil {
		return nil
	}

	if strings.TrimSpace(cfg.HashURL) != "" {
		remoteHash, err := c.fetchText(client, cfg.HashURL, 10*time.Second)
		if err != nil {
			return fmt.Errorf("fetch pricing hash: %w", err)
		}
		remoteHash = firstField(remoteHash)
		c.mu.RLock()
		localHash := c.hash
		c.mu.RUnlock()
		if remoteHash != "" && strings.EqualFold(remoteHash, localHash) {
			return nil
		}
		return c.download(client, remoteHash)
	}

	info, err := os.Stat(c.pricingFilePath())
	if err == nil && time.Since(info.ModTime()) < time.Duration(cfg.UpdateIntervalHours)*time.Hour {
		return nil
	}
	return c.download(client, "")
}

// ErrSyncDisabled 远程价目同步关闭（pricing.enabled=false 或没配 remoteURL）。
var ErrSyncDisabled = errors.New("remote pricing sync is disabled")

// SyncResult 一次手动同步的结果。
type SyncResult struct {
	Models   int       `json:"models"`  // 同步后价目表里的模型条目数
	Changed  bool      `json:"changed"` // 远程有新版本并已下载
	Hash     string    `json:"hash"`
	SyncedAt time.Time `json:"synced_at"`
}

// SyncNow 立即检查远程价目表：哈希变了（或拿不到哈希）就下载并热更新，没变就沿用当前表。
// 管理端「从远程同步」调用；不用等下一次定时检查。
func (c *Catalog) SyncNow() (SyncResult, error) {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	c.runMu.Lock()
	cfg, client := c.cfg, c.client
	c.runMu.Unlock()
	if !cfg.Enabled || strings.TrimSpace(cfg.RemoteURL) == "" {
		return SyncResult{}, ErrSyncDisabled
	}
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	c.mu.RLock()
	before := c.hash
	c.mu.RUnlock()

	remoteHash := ""
	if strings.TrimSpace(cfg.HashURL) != "" {
		if h, err := c.fetchText(client, cfg.HashURL, 10*time.Second); err == nil {
			remoteHash = firstField(h)
		}
	}
	if remoteHash == "" || !strings.EqualFold(remoteHash, before) {
		if err := c.download(client, remoteHash); err != nil {
			return SyncResult{}, err
		}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return SyncResult{
		Models:   len(c.defaults),
		Changed:  !strings.EqualFold(c.hash, before),
		Hash:     c.hash,
		SyncedAt: time.Now(),
	}, nil
}

func (c *Catalog) download(client *http.Client, remoteHash string) error {
	c.runMu.Lock()
	cfg := c.cfg
	c.runMu.Unlock()
	body, err := c.fetchBytes(client, cfg.RemoteURL, 30*time.Second)
	if err != nil {
		return fmt.Errorf("download pricing data: %w", err)
	}
	if err := c.loadDefaults(body); err != nil {
		return fmt.Errorf("parse remote pricing data: %w", err)
	}
	sum := sha256.Sum256(body)
	dataHash := fmt.Sprintf("%x", sum)
	if remoteHash != "" && !strings.EqualFold(remoteHash, dataHash) {
		c.logError("pricing hash differs from downloaded data", nil)
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("create pricing cache directory: %w", err)
	}
	if err := os.WriteFile(c.pricingFilePath(), body, 0o644); err != nil {
		return fmt.Errorf("write pricing cache: %w", err)
	}
	syncHash := remoteHash
	if syncHash == "" {
		syncHash = dataHash
	}
	if err := os.WriteFile(c.hashFilePath(), []byte(syncHash+"\n"), 0o644); err != nil {
		return fmt.Errorf("write pricing hash: %w", err)
	}
	c.mu.Lock()
	c.hash = syncHash
	c.mu.Unlock()
	return nil
}

func (c *Catalog) fetchBytes(client *http.Client, rawURL string, timeout time.Duration) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func (c *Catalog) fetchText(client *http.Client, rawURL string, timeout time.Duration) (string, error) {
	body, err := c.fetchBytes(client, rawURL, timeout)
	return string(body), err
}

func firstField(s string) string {
	fields := strings.Fields(s)
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
}

func (c *Catalog) logError(msg string, err error) {
	if c.log == nil {
		return
	}
	if err != nil {
		c.log.Warn(msg, "err", err)
		return
	}
	c.log.Warn(msg)
}

// seedFallbackPrices 对齐 sub2api billing_service.go 硬编码回退价。
// LiteLLM JSON 未必覆盖全部在用模型（如 grok-4.5），无表项时靠此表计费。
func (c *Catalog) seedFallbackPrices() {
	// xAI Grok 4.5: $2 / $6 / cached $0.50 per MTok
	c.fallbacks["grok-4.5"] = ModelPricing{
		InputPricePerToken:     2e-6,
		OutputPricePerToken:    6e-6,
		CacheReadPricePerToken: 0.5e-6,
	}
	// xAI Grok 4.3: $1.25 / $2.50 / cached $0.20 per MTok
	c.fallbacks["grok-4.3"] = ModelPricing{
		InputPricePerToken:     1.25e-6,
		OutputPricePerToken:    2.5e-6,
		CacheReadPricePerToken: 0.2e-6,
	}
	// xAI Grok Build 0.1: $1 / $2 / cached $0.20 per MTok
	c.fallbacks["grok-build-0.1"] = ModelPricing{
		InputPricePerToken:     1e-6,
		OutputPricePerToken:    2e-6,
		CacheReadPricePerToken: 0.2e-6,
	}
	// DeepSeek 官方价（$/token），作家族前缀回退；精确表项见 default_prices.json
	// chat / V3 系常见计费：$0.28 / $0.42 per MTok，缓存读 $0.028
	c.fallbacks["deepseek-chat"] = ModelPricing{
		InputPricePerToken:     2.8e-7,
		OutputPricePerToken:    4.2e-7,
		CacheReadPricePerToken: 2.8e-8,
	}
	c.fallbacks["deepseek-reasoner"] = ModelPricing{
		InputPricePerToken:     2.8e-7,
		OutputPricePerToken:    4.2e-7,
		CacheReadPricePerToken: 2.8e-8,
	}
	// R1 系（LiteLLM deepseek/deepseek-r1）
	c.fallbacks["deepseek-r1"] = ModelPricing{
		InputPricePerToken:  5.5e-7,
		OutputPricePerToken: 2.19e-6,
	}
	c.fallbacks["deepseek-coder"] = ModelPricing{
		InputPricePerToken:  1.4e-7,
		OutputPricePerToken: 2.8e-7,
	}
	c.fallbacks["deepseek-v3"] = ModelPricing{
		InputPricePerToken:     2.7e-7,
		OutputPricePerToken:    1.1e-6,
		CacheReadPricePerToken: 7e-8,
	}
	c.fallbacks["deepseek-v3.2"] = ModelPricing{
		InputPricePerToken:  2.8e-7,
		OutputPricePerToken: 4e-7,
	}
	c.fallbacks["deepseek-v4-flash"] = ModelPricing{
		InputPricePerToken:     1.4e-7,
		OutputPricePerToken:    2.8e-7,
		CacheReadPricePerToken: 2.8e-9,
	}
	c.fallbacks["deepseek-v4-pro"] = ModelPricing{
		InputPricePerToken:     4.35e-7,
		OutputPricePerToken:    8.7e-7,
		CacheReadPricePerToken: 3.625e-9,
	}
}

// DefaultPriceItem 内置默认价（管理端只读展示）。
type DefaultPriceItem struct {
	ModelName                          string  `json:"model_name"`
	InputPricePerToken                 float64 `json:"input_price_per_token"`
	InputPricePerTokenPriority         float64 `json:"input_price_per_token_priority"`
	OutputPricePerToken                float64 `json:"output_price_per_token"`
	OutputPricePerTokenPriority        float64 `json:"output_price_per_token_priority"`
	CacheCreationPricePerToken         float64 `json:"cache_creation_price_per_token"`
	CacheCreationPricePerTokenPriority float64 `json:"cache_creation_price_per_token_priority"`
	CacheCreation1hPricePerToken       float64 `json:"cache_creation_1h_price_per_token"`
	CacheReadPricePerToken             float64 `json:"cache_read_price_per_token"`
	CacheReadPricePerTokenPriority     float64 `json:"cache_read_price_per_token_priority"`
	ImageOutputPricePerToken           float64 `json:"image_output_price_per_token"`
	LongContextInputThreshold          int     `json:"long_context_input_threshold"`
	LongContextInputMultiplier         float64 `json:"long_context_input_multiplier"`
	LongContextOutputMultiplier        float64 `json:"long_context_output_multiplier"`
}

// ListDefaults 返回内置价目表（含硬编码 fallback），可选子串过滤。
func (c *Catalog) ListDefaults(query string) []DefaultPriceItem {
	c.mu.RLock()
	defer c.mu.RUnlock()
	q := strings.ToLower(strings.TrimSpace(query))
	merged := make(map[string]ModelPricing, len(c.defaults)+len(c.fallbacks))
	for k, v := range c.defaults {
		merged[k] = v
	}
	// fallback 仅在 defaults 未覆盖时展示
	for k, v := range c.fallbacks {
		if _, ok := merged[k]; !ok {
			merged[k] = v
		}
	}
	out := make([]DefaultPriceItem, 0, len(merged))
	for name, p := range merged {
		if q != "" && !strings.Contains(name, q) {
			continue
		}
		out = append(out, DefaultPriceItem{
			ModelName:                          name,
			InputPricePerToken:                 p.InputPricePerToken,
			InputPricePerTokenPriority:         p.InputPricePerTokenPriority,
			OutputPricePerToken:                p.OutputPricePerToken,
			OutputPricePerTokenPriority:        p.OutputPricePerTokenPriority,
			CacheCreationPricePerToken:         p.CacheCreationPricePerToken,
			CacheCreationPricePerTokenPriority: p.CacheCreationPricePerTokenPriority,
			CacheCreation1hPricePerToken:       p.CacheCreation1hPricePerToken,
			CacheReadPricePerToken:             p.CacheReadPricePerToken,
			CacheReadPricePerTokenPriority:     p.CacheReadPricePerTokenPriority,
			ImageOutputPricePerToken:           p.ImageOutputPricePerToken,
			LongContextInputThreshold:          p.LongContextInputThreshold,
			LongContextInputMultiplier:         p.LongContextInputMultiplier,
			LongContextOutputMultiplier:        p.LongContextOutputMultiplier,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ModelName < out[j].ModelName })
	return out
}

// Resolve 优先 override，再 LiteLLM 默认表（模糊匹配），再硬编码家族回退。
// 对齐 sub2api：BillingService.GetModelPricing → PricingService → fallbackPrices。
func (c *Catalog) Resolve(model string) ModelPricing {
	p, _ := c.Lookup(model)
	return p
}

// Lookup 同 Resolve，另外返回是否找到了价格：false 表示覆盖、价目表与家族回退里都没有这个模型，
// 按它计费的费用会是 0。管理员在覆盖里显式设成 0 的算找到（有意免费）。
func (c *Catalog) Lookup(model string) (ModelPricing, bool) {
	model = strings.TrimSpace(model)
	if model == "" {
		return ModelPricing{}, false
	}
	// DB 覆盖（内存快照）：原名与小写都试
	if item, ok := c.lookupOverride(model); ok && item.Enabled {
		return pricingFromOverride(&item), true
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, candidate := range modelLookupCandidates(model) {
		if p, ok := c.defaults[candidate]; ok && (p.HasTokenPrice() || p.OutputPricePerImage > 0) {
			return p, true
		}
	}

	// 日期后缀模糊：claude-xxx-20250219 → claude-xxx
	for _, candidate := range modelLookupCandidates(model) {
		if i := strings.LastIndex(candidate, "-20"); i > 0 && len(candidate)-i >= 9 {
			base := candidate[:i]
			if p, ok := c.defaults[base]; ok && (p.HasTokenPrice() || p.OutputPricePerImage > 0) {
				return p, true
			}
		}
		// 4-5 ↔ 4.5 变体
		alt := strings.ReplaceAll(candidate, "-4-5-", "-4.5-")
		alt = strings.ReplaceAll(alt, "-4-5", "-4.5")
		if alt != candidate {
			if p, ok := c.defaults[alt]; ok && (p.HasTokenPrice() || p.OutputPricePerImage > 0) {
				return p, true
			}
		}
	}

	// 价目表里只有「渠道/模型」形式的条目
	for _, candidate := range modelLookupCandidates(model) {
		if key, ok := c.prefixed[candidate]; ok {
			if p, ok := c.defaults[key]; ok && (p.HasTokenPrice() || p.OutputPricePerImage > 0) {
				return p, true
			}
		}
	}

	// 硬编码家族回退（含 grok-4.5）
	if p := c.fallbackForModel(model); p.HasTokenPrice() {
		return p, true
	}
	return ModelPricing{}, false
}

func pricingFromOverride(item *storage.ModelPriceOverride) ModelPricing {
	p := ModelPricing{
		ModelName:                  item.ModelName,
		BillingMode:                normalizeBillingMode(item.BillingMode),
		InputPricePerToken:         item.InputPricePerToken,
		OutputPricePerToken:        item.OutputPricePerToken,
		CacheCreationPricePerToken: item.CacheCreationPricePerToken,
		CacheReadPricePerToken:     item.CacheReadPricePerToken,
		PerRequestPrice:            item.PerRequestPrice,
		ImagePrice1K:               item.ImagePrice1K,
		ImagePrice2K:               item.ImagePrice2K,
		ImagePrice4K:               item.ImagePrice4K,
		VideoPrice480P:             item.VideoPrice480P,
		VideoPrice720P:             item.VideoPrice720P,
		VideoPrice1080P:            item.VideoPrice1080P,
	}
	if strings.TrimSpace(item.PricingTiersJSON) != "" {
		_ = json.Unmarshal([]byte(item.PricingTiersJSON), &p.RequestTiers)
	}
	return p
}

func normalizeBillingMode(raw string) BillingMode {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "image_generation" {
		return BillingModeImage
	}
	switch BillingMode(raw) {
	case BillingModePerRequest, BillingModeImage, BillingModeVideo:
		return BillingMode(strings.ToLower(strings.TrimSpace(raw)))
	default:
		return BillingModeToken
	}
}

func modelLookupCandidates(model string) []string {
	model = strings.ToLower(strings.TrimSpace(model))
	model = strings.TrimLeft(model, "/")
	model = strings.TrimPrefix(model, "models/")
	candidates := []string{model}
	if idx := strings.LastIndex(model, "/"); idx >= 0 && idx+1 < len(model) {
		candidates = append(candidates, model[idx+1:])
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(candidates))
	for _, c := range candidates {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		if _, ok := seen[c]; ok {
			continue
		}
		seen[c] = struct{}{}
		out = append(out, c)
	}
	return out
}

// fallbackForModel 对齐 sub2api getFallbackPricing 中与网关相关的关键分支。
// 调用方须已持有 c.mu 读锁（或单线程初始化后只读）。
func (c *Catalog) fallbackForModel(model string) ModelPricing {
	modelLower := strings.ToLower(strings.TrimSpace(model))
	switch modelLower {
	case "grok", "grok-latest", "grok-4.5", "grok-4.5-latest", "grok-build-latest":
		return c.fallbacks["grok-4.5"]
	case "grok-4.3",
		"grok-4.20-0309-reasoning",
		"grok-4.20-0309-non-reasoning",
		"grok-4.20-multi-agent-0309",
		"grok-4.20-reasoning",
		"grok-4.20-non-reasoning":
		return c.fallbacks["grok-4.3"]
	case "grok-build", "grok-build-0.1", "grok-composer", "grok-composer-2.5-fast", "composer-2.5":
		return c.fallbacks["grok-build-0.1"]
	}
	// 前缀宽松匹配：grok-4.5-xxx
	if strings.HasPrefix(modelLower, "grok-4.5") {
		return c.fallbacks["grok-4.5"]
	}
	if strings.HasPrefix(modelLower, "grok-4.3") {
		return c.fallbacks["grok-4.3"]
	}
	if strings.HasPrefix(modelLower, "grok-build") {
		return c.fallbacks["grok-build-0.1"]
	}

	// DeepSeek 家族：上游常带 deepseek/ 前缀或日期/变体后缀
	if strings.Contains(modelLower, "deepseek") {
		base := modelLower
		if idx := strings.LastIndex(base, "/"); idx >= 0 && idx+1 < len(base) {
			base = base[idx+1:]
		}
		switch {
		case strings.Contains(base, "v4-pro") || strings.Contains(base, "v4.pro"):
			return c.priceOrFallback("deepseek-v4-pro")
		case strings.Contains(base, "v4-flash") || strings.Contains(base, "v4.flash"):
			return c.priceOrFallback("deepseek-v4-flash")
		case strings.Contains(base, "coder"):
			return c.priceOrFallback("deepseek-coder")
		case strings.Contains(base, "reasoner"):
			return c.priceOrFallback("deepseek-reasoner")
		case strings.Contains(base, "r1"):
			return c.priceOrFallback("deepseek-r1")
		case strings.Contains(base, "v3.2") || strings.Contains(base, "v3-2"):
			return c.priceOrFallback("deepseek-v3.2")
		case strings.Contains(base, "v3.1") || strings.Contains(base, "v3-1"):
			return c.priceOrFallback("deepseek-v3")
		case strings.Contains(base, "v3"):
			return c.priceOrFallback("deepseek-v3")
		default:
			return c.priceOrFallback("deepseek-chat")
		}
	}
	return ModelPricing{}
}

// priceOrFallback 优先 defaults 表，其次 seed fallbacks。
func (c *Catalog) priceOrFallback(name string) ModelPricing {
	if p, ok := c.defaults[name]; ok && p.HasTokenPrice() {
		return p
	}
	if p, ok := c.fallbacks[name]; ok && p.HasTokenPrice() {
		return p
	}
	return ModelPricing{}
}

// priceOverrideSnapshot 价格覆盖表快照：原名索引 + 小写索引。
type priceOverrideSnapshot struct {
	exact map[string]storage.ModelPriceOverride
	lower map[string]storage.ModelPriceOverride
}

// lookupOverride 按原名、再按小写名找价格覆盖（与以前逐条查库的顺序一致）。
func (c *Catalog) lookupOverride(model string) (storage.ModelPriceOverride, bool) {
	if c.overrides == nil {
		return storage.ModelPriceOverride{}, false
	}
	c.overrideCacheOnce.Do(func() {
		c.overrideCache = cfgcache.New[string, priceOverrideSnapshot](overrideCacheTTL, true)
	})
	snap, err := c.overrideCache.Get("all", func() (priceOverrideSnapshot, error) {
		list, err := c.overrides.List()
		if err != nil {
			return priceOverrideSnapshot{}, err
		}
		snap := priceOverrideSnapshot{exact: map[string]storage.ModelPriceOverride{}, lower: map[string]storage.ModelPriceOverride{}}
		for _, item := range list {
			snap.exact[item.ModelName] = item
			if _, ok := snap.lower[strings.ToLower(item.ModelName)]; !ok {
				snap.lower[strings.ToLower(item.ModelName)] = item
			}
		}
		return snap, nil
	})
	if err != nil {
		return storage.ModelPriceOverride{}, false
	}
	if item, ok := snap.exact[model]; ok {
		return item, true
	}
	item, ok := snap.lower[strings.ToLower(model)]
	return item, ok
}
