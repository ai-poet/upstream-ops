package gateway

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/json"
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
	"github.com/bejix/upstream-ops/backend/storage"
)

//go:embed pricing/default_prices.json
var defaultPricesJSON []byte

// ModelPricing per-token USD 单价。
type ModelPricing struct {
	ModelName                          string
	BillingMode                        BillingMode
	InputPricePerToken                 float64
	InputPricePerTokenPriority         float64
	OutputPricePerToken                float64
	OutputPricePerTokenPriority        float64
	CacheCreationPricePerToken         float64
	CacheCreationPricePerTokenPriority float64
	CacheCreation1hPricePerToken       float64
	CacheReadPricePerToken             float64
	CacheReadPricePerTokenPriority     float64
	ImageInputPricePerToken            float64
	ImageOutputPricePerToken           float64
	LongContextInputThreshold          int
	LongContextInputMultiplier         float64
	LongContextOutputMultiplier        float64
	PerRequestPrice                    float64
	RequestTiers                       []PricingTier
	ImagePrice1K                       float64
	ImagePrice2K                       float64
	ImagePrice4K                       float64
	VideoPrice480P                     float64
	VideoPrice720P                     float64
	VideoPrice1080P                    float64
	OutputPricePerImage                float64
}

type BillingMode string

const (
	BillingModeToken      BillingMode = "token"
	BillingModePerRequest BillingMode = "per_request"
	BillingModeImage      BillingMode = "image"
	BillingModeVideo      BillingMode = "video"
)

// PricingTier 是按次计费的尺寸标签或上下文窗口分层价格。
// MaxTokens 为 nil 时表示无上限；TierLabel 优先用于图片/视频尺寸匹配。
type PricingTier struct {
	TierLabel       string  `json:"tier_label"`
	MinTokens       int     `json:"min_tokens"`
	MaxTokens       *int    `json:"max_tokens,omitempty"`
	PerRequestPrice float64 `json:"per_request_price"`
}

// HasTokenPrice 是否具备 token 计费单价（对齐 sub2api TokenPricingAbsent 语义）。
func (p ModelPricing) HasTokenPrice() bool {
	return p.InputPricePerToken > 0 || p.OutputPricePerToken > 0 ||
		p.CacheCreationPricePerToken > 0 || p.CacheReadPricePerToken > 0
}

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
}

// PricingCatalog 内置价 + DB 覆盖 + 硬编码家族回退（对齐 sub2api BillingService）。
type PricingCatalog struct {
	mu        sync.RWMutex
	defaults  map[string]ModelPricing
	fallbacks map[string]ModelPricing
	overrides *storage.ModelPriceOverrides
	// overrideCache 价格覆盖整表缓存（计费每个请求都要查，表很小）；写入后随网关配置版本失效
	overrideCacheOnce sync.Once
	overrideCache     *configCache[string, priceOverrideSnapshot]

	runMu  sync.Mutex
	stopCh chan struct{}
	wg     sync.WaitGroup
	cfg    config.PricingConfig
	client *http.Client
	log    *slog.Logger
	hash   string
}

func NewPricingCatalog(overrides *storage.ModelPriceOverrides) *PricingCatalog {
	c := &PricingCatalog{
		defaults:  map[string]ModelPricing{},
		fallbacks: map[string]ModelPricing{},
		overrides: overrides,
	}
	c.loadDefaults(defaultPricesJSON)
	c.seedFallbackPrices()
	return c
}

func (c *PricingCatalog) loadDefaults(body []byte) error {
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
		// 没有 token 或按图价格的条目跳过。仅有图片价的模型仍需保留，
		// 但 Resolve 的 token 路径不会把它当成 token 价使用。
		if e.InputCostPerToken == nil && e.OutputCostPerToken == nil && e.OutputCostPerImage == nil {
			continue
		}
		p := ModelPricing{BillingMode: normalizeBillingMode(e.Mode)}
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
	c.mu.Lock()
	c.defaults = defaults
	c.mu.Unlock()
	return nil
}

// Start 启动远程价目同步。启动失败不会影响内置价目表；后续定时任务会继续重试。
func (c *PricingCatalog) Start(cfg config.PricingConfig, log *slog.Logger) {
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
func (c *PricingCatalog) Stop() {
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

func (c *PricingCatalog) pricingFilePath() string {
	return filepath.Join(c.cfg.DataDir, "model_prices_and_context_window.json")
}

func (c *PricingCatalog) hashFilePath() string {
	return filepath.Join(c.cfg.DataDir, "model_prices_and_context_window.sha256")
}

func (c *PricingCatalog) loadCached() error {
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

func (c *PricingCatalog) sync() error {
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

func (c *PricingCatalog) download(client *http.Client, remoteHash string) error {
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

func (c *PricingCatalog) fetchBytes(client *http.Client, rawURL string, timeout time.Duration) ([]byte, error) {
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

func (c *PricingCatalog) fetchText(client *http.Client, rawURL string, timeout time.Duration) (string, error) {
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

func (c *PricingCatalog) logError(msg string, err error) {
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
func (c *PricingCatalog) seedFallbackPrices() {
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
func (c *PricingCatalog) ListDefaults(query string) []DefaultPriceItem {
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
func (c *PricingCatalog) Resolve(model string) ModelPricing {
	model = strings.TrimSpace(model)
	if model == "" {
		return ModelPricing{}
	}
	// DB 覆盖（内存快照）：原名与小写都试
	if item, ok := c.lookupOverride(model); ok && item.Enabled {
		return pricingFromOverride(&item)
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, candidate := range modelLookupCandidates(model) {
		if p, ok := c.defaults[candidate]; ok && (p.HasTokenPrice() || p.OutputPricePerImage > 0) {
			return p
		}
	}

	// 日期后缀模糊：claude-xxx-20250219 → claude-xxx
	for _, candidate := range modelLookupCandidates(model) {
		if i := strings.LastIndex(candidate, "-20"); i > 0 && len(candidate)-i >= 9 {
			base := candidate[:i]
			if p, ok := c.defaults[base]; ok && (p.HasTokenPrice() || p.OutputPricePerImage > 0) {
				return p
			}
		}
		// 4-5 ↔ 4.5 变体
		alt := strings.ReplaceAll(candidate, "-4-5-", "-4.5-")
		alt = strings.ReplaceAll(alt, "-4-5", "-4.5")
		if alt != candidate {
			if p, ok := c.defaults[alt]; ok && (p.HasTokenPrice() || p.OutputPricePerImage > 0) {
				return p
			}
		}
	}

	// 硬编码家族回退（含 grok-4.5）
	if p := c.fallbackForModel(model); p.HasTokenPrice() {
		return p
	}
	return ModelPricing{}
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
func (c *PricingCatalog) fallbackForModel(model string) ModelPricing {
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
func (c *PricingCatalog) priceOrFallback(name string) ModelPricing {
	if p, ok := c.defaults[name]; ok && p.HasTokenPrice() {
		return p
	}
	if p, ok := c.fallbacks[name]; ok && p.HasTokenPrice() {
		return p
	}
	return ModelPricing{}
}

// UsageTokens token 计数。
//
// 约定（对齐 sub2api RecordUsage）：
//   - 从上游解析时，InputTokens 可能是「总输入」（含 cache 明细）
//   - 落库 / 计费前应调用 SplitOpenAIUsageBuckets，使 InputTokens 与
//     CacheRead / CacheCreation 互斥，避免双重计费
type UsageTokens struct {
	InputTokens           int
	OutputTokens          int
	CacheCreationTokens   int
	CacheReadTokens       int
	CacheCreation5mTokens int
	CacheCreation1hTokens int
	ImageOutputTokens     int
	// ReasoningTokens 来自 completion_tokens_details.reasoning_tokens（展示用，不单独计费）
	ReasoningTokens int
}

// SplitOpenAIUsageBuckets 对齐 sub2api：
//
//	OpenAI 的 prompt_tokens / input_tokens 是总输入，通常已包含 cache_read / cache_creation。
//	拆成互斥桶后再计费与落库，避免缓存部分既算「输入」又算「读/写缓存」。
//
//	actualInput = max(0, totalInput - cacheRead - cacheCreation)
func SplitOpenAIUsageBuckets(raw UsageTokens) UsageTokens {
	out := raw
	actual := raw.InputTokens - raw.CacheReadTokens - raw.CacheCreationTokens
	if actual < 0 {
		actual = 0
	}
	out.InputTokens = actual
	return out
}

// CostBreakdown 费用拆分。
type CostBreakdown struct {
	InputCost         float64
	ImageInputCost    float64
	OutputCost        float64
	CacheCreationCost float64
	CacheReadCost     float64
	ImageOutputCost   float64
	TotalCost         float64
	ActualCost        float64
	BillingMode       string
}

// Cost 按本价目与 token 桶计算费用；见 CalculateCost。
func (p ModelPricing) Cost(tokens UsageTokens, rateMultiplier, billingRateMultiplier float64) CostBreakdown {
	return CalculateCost(p, tokens, rateMultiplier, billingRateMultiplier)
}

// CalculateCost base = tokens * unit_price；actual = base × 账号计费倍率。
//
// tokens 应为已 SplitOpenAIUsageBuckets 的互斥桶：
// 输入 / 缓存读 / 缓存写 / 输出 各自乘单价，不再把 cache 算进普通输入。
//
// rateMultiplier / billingRateMultiplier 语义：二者均为源分组倍率换算结果
// （原值时即源 ratio，不是强制 1）。优先用 billingRateMultiplier 作为账号计费倍率；
// 无效时回退 rateMultiplier。只乘一次，避免「有效倍率 × 计费倍率」双重放大。
func CalculateCost(p ModelPricing, tokens UsageTokens, rateMultiplier, billingRateMultiplier float64) CostBreakdown {
	return CalculateCostWithServiceTier(p, tokens, rateMultiplier, billingRateMultiplier, "")
}

// CalculateCostWithServiceTier 对齐 sub2api：priority 优先使用表中专属价格，
// 没有专属价格时 priority ×2、flex ×0.5；长上下文对输入侧和输出侧分别加价。
func CalculateCostWithServiceTier(p ModelPricing, tokens UsageTokens, rateMultiplier, billingRateMultiplier float64, serviceTier string) CostBreakdown {
	return CalculateCostWithBillingInput(p, tokens, rateMultiplier, billingRateMultiplier, serviceTier, BillingInput{})
}

// BillingInput 携带按次、图片、视频模式需要的请求参数。
type BillingInput struct {
	RequestCount         int
	SizeTier             string
	VideoDurationSeconds int
	EndpointMode         BillingMode
}

// CalculateCostWithBillingInput 统一覆盖 token / per_request / image / video 四种计费模式。
func CalculateCostWithBillingInput(p ModelPricing, tokens UsageTokens, rateMultiplier, billingRateMultiplier float64, serviceTier string, input BillingInput) CostBreakdown {
	accountRate := billingRateMultiplier
	if accountRate <= 0 {
		accountRate = rateMultiplier
	}
	if accountRate <= 0 {
		accountRate = 1
	}
	count := input.RequestCount
	if count <= 0 {
		count = 1
	}
	switch p.BillingMode {
	case BillingModePerRequest:
		unit := requestUnitPrice(p, input.SizeTier, tokens.InputTokens+tokens.CacheCreationTokens+tokens.CacheReadTokens)
		return fixedCost(unit*float64(count), accountRate, BillingModePerRequest)
	case BillingModeImage:
		return fixedCost(imageUnitPrice(p, input.SizeTier)*float64(count), accountRate, BillingModeImage)
	case BillingModeVideo:
		duration := normalizeVideoDuration(input.VideoDurationSeconds)
		return fixedCost(videoUnitPrice(p, input.SizeTier)*float64(duration)*float64(count), accountRate, BillingModeVideo)
	}

	inPrice, outPrice := p.InputPricePerToken, p.OutputPricePerToken
	cacheCreatePrice, cacheReadPrice := p.CacheCreationPricePerToken, p.CacheReadPricePerToken
	tierMultiplier := 1.0
	if strings.EqualFold(strings.TrimSpace(serviceTier), "priority") &&
		(p.InputPricePerTokenPriority > 0 || p.OutputPricePerTokenPriority > 0 ||
			p.CacheCreationPricePerTokenPriority > 0 || p.CacheReadPricePerTokenPriority > 0) {
		if p.InputPricePerTokenPriority > 0 {
			inPrice = p.InputPricePerTokenPriority
		}
		if p.OutputPricePerTokenPriority > 0 {
			outPrice = p.OutputPricePerTokenPriority
		}
		if p.CacheCreationPricePerTokenPriority > 0 {
			cacheCreatePrice = p.CacheCreationPricePerTokenPriority
		}
		if p.CacheReadPricePerTokenPriority > 0 {
			cacheReadPrice = p.CacheReadPricePerTokenPriority
		}
	} else {
		switch strings.ToLower(strings.TrimSpace(serviceTier)) {
		case "priority":
			tierMultiplier = 2
		case "flex":
			tierMultiplier = .5
		}
	}

	longContext := p.LongContextInputThreshold > 0 &&
		tokens.InputTokens+tokens.CacheCreationTokens+tokens.CacheReadTokens > p.LongContextInputThreshold &&
		(p.LongContextInputMultiplier > 1 || p.LongContextOutputMultiplier > 1)
	cacheMultiplier := 1.0
	if longContext {
		if p.LongContextInputMultiplier > 0 {
			inPrice *= p.LongContextInputMultiplier
			cacheReadPrice *= p.LongContextInputMultiplier
			cacheMultiplier = p.LongContextInputMultiplier
		}
		if p.LongContextOutputMultiplier > 0 {
			outPrice *= p.LongContextOutputMultiplier
		}
	}

	imageOutputTokens := tokens.ImageOutputTokens
	textOutputTokens := tokens.OutputTokens - imageOutputTokens
	if textOutputTokens < 0 {
		textOutputTokens = 0
	}
	in := float64(tokens.InputTokens) * inPrice
	out := float64(textOutputTokens) * outPrice
	imgPrice := p.ImageOutputPricePerToken
	if imgPrice <= 0 {
		imgPrice = outPrice
	}
	img := float64(imageOutputTokens) * imgPrice
	cc := cacheCreationCost(tokens, cacheCreatePrice, p.CacheCreation1hPricePerToken, cacheMultiplier)
	cr := float64(tokens.CacheReadTokens) * cacheReadPrice
	in *= tierMultiplier
	out *= tierMultiplier
	img *= tierMultiplier
	cc *= tierMultiplier
	cr *= tierMultiplier
	total := in + out + cc + cr + img
	return CostBreakdown{
		InputCost:         in,
		OutputCost:        out,
		CacheCreationCost: cc,
		CacheReadCost:     cr,
		ImageOutputCost:   img,
		TotalCost:         total,
		ActualCost:        total * accountRate,
		BillingMode:       string(BillingModeToken),
	}
}

func fixedCost(total, rate float64, mode BillingMode) CostBreakdown {
	return CostBreakdown{TotalCost: total, ActualCost: total * rate, BillingMode: string(mode)}
}

func requestUnitPrice(p ModelPricing, tier string, contextTokens int) float64 {
	for _, item := range p.RequestTiers {
		if item.PerRequestPrice <= 0 || !strings.EqualFold(strings.TrimSpace(item.TierLabel), strings.TrimSpace(tier)) || strings.TrimSpace(tier) == "" {
			continue
		}
		return item.PerRequestPrice
	}
	for _, item := range p.RequestTiers {
		if item.PerRequestPrice > 0 && contextTokens > item.MinTokens && (item.MaxTokens == nil || contextTokens <= *item.MaxTokens) {
			return item.PerRequestPrice
		}
	}
	return p.PerRequestPrice
}

func imageUnitPrice(p ModelPricing, tier string) float64 {
	switch normalizeImageTier(tier) {
	case "4K":
		if p.ImagePrice4K > 0 {
			return p.ImagePrice4K
		}
	case "2K":
		if p.ImagePrice2K > 0 {
			return p.ImagePrice2K
		}
	default:
		if p.ImagePrice1K > 0 {
			return p.ImagePrice1K
		}
	}
	if price, ok := defaultGrokImagePrice(p.ModelName, normalizeImageTier(tier)); ok {
		return price
	}
	modelPrice := p.OutputPricePerImage
	if modelPrice <= 0 {
		modelPrice = .134
	}
	switch normalizeImageTier(tier) {
	case "4K":
		return modelPrice * 2
	case "2K":
		return modelPrice * 1.5
	default:
		return modelPrice
	}
}

func videoUnitPrice(p ModelPricing, resolution string) float64 {
	switch normalizeVideoResolution(resolution) {
	case "1080p":
		if p.VideoPrice1080P > 0 {
			return p.VideoPrice1080P
		}
	case "720p":
		if p.VideoPrice720P > 0 {
			return p.VideoPrice720P
		}
	default:
		if p.VideoPrice480P > 0 {
			return p.VideoPrice480P
		}
	}
	if price, ok := defaultGrokVideoPrice(p.ModelName, normalizeVideoResolution(resolution)); ok {
		return price
	}
	return imageUnitPrice(p, "2K")
}

func defaultGrokImagePrice(model, tier string) (float64, bool) {
	model = strings.ToLower(strings.TrimSpace(model))
	quality := model == "grok-imagine-image-quality"
	if !quality && model != "grok-imagine" && model != "grok-imagine-image" && model != "grok-imagine-edit" {
		return 0, false
	}
	if quality {
		if tier == "1K" {
			return .05, true
		}
		return .07, true
	}
	return .02, true
}

func defaultGrokVideoPrice(model, resolution string) (float64, bool) {
	model = strings.ToLower(strings.TrimSpace(model))
	if strings.HasPrefix(model, "grok-imagine-video-1.5") {
		switch resolution {
		case "1080p":
			return .25, true
		case "720p":
			return .14, true
		default:
			return .08, true
		}
	}
	if strings.HasPrefix(model, "grok-imagine-video") {
		if resolution == "480p" {
			return .05, true
		}
		return .07, true
	}
	return 0, false
}

func normalizeImageTier(raw string) string {
	v := strings.ToLower(strings.TrimSpace(raw))
	switch {
	case strings.Contains(v, "4k"), strings.Contains(v, "4096"):
		return "4K"
	case strings.Contains(v, "2k"), strings.Contains(v, "1536"), strings.Contains(v, "2048"):
		return "2K"
	default:
		return "1K"
	}
}

func normalizeVideoResolution(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1080", "1080p", "fhd", "full_hd", "full-hd":
		return "1080p"
	case "720", "720p", "hd":
		return "720p"
	default:
		return "480p"
	}
}

func normalizeVideoDuration(seconds int) int {
	if seconds <= 0 {
		return 8
	}
	if seconds > 15 {
		return 15
	}
	return seconds
}

func cacheCreationCost(tokens UsageTokens, price5m, price1h, multiplier float64) float64 {
	if price1h <= price5m || price1h <= 0 {
		return float64(tokens.CacheCreationTokens) * price5m * multiplier
	}
	if tokens.CacheCreation5mTokens == 0 && tokens.CacheCreation1hTokens == 0 {
		return float64(tokens.CacheCreationTokens) * price5m * multiplier
	}
	return (float64(tokens.CacheCreation5mTokens)*price5m +
		float64(tokens.CacheCreation1hTokens)*price1h) * multiplier
}

// priceOverrideSnapshot 价格覆盖表快照：原名索引 + 小写索引。
type priceOverrideSnapshot struct {
	exact map[string]storage.ModelPriceOverride
	lower map[string]storage.ModelPriceOverride
}

// lookupOverride 按原名、再按小写名找价格覆盖（与以前逐条查库的顺序一致）。
func (c *PricingCatalog) lookupOverride(model string) (storage.ModelPriceOverride, bool) {
	if c.overrides == nil {
		return storage.ModelPriceOverride{}, false
	}
	c.overrideCacheOnce.Do(func() {
		c.overrideCache = newConfigCache[string, priceOverrideSnapshot](configCacheTTL, true)
	})
	snap, err := c.overrideCache.get("all", func() (priceOverrideSnapshot, error) {
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
