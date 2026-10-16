package pricing

// 参考价目：网关自己的价目表（默认是 sub2api 的精选表，二百多个模型）里没有的模型，
// 到参考价目表（默认 LiteLLM 全量表，四千多条，覆盖各家国产模型）查价格，由管理员确认后采用为价格覆盖。
// 参考价不直接参与计费：同一个模型在不同渠道的定价可能不同，用不用由人决定。

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// referenceCacheTTL 参考价目表下载后缓存多久（表有近 3MB，不必每次点按钮都下载）。
const referenceCacheTTL = time.Hour

// ErrReferenceDisabled 没配参考价目表（pricing.referenceURL）。
var ErrReferenceDisabled = errors.New("reference pricing table is not configured")

// ReferencePrice 参考价目表里查到的价格。token 单价单位 USD/token，按张单价 USD/张。
type ReferencePrice struct {
	SourceKey                  string  `json:"source_key"` // 参考表里的条目名，如 zai/glm-4.5
	BillingMode                string  `json:"billing_mode"`
	InputPricePerToken         float64 `json:"input_price_per_token"`
	OutputPricePerToken        float64 `json:"output_price_per_token"`
	CacheCreationPricePerToken float64 `json:"cache_creation_price_per_token"`
	CacheReadPricePerToken     float64 `json:"cache_read_price_per_token"`
	OutputPricePerImage        float64 `json:"output_price_per_image,omitempty"`
	// FirstTierUpTo 条目只有按输入长度分档的价格，上面取的是最低一档；这一档的输入上限（token）
	FirstTierUpTo int `json:"first_tier_up_to,omitempty"`
}

type referenceTable struct {
	mu      sync.Mutex
	url     string
	at      time.Time
	catalog *Catalog
}

// ReferencePrices 到参考价目表里查这些模型（与网关价目表同样的匹配规则，含「渠道/模型」条目）；查不到的不在结果里。
func (c *Catalog) ReferencePrices(models []string) (map[string]ReferencePrice, error) {
	ref, err := c.referenceCatalog()
	if err != nil {
		return nil, err
	}
	out := map[string]ReferencePrice{}
	for _, model := range models {
		p, ok := ref.Lookup(model)
		if !ok {
			continue
		}
		out[model] = ReferencePrice{
			SourceKey:                  p.ModelName,
			BillingMode:                string(p.BillingMode),
			InputPricePerToken:         p.InputPricePerToken,
			OutputPricePerToken:        p.OutputPricePerToken,
			CacheCreationPricePerToken: p.CacheCreationPricePerToken,
			CacheReadPricePerToken:     p.CacheReadPricePerToken,
			OutputPricePerImage:        p.OutputPricePerImage,
			FirstTierUpTo:              p.FirstTierUpTo,
		}
	}
	return out, nil
}

// referenceCatalog 下载（或复用一小时内的缓存）并解析参考价目表。
func (c *Catalog) referenceCatalog() (*Catalog, error) {
	c.runMu.Lock()
	cfg, client := c.cfg, c.client
	c.runMu.Unlock()
	url := strings.TrimSpace(cfg.ReferenceURL)
	if url == "" {
		return nil, ErrReferenceDisabled
	}
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	c.ref.mu.Lock()
	defer c.ref.mu.Unlock()
	if c.ref.catalog != nil && c.ref.url == url && time.Since(c.ref.at) < referenceCacheTTL {
		return c.ref.catalog, nil
	}
	body, err := c.fetchBytes(client, url, 60*time.Second)
	if err != nil {
		return nil, fmt.Errorf("download reference pricing: %w", err)
	}
	ref := &Catalog{defaults: map[string]ModelPricing{}, fallbacks: map[string]ModelPricing{}, firstTierFallback: true}
	if err := ref.loadDefaults(body); err != nil {
		return nil, fmt.Errorf("parse reference pricing: %w", err)
	}
	c.ref.url, c.ref.at, c.ref.catalog = url, time.Now(), ref
	return ref, nil
}
