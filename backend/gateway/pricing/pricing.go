// Package pricing 模型价目与计费：内置价目（default_prices.json）+ 远程同步 + 数据库价格覆盖 + 家族回退，
// 以及按 token / 按次 / 图片 / 视频计算单次请求费用。网关在记用量时调用。
package pricing

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
	// LongContextThresholdInclusive 输入 token 达到阈值就进长上下文档（xAI 的口径）；其余厂商是严格大于
	LongContextThresholdInclusive bool
	PerRequestPrice               float64
	RequestTiers                  []Tier
	ImagePrice1K                  float64
	ImagePrice2K                  float64
	ImagePrice4K                  float64
	VideoPrice480P                float64
	VideoPrice720P                float64
	VideoPrice1080P               float64
	OutputPricePerImage           float64
	// FirstTierUpTo 只用于参考价目表：条目只有分档定价，这里的单价取的是最低一档，档位上限（输入 token 数）
	FirstTierUpTo int
}

type BillingMode string

const (
	BillingModeToken      BillingMode = "token"
	BillingModePerRequest BillingMode = "per_request"
	BillingModeImage      BillingMode = "image"
	BillingModeVideo      BillingMode = "video"
)

// Tier 是按次计费的尺寸标签或上下文窗口分层价格。
// MaxTokens 为 nil 时表示无上限；TierLabel 优先用于图片/视频尺寸匹配。
type Tier struct {
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
