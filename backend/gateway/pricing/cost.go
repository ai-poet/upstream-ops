package pricing

import (
	"strings"
)

// UsageTokens token 计数。
//
// 约定（对齐 sub2api RecordUsage）：
//   - 从 OpenAI（Chat / Responses）解析时 InputTokens 是「总输入」（含 cache 明细），
//     落库 / 计费前要调用 SplitOpenAIUsageBuckets，使 InputTokens 与 CacheRead / CacheCreation 互斥；
//   - 从 Anthropic / Gemini 解析出来的已经互斥，不能再拆（否则输入被少算）
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
	// ImageModel Responses 请求带了生图工具时，出图按哪个模型计价（工具里的 model，没写默认 gpt-image-2）。
	// 只有响应里真的出了图才按图片计费（见 WithResponse），对齐 sub2api。
	ImageModel string
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

	totalInput := tokens.InputTokens + tokens.CacheCreationTokens + tokens.CacheReadTokens
	overThreshold := totalInput > p.LongContextInputThreshold
	if p.LongContextThresholdInclusive {
		overThreshold = totalInput >= p.LongContextInputThreshold
	}
	longContext := p.LongContextInputThreshold > 0 && overThreshold &&
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
