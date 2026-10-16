package pricing

// 价目表 *_above_XXXk_tokens 长上下文价的折算（用例移植自 sub2api pricing_service_test.go
// TestParsePricingData_DerivesLongContextFromAboveTierFields 等）。

import (
	"math"
	"testing"
)

func loadTestTable(t *testing.T, body string) *Catalog {
	t.Helper()
	c := &Catalog{defaults: map[string]ModelPricing{}, fallbacks: map[string]ModelPricing{}}
	if err := c.loadDefaults([]byte(body)); err != nil {
		t.Fatal(err)
	}
	return c
}

func near(a, b float64) bool { return math.Abs(a-b) < 1e-12 }

func TestDeriveLongContextFromAboveTierFields(t *testing.T) {
	c := loadTestTable(t, `{
		"gpt-above": {"litellm_provider": "openai", "mode": "chat",
			"input_cost_per_token": 5e-06, "output_cost_per_token": 3e-05,
			"cache_read_input_token_cost": 5e-07,
			"input_cost_per_token_above_272k_tokens": 1e-05,
			"output_cost_per_token_above_272k_tokens": 4.5e-05,
			"cache_read_input_token_cost_above_272k_tokens": 1e-06,
			"input_cost_per_token_above_272k_tokens_flex": 5e-06,
			"output_cost_per_token_above_272k_tokens_flex": 2.25e-05},
		"gemini-above": {"litellm_provider": "vertex_ai-language-models", "mode": "chat",
			"input_cost_per_token": 1.25e-06, "output_cost_per_token": 1e-05,
			"input_cost_per_token_above_200k_tokens": 2.5e-06,
			"output_cost_per_token_above_200k_tokens": 1.5e-05},
		"explicit-wins": {"litellm_provider": "openai", "mode": "chat",
			"input_cost_per_token": 5e-06, "output_cost_per_token": 3e-05,
			"long_context_input_cost_multiplier": 1,
			"long_context_output_cost_multiplier": 1,
			"input_cost_per_token_above_272k_tokens": 1e-05,
			"output_cost_per_token_above_272k_tokens": 4.5e-05},
		"explicit-zero": {"litellm_provider": "openai", "mode": "chat",
			"input_cost_per_token": 5e-06, "output_cost_per_token": 3e-05,
			"long_context_input_token_threshold": 0,
			"input_cost_per_token_above_272k_tokens": 1e-05,
			"output_cost_per_token_above_272k_tokens": 4.5e-05},
		"no-surcharge": {"litellm_provider": "openai", "mode": "chat",
			"input_cost_per_token": 5e-06, "output_cost_per_token": 3e-05,
			"input_cost_per_token_above_272k_tokens": 5e-06,
			"output_cost_per_token_above_272k_tokens": 3e-05},
		"cache-only-above": {"litellm_provider": "openai", "mode": "chat",
			"input_cost_per_token": 5e-06, "output_cost_per_token": 3e-05,
			"cache_read_input_token_cost_above_272k_tokens": 1e-06},
		"multi-threshold": {"litellm_provider": "openai", "mode": "chat",
			"input_cost_per_token": 1e-06, "output_cost_per_token": 2e-06,
			"input_cost_per_token_above_128k_tokens": 2e-06,
			"input_cost_per_token_above_272k_tokens": 4e-06},
		"grok-above": {"litellm_provider": "xai", "mode": "chat",
			"input_cost_per_token": 2e-06, "output_cost_per_token": 6e-06,
			"input_cost_per_token_above_200k_tokens": 4e-06,
			"output_cost_per_token_above_200k_tokens": 1.2e-05}
	}`)
	get := func(name string) ModelPricing {
		p, ok := c.Lookup(name)
		if !ok {
			t.Fatalf("%s not loaded", name)
		}
		return p
	}

	gpt := get("gpt-above")
	if gpt.LongContextInputThreshold != 272000 || !near(gpt.LongContextInputMultiplier, 2) || !near(gpt.LongContextOutputMultiplier, 1.5) {
		t.Fatalf("gpt-above: 阈值取自字段名（_flex 变体不参与）：%+v", gpt)
	}
	if gpt.LongContextThresholdInclusive {
		t.Fatal("OpenAI 是严格大于")
	}
	gem := get("gemini-above")
	if gem.LongContextInputThreshold != 200000 || !near(gem.LongContextInputMultiplier, 2) || !near(gem.LongContextOutputMultiplier, 1.5) {
		t.Fatalf("gemini-above: %+v", gem)
	}
	if ex := get("explicit-wins"); ex.LongContextInputThreshold != 0 || !near(ex.LongContextInputMultiplier, 1) {
		t.Fatalf("显式 long_context_* 字段优先，不做折算：%+v", ex)
	}
	if ez := get("explicit-zero"); ez.LongContextInputThreshold != 0 || ez.LongContextInputMultiplier != 0 {
		t.Fatalf("显式 0 阈值关闭长上下文：%+v", ez)
	}
	if ns := get("no-surcharge"); ns.LongContextInputThreshold != 0 {
		t.Fatalf("above 价不高于基础价视为无附加费：%+v", ns)
	}
	if co := get("cache-only-above"); co.LongContextInputThreshold != 0 {
		t.Fatalf("仅 cache 侧 above 字段不构成阶梯：%+v", co)
	}
	if mt := get("multi-threshold"); mt.LongContextInputThreshold != 128000 || !near(mt.LongContextInputMultiplier, 2) || !near(mt.LongContextOutputMultiplier, 1) {
		t.Fatalf("多阈值取最小、只有输入一侧时输出倍率为 1：%+v", mt)
	}
	grok := get("grok-above")
	if grok.LongContextInputThreshold != 200000 || !grok.LongContextThresholdInclusive {
		t.Fatalf("xAI 阈值语义为达到即进高档：%+v", grok)
	}
}

// 计费：超过阈值时输入、缓存读按输入倍率加价，输出按输出倍率加价；OpenAI 恰好等于阈值不加价，xAI 等于阈值就加价。
func TestLongContextCostAtThreshold(t *testing.T) {
	c := loadTestTable(t, `{
		"gpt-6.1-sol": {"litellm_provider": "openai", "mode": "chat",
			"input_cost_per_token": 2e-06, "output_cost_per_token": 1e-05, "cache_read_input_token_cost": 1e-07,
			"input_cost_per_token_above_272k_tokens": 4e-06, "output_cost_per_token_above_272k_tokens": 1.5e-05},
		"grok-above": {"litellm_provider": "xai", "mode": "chat",
			"input_cost_per_token": 2e-06, "output_cost_per_token": 6e-06,
			"input_cost_per_token_above_200k_tokens": 4e-06, "output_cost_per_token_above_200k_tokens": 1.2e-05}
	}`)
	gpt, _ := c.Lookup("gpt-6.1-sol")
	base := func(in, cr, out int) float64 { return float64(in)*2e-6 + float64(cr)*1e-7 + float64(out)*1e-5 }

	// 输入 + 缓存读 = 272000：不超过，按普通价
	at := CalculateCost(gpt, UsageTokens{InputTokens: 2000, CacheReadTokens: 270000, OutputTokens: 1000}, 1, 1)
	if !near(at.TotalCost, base(2000, 270000, 1000)) {
		t.Fatalf("恰好等于阈值不应加价：%v", at.TotalCost)
	}
	// 272001：输入与缓存读 ×2，输出 ×1.5
	over := CalculateCost(gpt, UsageTokens{InputTokens: 2001, CacheReadTokens: 270000, OutputTokens: 1000}, 1, 1)
	want := 2001*4e-6 + 270000*2e-7 + 1000*1.5e-5
	if !near(over.TotalCost, want) || !near(over.InputCost, 2001*4e-6) || !near(over.CacheReadCost, 270000*2e-7) || !near(over.OutputCost, 1000*1.5e-5) {
		t.Fatalf("超过阈值：%+v，want total %v", over, want)
	}

	grok, _ := c.Lookup("grok-above")
	g := CalculateCost(grok, UsageTokens{InputTokens: 200000, OutputTokens: 10}, 1, 1)
	if !near(g.TotalCost, 200000*4e-6+10*1.2e-5) {
		t.Fatalf("xAI 达到阈值就应加价：%v", g.TotalCost)
	}
}
