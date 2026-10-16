package pricing

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/bejix/upstream-ops/backend/config"
)

// 参考价：按网关同样的规则匹配（含「渠道/模型」条目，官方渠道优先），参考表一小时内只下载一次。
func TestReferencePrices(t *testing.T) {
	var downloads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		downloads.Add(1)
		_, _ = w.Write([]byte(`{
			"azure_ai/kimi-x": {"input_cost_per_token": 0.000002, "output_cost_per_token": 0.000008},
			"moonshot/kimi-x": {"input_cost_per_token": 0.0000006, "output_cost_per_token": 0.0000025},
			"glm-y": {"input_cost_per_token": 0.0000005, "output_cost_per_token": 0.000002, "cache_read_input_token_cost": 0.0000001},
			"dashscope/qwen-z": {"tiered_pricing": [
				{"input_cost_per_token": 0.0000024, "output_cost_per_token": 0.000012, "range": [32000, 128000]},
				{"input_cost_per_token": 0.0000012, "output_cost_per_token": 0.000006, "range": [0, 32000]}
			]},
			"deepinfra/qwen/qwen-z": {"input_cost_per_token": 0.000002, "output_cost_per_token": 0.000009}
		}`))
	}))
	defer server.Close()

	c := NewCatalog(nil)
	c.Start(config.PricingConfig{ReferenceURL: server.URL, DataDir: t.TempDir(), HashCheckIntervalMinutes: 60}, nil)
	defer c.Stop()
	for i := 0; i < 2; i++ {
		refs, err := c.ReferencePrices([]string{"kimi-x", "glm-y", "qwen-z", "missing"})
		if err != nil {
			t.Fatal(err)
		}
		if got := refs["kimi-x"]; got.SourceKey != "moonshot/kimi-x" || got.InputPricePerToken != 6e-7 {
			t.Fatalf("kimi-x = %+v, want the moonshot/ entry", got)
		}
		if got := refs["glm-y"]; got.SourceKey != "glm-y" || got.CacheReadPricePerToken != 1e-7 {
			t.Fatalf("glm-y = %+v", got)
		}
		if _, ok := refs["missing"]; ok {
			t.Fatal("missing model should have no reference")
		}
		// 官方条目只有分档价：取最低一档，并标出档位上限
		if got := refs["qwen-z"]; got.SourceKey != "dashscope/qwen-z" || got.InputPricePerToken != 1.2e-6 || got.FirstTierUpTo != 32000 {
			t.Fatalf("qwen-z = %+v, want the first tier of dashscope/", got)
		}
	}
	if n := downloads.Load(); n != 1 {
		t.Fatalf("reference table downloaded %d times, want 1 (cached)", n)
	}
	// 参考价不影响网关自己的计费
	if _, ok := c.Lookup("kimi-x"); ok {
		t.Fatal("reference price must not be used for billing")
	}

	if _, err := NewCatalog(nil).ReferencePrices([]string{"x"}); !errors.Is(err, ErrReferenceDisabled) {
		t.Fatalf("no reference url: %v", err)
	}
}
