package pricing

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/bejix/upstream-ops/backend/config"
)

// 价目表里只有「渠道/模型」形式的条目，也能按裸模型名找到；裸名自己有条目时用裸名的。
func TestLookupMatchesProviderPrefixedKeys(t *testing.T) {
	c := NewCatalog(nil)
	if err := c.loadDefaults([]byte(`{
		"moonshot/kimi-k2-x": {"input_cost_per_token": 0.0000006, "output_cost_per_token": 0.0000025},
		"openrouter/moonshotai/kimi-k2-x": {"input_cost_per_token": 0.000001, "output_cost_per_token": 0.000003},
		"gpt-4o": {"input_cost_per_token": 0.0000025, "output_cost_per_token": 0.00001},
		"azure/gpt-4o": {"input_cost_per_token": 0.000005, "output_cost_per_token": 0.00002}
	}`)); err != nil {
		t.Fatal(err)
	}
	if p, ok := c.Lookup("kimi-k2-x"); !ok || p.InputPricePerToken != 6e-7 {
		t.Fatalf("kimi-k2-x = %+v %v, want the moonshot/ entry", p, ok)
	}
	if p, ok := c.Lookup("Moonshot/Kimi-K2-X"); !ok || p.InputPricePerToken != 6e-7 {
		t.Fatalf("prefixed request = %+v %v", p, ok)
	}
	if p, ok := c.Lookup("gpt-4o"); !ok || p.InputPricePerToken != 2.5e-6 {
		t.Fatalf("gpt-4o = %+v %v, want the bare entry", p, ok)
	}
	if _, ok := c.Lookup("no-such-model"); ok {
		t.Fatal("unknown model should be unpriced")
	}
}

// 管理端「从远程同步」：远程有新版本就下载并立即生效，没变化就不重复下载，同步关闭时报错。
func TestSyncNow(t *testing.T) {
	var (
		mu        sync.Mutex
		body      = []byte(`{"old-model":{"input_cost_per_token":0.000001,"output_cost_per_token":0.000002}}`)
		downloads int
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/prices.json":
			downloads++
			_, _ = w.Write(body)
		case "/prices.sha256":
			_, _ = fmt.Fprintf(w, "%x  prices.json\n", sha256.Sum256(body))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	c := NewCatalog(nil)
	c.Start(config.PricingConfig{
		Enabled:                  true,
		RemoteURL:                server.URL + "/prices.json",
		HashURL:                  server.URL + "/prices.sha256",
		DataDir:                  t.TempDir(),
		HashCheckIntervalMinutes: 60,
	}, nil)
	defer c.Stop()
	if _, ok := c.Lookup("new-model"); ok {
		t.Fatal("new-model priced before it is published")
	}

	mu.Lock()
	body = []byte(`{"old-model":{"input_cost_per_token":0.000001,"output_cost_per_token":0.000002},"new-model":{"input_cost_per_token":0.000003,"output_cost_per_token":0.000004}}`)
	mu.Unlock()
	res, err := c.SyncNow()
	if err != nil || !res.Changed || res.Models != 2 {
		t.Fatalf("sync = %+v %v", res, err)
	}
	if p, ok := c.Lookup("new-model"); !ok || p.InputPricePerToken != 3e-6 {
		t.Fatalf("new-model after sync = %+v %v", p, ok)
	}

	mu.Lock()
	before := downloads
	mu.Unlock()
	res, err = c.SyncNow()
	mu.Lock()
	after := downloads
	mu.Unlock()
	if err != nil || res.Changed || after != before {
		t.Fatalf("unchanged remote: %+v %v, downloads %d → %d", res, err, before, after)
	}

	disabled := NewCatalog(nil)
	if _, err := disabled.SyncNow(); !errors.Is(err, ErrSyncDisabled) {
		t.Fatalf("disabled sync err = %v", err)
	}
}

// 网关自己计费不用分档条目的最低档（长上下文会少算）；只有参考价会用。
func TestLookupIgnoresTieredOnlyEntries(t *testing.T) {
	c := NewCatalog(nil)
	if err := c.loadDefaults([]byte(`{"dashscope/qwen-z":{"tiered_pricing":[{"input_cost_per_token":0.0000012,"output_cost_per_token":0.000006,"range":[0,32000]}]}}`)); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.Lookup("qwen-z"); ok {
		t.Fatal("tiered-only entry must not price gateway billing")
	}
}
