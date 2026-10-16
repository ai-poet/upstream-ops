package gateway

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/pricing"
	"github.com/bejix/upstream-ops/backend/storage"
)

// 后台批量写入：请求结束时用量日志可能还在队列里，Stop 后必须全部落库，额度增量也要写回。
func TestWriteBehindFlushesUsageAndQuota(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, chatCompletionJSON)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "openai_chat"})
	h.svc.StartWriteBehind()

	// 16 个并发工作协程发 300 个请求。不要一次起 300 个：本机假上游的监听队列（macOS 默认 128）
	// 在同时新建几百个连接时会溢出并 RST，测的就成了测试环境而不是网关
	const n, workers = 300, 16
	var wg sync.WaitGroup
	var next atomic.Int32
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for next.Add(1) <= n {
				if w := h.do(http.MethodPost, "/v1/chat/completions", `{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}`, nil); w.Code != http.StatusOK {
					t.Errorf("status %d %s", w.Code, w.Body.String())
				}
			}
		}()
	}
	wg.Wait()
	key, _ := h.svc.Keys.FindByHash(HashAPIKey(h.key))
	pending := h.svc.Runtime.pendingKeyQuota(key.ID)

	h.svc.stopWriteBehind()

	if got, _ := h.svc.Usage.CountAll(); got != n {
		t.Fatalf("usage rows after stop = %d, want %d", got, n)
	}
	key, _ = h.svc.Keys.FindByHash(HashAPIKey(h.key))
	if key.QuotaUsed <= 0 {
		t.Fatalf("quota_used not written back: %v (pending before stop %v)", key.QuotaUsed, pending)
	}
	if key.LastUsedAt == nil {
		t.Fatal("last_used_at not written back")
	}
	if p := h.svc.Runtime.pendingKeyQuota(key.ID); p != 0 {
		t.Fatalf("pending quota after stop = %v", p)
	}
	// 停止后回到同步写
	_ = h.do(http.MethodPost, "/v1/chat/completions", `{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}`, nil)
	if got, _ := h.svc.Usage.CountAll(); got != n+1 {
		t.Fatalf("usage rows after a request post-stop = %d, want %d", got, n+1)
	}
}

// 额度检查要算上还没写回库的增量，不能因为写回延迟多放行。
func TestWriteBehindQuotaCheckIncludesPending(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, chatCompletionJSON)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "openai_chat"})
	key, _ := h.svc.Keys.FindByHash(HashAPIKey(h.key))
	key.Quota = 1
	if err := h.svc.Keys.Update(key); err != nil {
		t.Fatal(err)
	}
	h.svc.StartWriteBehind()
	t.Cleanup(h.svc.stopWriteBehind)

	req := `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`
	if w := h.do(http.MethodPost, "/v1/chat/completions", req, nil); w.Code != http.StatusOK {
		t.Fatalf("under quota: %d %s", w.Code, w.Body.String())
	}
	h.svc.Runtime.addKeyQuotaUsed(key.ID, 1.5) // 还在内存里，没写回
	w := h.do(http.MethodPost, "/v1/chat/completions", req, nil)
	if w.Code == http.StatusOK || !strings.Contains(w.Body.String(), "quota exceeded") {
		t.Fatalf("pending usage must count toward the quota: %d %s", w.Code, w.Body.String())
	}
}

// 配置缓存：本进程的写入让缓存立即失效（停用密钥、暂停路由马上生效）。
func TestConfigCacheInvalidatedByWrites(t *testing.T) {
	a := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, chatCompletionJSON)
	})
	b := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, chatCompletionJSON)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: a, Protocol: "openai_chat", Weight: 10}, e2eRouteSpec{Upstream: b, Protocol: "openai_chat", Weight: 1})
	req := `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`
	_ = h.do(http.MethodPost, "/v1/chat/completions", req, nil)
	if len(a.Calls()) != 1 {
		t.Fatalf("warm-up should hit route a")
	}
	// 暂停 a：下一个请求立刻改走 b
	if err := h.repo.SetTempUnschedulable(h.routes[0].ID, time.Now().Add(time.Minute), "test", time.Now(), "r1"); err != nil {
		t.Fatal(err)
	}
	_ = h.do(http.MethodPost, "/v1/chat/completions", req, nil)
	if len(a.Calls()) != 1 || len(b.Calls()) != 1 {
		t.Fatalf("paused route still used from cache: a=%d b=%d", len(a.Calls()), len(b.Calls()))
	}
	// 停用密钥：下一个请求立刻被拒
	key, _ := h.svc.Keys.FindByHash(HashAPIKey(h.key))
	key.Status = "disabled"
	if err := h.svc.Keys.Update(key); err != nil {
		t.Fatal(err)
	}
	if w := h.do(http.MethodPost, "/v1/chat/completions", req, nil); w.Code == http.StatusOK {
		t.Fatalf("disabled key still accepted from cache")
	}
}

// 上游连接在请求间复用（以前每个请求新建 Transport，连接数 = 请求数）。
func TestE2E_UpstreamConnectionsReused(t *testing.T) {
	var conns atomic.Int64
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_, _ = io.WriteString(w, chatCompletionJSON)
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns.Add(1)
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)
	h := newE2E(t, nil, e2eRouteSpec{Upstream: &mockUpstream{Server: srv}, Protocol: "openai_chat"})
	for i := 0; i < 20; i++ {
		if w := h.do(http.MethodPost, "/v1/chat/completions", `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`, nil); w.Code != http.StatusOK {
			t.Fatalf("status %d", w.Code)
		}
	}
	if got := conns.Load(); got != 1 {
		t.Fatalf("upstream connections = %d for 20 sequential requests, want 1", got)
	}
}

// 价格覆盖缓存：写入后立即生效。
func TestPricingOverrideCacheRefreshesOnWrite(t *testing.T) {
	db := openGatewayTestDB(t)
	prices := storage.NewModelPriceOverrides(db)
	catalog := pricing.NewCatalog(prices)
	if p := catalog.Resolve("my-priced-model"); p.InputPricePerToken != 0 {
		t.Fatalf("unexpected price before override: %+v", p)
	}
	if err := prices.Upsert(&storage.ModelPriceOverride{ModelName: "My-Priced-Model", BillingMode: "token", InputPricePerToken: 0.001, OutputPricePerToken: 0.002, Enabled: true, PricingTiersJSON: "[]"}); err != nil {
		t.Fatal(err)
	}
	if p := catalog.Resolve("my-priced-model"); p.InputPricePerToken != 0.001 {
		t.Fatalf("override not visible after write: %+v", p)
	}
}
