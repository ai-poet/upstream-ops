package gateway

// 收到协议终态后不立刻关上游：HTTP/2 上提前关 body 会给上游发 RST_STREAM，每条流都被重置，
// 上游防滥用规则会回 GOAWAY ENHANCE_YOUR_CALM 断掉整条连接（线上 2026-10-16）。见 forwardStreamPlan。

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// h2Upstream HTTP/2 + TLS 的模拟上游：写完终态事件后过 hold 才结束流（END_STREAM 晚到），
// 期间若请求被客户端取消（RST_STREAM）就记一次。
func h2Upstream(t *testing.T, hold time.Duration, resets *atomic.Int32) *mockUpstream {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		for _, f := range []string{respCreated, respDelta, respCompleted} {
			_, _ = w.Write([]byte(f))
			fl.Flush()
		}
		select {
		case <-r.Context().Done():
			resets.Add(1)
		case <-time.After(hold):
		}
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return &mockUpstream{Server: srv}
}

func trustUpstreamTLS(h *e2eHarness, up *mockUpstream) {
	tr := h.svc.httpClientForTarget(nil, nil).Transport.(*http.Transport)
	tr.TLSClientConfig = up.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
}

func TestE2E_UpstreamStreamEndsNormallyAfterTerminal(t *testing.T) {
	var resets atomic.Int32
	up := h2Upstream(t, 300*time.Millisecond, &resets)
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "openai_responses"})
	trustUpstreamTLS(h, up)
	for i := 0; i < 5; i++ {
		start := time.Now()
		w := h.do(http.MethodPost, "/v1/responses", responsesBody, nil)
		if !strings.HasSuffix(w.Body.String(), respCompleted) {
			t.Fatalf("body %q", w.Body.String())
		}
		if took := time.Since(start); took > 250*time.Millisecond {
			t.Fatalf("客户端被拖到上游结束才收尾：%s", took)
		}
	}
	time.Sleep(600 * time.Millisecond) // 等后台把上游流读完
	if n := resets.Load(); n != 0 {
		t.Fatalf("网关重置了 %d 条上游流，应为 0（修复前每条都会被重置）", n)
	}
	if n := h.svc.metrics.upstreamResets.Load(); n != 0 {
		t.Fatalf("reset metric = %d", n)
	}
}

// 上游在终态之后迟迟不结束：宽限期一到照样关掉，计入指标。
func TestE2E_UpstreamStreamResetAfterGrace(t *testing.T) {
	old := upstreamDrainGrace
	upstreamDrainGrace = 100 * time.Millisecond
	defer func() { upstreamDrainGrace = old }()
	var resets atomic.Int32
	up := h2Upstream(t, 5*time.Second, &resets)
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "openai_responses"})
	trustUpstreamTLS(h, up)
	w := h.do(http.MethodPost, "/v1/responses", responsesBody, nil)
	if !strings.HasSuffix(w.Body.String(), respCompleted) {
		t.Fatalf("body %q", w.Body.String())
	}
	deadline := time.Now().Add(2 * time.Second)
	for resets.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if resets.Load() != 1 || h.svc.metrics.upstreamResets.Load() != 1 {
		t.Fatalf("resets=%d metric=%d, want 1/1", resets.Load(), h.svc.metrics.upstreamResets.Load())
	}
}
