package gateway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
)

// 共享的转发 client 按「代理 + 转发超时」缓存；拉模型列表只能给自己的请求限时，
// 不能改这个 client 的 Timeout——否则之后所有经它转发的流超过 30 秒就会被掐断，
// 客户端看到的是 "upstream stream disconnected"（线上 2026-10-16 的事故）。
func TestFetchUpstreamModels_DoesNotMutateSharedClient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"m1"}]}`))
	}))
	defer srv.Close()

	svc := NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	ch := &storage.Channel{SiteURL: srv.URL}
	want := svc.gatewayRuntime().ForwardTimeout()
	before := svc.httpClientForChannel(ch)
	if before.Timeout != want {
		t.Fatalf("转发 client 初始超时 = %s，应为 forwardTimeout %s", before.Timeout, want)
	}

	if _, err := svc.fetchUpstreamModels(context.Background(), ch, "sk", ""); err != nil {
		t.Fatalf("fetch: %v", err)
	}

	after := svc.httpClientForChannel(ch)
	if after != before {
		t.Fatalf("拉模型后转发 client 被换掉了，连接池不再复用")
	}
	if after.Timeout != want {
		t.Fatalf("拉模型把共享转发 client 的超时改成了 %s，应保持 %s", after.Timeout, want)
	}
}

// 拉模型列表仍要有自己的超时：上游 /v1/models 卡住不能拖住调用方。
func TestFetchUpstreamModels_OwnTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)

	old := upstreamModelsTimeout
	upstreamModelsTimeout = 50 * time.Millisecond
	defer func() { upstreamModelsTimeout = old }()

	svc := NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	ch := &storage.Channel{SiteURL: srv.URL}
	start := time.Now()
	_, err := svc.fetchUpstreamModels(context.Background(), ch, "sk", "")
	if err == nil {
		t.Fatal("上游卡住时应超时报错")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("应是超时错误，got %v", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("超时花了 %s，自己的超时没有生效", took)
	}
	if got := svc.httpClientForChannel(ch).Timeout; got != svc.gatewayRuntime().ForwardTimeout() {
		t.Fatalf("超时路径也不得改共享 client：Timeout = %s", got)
	}
}

// WebSocket 拨号要求 HTTPClient.Timeout 为 0（coder/websocket 用 context 控制超时），
// 只能拿共享 client 的副本去拨，不能把共享 client 的超时清零。
func TestWebsocketHTTPClient_CopiesSharedClient(t *testing.T) {
	svc := NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	ch := &storage.Channel{SiteURL: "https://example.com"}
	shared := svc.httpClientForChannel(ch)
	want := shared.Timeout
	if want == 0 {
		t.Fatal("共享 client 应带 forwardTimeout")
	}

	ws := websocketHTTPClient(shared)
	if ws == shared {
		t.Fatal("应返回副本而不是共享 client 本身")
	}
	if ws.Timeout != 0 {
		t.Fatalf("WebSocket 拨号 client 的 Timeout 应为 0，got %s", ws.Timeout)
	}
	if ws.Transport != shared.Transport {
		t.Fatal("副本应复用同一个 Transport（连接池）")
	}
	if shared.Timeout != want {
		t.Fatalf("共享 client 的超时被改成了 %s", shared.Timeout)
	}
}

// 端到端：拉过一次模型列表之后，比拉模型超时更长的流仍要完整转发到客户端。
// 修复前这个流会在 upstreamModelsTimeout 处被掐断，客户端收到 stream_read_error。
func TestE2E_StreamOutlivesModelsFetchTimeout(t *testing.T) {
	old := upstreamModelsTimeout
	upstreamModelsTimeout = 300 * time.Millisecond
	defer func() { upstreamModelsTimeout = old }()

	first := "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"slow\"},\"finish_reason\":null}]}\n\n"
	rest := "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" +
		"data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"choices\":[],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3}}\n\n" +
		"data: [DONE]\n\n"
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		if r.URL.Path == "/v1/models" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":[{"id":"gpt-4o"}]}`))
			return
		}
		writeSSE(w, first)
		time.Sleep(2 * upstreamModelsTimeout) // 比拉模型的超时更久才续上
		writeSSE(w, rest)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "openai_chat"})

	// 先拉一次模型列表（与 /v1/models、自动恢复、后台拉模型走同一函数）
	if _, err := h.svc.fetchUpstreamModels(context.Background(), &storage.Channel{SiteURL: up.URL}, "sk", ""); err != nil {
		t.Fatalf("fetch models: %v", err)
	}

	w := h.do(http.MethodPost, "/v1/chat/completions",
		`{"model":"gpt-4o","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}]}`, nil)
	if got := w.Body.String(); got != first+rest {
		t.Fatalf("流在拉模型超时处被掐断了\n got: %q\nwant: %q", got, first+rest)
	}
	if u := latestUsage(t, h); !u.Success || u.OutputTokens != 3 {
		t.Fatalf("usage = out %d success %v", u.OutputTokens, u.Success)
	}
}
