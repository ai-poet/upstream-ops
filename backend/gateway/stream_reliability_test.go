package gateway

// 流式转发可靠性：首个可见输出之前的断流可重试、提交前保活、断流原因、传输错误不立刻冷却、
// 全部冷却时仍试一条、请求体上限、指标端点。行为照 sub2api（见 stream_preoutput.go、route_health.go）。

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/storage"
)

const (
	respCreated    = "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"status\":\"in_progress\"}}\n\n"
	respInProgress = "event: response.in_progress\ndata: {\"type\":\"response.in_progress\",\"response\":{\"id\":\"resp_1\",\"status\":\"in_progress\"}}\n\n"
	respDelta      = "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n"
	respCompleted  = "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"usage\":{\"input_tokens\":7,\"output_tokens\":3,\"total_tokens\":10}}}\n\n"
	responsesBody  = `{"model":"gpt-x","stream":true,"input":"hi"}`
)

// cutConnection 写完已有的帧后直接掐断 TCP 连接（客户端读到 unexpected EOF），模拟上游中途断流。
func cutConnection(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	conn, buf, err := w.(http.Hijacker).Hijack()
	if err != nil {
		t.Errorf("hijack: %v", err)
		return
	}
	_ = buf.Flush()
	_ = conn.Close()
}

func setGatewayConfig(h *e2eHarness, mutate func(*config.GatewayConfig)) {
	cfg := h.svc.gatewayRuntime()
	mutate(&cfg)
	h.svc.UpdateGatewayConfig(cfg)
}

// 上游在 response.created / in_progress 之后、第一个输出之前断开：客户端什么都没收到，网关同路由重试，
// 客户端拿到的是第二次的完整流（以前收到 response.created 就提交，之后断开只能回 stream_read_error）。
func TestE2E_ResponsesDisconnectBeforeOutputIsRetried(t *testing.T) {
	var calls atomic.Int32
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		if calls.Add(1) == 1 {
			writeSSE(w, respCreated, respInProgress)
			cutConnection(t, w)
			return
		}
		writeSSE(w, respCreated, respInProgress, respDelta, respCompleted)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "openai_responses"})
	w := h.do(http.MethodPost, "/v1/responses", responsesBody, nil)
	want := respCreated + respInProgress + respDelta + respCompleted
	if w.Code != http.StatusOK || w.Body.String() != want {
		t.Fatalf("status %d\n got: %q\nwant: %q", w.Code, w.Body.String(), want)
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("upstream calls = %d, want 2（断开后应同路由重试一次）", n)
	}
	if u := latestUsage(t, h); !u.Success || u.OutputTokens != 3 {
		t.Fatalf("usage success=%v out=%d", u.Success, u.OutputTokens)
	}
	if r := h.route(0); r.TempUnschedulableUntil != nil {
		t.Fatalf("一次断流不应冷却路由：until=%v", r.TempUnschedulableUntil)
	}
}

// 已经写出可见输出后断开：没法重试，客户端收到带原因的错误终态（sub2api 同样写 "upstream stream disconnected: <原因>"）。
func TestE2E_ResponsesDisconnectAfterOutputReportsCause(t *testing.T) {
	var calls atomic.Int32
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		calls.Add(1)
		writeSSE(w, respCreated, respDelta)
		cutConnection(t, w)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "openai_responses"})
	w := h.do(http.MethodPost, "/v1/responses", responsesBody, nil)
	got := w.Body.String()
	if !strings.HasPrefix(got, respCreated+respDelta) {
		t.Fatalf("已输出的内容应原样到达客户端：%q", got)
	}
	if !strings.Contains(got, "upstream stream disconnected: unexpected EOF") || !strings.Contains(got, "response.failed") {
		t.Fatalf("应以带原因的 response.failed 结束：%q", got)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("已提交的流不能重试：upstream calls = %d", n)
	}
}

// 推理阶段迟迟没有可见输出：给客户端写 SSE 注释保活（提交 200 响应头），输出到了照常写出。
func TestE2E_PreOutputKeepaliveBeforeFirstOutput(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		writeSSE(w, respCreated, respInProgress)
		time.Sleep(1500 * time.Millisecond)
		writeSSE(w, respDelta, respCompleted)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "openai_responses"})
	setGatewayConfig(h, func(c *config.GatewayConfig) { c.StreamKeepaliveSeconds = 1 })
	w := h.do(http.MethodPost, "/v1/responses", responsesBody, nil)
	want := ": keepalive\n\n" + respCreated + respInProgress + respDelta + respCompleted
	if w.Code != http.StatusOK || w.Body.String() != want {
		t.Fatalf("status %d\n got: %q\nwant: %q", w.Code, w.Body.String(), want)
	}
	if ct := w.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q", ct)
	}
}

// 保活之后所有尝试都失败：响应头已发出，最终错误只能在流里以 SSE 错误终态给出，不能再写 JSON 错误体。
func TestE2E_PreludeThenAllAttemptsFailEndsWithSSEError(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		writeSSE(w, respCreated)
		time.Sleep(1200 * time.Millisecond)
		cutConnection(t, w)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "openai_responses"})
	setGatewayConfig(h, func(c *config.GatewayConfig) { c.StreamKeepaliveSeconds = 1 })
	w := h.do(http.MethodPost, "/v1/responses", responsesBody, nil)
	got := w.Body.String()
	if w.Code != http.StatusOK || !strings.HasPrefix(got, ": keepalive\n\n") {
		t.Fatalf("status %d body %q", w.Code, got)
	}
	if strings.Contains(got, "response.created") {
		t.Fatalf("失败尝试攒着的事件不应写给客户端：%q", got)
	}
	if !strings.Contains(got, "event: response.failed") || strings.Contains(got, `"upstream_ops_request_id"`) {
		t.Fatalf("应以 SSE 错误终态结束，而不是 JSON 错误体：%q", got)
	}
}

// 哪些上游 Responses 事件算「可见输出」（与 sub2api openAIStreamDataStartsClientOutput 对照）。
func TestResponsesEventStartsOutput(t *testing.T) {
	cases := []struct {
		event, data string
		want        bool
	}{
		{"response.created", `{"type":"response.created"}`, false},
		{"response.in_progress", `{"type":"response.in_progress"}`, false},
		{"", `{"type":"response.created"}`, false}, // 没有 event 行时看 data.type
		{"response.output_item.added", `{"type":"response.output_item.added","item":{"type":"reasoning","summary":[]}}`, false},
		{"response.output_item.added", `{"type":"response.output_item.added","item":{"type":"reasoning","encrypted_content":"x"}}`, true},
		{"response.output_item.added", `{"type":"response.output_item.added","item":{"type":"message","content":[]}}`, false},
		{"response.output_item.added", `{"type":"response.output_item.added","item":{"type":"function_call","arguments":""}}`, false},
		{"response.content_part.added", `{"type":"response.content_part.added","part":{"type":"output_text","text":""}}`, false},
		{"response.content_part.added", `{"type":"response.content_part.added","part":{"type":"output_text","text":"a"}}`, true},
		{"response.reasoning_summary_part.added", `{"part":{"type":"summary_text","text":""}}`, false},
		{"response.output_text.delta", `{"delta":"a"}`, true},
		{"response.completed", `{"type":"response.completed"}`, true},
		{"response.failed", `{"type":"response.failed"}`, true}, // 与 sub2api 不同：失败事件原样写出
		{"error", `{"type":"error"}`, true},
		{"response.created", ``, false},
	}
	for _, tc := range cases {
		if got := responsesEventStartsOutput(tc.event, tc.data); got != tc.want {
			t.Errorf("%s %s = %v, want %v", tc.event, tc.data, got, tc.want)
		}
	}
}

func TestStreamDisconnectCause(t *testing.T) {
	cases := map[string]error{
		"unexpected EOF":                    io.ErrUnexpectedEOF,
		"connection reset by peer":          fmt.Errorf("read tcp 10.0.0.1:1->10.0.0.2:2: %w", syscall.ECONNRESET),
		"http2 stream error INTERNAL_ERROR": errors.New("stream error: stream ID 7; INTERNAL_ERROR; received from peer"),
		"http2 GOAWAY ENHANCE_YOUR_CALM":    errors.New(`http2: server sent GOAWAY and closed the connection; LastStreamID=9, ErrCode=ENHANCE_YOUR_CALM, debug=""`),
		"http2 connection lost":             errors.New("http2: client connection lost"),
		"deadline exceeded":                 context.DeadlineExceeded,
		"upstream connection error":         errors.New("something odd at 10.1.2.3:443"),
	}
	for want, err := range cases {
		got := streamDisconnectCause(err)
		if got != want {
			t.Errorf("cause(%v) = %q, want %q", err, got, want)
		}
		if strings.Contains(got, "10.") {
			t.Errorf("原因里不能带地址：%q", got)
		}
	}
}

// 连续传输失败才冷却；同一时刻（一次连接断开）的多次失败只算一次；成功清零。
func TestRouteHealthStreakCollapsesBursts(t *testing.T) {
	var h routeHealth
	for i := 0; i < 10; i++ {
		if h.noteTransportFailure(1) {
			t.Fatal("同一时刻的一串失败不应触发冷却")
		}
	}
	if s := h.transportStreak(1); s != 1 {
		t.Fatalf("合并后 streak = %d, want 1", s)
	}
	h.noteSuccess(1)
	if s := h.transportStreak(1); s != 0 {
		t.Fatalf("成功后 streak = %d", s)
	}
	// 拉开间隔的失败：第三次触发冷却并清零
	h.mu.Lock()
	h.streak[2], h.counted[2] = 2, time.Now().Add(-transportFailureCollapse-time.Second)
	h.mu.Unlock()
	if !h.noteTransportFailure(2) {
		t.Fatal("连续第三次失败应冷却")
	}
	if s := h.transportStreak(2); s != 0 {
		t.Fatalf("冷却后应清零：%d", s)
	}
}

func TestRouteTryBudget(t *testing.T) {
	b := newRouteTryBudget(retryPolicy{enabled: true, sameRouteRetries: 1})
	if !b.take(false) || b.backoff != 0 {
		t.Fatal("组配置的重试先用，不退避")
	}
	if b.take(false) {
		t.Fatal("非传输错误用完组配置就不再重试")
	}
	if !b.take(true) || b.backoff != transportRetryBackoff {
		t.Fatal("传输错误另补一次，带退避")
	}
	if b.take(true) {
		t.Fatal("传输错误只补一次")
	}
	off := newRouteTryBudget(retryPolicy{enabled: false})
	if off.take(true) {
		t.Fatal("重试总开关关闭时不补试")
	}
}

// 唯一的路由在冷却中：仍借它试一次，成功后解除冷却（以前直接回 all upstream routes failed）。
func TestE2E_AllRoutesCooledStillTriesEarliest(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, chatCompletionJSON)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "openai_chat"})
	if err := h.repo.SetTempUnschedulable(h.routes[0].ID, time.Now().Add(time.Minute), "test", time.Now(), "r"); err != nil {
		t.Fatal(err)
	}
	w := h.do(http.MethodPost, "/v1/chat/completions", `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d body %s", w.Code, w.Body.String())
	}
	if r := h.route(0); r.TempUnschedulableUntil != nil {
		t.Fatalf("借用成功后应解除冷却：%v", r.TempUnschedulableUntil)
	}
	if n := h.svc.metrics.routeCooldownBorrow.Load(); n != 1 {
		t.Fatalf("borrow metric = %d", n)
	}
}

// 冷却中的路由被禁用 / 自动禁用时不借用。
func TestEarliestCooledRouteSkipsDisabled(t *testing.T) {
	now := time.Now()
	later, sooner := now.Add(time.Minute), now.Add(10*time.Second)
	routes := []storage.GatewayRoute{
		{ID: 1, Enabled: true, GatewayProviderID: 1, SourceKind: storage.GatewayRouteSourceProvider, TempUnschedulableUntil: &later},
		{ID: 2, Enabled: true, GatewayProviderID: 1, SourceKind: storage.GatewayRouteSourceProvider, TempUnschedulableUntil: &sooner, AutoDisabled: true},
		{ID: 3, Enabled: false, GatewayProviderID: 1, SourceKind: storage.GatewayRouteSourceProvider, TempUnschedulableUntil: &sooner},
	}
	got, ok := earliestCooledRoute(routes, nil, now, nil)
	if !ok || got.Route.ID != 1 {
		t.Fatalf("got %v %v, want route 1", got.Route.ID, ok)
	}
	if _, ok := earliestCooledRoute(routes, nil, now, map[uint]struct{}{1: {}}); ok {
		t.Fatal("排除后不应再借用")
	}
}

// 请求体超过上限回 413，不整包读进内存。
func TestE2E_RequestBodyLimit(t *testing.T) {
	var calls atomic.Int32
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		calls.Add(1)
		_, _ = io.WriteString(w, chatCompletionJSON)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "openai_chat"})
	setGatewayConfig(h, func(c *config.GatewayConfig) { c.MaxRequestBodyBytes = 1024 })
	big := `{"model":"gpt-x","messages":[{"role":"user","content":"` + strings.Repeat("a", 2048) + `"}]}`
	w := h.do(http.MethodPost, "/v1/chat/completions", big, nil)
	if w.Code != http.StatusRequestEntityTooLarge || !strings.Contains(w.Body.String(), "request_too_large") {
		t.Fatalf("status %d body %s", w.Code, w.Body.String())
	}
	if calls.Load() != 0 {
		t.Fatal("超限请求不应发往上游")
	}
	// 上限内照常转发
	w = h.do(http.MethodPost, "/v1/chat/completions", `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
}

// 旁路记用量的缓冲有上限：大流只留头尾，用量照常算出。
func TestBoundedLineBufferKeepsHeadAndTail(t *testing.T) {
	b := newBoundedLineBuffer(64, 256)
	b.WriteLine(`{"type":"message_start","usage":{"input_tokens":7}}`)
	for i := 0; i < 10000; i++ {
		b.WriteLine(`{"type":"content_block_delta","delta":{"text":"xxxxxxxxxxxxxxxx"}}`)
	}
	b.WriteLine(`{"type":"message_delta","usage":{"output_tokens":3}}`)
	if n := b.Len(); n > 64+2*256+128 {
		t.Fatalf("缓冲大小 %d 没有封顶", n)
	}
	got := string(b.Bytes())
	if !strings.HasPrefix(got, `{"type":"message_start"`) || !strings.HasSuffix(got, "\"output_tokens\":3}}\n") {
		t.Fatalf("应保留首行与末行：%q…%q", got[:40], got[len(got)-40:])
	}
	// 再长的最后一行也完整保留
	long := `{"type":"response.completed","x":"` + strings.Repeat("y", 1000) + `"}`
	b.WriteLine(long)
	if !strings.HasSuffix(string(b.Bytes()), long+"\n") {
		t.Fatal("超长的最后一行被截掉了")
	}
}

func TestE2E_LargeStreamUsageStillCounted(t *testing.T) {
	chunk := `data: {"id":"c","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"` + strings.Repeat("z", 4096) + `"}}]}` + "\n\n"
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		frames := make([]string, 0, 1100)
		for i := 0; i < 1000; i++ { // ~4MB
			frames = append(frames, chunk)
		}
		frames = append(frames,
			"data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
			"data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"choices\":[],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3}}\n\n",
			"data: [DONE]\n\n")
		writeSSE(w, frames...)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "openai_chat"})
	w := h.do(http.MethodPost, "/v1/chat/completions", `{"model":"gpt-x","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}]}`, nil)
	if w.Code != http.StatusOK || !strings.HasSuffix(w.Body.String(), "data: [DONE]\n\n") {
		t.Fatalf("status %d", w.Code)
	}
	if u := latestUsage(t, h); !u.Success || u.InputTokens != 7 || u.OutputTokens != 3 {
		t.Fatalf("usage in=%d out=%d success=%v", u.InputTokens, u.OutputTokens, u.Success)
	}
}

// 转发 client 开了 HTTP/2 健康探测（sub2api enableHTTP2KeepAlive）。
func TestUpstreamClientHasHTTP2HealthCheck(t *testing.T) {
	c := newUpstreamClient("", time.Minute)
	tr, ok := c.Transport.(*http.Transport)
	if !ok || tr.HTTP2 == nil {
		t.Fatal("transport 没有 HTTP2 配置")
	}
	if tr.HTTP2.SendPingTimeout != upstreamHTTP2PingInterval || tr.HTTP2.PingTimeout != upstreamHTTP2PingTimeout {
		t.Fatalf("ping = %s / %s", tr.HTTP2.SendPingTimeout, tr.HTTP2.PingTimeout)
	}
	if !tr.ForceAttemptHTTP2 {
		t.Fatal("仍应优先 HTTP/2")
	}
}

// GET /metrics：没配令牌 404，令牌错 401，对了输出 Prometheus 文本；计数随请求增加。
func TestMetricsEndpoint(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, chatCompletionJSON)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "openai_chat"})
	h.engine.GET("/metrics", h.svc.MetricsHandler())
	get := func(auth string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		rec := httptest.NewRecorder()
		h.engine.ServeHTTP(rec, req)
		return rec
	}
	if w := get("Bearer x"); w.Code != http.StatusNotFound {
		t.Fatalf("未配置令牌应 404：%d", w.Code)
	}
	setGatewayConfig(h, func(c *config.GatewayConfig) { c.MetricsToken = "secret" })
	if w := get("Bearer wrong"); w.Code != http.StatusUnauthorized {
		t.Fatalf("令牌错应 401：%d", w.Code)
	}
	_ = h.do(http.MethodPost, "/v1/chat/completions", `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`, nil)
	w := get("Bearer secret")
	if w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	body := w.Body.String()
	for _, want := range []string{
		`gateway_requests_total{class="2xx"} 1`,
		`gateway_upstream_attempts_total{result="success"} 1`,
		"# TYPE gateway_streams_inflight gauge",
		"gateway_write_behind_usage_queue_depth",
		"gateway_routes_paused 0",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("缺少 %q：\n%s", want, body)
		}
	}
	// 每行要么是注释要么是 "名字{标签} 值"
	sc := bufio.NewScanner(strings.NewReader(body))
	for sc.Scan() {
		line := sc.Text()
		if line != "" && !strings.HasPrefix(line, "# ") && len(strings.Fields(line)) != 2 {
			t.Fatalf("格式不对：%q", line)
		}
	}
}
