package gateway

// 首个可见输出之前的流内错误（照 sub2api）与 Chat 终态信号，见 stream_preoutput_failure.go。

import (
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
)

// 线上 2026-10-16 见过的 response.failed（上游过载），原样转给了客户端。
const respFailedOverloaded = "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_1\",\"status\":\"failed\",\"error\":{\"code\":\"server_error\",\"message\":\"Our servers are currently overloaded. Please try again later.\",\"param\":null,\"type\":\"service_unavailable_error\"}}}\n\n"

func TestPreOutputStreamFailureClassification(t *testing.T) {
	cases := []struct {
		name, event, data string
		kind              protocolKind
		wantOK            bool
		wantStatus        int
	}{
		{"responses overloaded", "response.failed", `{"type":"response.failed","response":{"error":{"code":"server_error","message":"Our servers are currently overloaded. Please try again later.","type":"service_unavailable_error"}}}`, protocol.KindOpenAIResponses, true, http.StatusServiceUnavailable},
		{"responses processing error", "response.failed", `{"type":"response.failed","response":{"error":{"code":"server_error","message":"An error occurred while processing your request. You can retry your request, or contact us through our help center at help.openai.com if the error persists. Please include the request ID x in your message."}}}`, protocol.KindOpenAIResponses, true, http.StatusBadGateway},
		{"responses rate limit", "response.failed", `{"response":{"error":{"code":"rate_limit_exceeded","type":"rate_limit_error","message":"slow down"}}}`, protocol.KindOpenAIResponses, true, http.StatusTooManyRequests},
		{"responses context window", "response.failed", `{"response":{"error":{"code":"context_length_exceeded","message":"Your input exceeds the context window of this model."}}}`, protocol.KindOpenAIResponses, false, 0},
		{"responses content policy", "response.failed", `{"response":{"error":{"code":"content_policy_violation","message":"blocked"}}}`, protocol.KindOpenAIResponses, false, 0},
		{"responses invalid request", "response.failed", `{"response":{"error":{"type":"invalid_request_error","message":"bad param"}}}`, protocol.KindOpenAIResponses, false, 0},
		{"responses error event overloaded", "error", `{"type":"error","error":{"code":"server_is_overloaded","message":"busy"}}`, protocol.KindOpenAIResponses, true, http.StatusServiceUnavailable},
		// 与 sub2api 一致：错误码只看 error.code / response.error.code，顶层 code 不认
		{"responses error event top-level code", "error", `{"type":"error","code":"server_is_overloaded","message":"busy"}`, protocol.KindOpenAIResponses, false, 0},
		{"responses error event plain", "error", `{"type":"error","message":"something broke"}`, protocol.KindOpenAIResponses, false, 0},
		{"responses error event try again", "", `{"type":"error","message":"temporary failure, please try again"}`, protocol.KindOpenAIResponses, true, http.StatusBadGateway},
		{"responses output not failure", "response.output_text.delta", `{"delta":"a"}`, protocol.KindOpenAIResponses, false, 0},
		{"anthropic overloaded", "error", `{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`, protocolAnthropic, true, 529},
		{"anthropic api error", "error", `{"type":"error","error":{"type":"api_error","message":"Internal"}}`, protocolAnthropic, true, http.StatusInternalServerError},
		{"anthropic invalid request", "error", `{"type":"error","error":{"type":"invalid_request_error","message":"bad"}}`, protocolAnthropic, true, http.StatusBadRequest},
		{"anthropic message_start", "message_start", `{"type":"message_start","message":{}}`, protocolAnthropic, false, 0},
		{"chat error chunk untouched", "", `{"error":{"message":"overloaded"}}`, protocol.KindOpenAIChat, false, 0},
	}
	for _, tc := range cases {
		status, body, ok := preOutputStreamFailure(tc.event, tc.data, tc.kind)
		if ok != tc.wantOK || status != tc.wantStatus {
			t.Errorf("%s: ok=%v status=%d, want ok=%v status=%d", tc.name, ok, status, tc.wantOK, tc.wantStatus)
		}
		if ok {
			if _, msg, _ := extractUpstreamError(body); msg == "" {
				t.Errorf("%s: 错误体取不出信息：%s", tc.name, body)
			}
		}
	}
}

func TestChatChunkFinished(t *testing.T) {
	for data, want := range map[string]bool{
		`{"choices":[{"index":0,"delta":{"content":"a"},"finish_reason":null}]}`: false,
		`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`:            true,
		`{"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`:      true,
		`{"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":2}}`:       true,
		`{"choices":[{"index":0,"delta":{"content":"a"}}],"usage":null}`:         false,
		`[DONE]`: false,
	} {
		if got := chatChunkFinished(data); got != want {
			t.Errorf("chatChunkFinished(%s) = %v, want %v", data, got, want)
		}
	}
}

// Responses：输出前上游 response.failed（过载）——换到第二条路由，客户端拿到的是第二条路由的完整流。
func TestE2E_ResponsesFailedBeforeOutputFailsOver(t *testing.T) {
	var first, second atomic.Int32
	upA := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		first.Add(1)
		writeSSE(w, respCreated, respInProgress, respFailedOverloaded)
	})
	upB := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		second.Add(1)
		writeSSE(w, respCreated, respDelta, respCompleted)
	})
	h := newE2E(t, nil,
		e2eRouteSpec{Upstream: upA, Protocol: "openai_responses", Weight: 2},
		e2eRouteSpec{Upstream: upB, Protocol: "openai_responses", Weight: 1})
	w := h.do(http.MethodPost, "/v1/responses", responsesBody, nil)
	want := respCreated + respDelta + respCompleted
	if w.Code != http.StatusOK || w.Body.String() != want {
		t.Fatalf("status %d\n got: %q\nwant: %q", w.Code, w.Body.String(), want)
	}
	if first.Load() != 1 || second.Load() != 1 {
		t.Fatalf("calls a=%d b=%d", first.Load(), second.Load())
	}
	if r := h.route(0); r.TempUnschedulableUntil == nil {
		t.Fatal("过载的路由应按 503 进冷却")
	}
}

// Responses：不可重试的失败（上下文超长）照常写给客户端，不换路由。
func TestE2E_ResponsesNonRetryableFailureStillForwarded(t *testing.T) {
	failed := "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"context_length_exceeded\",\"message\":\"Your input exceeds the context window of this model.\"}}}\n\n"
	var calls atomic.Int32
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		calls.Add(1)
		writeSSE(w, respCreated, failed)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "openai_responses"})
	w := h.do(http.MethodPost, "/v1/responses", responsesBody, nil)
	if got := w.Body.String(); got != respCreated+failed {
		t.Fatalf("应原样转发：%q", got)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d", calls.Load())
	}
}

// Responses：两条路由都过载，重试耗尽后以上游错误回给客户端（没写出过任何东西，仍是普通 HTTP 错误）。
func TestE2E_ResponsesFailedBeforeOutputExhaustedReturnsError(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		writeSSE(w, respCreated, respFailedOverloaded)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "openai_responses"})
	w := h.do(http.MethodPost, "/v1/responses", responsesBody, nil)
	if w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), "currently overloaded") {
		t.Fatalf("status %d body %s", w.Code, w.Body.String())
	}
}

const (
	anthMessageStart = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"usage\":{\"input_tokens\":7,\"output_tokens\":1}}}\n\n"
	anthOverloaded   = "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n"
	anthRest         = "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n" +
		"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n" +
		"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":3}}\n\n" +
		"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
	anthBody = `{"model":"claude-x","stream":true,"max_tokens":10,"messages":[{"role":"user","content":"hi"}]}`
)

// Anthropic：流一开始就是 event: error（过载）——按 529 换路由（sub2api 同样换号）。
func TestE2E_AnthropicErrorEventBeforeOutputFailsOver(t *testing.T) {
	upA := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		writeSSE(w, anthOverloaded)
	})
	upB := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		writeSSE(w, anthMessageStart, anthRest)
	})
	h := newE2E(t, nil,
		e2eRouteSpec{Upstream: upA, Protocol: "anthropic", Weight: 2},
		e2eRouteSpec{Upstream: upB, Protocol: "anthropic", Weight: 1})
	w := h.do(http.MethodPost, "/v1/messages", anthBody, nil)
	if w.Code != http.StatusOK || w.Body.String() != anthMessageStart+anthRest {
		t.Fatalf("status %d body %q", w.Code, w.Body.String())
	}
	if u := latestUsage(t, h); !u.Success || u.OutputTokens != 3 {
		t.Fatalf("usage success=%v out=%d", u.Success, u.OutputTokens)
	}
}

// Anthropic：message_start 已经写出之后的 error 事件照旧转给客户端（已提交，不能再换路由）。
func TestE2E_AnthropicErrorEventAfterStartForwarded(t *testing.T) {
	var calls atomic.Int32
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		calls.Add(1)
		writeSSE(w, anthMessageStart, anthOverloaded)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "anthropic"})
	w := h.do(http.MethodPost, "/v1/messages", anthBody, nil)
	if got := w.Body.String(); got != anthMessageStart+anthOverloaded {
		t.Fatalf("got %q", got)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d", calls.Load())
	}
}

// Chat：上游发完 finish_reason / usage 块后不带 [DONE] 直接关连接——算正常结束，不追加错误（以前判成断流）。
func TestE2E_ChatStreamEndsWithoutDoneIsComplete(t *testing.T) {
	stream := "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n" +
		"data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" +
		"data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"choices\":[],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3}}\n\n"
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		writeSSE(w, stream)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "openai_chat"})
	w := h.do(http.MethodPost, "/v1/chat/completions", `{"model":"gpt-x","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}]}`, nil)
	if got := w.Body.String(); got != stream {
		t.Fatalf("应原样结束，不追加错误：%q", got)
	}
	if u := latestUsage(t, h); !u.Success || u.OutputTokens != 3 {
		t.Fatalf("usage success=%v out=%d err=%s", u.Success, u.OutputTokens, u.ErrorMessage)
	}
}

// Chat：真的半截断开（没有任何终止信号）仍按断流处理。
func TestE2E_ChatStreamTruncatedStillFails(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		writeSSE(w, "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n")
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "openai_chat"})
	w := h.do(http.MethodPost, "/v1/chat/completions", `{"model":"gpt-x","stream":true,"messages":[{"role":"user","content":"hi"}]}`, nil)
	if !strings.Contains(w.Body.String(), "closed before terminal event") {
		t.Fatalf("got %q", w.Body.String())
	}
}
