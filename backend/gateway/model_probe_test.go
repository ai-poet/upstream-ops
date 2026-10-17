package gateway

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/tidwall/gjson"
)

func probeOne(t *testing.T, h *e2eHarness, model string, in TestModelInput) ModelTestResult {
	t.Helper()
	routeID := h.routes[0].ID
	in.Model = model
	in.RouteID = &routeID
	res, err := h.svc.Admin.TestGroupModel(context.Background(), h.group.ID, in)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 {
		t.Fatalf("want 1 result, got %d", len(res))
	}
	return res[0]
}

func sseWrite(w http.ResponseWriter, frames ...string) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	for _, f := range frames {
		_, _ = w.Write([]byte(f))
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
	}
}

func TestDefaultProbeEndpoint(t *testing.T) {
	cases := map[string]string{
		"gpt-6.1-sol":             probeEndpointResponses,
		"gpt-5.6-terra":           probeEndpointResponses,
		"codex-auto-review":       probeEndpointResponses,
		"o3-mini":                 probeEndpointResponses,
		"claude-opus-5-5":         probeEndpointAnthropic,
		"gpt-image-2.5-sunburst":  probeEndpointImages,
		"dall-e-3":                probeEndpointImages,
		"grok-4.7":                probeEndpointChat,
		"kimi-k3":                 probeEndpointChat,
		"gpt-oss-120b":            probeEndpointChat,
		"gemini-3-pro":            probeEndpointChat,
		"deepseek-v4.1-flash":     probeEndpointChat,
		"openrouter/gpt-6-astra":  probeEndpointResponses,
		"anthropic/claude-haiku5": probeEndpointAnthropic,
	}
	for model, want := range cases {
		if got := defaultProbeEndpoint(model); got != want {
			t.Errorf("%s: got %q want %q", model, got, want)
		}
	}
}

func TestBuildProbeBody_ChatTokenCaps(t *testing.T) {
	// 普通模型 max_tokens=16（同 new-api）；GPT-5 及以后 / o 系不带上限；Gemini 3000
	if v := gjson.GetBytes(buildProbeBody(probeEndpointChat, "grok-4.7", false), "max_tokens").Int(); v != 16 {
		t.Fatalf("grok max_tokens = %d", v)
	}
	for _, m := range []string{"gpt-6-astra", "gpt-5", "o3"} {
		b := buildProbeBody(probeEndpointChat, m, true)
		if gjson.GetBytes(b, "max_tokens").Exists() || gjson.GetBytes(b, "max_completion_tokens").Exists() {
			t.Fatalf("%s should carry no token cap: %s", m, b)
		}
	}
	if v := gjson.GetBytes(buildProbeBody(probeEndpointChat, "gpt-4.1", false), "max_tokens").Int(); v != 16 {
		t.Fatalf("gpt-4.1 max_tokens = %d", v)
	}
	if v := gjson.GetBytes(buildProbeBody(probeEndpointChat, "gemini-3-pro", false), "max_tokens").Int(); v != 3000 {
		t.Fatalf("gemini max_tokens = %d", v)
	}
	img := buildProbeBody(probeEndpointImages, "gpt-image-2", false)
	if gjson.GetBytes(img, "quality").String() != "low" || gjson.GetBytes(img, "n").Int() != 1 {
		t.Fatalf("image body: %s", img)
	}
}

// GPT 模型默认按 Responses 流式测：首字取第一个输出事件（response.created 不算），回复取文本增量
func TestProbe_ResponsesStreamMeasuresFirstToken(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		_, _ = w.Write([]byte("event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"model\":\"gpt-6-astra-2026\"}}\n\n"))
		fl.Flush()
		time.Sleep(150 * time.Millisecond)
		_, _ = w.Write([]byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"Hi\"}\n\n" +
			"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\" there\"}\n\n" +
			"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"))
		fl.Flush()
	})
	defer up.Close()
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up})

	res := probeOne(t, h, "gpt-6-astra", TestModelInput{})
	if !res.OK {
		t.Fatalf("want ok, got %+v", res)
	}
	if res.Endpoint != probeEndpointResponses || !res.Stream || res.UpstreamPath != "/v1/responses" {
		t.Fatalf("endpoint=%q stream=%v path=%q", res.Endpoint, res.Stream, res.UpstreamPath)
	}
	if res.FirstTokenMS == nil || *res.FirstTokenMS < 140 {
		t.Fatalf("first token should wait for the first output event, got %v", res.FirstTokenMS)
	}
	if res.Reply != "Hi there" || res.ResponseModel != "gpt-6-astra-2026" {
		t.Fatalf("reply=%q model=%q", res.Reply, res.ResponseModel)
	}
	calls := up.calls
	if len(calls) != 1 || gjson.GetBytes(calls[0].Body, "input.0.content").String() != "hi" || !gjson.GetBytes(calls[0].Body, "stream").Bool() {
		t.Fatalf("unexpected upstream request: %+v", calls)
	}
}

// Responses 流在终态前断开：算失败（线上「upstream stream disconnected」就是这种）
func TestProbe_StreamEndingBeforeTerminalFails(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		sseWrite(w, "event: response.created\ndata: {\"type\":\"response.created\"}\n\n",
			"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"Hi\"}\n\n")
	})
	defer up.Close()
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up})
	res := probeOne(t, h, "gpt-6-astra", TestModelInput{})
	if res.OK || !strings.Contains(res.Error, "before completion") {
		t.Fatalf("want failure, got %+v", res)
	}
}

// 流里的 response.failed：失败并带上游消息
func TestProbe_StreamFailedEvent(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		sseWrite(w, "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"message\":\"account quota exhausted\"}}}\n\n")
	})
	defer up.Close()
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up})
	res := probeOne(t, h, "gpt-6-astra", TestModelInput{})
	if res.OK || !strings.Contains(res.Error, "quota") {
		t.Fatalf("want quota failure, got %+v", res)
	}
}

// 非流式 200 但响应体带 error：算失败（同 new-api detectErrorFromTestResponseBody）
func TestProbe_NonStream200WithErrorPayload(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"error":{"message":"model not available for this key"}}`))
	})
	defer up.Close()
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up})
	off := false
	res := probeOne(t, h, "kimi-k3", TestModelInput{Stream: &off})
	if res.OK || res.Error != "model not available for this key" {
		t.Fatalf("want error payload failure, got %+v", res)
	}
	if res.Stream || res.FirstTokenMS != nil {
		t.Fatalf("non-stream test should have no first token: %+v", res)
	}
}

// Claude 走 Messages 流式：首字取 content_block_delta，终态 message_stop
func TestProbe_AnthropicStream(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		sseWrite(w,
			"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-opus-5-5\"}}\n\n",
			"event: ping\ndata: {\"type\":\"ping\"}\n\n",
			"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"你好\"}}\n\n",
			"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
	})
	defer up.Close()
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up})
	res := probeOne(t, h, "claude-opus-5-5", TestModelInput{})
	if !res.OK || res.Reply != "你好" || res.FirstTokenMS == nil || res.UpstreamProtocol != "anthropic" {
		t.Fatalf("unexpected result %+v", res)
	}
	call := up.calls[0]
	if call.Path != "/v1/messages" || gjson.GetBytes(call.Body, "max_tokens").Int() != 1024 {
		t.Fatalf("unexpected upstream request %s %s", call.Path, call.Body)
	}
}

// 指定端点：Claude 模型选 Chat 时，auto 路由仍把它送到 Anthropic 上游（与真实转发一致，经协议转换）
func TestProbe_ChosenEndpointGoesThroughConversion(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"m","type":"message","model":"claude-haiku-5-5","content":[{"type":"text","text":"hey"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`))
	})
	defer up.Close()
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up})
	off := false
	res := probeOne(t, h, "claude-haiku-5-5", TestModelInput{Endpoint: "chat", Stream: &off})
	if !res.OK || res.Endpoint != probeEndpointChat || res.UpstreamProtocol != "anthropic" || res.UpstreamPath != "/v1/messages" {
		t.Fatalf("unexpected result %+v", res)
	}
	if res.Reply != "hey" {
		t.Fatalf("reply = %q", res.Reply)
	}
}

// 生图模型默认走 Images，非流式，要求响应里真有图片
func TestProbe_ImagesEndpoint(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"created":1,"data":[{"b64_json":"aGk="}]}`))
	})
	defer up.Close()
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up})
	res := probeOne(t, h, "gpt-image-2", TestModelInput{})
	if !res.OK || res.Endpoint != probeEndpointImages || res.Stream || res.UpstreamPath != "/v1/images/generations" {
		t.Fatalf("unexpected result %+v", res)
	}
	if gjson.GetBytes(up.calls[0].Body, "prompt").String() == "" {
		t.Fatalf("image prompt missing: %s", up.calls[0].Body)
	}
}

// 非 2xx：失败并带上游消息与状态码
func TestProbe_UpstreamStatusError(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":{"message":"group forbidden"}}`))
	})
	defer up.Close()
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up})
	res := probeOne(t, h, "grok-4.7", TestModelInput{})
	if res.OK || res.StatusCode != http.StatusForbidden || res.Error != "group forbidden" {
		t.Fatalf("unexpected result %+v", res)
	}
}

// 单测指定路由时，禁用的路由也能测（启用前先验证）
func TestProbe_DisabledRouteCanBeTested(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		sseWrite(w, "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n",
			"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n", "data: [DONE]\n\n")
	})
	defer up.Close()
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up})
	r := h.route(0)
	r.Enabled = false
	if err := h.repo.SaveForGroup(h.group.ID, []storage.GatewayRoute{r}); err != nil {
		t.Fatal(err)
	}
	h.routes[0] = h.route(0)
	res := probeOne(t, h, "kimi-k3", TestModelInput{})
	if !res.OK || res.Reply != "ok" {
		t.Fatalf("unexpected result %+v", res)
	}
}

func TestNormalizeProbeEndpoint(t *testing.T) {
	for in, want := range map[string]string{"": "", "auto": "", "openai_responses": probeEndpointResponses, "Messages": probeEndpointAnthropic, "image-generation": probeEndpointImages} {
		got, err := normalizeProbeEndpoint(in)
		if err != nil || got != want {
			t.Errorf("%q: got %q err %v", in, got, err)
		}
	}
	if _, err := normalizeProbeEndpoint("embeddings"); err == nil {
		t.Fatal("unknown endpoint should be rejected")
	}
}
