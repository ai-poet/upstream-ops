package gateway

// 转发保真：同协议、没有模型映射时，请求体 / 查询串原样到达上游，响应体原样回给客户端。
// 已知的、有意的差异都写在用例里：Chat 流式客户端没要 usage 时网关注入 include_usage（计费要用），
// 上游因此多回的 usage 帧不转给客户端；跨协议转换时不带原查询串；Gemini 的 ?key= 是网关密钥不外传。

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// 请求体与查询串：逐字节比对上游收到的内容。
func TestE2E_RequestFidelity(t *testing.T) {
	odd := `{"zeta":1,"big":1234567890123456789,"html":"<b>&</b>","emoji":"😀","nested":{"b":[1,2],"a":null},`
	cases := []struct {
		name, protocol, path, body string
		wantURI, wantBody          string // wantBody 为空表示与 body 相同
	}{
		{name: "chat", protocol: "openai_chat", path: "/v1/chat/completions?foo=bar&a=1",
			body:    odd + `"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}` + "\n",
			wantURI: "/v1/chat/completions?foo=bar&a=1"},
		{name: "chat stream with usage", protocol: "openai_chat", path: "/v1/chat/completions",
			body:    `{"model":"gpt-4o","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}]}`,
			wantURI: "/v1/chat/completions"},
		{name: "chat stream without usage", protocol: "openai_chat", path: "/v1/chat/completions",
			body:     `{"model":"gpt-4o","stream":true,"messages":[{"role":"user","content":"hi"}]}`,
			wantURI:  "/v1/chat/completions",
			wantBody: `{"model":"gpt-4o","stream":true,"messages":[{"role":"user","content":"hi"}],"stream_options":{"include_usage":true}}`},
		{name: "responses", protocol: "openai_responses", path: "/v1/responses",
			body:    odd + `"model":"gpt-4o","input":"hi","store":false}`,
			wantURI: "/v1/responses"},
		{name: "responses stream", protocol: "openai_responses", path: "/v1/responses",
			body:    `{"model":"gpt-4o","stream":true,"input":"hi"}`,
			wantURI: "/v1/responses"},
		{name: "anthropic", protocol: "anthropic", path: "/v1/messages?beta=true",
			body:    odd + `"model":"claude-sonnet-4-5","max_tokens":10,"messages":[{"role":"user","content":"hi"}]}`,
			wantURI: "/v1/messages?beta=true"},
		{name: "anthropic stream", protocol: "anthropic", path: "/v1/messages?beta=true",
			body:    `{"model":"claude-sonnet-4-5","stream":true,"max_tokens":10,"messages":[{"role":"user","content":"hi"}]}`,
			wantURI: "/v1/messages?beta=true"},
		{name: "gemini", protocol: "gemini", path: "/v1beta/models/gemini-2.5-pro:generateContent?key=gateway-secret&foo=bar",
			body:    odd + `"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`,
			wantURI: "/v1beta/models/gemini-2.5-pro:generateContent?foo=bar"},
		{name: "gemini stream", protocol: "gemini", path: "/v1beta/models/gemini-2.5-pro:streamGenerateContent?alt=sse",
			body:    `{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`,
			wantURI: "/v1beta/models/gemini-2.5-pro:streamGenerateContent?alt=sse"},
		// 跨协议：上游端点换了，原查询串不带
		{name: "anthropic client via chat upstream", protocol: "openai_chat", path: "/v1/messages?beta=true",
			body:    `{"model":"claude-sonnet-4-5","max_tokens":10,"messages":[{"role":"user","content":"hi"}]}`,
			wantURI: "/v1/chat/completions"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotBody, gotURI string
			up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
				gotBody, gotURI = string(body), r.URL.RequestURI()
				if strings.Contains(tc.name, "stream") {
					writeSSE(w, "data: [DONE]\n\n")
					return
				}
				_, _ = io.WriteString(w, `{}`)
			})
			h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: tc.protocol})
			header := map[string]string{"Authorization": "Bearer " + h.key}
			if tc.protocol == "gemini" {
				header = map[string]string{"x-goog-api-key": h.key}
			}
			_ = h.do(http.MethodPost, tc.path, tc.body, header)
			if gotURI != tc.wantURI {
				t.Fatalf("upstream URI = %q, want %q", gotURI, tc.wantURI)
			}
			if strings.Contains(tc.name, "via") {
				return // 跨协议请求体会被转换，只看查询串
			}
			want := tc.wantBody
			if want == "" {
				want = tc.body
			}
			if gotBody != want {
				t.Fatalf("upstream body differs\n got: %s\nwant: %s", gotBody, want)
			}
		})
	}
}

// 非流式响应体：四种协议逐字节原样回给客户端，上游响应头带回。
func TestE2E_ResponseFidelity(t *testing.T) {
	cases := []struct{ protocol, path, req, resp string }{
		{"openai_chat", "/v1/chat/completions", `{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}`,
			`{"zeta":1,"id":"x","object":"chat.completion","big":1234567890123456789,"html":"<b>&</b>","choices":[{"index":0,"message":{"role":"assistant","content":"你好  "},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":1}}`},
		{"openai_responses", "/v1/responses", `{"model":"gpt-4o","input":"hi"}`,
			`{"zeta":1,"id":"resp_x","object":"response","status":"completed","output":[],"usage":{"input_tokens":5,"output_tokens":1}}`},
		{"anthropic", "/v1/messages", `{"model":"claude-sonnet-4-5","max_tokens":10,"messages":[{"role":"user","content":"hi"}]}`,
			`{"zeta":1,"id":"msg_x","type":"message","role":"assistant","content":[{"type":"text","text":"<b>&"}],"stop_reason":"end_turn","usage":{"input_tokens":5,"output_tokens":1}}`},
		{"gemini", "/v1beta/models/gemini-2.5-pro:generateContent", `{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`,
			`{"zeta":1,"candidates":[{"content":{"role":"model","parts":[{"text":"<b>&"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":5,"candidatesTokenCount":1}}`},
	}
	for _, tc := range cases {
		t.Run(tc.protocol, func(t *testing.T) {
			up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Upstream-Thing", "kept")
				_, _ = io.WriteString(w, tc.resp)
			})
			h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: tc.protocol})
			header := map[string]string{}
			if tc.protocol == "gemini" {
				header["x-goog-api-key"] = h.key
			}
			w := h.do(http.MethodPost, tc.path, tc.req, header)
			if w.Code != http.StatusOK || w.Body.String() != tc.resp {
				t.Fatalf("status %d\n got: %s\nwant: %s", w.Code, w.Body.String(), tc.resp)
			}
			if w.Header().Get("X-Upstream-Thing") != "kept" {
				t.Fatalf("upstream header dropped: %v", w.Header())
			}
		})
	}
}

// 流式响应：同协议透传逐字节一致；客户端没要 usage 时，网关注入 include_usage 换来的 usage 帧
// 只用于记用量，不转给客户端（很多客户端直接读 choices[0]，空 choices 的帧会让它们出错）。
func TestE2E_StreamResponseFidelity(t *testing.T) {
	content := "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"<b>&\"},\"finish_reason\":null}]}\n\n" +
		"data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"
	usageFrame := "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"choices\":[],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3}}\n\n"
	done := "data: [DONE]\n\n"
	anthropic := "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"usage\":{\"input_tokens\":7,\"output_tokens\":1}}}\n\n" +
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
		"event: ping\ndata: {\"type\": \"ping\"}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"<b>&\"}}\n\n" +
		"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n" +
		"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":3}}\n\n" +
		"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"

	cases := []struct {
		name, protocol, path, req, upstream, wantClient string
	}{
		{"chat, client asked for usage", "openai_chat", "/v1/chat/completions",
			`{"model":"gpt-4o","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}]}`,
			content + usageFrame + done, content + usageFrame + done},
		{"chat, usage injected by the gateway", "openai_chat", "/v1/chat/completions",
			`{"model":"gpt-4o","stream":true,"messages":[{"role":"user","content":"hi"}]}`,
			content + usageFrame + done, content + done},
		{"anthropic", "anthropic", "/v1/messages",
			`{"model":"claude-sonnet-4-5","stream":true,"max_tokens":10,"messages":[{"role":"user","content":"hi"}]}`,
			anthropic, anthropic},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
				writeSSE(w, tc.upstream)
			})
			h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: tc.protocol})
			w := h.do(http.MethodPost, tc.path, tc.req, nil)
			if got := w.Body.String(); got != tc.wantClient {
				t.Fatalf("client stream differs\n got: %q\nwant: %q", got, tc.wantClient)
			}
			if u := latestUsage(t, h); !u.Success || u.InputTokens != 7 || u.OutputTokens != 3 {
				t.Fatalf("usage = in %d out %d success %v", u.InputTokens, u.OutputTokens, u.Success)
			}
		})
	}
}

// 网关自己的鉴权参数不外传，其余参数顺序和编码不变。
func TestForwardQueryKeepsOrderAndDropsGatewayKey(t *testing.T) {
	for raw, want := range map[string]string{
		"b=2&a=1":                     "b=2&a=1",
		"key=secret&alt=sse&x=%2Fy":   "alt=sse&x=%2Fy",
		"api_key=s&beta=true":         "beta=true",
		"%6Bey=encoded-secret&keep=1": "keep=1",
		"":                            "",
	} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodPost, "/x?"+raw, nil)
		if got := forwardQuery(c); got != want {
			t.Fatalf("forwardQuery(%q) = %q, want %q", raw, got, want)
		}
	}
}
