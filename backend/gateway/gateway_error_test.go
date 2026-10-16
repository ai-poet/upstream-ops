package gateway

import (
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/gin-gonic/gin"
)

var testRequestIDPattern = regexp.MustCompile(`[0-9a-f]{24}`)

func writeTestError(kind protocolKind, e *gatewayError) *httptest.ResponseRecorder {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/x", nil)
	writeError(c, kind, e)
	return w
}

// 网关自己产生的错误按客户端协议渲染，带上 request id（响应头 + JSON 字段）。
func TestWriteErrorGatewayShapes(t *testing.T) {
	for _, tc := range []struct {
		kind protocolKind
		want string
	}{
		{protocol.KindOpenAIChat, `{"error":{"message":"bad","type":"invalid_request_error"},"upstream_ops_request_id":"<rid>"}`},
		{protocol.KindOpenAIResponses, `{"error":{"message":"bad","type":"invalid_request_error"},"upstream_ops_request_id":"<rid>"}`},
		{protocol.KindAnthropic, `{"error":{"message":"bad","type":"invalid_request_error"},"type":"error","upstream_ops_request_id":"<rid>"}`},
		{protocol.KindGemini, `{"error":{"code":400,"message":"bad","status":"INVALID_ARGUMENT"},"upstream_ops_request_id":"<rid>"}`},
	} {
		w := writeTestError(tc.kind, newGatewayError(http.StatusBadRequest, "invalid_request_error", "bad"))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d", tc.kind, w.Code)
		}
		if got := testRequestIDPattern.ReplaceAllString(w.Body.String(), "<rid>"); got != tc.want {
			t.Fatalf("%s body = %s\nwant %s", tc.kind, got, tc.want)
		}
		if ct := w.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
			t.Fatalf("%s content-type = %q", tc.kind, ct)
		}
		if !testRequestIDPattern.MatchString(w.Header().Get(headerUpstreamOpsRequestID)) {
			t.Fatalf("%s missing request id header", tc.kind)
		}
	}
}

// 上游错误体原样回显：注入 request id，带回上游响应头（不抄上游的 Content-Length / request id）。
func TestWriteErrorUpstreamEcho(t *testing.T) {
	header := http.Header{
		"X-Up":                     []string{"1"},
		"Content-Length":           []string{"999"},
		headerUpstreamOpsRequestID: []string{"from-upstream"},
	}
	w := writeTestError(protocol.KindOpenAIChat, upstreamError(http.StatusTooManyRequests, []byte(`{"error":{"message":"slow down","type":"rate_limit_error","code":"rl"}}`), header))
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d", w.Code)
	}
	want := `{"error":{"message":"slow down","type":"rate_limit_error","code":"rl"},"upstream_ops_request_id":"<rid>"}`
	if got := testRequestIDPattern.ReplaceAllString(w.Body.String(), "<rid>"); got != want {
		t.Fatalf("body = %s", got)
	}
	if w.Header().Get("X-Up") != "1" || w.Header().Get("Content-Length") != "" || w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("headers = %v", w.Header())
	}
	if got := w.Header().Get(headerUpstreamOpsRequestID); got == "from-upstream" || !testRequestIDPattern.MatchString(got) {
		t.Fatalf("request id header = %q", got)
	}
}

// 端到端：上游错误体原样回给客户端——不截断、不重排键、不丢大整数精度、不把纯文本包成 JSON；
// JSON 对象只在末尾多一个 request id 字段。
func TestE2E_UpstreamErrorEchoFidelity(t *testing.T) {
	huge := `{"error":{"message":"` + strings.Repeat("x", 100*1024) + `","type":"invalid_request_error"}}`
	cases := []struct {
		name, body, contentType string
		code                    int
		want, wantType          string // want 为空表示与上游一致
	}{
		{name: "json", code: 400, body: `{"error":{"type":"invalid_request_error","message":"bad <x> & y","code":1234567890123456789,"param":"messages"}}`, contentType: "application/json",
			want: `{"error":{"type":"invalid_request_error","message":"bad <x> & y","code":1234567890123456789,"param":"messages"},"upstream_ops_request_id":"<rid>"}`, wantType: "application/json"},
		{name: "huge json", code: 400, body: huge, contentType: "application/json",
			want: strings.TrimSuffix(huge, "}") + `,"upstream_ops_request_id":"<rid>"}`, wantType: "application/json"},
		{name: "plain text", code: 403, body: "<html>blocked by upstream WAF</html>", contentType: "text/html", wantType: "text/html"},
		{name: "empty", code: 401, body: "", wantType: "text/plain; charset=utf-8"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
				if tc.contentType != "" {
					w.Header().Set("Content-Type", tc.contentType)
				}
				w.WriteHeader(tc.code)
				_, _ = io.WriteString(w, tc.body)
			})
			h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "openai_chat"})
			w := h.do(http.MethodPost, "/v1/chat/completions", `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, nil)
			if w.Code != tc.code {
				t.Fatalf("status = %d, want %d", w.Code, tc.code)
			}
			want := tc.want
			if want == "" {
				want = tc.body
			}
			if got := testRequestIDPattern.ReplaceAllString(w.Body.String(), "<rid>"); got != want {
				if len(got) > 300 {
					got = got[:300] + "…"
				}
				t.Fatalf("body = %s", got)
			}
			if ct := w.Header().Get("Content-Type"); ct != tc.wantType {
				t.Fatalf("content-type = %q, want %q", ct, tc.wantType)
			}
			if !testRequestIDPattern.MatchString(w.Header().Get(headerUpstreamOpsRequestID)) {
				t.Fatal("missing request id header")
			}
		})
	}
}

// 所有路由都失败时回的最后一个上游错误体，同样原样回显。
func TestE2E_LastUpstreamErrorEchoFidelity(t *testing.T) {
	body := `{"error":{"type":"server_error","message":"busy","retry_after_ms":1234567890123456789}}`
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, b []byte) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, body)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "openai_chat"}, e2eRouteSpec{Upstream: up, Protocol: "openai_chat"})
	w := h.do(http.MethodPost, "/v1/chat/completions", `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, nil)
	want := strings.TrimSuffix(body, "}") + `,"upstream_ops_request_id":"<rid>"}`
	if got := testRequestIDPattern.ReplaceAllString(w.Body.String(), "<rid>"); w.Code != http.StatusServiceUnavailable || got != want {
		t.Fatalf("status %d body %s", w.Code, got)
	}
}
