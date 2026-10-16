package gateway

import (
	"net/http"
	"net/http/httptest"
	"regexp"
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
	want := `{"error":{"code":"rl","message":"slow down","type":"rate_limit_error"},"upstream_ops_request_id":"<rid>"}`
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
