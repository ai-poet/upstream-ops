// 网关错误：回给客户端的错误只有一种表示（gatewayError），只经 writeError 一个出口输出。
//
// 两种来源：
//   - 网关自己产生的（鉴权失败、没有路由、请求不合法、协议转换失败……）：按客户端协议渲染；
//   - 上游返回的错误体：先用 convertErrorBody 转成客户端协议，再原样回显，可带上游响应头。
//
// 两种都附上网关 request id（响应头 + JSON 字段）。流式已提交后的错误帧见 writeStreamTerminalError，
// WebSocket 会话里的错误见 writeWSResponsesFailed；写进用量日志的错误详情是另一回事，见 usageErrorInfo。
package gateway

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/gin-gonic/gin"
)

// gatewayError 回给客户端的一个错误。
type gatewayError struct {
	status int
	typ    string // OpenAI 风格的 error.type；Gemini 客户端换算成 RPC status
	msg    string
	// body 上游错误体（已转成客户端协议）；非空时原样回显，typ / msg 不用
	body   []byte
	header http.Header // 回显上游错误时一并带回的上游响应头（可为 nil）
}

// newGatewayError 网关自己产生的错误。
func newGatewayError(status int, typ, msg string) *gatewayError {
	return &gatewayError{status: status, typ: typ, msg: msg}
}

// authError 鉴权失败。
func authError(msg string) *gatewayError {
	return newGatewayError(http.StatusUnauthorized, "authentication_error", msg)
}

// upstreamError 回显上游错误体（body 已转成客户端协议）。
func upstreamError(status int, body []byte, header http.Header) *gatewayError {
	return &gatewayError{status: status, body: body, header: header}
}

// writeError 向客户端写错误响应：数据面所有非流式错误都从这里出去。
func writeError(c *gin.Context, kind protocolKind, e *gatewayError) {
	reqID := ensureGatewayRequestID(c)
	if len(e.body) > 0 {
		if e.header != nil {
			copyResponseHeaders(c.Writer.Header(), e.header)
		}
		c.Writer.Header().Del("Content-Length")
		c.Header("Content-Type", "application/json")
		c.Status(e.status)
		_, _ = c.Writer.Write(injectUpstreamOpsRequestID(e.body, reqID))
		return
	}
	obj := clientErrorObject(kind, e.typ, e.msg, e.status)
	obj[jsonKeyUpstreamOpsRequestID] = reqID
	c.JSON(e.status, obj)
}

func convertErrorBody(body []byte, client, upstream protocolKind, converted bool) []byte {
	in := protocol.NormalizeKind(client)
	up := protocol.NormalizeKind(upstream)
	if in == up || len(bytes.TrimSpace(body)) == 0 {
		return body
	}
	// OpenAI Chat ↔ Responses 的错误体同为 {"error":{...}}，原样保留 code / param
	if protocol.IsOpenAIFamily(in) && protocol.IsOpenAIFamily(up) {
		return body
	}
	// 透传端点（同协议族）且客户端不是 Gemini：保持原样
	if !converted && in != protocol.KindGemini && up != protocol.KindGemini {
		return body
	}
	typ, msg, code := extractUpstreamError(body)
	if msg == "" {
		return body
	}
	return renderClientError(in, typ, msg, code)
}

// extractUpstreamError 从任意协议的错误体里取 type / message / code。
// 覆盖 OpenAI {"error":{...}}、Anthropic {"type":"error","error":{...}}、Gemini {"error":{"code","message","status"}}、
// 以及 [{"error":...}]（Gemini 流式数组）与 {"message":...} 这类裸错误。
func extractUpstreamError(body []byte) (typ, msg string, code int) {
	trim := bytes.TrimSpace(body)
	if len(trim) > 0 && trim[0] == '[' {
		var arr []json.RawMessage
		if json.Unmarshal(trim, &arr) == nil && len(arr) > 0 {
			trim = arr[0]
		}
	}
	var m map[string]any
	if json.Unmarshal(trim, &m) != nil {
		return "", "", 0
	}
	if errObj, ok := m["error"].(map[string]any); ok {
		typ = stringValue(errObj["type"])
		if typ == "" {
			typ = stringValue(errObj["status"])
		}
		msg = stringValue(errObj["message"])
		if f, ok := errObj["code"].(float64); ok {
			code = int(f)
		} else if typ == "" {
			typ = stringValue(errObj["code"])
		}
		return typ, msg, code
	}
	if s, ok := m["error"].(string); ok {
		return stringValue(m["type"]), s, 0
	}
	return stringValue(m["type"]), stringValue(m["message"]), 0
}

// renderClientError 按客户端协议重新包装上游错误。
func renderClientError(kind protocolKind, typ, msg string, code int) []byte {
	if typ == "" {
		typ = "api_error"
	}
	if code == 0 {
		code = http.StatusInternalServerError
	}
	out, _ := json.Marshal(clientErrorObject(kind, typ, msg, code))
	return out
}

// clientErrorObject 三种客户端协议的错误形状：OpenAI {"error":{message,type}}、
// Anthropic {"type":"error","error":{type,message}}、Gemini {"error":{code,message,status}}。
func clientErrorObject(kind protocolKind, typ, msg string, code int) map[string]any {
	switch protocol.NormalizeKind(kind) {
	case protocol.KindAnthropic:
		return map[string]any{
			"type":  "error",
			"error": map[string]any{"type": typ, "message": msg},
		}
	case protocol.KindGemini:
		return map[string]any{
			"error": map[string]any{"code": code, "message": msg, "status": geminiStatusFor(typ, code)},
		}
	default:
		return map[string]any{
			"error": map[string]any{"message": msg, "type": typ},
		}
	}
}

// geminiStatusFor 把错误类型 / HTTP 码映射到 Google RPC status。
func geminiStatusFor(typ string, code int) string {
	t := strings.ToUpper(strings.TrimSpace(typ))
	switch t {
	case "INVALID_ARGUMENT", "FAILED_PRECONDITION", "OUT_OF_RANGE", "UNAUTHENTICATED", "PERMISSION_DENIED",
		"NOT_FOUND", "RESOURCE_EXHAUSTED", "INTERNAL", "UNAVAILABLE", "DEADLINE_EXCEEDED", "CANCELLED", "ABORTED":
		return t
	}
	switch strings.ToLower(typ) {
	case "invalid_request_error":
		return "INVALID_ARGUMENT"
	case "authentication_error":
		return "UNAUTHENTICATED"
	case "permission_error":
		return "PERMISSION_DENIED"
	case "not_found_error":
		return "NOT_FOUND"
	case "rate_limit_error":
		return "RESOURCE_EXHAUSTED"
	case "overloaded_error":
		return "UNAVAILABLE"
	}
	switch {
	case code == 400:
		return "INVALID_ARGUMENT"
	case code == 401:
		return "UNAUTHENTICATED"
	case code == 403:
		return "PERMISSION_DENIED"
	case code == 404:
		return "NOT_FOUND"
	case code == 429:
		return "RESOURCE_EXHAUSTED"
	case code == 503:
		return "UNAVAILABLE"
	case code == 504:
		return "DEADLINE_EXCEEDED"
	default:
		return "INTERNAL"
	}
}
