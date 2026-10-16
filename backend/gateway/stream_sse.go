// SSE 帧解析与流式路径无状态辅助。
package gateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/bejix/upstream-ops/backend/gateway/pricing"
	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/gin-gonic/gin"
)

func parseSSEEventLines(lines []string) (eventName, data string) {
	var dataParts []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "event:") {
			eventName = strings.TrimSpace(strings.TrimPrefix(trimmed, "event:"))
			continue
		}
		if strings.HasPrefix(trimmed, "data:") {
			dataParts = append(dataParts, strings.TrimSpace(strings.TrimPrefix(trimmed, "data:")))
		}
	}
	data = strings.Join(dataParts, "\n")
	return eventName, data
}

// sseEventHasPayload 判断 SSE 事件是否包含可提交的有效载荷（非纯注释）。
func sseEventHasPayload(lines []string) bool {
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, ":") {
			continue
		}
		if strings.HasPrefix(trimmed, "event:") || strings.HasPrefix(trimmed, "data:") ||
			strings.HasPrefix(trimmed, "id:") || strings.HasPrefix(trimmed, "retry:") {
			return true
		}
		// 非注释、非空行：视为有效
		return true
	}
	return false
}

// sseFrameIsTerminal 判断已写出（或即将写出）的 SSE 帧是否为客户端侧流结束标记。
// 用于区分「中途断开」与「收完终端帧后正常关连接」。
// 覆盖：Chat [DONE]、Anthropic message_stop、Responses response.completed 等。
func sseFrameIsTerminal(frame []byte) bool {
	if len(frame) == 0 {
		return false
	}
	s := string(frame)
	// OpenAI Chat 系：data: [DONE]
	if strings.Contains(s, "data: [DONE]") || strings.Contains(s, "data:[DONE]") {
		return true
	}
	// Anthropic：message_stop / error 事件
	if strings.Contains(s, "event: message_stop") || strings.Contains(s, "event:message_stop") {
		return true
	}
	if strings.Contains(s, `"type":"message_stop"`) || strings.Contains(s, `"type": "message_stop"`) {
		return true
	}
	if strings.Contains(s, "event: error") || strings.Contains(s, "event:error") {
		return true
	}
	// OpenAI Responses（/v1/responses）：无 [DONE]，以 response.completed / failed 等收尾。
	// 若漏识别，客户端正常收完后关连接会被误记为 error_type=client。
	if strings.Contains(s, "event: response.completed") || strings.Contains(s, "event:response.completed") ||
		strings.Contains(s, "event: response.done") || strings.Contains(s, "event:response.done") ||
		strings.Contains(s, "event: response.failed") || strings.Contains(s, "event:response.failed") ||
		strings.Contains(s, "event: response.incomplete") || strings.Contains(s, "event:response.incomplete") ||
		strings.Contains(s, "event: response.cancelled") || strings.Contains(s, "event:response.cancelled") ||
		strings.Contains(s, "event: response.canceled") || strings.Contains(s, "event:response.canceled") {
		return true
	}
	// Gemini：带 finishReason 的块即终态（SSE 与数组模式都适用）
	if strings.Contains(s, `"finishReason":"`) || strings.Contains(s, `"finishReason": "`) {
		return true
	}
	if strings.Contains(s, `"type":"response.completed"`) || strings.Contains(s, `"type": "response.completed"`) ||
		strings.Contains(s, `"type":"response.done"`) || strings.Contains(s, `"type": "response.done"`) ||
		strings.Contains(s, `"type":"response.failed"`) || strings.Contains(s, `"type": "response.failed"`) ||
		strings.Contains(s, `"type":"response.incomplete"`) || strings.Contains(s, `"type": "response.incomplete"`) ||
		strings.Contains(s, `"type":"response.cancelled"`) || strings.Contains(s, `"type": "response.cancelled"`) ||
		strings.Contains(s, `"type":"response.canceled"`) || strings.Contains(s, `"type": "response.canceled"`) {
		return true
	}
	return false
}

// upstreamSSETerminal 判断上游协议层是否已经给出终态。转换器 Close 会补齐客户端帧，
// 但不能把上游意外 EOF 伪装成成功完成。
func upstreamSSETerminal(eventName, data string, kind protocolKind) (terminal, success bool, message string) {
	data = strings.TrimSpace(data)
	if data == "[DONE]" {
		return true, true, ""
	}
	name := strings.TrimSpace(eventName)
	var payload map[string]any
	if json.Unmarshal([]byte(data), &payload) == nil {
		if typ, _ := payload["type"].(string); typ != "" {
			name = typ
		}
	}
	name = strings.ToLower(strings.TrimSpace(name))
	switch protocol.NormalizeKind(kind) {
	case protocol.KindAnthropic:
		switch name {
		case "message_stop":
			return true, true, ""
		case "error":
			return true, false, sseErrorMessage(payload, "upstream stream failed")
		}
	case protocol.KindGemini:
		// 正常结束靠 EOF（见 geminiUpstreamFinished）；这里只识别流中错误
		if _, ok := payload["error"]; ok {
			return true, false, sseErrorMessage(payload, "upstream stream failed")
		}
		return false, false, ""
	default:
		switch name {
		case "response.completed", "response.done", "response.incomplete":
			if response, _ := payload["response"].(map[string]any); response != nil {
				switch strings.ToLower(strings.TrimSpace(stringValue(response["status"]))) {
				case "failed", "cancelled", "canceled":
					return true, false, sseErrorMessage(response, "upstream response failed")
				}
			}
			return true, true, ""
		case "response.failed", "response.cancelled", "response.canceled", "error":
			return true, false, sseErrorMessage(payload, "upstream response failed")
		}
	}
	return false, false, ""
}

// geminiErrorFrame Gemini 流中的错误块（SSE 形式；数组模式由 geminiArrayEncoder 改写）。
func geminiErrorFrame(errType, message string) []byte {
	code := 500
	if errType == "stream_timeout" {
		code = 504
	}
	body, _ := json.Marshal(map[string]any{
		"error": map[string]any{"code": code, "message": message, "status": geminiStatusFor(errType, code)},
	})
	return []byte(fmt.Sprintf("data: %s\n\n", body))
}

func sseErrorMessage(payload map[string]any, fallback string) string {
	if response, _ := payload["response"].(map[string]any); response != nil {
		payload = response
	}
	if errObj, _ := payload["error"].(map[string]any); errObj != nil {
		if message := strings.TrimSpace(stringValue(errObj["message"])); message != "" {
			return message
		}
	}
	return fallback
}

func stringValue(v any) string {
	s, _ := v.(string)
	return s
}

// finalizeStreamClientDisconnect 在流结束时整理 client 断开标记：
// - 已向客户端交付终端帧：关连接属正常收尾，清除 ClientDisconnected / errClientDisconnected
// - 中途断开：补上 StreamErr=errClientDisconnected 供上层记 error_type=client
func finalizeStreamClientDisconnect(r *streamAttemptResult) {
	if r == nil || !r.ClientDisconnected {
		return
	}
	if r.DownstreamComplete {
		r.ClientDisconnected = false
		if r.StreamErr != nil && errors.Is(r.StreamErr, errClientDisconnected) {
			r.StreamErr = nil
		}
		return
	}
	if r.StreamErr == nil {
		r.StreamErr = errClientDisconnected
	}
}

func commitSSEHeaders(c *gin.Context, upstream http.Header) error {
	if c == nil {
		return errors.New("nil gin context")
	}
	if _, ok := c.Writer.(http.Flusher); !ok {
		return errors.New("streaming not supported")
	}
	if upstream != nil {
		copyResponseHeaders(c.Writer.Header(), upstream)
	}
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")
	c.Writer.Header().Del("Content-Length")
	reqID := ensureGatewayRequestID(c)
	setGatewayRequestIDHeaders(c, reqID)
	c.Status(http.StatusOK)
	return nil
}

func flushWriter(c *gin.Context) {
	if c == nil {
		return
	}
	if f, ok := c.Writer.(http.Flusher); ok {
		f.Flush()
	}
}

func streamKeepaliveFrame(clientKind protocolKind) []byte {
	if protocol.NormalizeKind(clientKind) == protocol.KindAnthropic {
		return []byte("event: ping\ndata: {\"type\":\"ping\"}\n\n")
	}
	return []byte(":\n\n")
}

// writeStreamTerminalError 在已 commit 的 SSE 流上写入终端错误（只应调用一次）。
func writeStreamTerminalError(c *gin.Context, kind protocolKind, errType, message string) error {
	if c == nil {
		return errors.New("nil context")
	}
	if errType == "" {
		errType = "api_error"
	}
	if message == "" {
		message = errType
	}
	var payload []byte
	if protocol.NormalizeKind(kind) == protocol.KindGemini {
		payload = geminiErrorFrame(errType, message)
	} else if protocol.NormalizeKind(kind) == protocol.KindAnthropic {
		body, _ := json.Marshal(map[string]any{
			"type": "error",
			"error": map[string]string{
				"type":    errType,
				"message": message,
			},
		})
		payload = []byte(fmt.Sprintf("event: error\ndata: %s\n\n", body))
	} else if protocol.NormalizeKind(kind) == protocol.KindOpenAIResponses {
		body, _ := json.Marshal(map[string]any{
			"type": "response.failed",
			"response": map[string]any{
				"id":     "resp_gateway_error",
				"object": "response",
				"status": "failed",
				"output": []any{},
				"error": map[string]any{
					"code":    errType,
					"message": message,
				},
			},
		})
		payload = []byte(fmt.Sprintf("event: response.failed\ndata: %s\n\n", body))
	} else {
		body, _ := json.Marshal(map[string]any{
			"error": map[string]any{
				"message": message,
				"type":    errType,
			},
		})
		payload = []byte(fmt.Sprintf("data: %s\n\ndata: [DONE]\n\n", body))
	}
	if _, err := c.Writer.Write(payload); err != nil {
		return err
	}
	flushWriter(c)
	return nil
}

func mergeStreamUsage(dst *pricing.UsageTokens, data string, kind protocolKind) {
	if dst == nil || data == "" {
		return
	}
	var u pricing.UsageTokens
	if protocol.NormalizeKind(kind) == protocol.KindGemini {
		u = ParseGeminiUsage([]byte(data))
	} else if protocol.NormalizeKind(kind) == protocol.KindAnthropic {
		u = ParseAnthropicUsage([]byte(data))
		// Anthropic SSE 中 usage 常嵌在 message_start / message_delta
		if !usageNonEmpty(u) {
			var raw map[string]any
			if json.Unmarshal([]byte(data), &raw) == nil {
				if msg, ok := raw["message"].(map[string]any); ok {
					if usageObj, ok := msg["usage"].(map[string]any); ok {
						b, _ := json.Marshal(map[string]any{"usage": usageObj})
						u = ParseAnthropicUsage(b)
					}
				}
				if usageObj, ok := raw["usage"].(map[string]any); ok {
					b, _ := json.Marshal(map[string]any{"usage": usageObj})
					u = ParseAnthropicUsage(b)
				}
			}
		}
	} else {
		u = ParseOpenAIUsage([]byte(data))
	}
	if usageNonEmpty(u) {
		*dst = mergeUsagePreferNewer(*dst, u)
	}
}

func finalizeStreamTokens(live pricing.UsageTokens, raw []byte, kind protocolKind) pricing.UsageTokens {
	if usageNonEmpty(live) {
		// 再用整包兜底补 cache 等字段
		if len(raw) > 0 {
			fallback := parseUsageByKind(raw, true, kind)
			if live.CacheReadTokens == 0 && fallback.CacheReadTokens > 0 {
				live.CacheReadTokens = fallback.CacheReadTokens
			}
			if live.CacheCreationTokens == 0 && fallback.CacheCreationTokens > 0 {
				live.CacheCreationTokens = fallback.CacheCreationTokens
			}
			if live.InputTokens == 0 && fallback.InputTokens > 0 {
				live.InputTokens = fallback.InputTokens
			}
			if live.OutputTokens == 0 && fallback.OutputTokens > 0 {
				live.OutputTokens = fallback.OutputTokens
			}
		}
		return live
	}
	if len(raw) == 0 {
		return live
	}
	return parseUsageByKind(raw, true, kind)
}
