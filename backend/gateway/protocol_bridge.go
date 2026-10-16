// gateway 与 protocol 包之间的请求/响应/错误体桥接。
package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"

	"github.com/bejix/upstream-ops/backend/gateway/pricing"
	"github.com/bejix/upstream-ops/backend/gateway/protocol"
)

type protocolKind = protocol.Kind

const (
	protocolOpenAI    = protocol.KindOpenAI
	protocolAnthropic = protocol.KindAnthropic
)

func prepareUpstreamRequest(
	body []byte,
	inbound, upstream protocolKind,
	model string,
	stream bool,
	inboundPath string,
) (fwd []byte, upstreamPath string, converted bool, err error) {
	fwd, upstreamPath, plan, err := planUpstreamRequest(context.Background(), body, inbound, upstream, "", model, stream, inboundPath)
	if err != nil {
		return nil, "", false, err
	}
	return fwd, upstreamPath, plan.Converted, nil
}

// prepareLegacyUpstreamRequest 旧转换器路径（engine=legacy，或 relaykit 不支持的 tool_search 往返）。
// Responses 降级到 Chat / Anthropic 时保留可逆的工具映射。
func prepareLegacyUpstreamRequest(
	fwd []byte,
	in, up protocolKind,
	model string,
	stream bool,
) ([]byte, protocol.ResponsesToolBridge, error) {
	if in == protocol.KindOpenAIResponses && (up == protocol.KindOpenAIChat || up == protocol.KindAnthropic) {
		chat, bridge, err := protocol.ResponsesToOpenAIChatRequestWithToolBridge(fwd, model, stream)
		if err != nil {
			return nil, protocol.ResponsesToolBridge{}, err
		}
		if up == protocol.KindAnthropic {
			out, err := protocol.OpenAIToAnthropicRequest(chat, model, stream)
			return out, bridge, err
		}
		return chat, bridge, nil
	}
	out, _, err := protocol.ConvertRequest(in, up, fwd, model, stream)
	return out, protocol.ResponsesToolBridge{}, err
}

// convertUpstreamResponse 将上游响应转回客户端协议。
func convertUpstreamResponse(
	respBody []byte,
	inbound, upstream protocolKind,
	model string,
	stream bool,
	converted bool,
) []byte {
	if !converted || len(respBody) == 0 {
		return respBody
	}
	in := protocol.NormalizeKind(inbound)
	up := protocol.NormalizeKind(upstream)

	// 流式：Anthropic↔OpenAI SSE 互转；Responses 相关尽力转换
	if stream {
		return protocol.ConvertStreamResponse(in, up, respBody, model, wrapAsOpenAISSE, tryExtractJSONObject)
	}
	return protocol.ConvertResponse(in, up, respBody, model)
}

func convertUpstreamResponseWithToolBridge(
	respBody []byte,
	inbound, upstream protocolKind,
	model string,
	stream bool,
	converted bool,
	bridge protocol.ResponsesToolBridge,
) []byte {
	if !stream && converted && protocol.NormalizeKind(inbound) == protocol.KindOpenAIResponses {
		var chat []byte
		var err error
		switch protocol.NormalizeKind(upstream) {
		case protocol.KindOpenAIChat, protocol.KindOpenAI:
			chat = respBody
		case protocol.KindAnthropic:
			chat, err = protocol.AnthropicToOpenAIResponse(respBody, model)
		}
		if err == nil && len(chat) > 0 {
			if out, convErr := protocol.OpenAIChatToResponsesResponseWithToolBridge(chat, model, bridge); convErr == nil {
				return out
			}
		}
	}
	return convertUpstreamResponse(respBody, inbound, upstream, model, stream, converted)
}

// tryExtractJSONObject 从可能的 SSE 缓冲中取最后一段 JSON 对象（Responses 流结束时的完整事件）。
func tryExtractJSONObject(body []byte) []byte {
	trim := bytes.TrimSpace(body)
	if len(trim) == 0 {
		return body
	}
	if trim[0] == '{' {
		return trim
	}
	// SSE: 找最后一个 data: { ... }
	var last []byte
	for _, line := range bytes.Split(body, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if bytes.HasPrefix(line, []byte("data:")) {
			payload := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
			if len(payload) > 0 && payload[0] == '{' {
				last = payload
			}
		}
	}
	if len(last) > 0 {
		return last
	}
	return body
}

func wrapAsOpenAISSE(chatJSON []byte) []byte {
	// 将完整 chat.completion 拆成流式 chunk + DONE。
	// 工具调用：流式 delta.tool_calls 需带 index，否则多数客户端忽略。
	var m map[string]any
	if err := json.Unmarshal(chatJSON, &m); err != nil {
		return chatJSON
	}
	id := m["id"]
	model := m["model"]
	var frames [][]byte

	emit := func(delta map[string]any, finish any, usage any) {
		chunk := map[string]any{
			"id":     id,
			"object": "chat.completion.chunk",
			"model":  model,
			"choices": []any{
				map[string]any{
					"index":         0,
					"delta":         delta,
					"finish_reason": finish,
				},
			},
		}
		if usage != nil {
			chunk["usage"] = usage
		}
		raw, _ := json.Marshal(chunk)
		frames = append(frames, raw)
	}

	var finish any = "stop"
	var usage any
	if u, ok := m["usage"]; ok {
		usage = u
	}

	if choices, ok := m["choices"].([]any); ok && len(choices) > 0 {
		if ch, ok := choices[0].(map[string]any); ok {
			if fr, ok := ch["finish_reason"]; ok && fr != nil {
				finish = fr
			}
			if msg, ok := ch["message"].(map[string]any); ok {
				// role 首包（纯工具调用时 content 为 null，仍发 role）
				roleChunk := map[string]any{"role": "assistant"}
				if s, ok := msg["content"].(string); ok && s != "" {
					roleChunk["content"] = s
				}
				emit(roleChunk, nil, nil)

				if tcs, ok := msg["tool_calls"].([]any); ok && len(tcs) > 0 {
					// 每个 tool_call 带 index，符合 chat.completion.chunk 约定
					indexed := make([]any, 0, len(tcs))
					for i, raw := range tcs {
						tc, _ := raw.(map[string]any)
						if tc == nil {
							continue
						}
						item := map[string]any{
							"index": i,
							"id":    tc["id"],
							"type":  "function",
						}
						if typ, ok := tc["type"].(string); ok && typ != "" {
							item["type"] = typ
						}
						if fn, ok := tc["function"].(map[string]any); ok {
							item["function"] = fn
						}
						indexed = append(indexed, item)
					}
					if len(indexed) > 0 {
						emit(map[string]any{"tool_calls": indexed}, nil, nil)
						if finish == "stop" || finish == nil || finish == "" {
							finish = "tool_calls"
						}
					}
				}
			}
		}
	}

	// 结束帧（带 finish_reason + usage）
	emit(map[string]any{}, finish, usage)

	var b strings.Builder
	for _, f := range frames {
		b.WriteString("data: ")
		b.Write(f)
		b.WriteString("\n\n")
	}
	b.WriteString("data: [DONE]\n\n")
	return []byte(b.String())
}

func parseUsageByKind(body []byte, stream bool, kind protocolKind) pricing.UsageTokens {
	return ParseUsage(body, stream, kind)
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
	var out []byte
	switch protocol.NormalizeKind(kind) {
	case protocol.KindAnthropic:
		out, _ = json.Marshal(map[string]any{
			"type":  "error",
			"error": map[string]any{"type": typ, "message": msg},
		})
	case protocol.KindGemini:
		if code == 0 {
			code = 500
		}
		out, _ = json.Marshal(map[string]any{
			"error": map[string]any{"code": code, "message": msg, "status": geminiStatusFor(typ, code)},
		})
	default:
		out, _ = json.Marshal(map[string]any{
			"error": map[string]any{"message": msg, "type": typ},
		})
	}
	return out
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
