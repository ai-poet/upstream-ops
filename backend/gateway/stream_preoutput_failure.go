// 数据面：首个可见输出之前，上游在流里报错（HTTP 200 + 流内错误事件）时，可重试的按语义状态码
// 记为本次尝试失败，走重试 / 换路由；客户端什么都没收到（照 sub2api）。
//
//   - OpenAI Responses：response.failed / error 事件。是否可重试、语义状态码移植自 sub2api
//     openai_gateway_passthrough.go 的 openAIStreamFailedEventShouldFailover、openAIStreamErrorEventShouldFailover、
//     openAIStreamFailureStatus（线上见过 "Our servers are currently overloaded" 的 response.failed 被原样转给客户端）。
//   - Anthropic：event: error。sub2api（gateway_upstream_response.go / gateway_forward.go）不论类型一律换号；
//     这里按 Anthropic 文档的错误类型映射状态码，交给网关自己的重试 / 冷却 / 自动禁用规则判断
//     （sub2api 把非过载错误一律当 403 是为了触发它的换号，这里的 403 意味着停止并回显，不能照搬）。
//   - Chat Completions / Gemini：sub2api 没有对应处理，不动。
package gateway

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/tidwall/gjson"
)

// preOutputStreamFailure 上游流内错误事件在首个可见输出之前到达时，判断要不要当作本次尝试失败。
// 返回 ok=false 表示照常写给客户端（不可重试的错误，例如上下文超长、内容策略）。
// body 是给重试 / 回显用的上游协议错误体。
func preOutputStreamFailure(eventName, data string, upKind protocolKind) (status int, body []byte, ok bool) {
	payload := []byte(strings.TrimSpace(data))
	if len(payload) == 0 || !gjson.ValidBytes(payload) {
		return 0, nil, false
	}
	eventType := strings.TrimSpace(eventName)
	if eventType == "" {
		eventType = strings.TrimSpace(gjson.GetBytes(payload, "type").String())
	}
	switch protocol.NormalizeKind(upKind) {
	case protocol.KindOpenAIResponses:
		msg := responsesFailureMessage(payload)
		var retry bool
		switch eventType {
		case "response.failed":
			retry = responsesFailedEventShouldFailover(payload, msg)
		case "error":
			retry = responsesErrorEventShouldFailover(payload, msg)
		default:
			return 0, nil, false
		}
		if !retry {
			return 0, nil, false
		}
		return responsesFailureStatus(payload, msg), responsesFailureBody(payload, msg), true
	case protocol.KindAnthropic:
		if eventType != "error" {
			return 0, nil, false
		}
		return anthropicErrorStatus(gjson.GetBytes(payload, "error.type").String()), payload, true
	}
	return 0, nil, false
}

// anthropicErrorStatus Anthropic 错误类型对应的 HTTP 状态码（Anthropic API 文档 Errors 一节）。
func anthropicErrorStatus(errType string) int {
	switch strings.TrimSpace(errType) {
	case "invalid_request_error":
		return http.StatusBadRequest
	case "authentication_error":
		return http.StatusUnauthorized
	case "billing_error":
		return http.StatusPaymentRequired
	case "permission_error":
		return http.StatusForbidden
	case "not_found_error":
		return http.StatusNotFound
	case "request_too_large":
		return http.StatusRequestEntityTooLarge
	case "rate_limit_error":
		return http.StatusTooManyRequests
	case "timeout_error":
		return http.StatusGatewayTimeout
	case "overloaded_error":
		return 529
	default: // api_error 及未知类型
		return http.StatusInternalServerError
	}
}

// responsesFailureMessage 错误信息：response.error.message / error.message / message。
func responsesFailureMessage(payload []byte) string {
	for _, path := range []string{"response.error.message", "error.message", "message"} {
		if m := strings.TrimSpace(gjson.GetBytes(payload, path).String()); m != "" {
			return m
		}
	}
	return ""
}

func responsesFailureField(payload []byte, field string) string {
	v := strings.TrimSpace(gjson.GetBytes(payload, "response.error."+field).String())
	if v == "" {
		v = strings.TrimSpace(gjson.GetBytes(payload, "error."+field).String())
	}
	return strings.ToLower(v)
}

// responsesFailureBody 拼成 {"error":{...}}，与上游 HTTP 错误体同形，重试耗尽时按它回显给客户端。
func responsesFailureBody(payload []byte, msg string) []byte {
	errObj := map[string]any{"message": msg}
	if typ := responsesFailureField(payload, "type"); typ != "" {
		errObj["type"] = typ
	}
	if code := responsesFailureField(payload, "code"); code != "" {
		errObj["code"] = code
	} else if code := strings.TrimSpace(gjson.GetBytes(payload, "code").String()); code != "" {
		errObj["code"] = code
	}
	if msg == "" {
		errObj["message"] = "upstream response failed"
	}
	b, _ := json.Marshal(map[string]any{"error": errObj})
	return b
}

// 以下移植自 sub2api（openai_gateway_passthrough.go / openai_gateway_upstream_errors.go），保留其判定顺序。

var responsesErrorStatusPaths = []string{
	"response.error.status_code", "response.error.status", "error.status_code", "error.status", "status_code", "status",
}

func responsesFailedEventShouldFailover(payload []byte, msg string) bool {
	if isContextWindowError(msg, payload) {
		return false
	}
	if isUpstreamAccessStateError(payload) {
		return true
	}
	status := responsesFailureStatus(payload, msg)
	if status == http.StatusForbidden {
		return isUpstreamAccessStateError(payload) || isStreamCredentialAuthFailure(payload)
	}
	if status == http.StatusTooManyRequests {
		return true
	}
	if isTransientProcessingError(msg, payload) {
		return true
	}
	combined := strings.TrimSpace(strings.ToLower(msg) + " " + responsesFailureField(payload, "code") + " " + responsesFailureField(payload, "type"))
	if combined == "" {
		return true
	}
	for _, marker := range []string{"invalid_request", "content_policy", "policy", "safety", "high-risk cyber", "not allowed", "violat"} {
		if strings.Contains(combined, marker) {
			return false
		}
	}
	return true
}

func responsesErrorEventShouldFailover(payload []byte, msg string) bool {
	if isContextWindowError(msg, payload) {
		return false
	}
	if isUpstreamAccessStateError(payload) {
		return true
	}
	switch responsesFailureSemanticStatus(payload, msg) {
	case http.StatusForbidden:
		return isUpstreamAccessStateError(payload) || isStreamCredentialAuthFailure(payload)
	case http.StatusUnauthorized, http.StatusTooManyRequests, 529:
		return true
	}
	if isTransientProcessingError(msg, payload) {
		return true
	}
	combined := strings.ToLower(msg + " " + gjson.GetBytes(payload, "error.message").String() + " " + gjson.GetBytes(payload, "response.error.message").String())
	return strings.Contains(combined, "temporary") || strings.Contains(combined, "try again") || strings.Contains(combined, "please retry")
}

// responsesFailureStatus 流内失败事件的状态码（sub2api openAIStreamFailureStatus）：
// 鉴权 / 权限 / 限流 / 过载按语义，容量降载 503，其余 502。
func responsesFailureStatus(payload []byte, msg string) int {
	switch s := responsesFailureSemanticStatus(payload, msg); s {
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusTooManyRequests, 529:
		return s
	case http.StatusServiceUnavailable:
		if isCapacityShedEvent(payload) {
			return s
		}
	}
	return http.StatusBadGateway
}

func responsesFailureSemanticStatus(payload []byte, msg string) int {
	if isContextWindowError(msg, payload) {
		return http.StatusBadRequest
	}
	code := responsesFailureField(payload, "code")
	errType := responsesFailureField(payload, "type")
	combined := strings.TrimSpace(errType + " " + code + " " + strings.ToLower(msg))
	for _, path := range responsesErrorStatusPaths {
		if s := int(gjson.GetBytes(payload, path).Int()); s == http.StatusUnauthorized || s == http.StatusForbidden || s == http.StatusTooManyRequests || s == 529 {
			return s
		}
	}
	switch {
	case strings.Contains(combined, "rate_limit"):
		return http.StatusTooManyRequests
	case strings.Contains(errType, "invalid_request"):
		return http.StatusBadRequest
	case strings.Contains(combined, "authentication") || strings.Contains(combined, "unauthorized") || strings.Contains(combined, "invalid_api_key"):
		return http.StatusUnauthorized
	case strings.Contains(combined, "permission") || strings.Contains(combined, "forbidden") || strings.Contains(combined, "access denied"):
		return http.StatusForbidden
	case isUpstreamAccessStateError(payload):
		return http.StatusForbidden
	case isCapacityShedEvent(payload):
		return http.StatusServiceUnavailable
	}
	return http.StatusBadGateway
}

func isCapacityShedMessage(text string) bool {
	lower := strings.ToLower(strings.TrimSpace(text))
	return strings.Contains(lower, "server is overloaded") ||
		strings.Contains(lower, "servers are overloaded") ||
		strings.Contains(lower, "servers are currently overloaded")
}

func isCapacityShedEvent(payload []byte) bool {
	switch responsesFailureField(payload, "code") {
	case "server_is_overloaded", "slow_down":
		return true
	}
	for _, path := range []string{"response.error.message", "error.message", "message"} {
		if isCapacityShedMessage(gjson.GetBytes(payload, path).String()) {
			return true
		}
	}
	return false
}

// isTransientProcessingError sub2api isOpenAITransientProcessingError 在流内事件上的用法（按 400 判定）。
func isTransientProcessingError(msg string, payload []byte) bool {
	if code := responsesFailureField(payload, "code"); code == "server_is_overloaded" || code == "slow_down" {
		return true
	}
	if isCapacityShedMessage(msg) || isCapacityShedMessage(gjson.GetBytes(payload, "error.message").String()) ||
		isCapacityShedMessage(gjson.GetBytes(payload, "response.error.message").String()) {
		return true
	}
	match := func(text string) bool {
		lower := strings.ToLower(strings.TrimSpace(text))
		if lower == "" {
			return false
		}
		if strings.Contains(lower, "an error occurred while processing your request") || strings.Contains(lower, "selected model is at capacity") {
			return true
		}
		return strings.Contains(lower, "you can retry your request") && strings.Contains(lower, "help.openai.com") && strings.Contains(lower, "request id")
	}
	return match(msg) || match(gjson.GetBytes(payload, "error.message").String()) ||
		match(gjson.GetBytes(payload, "response.error.message").String()) || match(gjson.GetBytes(payload, "message").String())
}

func isContextWindowError(msg string, payload []byte) bool {
	match := func(text string) bool {
		lower := strings.ToLower(strings.TrimSpace(text))
		if lower == "" {
			return false
		}
		if strings.Contains(lower, "context_too_large") || strings.Contains(lower, "context_length_exceeded") ||
			strings.Contains(lower, "maximum context length") || strings.Contains(lower, "max context length") {
			return true
		}
		exceeded := strings.Contains(lower, "exceed") || strings.Contains(lower, "too large") || strings.Contains(lower, "too long")
		if (strings.Contains(lower, "context window") || strings.Contains(lower, "context length")) && exceeded {
			return true
		}
		return strings.Contains(lower, "token limit") && strings.Contains(lower, "context") && exceeded
	}
	if match(msg) {
		return true
	}
	for _, path := range []string{"error.message", "response.error.message", "message", "error.code", "response.error.code", "code"} {
		if match(gjson.GetBytes(payload, path).String()) {
			return true
		}
	}
	return false
}

func isUpstreamAccessStateError(payload []byte) bool {
	for _, path := range []string{"error.code", "response.error.code", "detail.code", "code"} {
		v := strings.ToLower(strings.TrimSpace(gjson.GetBytes(payload, path).String()))
		if v == "deactivated_workspace" {
			return true
		}
		for _, subject := range []string{"workspace", "account", "organization", "org"} {
			for _, state := range []string{"deactivated", "disabled", "suspended"} {
				if v == subject+"_"+state || v == state+"_"+subject {
					return true
				}
			}
		}
	}
	return false
}

func isStreamCredentialAuthFailure(payload []byte) bool {
	for _, path := range responsesErrorStatusPaths {
		if int(gjson.GetBytes(payload, path).Int()) == http.StatusUnauthorized {
			return true
		}
	}
	for _, path := range []string{"response.error.type", "error.type", "type"} {
		switch strings.ToLower(strings.TrimSpace(gjson.GetBytes(payload, path).String())) {
		case "authentication_error", "authentication_failed", "unauthorized_error":
			return true
		}
	}
	for _, path := range []string{"response.error.code", "error.code", "code"} {
		switch strings.ToLower(strings.TrimSpace(gjson.GetBytes(payload, path).String())) {
		case "invalid_api_key", "api_key_disabled", "unauthorized", "authentication_error",
			"invalid_token", "access_token_invalid", "token_revoked", "token_invalidated",
			"invalid_credentials", "credential_invalid":
			return true
		}
	}
	return false
}
