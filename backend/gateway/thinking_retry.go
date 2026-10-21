// 数据面：Claude thinking 签名错误的补救。
//
// 对话历史里的 thinking 块带着上游账号签的 signature，换了上游（另一条路由、另一家中转）就验不过，
// 回 400「Invalid `signature` in `thinking` block」。照 sub2api gateway_forward.go：遇到这类 400，
// 把 thinking 块降级成普通文本、去掉顶层 thinking，在同一路由上再试一次（FilterThinkingBlocksForRetry）。
// 会话粘性（session_affinity.go）让同一对话尽量留在同一路由，这里是它之外的兜底。
package gateway

import (
	"encoding/json"
	"strings"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// isThinkingSignatureError 上游 400 是不是 thinking 块签名 / 结构问题（移植 sub2api isThinkingBlockSignatureError）。
func isThinkingSignatureError(body []byte) bool {
	_, msg, _ := extractUpstreamError(body)
	msg = strings.ToLower(strings.TrimSpace(msg))
	if msg == "" {
		return false
	}
	if strings.Contains(msg, "signature") {
		return true
	}
	mentionsThinking := strings.Contains(msg, "thinking")
	switch {
	case strings.Contains(msg, "expected") && mentionsThinking:
		return true
	case strings.Contains(msg, "cannot be modified") && mentionsThinking:
		return true
	case strings.Contains(msg, "thinking block must contain"):
		return true
	case strings.Contains(msg, "non-empty content") || strings.Contains(msg, "empty content") ||
		strings.Contains(msg, "content blocks must be non-empty"):
		return true
	}
	return false
}

// shouldRetryWithoutThinking 这次失败能不能靠去掉 thinking 块在同一路由补救：
// Anthropic 上游、Claude 模型、400 签名类错误、这条路由还没去过 thinking。
func shouldRetryWithoutThinking(at *relayAttempt, out attemptOutcome) bool {
	if at == nil || at.routeChoice == nil || at.stripThinking || out.Err != nil || out.Status != 400 {
		return false
	}
	if protocol.NormalizeKind(at.upstreamKind) != protocol.KindAnthropic || !protocol.LooksLikeClaudeModel(at.upstreamModel) {
		return false
	}
	if !isThinkingSignatureError(out.Body) {
		return false
	}
	filtered := filterThinkingBlocksForRetry(at.call.body, at.upstreamModel)
	return string(filtered) != string(at.call.body)
}

// filterThinkingBlocksForRetry 移植 sub2api FilterThinkingBlocksForRetry（仅 Anthropic 官方协议的 Claude 模型）：
//   - 去掉顶层 thinking（Sonnet 5.5 的 between_tools 保留），并去掉依赖 thinking 的 context_management 策略；
//   - thinking 块转成 text 块（保留内容），redacted_thinking 块删除；
//   - 删除空 text 块（含 tool_result 里嵌套的）；消息被删空时补占位文本。
//
// 解析失败时原样返回。
func filterThinkingBlocksForRetry(body []byte, model string) []byte {
	if !protocol.LooksLikeClaudeModel(model) || !gjson.ValidBytes(body) {
		return body
	}
	msgsRes := gjson.GetBytes(body, "messages")
	if !msgsRes.IsArray() {
		return body
	}
	var messages []any
	if err := json.Unmarshal([]byte(msgsRes.Raw), &messages); err != nil {
		return body
	}
	thinkingMode := gjson.GetBytes(body, "thinking.type").String()
	deleteTopThinking := gjson.GetBytes(body, "thinking").Exists() &&
		!(isSonnet55Model(model) && thinkingMode == "between_tools")

	modified := false
	for i := range messages {
		msg, ok := messages[i].(map[string]any)
		if !ok {
			continue
		}
		content, ok := msg["content"].([]any)
		if !ok {
			continue
		}
		role, _ := msg["role"].(string)
		newContent, changed := filterThinkingContent(content)
		if !changed && len(content) > 0 {
			continue
		}
		modified = true
		if len(newContent) == 0 {
			placeholder := "(content removed)"
			if role == "assistant" {
				placeholder = "(assistant content removed)"
			}
			newContent = []any{map[string]any{"type": "text", "text": placeholder}}
		}
		msg["content"] = newContent
	}
	if !modified && !deleteTopThinking {
		return body
	}
	out := body
	if deleteTopThinking {
		b, err := sjson.DeleteBytes(out, "thinking")
		if err != nil {
			return body
		}
		out = removeThinkingDependentStrategies(b)
	}
	if modified {
		raw, err := json.Marshal(messages)
		if err != nil {
			return body
		}
		b, err := sjson.SetRawBytes(out, "messages", raw)
		if err != nil {
			return body
		}
		out = b
	}
	return out
}

// filterThinkingContent 一条消息的 content 数组：thinking → text，删 redacted_thinking 与空 text。
func filterThinkingContent(content []any) ([]any, bool) {
	out := make([]any, 0, len(content))
	changed := false
	for _, block := range content {
		m, ok := block.(map[string]any)
		if !ok {
			out = append(out, block)
			continue
		}
		typ, _ := m["type"].(string)
		switch typ {
		case "text":
			if txt, _ := m["text"].(string); txt == "" {
				changed = true
				continue
			}
		case "thinking":
			changed = true
			if txt, _ := m["thinking"].(string); txt != "" {
				out = append(out, map[string]any{"type": "text", "text": txt})
			}
			continue
		case "redacted_thinking":
			changed = true
			continue
		case "":
			if raw, has := m["thinking"]; has {
				changed = true
				switch v := raw.(type) {
				case string:
					if v != "" {
						out = append(out, map[string]any{"type": "text", "text": v})
					}
				default:
					if b, err := json.Marshal(v); err == nil && len(b) > 0 {
						out = append(out, map[string]any{"type": "text", "text": string(b)})
					}
				}
				continue
			}
		case "tool_result":
			if nested, ok := m["content"].([]any); ok {
				cleaned := make([]any, 0, len(nested))
				dropped := false
				for _, nb := range nested {
					if nm, ok := nb.(map[string]any); ok {
						if t, _ := nm["type"].(string); t == "text" {
							if txt, _ := nm["text"].(string); txt == "" {
								dropped = true
								continue
							}
						}
					}
					cleaned = append(cleaned, nb)
				}
				if dropped {
					changed = true
					cp := make(map[string]any, len(m))
					for k, v := range m {
						cp[k] = v
					}
					cp["content"] = cleaned
					out = append(out, cp)
					continue
				}
			}
		}
		out = append(out, block)
	}
	return out, changed
}

// removeThinkingDependentStrategies 去掉顶层 thinking 后，context_management.edits 里要求开 thinking 的
// 策略（clear_thinking_20251015）也要去掉，否则上游回 400（同 sub2api removeThinkingDependentContextStrategies）。
func removeThinkingDependentStrategies(body []byte) []byte {
	edits := gjson.GetBytes(body, "context_management.edits")
	if !edits.IsArray() {
		return body
	}
	var kept []json.RawMessage
	removed := false
	edits.ForEach(func(_, v gjson.Result) bool {
		if v.Get("type").String() == "clear_thinking_20251015" {
			removed = true
			return true
		}
		kept = append(kept, json.RawMessage(v.Raw))
		return true
	})
	if !removed {
		return body
	}
	if len(kept) == 0 {
		if b, err := sjson.DeleteBytes(body, "context_management.edits"); err == nil {
			return b
		}
		return body
	}
	raw, err := json.Marshal(kept)
	if err != nil {
		return body
	}
	if b, err := sjson.SetRawBytes(body, "context_management.edits", raw); err == nil {
		return b
	}
	return body
}

// isSonnet55Model claude-sonnet-5-5（含带日期 / 点号的写法）。
func isSonnet55Model(model string) bool {
	m := strings.ToLower(model)
	return strings.Contains(m, "sonnet-5-5") || strings.Contains(m, "sonnet-5.5")
}
