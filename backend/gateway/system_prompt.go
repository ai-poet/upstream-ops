package gateway

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
)

const (
	systemPromptKeyScopeAll      = "all"
	systemPromptKeyScopeSelected = "selected"
)

type SystemPromptRule struct {
	Enabled  *bool  `json:"enabled"`
	Text     string `json:"text"`
	Override bool   `json:"override"`
	KeyScope string `json:"key_scope"`
	KeyIDs   []uint `json:"key_ids,omitempty"`
}

func (rt *Runtime) applySystemPrompt(group *storage.GatewayGroup, key *storage.GatewayKey, path string, kind protocolKind, body []byte) ([]byte, error) {
	if group == nil || key == nil {
		return body, nil
	}
	normalizedKind := protocol.NormalizeKind(kind)
	var inject func([]byte, string, bool) ([]byte, error)
	switch {
	case normalizedKind == protocol.KindOpenAIChat && path == "/v1/chat/completions":
		inject = injectChatSystemPrompt
	case normalizedKind == protocol.KindOpenAIResponses && (path == "/v1/responses" || path == "/v1/responses/compact"):
		inject = injectResponsesSystemPrompt
	case normalizedKind == protocol.KindAnthropic && (path == "/v1/messages" || path == "/v1/messages/count_tokens"):
		inject = injectAnthropicSystemPrompt
	case normalizedKind == protocol.KindGemini && strings.HasPrefix(path, "/v1beta/models/") &&
		(strings.HasSuffix(path, ":generateContent") || strings.HasSuffix(path, ":streamGenerateContent")):
		inject = injectGeminiSystemPrompt
	default:
		return body, nil
	}

	rules := parseSystemPromptRules(group.SystemPromptRulesJSON)
	matching := make([]SystemPromptRule, 0, len(rules))
	for _, rule := range rules {
		if (rule.Enabled == nil || *rule.Enabled) && strings.TrimSpace(rule.Text) != "" && systemPromptRuleAppliesToKey(rule, key.ID) {
			matching = append(matching, rule)
		}
	}
	if len(matching) == 0 {
		return body, nil
	}

	hasClientPrompt, err := requestHasSystemPrompt(body, normalizedKind)
	if err != nil {
		return nil, err
	}
	prompts := make([]string, 0, len(matching))
	for _, rule := range matching {
		if !hasClientPrompt || rule.Override {
			prompts = append(prompts, strings.TrimSpace(rule.Text))
		}
	}
	if len(prompts) == 0 {
		return body, nil
	}
	return inject(body, strings.Join(prompts, "\n"), true)
}

func normalizeSystemPromptRulesJSON(raw string) (string, []uint, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil, nil
	}
	var rules []SystemPromptRule
	if err := json.Unmarshal([]byte(raw), &rules); err != nil {
		return "", nil, errors.New("invalid system prompt rules")
	}
	allKeyIDs := make([]uint, 0)
	allSeen := make(map[uint]struct{})
	for i := range rules {
		rule := &rules[i]
		if rule.Enabled == nil {
			enabled := true
			rule.Enabled = &enabled
		}
		rule.Text = strings.TrimSpace(rule.Text)
		rule.KeyScope = strings.ToLower(strings.TrimSpace(rule.KeyScope))
		if rule.KeyScope == "" {
			rule.KeyScope = systemPromptKeyScopeAll
		}
		if rule.KeyScope != systemPromptKeyScopeAll && rule.KeyScope != systemPromptKeyScopeSelected {
			return "", nil, errors.New("invalid system prompt key scope")
		}
		if rule.KeyScope == systemPromptKeyScopeAll {
			rule.KeyIDs = nil
			continue
		}
		seen := make(map[uint]struct{}, len(rule.KeyIDs))
		ids := make([]uint, 0, len(rule.KeyIDs))
		for _, id := range rule.KeyIDs {
			if id == 0 {
				continue
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			ids = append(ids, id)
			if _, ok := allSeen[id]; !ok {
				allSeen[id] = struct{}{}
				allKeyIDs = append(allKeyIDs, id)
			}
		}
		rule.KeyIDs = ids
	}
	normalized, _ := json.Marshal(rules)
	return string(normalized), allKeyIDs, nil
}

func parseSystemPromptRules(raw string) []SystemPromptRule {
	var rules []SystemPromptRule
	if json.Unmarshal([]byte(raw), &rules) != nil {
		return nil
	}
	return rules
}

func removeSystemPromptKeyID(raw string, keyID uint) (string, bool) {
	rules := parseSystemPromptRules(raw)
	changed := false
	for i := range rules {
		if strings.ToLower(strings.TrimSpace(rules[i].KeyScope)) != systemPromptKeyScopeSelected {
			continue
		}
		ids := rules[i].KeyIDs[:0]
		for _, id := range rules[i].KeyIDs {
			if id == keyID {
				changed = true
				continue
			}
			ids = append(ids, id)
		}
		rules[i].KeyIDs = ids
	}
	if !changed {
		return raw, false
	}
	normalized, _ := json.Marshal(rules)
	return string(normalized), true
}

func systemPromptRuleAppliesToKey(rule SystemPromptRule, keyID uint) bool {
	switch strings.ToLower(strings.TrimSpace(rule.KeyScope)) {
	case "", systemPromptKeyScopeAll:
		return true
	case systemPromptKeyScopeSelected:
		for _, id := range rule.KeyIDs {
			if id == keyID {
				return true
			}
		}
	}
	return false
}

func requestHasSystemPrompt(body []byte, kind protocol.Kind) (bool, error) {
	request, err := decodePromptRequest(body)
	if err != nil {
		return false, err
	}
	switch protocol.NormalizeKind(kind) {
	case protocol.KindOpenAIChat:
		var messages []json.RawMessage
		if raw, ok := request["messages"]; ok && string(raw) != "null" {
			if err := json.Unmarshal(raw, &messages); err != nil {
				return false, errors.New("invalid messages")
			}
		}
		for _, raw := range messages {
			var message struct {
				Role string `json:"role"`
			}
			if json.Unmarshal(raw, &message) == nil && (message.Role == "system" || message.Role == "developer") {
				return true, nil
			}
		}
	case protocol.KindOpenAIResponses:
		raw, ok := request["instructions"]
		if ok && string(raw) != "null" {
			var text string
			if json.Unmarshal(raw, &text) == nil {
				if strings.TrimSpace(text) != "" {
					return true, nil
				}
			} else {
				return true, nil
			}
		}
		if raw, ok := request["input"]; ok && string(raw) != "null" {
			var items []struct {
				Role string `json:"role"`
			}
			if json.Unmarshal(raw, &items) == nil {
				for _, item := range items {
					if item.Role == "system" || item.Role == "developer" {
						return true, nil
					}
				}
			} else {
				var item struct {
					Role string `json:"role"`
				}
				if json.Unmarshal(raw, &item) == nil && (item.Role == "system" || item.Role == "developer") {
					return true, nil
				}
			}
		}
	case protocol.KindAnthropic:
		raw, ok := request["system"]
		if !ok || string(raw) == "null" {
			return false, nil
		}
		var text string
		if json.Unmarshal(raw, &text) == nil {
			return strings.TrimSpace(text) != "", nil
		}
		return true, nil
	case protocol.KindGemini:
		_, instruction := geminiSystemInstruction(request)
		return len(instruction.Parts) > 0, nil
	}
	return false, nil
}

type geminiInstruction struct {
	Role  string            `json:"role,omitempty"`
	Parts []json.RawMessage `json:"parts"`
}

// geminiSystemInstruction 取 systemInstruction（兼容 snake_case），返回所用的键名与内容。
func geminiSystemInstruction(request map[string]json.RawMessage) (string, geminiInstruction) {
	for _, k := range []string{"systemInstruction", "system_instruction"} {
		raw, ok := request[k]
		if !ok || string(raw) == "null" {
			continue
		}
		var ins geminiInstruction
		if json.Unmarshal(raw, &ins) == nil {
			return k, ins
		}
	}
	return "systemInstruction", geminiInstruction{}
}

// injectGeminiSystemPrompt 在 systemInstruction.parts 最前面插入提示词。
func injectGeminiSystemPrompt(body []byte, prompt string, override bool) ([]byte, error) {
	request, err := decodePromptRequest(body)
	if err != nil {
		return nil, err
	}
	key, ins := geminiSystemInstruction(request)
	if len(ins.Parts) > 0 && !override {
		return body, nil
	}
	part, _ := json.Marshal(map[string]string{"text": prompt})
	ins.Parts = append([]json.RawMessage{part}, ins.Parts...)
	ins.Role = ""
	request[key], _ = json.Marshal(ins)
	return json.Marshal(request)
}

func injectChatSystemPrompt(body []byte, prompt string, override bool) ([]byte, error) {
	request, err := decodePromptRequest(body)
	if err != nil {
		return nil, err
	}

	var messages []json.RawMessage
	if raw, ok := request["messages"]; ok && string(raw) != "null" {
		if err := json.Unmarshal(raw, &messages); err != nil {
			return nil, errors.New("invalid messages")
		}
	}

	systemIndex := -1
	for i, raw := range messages {
		var message struct {
			Role string `json:"role"`
		}
		if json.Unmarshal(raw, &message) == nil && (message.Role == "system" || message.Role == "developer") {
			systemIndex = i
			break
		}
	}
	if systemIndex < 0 {
		injected, _ := json.Marshal(map[string]any{"role": "system", "content": prompt})
		messages = append([]json.RawMessage{injected}, messages...)
	} else {
		if !override {
			return body, nil
		}
		updated, err := prependChatSystemContent(messages[systemIndex], prompt)
		if err != nil {
			return nil, err
		}
		messages[systemIndex] = updated
	}

	request["messages"], _ = json.Marshal(messages)
	return json.Marshal(request)
}

func prependChatSystemContent(raw json.RawMessage, prompt string) ([]byte, error) {
	var message map[string]json.RawMessage
	if err := json.Unmarshal(raw, &message); err != nil {
		return nil, errors.New("invalid system message")
	}
	content, ok := message["content"]
	if !ok || string(content) == "null" {
		message["content"], _ = json.Marshal(prompt)
		return json.Marshal(message)
	}
	var text string
	if json.Unmarshal(content, &text) == nil {
		text = strings.TrimSpace(text)
		if text == "" {
			message["content"], _ = json.Marshal(prompt)
		} else {
			message["content"], _ = json.Marshal(prompt + "\n" + text)
		}
		return json.Marshal(message)
	}
	var parts []json.RawMessage
	if json.Unmarshal(content, &parts) != nil {
		return nil, errors.New("invalid system message content")
	}
	part, _ := json.Marshal(map[string]any{"type": "text", "text": prompt})
	parts = append([]json.RawMessage{part}, parts...)
	message["content"], _ = json.Marshal(parts)
	return json.Marshal(message)
}

func injectResponsesSystemPrompt(body []byte, prompt string, override bool) ([]byte, error) {
	request, err := decodePromptRequest(body)
	if err != nil {
		return nil, err
	}
	raw, ok := request["instructions"]
	if !ok || string(raw) == "null" {
		request["instructions"], _ = json.Marshal(prompt)
		return json.Marshal(request)
	}
	var existing string
	if json.Unmarshal(raw, &existing) != nil {
		if !override {
			return body, nil
		}
		request["instructions"], _ = json.Marshal(prompt)
		return json.Marshal(request)
	}
	existing = strings.TrimSpace(existing)
	if existing == "" {
		request["instructions"], _ = json.Marshal(prompt)
	} else if override {
		request["instructions"], _ = json.Marshal(prompt + "\n" + existing)
	} else {
		return body, nil
	}
	return json.Marshal(request)
}

func injectAnthropicSystemPrompt(body []byte, prompt string, override bool) ([]byte, error) {
	request, err := decodePromptRequest(body)
	if err != nil {
		return nil, err
	}
	raw, ok := request["system"]
	if !ok || string(raw) == "null" {
		request["system"], _ = json.Marshal(prompt)
		return json.Marshal(request)
	}
	var existing string
	if json.Unmarshal(raw, &existing) == nil {
		existing = strings.TrimSpace(existing)
		if existing == "" {
			request["system"], _ = json.Marshal(prompt)
		} else if override {
			request["system"], _ = json.Marshal(prompt + "\n" + existing)
		} else {
			return body, nil
		}
		return json.Marshal(request)
	}
	if !override {
		return body, nil
	}
	var parts []json.RawMessage
	if json.Unmarshal(raw, &parts) != nil {
		parts = nil
	}
	part, _ := json.Marshal(map[string]any{"type": "text", "text": prompt})
	parts = append([]json.RawMessage{part}, parts...)
	request["system"], _ = json.Marshal(parts)
	return json.Marshal(request)
}

func decodePromptRequest(body []byte) (map[string]json.RawMessage, error) {
	var request map[string]json.RawMessage
	if err := json.Unmarshal(body, &request); err != nil || request == nil {
		return nil, errors.New("invalid request body")
	}
	return request, nil
}

func (rt *Runtime) applyResponsesWebSocketSystemPrompt(group *storage.GatewayGroup, key *storage.GatewayKey, body []byte) ([]byte, error) {
	var event struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(body, &event) != nil || event.Type != "response.create" {
		return body, nil
	}
	return rt.applySystemPrompt(group, key, "/v1/responses", protocol.KindOpenAIResponses, body)
}
