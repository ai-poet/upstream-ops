package gateway

import (
	"encoding/json"
	"errors"
	"path"
	"strings"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/tidwall/gjson"
)

const (
	serviceTierAll      = "all"
	serviceTierPriority = "priority"
	serviceTierFlex     = "flex"

	serviceTierFilter        = "filter"
	serviceTierPassthrough   = "passthrough"
	serviceTierBlock         = "block"
	serviceTierForcePriority = "force_priority"
)

// ServiceTierRule 是网关组针对 OpenAI service_tier 的处理规则。
// KeyScope=all 表示所有 Key；KeyScope=selected 时由 KeyIDs 指定目标 Key。
type ServiceTierRule struct {
	Tier     string `json:"tier"`
	Action   string `json:"action"`
	KeyScope string `json:"key_scope,omitempty"`
	KeyIDs   []uint `json:"key_ids,omitempty"`
	// KeyID 仅兼容旧版单 Key 规则，保存时会归并到 KeyIDs。
	KeyID           uint     `json:"key_id,omitempty"`
	LegacyUserEmail string   `json:"user_email,omitempty"`
	Models          []string `json:"models,omitempty"`
}

func normalizeServiceTierRulesJSON(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	var rules []ServiceTierRule
	if err := json.Unmarshal([]byte(raw), &rules); err != nil {
		return "", errors.New("invalid service tier rules")
	}
	for i := range rules {
		rule := &rules[i]
		rule.Tier = normalizeServiceTier(rule.Tier)
		if rule.Tier == "" {
			return "", errors.New("invalid service tier match")
		}
		rule.Action = normalizeServiceTierAction(rule.Action)
		if rule.Action == "" {
			return "", errors.New("invalid service tier action")
		}
		seenKeyIDs := make(map[uint]struct{}, len(rule.KeyIDs)+1)
		keyIDs := make([]uint, 0, len(rule.KeyIDs)+1)
		for _, keyID := range append(rule.KeyIDs, rule.KeyID) {
			if keyID == 0 {
				continue
			}
			if _, seen := seenKeyIDs[keyID]; seen {
				continue
			}
			seenKeyIDs[keyID] = struct{}{}
			keyIDs = append(keyIDs, keyID)
		}
		rule.KeyIDs = keyIDs
		rule.KeyID = 0
		rule.LegacyUserEmail = strings.ToLower(strings.TrimSpace(rule.LegacyUserEmail))
		rule.KeyScope = strings.ToLower(strings.TrimSpace(rule.KeyScope))
		if rule.KeyScope == "" {
			if len(rule.KeyIDs) > 0 || rule.LegacyUserEmail != "" {
				rule.KeyScope = "selected"
			} else {
				rule.KeyScope = "all"
			}
		}
		if rule.KeyScope != "all" && rule.KeyScope != "selected" {
			return "", errors.New("invalid service tier key scope")
		}
		models := make([]string, 0, len(rule.Models))
		for _, model := range rule.Models {
			if model = strings.TrimSpace(model); model != "" {
				models = append(models, model)
			}
		}
		rule.Models = models
	}
	body, _ := json.Marshal(rules)
	return string(body), nil
}

func normalizeServiceTierAction(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", serviceTierFilter:
		return serviceTierFilter
	case "pass", serviceTierPassthrough:
		return serviceTierPassthrough
	case serviceTierBlock:
		return serviceTierBlock
	case serviceTierForcePriority:
		return serviceTierForcePriority
	default:
		return ""
	}
}

func serviceTierRules(raw string) []ServiceTierRule {
	normalized, err := normalizeServiceTierRulesJSON(raw)
	if err != nil {
		return nil
	}
	var rules []ServiceTierRule
	_ = json.Unmarshal([]byte(normalized), &rules)
	return rules
}

func normalizeServiceTier(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case serviceTierAll:
		return serviceTierAll
	case serviceTierPriority, "fast":
		return serviceTierPriority
	case serviceTierFlex:
		return serviceTierFlex
	default:
		return ""
	}
}

type serviceTierBlockedError struct {
	Tier  string
	Model string
}

func (e *serviceTierBlockedError) Error() string {
	if e == nil {
		return "openai request is blocked by gateway policy"
	}
	if strings.TrimSpace(e.Model) == "" {
		return "openai service_tier=" + e.Tier + " is blocked by gateway policy"
	}
	return "openai service_tier=" + e.Tier + " is blocked for model " + e.Model
}

func (rt *Runtime) applyServiceTierRules(group *storage.GatewayGroup, key *storage.GatewayKey, kind protocolKind, model string, body []byte) ([]byte, error) {
	if group == nil || key == nil || !protocol.IsOpenAIFamily(kind) {
		return body, nil
	}
	// 绝大多数请求不带 service_tier：扫一下就返回，不整包解析
	if !looksLikeJSONObject(body) || gjson.GetBytes(body, "service_tier").Type != gjson.String {
		return body, nil
	}
	var request map[string]json.RawMessage
	if json.Unmarshal(body, &request) != nil {
		return body, nil
	}
	rawTier, ok := request["service_tier"]
	if !ok {
		return body, nil
	}
	var tier string
	if json.Unmarshal(rawTier, &tier) != nil || strings.TrimSpace(tier) == "" {
		return body, nil
	}
	normalizedTier := normalizeServiceTier(tier)
	rule := matchingServiceTierRule(serviceTierRules(group.ServiceTierRulesJSON), key, model, tier)
	if rule != nil {
		switch rule.Action {
		case serviceTierFilter:
			delete(request, "service_tier")
			filtered, err := json.Marshal(request)
			if err != nil {
				return body, nil
			}
			return filtered, nil
		case serviceTierBlock:
			blockedTier := normalizedTier
			if blockedTier == "" {
				blockedTier = strings.ToLower(strings.TrimSpace(tier))
			}
			return body, &serviceTierBlockedError{Tier: blockedTier, Model: model}
		case serviceTierForcePriority:
			request["service_tier"], _ = json.Marshal(serviceTierPriority)
			forced, err := json.Marshal(request)
			if err != nil {
				return body, nil
			}
			return forced, nil
		}
	}
	if normalizedTier != serviceTierPriority && normalizedTier != serviceTierFlex {
		return body, nil
	}
	if tier == normalizedTier {
		return body, nil
	}
	request["service_tier"], _ = json.Marshal(normalizedTier)
	normalized, err := json.Marshal(request)
	if err != nil {
		return body, nil
	}
	return normalized, nil
}

// 指定 Key 规则优先；仅当指定规则未匹配当前 tier/model 时，才回退到全部 Key 规则。
func matchingServiceTierRule(rules []ServiceTierRule, key *storage.GatewayKey, model, tier string) *ServiceTierRule {
	if key == nil {
		return nil
	}
	for _, keyScoped := range []bool{true, false} {
		for i := range rules {
			rule := &rules[i]
			hasKey := serviceTierRuleIsKeyScoped(rule)
			if hasKey != keyScoped {
				continue
			}
			if hasKey && !serviceTierRuleHasKey(rule, key.ID) {
				continue
			}
			if rule.LegacyUserEmail != "" && !strings.EqualFold(rule.LegacyUserEmail, key.OwnerEmail) {
				continue
			}
			if !serviceTierMatches(rule.Tier, tier) || !serviceTierModelMatches(rule.Models, model) {
				continue
			}
			return rule
		}
	}
	return nil
}

func serviceTierRuleIsKeyScoped(rule *ServiceTierRule) bool {
	if rule == nil {
		return false
	}
	return rule.KeyScope == "selected" || len(rule.KeyIDs) > 0 || rule.KeyID > 0 || rule.LegacyUserEmail != ""
}

func serviceTierRuleHasKey(rule *ServiceTierRule, keyID uint) bool {
	if rule == nil {
		return false
	}
	if len(rule.KeyIDs) == 0 && rule.KeyID == 0 {
		return rule.LegacyUserEmail != ""
	}
	for _, id := range rule.KeyIDs {
		if id == keyID {
			return true
		}
	}
	return rule.KeyID == keyID
}

func serviceTierMatches(ruleTier, requestTier string) bool {
	if normalizeServiceTier(ruleTier) == serviceTierAll {
		return true
	}
	return normalizeServiceTier(ruleTier) == normalizeServiceTier(requestTier)
}

func serviceTierModelMatches(patterns []string, model string) bool {
	if len(patterns) == 0 {
		return true
	}
	model = strings.ToLower(strings.TrimSpace(model))
	for _, pattern := range patterns {
		pattern = strings.ToLower(strings.TrimSpace(pattern))
		if pattern == "" {
			continue
		}
		matched, err := path.Match(pattern, model)
		if err == nil && matched {
			return true
		}
	}
	return false
}
