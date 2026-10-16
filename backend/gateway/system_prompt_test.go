package gateway

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
)

func TestApplySystemPromptRulesOrderScopeAndEndpoint(t *testing.T) {
	rules := `[
		{"enabled":true,"text":"all-first","key_scope":"all"},
		{"enabled":true,"text":"selected-second","key_scope":"selected","key_ids":[3,7,9]},
		{"enabled":false,"text":"disabled","key_scope":"all"},
		{"enabled":true,"text":"other-key","key_scope":"selected","key_ids":[8]}
	]`
	body := []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`)
	tests := []struct {
		name string
		key  uint
		path string
		kind protocol.Kind
		want string
	}{
		{name: "多个匹配规则按顺序注入", key: 7, path: "/v1/chat/completions", kind: protocol.KindOpenAIChat, want: "all-first\nselected-second"},
		{name: "另一指定 Key", key: 8, path: "/v1/chat/completions", kind: protocol.KindOpenAIChat, want: "all-first\nother-key"},
		{name: "未指定 Key 只应用全部规则", key: 10, path: "/v1/chat/completions", kind: protocol.KindOpenAIChat, want: "all-first"},
		{name: "兼容 openai 旧协议名", key: 10, path: "/v1/chat/completions", kind: protocol.KindOpenAI, want: "all-first"},
		{name: "Responses compact", key: 7, path: "/v1/responses/compact", kind: protocol.KindOpenAIResponses, want: "all-first\nselected-second"},
		{name: "legacy completions 不注入", key: 7, path: "/v1/completions", kind: protocol.KindOpenAIChat},
		{name: "embeddings 不注入", key: 7, path: "/v1/embeddings", kind: protocol.KindOpenAIChat},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := (&Runtime{}).applySystemPrompt(
				&storage.GatewayGroup{SystemPromptRulesJSON: rules},
				&storage.GatewayKey{ID: tt.key}, tt.path, tt.kind, body,
			)
			if err != nil {
				t.Fatalf("apply: %v", err)
			}
			if tt.want == "" {
				if string(out) != string(body) {
					t.Fatalf("body changed: %s", out)
				}
				return
			}
			var request map[string]json.RawMessage
			if err := json.Unmarshal(out, &request); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if tt.kind == protocol.KindOpenAIResponses {
				var instructions string
				if json.Unmarshal(request["instructions"], &instructions) != nil || instructions != tt.want {
					t.Fatalf("instructions=%s", request["instructions"])
				}
				return
			}
			var messages []struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			}
			if json.Unmarshal(request["messages"], &messages) != nil || len(messages) != 2 || messages[0].Role != "system" || messages[0].Content != tt.want {
				t.Fatalf("messages=%+v", messages)
			}
		})
	}

	emptySelected := &storage.GatewayGroup{SystemPromptRulesJSON: `[{"enabled":true,"text":"none","key_scope":"selected","key_ids":[]}]`}
	out, err := (&Runtime{}).applySystemPrompt(emptySelected, &storage.GatewayKey{ID: 7}, "/v1/chat/completions", protocol.KindOpenAIChat, body)
	if err != nil || string(out) != string(body) {
		t.Fatalf("empty selected scope changed body: %s, err=%v", out, err)
	}
}

func TestApplySystemPromptRulesRespectOverride(t *testing.T) {
	rules := `[
		{"enabled":true,"text":"skip","override":false,"key_scope":"all"},
		{"enabled":true,"text":"first","override":true,"key_scope":"all"},
		{"enabled":true,"text":"second","override":true,"key_scope":"selected","key_ids":[7]}
	]`
	body := []byte(`{"model":"m","messages":[{"role":"developer","content":[{"type":"text","text":"client"},{"type":"text","text":"tail","cache_control":{"type":"ephemeral"}}]}]}`)
	out, err := (&Runtime{}).applySystemPrompt(
		&storage.GatewayGroup{SystemPromptRulesJSON: rules},
		&storage.GatewayKey{ID: 7}, "/v1/chat/completions", protocol.KindOpenAIChat, body,
	)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	var request struct {
		Messages []struct {
			Content []map[string]any `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(out, &request); err != nil {
		t.Fatalf("decode: %v", err)
	}
	content := request.Messages[0].Content
	if len(content) != 3 || content[0]["text"] != "first\nsecond" || content[2]["cache_control"] == nil {
		t.Fatalf("content=%+v", content)
	}
}

func TestInjectResponsesAndAnthropicSystemPrompt(t *testing.T) {
	rules := `[
		{"enabled":true,"text":"first","override":true,"key_scope":"all"},
		{"enabled":true,"text":"second","override":true,"key_scope":"all"}
	]`
	group := &storage.GatewayGroup{SystemPromptRulesJSON: rules}
	key := &storage.GatewayKey{ID: 1}
	rt := &Runtime{}

	responses, err := rt.applySystemPrompt(group, key, "/v1/responses", protocol.KindOpenAIResponses, []byte(`{"model":"m","instructions":"client"}`))
	if err != nil {
		t.Fatalf("responses: %v", err)
	}
	var response map[string]any
	if err := json.Unmarshal(responses, &response); err != nil || response["instructions"] != "first\nsecond\nclient" {
		t.Fatalf("responses=%s", responses)
	}

	anthropic, err := rt.applySystemPrompt(group, key, "/v1/messages", protocol.KindAnthropic, []byte(`{"model":"m","system":[{"type":"text","text":"client","cache_control":{"type":"ephemeral"}}],"messages":[]}`))
	if err != nil {
		t.Fatalf("anthropic: %v", err)
	}
	var request struct {
		System []map[string]any `json:"system"`
	}
	if err := json.Unmarshal(anthropic, &request); err != nil {
		t.Fatalf("decode anthropic: %v", err)
	}
	if len(request.System) != 2 || request.System[0]["text"] != "first\nsecond" || request.System[1]["cache_control"] == nil {
		t.Fatalf("system=%+v", request.System)
	}
}

func TestResponsesInputSystemPromptCountsAsClientPrompt(t *testing.T) {
	rules := `[{"text":"gateway","key_scope":"all"}]`
	body := []byte(`{"model":"m","input":[{"role":"developer","content":"client"}]}`)
	out, err := (&Runtime{}).applySystemPrompt(
		&storage.GatewayGroup{SystemPromptRulesJSON: rules},
		&storage.GatewayKey{ID: 1}, "/v1/responses", protocol.KindOpenAIResponses, body,
	)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if string(out) != string(body) {
		t.Fatalf("existing input system prompt was changed: %s", out)
	}
}

func TestApplyResponsesWebSocketSystemPromptOnlyOnCreate(t *testing.T) {
	rt := &Runtime{}
	group := &storage.GatewayGroup{SystemPromptRulesJSON: `[{"enabled":true,"text":"first","key_scope":"all"},{"enabled":true,"text":"second","key_scope":"all"}]`}
	key := &storage.GatewayKey{ID: 1}
	created, err := rt.applyResponsesWebSocketSystemPrompt(group, key, []byte(`{"type":"response.create","model":"m"}`))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	var event map[string]any
	if err := json.Unmarshal(created, &event); err != nil || event["instructions"] != "first\nsecond" {
		t.Fatalf("created=%s", created)
	}
	other := []byte(`{"type":"response.append","input":[]}`)
	unchanged, err := rt.applyResponsesWebSocketSystemPrompt(group, key, other)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if string(unchanged) != string(other) {
		t.Fatalf("append changed: %s", unchanged)
	}
}

func TestInjectedSystemPromptSurvivesProtocolConversion(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		inbound  protocol.Kind
		upstream protocol.Kind
		body     string
		assert   func(t *testing.T, body []byte)
	}{
		{
			name: "chat to anthropic", path: "/v1/chat/completions",
			inbound: protocol.KindOpenAIChat, upstream: protocol.KindAnthropic,
			body: `{"model":"m","messages":[{"role":"user","content":"hi"}]}`,
			assert: func(t *testing.T, body []byte) {
				var request map[string]any
				if json.Unmarshal(body, &request) != nil || request["system"] != "first\nsecond" {
					t.Fatalf("converted=%s", body)
				}
			},
		},
		{
			name: "responses to chat", path: "/v1/responses",
			inbound: protocol.KindOpenAIResponses, upstream: protocol.KindOpenAIChat,
			body: `{"model":"m","input":"hi"}`,
			assert: func(t *testing.T, body []byte) {
				var request struct {
					Messages []map[string]any `json:"messages"`
				}
				if json.Unmarshal(body, &request) != nil || len(request.Messages) == 0 || request.Messages[0]["content"] != "first\nsecond" {
					t.Fatalf("converted=%s", body)
				}
			},
		},
		{
			name: "anthropic to responses", path: "/v1/messages",
			inbound: protocol.KindAnthropic, upstream: protocol.KindOpenAIResponses,
			body: `{"model":"m","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`,
			assert: func(t *testing.T, body []byte) {
				var request map[string]any
				if json.Unmarshal(body, &request) != nil || request["instructions"] != "first\nsecond" {
					t.Fatalf("converted=%s", body)
				}
			},
		},
	}
	rules := `[{"enabled":true,"text":"first","key_scope":"all"},{"enabled":true,"text":"second","key_scope":"all"}]`
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			injected, err := (&Runtime{}).applySystemPrompt(
				&storage.GatewayGroup{SystemPromptRulesJSON: rules},
				&storage.GatewayKey{ID: 1}, tt.path, tt.inbound, []byte(tt.body),
			)
			if err != nil {
				t.Fatalf("inject: %v", err)
			}
			converted, _, err := protocol.ConvertRequest(tt.inbound, tt.upstream, injected, "m", false)
			if err != nil {
				t.Fatalf("convert: %v", err)
			}
			tt.assert(t, converted)
		})
	}
}

func TestNormalizeSystemPromptRulesJSON(t *testing.T) {
	raw, keyIDs, err := normalizeSystemPromptRulesJSON(`[
		{"text":" first ","key_scope":"selected","key_ids":[41,42,41,0]},
		{"enabled":false,"text":"second","override":true,"key_scope":"all","key_ids":[99]}
	]`)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	want := `[{"enabled":true,"text":"first","override":false,"key_scope":"selected","key_ids":[41,42]},{"enabled":false,"text":"second","override":true,"key_scope":"all"}]`
	if raw != want {
		t.Fatalf("normalized=%s", raw)
	}
	if fmt.Sprint(keyIDs) != "[41 42]" {
		t.Fatalf("key ids=%v", keyIDs)
	}
}

func TestUpdateGroupValidatesPromptRuleKeys(t *testing.T) {
	db := openGatewayTestDB(t)
	groups := storage.NewGatewayGroups(db)
	keys := storage.NewGatewayKeys(db)
	svc := NewService(groups, keys, storage.NewGatewayRoutes(db), nil, nil, nil, nil, nil, nil)
	first, err := svc.Admin.CreateGroup(CreateGroupInput{Name: "prompt-first"})
	if err != nil {
		t.Fatalf("create first: %v", err)
	}
	second, err := svc.Admin.CreateGroup(CreateGroupInput{Name: "prompt-second"})
	if err != nil {
		t.Fatalf("create second: %v", err)
	}
	firstKey := &storage.GatewayKey{GroupID: first.ID, Name: "prompt-key-1", KeyHash: "prompt-key-hash-1", KeyPrefix: "sk-prompt-1", KeyCipher: "cipher", Status: storage.GatewayKeyStatusActive}
	secondKey := &storage.GatewayKey{GroupID: first.ID, Name: "prompt-key-2", KeyHash: "prompt-key-hash-2", KeyPrefix: "sk-prompt-2", KeyCipher: "cipher", Status: storage.GatewayKeyStatusActive}
	otherKey := &storage.GatewayKey{GroupID: second.ID, Name: "prompt-key-other", KeyHash: "prompt-key-hash-other", KeyPrefix: "sk-prompt-other", KeyCipher: "cipher", Status: storage.GatewayKeyStatusActive}
	for _, key := range []*storage.GatewayKey{firstKey, secondKey, otherKey} {
		if err := keys.Create(key); err != nil {
			t.Fatalf("create key: %v", err)
		}
	}

	rules := fmt.Sprintf(`[{"text":"first","key_scope":"selected","key_ids":[%d,%d,%d]},{"text":"second","key_scope":"all"}]`, firstKey.ID, secondKey.ID, firstKey.ID)
	updated, err := svc.Admin.UpdateGroup(first.ID, UpdateGroupInput{SystemPromptRulesJSON: &rules})
	if err != nil {
		t.Fatalf("update rules: %v", err)
	}
	want := fmt.Sprintf(`[{"enabled":true,"text":"first","override":false,"key_scope":"selected","key_ids":[%d,%d]},{"enabled":true,"text":"second","override":false,"key_scope":"all"}]`, firstKey.ID, secondKey.ID)
	if updated.SystemPromptRulesJSON != want {
		t.Fatalf("rules=%s", updated.SystemPromptRulesJSON)
	}

	crossGroup := fmt.Sprintf(`[{"text":"bad","key_scope":"selected","key_ids":[%d,%d]}]`, firstKey.ID, otherKey.ID)
	if _, err := svc.Admin.UpdateGroup(first.ID, UpdateGroupInput{SystemPromptRulesJSON: &crossGroup}); err == nil {
		t.Fatal("expected cross-group key rejection")
	}
}

func TestDeleteKeyRemovesSystemPromptRuleReference(t *testing.T) {
	db := openGatewayTestDB(t)
	groups := storage.NewGatewayGroups(db)
	keys := storage.NewGatewayKeys(db)
	svc := NewService(groups, keys, storage.NewGatewayRoutes(db), nil, nil, nil, nil, nil, nil)
	group, err := svc.Admin.CreateGroup(CreateGroupInput{Name: "prompt-delete"})
	if err != nil {
		t.Fatalf("create group: %v", err)
	}
	key := &storage.GatewayKey{GroupID: group.ID, Name: "prompt-delete-key", KeyHash: "prompt-delete-hash", KeyPrefix: "sk-prompt-delete", KeyCipher: "cipher", Status: storage.GatewayKeyStatusActive}
	if err := keys.Create(key); err != nil {
		t.Fatalf("create key: %v", err)
	}
	rules := fmt.Sprintf(`[{"text":"selected","key_scope":"selected","key_ids":[%d]},{"text":"all","key_scope":"all"}]`, key.ID)
	if _, err := svc.Admin.UpdateGroup(group.ID, UpdateGroupInput{SystemPromptRulesJSON: &rules}); err != nil {
		t.Fatalf("update rules: %v", err)
	}
	if err := svc.Admin.DeleteKey(key.ID); err != nil {
		t.Fatalf("delete key: %v", err)
	}
	updated, err := groups.FindByID(group.ID)
	if err != nil {
		t.Fatalf("find group: %v", err)
	}
	want := `[{"enabled":true,"text":"selected","override":false,"key_scope":"selected"},{"enabled":true,"text":"all","override":false,"key_scope":"all"}]`
	if updated.SystemPromptRulesJSON != want {
		t.Fatalf("rules=%s", updated.SystemPromptRulesJSON)
	}
}
