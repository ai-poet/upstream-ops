package protocol

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func joinFrames(frames [][]byte) string {
	var b strings.Builder
	for _, f := range frames {
		b.Write(f)
	}
	return b.String()
}

func feedSSE(conv StreamConverter, raw string) [][]byte {
	var out [][]byte
	for _, block := range strings.Split(raw, "\n\n") {
		block = strings.TrimSpace(block)
		if block == "" {
			continue
		}
		var event, data string
		for _, line := range strings.Split(block, "\n") {
			if strings.HasPrefix(line, "event:") {
				event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			}
			if strings.HasPrefix(line, "data:") {
				data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			}
		}
		out = append(out, conv.Feed(event, data)...)
	}
	return append(out, conv.Close()...)
}

func TestRelayKit_ChatToClaudeRequest(t *testing.T) {
	cv := NewConversion(KindOpenAIChat, KindAnthropic, "client-model", "claude-sonnet-4-5", true)
	out, err := cv.Request(context.Background(), []byte(`{
		"model":"client-model","stream":true,"reasoning_effort":"high",
		"messages":[{"role":"system","content":"be brief"},{"role":"user","content":"hi"}],
		"tools":[{"type":"function","function":{"name":"Bash","parameters":{"type":"object","properties":{"cmd":{"type":"string"}}}}}]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	if m["model"] != "claude-sonnet-4-5" || m["stream"] != true {
		t.Fatalf("model/stream: %s", out)
	}
	if m["system"] == nil || m["tools"] == nil {
		t.Fatalf("system/tools missing: %s", out)
	}
	maxTokens, _ := m["max_tokens"].(float64)
	thinking, _ := m["thinking"].(map[string]any)
	if thinking == nil {
		t.Fatalf("thinking missing: %s", out)
	}
	if budget, ok := thinking["budget_tokens"].(float64); ok && budget >= maxTokens {
		t.Fatalf("budget %v must be below max_tokens %v: %s", budget, maxTokens, out)
	}
	t.Logf("steps=%v quality=%s out=%s", cv.Steps, cv.Quality, out)
}

func TestRelayKit_GeminiToChatRequestAndBack(t *testing.T) {
	cv := NewConversion(KindGemini, KindOpenAIChat, "gemini-2.5-pro", "gpt-4.1", true)
	out, err := cv.Request(context.Background(), []byte(`{
		"systemInstruction":{"parts":[{"text":"sys"}]},
		"contents":[{"role":"user","parts":[{"text":"hello"}]}],
		"generationConfig":{"maxOutputTokens":100,"temperature":0.3}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	s := string(out)
	if !strings.Contains(s, `"model":"gpt-4.1"`) || !strings.Contains(s, `"stream":true`) || !strings.Contains(s, "hello") {
		t.Fatalf("chat body: %s", s)
	}
	conv, err := cv.NewStream(context.Background(), "chatcmpl-1")
	if err != nil {
		t.Fatal(err)
	}
	frames := feedSSE(conv, `data: {"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","content":"Hel"}}]}

data: {"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"lo"},"finish_reason":"stop"}]}

data: {"id":"c1","object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}

data: [DONE]`)
	got := joinFrames(frames)
	if !strings.Contains(got, `"candidates"`) || !strings.Contains(got, "Hel") || !strings.Contains(got, "STOP") {
		t.Fatalf("gemini stream: %s", got)
	}
	if strings.Contains(got, "chat.completion.chunk") || strings.Contains(got, "[DONE]") || strings.Contains(got, "billing_usage") {
		t.Fatalf("chat / internal fields leaked into gemini stream: %s", got)
	}
	t.Logf("gemini frames:\n%s", got)
}

func TestRelayKit_ChatToGeminiRequestHasNoModel(t *testing.T) {
	cv := NewConversion(KindOpenAIChat, KindGemini, "gemini-2.5-flash", "gemini-2.5-flash", true)
	out, err := cv.Request(context.Background(), []byte(`{"model":"gemini-2.5-flash","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	_ = json.Unmarshal(out, &m)
	if _, ok := m["model"]; ok {
		t.Fatalf("gemini body must not carry model: %s", out)
	}
	if _, ok := m["contents"]; !ok {
		t.Fatalf("contents missing: %s", out)
	}
	conv, err := cv.NewStream(context.Background(), "chatcmpl-x")
	if err != nil {
		t.Fatal(err)
	}
	frames := feedSSE(conv, `data: {"candidates":[{"content":{"role":"model","parts":[{"text":"AB"}]}}]}

data: {"candidates":[{"content":{"role":"model","parts":[{"text":"CD"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":2,"candidatesTokenCount":2,"totalTokenCount":4}}`)
	got := joinFrames(frames)
	if !strings.Contains(got, "chat.completion.chunk") || !strings.Contains(got, "AB") || !strings.HasSuffix(strings.TrimSpace(got), "data: [DONE]") {
		t.Fatalf("chat stream: %s", got)
	}
}

func TestRelayKit_ClaudeStreamToResponses(t *testing.T) {
	cv := NewConversion(KindOpenAIResponses, KindAnthropic, "m", "claude-sonnet-4-5", true)
	if _, err := cv.Request(context.Background(), []byte(`{"model":"m","stream":true,"input":"hi","max_output_tokens":64}`)); err != nil {
		t.Fatal(err)
	}
	conv, err := cv.NewStream(context.Background(), "resp_1")
	if err != nil {
		t.Fatal(err)
	}
	frames := feedSSE(conv, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"model\":\"m\",\"role\":\"assistant\",\"content\":[],\"usage\":{\"input_tokens\":3,\"output_tokens\":0}}}\n\n"+
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n"+
		"event: ping\ndata: {\"type\":\"ping\"}\n\n"+
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"AB\"}}\n\n"+
		"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n"+
		"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\n"+
		"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
	got := joinFrames(frames)
	for _, want := range []string{"event: response.created", "response.output_text.delta", "AB", "event: response.completed"} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in:\n%s", want, got)
		}
	}
}

func TestRelayKit_ResponsesNonStreamToChat(t *testing.T) {
	cv := NewConversion(KindOpenAIChat, KindOpenAIResponses, "m", "gpt-5", false)
	if _, err := cv.Request(context.Background(), []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`)); err != nil {
		t.Fatal(err)
	}
	out, err := cv.Response(context.Background(), []byte(`{"id":"resp_1","object":"response","status":"completed","model":"gpt-5",
		"output":[{"type":"message","id":"msg_1","role":"assistant","content":[{"type":"output_text","text":"hello"}]}],
		"usage":{"input_tokens":3,"output_tokens":1,"total_tokens":4}}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), `"chat.completion"`) || !strings.Contains(string(out), "hello") {
		t.Fatalf("chat response: %s", out)
	}
}

// 经 Chat 中转的两跳方向：请求能转、流式能收尾。
func TestRelayKit_MultiHopDirections(t *testing.T) {
	claudeSSE := "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"model\":\"m\",\"role\":\"assistant\",\"content\":[],\"usage\":{\"input_tokens\":3,\"output_tokens\":0}}}\n\n" +
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"AB\"}}\n\n" +
		"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n" +
		"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\n" +
		"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
	geminiSSE := `data: {"candidates":[{"content":{"role":"model","parts":[{"text":"AB"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":2,"candidatesTokenCount":1,"totalTokenCount":3}}` + "\n\n"
	cases := []struct {
		name       string
		in, up     Kind
		body       string
		upstream   string
		wantReq    string
		wantStream []string
	}{
		{"gemini←claude", KindGemini, KindAnthropic, `{"contents":[{"role":"user","parts":[{"text":"hi"}]}],"generationConfig":{"maxOutputTokens":64}}`, claudeSSE, `"messages"`, []string{`"candidates"`, "AB", "STOP"}},
		{"claude←gemini", KindAnthropic, KindGemini, `{"model":"m","max_tokens":64,"messages":[{"role":"user","content":"hi"}]}`, geminiSSE, `"contents"`, []string{"event: message_start", "AB", "event: message_stop"}},
		{"responses←gemini", KindOpenAIResponses, KindGemini, `{"model":"m","input":"hi"}`, geminiSSE, `"contents"`, []string{"event: response.created", "AB", "event: response.completed"}},
		{"gemini←responses", KindGemini, KindOpenAIResponses, `{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`,
			"event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\",\"model\":\"m\"}}\n\n" +
				"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"item_id\":\"m1\",\"delta\":\"AB\"}\n\n" +
				"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"status\":\"completed\",\"usage\":{\"input_tokens\":2,\"output_tokens\":1}}}\n\n",
			`"input"`, []string{`"candidates"`, "AB"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cv := NewConversion(tc.in, tc.up, "m", "m", true)
			out, err := cv.Request(context.Background(), []byte(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(out), tc.wantReq) {
				t.Fatalf("request %s missing %s: %s", cv.Steps, tc.wantReq, out)
			}
			conv, err := cv.NewStream(context.Background(), "id-1")
			if err != nil {
				t.Fatal(err)
			}
			got := joinFrames(feedSSE(conv, tc.upstream))
			for _, w := range tc.wantStream {
				if !strings.Contains(got, w) {
					t.Fatalf("stream missing %q:\n%s", w, got)
				}
			}
			if strings.Contains(got, "billing_usage") {
				t.Fatalf("internal field leaked:\n%s", got)
			}
		})
	}
}

// Gemini → Claude（两跳）且源请求没要推理：不能因为 relaykit 按源模型补的默认思考丢掉采样参数、抬高 max_tokens。
func TestRelayKit_GeminiToClaudeKeepsSampling(t *testing.T) {
	cv := NewConversion(KindGemini, KindAnthropic, "gemini-2.5-pro", "claude-sonnet-4-5", false)
	out, err := cv.Request(context.Background(), []byte(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}],"generationConfig":{"temperature":0.3,"topP":0.5,"topK":20,"maxOutputTokens":100}}`))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	_ = json.Unmarshal(out, &m)
	if m["temperature"] != 0.3 || m["top_p"] != 0.5 || m["top_k"] != float64(20) || m["max_tokens"] != float64(100) {
		t.Fatalf("sampling / max_tokens not preserved: %s diag=%v", out, cv.Diagnostics)
	}
	if _, ok := m["thinking"]; ok {
		t.Fatalf("defaulted thinking leaked: %s", out)
	}
}

// 只删 usage 里的内部字段，工具参数里用户自己的同名键原样保留。
func TestRelayKit_UsageInternalsStrippedOnlyFromUsage(t *testing.T) {
	cv := NewConversion(KindAnthropic, KindGemini, "claude-x", "gemini-2.5-flash", false)
	if _, err := cv.Request(context.Background(), []byte(`{"model":"claude-x","max_tokens":64,"messages":[{"role":"user","content":"hi"}]}`)); err != nil {
		t.Fatal(err)
	}
	out, err := cv.Response(context.Background(), []byte(`{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"f","args":{"billing_usage":"keep-me","usage_source":"mine"}}}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":2,"candidatesTokenCount":2,"totalTokenCount":4}}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), "keep-me") || !strings.Contains(string(out), `"usage_source":"mine"`) {
		t.Fatalf("user tool arguments were stripped: %s", out)
	}

	cv2 := NewConversion(KindOpenAIChat, KindAnthropic, "m", "claude-sonnet-4-5", false)
	if _, err := cv2.Request(context.Background(), []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`)); err != nil {
		t.Fatal(err)
	}
	out, _ = cv2.Response(context.Background(), []byte(`{"id":"msg_1","type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[{"type":"text","text":"hey"}],"stop_reason":"end_turn","usage":{"input_tokens":2,"output_tokens":1}}`))
	for _, k := range usageInternalKeys {
		if strings.Contains(string(out), `"`+k+`"`) {
			t.Fatalf("internal usage field %s reached the client: %s", k, out)
		}
	}
	if !strings.Contains(string(out), `"prompt_tokens":2`) {
		t.Fatalf("usage lost: %s", out)
	}
}

// Chat 流式块每块都带 id / model；Gemini → Chat 的 model 是上游模型名。
func TestRelayKit_ChatChunksCarryIDAndModel(t *testing.T) {
	cv := NewConversion(KindOpenAIChat, KindAnthropic, "client-m", "claude-sonnet-4-5", true)
	if _, err := cv.Request(context.Background(), []byte(`{"model":"client-m","stream":true,"max_tokens":64,"messages":[{"role":"user","content":"hi"}]}`)); err != nil {
		t.Fatal(err)
	}
	conv, err := cv.NewStream(context.Background(), "chatcmpl-1")
	if err != nil {
		t.Fatal(err)
	}
	got := joinFrames(feedSSE(conv, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"model\":\"claude-sonnet-4-5\",\"role\":\"assistant\",\"content\":[],\"usage\":{\"input_tokens\":3,\"output_tokens\":0}}}\n\n"+
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n"+
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"AB\"}}\n\n"+
		"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\n"+
		"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"))
	if strings.Contains(got, `"id":""`) || strings.Contains(got, `"model":""`) || strings.Contains(got, `"created":0`) {
		t.Fatalf("chat chunks with empty id/model/created:\n%s", got)
	}

	cv2 := NewConversion(KindOpenAIChat, KindGemini, "client-m", "gemini-2.5-flash", false)
	if _, err := cv2.Request(context.Background(), []byte(`{"model":"client-m","messages":[{"role":"user","content":"hi"}]}`)); err != nil {
		t.Fatal(err)
	}
	out, _ := cv2.Response(context.Background(), []byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":"AB"}]},"finishReason":"STOP"}]}`))
	if !strings.Contains(string(out), `"model":"gemini-2.5-flash"`) {
		t.Fatalf("gemini->chat response model: %s", out)
	}
}

// Gemini 流中的错误块不产出客户端帧（否则会提交 200 并失去顺延机会）。
func TestRelayKit_GeminiErrorChunkProducesNoFrame(t *testing.T) {
	cv := NewConversion(KindOpenAIChat, KindGemini, "m", "gemini-2.5-flash", true)
	if _, err := cv.Request(context.Background(), []byte(`{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`)); err != nil {
		t.Fatal(err)
	}
	conv, _ := cv.NewStream(context.Background(), "chatcmpl-1")
	if frames := conv.Feed("", `{"error":{"code":500,"message":"boom","status":"INTERNAL"}}`); len(frames) != 0 {
		t.Fatalf("error chunk produced frames: %s", joinFrames(frames))
	}
}

// 解析不了的上游事件不能被静默跳过：转换器记下错误，交给网关以失败结束。
func TestRelayKit_StreamDecodeErrorSurfaces(t *testing.T) {
	cv := NewConversion(KindAnthropic, KindOpenAIChat, "m", "gpt-x", true)
	if _, err := cv.Request(context.Background(), []byte(`{"model":"m","stream":true,"max_tokens":64,"messages":[{"role":"user","content":"hi"}]}`)); err != nil {
		t.Fatal(err)
	}
	conv, _ := cv.NewStream(context.Background(), "msg_1")
	errOf := func() error { return conv.(interface{ Err() error }).Err() }
	conv.Feed("", `{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"A"}}]}`)
	if err := errOf(); err != nil {
		t.Fatalf("valid chunk reported error: %v", err)
	}
	conv.Feed("", `{"id":"c1","created":"not-a-number","choices":[{"index":0,"delta":{"content":"B"}}]}`)
	if errOf() == nil {
		t.Fatal("undecodable chunk was silently dropped")
	}
}

func TestHasExplicitReasoning_NullThinkingConfig(t *testing.T) {
	if HasExplicitReasoning(KindGemini, []byte(`{"generationConfig":{"thinkingConfig":null}}`)) {
		t.Fatal("null thinkingConfig treated as explicit")
	}
	if !HasExplicitReasoning(KindGemini, []byte(`{"generationConfig":{"thinkingConfig":{"thinkingBudget":0}}}`)) {
		t.Fatal("thinkingConfig object must count as explicit")
	}
}
