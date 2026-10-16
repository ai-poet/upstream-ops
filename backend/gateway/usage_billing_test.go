package gateway

import (
	"bytes"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
)

// latestUsage 最近一条用量日志（流式请求的用量可能在回包后才写入，稍等）。
func latestUsage(t *testing.T, h *e2eHarness) storage.GatewayUsageLog {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		page, err := h.svc.Usage.List(storage.GatewayUsageQuery{Page: 1, PageSize: 1})
		if err == nil && page != nil && len(page.Items) > 0 {
			return page.Items[0].GatewayUsageLog
		}
		if time.Now().After(deadline) {
			t.Fatalf("no usage log: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type wantUsage struct {
	in, out, cacheRead, cacheCreate, create5m, create1h int
}

func checkUsage(t *testing.T, name string, got storage.GatewayUsageLog, want wantUsage) {
	t.Helper()
	have := wantUsage{got.InputTokens, got.OutputTokens, got.CacheReadTokens, got.CacheCreationTokens, got.CacheCreation5mTokens, got.CacheCreation1hTokens}
	if have != want {
		t.Fatalf("%s usage = %+v, want %+v", name, have, want)
	}
}

// 落库的输入 token 是「不含缓存」的那部分：OpenAI 的输入含缓存要扣掉；
// Anthropic 的 input_tokens 本来就不含缓存、Gemini 解析时已扣过，不能再扣。
func TestE2E_UsageBucketsPerUpstreamProtocol(t *testing.T) {
	cases := []struct {
		name, protocol, path, req, resp string
		header                          map[string]string
		want                            wantUsage
	}{
		{
			name: "chat", protocol: "openai_chat", path: "/v1/chat/completions",
			req:  `{"model":"m","messages":[{"role":"user","content":"hi"}]}`,
			resp: `{"id":"x","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1000,"completion_tokens":50,"prompt_tokens_details":{"cached_tokens":400}}}`,
			want: wantUsage{in: 600, out: 50, cacheRead: 400},
		},
		{
			name: "responses", protocol: "openai_responses", path: "/v1/responses",
			req:  `{"model":"m","input":"hi"}`,
			resp: `{"id":"resp_x","object":"response","status":"completed","output":[],"usage":{"input_tokens":1000,"output_tokens":50,"input_tokens_details":{"cached_tokens":400}}}`,
			want: wantUsage{in: 600, out: 50, cacheRead: 400},
		},
		{
			name: "anthropic", protocol: "anthropic", path: "/v1/messages",
			req:  `{"model":"m","max_tokens":10,"messages":[{"role":"user","content":"hi"}]}`,
			resp: `{"id":"msg_x","type":"message","role":"assistant","content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn","usage":{"input_tokens":100,"output_tokens":50,"cache_read_input_tokens":1000,"cache_creation_input_tokens":300,"cache_creation":{"ephemeral_5m_input_tokens":100,"ephemeral_1h_input_tokens":200}}}`,
			want: wantUsage{in: 100, out: 50, cacheRead: 1000, cacheCreate: 300, create5m: 100, create1h: 200},
		},
		{
			name: "gemini", protocol: "gemini", path: "/v1beta/models/m:generateContent",
			req:    `{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`,
			resp:   `{"candidates":[{"content":{"role":"model","parts":[{"text":"hi"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1000,"cachedContentTokenCount":400,"candidatesTokenCount":50,"thoughtsTokenCount":20}}`,
			header: map[string]string{"x-goog-api-key": "set-below"},
			want:   wantUsage{in: 600, out: 70, cacheRead: 400},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, tc.resp)
			})
			h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: tc.protocol})
			header := map[string]string{}
			if tc.header != nil {
				header["x-goog-api-key"] = h.key
			}
			if w := h.do(http.MethodPost, tc.path, tc.req, header); w.Code != http.StatusOK {
				t.Fatalf("status %d %s", w.Code, w.Body.String())
			}
			checkUsage(t, tc.name, latestUsage(t, h), tc.want)
		})
	}
}

// Anthropic 流式：用量分散在 message_start（输入、缓存）与 message_delta（输出），合起来落库。
func TestE2E_AnthropicStreamUsageBuckets(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		writeSSE(w,
			"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"usage\":{\"input_tokens\":100,\"cache_read_input_tokens\":1000,\"cache_creation_input_tokens\":200,\"cache_creation\":{\"ephemeral_5m_input_tokens\":0,\"ephemeral_1h_input_tokens\":200},\"output_tokens\":1}}}\n\n",
			"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
			"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n",
			"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
			"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":50}}\n\n",
			"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
		)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "anthropic"})
	if w := h.do(http.MethodPost, "/v1/messages", `{"model":"m","stream":true,"max_tokens":10,"messages":[{"role":"user","content":"hi"}]}`, nil); w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	checkUsage(t, "anthropic stream", latestUsage(t, h), wantUsage{in: 100, out: 50, cacheRead: 1000, cacheCreate: 200, create1h: 200})
}

// 1 小时缓存写入按 1 小时单价计费（以前全按 5 分钟单价）。
func TestE2E_AnthropicOneHourCacheWritePricing(t *testing.T) {
	const model = "claude-sonnet-4-5"
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, `{"id":"msg_x","type":"message","role":"assistant","content":[{"type":"text","text":"hi"}],"stop_reason":"end_turn","usage":{"input_tokens":10,"output_tokens":5,"cache_creation_input_tokens":3000,"cache_creation":{"ephemeral_5m_input_tokens":1000,"ephemeral_1h_input_tokens":2000}}}`)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "anthropic"})
	price := h.svc.Pricing.Resolve(model)
	if price.CacheCreation1hPricePerToken <= price.CacheCreationPricePerToken {
		t.Fatalf("test needs a model with a higher 1h cache write price: %+v", price)
	}
	if w := h.do(http.MethodPost, "/v1/messages", `{"model":"`+model+`","max_tokens":10,"messages":[{"role":"user","content":"hi"}]}`, nil); w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	got := latestUsage(t, h)
	want := 1000*price.CacheCreationPricePerToken + 2000*price.CacheCreation1hPricePerToken
	if math.Abs(got.CacheCreationCost-want) > 1e-12 {
		t.Fatalf("cache creation cost = %v, want %v (5m %v / 1h %v per token)", got.CacheCreationCost, want, price.CacheCreationPricePerToken, price.CacheCreation1hPricePerToken)
	}
}

// 失败的尝试不收费：按张计费的模型失败一次再成功，只记成功那次的费用，统计合计与密钥已用额度一致。
func TestE2E_FailedAttemptsNotCharged(t *testing.T) {
	bad := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `{"error":{"message":"busy","type":"server_error"}}`)
	})
	good := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, chatCompletionJSON)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: bad, Protocol: "openai_chat", Weight: 10}, e2eRouteSpec{Upstream: good, Protocol: "openai_chat", Weight: 1})
	if w := h.do(http.MethodPost, "/v1/chat/completions", `{"model":"gemini-2.0-flash-exp-image-generation","messages":[{"role":"user","content":"draw"}]}`, nil); w.Code != http.StatusOK {
		t.Fatalf("status %d %s", w.Code, w.Body.String())
	}
	page, err := h.svc.Usage.List(storage.GatewayUsageQuery{Page: 1, PageSize: 10})
	if err != nil || len(page.Items) != 2 {
		t.Fatalf("usage rows: %v %+v", err, page)
	}
	for _, it := range page.Items {
		if !it.Success && (it.ActualCost != 0 || it.TotalCost != 0) {
			t.Fatalf("failed attempt charged: %+v", it.GatewayUsageLog)
		}
		if it.Success && it.ActualCost <= 0 {
			t.Fatalf("successful attempt not charged: %+v", it.GatewayUsageLog)
		}
	}
	key, _ := h.svc.Keys.FindByHash(HashAPIKey(h.key))
	if math.Abs(page.SumCost-key.QuotaUsed) > 1e-12 {
		t.Fatalf("usage sum %v != key quota used %v", page.SumCost, key.QuotaUsed)
	}
}

// 流已经写给客户端后上游中途断开：按 token 记下已消耗的标准价（对账上游用），但不向密钥收费。
func TestE2E_StreamBrokenAfterCommitNotCharged(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		writeSSE(w,
			`data: {"id":"c","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"partial"},"finish_reason":null}]}`+"\n\n",
			`data: {"id":"c","object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":1000,"completion_tokens":50}}`+"\n\n",
		) // 没有 finish_reason / [DONE] 就断开
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "openai_chat"})
	_ = h.do(http.MethodPost, "/v1/chat/completions", `{"model":"gpt-4o","stream":true,"messages":[{"role":"user","content":"hi"}]}`, nil)
	got := latestUsage(t, h)
	if got.Success || got.OutputTokens != 50 {
		t.Fatalf("usage = %+v", got)
	}
	if got.ActualCost != 0 || got.TotalCost <= 0 {
		t.Fatalf("broken stream cost: actual=%v total=%v", got.ActualCost, got.TotalCost)
	}
	if key, _ := h.svc.Keys.FindByHash(HashAPIKey(h.key)); key.QuotaUsed != 0 {
		t.Fatalf("key charged %v for a broken stream", key.QuotaUsed)
	}
}

// 找不到价格的模型：用量日志标记未定价、费用为 0；告警日志同一模型限频只打一次。有价格的模型不标记。
func TestE2E_UnpricedModelMarkedAndWarned(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, chatCompletionJSON)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "openai_chat"})
	var logBuf bytes.Buffer
	h.svc.Log = slog.New(slog.NewTextHandler(&logBuf, nil))

	for i := 0; i < 3; i++ {
		if w := h.do(http.MethodPost, "/v1/chat/completions", `{"model":"totally-unknown-model-x","messages":[{"role":"user","content":"hi"}]}`, nil); w.Code != http.StatusOK {
			t.Fatalf("status %d", w.Code)
		}
	}
	got := latestUsage(t, h)
	if !got.Unpriced || got.TotalCost != 0 || got.ActualCost != 0 || got.InputTokens == 0 {
		t.Fatalf("unpriced usage = %+v", got)
	}
	if n := strings.Count(logBuf.String(), "has no price"); n != 1 {
		t.Fatalf("warnings = %d, want 1 (rate limited)\n%s", n, logBuf.String())
	}
	if !strings.Contains(logBuf.String(), "model=totally-unknown-model-x") {
		t.Fatalf("warning lacks model: %s", logBuf.String())
	}

	if w := h.do(http.MethodPost, "/v1/chat/completions", `{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}`, nil); w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	if got := latestUsage(t, h); got.Unpriced || got.TotalCost <= 0 {
		t.Fatalf("priced model usage = %+v", got)
	}
}
