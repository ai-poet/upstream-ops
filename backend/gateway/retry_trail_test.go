package gateway

import (
	"io"
	"net/http"
	"sort"
	"strings"
	"testing"

	"github.com/bejix/upstream-ops/backend/storage"
)

// requestTrail 最近一个请求的全部尝试（按尝试序号）。
func requestTrail(t *testing.T, h *e2eHarness) []storage.GatewayUsageLog {
	t.Helper()
	page, err := h.svc.Usage.List(storage.GatewayUsageQuery{Page: 1, PageSize: 50})
	if err != nil || page == nil || len(page.Items) == 0 {
		t.Fatalf("usage logs: %v", err)
	}
	rid := page.Items[0].RequestID
	var out []storage.GatewayUsageLog
	for _, it := range page.Items {
		if it.RequestID == rid {
			out = append(out, it.GatewayUsageLog)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Attempt < out[j].Attempt })
	return out
}

type trailStep struct{ reason, action string }

func assertTrail(t *testing.T, got []storage.GatewayUsageLog, want []trailStep) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("attempts = %d, want %d: %+v", len(got), len(want), got)
	}
	for i, w := range want {
		if got[i].FailoverReason != w.reason || got[i].NextAction != w.action {
			t.Fatalf("attempt %d: reason=%q action=%q, want %q %q", i+1, got[i].FailoverReason, got[i].NextAction, w.reason, w.action)
		}
		if w.action != "" && !strings.Contains(got[i].ErrorDetail, "next_action: "+w.action) {
			t.Fatalf("attempt %d detail missing next_action: %s", i+1, got[i].ErrorDetail)
		}
		if strings.Contains(got[i].ErrorDetail, "failover_decision") {
			t.Fatalf("attempt %d still has the old decision line: %s", i+1, got[i].ErrorDetail)
		}
	}
}

// 签名错误：只记「去掉 thinking 块后同路由重试」，不再同时记一行 http_400。
func TestRetryTrail_ThinkingSignature(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		if strings.Contains(string(body), `"signature"`) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"type":"error","error":{"type":"invalid_request_error","message":"messages.1.content.0: Invalid `+"`signature`"+` in `+"`thinking`"+` block"}}`)
			return
		}
		_, _ = io.WriteString(w, anthropicOKJSON)
	})
	h := newE2E(t, func(g *storage.GatewayGroup) { g.FailoverOn4xx = true }, e2eRouteSpec{Upstream: up, Protocol: "anthropic"})
	if w := h.do(http.MethodPost, "/v1/messages", claudeHistoryWithThinking, nil); w.Code != http.StatusOK {
		t.Fatalf("status=%d", w.Code)
	}
	assertTrail(t, requestTrail(t, h), []trailStep{
		{failoverReasonThinkingSignature, nextActionRetryWithoutThinking},
		{"", ""},
	})
}

// 余额不足：路由冷却并换下一条路由；只剩这一条时冷却后把错误回给客户端。
func TestRetryTrail_AccountFailure(t *testing.T) {
	a := newMockUpstream(t, balance403)
	b := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, chatCompletionJSON)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: a, Protocol: "openai_chat", Weight: 10}, e2eRouteSpec{Upstream: b, Protocol: "openai_chat", Weight: 1})
	if w := h.do(http.MethodPost, "/v1/chat/completions", `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`, nil); w.Code != http.StatusOK {
		t.Fatalf("status=%d", w.Code)
	}
	assertTrail(t, requestTrail(t, h), []trailStep{
		{accountFailureReasonBalance, nextActionCooldown + "," + nextActionSwitchRoute},
		{"", ""},
	})

	single := newE2E(t, nil, e2eRouteSpec{Upstream: newMockUpstream(t, balance403), Protocol: "openai_chat"})
	if w := single.do(http.MethodPost, "/v1/chat/completions", `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`, nil); w.Code != http.StatusForbidden {
		t.Fatalf("single route status=%d", w.Code)
	}
	assertTrail(t, requestTrail(t, single), []trailStep{
		{accountFailureReasonBalance, nextActionCooldown + "," + nextActionGiveUp},
	})
}

// 同路由重试：502 先在原路由重试，重试仍失败、组冷却开着时冷却后换路由。
func TestRetryTrail_SameRouteRetryThenSwitch(t *testing.T) {
	a := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(w, `{"error":{"message":"bad gateway"}}`)
	})
	b := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, chatCompletionJSON)
	})
	h := newE2E(t, func(g *storage.GatewayGroup) { g.RetryCount = 1 },
		e2eRouteSpec{Upstream: a, Protocol: "openai_chat", Weight: 10}, e2eRouteSpec{Upstream: b, Protocol: "openai_chat", Weight: 1})
	if w := h.do(http.MethodPost, "/v1/chat/completions", `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`, nil); w.Code != http.StatusOK {
		t.Fatalf("status=%d", w.Code)
	}
	assertTrail(t, requestTrail(t, h), []trailStep{
		{"http_502", nextActionRetrySameRoute},
		{"http_502", nextActionCooldown + "," + nextActionSwitchRoute},
		{"", ""},
	})
}

// 不可重试（上下文超长）：错误直接回给客户端。
func TestRetryTrail_Stop(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"error":{"message":"prompt is too long: 1161691 tokens > 1000000 maximum"}}`)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "openai_chat"})
	if w := h.do(http.MethodPost, "/v1/chat/completions", `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`, nil); w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d", w.Code)
	}
	assertTrail(t, requestTrail(t, h), []trailStep{{"context_length", nextActionStop}})
}
