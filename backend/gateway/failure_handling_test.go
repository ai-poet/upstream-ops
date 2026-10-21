package gateway

import (
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/tidwall/gjson"
)

// 失败分类：哪些在原路由重试、哪些直接换路由、哪些让路由冷却（数据见线上 2026-10-21 的 24 小时统计）。
func TestClassifyFailure_SwitchVersusRetry(t *testing.T) {
	rt := NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil).Runtime
	f4 := &storage.GatewayGroup{FailoverOn4xx: true}
	plain := &storage.GatewayGroup{}
	cases := []struct {
		name        string
		group       *storage.GatewayGroup
		status      int
		body        string
		action      failureAction
		reason      string
		cooldown    bool
		channelWide bool
	}{
		{"sub2api balance 403", f4, 403, `{"error":{"type":"api_error","message":"Insufficient account balance"}}`, failSwitchRoute, accountFailureReasonBalance, true, true},
		{"balance switches even without 4xx failover", plain, 403, `{"error":{"message":"Insufficient user balance. Please top up your user balance."}}`, failSwitchRoute, accountFailureReasonBalance, true, true},
		{"bilingual balance", f4, 403, `{"error":{"message":"账户余额不足 / Insufficient account balance"}}`, failSwitchRoute, accountFailureReasonBalance, true, true},
		{"wallet quota", plain, 403, `{"error":{"message":"wallet quota insufficient"}}`, failSwitchRoute, accountFailureReasonBalance, true, true},
		{"no access to group", f4, 403, `{"error":{"message":"无权访问 gpt-image2 分组 (request id: x)"}}`, failSwitchRoute, accountFailureReasonForbidden, true, false},
		{"invalid key 401", f4, 401, `{"error":{"message":"invalid api key"}}`, failSwitchRoute, accountFailureReasonAuth, true, false},
		{"payment required 402", plain, 402, `{"error":{"message":"pay up"}}`, failSwitchRoute, accountFailureReasonBalance, true, true},
		// 请求级拒绝不处罚路由：换路由（开了 4xx 顺延时），但不冷却、不在原路由重试
		{"cyber policy block", f4, 403, `{"error":{"message":"该会话已被网络安全策略屏蔽，请开启新会话 / This session is blocked by cyber-security policy"}}`, failSwitchRoute, "http_403", false, false},
		{"cyber policy without 4xx failover stops", plain, 403, `{"error":{"message":"This session is blocked by cyber-security policy"}}`, failStop, "http_403", false, false},
		{"cloudflare html 403", f4, 403, "<!doctype html>\n<html><head><title>Attention Required</title></head><body>blocked</body></html>", failSwitchRoute, "http_403_html", false, false},
		{"thinking signature 400", f4, 400, `{"type":"error","error":{"type":"invalid_request_error","message":"messages.1.content.0: Invalid ` + "`signature`" + ` in ` + "`thinking`" + ` block"}}`, failSwitchRoute, "http_400", false, false},
		{"quoted echo does not fake a balance error", f4, 400, `{"error":{"message":"Invalid value: 'insufficient balance'"}}`, failSwitchRoute, "http_400", false, false},
		// 同路由重试有用的仍然重试
		{"502 retried", f4, 502, `{"error":{"message":"bad gateway"}}`, failRetry, "http_502", false, false},
		{"html 524 retried", f4, 524, "<!DOCTYPE html><html><body>timeout</body></html>", failRetry, "http_524", false, false},
		{"concurrency 429 retried", f4, 429, `{"error":{"message":"Concurrency limit exceeded for account, please retry later"}}`, failRetry, "http_429", false, false},
		{"explicit retry codes keep same-route retry", &storage.GatewayGroup{RetryStatusCodes: "400,429,500-599"}, 400, `{"error":{"message":"flaky"}}`, failRetry, "http_400", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := rt.classifyFailure(tc.group, tc.status, nil, []byte(tc.body), nil, false, "claude-opus-5-5")
			if d.Action != tc.action || d.Reason != tc.reason {
				t.Fatalf("decision %+v, want action %d reason %q", d, tc.action, tc.reason)
			}
			if (d.Cooldown == accountFailureCooldown) != tc.cooldown || d.ChannelWide != tc.channelWide {
				t.Fatalf("decision %+v, want cooldown=%v channelWide=%v", d, tc.cooldown, tc.channelWide)
			}
		})
	}
}

// 账号级失败：180 分钟内第 3 次才停用；3 秒内并发撞上的只算一次；窗口外的不算。
func TestRouteHealth_AccountStrikes(t *testing.T) {
	var h routeHealth
	t0 := time.Now()
	if n := h.noteAccountFailure(7, t0); n != 1 {
		t.Fatalf("first strike = %d", n)
	}
	if n := h.noteAccountFailure(7, t0.Add(time.Second)); n != 1 {
		t.Fatalf("burst should collapse, got %d", n)
	}
	if n := h.noteAccountFailure(7, t0.Add(11*time.Minute)); n != 2 {
		t.Fatalf("second strike = %d", n)
	}
	if n := h.noteAccountFailure(7, t0.Add(185*time.Minute)); n != 2 {
		t.Fatalf("strike outside window should drop the oldest, got %d", n)
	}
	if n := h.noteAccountFailure(7, t0.Add(190*time.Minute)); n != 3 {
		t.Fatalf("third strike = %d", n)
	}
}

func balance403(w http.ResponseWriter, _ *http.Request, _ []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusForbidden)
	_, _ = io.WriteString(w, `{"error":{"type":"api_error","message":"Insufficient account balance"}}`)
}

// 余额耗尽的路由：只打一次就换路由，冷却 10 分钟（组冷却为 0 也一样），下一个请求不再碰它。
func TestE2E_AccountFailureSwitchesAndCoolsRoute(t *testing.T) {
	a := newMockUpstream(t, balance403)
	b := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, chatCompletionJSON)
	})
	h := newE2E(t, func(g *storage.GatewayGroup) {
		g.FailoverOn4xx = true
		g.RetryCount = 3
	}, e2eRouteSpec{Upstream: a, Protocol: "openai_chat", Weight: 10}, e2eRouteSpec{Upstream: b, Protocol: "openai_chat", Weight: 1})
	// 线上 codex pro 等组的冷却是 0（建组时 0 会被列默认值 30 顶掉，这里建好后再改）
	h.group.CooldownSeconds = 0
	if err := h.svc.Groups.Update(h.group); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		w := h.do(http.MethodPost, "/v1/chat/completions", `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`, nil)
		if w.Code != http.StatusOK {
			t.Fatalf("request %d: status=%d body=%s", i, w.Code, w.Body.String())
		}
	}
	if n := len(a.Calls()); n != 1 {
		t.Fatalf("balance-exhausted route called %d times, want 1 (no same-route retry, then cooled)", n)
	}
	r := h.route(0)
	if r.TempUnschedulableUntil == nil || time.Until(*r.TempUnschedulableUntil) < 9*time.Minute {
		t.Fatalf("want ~10min cooldown, got %v", r.TempUnschedulableUntil)
	}
}

// 余额属于上游账号：同一直连渠道下的其它路由（含别的组）一起冷却。
func TestSetTempUnschedulableForSource(t *testing.T) {
	db := openGatewayTestDB(t)
	routes := storage.NewGatewayRoutes(db)
	list := []storage.GatewayRoute{
		{GatewayGroupID: 1, Position: 0, SourceKind: storage.GatewayRouteSourceProvider, GatewayProviderID: 5, Enabled: true},
		{GatewayGroupID: 2, Position: 0, SourceKind: storage.GatewayRouteSourceProvider, GatewayProviderID: 5, Enabled: true},
		{GatewayGroupID: 2, Position: 1, SourceKind: storage.GatewayRouteSourceProvider, GatewayProviderID: 6, Enabled: true},
		{GatewayGroupID: 3, Position: 0, SourceKind: storage.GatewayRouteSourceMonitor, SourceChannelID: 5, Enabled: true},
	}
	if err := routes.SaveForGroup(1, list[:1]); err != nil {
		t.Fatal(err)
	}
	if err := routes.SaveForGroup(2, list[1:3]); err != nil {
		t.Fatal(err)
	}
	if err := routes.SaveForGroup(3, list[3:]); err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(10 * time.Minute)
	n, err := routes.SetTempUnschedulableForSource(storage.GatewayRouteSourceProvider, 5, until, "balance", time.Now(), "rid")
	if err != nil || n != 2 {
		t.Fatalf("affected=%d err=%v, want 2", n, err)
	}
	for _, g := range []uint{1, 2, 3} {
		saved, _ := routes.ListByGroupID(g)
		for _, r := range saved {
			paused := r.TempUnschedulableUntil != nil
			want := r.SourceKind == storage.GatewayRouteSourceProvider && r.GatewayProviderID == 5
			if paused != want {
				t.Fatalf("route %+v paused=%v want %v", r, paused, want)
			}
		}
	}
	// 已经冷却得更久的不缩短
	if n, _ := routes.SetTempUnschedulableForSource(storage.GatewayRouteSourceProvider, 5, time.Now().Add(time.Minute), "x", time.Now(), "rid"); n != 0 {
		t.Fatalf("shorter pause overwrote %d routes", n)
	}
}

const claudeHistoryWithThinking = `{"model":"claude-opus-5-5","max_tokens":64,"thinking":{"type":"adaptive"},
"metadata":{"user_id":"{\"device_id\":\"d1\",\"account_uuid\":\"a1\",\"session_id\":\"6f1c2b9e-0000-4000-8000-000000000001\"}"},
"messages":[{"role":"user","content":"hi"},
{"role":"assistant","content":[{"type":"thinking","thinking":"let me think","signature":"sig-from-other-upstream"},{"type":"redacted_thinking","data":"xx"},{"type":"text","text":"hello"}]},
{"role":"user","content":"again"}]}`

const anthropicOKJSON = `{"id":"msg_1","type":"message","role":"assistant","model":"claude-opus-5-5","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":3,"output_tokens":1}}`

// thinking 签名错误：去掉 thinking 块在同一路由补试一次（同 sub2api FilterThinkingBlocksForRetry）。
func TestE2E_ThinkingSignatureRetriedWithoutThinking(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		if strings.Contains(string(body), `"signature"`) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"type":"error","error":{"type":"invalid_request_error","message":"messages.1.content.0: Invalid `+"`signature`"+` in `+"`thinking`"+` block"}}`)
			return
		}
		_, _ = io.WriteString(w, anthropicOKJSON)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "anthropic"})
	w := h.do(http.MethodPost, "/v1/messages", claudeHistoryWithThinking, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	calls := up.Calls()
	if len(calls) != 2 {
		t.Fatalf("upstream calls = %d, want 2", len(calls))
	}
	retry := calls[1].Body
	if gjson.GetBytes(retry, "thinking").Exists() {
		t.Fatalf("top-level thinking kept: %s", retry)
	}
	content := gjson.GetBytes(retry, "messages.1.content")
	if content.Get("#(type==\"thinking\")").Exists() || content.Get("#(type==\"redacted_thinking\")").Exists() {
		t.Fatalf("thinking blocks kept: %s", content.Raw)
	}
	if content.Get("0.text").String() != "let me think" || content.Get("1.text").String() != "hello" {
		t.Fatalf("thinking not converted to text: %s", content.Raw)
	}
}

// 会话粘性：一轮在 B 上成功后，同一会话下一轮先回 B（即使 A 已恢复）；别的会话照常按优先级先走 A。
func TestE2E_SessionStaysOnRouteThatServedIt(t *testing.T) {
	var aDown atomic.Bool
	aDown.Store(true)
	a := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		if aDown.Load() {
			// 换路由但不冷却的失败（参数类 4xx），A 之后仍可调度
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"error":{"message":"temporarily rejected"}}`)
			return
		}
		_, _ = io.WriteString(w, chatCompletionJSON)
	})
	b := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, chatCompletionJSON)
	})
	h := newE2E(t, func(g *storage.GatewayGroup) { g.FailoverOn4xx = true },
		e2eRouteSpec{Upstream: a, Protocol: "openai_chat", Weight: 10}, e2eRouteSpec{Upstream: b, Protocol: "openai_chat", Weight: 1})
	body := `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`
	session := map[string]string{"session_id": "conv-1"}

	if w := h.do(http.MethodPost, "/v1/chat/completions", body, session); w.Code != http.StatusOK {
		t.Fatalf("first turn status=%d", w.Code)
	}
	aDown.Store(false)
	aBefore := len(a.Calls())
	if w := h.do(http.MethodPost, "/v1/chat/completions", body, session); w.Code != http.StatusOK {
		t.Fatalf("second turn status=%d", w.Code)
	}
	if len(a.Calls()) != aBefore || len(b.Calls()) != 2 {
		t.Fatalf("second turn left the session's route: a=%d b=%d", len(a.Calls())-aBefore, len(b.Calls()))
	}
	if w := h.do(http.MethodPost, "/v1/chat/completions", body, map[string]string{"session_id": "conv-2"}); w.Code != http.StatusOK {
		t.Fatalf("other session status=%d", w.Code)
	}
	if len(a.Calls()) != aBefore+1 {
		t.Fatalf("another session should start on the priority route: a=%d (before %d) b=%d", len(a.Calls()), aBefore, len(b.Calls()))
	}
}

func TestSessionKeyFor(t *testing.T) {
	h := http.Header{}
	claude := []byte(`{"metadata":{"user_id":"user_` + strings.Repeat("a", 64) + `_account_b_session_6f1c2b9e-0000-4000-8000-000000000001"},"messages":[{"role":"user","content":"x"}]}`)
	claudeJSON := []byte(`{"metadata":{"user_id":"{\"device_id\":\"d\",\"session_id\":\"6f1c2b9e-0000-4000-8000-000000000001\"}"},"messages":[{"role":"user","content":"y"}]}`)
	if k1, k2 := sessionKeyFor(h, claude), sessionKeyFor(h, claudeJSON); k1 == "" || k1 != k2 {
		t.Fatalf("legacy and JSON metadata.user_id with the same session should match: %q %q", k1, k2)
	}
	turn1 := []byte(`{"model":"gpt-x","instructions":"sys","input":[{"role":"user","content":"start"}]}`)
	turn2 := []byte(`{"model":"gpt-x","instructions":"sys","input":[{"role":"user","content":"start"},{"role":"assistant","content":"ok"},{"role":"user","content":"more"}]}`)
	if k1, k2 := sessionKeyFor(h, turn1), sessionKeyFor(h, turn2); k1 == "" || k1 != k2 {
		t.Fatalf("content fallback should be stable across turns: %q %q", k1, k2)
	}
	if sessionKeyFor(h, []byte(`{"model":"gpt-x"}`)) != "" {
		t.Fatal("no user message should give no session")
	}
	hh := http.Header{}
	hh.Set("session_id", "abc")
	if sessionKeyFor(hh, turn1) == sessionKeyFor(h, turn1) {
		t.Fatal("explicit session header should win over content")
	}
}

// 路由回「模型不存在」后，同模型的下一个请求把它排到最后，不再先去碰。
func TestE2E_ModelGapDemotesRoute(t *testing.T) {
	a := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"error":{"message":"Model \"gpt-x\" is not supported by any configured account in this group"}}`)
	})
	b := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, chatCompletionJSON)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: a, Protocol: "openai_chat", Weight: 10}, e2eRouteSpec{Upstream: b, Protocol: "openai_chat", Weight: 1})
	for i := 0; i < 3; i++ {
		// 每次换个会话，免得会话粘性掩盖了后排
		hdr := map[string]string{"session_id": "s" + string(rune('a'+i))}
		if w := h.do(http.MethodPost, "/v1/chat/completions", `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`, hdr); w.Code != http.StatusOK {
			t.Fatalf("status=%d", w.Code)
		}
	}
	if n := len(a.Calls()); n != 1 {
		t.Fatalf("route without the model called %d times, want 1", n)
	}
}

// 流式不设总时长：转发超时只管等第一段数据，之后持续出数据的长流不会被掐断。
func TestE2E_LongStreamOutlivesForwardTimeout(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		for i := 0; i < 6; i++ {
			_, _ = io.WriteString(w, `data: {"id":"c","object":"chat.completion.chunk","model":"gpt-x","choices":[{"index":0,"delta":{"content":"x"}}]}`+"\n\n")
			fl.Flush()
			time.Sleep(300 * time.Millisecond)
		}
		_, _ = io.WriteString(w, `data: {"id":"c","object":"chat.completion.chunk","model":"gpt-x","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`+"\n\n"+"data: [DONE]\n\n")
		fl.Flush()
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "openai_chat"})
	setGatewayConfig(h, func(c *config.GatewayConfig) { c.ForwardTimeoutSeconds = 1 })
	w := h.do(http.MethodPost, "/v1/chat/completions", `{"model":"gpt-x","stream":true,"messages":[{"role":"user","content":"hi"}]}`, nil)
	out := w.Body.String()
	if w.Code != http.StatusOK || !strings.Contains(out, "[DONE]") || strings.Contains(out, "stream_timeout") {
		t.Fatalf("long stream was cut: status=%d body=%s", w.Code, out)
	}
	if n := strings.Count(out, `"content":"x"`); n != 6 {
		t.Fatalf("got %d chunks, want 6", n)
	}
}

// 180 分钟内第 3 次账号级失败：路由自动禁用，交给后台恢复探测（同 sub2api 403 连续 3 次停用）。
func TestE2E_RepeatedAccountFailureAutoDisables(t *testing.T) {
	a := newMockUpstream(t, balance403)
	b := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, chatCompletionJSON)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: a, Protocol: "openai_chat", Weight: 10}, e2eRouteSpec{Upstream: b, Protocol: "openai_chat", Weight: 1})
	routeA := h.routes[0].ID
	now := time.Now()
	h.svc.routeHealth.noteAccountFailure(routeA, now.Add(-40*time.Minute))
	h.svc.routeHealth.noteAccountFailure(routeA, now.Add(-20*time.Minute))
	if w := h.do(http.MethodPost, "/v1/chat/completions", `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`, nil); w.Code != http.StatusOK {
		t.Fatalf("status=%d", w.Code)
	}
	if r := h.route(0); !r.AutoDisabled {
		t.Fatalf("route not auto-disabled after the third account failure: %+v", r)
	}
}
