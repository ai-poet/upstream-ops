package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/coder/websocket"
)

// Responses WebSocket：选 Responses 协议的路由、按映射改写模型、上游事件原样回给客户端、
// 终态事件时按上游用量记一条日志。
func TestE2E_ResponsesWebSocket(t *testing.T) {
	var (
		mu       sync.Mutex
		upFirst  []byte
		upHeader http.Header
	)
	upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		ctx := r.Context()
		_, first, err := conn.Read(ctx)
		if err != nil {
			return
		}
		mu.Lock()
		upFirst, upHeader = first, r.Header.Clone()
		mu.Unlock()
		_ = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"response.created","response":{"id":"resp_1"}}`))
		_ = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"response.completed","response":{"id":"resp_1","usage":{"input_tokens":11,"output_tokens":7,"total_tokens":18}}}`))
		_, _, _ = conn.Read(ctx) // 等客户端关闭
	}))
	t.Cleanup(upSrv.Close)
	h := newE2E(t, nil, e2eRouteSpec{
		Upstream: &mockUpstream{Server: upSrv},
		Protocol: "openai_responses",
		Mapping:  `{"gpt-x":"gpt-up"}`,
	})
	gw := httptest.NewServer(h.engine)
	t.Cleanup(gw.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(gw.URL, "http")+"/v1/responses", &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": []string{"Bearer " + h.key}},
	})
	if err != nil {
		t.Fatalf("dial gateway: %v", err)
	}
	defer conn.CloseNow()
	if err := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"response.create","model":"gpt-x","input":"hi"}`)); err != nil {
		t.Fatal(err)
	}
	var types []string
	for {
		_, msg, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("read: %v (got %v)", err, types)
		}
		var ev struct {
			Type string `json:"type"`
		}
		_ = json.Unmarshal(msg, &ev)
		types = append(types, ev.Type)
		if ev.Type == "response.completed" || ev.Type == "response.failed" {
			break
		}
	}
	_ = conn.Close(websocket.StatusNormalClosure, "")
	if strings.Join(types, ",") != "response.created,response.completed" {
		t.Fatalf("client events = %v", types)
	}

	mu.Lock()
	first, header := upFirst, upHeader
	mu.Unlock()
	var create struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(first, &create); err != nil || create.Model != "gpt-up" {
		t.Fatalf("upstream response.create = %s, want model gpt-up", first)
	}
	if got := header.Get("Authorization"); got != "Bearer sk-up-a" {
		t.Fatalf("upstream Authorization = %q", got)
	}

	// 用量在网关收到终态事件后写入，可能比客户端读到终态稍晚
	var item storage.GatewayUsageLogItem
	deadline := time.Now().Add(3 * time.Second)
	for {
		page, err := h.svc.Usage.List(storage.GatewayUsageQuery{Page: 1, PageSize: 10})
		if err == nil && page != nil && len(page.Items) > 0 {
			item = page.Items[0]
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no usage log written: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !item.Success || item.StatusCode != http.StatusOK || !item.Stream {
		t.Fatalf("usage log status: %+v", item.GatewayUsageLog)
	}
	if item.InputTokens != 11 || item.OutputTokens != 7 {
		t.Fatalf("usage tokens: input=%d output=%d", item.InputTokens, item.OutputTokens)
	}
	if item.RequestedModel != "gpt-x" || item.UpstreamModel != "gpt-up" || item.InboundEndpoint != "/v1/responses" ||
		item.UpstreamProtocol != "openai_responses" || item.RouteID != h.routes[0].ID || item.Attempt != 1 {
		t.Fatalf("usage log fields: %+v", item.GatewayUsageLog)
	}
}

// 组里没有 Responses 协议的路由：回 response.failed，不记用量。
func TestE2E_ResponsesWebSocketNoNativeRoute(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "openai_chat"})
	gw := httptest.NewServer(h.engine)
	t.Cleanup(gw.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(gw.URL, "http")+"/v1/responses", &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": []string{"Bearer " + h.key}},
	})
	if err != nil {
		t.Fatalf("dial gateway: %v", err)
	}
	defer conn.CloseNow()
	if err := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"response.create","model":"gpt-x","input":"hi"}`)); err != nil {
		t.Fatal(err)
	}
	_, msg, err := conn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), `"response.failed"`) || !strings.Contains(string(msg), "no native Responses WebSocket route") {
		t.Fatalf("got %s", msg)
	}
	if n, _ := h.svc.Usage.CountAll(); n != 0 {
		t.Fatalf("usage rows = %d, want 0", n)
	}
}
