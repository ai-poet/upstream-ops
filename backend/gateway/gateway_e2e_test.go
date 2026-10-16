package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

// recordedCall 记录 mock 上游收到的请求。
type recordedCall struct {
	Method string
	Path   string // 含查询串
	Header http.Header
	Body   []byte
}

type mockUpstream struct {
	*httptest.Server
	mu    sync.Mutex
	calls []recordedCall
}

func newMockUpstream(t *testing.T, handler func(w http.ResponseWriter, r *http.Request, body []byte)) *mockUpstream {
	t.Helper()
	m := &mockUpstream{}
	m.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		m.mu.Lock()
		m.calls = append(m.calls, recordedCall{Method: r.Method, Path: r.URL.RequestURI(), Header: r.Header.Clone(), Body: body})
		m.mu.Unlock()
		handler(w, r, body)
	}))
	t.Cleanup(m.Close)
	return m
}

func (m *mockUpstream) Calls() []recordedCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]recordedCall(nil), m.calls...)
}

type e2eRouteSpec struct {
	Upstream *mockUpstream
	Protocol string
	Weight   int // 优先级
	Mapping  string
}

type e2eHarness struct {
	t      *testing.T
	engine *gin.Engine
	key    string
	svc    *Service
	group  *storage.GatewayGroup
	routes []storage.GatewayRoute
	repo   *storage.GatewayRoutes
}

func newE2E(t *testing.T, mutate func(g *storage.GatewayGroup), specs ...e2eRouteSpec) *e2eHarness {
	t.Helper()
	gin.SetMode(gin.TestMode)
	db := openGatewayTestDB(t)
	groups := storage.NewGatewayGroups(db)
	keys := storage.NewGatewayKeys(db)
	routes := storage.NewGatewayRoutes(db)
	providers := storage.NewGatewayProviders(db)
	usage := storage.NewGatewayUsageLogs(db)
	cipher, err := crypto.NewCipher("test-secret-e2e")
	if err != nil {
		t.Fatal(err)
	}
	g := &storage.GatewayGroup{
		Name:              "e2e",
		Status:            storage.GatewayGroupStatusActive,
		ModelsMode:        storage.GatewayModelsModeManual,
		RateSortDirection: "asc",
		RetryEnabled:      true,
		FailoverEnabled:   true,
		FailoverMax:       8,
		CooldownSeconds:   30,
		ModelAwareRouting: true,
		LoadBalanceMode:   LoadBalanceOrdered,
	}
	if mutate != nil {
		mutate(g)
	}
	if err := groups.Create(g); err != nil {
		t.Fatal(err)
	}
	var list []storage.GatewayRoute
	for i, spec := range specs {
		secret, _ := cipher.Encrypt("sk-up-" + string(rune('a'+i)))
		p := &storage.GatewayProvider{
			Name: "p" + string(rune('a'+i)), BaseURL: spec.Upstream.URL, APIKeyCipher: secret,
			UpstreamProtocol: spec.Protocol, AuthStyle: storage.GatewayProviderAuthBoth, Enabled: true,
		}
		if p.UpstreamProtocol == "" {
			p.UpstreamProtocol = storage.GatewayUpstreamProtocolAuto
		}
		if err := providers.Create(p); err != nil {
			t.Fatal(err)
		}
		w := spec.Weight
		if w == 0 {
			w = 1
		}
		list = append(list, storage.GatewayRoute{
			GatewayGroupID: g.ID, Position: i, SourceKind: storage.GatewayRouteSourceProvider,
			GatewayProviderID: p.ID, Weight: w, Enabled: true, RateConvertMode: "custom", RateConvertValue: 1,
			UpstreamProtocol: storage.GatewayUpstreamProtocolAuto, ModelMappingJSON: spec.Mapping,
		})
	}
	if err := routes.SaveForGroup(g.ID, list); err != nil {
		t.Fatal(err)
	}
	saved, _ := routes.ListByGroupID(g.ID)

	clientKey := "sk-e2e-client-key"
	kc, _ := cipher.Encrypt(clientKey)
	if err := keys.Create(&storage.GatewayKey{
		GroupID: g.ID, Name: "c", KeyHash: HashAPIKey(clientKey), KeyPrefix: KeyPrefix(clientKey),
		KeyCipher: kc, Status: storage.GatewayKeyStatusActive,
	}); err != nil {
		t.Fatal(err)
	}
	svc := NewService(groups, keys, routes, usage, nil, nil, nil, cipher, nil)
	svc.SetProviders(providers)
	engine := gin.New()
	RegisterPublicWithoutRoot(engine, svc)
	return &e2eHarness{t: t, engine: engine, key: clientKey, svc: svc, group: g, routes: saved, repo: routes}
}

func (h *e2eHarness) do(method, path, body string, header map[string]string) *httptest.ResponseRecorder {
	h.t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if _, ok := header["x-goog-api-key"]; !ok {
		req.Header.Set("Authorization", "Bearer "+h.key)
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.engine.ServeHTTP(w, req)
	return w
}

func (h *e2eHarness) route(i int) storage.GatewayRoute {
	h.t.Helper()
	r, err := h.repo.FindByID(h.routes[i].ID)
	if err != nil {
		h.t.Fatal(err)
	}
	return *r
}

func writeSSE(w http.ResponseWriter, frames ...string) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	for _, f := range frames {
		_, _ = io.WriteString(w, f)
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
	}
}

const chatCompletionJSON = `{"id":"chatcmpl-1","object":"chat.completion","created":1,"model":"gpt-x","choices":[{"index":0,"message":{"role":"assistant","content":"hello there"},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7}}`

func TestE2E_GeminiInboundToChatUpstream(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		if bytes.Contains(body, []byte(`"stream":true`)) {
			writeSSE(w,
				`data: {"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","content":"Hel"}}]}`+"\n\n",
				`data: {"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"lo"},"finish_reason":"stop"}]}`+"\n\n",
				`data: {"id":"c1","object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`+"\n\n",
				"data: [DONE]\n\n",
			)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, chatCompletionJSON)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: storage.GatewayUpstreamProtocolOpenAIChat})
	req := `{"systemInstruction":{"parts":[{"text":"sys"}]},"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`

	t.Run("generateContent", func(t *testing.T) {
		w := h.do(http.MethodPost, "/v1beta/models/gpt-x:generateContent", req, map[string]string{"x-goog-api-key": h.key})
		if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"candidates"`) || !strings.Contains(w.Body.String(), "hello there") {
			t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
		}
		last := up.Calls()[len(up.Calls())-1]
		if last.Path != "/v1/chat/completions" || !bytes.Contains(last.Body, []byte(`"model":"gpt-x"`)) {
			t.Fatalf("upstream call %s %s", last.Path, last.Body)
		}
		if bytes.Contains(last.Body, []byte("reasoning_effort")) {
			t.Fatalf("defaulted reasoning leaked to chat upstream: %s", last.Body)
		}
	})
	t.Run("streamGenerateContent alt=sse", func(t *testing.T) {
		w := h.do(http.MethodPost, "/v1beta/models/gpt-x:streamGenerateContent?alt=sse", req, map[string]string{"x-goog-api-key": h.key})
		out := w.Body.String()
		if w.Code != http.StatusOK || !strings.Contains(out, "data: {") || !strings.Contains(out, `"finishReason":"STOP"`) {
			t.Fatalf("status=%d body=%s", w.Code, out)
		}
		if strings.Contains(out, "[DONE]") || strings.Contains(out, "chat.completion") {
			t.Fatalf("chat frames leaked: %s", out)
		}
	})
	t.Run("streamGenerateContent json array with ?key=", func(t *testing.T) {
		req2 := httptest.NewRequest(http.MethodPost, "/v1beta/models/gpt-x:streamGenerateContent?key="+h.key, strings.NewReader(req))
		req2.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		h.engine.ServeHTTP(w, req2)
		var arr []map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &arr); err != nil || len(arr) == 0 {
			t.Fatalf("not a JSON array (%v): %s", err, w.Body.String())
		}
		last := up.Calls()[len(up.Calls())-1]
		if strings.Contains(last.Path, "key=") {
			t.Fatalf("gateway key leaked upstream: %s", last.Path)
		}
	})
	t.Run("countTokens falls back to estimate", func(t *testing.T) {
		w := h.do(http.MethodPost, "/v1beta/models/gpt-x:countTokens", req, map[string]string{"x-goog-api-key": h.key})
		if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "totalTokens") {
			t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
		}
	})
	t.Run("auth error is google shaped", func(t *testing.T) {
		w := h.do(http.MethodPost, "/v1beta/models/gpt-x:generateContent", req, map[string]string{"x-goog-api-key": "wrong"})
		if w.Code != http.StatusUnauthorized || !strings.Contains(w.Body.String(), `"status":"UNAUTHENTICATED"`) {
			t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
		}
	})
}

func TestE2E_ChatInboundToGeminiUpstreamStream(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		writeSSE(w,
			`data: {"candidates":[{"content":{"role":"model","parts":[{"text":"AB"}]}}]}`+"\r\n\r\n",
			`data: {"candidates":[{"content":{"role":"model","parts":[{"text":"CD"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":4,"candidatesTokenCount":2,"thoughtsTokenCount":1,"totalTokenCount":7}}`+"\r\n\r\n",
		)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: storage.GatewayUpstreamProtocolGemini})
	w := h.do(http.MethodPost, "/v1/chat/completions", `{"model":"gemini-2.5-flash","stream":true,"messages":[{"role":"user","content":"hi"}]}`, nil)
	out := w.Body.String()
	if w.Code != http.StatusOK || !strings.Contains(out, "chat.completion.chunk") || !strings.Contains(out, "AB") || !strings.Contains(out, "[DONE]") {
		t.Fatalf("status=%d body=%s", w.Code, out)
	}
	call := up.Calls()[0]
	if call.Path != "/v1beta/models/gemini-2.5-flash:streamGenerateContent?alt=sse" {
		t.Fatalf("upstream path %s", call.Path)
	}
	if call.Header.Get("x-goog-api-key") == "" {
		t.Fatalf("missing x-goog-api-key header")
	}
	if call.Header.Get("Authorization") != "" {
		t.Fatalf("gemini upstream must not get a Bearer header by default: %q", call.Header.Get("Authorization"))
	}
	if bytes.Contains(call.Body, []byte(`"model"`)) {
		t.Fatalf("gemini body must not carry model: %s", call.Body)
	}
	page, err := h.svc.Usage.List(storage.GatewayUsageQuery{Page: 1, PageSize: 10})
	if err != nil || page == nil || len(page.Items) == 0 {
		t.Fatalf("usage logs: %v %+v", err, page)
	}
	if got := page.Items[0]; !got.Success || got.OutputTokens != 3 || got.InputTokens != 4 {
		t.Fatalf("usage log: %+v", got)
	}
}

func TestE2E_ModelNotFoundSkipsWithoutCooldown(t *testing.T) {
	a := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `{"error":{"message":"当前分组 default 下对于模型 gpt-x 无可用渠道","type":"new_api_error"}}`)
	})
	b := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, chatCompletionJSON)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: a, Protocol: "openai_chat"}, e2eRouteSpec{Upstream: b, Protocol: "openai_chat"})
	w := h.do(http.MethodPost, "/v1/chat/completions", `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if r := h.route(0); r.TempUnschedulableUntil != nil {
		t.Fatalf("model-unavailable route must not be cooled down: %v", r.TempUnschedulableUntil)
	}
}

func TestE2E_PlainFourHundredNotFailedOverByDefault(t *testing.T) {
	a := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"error":{"message":"temperature out of range","type":"invalid_request_error"}}`)
	})
	b := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, chatCompletionJSON)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: a, Protocol: "openai_chat"}, e2eRouteSpec{Upstream: b, Protocol: "openai_chat"})
	w := h.do(http.MethodPost, "/v1/chat/completions", `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`, nil)
	if w.Code != http.StatusBadRequest || len(b.Calls()) != 0 {
		t.Fatalf("status=%d b_calls=%d body=%s", w.Code, len(b.Calls()), w.Body.String())
	}
}

func TestE2E_ContextLengthNeverFailsOver(t *testing.T) {
	a := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"error":{"message":"This model's maximum context length is 128000 tokens","code":"context_length_exceeded"}}`)
	})
	b := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, chatCompletionJSON)
	})
	h := newE2E(t, func(g *storage.GatewayGroup) { g.FailoverOn4xx = true },
		e2eRouteSpec{Upstream: a, Protocol: "openai_chat"}, e2eRouteSpec{Upstream: b, Protocol: "openai_chat"})
	w := h.do(http.MethodPost, "/v1/chat/completions", `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`, nil)
	if w.Code != http.StatusBadRequest || len(b.Calls()) != 0 {
		t.Fatalf("status=%d b_calls=%d", w.Code, len(b.Calls()))
	}
}

func TestE2E_RetryAfterExtendsCooldown(t *testing.T) {
	a := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":{"message":"rate limited"}}`)
	})
	b := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, chatCompletionJSON)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: a, Protocol: "openai_chat"}, e2eRouteSpec{Upstream: b, Protocol: "openai_chat"})
	w := h.do(http.MethodPost, "/v1/chat/completions", `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d", w.Code)
	}
	r := h.route(0)
	if r.TempUnschedulableUntil == nil || time.Until(*r.TempUnschedulableUntil) < 100*time.Second {
		t.Fatalf("want ~120s cooldown, got %v", r.TempUnschedulableUntil)
	}
}

func TestE2E_AutoDisableAndRecover(t *testing.T) {
	var healthy atomic.Bool
	a := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		if healthy.Load() {
			_, _ = io.WriteString(w, chatCompletionJSON)
			return
		}
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"error":{"message":"该令牌额度已用尽","type":"new_api_error"}}`)
	})
	b := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, chatCompletionJSON)
	})
	h := newE2E(t, func(g *storage.GatewayGroup) {
		g.AutoDisableEnabled = true
		g.ModelsJSON = `[{"id":"gpt-x","source":"custom"}]`
	}, e2eRouteSpec{Upstream: a, Protocol: "openai_chat"}, e2eRouteSpec{Upstream: b, Protocol: "openai_chat"})
	w := h.do(http.MethodPost, "/v1/chat/completions", `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if r := h.route(0); !r.AutoDisabled {
		t.Fatalf("route should be auto-disabled: %+v", r)
	}
	before := len(a.Calls())
	_ = h.do(http.MethodPost, "/v1/chat/completions", `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`, nil)
	if len(a.Calls()) != before {
		t.Fatal("auto-disabled route must not be scheduled")
	}
	if n := h.svc.RecoverAutoDisabledRoutes(context.Background()); n != 0 {
		t.Fatalf("still failing, recovered=%d", n)
	}
	healthy.Store(true)
	if n := h.svc.RecoverAutoDisabledRoutes(context.Background()); n != 1 {
		t.Fatalf("recovered=%d", n)
	}
	if r := h.route(0); r.AutoDisabled {
		t.Fatal("route should be recovered")
	}
}

func TestE2E_TaskAffinityRoutesPollToCreator(t *testing.T) {
	a := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})
	b := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		if r.Method == http.MethodPost {
			_, _ = io.WriteString(w, `{"id":"video_task_9","status":"queued"}`)
			return
		}
		_, _ = io.WriteString(w, `{"id":"video_task_9","status":"done"}`)
	})
	h := newE2E(t, func(g *storage.GatewayGroup) { g.CooldownSeconds = 0 },
		e2eRouteSpec{Upstream: a, Protocol: "openai_chat"}, e2eRouteSpec{Upstream: b, Protocol: "openai_chat"})
	if w := h.do(http.MethodPost, "/v1/videos", `{"model":"grok-video","prompt":"cat"}`, nil); w.Code != http.StatusOK {
		t.Fatalf("create status=%d", w.Code)
	}
	aBefore := len(a.Calls())
	w := h.do(http.MethodGet, "/v1/videos/video_task_9?variant=thumb", "", nil)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "done") {
		t.Fatalf("poll status=%d body=%s", w.Code, w.Body.String())
	}
	if len(a.Calls()) != aBefore {
		t.Fatal("poll should go straight to the creating route")
	}
	last := b.Calls()[len(b.Calls())-1]
	if last.Path != "/v1/videos/video_task_9?variant=thumb" {
		t.Fatalf("poll path %s", last.Path)
	}
}

func TestE2E_ConvertErrorFailsOverToNativeRoute(t *testing.T) {
	a := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, chatCompletionJSON)
	})
	b := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, `{"id":"resp_1","object":"response","status":"completed","model":"gpt-x","output":[{"type":"message","id":"m1","role":"assistant","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: a, Protocol: "openai_chat"}, e2eRouteSpec{Upstream: b, Protocol: "openai_responses"})
	w := h.do(http.MethodPost, "/v1/responses", `{"model":"gpt-x","previous_response_id":"resp_0","input":"hi"}`, nil)
	if w.Code != http.StatusOK || len(a.Calls()) != 0 || len(b.Calls()) != 1 {
		t.Fatalf("status=%d a=%d b=%d body=%s", w.Code, len(a.Calls()), len(b.Calls()), w.Body.String())
	}
}

func TestE2E_ModelAwareRoutingPrefersDeclaredRoute(t *testing.T) {
	a := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, chatCompletionJSON)
	})
	b := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, chatCompletionJSON)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: a, Protocol: "openai_chat"}, e2eRouteSpec{Upstream: b, Protocol: "openai_chat"})
	// 只有第二条路由声明了 gpt-x
	raw, _ := json.Marshal([]ModelListItem{{ID: "gpt-x", Source: "sync", Sources: []ModelSource{{RouteID: h.routes[1].ID}}}})
	h.group.ModelsJSON = string(raw)
	if err := h.svc.Groups.Update(h.group); err != nil {
		t.Fatal(err)
	}
	_ = h.do(http.MethodPost, "/v1/chat/completions", `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`, nil)
	if len(a.Calls()) != 0 || len(b.Calls()) != 1 {
		t.Fatalf("a=%d b=%d", len(a.Calls()), len(b.Calls()))
	}
}

func TestE2E_LegacyEngineStillConverts(t *testing.T) {
	protocol.SetEngine(protocol.EngineLegacy)
	t.Cleanup(func() { protocol.SetEngine(protocol.EngineRelayKit) })
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, `{"id":"msg_1","type":"message","role":"assistant","model":"claude-x","content":[{"type":"text","text":"hey"}],"stop_reason":"end_turn","usage":{"input_tokens":2,"output_tokens":1}}`)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "anthropic"})
	w := h.do(http.MethodPost, "/v1/chat/completions", `{"model":"claude-x","messages":[{"role":"user","content":"hi"}]}`, nil)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"chat.completion"`) || !strings.Contains(w.Body.String(), "hey") {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if up.Calls()[0].Path != "/v1/messages" {
		t.Fatalf("path %s", up.Calls()[0].Path)
	}
}

func TestRandomModeShufflesWithinPriority(t *testing.T) {
	firsts := map[uint]int{}
	for i := 0; i < 2000; i++ {
		cands := []ScoredRoute{
			{Route: storage.GatewayRoute{ID: 1, Weight: 5}, EffectiveRate: 2},
			{Route: storage.GatewayRoute{ID: 2, Weight: 5}, EffectiveRate: 1},
			{Route: storage.GatewayRoute{ID: 3, Weight: 1}, EffectiveRate: 0.1},
		}
		shufflePriorityTiers(cands)
		if cands[2].Route.ID != 3 {
			t.Fatal("lower priority tier must stay behind")
		}
		firsts[cands[0].Route.ID]++
	}
	// 同优先级 5 的两条大致各占一半
	if firsts[1] < 800 || firsts[2] < 800 {
		t.Fatalf("not shuffled within tier: %v", firsts)
	}
}

func TestE2E_HigherPriorityRouteCalledFirst(t *testing.T) {
	a := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, chatCompletionJSON)
	})
	b := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, chatCompletionJSON)
	})
	// 第二条优先级 10，应先于优先级 1 的第一条
	h := newE2E(t, nil, e2eRouteSpec{Upstream: a, Protocol: "openai_chat", Weight: 1}, e2eRouteSpec{Upstream: b, Protocol: "openai_chat", Weight: 10})
	_ = h.do(http.MethodPost, "/v1/chat/completions", `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`, nil)
	if len(a.Calls()) != 0 || len(b.Calls()) != 1 {
		t.Fatalf("a=%d b=%d", len(a.Calls()), len(b.Calls()))
	}
}

func TestStatusCodeRanges(t *testing.T) {
	norm, err := NormalizeStatusCodeRanges(" 429, 500-503 ,520-599")
	if err != nil || norm != "429,500-503,520-599" {
		t.Fatalf("norm=%q err=%v", norm, err)
	}
	if _, err := NormalizeStatusCodeRanges("700"); err == nil {
		t.Fatal("want error for out-of-range code")
	}
	ranges, _ := parseStatusCodeRanges(norm)
	for code, want := range map[int]bool{429: true, 502: true, 504: false, 530: true, 400: false} {
		if statusInRanges(code, ranges) != want {
			t.Fatalf("code %d", code)
		}
	}
}

func TestRetryAfterParsing(t *testing.T) {
	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	h := http.Header{}
	h.Set("Retry-After", "45")
	if d := retryAfter(h, now); d != 45*time.Second {
		t.Fatalf("seconds: %v", d)
	}
	h.Set("Retry-After", now.Add(90*time.Second).Format(http.TimeFormat))
	if d := retryAfter(h, now); d != 90*time.Second {
		t.Fatalf("date: %v", d)
	}
	h.Set("Retry-After", "99999")
	if d := retryAfter(h, now); d != maxRetryAfterCooldown {
		t.Fatalf("clamp: %v", d)
	}
}

func TestMultipartModelRewrite(t *testing.T) {
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	_ = mw.WriteField("model", "whisper-alias")
	fw, _ := mw.CreateFormFile("file", "a.wav")
	_, _ = fw.Write([]byte("RIFFDATA"))
	_ = mw.Close()
	ct := mw.FormDataContentType()
	if got := requestModel(ct, buf.Bytes()); got != "whisper-alias" {
		t.Fatalf("model=%q", got)
	}
	out := rewriteMultipartModel(ct, buf.Bytes(), "whisper-1")
	if got := requestModel(ct, out); got != "whisper-1" {
		t.Fatalf("rewritten model=%q", got)
	}
	if !bytes.Contains(out, []byte("RIFFDATA")) {
		t.Fatal("file part lost")
	}
}

func TestTaskIDsFromResponse(t *testing.T) {
	ids := taskIDsFromResponse([]byte(`{"name":"cachedContents/abc123","data":[{"task_id":"t1"}]}`))
	want := map[string]bool{"cachedContents/abc123": true, "abc123": true, "t1": true}
	for _, id := range ids {
		delete(want, id)
	}
	if len(want) != 0 {
		t.Fatalf("missing %v in %v", want, ids)
	}
}

func TestGeminiArrayEncoder(t *testing.T) {
	e := &geminiArrayEncoder{}
	var out bytes.Buffer
	for _, f := range e.encode([][]byte{[]byte("data: {\"a\":1}\n\n"), []byte("data: {\"b\":2}\n\n"), geminiArrayCloseFrame}) {
		out.Write(f)
	}
	var arr []map[string]int
	if err := json.Unmarshal(out.Bytes(), &arr); err != nil || len(arr) != 2 {
		t.Fatalf("%v %s", err, out.String())
	}
}

func TestIsPublicIPBlocksInternal(t *testing.T) {
	for _, ip := range []string{"127.0.0.1", "10.1.2.3", "192.168.1.1", "169.254.169.254", "100.64.0.1", "::1", "fd00::1", "0.0.0.0",
		"64:ff9b::a00:1", "2002:a00:1::1", "fec0::1", "::ffff:127.0.0.1"} {
		if isPublicIP(parseIP(t, ip)) {
			t.Fatalf("%s must be blocked", ip)
		}
	}
	if !isPublicIP(parseIP(t, "8.8.8.8")) {
		t.Fatal("public ip blocked")
	}
}

func parseIP(t *testing.T, s string) net.IP {
	t.Helper()
	ip := net.ParseIP(s)
	if ip == nil {
		t.Fatalf("bad ip %s", s)
	}
	return ip
}

// 重试关闭时，Gemini 原生透传也要越过不能接 Gemini 的高优先级路由，且不占顺延次数。
func TestE2E_GeminiPassthroughSkipsNonGeminiRoutes(t *testing.T) {
	chat := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, chatCompletionJSON)
	})
	gem := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, `{"embedding":{"values":[0.1,0.2]}}`)
	})
	h := newE2E(t, func(g *storage.GatewayGroup) { g.RetryEnabled = false },
		e2eRouteSpec{Upstream: chat, Protocol: "openai_chat", Weight: 10},
		e2eRouteSpec{Upstream: gem, Protocol: "gemini", Weight: 1})
	if got, _ := h.svc.Groups.FindByID(h.group.ID); got.RetryEnabled {
		t.Fatal("fixture: retry should be disabled")
	}
	w := h.do(http.MethodPost, "/v1beta/models/gemini-embedding-001:embedContent", `{"content":{"parts":[{"text":"hi"}]}}`, map[string]string{"x-goog-api-key": h.key})
	if w.Code != http.StatusOK || len(gem.Calls()) != 1 || len(chat.Calls()) != 0 {
		t.Fatalf("status=%d body=%s gem=%d chat=%d", w.Code, w.Body.String(), len(gem.Calls()), len(chat.Calls()))
	}
	if got := gem.Calls()[0].Path; got != "/v1beta/models/gemini-embedding-001:embedContent" {
		t.Fatalf("upstream path %s", got)
	}
}

// Google 的 embedding 模型名不以 gemini 开头，auto 路由也要能接。
func TestE2E_GeminiEmbeddingModelOnAutoRoute(t *testing.T) {
	gem := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, `{"embedding":{"values":[0.1]}}`)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: gem, Protocol: "auto"})
	w := h.do(http.MethodPost, "/v1beta/models/text-embedding-004:embedContent", `{"content":{"parts":[{"text":"hi"}]}}`, map[string]string{"x-goog-api-key": h.key})
	if w.Code != http.StatusOK || len(gem.Calls()) != 1 {
		t.Fatalf("status=%d body=%s calls=%d", w.Code, w.Body.String(), len(gem.Calls()))
	}
	if protocol.LooksLikeGeminiModel("text-embedding-3-small") || protocol.LooksLikeGeminiModel("text-embedding-ada-002") {
		t.Fatal("OpenAI embedding models must not look like Gemini")
	}
}

const claudeStreamThenError = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-x\",\"content\":[],\"usage\":{\"input_tokens\":3,\"output_tokens\":0}}}\n\n" +
	"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
	"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n" +
	"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n" +
	"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\n"

// 数组模式：任何结束路径都必须写出 "]"，客户端拿到的是合法 JSON。
func TestE2E_GeminiArrayAlwaysClosed(t *testing.T) {
	cases := map[string]string{
		// 已给出 finishReason 后上游报错：只闭合数组
		"error after finish": claudeStreamThenError +
			"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n",
		// 没给出终态就断开：错误元素 + 闭合
		"eof before finish": "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-x\",\"content\":[],\"usage\":{\"input_tokens\":3,\"output_tokens\":0}}}\n\n" +
			"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
			"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n",
		// 正常结束
		"complete": claudeStreamThenError + "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
	}
	for name, upstream := range cases {
		t.Run(name, func(t *testing.T) {
			up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) { writeSSE(w, upstream) })
			h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "anthropic"})
			w := h.do(http.MethodPost, "/v1beta/models/claude-x:streamGenerateContent", `{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`, map[string]string{"x-goog-api-key": h.key})
			var arr []map[string]any
			if err := json.Unmarshal(w.Body.Bytes(), &arr); err != nil || len(arr) == 0 {
				t.Fatalf("not a closed JSON array (%v): %q", err, w.Body.String())
			}
			_, hasErr := arr[len(arr)-1]["error"]
			if wantErr := name == "eof before finish"; hasErr != wantErr {
				t.Fatalf("trailing error element=%v want %v: %s", hasErr, wantErr, w.Body.String())
			}
		})
	}
}

// 转换中途遇到解析不了的上游块：不能悄悄丢内容再报成功，要以错误终态结束并记失败。
func TestE2E_StreamConvertErrorEndsWithError(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		writeSSE(w,
			`data: {"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","content":"Hel"}}]}`+"\n\n",
			`data: {"id":"c1","created":"oops","choices":[{"index":0,"delta":{"content":"lo"}}]}`+"\n\n",
			`data: {"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`+"\n\n",
			"data: [DONE]\n\n",
		)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "openai_chat"})
	w := h.do(http.MethodPost, "/v1/messages", `{"model":"gpt-x","stream":true,"max_tokens":64,"messages":[{"role":"user","content":"hi"}]}`, nil)
	out := w.Body.String()
	if !strings.Contains(out, "event: error") || strings.Contains(out, "event: message_stop") {
		t.Fatalf("want an error terminal and no message_stop: %s", out)
	}
	page, _ := h.svc.Usage.List(storage.GatewayUsageQuery{Page: 1, PageSize: 10})
	if page == nil || len(page.Items) == 0 || page.Items[0].Success {
		t.Fatalf("usage should record a failure: %+v", page)
	}
}

// 请求里的图片下载失败：换哪条路由都一样，直接回 400，不逐条路由重试。
func TestE2E_MediaFailureStopsWithoutFailover(t *testing.T) {
	var calls atomic.Int32
	handler := func(w http.ResponseWriter, r *http.Request, body []byte) {
		calls.Add(1)
		_, _ = io.WriteString(w, `{}`)
	}
	a, b := newMockUpstream(t, handler), newMockUpstream(t, handler)
	h := newE2E(t, nil, e2eRouteSpec{Upstream: a, Protocol: "anthropic"}, e2eRouteSpec{Upstream: b, Protocol: "anthropic"})
	// 127.0.0.1 被 SSRF 防护拦下，下载必然失败
	w := h.do(http.MethodPost, "/v1/chat/completions",
		`{"model":"claude-x","messages":[{"role":"user","content":[{"type":"text","text":"hi"},{"type":"image_url","image_url":{"url":"http://127.0.0.1:9/a.png"}}]}]}`, nil)
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "failed to load media") {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if calls.Load() != 0 {
		t.Fatalf("upstreams called %d times", calls.Load())
	}
}

func TestMediaFetchStateCachesAndBudgets(t *testing.T) {
	ctx := withMediaFetchState(context.Background())
	src := func(u string) types.FileSource { return types.NewURLFileSource(u) }
	_, _, err1 := resolveMediaBase64(ctx, src("http://127.0.0.1:9/a.png"))
	_, _, err2 := resolveMediaBase64(ctx, src("http://127.0.0.1:9/a.png"))
	if err1 == nil || err2 == nil || err1.Error() != err2.Error() || mediaFetchFailure(ctx) == nil {
		t.Fatalf("errors %v / %v, failure %v", err1, err2, mediaFetchFailure(ctx))
	}
	st := mediaFetchStateFrom(ctx)
	if st.downloads != 1 {
		t.Fatalf("same url downloaded %d times", st.downloads)
	}
	st.downloads = mediaMaxDownloadsPerRequest
	if _, _, err := resolveMediaBase64(ctx, src("http://127.0.0.1:9/b.png")); err == nil || !strings.Contains(err.Error(), "too many media") {
		t.Fatalf("budget not enforced: %v", err)
	}
}

// 组模型清单是空的、路由也没映射：恢复探测改问上游 /v1/models 拿一个模型。
func TestE2E_AutoRecoverFallsBackToUpstreamModels(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		if r.Method == http.MethodGet && r.URL.Path == "/v1/models" {
			_, _ = io.WriteString(w, `{"object":"list","data":[{"id":"gpt-x","object":"model"}]}`)
			return
		}
		_, _ = io.WriteString(w, chatCompletionJSON)
	})
	h := newE2E(t, func(g *storage.GatewayGroup) { g.AutoDisableEnabled = true }, e2eRouteSpec{Upstream: up, Protocol: "openai_chat"})
	if err := h.repo.SetAutoDisabled(h.routes[0].ID, "test", time.Now(), "r1"); err != nil {
		t.Fatal(err)
	}
	if n := h.svc.RecoverAutoDisabledRoutes(context.Background()); n != 1 {
		t.Fatalf("recovered=%d calls=%+v", n, up.Calls())
	}
}

// 关掉组的自动禁用开关：已被自动禁用的路由一并放回调度。
func TestUpdateGroupTurningOffAutoDisableReleasesRoutes(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {})
	h := newE2E(t, func(g *storage.GatewayGroup) { g.AutoDisableEnabled = true }, e2eRouteSpec{Upstream: up, Protocol: "openai_chat"})
	if err := h.repo.SetAutoDisabled(h.routes[0].ID, "test", time.Now(), "r1"); err != nil {
		t.Fatal(err)
	}
	off := false
	if _, err := h.svc.Admin.UpdateGroup(h.group.ID, UpdateGroupInput{AutoDisableEnabled: &off}); err != nil {
		t.Fatal(err)
	}
	if h.route(0).AutoDisabled {
		t.Fatal("route still auto-disabled after turning the switch off")
	}
}

// Seedance：客户端任意前缀，上游一律 /api/v3/...；任务 id 转义后拼进路径，不会变成上游查询串。
func TestE2E_SeedanceCanonicalPathAndEscapedIDs(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, `{"id":"cgt-1"}`)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "openai_chat"})
	_ = h.do(http.MethodPost, "/v1/contents/generations/tasks", `{"model":"seedance-1","content":[]}`, nil)
	_ = h.do(http.MethodGet, "/contents/generations/tasks/cgt-1", "", nil)
	_ = h.do(http.MethodGet, "/v1/videos/abc%3Fx%3D1", "", nil)
	calls := up.Calls()
	if len(calls) != 3 {
		t.Fatalf("calls=%d", len(calls))
	}
	for i, want := range []string{"/api/v3/contents/generations/tasks", "/api/v3/contents/generations/tasks/cgt-1", "/v1/videos/abc%3Fx=1"} {
		if calls[i].Path != want {
			t.Fatalf("call %d path %q, want %q", i, calls[i].Path, want)
		}
	}
}

func TestJoinUpstreamURL(t *testing.T) {
	for base, want := range map[string]string{
		"https://ark.example.com":         "https://ark.example.com/api/v3/contents/generations/tasks",
		"https://ark.example.com/api/v3":  "https://ark.example.com/api/v3/contents/generations/tasks",
		"https://ark.example.com/api/v3/": "https://ark.example.com/api/v3/contents/generations/tasks",
		"https://ark.example.com/v3":      "https://ark.example.com/v3/contents/generations/tasks",
	} {
		if got := joinUpstreamURL(base, "/api/v3/contents/generations/tasks"); got != want {
			t.Fatalf("%s → %s, want %s", base, got, want)
		}
	}
	if got := joinUpstreamURL("https://x.example/v3", "/v1/chat/completions"); got != "https://x.example/v3/v1/chat/completions" {
		t.Fatalf("non-seedance path changed: %s", got)
	}
}

// Gemini embedding：body 里的 model 按 Gemini 要求是 models/{model}，映射后与 URL 一致；批量请求逐条改写。
func TestE2E_GeminiEmbedBodyModels(t *testing.T) {
	gem := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, `{"embeddings":[{"values":[0.1]}]}`)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: gem, Protocol: "gemini", Mapping: `{"emb-alias":"gemini-embedding-001"}`})
	_ = h.do(http.MethodPost, "/v1beta/models/emb-alias:batchEmbedContents",
		`{"requests":[{"model":"models/emb-alias","content":{"parts":[{"text":"a"}]}},{"model":"models/emb-alias","content":{"parts":[{"text":"b"}]}}]}`,
		map[string]string{"x-goog-api-key": h.key})
	_ = h.do(http.MethodPost, "/v1beta/models/embedding-001:embedContent",
		`{"model":"models/embedding-001","content":{"parts":[{"text":"a"}]}}`, map[string]string{"x-goog-api-key": h.key})
	calls := gem.Calls()
	if len(calls) != 2 {
		t.Fatalf("calls=%d", len(calls))
	}
	if calls[0].Path != "/v1beta/models/gemini-embedding-001:batchEmbedContents" ||
		strings.Count(string(calls[0].Body), `"model":"models/gemini-embedding-001"`) != 2 {
		t.Fatalf("batch call %s %s", calls[0].Path, calls[0].Body)
	}
	if !strings.Contains(string(calls[1].Body), `"model":"models/embedding-001"`) {
		t.Fatalf("models/ prefix dropped: %s", calls[1].Body)
	}
}

func TestNormalizeStatusCodeRangesMergesAndValidates(t *testing.T) {
	for in, want := range map[string]string{
		"500,501,502":     "500-502",
		"429, 500 - 503":  "429,500-503",
		"502-504\t500":    "500,502-504",
		"500-599,520-530": "500-599",
		"":                "",
	} {
		got, err := NormalizeStatusCodeRanges(in)
		if err != nil || got != want {
			t.Fatalf("%q → %q (%v), want %q", in, got, err, want)
		}
	}
	var many []string
	for c := 100; c < 600; c += 2 {
		many = append(many, strconv.Itoa(c))
	}
	if _, err := NormalizeStatusCodeRanges(strings.Join(many, ",")); err == nil {
		t.Fatal("over-long list should be rejected")
	}
	if got, err := NormalizeAutoDisableStatusCodes(" None "); err != nil || got != "none" {
		t.Fatalf("none → %q %v", got, err)
	}
	if _, err := NormalizeAutoDisableStatusCodes("401,abc"); err == nil {
		t.Fatal("invalid codes accepted")
	}
}
