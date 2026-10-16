package gateway

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/bejix/upstream-ops/backend/storage"
)

func TestRouteSupportsModel(t *testing.T) {
	all := &storage.GatewayRoute{}
	img := &storage.GatewayRoute{SupportedModels: []string{"gpt-image-*", "DALL-E-3", "gemini-embedding-001"}}
	for _, tc := range []struct {
		route  *storage.GatewayRoute
		models []string
		want   bool
	}{
		{all, []string{"anything"}, true},
		{img, []string{"gpt-image-1"}, true},
		{img, []string{"GPT-IMAGE-1.5"}, true},
		{img, []string{"dall-e-3"}, true},
		{img, []string{"models/gemini-embedding-001"}, true},
		{img, []string{"gpt-4o"}, false},
		// 组级映射后的名字命中也算
		{img, []string{"my-image-alias", "gpt-image-1"}, true},
		{img, []string{"gpt-image"}, false},
	} {
		if got := routeSupportsModel(tc.route, tc.models...); got != tc.want {
			t.Fatalf("routeSupportsModel(%v, %v)=%v want %v", tc.route.SupportedModels, tc.models, got, tc.want)
		}
	}
	routes := []storage.GatewayRoute{{ID: 1, SupportedModels: []string{"a"}}, {ID: 2}}
	if got := filterRoutesForModel(routes, ""); len(got) != 2 {
		t.Fatalf("requests without a model must not be filtered: %v", got)
	}
	if got := filterRoutesForModel(routes, "b"); len(got) != 1 || got[0].ID != 2 {
		t.Fatalf("filter: %v", got)
	}
	got := applySupportedModels(&storage.GatewayRoute{SupportedModels: []string{"only-listed", "gpt-image-*"}}, []string{"gpt-image-1", "gpt-4o"})
	if strings.Join(got, ",") != "only-listed,gpt-image-1" {
		t.Fatalf("applySupportedModels: %v", got)
	}
}

func TestGatewayRoutesSaveSupportedModels(t *testing.T) {
	db := openGatewayTestDB(t)
	routes := storage.NewGatewayRoutes(db)
	if err := routes.SaveForGroup(1, []storage.GatewayRoute{
		{SourceChannelID: 1, Enabled: true, SupportedModels: []string{" gpt-image-1 ", "GPT-IMAGE-1", "", "dall-e-3"}},
		{SourceChannelID: 2, Enabled: true},
	}); err != nil {
		t.Fatal(err)
	}
	list, _ := routes.ListByGroupID(1)
	if strings.Join(list[0].SupportedModels, ",") != "gpt-image-1,dall-e-3" {
		t.Fatalf("normalized list: %#v", list[0].SupportedModels)
	}
	if len(list[1].SupportedModels) != 0 {
		t.Fatalf("route without a list: %#v", list[1].SupportedModels)
	}
}

// 高优先级的画图路由只接画图模型：聊天请求越过它，画图请求走它；都不接时直接 404、不打上游。
func TestE2E_SupportedModelsFilterRoutes(t *testing.T) {
	image := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, `{"created":1,"data":[{"url":"https://example.com/a.png"}]}`)
	})
	chat := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, chatCompletionJSON)
	})
	h := newE2E(t, nil,
		e2eRouteSpec{Upstream: image, Protocol: "openai_chat", Weight: 10},
		e2eRouteSpec{Upstream: chat, Protocol: "openai_chat", Weight: 1})
	saveSupported(t, h, 0, []string{"gpt-image-*"})

	if w := h.do(http.MethodPost, "/v1/chat/completions", `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`, nil); w.Code != http.StatusOK {
		t.Fatalf("chat: %d %s", w.Code, w.Body.String())
	}
	if len(image.Calls()) != 0 || len(chat.Calls()) != 1 {
		t.Fatalf("chat request reached the image-only route: image=%d chat=%d", len(image.Calls()), len(chat.Calls()))
	}
	if w := h.do(http.MethodPost, "/v1/images/generations", `{"model":"gpt-image-1","prompt":"cat"}`, nil); w.Code != http.StatusOK {
		t.Fatalf("image: %d %s", w.Code, w.Body.String())
	}
	if len(image.Calls()) != 1 {
		t.Fatalf("image request should go to the image route: image=%d", len(image.Calls()))
	}

	// 两条路由都限定了模型，请求的模型谁都不接
	saveSupported(t, h, 1, []string{"gpt-4o"})
	w := h.do(http.MethodPost, "/v1/chat/completions", `{"model":"gpt-x","messages":[{"role":"user","content":"hi"}]}`, nil)
	if w.Code != http.StatusNotFound || !strings.Contains(w.Body.String(), "no route in this group forwards it") {
		t.Fatalf("want 404, got %d %s", w.Code, w.Body.String())
	}
	if len(chat.Calls()) != 1 {
		t.Fatalf("upstream called for an unsupported model")
	}
}

// /v1/models（自动聚合）只列出配了转发模型的路由声明的模型。
func TestE2E_SupportedModelsLimitModelList(t *testing.T) {
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		_, _ = io.WriteString(w, `{"object":"list","data":[{"id":"gpt-image-1"},{"id":"gpt-x"}]}`)
	})
	h := newE2E(t, func(g *storage.GatewayGroup) { g.ModelsMode = storage.GatewayModelsModeAuto }, e2eRouteSpec{Upstream: up, Protocol: "openai_chat"})
	saveSupported(t, h, 0, []string{"gpt-image-*", "listed-only"})
	w := h.do(http.MethodGet, "/v1/models", "", nil)
	var out struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("%v %s", err, w.Body.String())
	}
	var ids []string
	for _, m := range out.Data {
		ids = append(ids, m.ID)
	}
	got := strings.Join(ids, ",")
	if !strings.Contains(got, "gpt-image-1") || !strings.Contains(got, "listed-only") || strings.Contains(got, "gpt-x") {
		t.Fatalf("model list %q", got)
	}
}

func saveSupported(t *testing.T, h *e2eHarness, i int, models []string) {
	t.Helper()
	list, err := h.repo.ListByGroupID(h.group.ID)
	if err != nil {
		t.Fatal(err)
	}
	for j := range list {
		if list[j].ID == h.routes[i].ID {
			list[j].SupportedModels = models
		}
	}
	if err := h.repo.SaveForGroup(h.group.ID, list); err != nil {
		t.Fatal(err)
	}
}
