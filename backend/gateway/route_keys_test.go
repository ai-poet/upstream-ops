package gateway

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/bejix/upstream-ops/backend/connector"
	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/storage"
)

// fakeKeyAPI 记录上游 Key 的增改，模拟 NewAPI / Sub2API 的密钥管理接口。
type fakeKeyAPI struct {
	mu      sync.Mutex
	keys    []connector.APIKey
	creates int
	updates int
}

func (f *fakeKeyAPI) ListAPIKeys(_ context.Context, _ uint, q connector.APIKeyQuery) (*connector.APIKeyPage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []connector.APIKey
	for _, k := range f.keys {
		if q.Search == "" || strings.Contains(k.Name, q.Search) {
			out = append(out, k)
		}
	}
	return &connector.APIKeyPage{Items: out}, nil
}

func (f *fakeKeyAPI) ListAPIKeyGroups(context.Context, uint) ([]connector.APIKeyGroup, error) {
	return nil, nil
}

func (f *fakeKeyAPI) CreateAPIKey(_ context.Context, _ uint, req connector.APIKeyCreateRequest) (*connector.APIKey, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.creates++
	k := connector.APIKey{ID: int64(len(f.keys) + 1), Name: req.Name, Group: req.Group}
	f.keys = append(f.keys, k)
	return &k, nil
}

func (f *fakeKeyAPI) UpdateAPIKey(_ context.Context, _ uint, id int64, req connector.APIKeyUpdateRequest) (*connector.APIKey, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updates++
	for i := range f.keys {
		if f.keys[i].ID == id {
			if req.Name != nil {
				f.keys[i].Name = *req.Name
			}
			k := f.keys[i]
			return &k, nil
		}
	}
	return nil, nil
}

func (f *fakeKeyAPI) RevealAPIKey(_ context.Context, _ uint, id int64) (string, error) {
	return "sk-upstream-" + strconv.FormatInt(id, 10), nil
}

// 保存路由后只给缺密钥的监控渠道路由生成密钥：新路由生成一次，再保存不重复调用上游，
// 换了源分组的路由重新对齐（复用并改名已有 Key）。
func TestEnsureMissingRouteKeys(t *testing.T) {
	db := openGatewayTestDB(t)
	channels := storage.NewChannels(db)
	if err := channels.Create(&storage.Channel{Name: "up", Type: storage.ChannelTypeNewAPI, SiteURL: "https://up.example", Username: "u", PasswordCipher: "x"}); err != nil {
		t.Fatal(err)
	}
	cipher, err := crypto.NewCipher("route-keys-test")
	if err != nil {
		t.Fatal(err)
	}
	api := &fakeKeyAPI{}
	groups := storage.NewGatewayGroups(db)
	svc := NewService(groups, storage.NewGatewayKeys(db), storage.NewGatewayRoutes(db), nil, nil, channels, api, cipher, nil)
	g := &storage.GatewayGroup{Name: "g", Status: storage.GatewayGroupStatusActive}
	if err := groups.Create(g); err != nil {
		t.Fatal(err)
	}

	vip := int64(7)
	saved, err := svc.Admin.SaveRoutes(g.ID, []RouteInput{{SourceKind: "monitor", SourceChannelID: 1, SourceGroupID: &vip, SourceGroupName: "vip", Weight: 1, Enabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	res, err := svc.Admin.EnsureMissingRouteKeys(context.Background(), g.ID)
	if err != nil || res.OKCount != 1 || api.creates != 1 {
		t.Fatalf("first ensure: res=%+v err=%v creates=%d", res, err, api.creates)
	}
	route, _ := svc.Routes.FindByID(saved[0].ID)
	if route.SourceAPIKeyName != svc.Admin.stableUpstreamKeyName(1, &vip, "vip") || route.SourceAPIKeyCipher == "" {
		t.Fatalf("key not stored on route: %+v", route)
	}

	// 再保存一次（来源没变）：不再碰上游
	if _, err := svc.Admin.SaveRoutes(g.ID, []RouteInput{{ID: route.ID, SourceKind: "monitor", SourceChannelID: 1, SourceGroupID: &vip, SourceGroupName: "vip", Weight: 5, Enabled: true}}); err != nil {
		t.Fatal(err)
	}
	res, err = svc.Admin.EnsureMissingRouteKeys(context.Background(), g.ID)
	if err != nil || len(res.Routes) != 0 || api.creates != 1 || api.updates != 0 {
		t.Fatalf("second ensure touched upstream: res=%+v creates=%d updates=%d", res, api.creates, api.updates)
	}

	// 换源分组：需要对齐到新分组的统一名
	plus := int64(8)
	if _, err := svc.Admin.SaveRoutes(g.ID, []RouteInput{{ID: route.ID, SourceKind: "monitor", SourceChannelID: 1, SourceGroupID: &plus, SourceGroupName: "plus", Weight: 5, Enabled: true}}); err != nil {
		t.Fatal(err)
	}
	res, err = svc.Admin.EnsureMissingRouteKeys(context.Background(), g.ID)
	if err != nil || res.OKCount != 1 {
		t.Fatalf("ensure after source group change: res=%+v err=%v", res, err)
	}
	route, _ = svc.Routes.FindByID(route.ID)
	if route.SourceAPIKeyName != svc.Admin.stableUpstreamKeyName(1, &plus, "plus") {
		t.Fatalf("key not re-aligned to new source group: %q", route.SourceAPIKeyName)
	}
}

// 拉取路由模型：直连渠道用直连配置的 Key；监控渠道（未保存的草稿）在上游准备好统一 Key 后用它调 /v1/models。
func TestFetchRouteModels(t *testing.T) {
	var seenAuth []string
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		seenAuth = append(seenAuth, r.Header.Get("Authorization"))
		_, _ = io.WriteString(w, `{"object":"list","data":[{"id":"gpt-image-1"},{"id":"gpt-x"},{"id":"gpt-x"}]}`)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "openai_chat"})

	models, err := h.svc.Admin.FetchRouteModels(context.Background(), h.group.ID, RouteInput{SourceKind: "provider", GatewayProviderID: h.routes[0].GatewayProviderID})
	if err != nil || strings.Join(models, ",") != "gpt-image-1,gpt-x" {
		t.Fatalf("provider: %v %v", models, err)
	}
	if seenAuth[0] != "Bearer sk-up-a" {
		t.Fatalf("provider key not used: %q", seenAuth[0])
	}

	// 监控渠道：渠道站点指向 mock，上游 Key 由 fakeKeyAPI 建出来
	api := &fakeKeyAPI{}
	h.svc.ChannelAPI = api
	h.svc.Channels = storage.NewChannels(openGatewayTestDB(t))
	if err := h.svc.Channels.Create(&storage.Channel{Name: "up", Type: storage.ChannelTypeNewAPI, SiteURL: up.URL, Username: "u", PasswordCipher: "x"}); err != nil {
		t.Fatal(err)
	}
	vip := int64(7)
	draft := RouteInput{SourceKind: "monitor", SourceChannelID: 1, SourceGroupID: &vip, SourceGroupName: "vip"}
	if _, err := h.svc.Admin.FetchRouteModels(context.Background(), h.group.ID, draft); err != nil {
		t.Fatal(err)
	}
	if api.creates != 1 || seenAuth[1] != "Bearer sk-upstream-1" {
		t.Fatalf("monitor draft: creates=%d auth=%q", api.creates, seenAuth[1])
	}
	if _, err := h.svc.Admin.FetchRouteModels(context.Background(), h.group.ID, RouteInput{SourceKind: "monitor"}); err == nil {
		t.Fatal("route without a channel should be rejected")
	}
}
