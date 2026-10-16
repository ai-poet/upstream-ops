package gateway

// 生图结果 url 转 base64（移植自 sub2api openai_images_b64_backfill.go），见 images_b64_backfill.go。

import (
	"bytes"
	"context"
	"encoding/base64"
	"image"
	"image/color"
	"image/png"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/tidwall/gjson"
)

func tinyPNG(t *testing.T) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 1, 1))
	img.Set(0, 0, color.RGBA{R: 255, A: 255})
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// imageHost 提供图片 / 非图片内容的测试服务器；测试期间放开私网拦截（真实客户端会拦 127.0.0.1）。
func imageHost(t *testing.T, pngBytes []byte) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/a.png":
			w.Header().Set("Content-Type", "application/octet-stream") // 响应头不作为依据，以嗅探为准
			_, _ = w.Write(pngBytes)
		case "/page.html":
			w.Header().Set("Content-Type", "image/png") // 冒充图片
			_, _ = io.WriteString(w, "<html><body>not an image</body></html>")
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	old := imageBackfillClient
	imageBackfillClient = srv.Client()
	t.Cleanup(func() { imageBackfillClient = old })
	return srv
}

func enableImagesBackfill(t *testing.T, h *e2eHarness) {
	t.Helper()
	r := h.route(0)
	r.ImagesURLToB64JSON = true
	if err := h.repo.SaveForGroup(h.group.ID, []storage.GatewayRoute{r}); err != nil {
		t.Fatal(err)
	}
}

func TestE2E_ImagesURLBackfilledToB64JSON(t *testing.T) {
	pngBytes := tinyPNG(t)
	host := imageHost(t, pngBytes)
	dataURLPayload := base64.StdEncoding.EncodeToString(pngBytes)
	upstreamBody := `{"created":1,"data":[` +
		`{"url":"` + host.URL + `/a.png","revised_prompt":"p"},` +
		`{"b64_json":"already","url":"` + host.URL + `/a.png"},` +
		`{"url":"data:image/png;base64,` + dataURLPayload + `"},` +
		`{"url":"` + host.URL + `/page.html"},` +
		`{"url":"` + host.URL + `/missing.png"}]}`
	up := newMockUpstream(t, func(w http.ResponseWriter, r *http.Request, body []byte) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, upstreamBody)
	})
	h := newE2E(t, nil, e2eRouteSpec{Upstream: up, Protocol: "openai_chat"})
	req := `{"model":"gpt-image-2","prompt":"cat"}`

	// 开关关闭：原样返回
	if w := h.do(http.MethodPost, "/v1/images/generations", req, nil); w.Body.String() != upstreamBody {
		t.Fatalf("开关关闭时不应改响应：%s", w.Body.String())
	}

	enableImagesBackfill(t, h)
	w := h.do(http.MethodPost, "/v1/images/generations", req, nil)
	got := w.Body.Bytes()
	want := base64.StdEncoding.EncodeToString(pngBytes)
	if b := gjson.GetBytes(got, "data.0.b64_json").String(); b != want {
		t.Fatalf("url 应回填为 base64：%q", b)
	}
	if u := gjson.GetBytes(got, "data.0.url").String(); u != host.URL+"/a.png" || gjson.GetBytes(got, "data.0.revised_prompt").String() != "p" {
		t.Fatalf("url 与其它字段应保留：%s", got)
	}
	if b := gjson.GetBytes(got, "data.1.b64_json").String(); b != "already" {
		t.Fatalf("已有 b64_json 不应覆盖：%q", b)
	}
	if b := gjson.GetBytes(got, "data.2.b64_json").String(); b != dataURLPayload {
		t.Fatalf("data: url 应直接取载荷：%q", b)
	}
	if gjson.GetBytes(got, "data.3.b64_json").Exists() || gjson.GetBytes(got, "data.4.b64_json").Exists() {
		t.Fatalf("非图片内容 / 下载失败的项应原样保留：%s", got)
	}

	// 客户端显式要 url：不回填（JSON 与 multipart 都认）
	if w := h.do(http.MethodPost, "/v1/images/generations", `{"model":"gpt-image-2","prompt":"cat","response_format":"url"}`, nil); w.Body.String() != upstreamBody {
		t.Fatalf("response_format=url 时不应回填：%s", w.Body.String())
	}
	if !clientWantsImageURL("multipart/form-data; boundary=XX", []byte("--XX\r\nContent-Disposition: form-data; name=\"response_format\"\r\n\r\nurl\r\n--XX--\r\n")) {
		t.Fatal("multipart 里的 response_format=url 没认出来")
	}

	// 只作用于同步返回图片的 Images 端点
	if isImagesResultEndpoint("/v1/images/generations/async") || isImagesResultEndpoint("/v1/chat/completions") || !isImagesResultEndpoint("/v1/images/edits") {
		t.Fatal("端点判断不对")
	}
}

// 真实下载客户端拒绝回环 / 私网地址（防 SSRF）。
func TestFetchImageURLBase64BlocksPrivateAddress(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(tinyPNG(t))
	}))
	defer srv.Close()
	_, err := fetchImageURLBase64(context.Background(), srv.URL+"/a.png")
	if err == nil || !strings.Contains(err.Error(), "non-public") {
		t.Fatalf("应拒绝 127.0.0.1：%v", err)
	}
	if _, err := fetchImageURLBase64(context.Background(), "file:///etc/passwd"); err == nil {
		t.Fatal("应拒绝非 http(s)")
	}
}

// 管理端保存路由时带上开关。
func TestSaveRoutes_ImagesURLToB64JSON(t *testing.T) {
	db := openGatewayTestDB(t)
	groupsRepo := storage.NewGatewayGroups(db)
	routesRepo := storage.NewGatewayRoutes(db)
	svc := NewService(groupsRepo, storage.NewGatewayKeys(db), routesRepo, nil, nil, nil, nil, nil, nil)
	g := &storage.GatewayGroup{Name: "img", Status: storage.GatewayGroupStatusActive, RateSortDirection: "asc"}
	if err := groupsRepo.Create(g); err != nil {
		t.Fatal(err)
	}
	ch := &storage.Channel{Name: "ch", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.com", Username: "u", PasswordCipher: "x", CredentialMode: storage.CredentialModePassword}
	if err := db.Create(ch).Error; err != nil {
		t.Fatal(err)
	}
	saved, err := svc.Admin.SaveRoutes(g.ID, []RouteInput{{SourceKind: storage.GatewayRouteSourceMonitor, SourceChannelID: ch.ID, Weight: 1, Enabled: true, ImagesURLToB64JSON: true}})
	if err != nil || len(saved) != 1 {
		t.Fatalf("SaveRoutes: %v %v", saved, err)
	}
	got, err := routesRepo.FindByID(saved[0].ID)
	if err != nil || !got.ImagesURLToB64JSON {
		t.Fatalf("开关没存上：%+v %v", got, err)
	}
}
