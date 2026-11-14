package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/auth"
	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/logincaptcha"
	"github.com/bejix/upstream-ops/backend/runtimeconfig"
	"github.com/bejix/upstream-ops/backend/scheduler"
	"github.com/gin-gonic/gin"
)

func newLoginTestDeps(t *testing.T, authEnabled, captchaEnabled bool) *Deps {
	t.Helper()
	var authSvc *auth.Service
	if authEnabled {
		var err error
		authSvc, err = auth.New("admin", "secret", "test-token-secret", time.Hour)
		if err != nil {
			t.Fatalf("auth.New: %v", err)
		}
	}
	captchaSvc := logincaptcha.NewService()
	t.Cleanup(captchaSvc.Close)
	runtime := runtimeconfig.New(
		"", "", nil, nil, nil, nil, authSvc, nil,
		config.ProxyConfig{}, config.UpstreamConfig{}, config.GatewayConfig{}, nil,
	)
	runtime.SetLoginCaptchaEnabled(captchaEnabled)
	return &Deps{
		Runtime:      runtime,
		LoginCaptcha: captchaSvc,
	}
}

func serveLogin(t *testing.T, d *Deps, body string) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerAuth(r.Group("/api"), d)
	req := httptest.NewRequest(http.MethodPost, "/api/auth/login", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

func TestLoginRequiresCaptchaWhenEnabled(t *testing.T) {
	rec := serveLogin(t, newLoginTestDeps(t, true, true), `{"username":"admin","password":"secret"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if body.Code != captchaErrorRequired {
		t.Fatalf("code = %q, want %q", body.Code, captchaErrorRequired)
	}
}

func TestLoginDoesNotRequireCaptchaWhenSwitchDisabled(t *testing.T) {
	rec := serveLogin(t, newLoginTestDeps(t, true, false), `{"username":"admin","password":"secret"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func TestLoginDoesNotRequireCaptchaWhenAuthDisabled(t *testing.T) {
	rec := serveLogin(t, newLoginTestDeps(t, false, true), `{}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func TestCaptchaEndpointReturnsDataURI(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerAuth(r.Group("/api"), newLoginTestDeps(t, true, true))
	req := httptest.NewRequest(http.MethodGet, "/api/auth/captcha", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}
	var body struct {
		Data struct {
			Enabled     bool   `json:"enabled"`
			CaptchaID   string `json:"captcha_id"`
			ImageBase64 string `json:"image_base64"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if !body.Data.Enabled || body.Data.CaptchaID == "" {
		t.Fatalf("unexpected captcha response: %+v", body.Data)
	}
	if !strings.HasPrefix(body.Data.ImageBase64, "data:image/png;base64,") {
		t.Fatalf("image_base64 is not a PNG data URI: %q", body.Data.ImageBase64)
	}
}

func TestApplyFromFileKeepsIssuedLoginCaptcha(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	authCfg := config.AuthConfig{
		Enabled:         true,
		CaptchaEnabled:  true,
		Username:        "admin",
		Password:        "new-password",
		TokenSecret:     "new-token-secret",
		SessionTTLHours: 72,
	}
	if err := config.Save(path, &config.Config{Auth: authCfg}); err != nil {
		t.Fatalf("save config: %v", err)
	}

	initialAuth, err := auth.New("admin", "old-password", "old-token-secret", 72*time.Hour)
	if err != nil {
		t.Fatalf("auth.New: %v", err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := runtimeconfig.New(
		path,
		"",
		log,
		nil,
		nil,
		nil,
		initialAuth,
		nil,
		config.ProxyConfig{},
		config.UpstreamConfig{},
		config.GatewayConfig{},
		func(scfg config.SchedulerConfig, pcfg config.ProxyConfig) *scheduler.Scheduler {
			return scheduler.New(scfg, nil, nil, nil, nil, nil, nil, nil, nil, pcfg, log)
		},
	)
	mgr.SetLoginCaptchaEnabled(true)

	// 与 main.go 一样，让签发和登录接口共享同一个验证码 Service 和 Store。
	captchaStore := logincaptcha.NewStore(logincaptcha.DefaultTTL)
	captchaSvc := logincaptcha.NewServiceWithStore(captchaStore)
	t.Cleanup(captchaSvc.Close)
	deps := &Deps{Runtime: mgr, LoginCaptcha: captchaSvc}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerAuth(r.Group("/api"), deps)

	// 配置热更新前，通过真实接口签发验证码。
	req := httptest.NewRequest(http.MethodGet, "/api/auth/captcha", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("captcha status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var captchaBody struct {
		Data struct {
			CaptchaID string `json:"captcha_id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &captchaBody); err != nil {
		t.Fatalf("unmarshal captcha response: %v", err)
	}
	answer := captchaStore.Get(captchaBody.Data.CaptchaID, false)
	if captchaBody.Data.CaptchaID == "" || answer == "" {
		t.Fatal("captcha endpoint did not store an issued captcha")
	}

	if _, err := mgr.ApplyFromFile(); err != nil {
		t.Fatalf("ApplyFromFile: %v", err)
	}
	if mgr.CurrentAuth() == initialAuth {
		t.Fatal("auth service was not rebuilt")
	}

	// 热更新重建 auth.Service 后，原验证码仍须通过真实登录接口消费成功。
	loginBody, err := json.Marshal(map[string]string{
		"username":     "admin",
		"password":     "new-password",
		"captcha_id":   captchaBody.Data.CaptchaID,
		"captcha_code": answer,
	})
	if err != nil {
		t.Fatalf("marshal login request: %v", err)
	}
	loginRec := serveLogin(t, deps, string(loginBody))
	if loginRec.Code != http.StatusOK {
		t.Fatalf("login status = %d, body = %s", loginRec.Code, loginRec.Body.String())
	}
}
