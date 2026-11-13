package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/auth"
	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/logincaptcha"
	"github.com/bejix/upstream-ops/backend/runtimeconfig"
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
