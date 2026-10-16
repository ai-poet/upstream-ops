package api

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/runtimeconfig"
	"github.com/gin-gonic/gin"
)

func putGatewaySettings(t *testing.T, gatewayJSON string) (*httptest.ResponseRecorder, string) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := config.Save(path, &config.Config{}); err != nil {
		t.Fatal(err)
	}
	r := gin.New()
	registerSettings(r.Group("/api"), &Deps{
		Runtime: runtimeconfig.New(path, "", nil, nil, nil, nil, nil, nil, config.ProxyConfig{}, config.UpstreamConfig{}, config.GatewayConfig{}, nil),
	})
	body := `{
		"app":{"title":"T"},
		"auth":{"enabled":false},
		"scheduler":{"concurrency":1},
		"notifications":{"sendMaxAttempts":1},
		"gateway":` + gatewayJSON + `
	}`
	req := httptest.NewRequest(http.MethodPut, "/api/settings/config", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec, path
}

func TestSaveSettingsRejectsInvalidAutoDisableStatusCodes(t *testing.T) {
	rec, _ := putGatewaySettings(t, `{"autoDisableStatusCodes":"401,abc"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestSaveSettingsDoesNotPinDefaultAutoDisableRules(t *testing.T) {
	keywords := `["` + strings.Join(config.DefaultGatewayAutoDisableKeywords, `","`) + `"]`
	rec, path := putGatewaySettings(t, `{"autoDisableStatusCodes":"401","autoDisableKeywords":`+keywords+`}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "insufficient_quota") {
		t.Fatalf("default keywords were written into the config file:\n%s", raw)
	}
	got, err := config.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Gateway.AutoDisableStatusCodes != "401" || len(got.Gateway.AutoDisableKeywords) != len(config.DefaultGatewayAutoDisableKeywords) {
		t.Fatalf("defaults not restored on load: %+v", got.Gateway)
	}
}

func TestSaveSettingsKeepsCustomAutoDisableRules(t *testing.T) {
	rec, path := putGatewaySettings(t, `{"autoDisableStatusCodes":"none","autoDisableKeywords":["my quota is gone"]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	got, err := config.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Gateway.AutoDisableStatusCodes != "none" || len(got.Gateway.AutoDisableKeywords) != 1 || got.Gateway.AutoDisableKeywords[0] != "my quota is gone" {
		t.Fatalf("custom rules lost: %+v", got.Gateway)
	}
}

func TestIsGatewayPathKeepsSPARoutes(t *testing.T) {
	for _, p := range []string{"/v1beta/unknown", "/backend-api/x", "/antigravity/v1/x", "/v3/x", "/contents/x", "/api/x", "/v1"} {
		if !isGatewayPath(p) {
			t.Fatalf("%s should not fall back to the SPA", p)
		}
	}
	for _, p := range []string{"/", "/gateway", "/settings", "/captcha", "/notifications", "/v1betax", "/apidocs"} {
		if isGatewayPath(p) {
			t.Fatalf("%s must keep the SPA fallback", p)
		}
	}
}
