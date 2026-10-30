package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
)

func TestSub2APIBillingDeclaresGroupRate(t *testing.T) {
	h := newE2E(t, func(g *storage.GatewayGroup) {
		g.RateMultiplier = 0.65
	})

	assertRate := func(want float64) {
		t.Helper()
		w := h.do(http.MethodGet, "/v1/sub2api/billing", "", nil)
		if w.Code != http.StatusOK {
			t.Fatalf("billing status = %d, body = %s", w.Code, w.Body.String())
		}
		if got := w.Header().Get("Cache-Control"); got != "no-store" {
			t.Fatalf("Cache-Control = %q", got)
		}
		var body struct {
			Object                  string  `json:"object"`
			SchemaVersion           int     `json:"schema_version"`
			BillingScope            string  `json:"billing_scope"`
			GroupRateMultiplier     float64 `json:"group_rate_multiplier"`
			ResolvedRateMultiplier  float64 `json:"resolved_rate_multiplier"`
			PeakRateEnabled         bool    `json:"peak_rate_enabled"`
			EffectiveRateMultiplier float64 `json:"effective_rate_multiplier"`
			ObservedAt              string  `json:"observed_at"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if body.Object != "sub2api.key_billing" || body.SchemaVersion != 1 || body.BillingScope != "token" {
			t.Fatalf("unexpected billing envelope: %+v", body)
		}
		if body.GroupRateMultiplier != want || body.ResolvedRateMultiplier != want || body.EffectiveRateMultiplier != want {
			t.Fatalf("declared rates = %v/%v/%v, want %v", body.GroupRateMultiplier, body.ResolvedRateMultiplier, body.EffectiveRateMultiplier, want)
		}
		if body.PeakRateEnabled {
			t.Fatal("peak rate must be disabled")
		}
		if _, err := time.Parse(time.RFC3339Nano, body.ObservedAt); err != nil {
			t.Fatalf("observed_at = %q: %v", body.ObservedAt, err)
		}
	}
	assertRate(0.65)

	rate := 0.75
	if _, err := h.svc.Admin.UpdateGroup(h.group.ID, UpdateGroupInput{RateMultiplier: &rate}); err != nil {
		t.Fatal(err)
	}
	assertRate(0.75)

	req := httptest.NewRequest(http.MethodGet, "/v1/sub2api/billing", nil)
	req.Header.Set("Authorization", "Bearer sk-invalid")
	w := httptest.NewRecorder()
	h.engine.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("invalid key status = %d, body = %s", w.Code, w.Body.String())
	}
}

func TestGatewayGroupRateDefaultsAndValidation(t *testing.T) {
	h := newE2E(t, nil)

	created, err := h.svc.Admin.CreateGroup(CreateGroupInput{Name: "default-rate"})
	if err != nil {
		t.Fatal(err)
	}
	if created.RateMultiplier != 1 {
		t.Fatalf("default rate = %v, want 1", created.RateMultiplier)
	}

	zero := 0.0
	if _, err := h.svc.Admin.CreateGroup(CreateGroupInput{Name: "zero-rate", RateMultiplier: &zero}); err == nil {
		t.Fatal("expected zero rate to be rejected")
	}
	if _, err := h.svc.Admin.UpdateGroup(created.ID, UpdateGroupInput{RateMultiplier: &zero}); err == nil {
		t.Fatal("expected zero rate update to be rejected")
	}
}
