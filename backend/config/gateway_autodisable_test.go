package config

import "testing"

func TestGatewayAutoDisableDefaults(t *testing.T) {
	g := GatewayConfig{}.WithDefaults()
	if g.AutoDisableStatusCodes != DefaultGatewayAutoDisableStatusCodes || len(g.AutoDisableKeywords) == 0 {
		t.Fatalf("defaults: %+v", g)
	}
	if got := (GatewayConfig{AutoDisableStatusCodes: " OFF "}).WithDefaults().AutoDisableStatusCodes; got != GatewayAutoDisableStatusCodesNone {
		t.Fatalf("off → %q", got)
	}
	if got := (GatewayConfig{AutoRecoverMinutes: 1 << 40}).WithDefaults().AutoRecoverMinutes; got != MaxGatewayAutoRecoverMinutes {
		t.Fatalf("recover minutes not clamped: %d", got)
	}
	saved := g.ForSave()
	if saved.AutoDisableStatusCodes != "" || saved.AutoDisableKeywords != nil {
		t.Fatalf("ForSave kept defaults: %+v", saved)
	}
	custom := GatewayConfig{AutoDisableKeywords: []string{"x y"}}.WithDefaults().ForSave()
	if len(custom.AutoDisableKeywords) != 1 {
		t.Fatalf("ForSave dropped custom keywords: %+v", custom)
	}
}
