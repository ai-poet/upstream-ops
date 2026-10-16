package storage

import "testing"

// default:true 的布尔列：新建时显式传 false 必须原样落库。
func TestGatewayGroupsCreateKeepsFalseBools(t *testing.T) {
	db := openTestDB(t)
	groups := NewGatewayGroups(db)
	g := &GatewayGroup{Name: "g", RetryEnabled: false, FailoverEnabled: false, ModelAwareRouting: false}
	if err := groups.Create(g); err != nil {
		t.Fatal(err)
	}
	if g.RetryEnabled || g.FailoverEnabled || g.ModelAwareRouting {
		t.Fatalf("returned struct flipped to defaults: %+v", g)
	}
	got, err := groups.FindByID(g.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.RetryEnabled || got.FailoverEnabled || got.ModelAwareRouting {
		t.Fatalf("stored retry=%v failover=%v model_aware=%v, want all false", got.RetryEnabled, got.FailoverEnabled, got.ModelAwareRouting)
	}

	on := &GatewayGroup{Name: "on", RetryEnabled: true, FailoverEnabled: true, ModelAwareRouting: true}
	if err := groups.Create(on); err != nil {
		t.Fatal(err)
	}
	got, _ = groups.FindByID(on.ID)
	if !got.RetryEnabled || !got.FailoverEnabled || !got.ModelAwareRouting {
		t.Fatalf("true values lost: %+v", got)
	}
}

func TestGatewayRoutesSaveKeepsGeminiProtocolAndDisabled(t *testing.T) {
	db := openTestDB(t)
	routes := NewGatewayRoutes(db)
	if err := routes.SaveForGroup(3, []GatewayRoute{
		{SourceChannelID: 1, Weight: 1, Enabled: true, UpstreamProtocol: GatewayUpstreamProtocolGemini},
		{SourceChannelID: 2, Weight: 1, Enabled: false, UpstreamProtocol: "google"},
		{SourceChannelID: 3, Weight: 1, Enabled: true, UpstreamProtocol: "claude"},
	}); err != nil {
		t.Fatal(err)
	}
	list, err := routes.ListByGroupID(3)
	if err != nil || len(list) != 3 {
		t.Fatalf("list: %v len=%d", err, len(list))
	}
	if list[0].UpstreamProtocol != GatewayUpstreamProtocolGemini || list[1].UpstreamProtocol != GatewayUpstreamProtocolGemini {
		t.Fatalf("gemini protocol not kept: %q %q", list[0].UpstreamProtocol, list[1].UpstreamProtocol)
	}
	if list[2].UpstreamProtocol != GatewayUpstreamProtocolAnthropic {
		t.Fatalf("claude alias: %q", list[2].UpstreamProtocol)
	}
	if list[1].Enabled {
		t.Fatalf("new route saved as disabled came back enabled")
	}
	if !list[0].Enabled {
		t.Fatalf("enabled route came back disabled")
	}
}
