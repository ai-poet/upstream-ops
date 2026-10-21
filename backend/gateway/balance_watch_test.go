package gateway

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
)

// fakeBalanceAPI 能查余额的渠道服务。
type fakeBalanceAPI struct {
	fakeChannelAPIForResort
	mu      sync.Mutex
	balance float64
	calls   int
}

func (f *fakeBalanceAPI) GetChannelBalance(ctx context.Context, channelID uint) (float64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.balance, nil
}

func (f *fakeBalanceAPI) set(v float64) {
	f.mu.Lock()
	f.balance = v
	f.mu.Unlock()
}

// 余额不足：同渠道的监控路由一起冷却并开始盯余额；余额只是继续往下扣时不恢复，充值后立刻恢复
// （含累计停用的），因别的原因停着的路由不动。
func TestBalanceWatch_ResumesRoutesAfterRecharge(t *testing.T) {
	db := openGatewayTestDB(t)
	routes := storage.NewGatewayRoutes(db)
	monitor := func(group, channel uint, pos int) storage.GatewayRoute {
		return storage.GatewayRoute{GatewayGroupID: group, Position: pos, SourceKind: storage.GatewayRouteSourceMonitor,
			SourceChannelID: channel, Enabled: true, Weight: 1}
	}
	if err := routes.SaveForGroup(1, []storage.GatewayRoute{monitor(1, 22, 0), monitor(1, 22, 1)}); err != nil {
		t.Fatal(err)
	}
	if err := routes.SaveForGroup(2, []storage.GatewayRoute{monitor(2, 22, 0), monitor(2, 9, 1)}); err != nil {
		t.Fatal(err)
	}
	g1, _ := routes.ListByGroupID(1)
	g2, _ := routes.ListByGroupID(2)
	failing, sibling, otherGroup, otherChannel := g1[0], g1[1], g2[0], g2[1]

	api := &fakeBalanceAPI{balance: 0.03}
	svc := NewService(nil, nil, routes, nil, nil, nil, api, nil, nil)
	rt := svc.Runtime

	// 同渠道的兄弟路由先因「无权访问」停着：它不是余额问题，充值后也不该被恢复
	forbiddenUntil := time.Now().Add(30 * time.Minute)
	_ = routes.SetTempUnschedulable(sibling.ID, forbiddenUntil, "账号级失败：account_forbidden\n无权访问", time.Now(), "r0")

	req := &relayRequest{id: "r1"}
	at := &relayAttempt{routeChoice: &routeChoice{route: failing}}
	decision := failureDecision{Action: failSwitchRoute, Reason: accountFailureReasonBalance, Cooldown: accountFailureCooldown, ChannelWide: true}
	if until := rt.pauseRouteForAccountFailure(req, at, decision, usageErrorInfo{}, "Insufficient account balance"); until == nil {
		t.Fatal("route not paused")
	}
	paused, _ := routes.ListBalancePausedRoutes(22, balancePauseMarker, time.Now())
	if len(paused) != 2 { // failing + otherGroup（sibling 冷却得更久，没被覆盖）
		t.Fatalf("balance-paused routes = %v, want failing and the other group's route", paused)
	}

	svc.checkBalanceWatches(context.Background()) // 取基线 0.03
	api.set(0.01)                                 // 在途请求还在扣
	svc.checkBalanceWatches(context.Background())
	if r, _ := routes.FindByID(failing.ID); r.TempUnschedulableUntil == nil {
		t.Fatal("resumed without a recharge")
	}

	api.set(50)
	svc.checkBalanceWatches(context.Background())
	for _, id := range []uint{failing.ID, otherGroup.ID} {
		if r, _ := routes.FindByID(id); r.TempUnschedulableUntil != nil {
			t.Fatalf("route %d still paused after recharge", id)
		}
	}
	if r, _ := routes.FindByID(sibling.ID); r.TempUnschedulableUntil == nil {
		t.Fatal("route paused for another reason was resumed")
	}
	if r, _ := routes.FindByID(otherChannel.ID); r.TempUnschedulableUntil != nil {
		t.Fatal("route on another channel touched")
	}
	svc.balanceMu.Lock()
	left := len(svc.balance.m)
	svc.balanceMu.Unlock()
	if left != 0 {
		t.Fatalf("channel still watched after resume")
	}
}

// 累计停用的路由（原因带余额标记）充值后也恢复。
func TestBalanceWatch_ResumesAutoDisabledRoute(t *testing.T) {
	db := openGatewayTestDB(t)
	routes := storage.NewGatewayRoutes(db)
	if err := routes.SaveForGroup(1, []storage.GatewayRoute{{GatewayGroupID: 1, SourceKind: storage.GatewayRouteSourceMonitor, SourceChannelID: 5, Enabled: true}}); err != nil {
		t.Fatal(err)
	}
	list, _ := routes.ListByGroupID(1)
	_ = routes.SetAutoDisabled(list[0].ID, "累计 3 次"+balancePauseMarker+"\nInsufficient account balance", time.Now(), "r")

	api := &fakeBalanceAPI{balance: 0}
	svc := NewService(nil, nil, routes, nil, nil, nil, api, nil, nil)
	svc.watchChannelBalance(5)
	svc.checkBalanceWatches(context.Background())
	api.set(10)
	svc.checkBalanceWatches(context.Background())
	if r, _ := routes.FindByID(list[0].ID); r.AutoDisabled {
		t.Fatal("auto-disabled route not resumed after recharge")
	}
}

// 同渠道里因密钥失效被停用的路由：余额冷却会给它也写上冷却，但充值后只结束冷却，不解除它的停用。
func TestBalanceWatch_KeepsAuthDisabledRouteDisabled(t *testing.T) {
	db := openGatewayTestDB(t)
	routes := storage.NewGatewayRoutes(db)
	if err := routes.SaveForGroup(1, []storage.GatewayRoute{{GatewayGroupID: 1, SourceKind: storage.GatewayRouteSourceMonitor, SourceChannelID: 5, Enabled: true}}); err != nil {
		t.Fatal(err)
	}
	list, _ := routes.ListByGroupID(1)
	id := list[0].ID
	_ = routes.SetAutoDisabled(id, "累计 3 次账号级失败：account_auth\ninvalid key", time.Now(), "r")
	if _, err := routes.SetTempUnschedulableForSource(storage.GatewayRouteSourceMonitor, 5, time.Now().Add(10*time.Minute), balancePauseMarker+"\nbalance", time.Now(), "r2"); err != nil {
		t.Fatal(err)
	}
	api := &fakeBalanceAPI{balance: 0}
	svc := NewService(nil, nil, routes, nil, nil, nil, api, nil, nil)
	svc.watchChannelBalance(5)
	svc.checkBalanceWatches(context.Background())
	api.set(10)
	svc.checkBalanceWatches(context.Background())
	r, _ := routes.FindByID(id)
	if r.TempUnschedulableUntil != nil || !r.AutoDisabled {
		t.Fatalf("want cooldown ended but auth disable kept: until=%v disabled=%v", r.TempUnschedulableUntil, r.AutoDisabled)
	}
}

// 渠道服务查不了余额（直连渠道 / 测试替身）时不盯。
func TestBalanceWatch_SkippedWithoutBalanceReader(t *testing.T) {
	svc := NewService(nil, nil, nil, nil, nil, nil, &fakeChannelAPIForResort{}, nil, nil)
	svc.watchChannelBalance(3)
	if svc.balance.m != nil {
		t.Fatal("watched a channel without a balance reader")
	}
}
