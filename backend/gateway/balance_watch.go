// 后台：余额不足冷却期间主动查余额，充值后立即恢复路由。
//
// 路由因余额不足冷却 10 分钟（见 pauseRouteForAccountFailure）。上游账号如果一两分钟后就充了值，
// 干等到期太久：冷却期间每分钟用监控同一个余额接口查一次这个渠道的余额（免费、不发模型请求），
// 比冷却开始时多了（充值了）就立刻恢复这个渠道下因余额停着的路由，包括累计 3 次被停用的。
//
// 只对监控渠道有效：直连渠道没有余额接口，仍按冷却时长等。
// 冷却开始后马上查一次作为基线，之后余额高于基线才算充值——不用「大于 0」判断，
// 因为中转站的「余额不足」常在余额还剩几分钱时就触发（预扣费不够），大于 0 不代表能用。
package gateway

import (
	"context"
	"time"
)

const (
	// balanceWatchInterval 冷却期间查余额的间隔
	balanceWatchInterval = time.Minute
	// balanceWatchQueryTimeout 单次余额查询的超时
	balanceWatchQueryTimeout = 20 * time.Second
	// balanceWatchMaxAge 一个渠道最多盯这么久（路由已停用、迟迟不充值时不无限查下去；之后交给恢复探测）
	balanceWatchMaxAge = 24 * time.Hour
	// balancePauseMarker 写进冷却 / 停用原因里的标记，用来认出「因余额停的」路由
	balancePauseMarker = "账号级失败：" + accountFailureReasonBalance
)

// channelBalanceReader 能查渠道余额的渠道服务（channel.Service 实现了它）；ChannelAPI 不一定实现。
type channelBalanceReader interface {
	GetChannelBalance(ctx context.Context, channelID uint) (float64, error)
}

type balanceWatchEntry struct {
	since       time.Time
	baseline    float64
	hasBaseline bool
}

// balanceWatches 正在盯的渠道。
type balanceWatches struct {
	m    map[uint]*balanceWatchEntry
	wake chan struct{}
}

// watchChannelBalance 渠道因余额不足冷却：开始盯它的余额，并叫醒后台立刻取基线。
func (svc *core) watchChannelBalance(channelID uint) {
	if channelID == 0 {
		return
	}
	if _, ok := svc.ChannelAPI.(channelBalanceReader); !ok {
		return
	}
	svc.balanceMu.Lock()
	if svc.balance.m == nil {
		svc.balance.m = map[uint]*balanceWatchEntry{}
	}
	if _, exists := svc.balance.m[channelID]; !exists {
		svc.balance.m[channelID] = &balanceWatchEntry{since: time.Now()}
	}
	wake := svc.balance.wake
	svc.balanceMu.Unlock()
	if wake != nil {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

// balanceWatchLoop 每分钟（或有新渠道加入时）检查一遍。
func (svc *core) balanceWatchLoop(stop <-chan struct{}) {
	svc.balanceMu.Lock()
	if svc.balance.wake == nil {
		svc.balance.wake = make(chan struct{}, 1)
	}
	wake := svc.balance.wake
	svc.balanceMu.Unlock()
	ticker := time.NewTicker(balanceWatchInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
		case <-wake:
		}
		svc.checkBalanceWatches(context.Background())
	}
}

// checkBalanceWatches 逐个渠道：没有因余额停着的路由了就不再盯；否则查余额，比基线多了就恢复这些路由。
func (svc *core) checkBalanceWatches(ctx context.Context) {
	reader, ok := svc.ChannelAPI.(channelBalanceReader)
	if !ok || svc.Routes == nil {
		return
	}
	svc.balanceMu.Lock()
	ids := make([]uint, 0, len(svc.balance.m))
	for id := range svc.balance.m {
		ids = append(ids, id)
	}
	svc.balanceMu.Unlock()

	for _, channelID := range ids {
		svc.balanceMu.Lock()
		e := svc.balance.m[channelID]
		svc.balanceMu.Unlock()
		if e == nil {
			continue
		}
		now := time.Now()
		routeIDs, err := svc.Routes.ListBalancePausedRoutes(channelID, balancePauseMarker, now)
		if err != nil {
			continue
		}
		if len(routeIDs) == 0 || now.Sub(e.since) > balanceWatchMaxAge {
			svc.forgetBalanceWatch(channelID)
			continue
		}
		qctx, cancel := context.WithTimeout(ctx, balanceWatchQueryTimeout)
		balance, err := reader.GetChannelBalance(qctx, channelID)
		cancel()
		if err != nil {
			// 查不到（登录失败、上游超时）：下次再试，冷却照常到期
			continue
		}
		if !e.hasBaseline {
			e.baseline, e.hasBaseline = balance, true
			continue
		}
		if balance <= e.baseline {
			// 还在往下扣（冷却前已在途的请求）：基线跟着降，充值后比较的是最低点
			e.baseline = balance
			continue
		}
		if err := svc.Routes.ResumeRoutes(routeIDs, balancePauseMarker); err != nil {
			continue
		}
		for _, id := range routeIDs {
			svc.routeHealth.clearAccountFailures(id)
		}
		svc.forgetBalanceWatch(channelID)
		if svc.Log != nil {
			svc.Log.Info("gateway routes resumed: upstream account recharged",
				"channel_id", channelID, "routes", len(routeIDs), "baseline", e.baseline, "balance", balance)
		}
	}
}

func (svc *core) forgetBalanceWatch(channelID uint) {
	svc.balanceMu.Lock()
	delete(svc.balance.m, channelID)
	svc.balanceMu.Unlock()
}
