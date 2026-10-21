// 数据面：路由的瞬时故障处理。
//
// 传输层错误（连接被拒 / 重置 / 半途 EOF）大多是一次性的抖动：同一路由补试一次就好，
// 不该像 HTTP 429 / 5xx 那样立刻把路由冷却掉——线上一个模型常常只有一两条路由，
// 一次抖动就是整段时间全黑。只有连续几次请求都在传输层失败，才判定上游真的不可达并冷却。
package gateway

import (
	"context"
	"sync"
	"time"

	"github.com/bejix/upstream-ops/backend/connector"
	"github.com/bejix/upstream-ops/backend/storage"
)

const (
	// transportRetryBackoff 传输错误补试前的等待（与 sub2api sameAccountRetryDelay 一致）。
	transportRetryBackoff = 500 * time.Millisecond
	// transportFailureCollapse 这段时间内的多次传输失败只算一次：一条 HTTP/2 连接断开会同时打断
	// 复用它的所有流，逐个计数一次断连就会冷却路由（sub2api openai_proxy_stream_circuit.go #5056）。
	transportFailureCollapse = 3 * time.Second
	// transportFailuresBeforeCooldown 连续多少次请求（各自补试后仍失败）在传输层失败才冷却路由。
	transportFailuresBeforeCooldown = 3
)

// routeHealth 进程内的路由健康记忆：每条路由连续传输失败的次数（成功一次清零），
// 以及窗口内的账号级失败次数（余额 / 鉴权 / 无权限，见 accountFailureKind）。
type routeHealth struct {
	mu      sync.Mutex
	streak  map[uint]int
	counted map[uint]time.Time // 上一次计入 streak 的时间（合并窗口用）
	// accountStrikes 账号级失败的时间（只留 accountFailureWindow 内的）
	accountStrikes map[uint][]time.Time
}

// noteAccountFailure 记一次账号级失败，返回窗口内的累计次数（含这次）。
// 同一条路由在 transportFailureCollapse 内的多次只算一次：并发请求同时撞上同一个余额耗尽，不该一下就累计到停用。
func (h *routeHealth) noteAccountFailure(routeID uint, now time.Time) int {
	if routeID == 0 {
		return 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.accountStrikes == nil {
		h.accountStrikes = map[uint][]time.Time{}
	}
	kept := h.accountStrikes[routeID][:0]
	for _, t := range h.accountStrikes[routeID] {
		if now.Sub(t) < accountFailureWindow {
			kept = append(kept, t)
		}
	}
	if n := len(kept); n > 0 && now.Sub(kept[n-1]) < transportFailureCollapse {
		h.accountStrikes[routeID] = kept
		return n
	}
	kept = append(kept, now)
	h.accountStrikes[routeID] = kept
	return len(kept)
}

// clearAccountFailures 路由已停用或恢复：重新计数。
func (h *routeHealth) clearAccountFailures(routeID uint) {
	h.mu.Lock()
	delete(h.accountStrikes, routeID)
	h.mu.Unlock()
}

// noteTransportFailure 记一次传输失败；连续失败达到阈值时返回 true（该冷却了）并清零。
func (h *routeHealth) noteTransportFailure(routeID uint) (cool bool) {
	if routeID == 0 {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.streak == nil {
		h.streak, h.counted = map[uint]int{}, map[uint]time.Time{}
	}
	now := time.Now()
	if last, ok := h.counted[routeID]; ok && now.Sub(last) < transportFailureCollapse {
		return false
	}
	h.counted[routeID] = now
	h.streak[routeID]++
	if h.streak[routeID] < transportFailuresBeforeCooldown {
		return false
	}
	delete(h.streak, routeID)
	delete(h.counted, routeID)
	return true
}

// noteSuccess 路由成功一次：连续失败计数清零。
func (h *routeHealth) noteSuccess(routeID uint) {
	h.mu.Lock()
	if h.streak != nil {
		delete(h.streak, routeID)
		delete(h.counted, routeID)
	}
	h.mu.Unlock()
}

// transportStreak 当前连续失败次数（测试 / 指标用）。
func (h *routeHealth) transportStreak(routeID uint) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.streak[routeID]
}

// routeTryBudget 一条路由上还能重试几次：组配置的重试次数之外，传输层瞬时错误再补一次（带短退避）。
type routeTryBudget struct {
	normal    int // 组配置的同路由重试次数
	transport int // 传输错误的额外补试次数
	backoff   time.Duration
}

func newRouteTryBudget(p retryPolicy) routeTryBudget {
	b := routeTryBudget{normal: p.sameRouteRetries}
	if p.enabled {
		b.transport = 1
	}
	return b
}

// take 申请一次重试；transient 表示这次失败是传输层瞬时错误。返回能否再试，并设好 backoff。
func (b *routeTryBudget) take(transient bool) bool {
	b.backoff = 0
	if b.normal > 0 {
		b.normal--
		return true
	}
	if transient && b.transport > 0 {
		b.transport--
		b.backoff = transportRetryBackoff
		return true
	}
	return false
}

// isTransientTransportError 传输层瞬时错误：有传输错误、且不是首字超时（那是网关主动掐的）也不是协议转换失败。
func isTransientTransportError(err error) bool {
	return err != nil && !isFirstTokenTimeout(err) && !isProtocolConvertError(err)
}

// earliestCooledRoute 所有路由都在冷却时的兜底：挑最早到期的那条（其它条件仍须可调度）。
func earliestCooledRoute(routes []storage.GatewayRoute, groupsByChannel map[uint][]connector.APIKeyGroup, now time.Time, exclude map[uint]struct{}) (ScoredRoute, bool) {
	var best *storage.GatewayRoute
	for i := range routes {
		r := &routes[i]
		if _, skip := exclude[r.ID]; skip {
			continue
		}
		if r.TempUnschedulableUntil == nil || !r.TempUnschedulableUntil.After(now) {
			continue
		}
		cp := *r
		cp.TempUnschedulableUntil = nil
		if !IsRouteSchedulable(&cp, now) {
			continue // 禁用 / 自动禁用 / 没有密钥：冷却到期也不能用
		}
		if best == nil || r.TempUnschedulableUntil.Before(*best.TempUnschedulableUntil) {
			best = r
		}
	}
	if best == nil {
		return ScoredRoute{}, false
	}
	cp := *best
	rate := RateForRoute(&cp, groupsByChannel[cp.SourceChannelID])
	return ScoredRoute{Route: cp, EffectiveRate: rate, BillingRate: rate}, true
}

// sleepCtx 等 d；ctx 先结束时返回 false。
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
