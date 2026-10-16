// 数据面：请求路径上的数据库写改成后台批量写入（write-behind）。
//
// 以前每个请求在给客户端回包之前要同步写 4~5 次库（用量日志、累加额度、更新最后使用时间、路由成功计数）；
// SQLite 只有一个连接，这些写把所有请求串成一条队，吞吐卡在几百 QPS。现在：
//   - 用量日志进缓冲队列，后台攒满 usageBatchSize 条或每 usageFlushEvery 一个事务批量插入；
//     队列满时退回同步写（宁可慢，不丢）；
//   - 密钥的额度增量与最后使用时间在内存里合并，每 counterFlushEvery 一个事务写回；
//     额度检查用「库里已用 + 尚未写回的增量」，不会因为写回延迟而多放行；
//   - Stop 时把缓冲全部写完（主程序在 HTTP 服务关闭、在途请求结束之后调用）。
//
// 没调 StartWriteBehind（测试、嵌入方）时，各写入点保持原来的同步写。
// 参考 new-api 的 BATCH_UPDATE_ENABLED（model/utils.go）：同样会在进程崩溃时丢失最多一个周期的增量。
package gateway

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
)

const (
	usageQueueSize    = 8192
	usageBatchSize    = 256
	usageFlushEvery   = 500 * time.Millisecond
	counterFlushEvery = 2 * time.Second
)

type writeBehind struct {
	usage *storage.GatewayUsageLogs
	keys  *storage.GatewayKeys
	log   *slog.Logger

	usageCh chan *storage.GatewayUsageLog
	stopped atomic.Bool

	mu       sync.Mutex
	quota    map[uint]float64   // 尚未写回的额度增量
	inflight map[uint]float64   // 正在写回的额度增量（写回事务提交前仍计入额度检查）
	lastUsed map[uint]time.Time // 尚未写回的最后使用时间

	stop chan struct{}
	wg   sync.WaitGroup
}

func newWriteBehind(usage *storage.GatewayUsageLogs, keys *storage.GatewayKeys, log *slog.Logger) *writeBehind {
	return &writeBehind{
		usage:    usage,
		keys:     keys,
		log:      log,
		usageCh:  make(chan *storage.GatewayUsageLog, usageQueueSize),
		quota:    map[uint]float64{},
		inflight: map[uint]float64{},
		lastUsed: map[uint]time.Time{},
		stop:     make(chan struct{}),
	}
}

func (w *writeBehind) start() {
	w.wg.Add(2)
	go w.usageLoop()
	go w.counterLoop()
}

// close 停止接收并把缓冲全部写完。
func (w *writeBehind) close() {
	if w.stopped.Swap(true) {
		return
	}
	close(w.stop)
	w.wg.Wait()
}

// enqueueUsage 放进写入队列；已停止或队列满时返回 false，由调用方同步写。
func (w *writeBehind) enqueueUsage(item *storage.GatewayUsageLog) bool {
	if w.stopped.Load() {
		return false
	}
	select {
	case w.usageCh <- item:
		return true
	default:
		return false
	}
}

func (w *writeBehind) addQuota(keyID uint, amount float64) {
	if amount == 0 {
		return
	}
	w.mu.Lock()
	w.quota[keyID] += amount
	w.mu.Unlock()
}

func (w *writeBehind) touch(keyID uint, at time.Time) {
	w.mu.Lock()
	w.lastUsed[keyID] = at
	w.mu.Unlock()
}

// pendingQuota 还没落库的额度增量（含正在写回的）。
func (w *writeBehind) pendingQuota(keyID uint) float64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.quota[keyID] + w.inflight[keyID]
}

func (w *writeBehind) usageLoop() {
	defer w.wg.Done()
	batch := make([]*storage.GatewayUsageLog, 0, usageBatchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		w.writeUsage(batch)
		batch = make([]*storage.GatewayUsageLog, 0, usageBatchSize)
	}
	ticker := time.NewTicker(usageFlushEvery)
	defer ticker.Stop()
	for {
		select {
		case item := <-w.usageCh:
			batch = append(batch, item)
			if len(batch) >= usageBatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-w.stop:
			for {
				select {
				case item := <-w.usageCh:
					batch = append(batch, item)
					if len(batch) >= usageBatchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
	}
}

// writeUsage 批量插入；整批失败时逐条重试，尽量不丢记录。
func (w *writeBehind) writeUsage(batch []*storage.GatewayUsageLog) {
	if err := w.usage.CreateBatch(batch); err == nil {
		return
	} else if w.log != nil {
		w.log.Warn("batch write usage logs failed, retrying one by one", "rows", len(batch), "err", err)
	}
	for _, item := range batch {
		if err := w.usage.Create(item); err != nil && w.log != nil {
			w.log.Error("write usage log failed", "request_id", item.RequestID, "err", err)
		}
	}
}

func (w *writeBehind) counterLoop() {
	defer w.wg.Done()
	ticker := time.NewTicker(counterFlushEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			w.flushCounters()
		case <-w.stop:
			w.flushCounters()
			return
		}
	}
}

// flushCounters 把合并好的额度增量与最后使用时间一个事务写回；失败则放回下个周期重试。
func (w *writeBehind) flushCounters() {
	w.mu.Lock()
	if len(w.quota) == 0 && len(w.lastUsed) == 0 {
		w.mu.Unlock()
		return
	}
	quota, lastUsed := w.quota, w.lastUsed
	w.quota, w.lastUsed = map[uint]float64{}, map[uint]time.Time{}
	for id, d := range quota {
		w.inflight[id] += d
	}
	w.mu.Unlock()

	deltas := make(map[uint]storage.KeyUsageDelta, len(quota)+len(lastUsed))
	for id, d := range quota {
		deltas[id] = storage.KeyUsageDelta{QuotaUsed: d}
	}
	for id, at := range lastUsed {
		d := deltas[id]
		at := at
		d.LastUsedAt = &at
		deltas[id] = d
	}
	err := w.keys.ApplyUsageDeltas(deltas)

	w.mu.Lock()
	for id, d := range quota {
		if w.inflight[id] -= d; w.inflight[id] == 0 {
			delete(w.inflight, id)
		}
		if err != nil {
			w.quota[id] += d
		}
	}
	if err != nil {
		for id, at := range lastUsed {
			if cur, ok := w.lastUsed[id]; !ok || at.After(cur) {
				w.lastUsed[id] = at
			}
		}
	}
	w.mu.Unlock()
	if err != nil && w.log != nil {
		w.log.Warn("write back key usage failed, will retry", "keys", len(deltas), "err", err)
	}
}

// ---------- Service 侧入口 ----------

// StartWriteBehind 启动后台批量写入（主程序调用；重复调用无副作用）。
func (s *Service) StartWriteBehind() {
	if s == nil || s.Usage == nil || s.Keys == nil {
		return
	}
	w := newWriteBehind(s.Usage, s.Keys, s.Log)
	if s.writer.CompareAndSwap(nil, w) {
		w.start()
	}
}

// stopWriteBehind 停止后台写入并把缓冲写完；之后的写入回到同步。
func (s *Service) stopWriteBehind() {
	if w := s.writer.Swap(nil); w != nil {
		w.close()
	}
}

// saveUsageLog 写一条用量日志：后台队列可用时异步，否则同步。
func (rt *Runtime) saveUsageLog(item *storage.GatewayUsageLog) {
	if w := rt.writer.Load(); w != nil && w.enqueueUsage(item) {
		return
	}
	if err := rt.Usage.Create(item); err != nil && rt.Log != nil {
		rt.Log.Error("write usage log failed", "err", err)
	}
}

// addKeyQuotaUsed 累加密钥已用额度。
func (rt *Runtime) addKeyQuotaUsed(keyID uint, amount float64) {
	if w := rt.writer.Load(); w != nil {
		w.addQuota(keyID, amount)
		return
	}
	_ = rt.Keys.AddQuotaUsed(keyID, amount)
}

// touchKey 记录密钥最后使用时间。
func (rt *Runtime) touchKey(keyID uint, at time.Time) {
	if w := rt.writer.Load(); w != nil {
		w.touch(keyID, at)
		return
	}
	_ = rt.Keys.TouchLastUsed(keyID, at)
}

// pendingKeyQuota 还没写回库的已用额度（额度检查要算上）。
func (rt *Runtime) pendingKeyQuota(keyID uint) float64 {
	if w := rt.writer.Load(); w != nil {
		return w.pendingQuota(keyID)
	}
	return 0
}

// noteRouteSuccess 成功后清理路由的暂停 / 错误残留；路由本来就干净时不碰数据库
// （以前每次成功都执行两条 UPDATE，SQLite 下哪怕一行没改也要抢写锁）。
func (rt *Runtime) noteRouteSuccess(route *storage.GatewayRoute) {
	if route == nil || route.ID == 0 {
		return
	}
	// 请求开始时的快照可能已过时（期间别的请求把它暂停了）：用缓存里的最新状态判断，不查库
	current := route
	if list, err := rt.cachedRoutes(route.GatewayGroupID); err == nil {
		for i := range list {
			if list[i].ID == route.ID {
				current = &list[i]
				break
			}
		}
	}
	if !routeHasPauseResidue(current) {
		return
	}
	_ = rt.Routes.NoteSuccessForPauseError(route.ID)
}

// routeHasPauseResidue 与 NoteSuccessForPauseError 的 WHERE 条件一致。
func routeHasPauseResidue(r *storage.GatewayRoute) bool {
	return r.TempUnschedulableReason != "" || r.TempUnschedulableUntil != nil ||
		r.TempUnschedulableRequestID != "" || r.TempUnschedulableAt != nil
}
