// 运行指标：进程内原子计数，GET /metrics 以 Prometheus 文本格式输出，管理 API 另给 JSON 快照。
//
// 只记最基本的几类：请求数与耗时（按状态码档）、上游尝试结果、路由冷却 / 自动禁用、
// 流式在途数与流中途出错、后台批量写入的队列深度与失败数。没有引入指标库，避免多一个依赖。
package gateway

import (
	"crypto/subtle"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
)

// Metrics 网关指标。零值可用；所有方法对 nil 安全。
type Metrics struct {
	// 请求：按状态码档（2xx / 4xx / 5xx / other）
	requests   [4]atomic.Int64
	durationMS [4]atomic.Int64

	streamsInflight atomic.Int64
	streamErrors    atomic.Int64 // 已向客户端写出后流中途出错

	attemptsMu sync.Mutex
	attempts   map[string]int64 // 上游尝试结果：success / transport / first_token_timeout / http_429 / http_5xx / http_4xx / config / client

	routeCooldowns      [3]atomic.Int64 // 按原因：http / transport / first_token_timeout
	routeAutoDisables   atomic.Int64
	routeCooldownBorrow atomic.Int64 // 所有路由都在冷却时借用最早到期路由的次数
	upstreamResets      atomic.Int64 // 收到协议终态后，上游在宽限期内没结束、被网关重置（RST）的流

	writeBehindSyncFallbacks   atomic.Int64 // 用量队列满、退回同步写
	writeBehindUsageFailures   atomic.Int64 // 单条用量最终写入失败
	writeBehindCounterFailures atomic.Int64 // 额度 / 最后使用时间写回失败（会重试）
}

const (
	statusClass2xx = iota
	statusClass4xx
	statusClass5xx
	statusClassOther
)

var statusClassNames = [4]string{"2xx", "4xx", "5xx", "other"}

func statusClassOf(status int) int {
	switch {
	case status >= 200 && status < 300:
		return statusClass2xx
	case status >= 400 && status < 500:
		return statusClass4xx
	case status >= 500 && status < 600:
		return statusClass5xx
	}
	return statusClassOther
}

const (
	cooldownReasonHTTP = iota
	cooldownReasonTransport
	cooldownReasonFirstTokenTimeout
)

var cooldownReasonNames = [3]string{"http", "transport", "first_token_timeout"}

func (m *Metrics) observeRequest(status int, d time.Duration) {
	if m == nil {
		return
	}
	c := statusClassOf(status)
	m.requests[c].Add(1)
	m.durationMS[c].Add(d.Milliseconds())
}

func (m *Metrics) streamStarted() {
	if m != nil {
		m.streamsInflight.Add(1)
	}
}

func (m *Metrics) streamEnded() {
	if m != nil {
		m.streamsInflight.Add(-1)
	}
}

// streamFailed 已向客户端写出后流中途出错（客户端自己断开不算）。
func (m *Metrics) streamFailed() {
	if m != nil {
		m.streamErrors.Add(1)
	}
}

func (m *Metrics) upstreamStreamReset() {
	if m != nil {
		m.upstreamResets.Add(1)
	}
}

func (m *Metrics) syncFallback() {
	if m != nil {
		m.writeBehindSyncFallbacks.Add(1)
	}
}

func (m *Metrics) usageWriteFailed() {
	if m != nil {
		m.writeBehindUsageFailures.Add(1)
	}
}

func (m *Metrics) counterFlushFailed() {
	if m != nil {
		m.writeBehindCounterFailures.Add(1)
	}
}

// observeAttempt 一次上游尝试的结果；errType 是用量记录里的错误类型，status 是上游状态码。
func (m *Metrics) observeAttempt(success bool, errType string, status int, firstTokenTimeout bool) {
	if m == nil {
		return
	}
	key := "success"
	switch {
	case success:
	case firstTokenTimeout:
		key = "first_token_timeout"
	case errType == "transport" || errType == "config" || errType == "client":
		key = errType
	case status == 429:
		key = "http_429"
	case status >= 500:
		key = "http_5xx"
	case status >= 400:
		key = "http_4xx"
	default:
		key = "other"
	}
	m.attemptsMu.Lock()
	if m.attempts == nil {
		m.attempts = map[string]int64{}
	}
	m.attempts[key]++
	m.attemptsMu.Unlock()
}

func (m *Metrics) routeCooledDown(reason int) {
	if m != nil && reason >= 0 && reason < len(m.routeCooldowns) {
		m.routeCooldowns[reason].Add(1)
	}
}

func (m *Metrics) routeAutoDisabled() {
	if m != nil {
		m.routeAutoDisables.Add(1)
	}
}

func (m *Metrics) cooldownBorrowed() {
	if m != nil {
		m.routeCooldownBorrow.Add(1)
	}
}

// MetricsSnapshot 指标快照（管理 API 的 JSON）。
type MetricsSnapshot struct {
	Requests             map[string]int64   `json:"requests"`         // 按状态码档
	RequestDuration      map[string]int64   `json:"request_duration"` // 各档累计耗时（毫秒），除以请求数即平均
	StreamsInflight      int64              `json:"streams_inflight"`
	StreamErrors         int64              `json:"stream_errors"`
	Attempts             map[string]int64   `json:"attempts"`
	RouteCooldowns       map[string]int64   `json:"route_cooldowns"`
	RouteAutoDisabled    int64              `json:"route_auto_disabled"`
	RouteCooldownBorrows int64              `json:"route_cooldown_borrows"`
	UpstreamStreamResets int64              `json:"upstream_stream_resets"`
	RoutesPaused         int64              `json:"routes_paused"`        // 当前在冷却中的路由数
	RoutesAutoDisabled   int64              `json:"routes_auto_disabled"` // 当前自动禁用中的路由数
	WriteBehind          WriteBehindMetrics `json:"write_behind"`
}

// WriteBehindMetrics 后台批量写入的状态。
type WriteBehindMetrics struct {
	Enabled              bool  `json:"enabled"`
	UsageQueueDepth      int   `json:"usage_queue_depth"`
	UsageQueueCapacity   int   `json:"usage_queue_capacity"`
	PendingKeys          int   `json:"pending_keys"` // 尚未写回额度 / 最后使用时间的密钥数
	SyncFallbacks        int64 `json:"sync_fallbacks"`
	UsageWriteFailures   int64 `json:"usage_write_failures"`
	CounterFlushFailures int64 `json:"counter_flush_failures"`
}

func (m *Metrics) snapshot() MetricsSnapshot {
	s := MetricsSnapshot{
		Requests:        map[string]int64{},
		RequestDuration: map[string]int64{},
		Attempts:        map[string]int64{},
		RouteCooldowns:  map[string]int64{},
	}
	if m == nil {
		return s
	}
	for i, name := range statusClassNames {
		s.Requests[name] = m.requests[i].Load()
		s.RequestDuration[name] = m.durationMS[i].Load()
	}
	s.StreamsInflight = m.streamsInflight.Load()
	s.StreamErrors = m.streamErrors.Load()
	m.attemptsMu.Lock()
	for k, v := range m.attempts {
		s.Attempts[k] = v
	}
	m.attemptsMu.Unlock()
	for i, name := range cooldownReasonNames {
		s.RouteCooldowns[name] = m.routeCooldowns[i].Load()
	}
	s.RouteAutoDisabled = m.routeAutoDisables.Load()
	s.RouteCooldownBorrows = m.routeCooldownBorrow.Load()
	s.UpstreamStreamResets = m.upstreamResets.Load()
	s.WriteBehind = WriteBehindMetrics{
		SyncFallbacks:        m.writeBehindSyncFallbacks.Load(),
		UsageWriteFailures:   m.writeBehindUsageFailures.Load(),
		CounterFlushFailures: m.writeBehindCounterFailures.Load(),
	}
	return s
}

// Snapshot 当前指标（含后台写入队列状态与冷却中的路由数）。
func (s *Service) Snapshot() MetricsSnapshot {
	snap := s.metrics.snapshot()
	if w := s.writer.Load(); w != nil {
		snap.WriteBehind.Enabled = true
		snap.WriteBehind.UsageQueueDepth = len(w.usageCh)
		snap.WriteBehind.UsageQueueCapacity = cap(w.usageCh)
		w.mu.Lock()
		snap.WriteBehind.PendingKeys = len(w.quota)
		w.mu.Unlock()
	}
	if s.Routes != nil {
		if paused, disabled, err := s.Routes.CountUnschedulable(time.Now()); err == nil {
			snap.RoutesPaused, snap.RoutesAutoDisabled = paused, disabled
		}
	}
	return snap
}

// MetricsHandler GET /metrics：Prometheus 文本格式。网关设置里的 metricsToken 留空时不开放（404），
// 否则要求 Authorization: Bearer <metricsToken>。
func (s *Service) MetricsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		token := strings.TrimSpace(s.gatewayRuntime().MetricsToken)
		if token == "" {
			c.AbortWithStatus(http.StatusNotFound)
			return
		}
		got := strings.TrimSpace(strings.TrimPrefix(c.GetHeader("Authorization"), "Bearer "))
		if subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		c.Header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		c.Status(http.StatusOK)
		s.WritePrometheus(c.Writer)
	}
}

// WritePrometheus 以 Prometheus 文本格式（0.0.4）写出指标。
func (s *Service) WritePrometheus(w io.Writer) {
	snap := s.Snapshot()
	line := func(format string, a ...any) { _, _ = fmt.Fprintf(w, format+"\n", a...) }
	counterByLabel := func(name, help, label string, values map[string]int64) {
		line("# HELP %s %s", name, help)
		line("# TYPE %s counter", name)
		keys := make([]string, 0, len(values))
		for k := range values {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			line("%s{%s=%q} %d", name, label, k, values[k])
		}
	}
	gauge := func(name, help string, v int64) {
		line("# HELP %s %s", name, help)
		line("# TYPE %s gauge", name)
		line("%s %d", name, v)
	}
	counter := func(name, help string, v int64) {
		line("# HELP %s %s", name, help)
		line("# TYPE %s counter", name)
		line("%s %d", name, v)
	}
	counterByLabel("gateway_requests_total", "网关请求数，按响应状态码档", "class", snap.Requests)
	counterByLabel("gateway_request_duration_ms_total", "网关请求累计耗时（毫秒），按响应状态码档", "class", snap.RequestDuration)
	gauge("gateway_streams_inflight", "正在转发的流式请求数", snap.StreamsInflight)
	counter("gateway_stream_errors_total", "已向客户端写出后中途出错的流数", snap.StreamErrors)
	counterByLabel("gateway_upstream_attempts_total", "上游尝试数，按结果", "result", snap.Attempts)
	counterByLabel("gateway_route_cooldowns_total", "路由进入冷却的次数，按原因", "reason", snap.RouteCooldowns)
	counter("gateway_route_auto_disables_total", "路由被自动禁用的次数", snap.RouteAutoDisabled)
	counter("gateway_route_cooldown_borrows_total", "所有路由都在冷却时借用最早到期路由的次数", snap.RouteCooldownBorrows)
	counter("gateway_upstream_stream_resets_total", "收到协议终态后上游在宽限期内没结束、被网关重置的流数", snap.UpstreamStreamResets)
	gauge("gateway_routes_paused", "当前在冷却中的路由数", snap.RoutesPaused)
	gauge("gateway_routes_auto_disabled", "当前自动禁用中的路由数", snap.RoutesAutoDisabled)
	wb := snap.WriteBehind
	enabled := int64(0)
	if wb.Enabled {
		enabled = 1
	}
	gauge("gateway_write_behind_enabled", "后台批量写入是否开启", enabled)
	gauge("gateway_write_behind_usage_queue_depth", "待写入的用量日志条数", int64(wb.UsageQueueDepth))
	gauge("gateway_write_behind_usage_queue_capacity", "用量日志队列容量", int64(wb.UsageQueueCapacity))
	gauge("gateway_write_behind_pending_keys", "尚未写回额度 / 最后使用时间的密钥数", int64(wb.PendingKeys))
	counter("gateway_write_behind_sync_fallbacks_total", "用量队列满、退回同步写的次数", wb.SyncFallbacks)
	counter("gateway_write_behind_usage_write_failures_total", "用量日志最终写入失败的条数", wb.UsageWriteFailures)
	counter("gateway_write_behind_counter_flush_failures_total", "额度 / 最后使用时间写回失败的次数", wb.CounterFlushFailures)
}
