// 数据面：一次转发的上下文。
//
// 分三层，生命周期各不相同：
//   - relayRequest：请求级。鉴权、读 body、扫字段、准备好候选路由后就不再变，整个请求共用；
//   - routeChoice：选中的一条路由。同一路由上的重试共用（上游模型、倍率、首字超时）；
//   - relayAttempt：一次上游尝试。每发一次请求新建一份，各记一条用量。
//
// 以前这些值是 handleForward 里二十多个局部变量，再以参数传给 recordUsage（19 个）
// 与各转发函数（10～13 个）。参考 new-api 的 RelayInfo，但按生命周期拆开，不堆成一个大结构体。
package gateway

import (
	"context"
	"net/http"
	"time"

	"github.com/bejix/upstream-ops/backend/connector"
	"github.com/bejix/upstream-ops/backend/gateway/pricing"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

// relayRequest 一次客户端请求（请求级，准备好后只读）。
type relayRequest struct {
	c  *gin.Context
	in forwardRequest

	id    string // 网关 request id
	key   *storage.GatewayKey
	group *storage.GatewayGroup

	body            []byte // 已注入系统提示词、应用服务等级规则
	contentType     string
	model           string // 客户端请求的模型
	stream          bool
	serviceTier     string
	reasoningEffort string
	thinkingEnabled bool
	billing         pricing.BillingInput

	// sessionKey 会话标识（哈希，空表示认不出），用于会话粘性
	sessionKey        string
	routes            []storage.GatewayRoute // 已按转发模型过滤
	groupsByChannel   map[uint][]connector.APIKeyGroup
	groupMapping      ModelMap
	prefs             routePreferences
	query             string // 透传给上游的查询串
	geminiPassthrough bool   // Gemini 原生透传：不跨协议，只能交给 Gemini 上游
	// convCtx 跨协议转换时下载的图片等媒体：整个请求共享缓存与预算，换路由不重复下载
	convCtx context.Context
	policy  retryPolicy
}

// retryPolicy 组级重试策略（已规范化）。
type retryPolicy struct {
	enabled          bool
	sameRouteRetries int // 同路由重试次数（不含首次）
	failoverEnabled  bool
	failoverMax      int // 最多顺延（换路由）几次
	cooldownSec      int
	// firstTokenTimeoutSec 组配置的首字超时（秒，0=关闭）；单次尝试实际用的见 routeChoice.firstTokenTimeout
	firstTokenTimeoutSec int
	firstTokenTimeout    time.Duration
}

// retryPolicyFor 读组配置：负数归零；重试总开关关闭时不重试、不顺延，失败直接回显。
func (rt *Runtime) retryPolicyFor(group *storage.GatewayGroup) retryPolicy {
	p := retryPolicy{
		enabled:              group.RetryEnabled,
		sameRouteRetries:     max(group.RetryCount, 0),
		failoverEnabled:      group.FailoverEnabled,
		failoverMax:          max(group.FailoverMax, 0),
		cooldownSec:          max(group.CooldownSeconds, 0),
		firstTokenTimeoutSec: clampFirstTokenTimeoutSec(group.FirstTokenTimeoutSec),
	}
	if p.firstTokenTimeoutSec > 0 {
		p.firstTokenTimeout = time.Duration(p.firstTokenTimeoutSec) * time.Second
	}
	if !p.enabled {
		p.sameRouteRetries = 0
		p.failoverEnabled = false
		p.failoverMax = 0
	}
	return p
}

// routeChoice 选中的一条路由（同路由重试共用）。
type routeChoice struct {
	route         storage.GatewayRoute
	rate          float64 // 有效倍率（排序 / 计费）
	billingRate   float64 // 账号计费倍率
	upstreamModel string
	chain         string // 模型映射链（写用量）
	// reasoningEffort 映射后的上游模型可能是 kimi/glm 等：thinking 开了但无档位时补 high
	reasoningEffort string
	// firstTokenTimeout 本路由实际用的首字超时：没有可顺延的下家时关闭（0）
	firstTokenTimeout time.Duration
	// stripThinking 上游回了 thinking 签名错误：本路由后续尝试把 thinking 块降级成文本（见 thinking_retry.go）
	stripThinking bool
}

// newRouteChoice 按组与路由的模型映射算出上游模型。
func newRouteChoice(req *relayRequest, cand ScoredRoute) *routeChoice {
	choice := &routeChoice{route: cand.Route, rate: cand.EffectiveRate, billingRate: cand.BillingRate}
	choice.upstreamModel, choice.chain = ResolveModel(req.model, cachedModelMapping(cand.Route.ModelMappingJSON), req.groupMapping)
	if choice.upstreamModel == "" {
		choice.upstreamModel = req.model
	}
	return choice
}

// relayAttempt 一次上游尝试。
type relayAttempt struct {
	*routeChoice
	no   int    // 全局尝试序号（写入 usage.attempt，同一 request_id 关联）
	kind string // primary / retry / failover

	target       *upstreamTarget // 解析失败时为 nil
	upstreamKind protocolKind
	plan         *conversionPlan
	call         upstreamCall
	// meta 随阶段补全：解析目标 → 定上游协议 → 转换完成（失败时只记已知的部分）
	meta usageRecordMeta
	// routeDisabled 这次失败让路由被自动停用（写进重试轨迹）
	routeDisabled bool
}

// upstreamCall 发往上游的一次 HTTP 调用。
type upstreamCall struct {
	target *upstreamTarget
	path   string
	method string
	header http.Header // 客户端请求头（只挑部分转发，见 buildUpstreamHTTPRequest）
	body   []byte
	kind   protocolKind // 上游协议
	stream bool
	// firstTokenTimeout 0=关闭；从发起请求起算到首字节的总等待
	firstTokenTimeout time.Duration
}

// attemptOutcome 一次尝试的上游结果。非流式只填 Status / Headers / Body / FirstTokenMS / Err。
type attemptOutcome struct {
	streamAttemptResult
	durationMS int64
}

// failed 未拿到可用的上游响应（传输错误或非 2xx）。
func (o *attemptOutcome) failed() bool {
	return o.Err != nil || o.Status >= 400
}

// attemptNext 一次尝试之后怎么走。
type attemptNext int

const (
	attemptProceed   attemptNext = iota // 准备完成，继续发请求（仅 prepareAttempt 返回）
	attemptDone                         // 请求结束：成功已回包，或要回的错误在 failoverState.reply
	attemptRetry                        // 同一路由再试一次
	attemptNextRoute                    // 本路由结束，看能否顺延
	attemptGiveUp                       // 不再尝试，回最后一次失败
)

// failoverState 尝试循环的进度与最后一次失败。
type failoverState struct {
	exclude       map[uint]struct{} // 已尝试或跳过的路由
	routesTried   int               // 已用过的路由数
	failoversDone int               // 已顺延次数（换到另一条路由的次数）
	attemptNo     int

	lastStatus int
	lastBody   []byte // 已转成客户端协议的上游错误体
	lastErr    error
	// lastEcho 最后一次失败是「换路由」类（账号级 / 4xx 顺延）：没有下家时原样回上游的状态码、错误体与响应头，
	// 与不顺延时直接回显一致（错误体为空也照回，而不是改成 502）
	lastEcho    bool
	lastHeaders http.Header

	// reply 尝试以 attemptDone 结束且需要回错误时（不可重试的上游错误、请求本身有误）要回的错误；成功时为 nil
	reply *gatewayError
}

// fail 结束请求并回 e。
func (st *failoverState) fail(e *gatewayError) attemptNext {
	st.reply = e
	return attemptDone
}

// lastFailure 所有尝试都没成功时回什么：有上游错误体就原样回（已转成客户端协议），否则按最后的错误回网关错误。
func (st *failoverState) lastFailure() *gatewayError {
	if st.lastStatus > 0 && st.lastEcho {
		return upstreamError(st.lastStatus, st.lastBody, st.lastHeaders)
	}
	if st.lastStatus > 0 && len(st.lastBody) > 0 {
		return upstreamError(st.lastStatus, st.lastBody, nil)
	}
	msg := "all upstream routes failed"
	status := http.StatusBadGateway
	errType := "api_error"
	if st.lastErr != nil {
		msg = st.lastErr.Error()
		if isProtocolConvertError(st.lastErr) {
			status, errType = http.StatusBadRequest, "invalid_request_error"
		}
	}
	return newGatewayError(status, errType, msg)
}

// canFailover 还能换到下一条路由。
func (st *failoverState) canFailover(p retryPolicy) bool {
	return p.failoverEnabled && st.failoversDone < p.failoverMax
}

// usageOutcome 一条用量记录的结果部分。
type usageOutcome struct {
	tokens       pricing.UsageTokens
	status       int
	success      bool
	errInfo      usageErrorInfo
	durationMS   int64
	firstTokenMS *int64
	// err 本次尝试的传输错误（只用来给指标分类，不落库）
	err error
}

// usageRecord 一条用量记录的全部输入（见 recordUsage）。
type usageRecord struct {
	key            *storage.GatewayKey
	group          *storage.GatewayGroup
	route          *storage.GatewayRoute
	target         *upstreamTarget // 可为 nil（目标解析失败）
	requestID      string
	requestedModel string
	upstreamModel  string
	chain          string
	rate           float64
	billingRate    float64
	stream         bool
	// upstreamKind 上游协议：决定用量要不要拆桶（见 recordUsage）；目标解析失败时为空
	upstreamKind protocolKind
	meta         usageRecordMeta
	usageOutcome
}

// recordAttempt 为一次转发尝试记一条用量，并计入上游尝试指标；成功时把会话绑到这条路由。
func (rt *Runtime) recordAttempt(req *relayRequest, at *relayAttempt, out usageOutcome) {
	rt.metrics.observeAttempt(out.success, out.errInfo.Type, out.status, isFirstTokenTimeout(out.err))
	if out.success {
		if req.key != nil && req.group != nil {
			rt.sessions.bind(req.group.ID, req.key.ID, req.sessionKey, at.route.ID)
		}
		rt.modelGaps.clear(at.route.ID, req.model)
	}
	rt.recordUsage(req.c, usageRecord{
		key:            req.key,
		group:          req.group,
		route:          &at.route,
		target:         at.target,
		requestID:      req.id,
		requestedModel: req.model,
		upstreamModel:  at.upstreamModel,
		chain:          at.chain,
		rate:           at.rate,
		billingRate:    at.billingRate,
		stream:         req.stream,
		upstreamKind:   at.upstreamKind,
		meta:           at.meta,
		usageOutcome:   out,
	})
}
