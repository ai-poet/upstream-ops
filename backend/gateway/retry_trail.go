// 数据面：重试轨迹。
//
// 每次失败的尝试记下「失败分类」与「网关接下来做了什么」（usage 的 failover_reason / next_action），
// 同一 request_id 的尝试按顺序串起来，就是这个请求的重试轨迹：
// 例如 #1 thinking_signature → retry_without_thinking → #2 成功；
// #1 account_balance → cooldown,switch_route → #2 成功。
// 以前详情里只有分类时顺手记的 failover_decision，和实际动作对不上（签名补救时会同时出现 http_400）。
package gateway

import (
	"strings"
	"time"
)

// 失败分类里 classifyFailure 之外的几种。
const (
	failoverReasonThinkingSignature = "thinking_signature"
	failoverReasonConfig            = "config"
	failoverReasonProtocolConvert   = "protocol_convert"
)

// 失败之后的动作。路由层面的处置（冷却 / 停用）排在前面，请求层面的去向排在最后。
const (
	nextActionRetrySameRoute       = "retry_same_route"       // 同路由重试
	nextActionRetryWithoutThinking = "retry_without_thinking" // 去掉 thinking 块后同路由重试
	nextActionSwitchRoute          = "switch_route"           // 换下一条路由
	nextActionGiveUp               = "give_up"                // 没有可换的路由或顺延次数用完：错误回给客户端
	nextActionStop                 = "stop"                   // 不可重试（或组关了重试）：错误直接回给客户端
	nextActionClientGone           = "client_gone"            // 客户端已断开：不再重试
	nextActionCooldown             = "cooldown"               // 路由进入冷却（截止时间见 cooldown_until）
	nextActionDisableRoute         = "disable_route"          // 路由被自动停用
)

// failoverNextAction 不在本路由重试时请求的去向：还能顺延且有下一条可调度路由就换路由，否则把错误回给客户端。
// 当前路由已在 exclude 里（见 relayWithFailover）。
func (rt *Runtime) failoverNextAction(req *relayRequest, st *failoverState) string {
	if !req.policy.enabled || !st.canFailover(req.policy) {
		return nextActionGiveUp
	}
	direction := ""
	if req.group != nil {
		direction = req.group.RateSortDirection
	}
	if len(SortRoutes(req.routes, req.groupsByChannel, direction, time.Now(), st.exclude)) == 0 {
		return nextActionGiveUp
	}
	return nextActionSwitchRoute
}

// markTrail 把失败分类与动作写进本次尝试的用量（结构化字段 + 详情末尾一行），路由被冷却 / 停用时排在动作前面。
func (rt *Runtime) markTrail(at *relayAttempt, info *usageErrorInfo, reason, action string) {
	steps := make([]string, 0, 2)
	switch {
	case at.routeDisabled:
		steps = append(steps, nextActionDisableRoute)
	case at.meta.CooldownUntil != nil:
		steps = append(steps, nextActionCooldown)
	}
	if action != "" {
		steps = append(steps, action)
	}
	at.meta.FailoverReason = reason
	at.meta.NextAction = strings.Join(steps, ",")
	if info == nil {
		return
	}
	var b strings.Builder
	b.WriteString(strings.TrimRight(info.Detail, "\n"))
	if reason != "" {
		b.WriteString("\nfailover_reason: ")
		b.WriteString(reason)
	}
	if at.meta.NextAction != "" {
		b.WriteString("\nnext_action: ")
		b.WriteString(at.meta.NextAction)
	}
	b.WriteString("\n")
	info.Detail = b.String()
}
