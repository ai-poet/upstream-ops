// 数据面：一次上游尝试。
//
// 解析目标 → 定上游协议 → 转换请求 → 发送 → 成功回包 / 已提交的流收尾 / 失败分类。
// 每次尝试记一条用量；返回值告诉 tryRoute 接下来怎么走（见 attemptNext）。
package gateway

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
)

// runAttempt 一次上游尝试；budget 是本路由剩下的重试次数（失败后由它决定重试还是收尾、冷却）。
func (rt *Runtime) runAttempt(req *relayRequest, at *relayAttempt, st *failoverState, budget *routeTryBudget) attemptNext {
	at.meta = usageRecordMeta{
		InboundEndpoint: req.in.Path,
		InboundProtocol: string(req.in.Kind),
		ServiceTier:     req.serviceTier,
		ReasoningEffort: at.reasoningEffort,
		Attempt:         at.no,
		AttemptKind:     at.kind,
	}
	if next := rt.prepareAttempt(req, at, st); next != attemptProceed {
		return next
	}
	out := rt.sendAttempt(req, at)
	// 已向客户端写出 SSE：不能再重试/顺延，直接记账并结束本次请求。
	if req.stream && out.Committed {
		rt.finishCommittedStream(req, at, out)
		return attemptDone
	}
	if out.failed() {
		return rt.handleAttemptFailure(req, at, st, out, budget)
	}
	rt.writeAttemptSuccess(req, at, out)
	return attemptDone
}

// prepareAttempt 解析上游目标、定上游协议、按协议转换请求。
// 路由配置有误或协议接不住时记一条配置错误并换路由；请求本身有误时结束请求、回 400。
func (rt *Runtime) prepareAttempt(req *relayRequest, at *relayAttempt, st *failoverState) attemptNext {
	c, kind, path := req.c, req.in.Kind, req.in.Path
	target, err := rt.resolveUpstreamTarget(&at.route)
	if err != nil {
		st.lastErr = err
		rt.recordAttempt(req, at, usageOutcome{errInfo: usageErrorInfo{
			Type:    "config",
			Summary: err.Error(),
			Detail:  fmt.Sprintf("config error\nroute_id: %d\nsource_kind: %s\nerror: %s\n", at.route.ID, at.route.NormalizeSourceKind(), err.Error()),
		}})
		// 配置错误：不再重试同路由，进入顺延判断
		return attemptNextRoute
	}
	at.target = target
	// 转发：组+路由 UA 三选一（透传 / 组 / 自定义）
	applyRouteUserAgent(target, req.group, &at.route)

	// 协议：路由显式优先；route=auto 且为 provider 时用 provider 默认
	at.upstreamKind = routeUpstreamKind(&at.route, target, kind, at.upstreamModel)
	// 透传端点不跨协议：上游协议跟随入站（Gemini 透传的路由已在选路时过滤）
	if protocol.IsPassthroughEndpoint(path) {
		at.upstreamKind = protocol.NormalizeKind(kind)
	}
	// legacy completions 不跨协议
	if protocol.IsOpenAIFamily(kind) && strings.Contains(path, "/completions") && !strings.Contains(path, "/chat/") {
		if at.upstreamKind == protocolAnthropic || at.upstreamKind == protocol.KindOpenAIResponses || at.upstreamKind == protocol.KindGemini {
			return st.fail(newGatewayError(http.StatusBadRequest, "invalid_request_error",
				"protocol conversion for /v1/completions is not supported; use /v1/chat/completions"))
		}
		at.upstreamKind = protocol.KindOpenAIChat
	}

	body := req.body
	if at.upstreamModel != req.model && isMultipartContentType(req.contentType) {
		body = rewriteMultipartModel(req.contentType, req.body, at.upstreamModel)
	}
	fwdBody, upstreamPath, plan, convErr := planUpstreamRequest(req.convCtx, body, kind, at.upstreamKind, req.model, at.upstreamModel, req.stream, path)
	if convErr != nil {
		// 这条路由的上游协议接不住该请求：记一条配置错误，换下一条路由
		st.lastErr = convErr
		st.lastStatus = 0
		at.meta.UpstreamProtocol = string(at.upstreamKind)
		at.meta.ProtocolConverted = true
		rt.recordAttempt(req, at, usageOutcome{errInfo: usageErrorInfo{
			Type:    "config",
			Summary: convErr.Error(),
			Detail:  fmt.Sprintf("protocol convert failed\nroute_id: %d\ninbound: %s\nupstream: %s\nerror: %s\n", at.route.ID, kind, at.upstreamKind, convErr.Error()),
		}})
		// 请求里的图片 / 文件下载失败：换哪条路由都一样，直接回 400
		if mediaErr := mediaFetchFailure(req.convCtx); mediaErr != nil {
			return st.fail(newGatewayError(http.StatusBadRequest, "invalid_request_error", "failed to load media from the request: "+mediaErr.Error()))
		}
		return attemptNextRoute
	}
	plan.GeminiArray = req.in.GeminiArray
	// 上次在这条路由上撞了 thinking 签名错误：去掉 thinking 块再发（同 sub2api FilterThinkingBlocksForRetry）
	if at.stripThinking && protocol.NormalizeKind(at.upstreamKind) == protocol.KindAnthropic {
		fwdBody = filterThinkingBlocksForRetry(fwdBody, at.upstreamModel)
	}
	// 查询串：没做协议转换（同协议、透传端点）时原样带给上游，如 Anthropic 的 ?beta=true、Azure 的 ?api-version=；
	// 跨协议时上游端点换了，原查询串对它没有意义，不带。网关自己的 ?key= / ?api_key= 已在 forwardQuery 去掉
	if req.query != "" && (!plan.Converted || protocol.IsPassthroughEndpoint(path)) {
		upstreamPath = appendQuery(upstreamPath, req.query)
	}

	at.plan = plan
	at.call = upstreamCall{
		target:            target,
		path:              upstreamPath,
		method:            c.Request.Method,
		header:            c.Request.Header,
		body:              fwdBody,
		kind:              at.upstreamKind,
		stream:            req.stream,
		firstTokenTimeout: at.firstTokenTimeout,
	}
	at.meta.UpstreamEndpoint = upstreamPath
	at.meta.UpstreamProtocol = string(at.upstreamKind)
	at.meta.ProtocolConverted = plan.Converted
	at.meta.UpstreamURL = joinUpstreamURL(target.BaseURL, upstreamPath)
	at.meta.BillingInput = req.billing
	return attemptProceed
}

// sendAttempt 发给上游。流式边读上游 SSE 边写客户端（Committed 后禁止 retry/failover）；非流式整包读回。
func (rt *Runtime) sendAttempt(req *relayRequest, at *relayAttempt) attemptOutcome {
	ctx := req.c.Request.Context()
	start := time.Now()
	var out attemptOutcome
	if req.stream {
		rt.metrics.streamStarted()
		out.streamAttemptResult = rt.forwardStreamPlan(ctx, req.c, at.call, at.plan)
		rt.metrics.streamEnded()
	} else {
		r := &out.streamAttemptResult
		r.Status, r.Headers, r.Body, r.FirstTokenMS, r.Err = rt.forwardOnce(ctx, at.call)
	}
	out.durationMS = time.Since(start).Milliseconds()
	return out
}

// writeAttemptSuccess 非流式成功：整包转换后写出，恢复路由调度并记账。
func (rt *Runtime) writeAttemptSuccess(req *relayRequest, at *relayAttempt, out attemptOutcome) {
	c := req.c
	tokens := parseUsageByKind(out.Body, false, at.upstreamKind)
	at.meta.BillingInput = req.billing.WithResponse(out.Body)
	clientBody := rt.convertResponseWithPlan(c.Request.Context(), out.Body, at.plan)
	// 路由开了「生图结果 url 转 base64」：只有 url 的图片由网关下载回填 b64_json（同 sub2api）
	if at.route.ImagesURLToB64JSON && isImagesResultEndpoint(req.in.Path) && !clientWantsImageURL(req.contentType, req.body) {
		clientBody = rt.backfillImagesB64JSON(c.Request.Context(), req.id, clientBody)
	}
	if at.plan.Converted && rt.Log != nil && len(clientBody) == 0 && len(out.Body) > 0 {
		rt.Log.Warn("response convert produced empty body")
	}
	if protocol.IsPassthroughEndpoint(req.in.Path) && c.Request.Method != http.MethodGet {
		rt.tasks.remember(req.group.ID, at.route.ID, taskIDsFromResponse(out.Body))
	}
	copyResponseHeaders(c.Writer.Header(), out.Headers)
	if at.plan.Converted {
		c.Writer.Header().Del("Content-Length")
		c.Header("Content-Type", "application/json")
	}
	c.Status(out.Status)
	_, _ = c.Writer.Write(clientBody)
	// 成功：立刻恢复调度；连续成功达到阈值后自动清除错误残留展示
	rt.noteRouteSuccess(&at.route)
	rt.recordAttempt(req, at, usageOutcome{
		tokens: tokens, status: out.Status, success: true,
		durationMS: out.durationMS, firstTokenMS: out.FirstTokenMS,
	})
}

// finishCommittedStream 已向客户端写出 SSE 的流（成功、出错或客户端断开）：只记账，不再重试/顺延。
// 客户端在 commit 后、终端帧交付前断开：记 success=true + error_type=client。
// 若已写出 [DONE]/message_stop 后客户端才关连接，forwardStream 会清掉 client 标记，记普通成功。
func (rt *Runtime) finishCommittedStream(req *relayRequest, at *relayAttempt, out attemptOutcome) {
	at.meta.BillingInput = req.billing.WithResponse(out.Body)
	onlyClientDisconnect := isClientDisconnectAfterCommit(out.ClientDisconnected, out.StreamErr)
	// 仅客户端断开仍算业务成功；真实 stream 错误才算失败。
	success := out.StreamErr == nil || onlyClientDisconnect
	errInfo := rt.committedStreamErrorInfo(req, at, out, onlyClientDisconnect)
	if success {
		rt.noteRouteSuccess(&at.route)
	} else {
		rt.metrics.streamFailed()
	}
	if !success && rt.Log != nil {
		rt.Log.Warn("gateway stream ended with error after commit",
			"request_id", req.id,
			"attempt", at.no,
			"route_id", at.route.ID,
			"upstream_url", at.meta.UpstreamURL,
			"err", errInfo.Summary,
			"client_disconnected", out.ClientDisconnected,
			"input_tokens", out.Tokens.InputTokens,
			"output_tokens", out.Tokens.OutputTokens,
		)
	}
	// status 通常为 200；保留已 drain 的 tokens（含客户端断开后的用量同步）。
	status := out.Status
	if status == 0 {
		status = http.StatusOK
	}
	rt.recordAttempt(req, at, usageOutcome{
		tokens: out.Tokens, status: status, success: success, errInfo: errInfo,
		durationMS: out.durationMS, firstTokenMS: out.FirstTokenMS,
	})
}

// committedStreamErrorInfo 已提交的流的错误说明：客户端断开记 client，流中途出错记 transport，正常结束为空。
func (rt *Runtime) committedStreamErrorInfo(req *relayRequest, at *relayAttempt, out attemptOutcome, onlyClientDisconnect bool) usageErrorInfo {
	if !onlyClientDisconnect && out.StreamErr == nil {
		return usageErrorInfo{}
	}
	method, upstreamURL := req.c.Request.Method, at.meta.UpstreamURL
	gwCfg := rt.gatewayRuntime()
	headerJSON := formatDebugHeaders(out.Headers, gwCfg.UsageErrorHeadersJSONBytes, gwCfg.UsageErrorHeaderValueRunes)
	headerPlain := formatHeadersPlain(out.Headers, gwCfg.UsageErrorHeaderValueRunes)
	var detail strings.Builder
	if !onlyClientDisconnect {
		fmt.Fprintf(&detail,
			"stream error after commit\nmethod: %s\nurl: %s\nerror: %s\nclient_disconnected: %v\n",
			method, upstreamURL, out.StreamErr.Error(), out.ClientDisconnected,
		)
		appendUpstreamHeadersToDetail(&detail, headerPlain)
		return usageErrorInfo{
			Type:            "transport",
			Summary:         out.StreamErr.Error(),
			Detail:          detail.String(),
			UpstreamHeaders: headerJSON,
		}
	}

	tokens := out.Tokens
	fmt.Fprintf(&detail,
		"client disconnected after stream commit\nmethod: %s\nurl: %s\nnote: 已继续读取上游以同步 usage/计费；上游可能仍完整计费\n",
		method, upstreamURL,
	)
	if out.StreamErr != nil {
		fmt.Fprintf(&detail, "stream_err: %s\n", out.StreamErr.Error())
	}
	fmt.Fprintf(&detail,
		"tokens: input=%d output=%d cache_read=%d cache_creation=%d\n",
		tokens.InputTokens, tokens.OutputTokens, tokens.CacheReadTokens, tokens.CacheCreationTokens,
	)
	appendUpstreamHeadersToDetail(&detail, headerPlain)
	if rt.Log != nil {
		rt.Log.Info("gateway stream client disconnected after commit",
			"request_id", req.id,
			"attempt", at.no,
			"route_id", at.route.ID,
			"upstream_url", upstreamURL,
			"input_tokens", tokens.InputTokens,
			"output_tokens", tokens.OutputTokens,
			"cache_read", tokens.CacheReadTokens,
		)
	}
	return usageErrorInfo{
		Type:            "client",
		Summary:         "客户端主动断开（已尽量同步上游用量）",
		Detail:          detail.String(),
		UpstreamHeaders: headerJSON,
	}
}

// handleAttemptFailure 上游失败（未提交给客户端）：分类后决定回显、同路由重试、顺延或放弃，
// 必要时暂停 / 停用路由，并记一条用量。
func (rt *Runtime) handleAttemptFailure(req *relayRequest, at *relayAttempt, st *failoverState, out attemptOutcome, budget *routeTryBudget) attemptNext {
	c := req.c
	errInfo := rt.attemptErrorInfo(req, at, out)
	// 客户端断开 / 请求 context 已取消：父 context 污染会导致后续重试/顺延全部变成
	// "context canceled"，且误伤路由冷却。此类错误应立刻停止，不再重试/顺延/冷却。
	clientCanceled := isClientContextError(out.Err, c) || out.ClientDisconnected
	if clientCanceled {
		annotateClientContextError(&errInfo, c, at.meta.UpstreamURL, c.Request.Method, out.Err)
	}
	decision := rt.classifyFailure(req.group, out.Status, out.Headers, out.Body, out.Err, req.in.TaskID != "", req.model, at.upstreamModel)
	if decision.Reason != "" && !clientCanceled {
		errInfo.Detail = strings.TrimRight(errInfo.Detail, "\n") + "\nfailover_decision: " + decision.Reason + "\n"
	}
	// 这条路由没有这个模型：10 分钟内同模型的请求把它排到最后（见 session_affinity.go）
	if decision.Action == failSkipRoute && decision.Reason == "model_unavailable" {
		rt.modelGaps.note(at.route.ID, req.model)
	}
	st.lastEcho, st.lastHeaders = false, nil
	if out.Err != nil {
		st.lastErr = out.Err
	} else {
		st.lastEcho, st.lastHeaders = decision.Action == failSwitchRoute, out.Headers
		st.lastStatus = out.Status
		st.lastBody = convertErrorBody(out.Body, req.in.Kind, at.upstreamKind, at.plan.Converted)
		st.lastErr = fmt.Errorf("upstream status %d: %s", out.Status, errInfo.Summary)
	}
	failed := usageOutcome{status: out.Status, errInfo: errInfo, durationMS: out.durationMS, firstTokenMS: out.FirstTokenMS, err: out.Err}

	// thinking 签名错误：去掉 thinking 块在同一路由补试一次，不占组配置的重试次数（同 sub2api）
	if !clientCanceled && shouldRetryWithoutThinking(at, out) {
		at.stripThinking = true
		failed.errInfo.Detail = strings.TrimRight(failed.errInfo.Detail, "\n") + "\nfailover_decision: thinking_signature_retry\n"
		rt.recordAttempt(req, at, failed)
		budget.backoff = 0
		return attemptRetry
	}

	// 不可重试的错误（默认 4xx、上下文超长等）：直接回显给客户端
	if decision.Action == failStop && !clientCanceled && out.Err == nil {
		rt.recordAttempt(req, at, failed)
		return st.fail(upstreamError(out.Status, st.lastBody, out.Headers))
	}

	// 还能不能在本路由重试：组配置的次数用完后，传输层瞬时错误还可补试一次
	transient := isTransientTransportError(out.Err)
	willRetry := !clientCanceled && req.policy.enabled && decision.Action == failRetry && budget.take(transient)
	if !clientCanceled { // 客户端取消不冷却
		at.meta.CooldownUntil = rt.pauseRouteAfterFailure(req, at, decision, errInfo, out.Status, !willRetry, transient)
	}
	rt.recordAttempt(req, at, failed)
	if rt.Log != nil {
		rt.Log.Warn("gateway upstream fail",
			"request_id", req.id,
			"attempt", at.no,
			"attempt_kind", at.kind,
			"route_id", at.route.ID,
			"status", out.Status,
			"upstream_url", at.meta.UpstreamURL,
			"err", errInfo.Summary,
			"decision", decision.Reason,
			"client_canceled", clientCanceled,
		)
	}
	// 客户端已取消：立刻结束，避免用已取消 context 继续打上游
	if clientCanceled || !req.policy.enabled {
		return attemptGiveUp
	}
	// 只有可重试错误才在同路由重试；其余直接顺延
	if willRetry {
		return attemptRetry
	}
	return attemptNextRoute
}

// attemptErrorInfo 失败尝试的错误说明；首字超时单独说明等了多久、可能已计费。
func (rt *Runtime) attemptErrorInfo(req *relayRequest, at *relayAttempt, out attemptOutcome) usageErrorInfo {
	method, upstreamURL := req.c.Request.Method, at.meta.UpstreamURL
	gwCfg := rt.gatewayRuntime()
	errInfo := rt.buildUpstreamErrorInfoCfg(gwCfg, out.Err, out.Status, out.Headers, out.Body, upstreamURL, method)
	if !isFirstTokenTimeout(out.Err) {
		return errInfo
	}
	errInfo.Type = "transport"
	appliedSec := int(at.firstTokenTimeout / time.Second)
	if appliedSec <= 0 {
		appliedSec = req.policy.firstTokenTimeoutSec
	}
	errInfo.Summary = fmt.Sprintf("首字超时（%ds）：未在限定时间内收到上游首字节，已主动断开", appliedSec)
	var detail strings.Builder
	fmt.Fprintf(&detail,
		"first token timeout\nmethod: %s\nurl: %s\ntimeout: %ds\nwaited: %dms\nnote: 上游可能已开始计费；将走重试/顺延，可能增加费用\n",
		method, upstreamURL, appliedSec, out.durationMS,
	)
	// 保留已拿到的上游响应头（若有）
	appendUpstreamHeadersToDetail(&detail, formatHeadersPlain(out.Headers, gwCfg.UsageErrorHeaderValueRunes))
	if errInfo.UpstreamHeaders == "" {
		errInfo.UpstreamHeaders = formatDebugHeaders(out.Headers, gwCfg.UsageErrorHeadersJSONBytes, gwCfg.UsageErrorHeaderValueRunes)
	}
	errInfo.Detail = detail.String()
	return errInfo
}

// pauseRouteAfterFailure 按失败分类停用路由，或在本路由最后一次尝试失败后进冷却；返回冷却截止时间（没冷却为 nil）。
// 传输层瞬时错误（transient）不立刻冷却：连续 transportFailuresBeforeCooldown 个请求都失败才冷却，
// 一次网络抖动不该让整条路由停摆。
func (rt *Runtime) pauseRouteAfterFailure(req *relayRequest, at *relayAttempt, decision failureDecision, errInfo usageErrorInfo, status int, lastTry, transient bool) *time.Time {
	reason := errInfo.Summary
	if strings.TrimSpace(errInfo.Detail) != "" {
		reason = truncateRunes(errInfo.Detail, 4000)
	}
	switch {
	case decision.Action == failSwitchRoute && decision.Cooldown > 0:
		return rt.pauseRouteForAccountFailure(req, at, decision, errInfo, reason)
	case decision.Action == failDisableRoute:
		rt.metrics.routeAutoDisabled()
		_ = rt.Routes.SetAutoDisabled(at.route.ID, reason, time.Now(), req.id)
		if rt.Log != nil {
			rt.Log.Warn("gateway route auto-disabled", "request_id", req.id, "route_id", at.route.ID, "status", status, "err", errInfo.Summary)
		}
	case decision.Action == failRetry && req.policy.enabled && lastTry && req.policy.cooldownSec > 0:
		cause := cooldownReasonHTTP
		switch {
		case transient:
			if !rt.routeHealth.noteTransportFailure(at.route.ID) {
				return nil
			}
			cause = cooldownReasonTransport
		case decision.Reason == "transport":
			cause = cooldownReasonFirstTokenTimeout
		}
		rt.metrics.routeCooledDown(cause)
		// 最后一次同路由尝试失败才进入冷却；Retry-After 更长时以它为准
		cool := time.Duration(req.policy.cooldownSec) * time.Second
		if decision.Cooldown > cool {
			cool = decision.Cooldown
		}
		until := time.Now().Add(cool)
		_ = rt.Routes.SetTempUnschedulable(at.route.ID, until, reason, time.Now(), req.id)
		return &until
	}
	return nil
}

// pauseRouteForAccountFailure 账号级失败（余额 / 鉴权 / 无权限）：照 sub2api handleOpenAI403，路由冷却 10 分钟，
// 180 分钟内第 3 次直接自动禁用（后台恢复探测成功后恢复）。不看组的冷却秒数——那是给瞬时错误的，
// 组冷却为 0 时，余额耗尽的路由以前每个请求都要先打一遍。余额耗尽按上游账号算，同渠道的路由一起冷却。
func (rt *Runtime) pauseRouteForAccountFailure(req *relayRequest, at *relayAttempt, decision failureDecision, errInfo usageErrorInfo, reason string) *time.Time {
	now := time.Now()
	if strikes := rt.routeHealth.noteAccountFailure(at.route.ID, now); strikes >= accountFailuresBeforeDisable {
		rt.metrics.routeAutoDisabled()
		rt.routeHealth.clearAccountFailures(at.route.ID)
		_ = rt.Routes.SetAutoDisabled(at.route.ID, fmt.Sprintf("%d 次账号级失败（%s）\n%s", strikes, decision.Reason, reason), now, req.id)
		if rt.Log != nil {
			rt.Log.Warn("gateway route auto-disabled after repeated account failures",
				"request_id", req.id, "route_id", at.route.ID, "reason", decision.Reason, "strikes", strikes, "err", errInfo.Summary)
		}
		return nil
	}
	until := now.Add(decision.Cooldown)
	rt.metrics.routeCooledDown(cooldownReasonAccount)
	if decision.ChannelWide {
		kind := at.route.NormalizeSourceKind()
		sourceID := at.route.SourceChannelID
		if kind == storage.GatewayRouteSourceProvider {
			sourceID = at.route.GatewayProviderID
		}
		if n, err := rt.Routes.SetTempUnschedulableForSource(kind, sourceID, until, reason, now, req.id); err == nil && n > 0 {
			if rt.Log != nil {
				rt.Log.Warn("gateway routes paused: upstream account out of balance",
					"request_id", req.id, "route_id", at.route.ID, "source_kind", kind, "source_id", sourceID, "routes", n)
			}
			return &until
		}
	}
	_ = rt.Routes.SetTempUnschedulable(at.route.ID, until, reason, now, req.id)
	return &until
}
