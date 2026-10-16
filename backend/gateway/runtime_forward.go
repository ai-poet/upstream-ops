// 数据面：转发入口（请求级准备 + 路由尝试循环）、非流式转发与上游目标解析。
// 单次尝试见 runtime_attempt.go，请求上下文见 relay_context.go。
package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/gin-gonic/gin"
)

// forwardRequest 一次客户端请求的转发参数。
type forwardRequest struct {
	// Path 规范化后的入站端点（写用量；透传类端点也是上游 path）
	Path string
	Kind protocolKind
	// Model 非空时覆盖 body 里的 model（Gemini 原生：模型在 URL 上）
	Model string
	// Stream 非空时覆盖 body 里的 stream（Gemini 原生：由 action 决定）
	Stream *bool
	// GeminiArray Gemini 流式且客户端未带 alt=sse：按 JSON 数组写出
	GeminiArray bool
	// TaskID 查询 / 取消异步任务时的任务 id：优先回到创建它的路由，404 直接换路由
	TaskID string
}

// HandleForward 主转发（含故障转移）。
func (rt *Runtime) HandleForward(c *gin.Context, path string, kind protocolKind) {
	rt.handleForward(c, forwardRequest{Path: path, Kind: kind})
}

// HandleTaskForward 异步任务查询 / 下载 / 取消：按任务 id 回到创建它的路由。
func (rt *Runtime) HandleTaskForward(c *gin.Context, path string, kind protocolKind, taskID string) {
	rt.handleForward(c, forwardRequest{Path: path, Kind: kind, TaskID: taskID})
}

// handleForward 准备请求、按路由尝试；任何一步失败都返回错误，在这里统一回给客户端。
func (rt *Runtime) handleForward(c *gin.Context, in forwardRequest) {
	req, gerr := rt.prepareRelay(c, in)
	if gerr == nil {
		gerr = rt.relayWithFailover(req)
	}
	if gerr != nil {
		writeError(c, in.Kind, gerr)
	}
}

// prepareRelay 请求级准备：鉴权 → 读 body → 准备候选路由。
func (rt *Runtime) prepareRelay(c *gin.Context, in forwardRequest) (*relayRequest, *gatewayError) {
	// 尽早生成/透传 request id，保证后续任意错误体都可带上
	req := &relayRequest{c: c, in: in, id: ensureGatewayRequestID(c)}
	auth, err := rt.Authenticate(c)
	if err != nil {
		return nil, authError(err.Error())
	}
	req.key, req.group = auth.Key, auth.Group
	if gerr := rt.readRelayBody(req); gerr != nil {
		return nil, gerr
	}
	rt.touchKey(req.key.ID, time.Now())
	if gerr := rt.prepareRelayRoutes(req); gerr != nil {
		return nil, gerr
	}
	return req, nil
}

// readRelayBody 读 body、注入系统提示词、扫一遍字段、应用服务等级规则。
func (rt *Runtime) readRelayBody(req *relayRequest) *gatewayError {
	c, kind, path := req.c, req.in.Kind, req.in.Path
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		return newGatewayError(http.StatusBadRequest, "invalid_request_error", "failed to read body")
	}
	body, err = rt.applySystemPrompt(req.group, req.key, path, kind, body)
	if err != nil {
		return newGatewayError(http.StatusBadRequest, "invalid_request_error", "system prompt injection failed: "+err.Error())
	}

	req.contentType = c.GetHeader("Content-Type")
	// 请求体只扫一遍：model、stream、服务等级、推理档位、thinking、计费参数一次拿齐
	facts := scanRequestFacts(path, body)
	req.model = strings.TrimSpace(req.in.Model)
	if req.model == "" {
		req.model = facts.model
	}
	if req.model == "" {
		// multipart（音频、图片编辑）的 model 在表单里
		req.model = requestModel(req.contentType, body)
	}
	beforeTier := body
	body, err = rt.applyServiceTierRules(req.group, req.key, kind, req.model, body)
	if err != nil {
		return newGatewayError(http.StatusForbidden, "permission_error", err.Error())
	}
	if !sameBuffer(beforeTier, body) {
		// 服务等级规则改写了 body（去掉或强制 service_tier）：按改写后的重新取
		facts = scanRequestFacts(path, body)
	}

	req.body = body
	req.stream = facts.stream
	if req.in.Stream != nil {
		req.stream = *req.in.Stream
	}
	req.serviceTier, req.reasoningEffort = facts.serviceTier, facts.reasoningEffort
	// thinking 是否开启：国产模型无 effort 档位时，按上游映射模型补默认 high（对齐 sub2api）
	req.thinkingEnabled = facts.thinkingEnabled
	req.billing = facts.billing
	return nil
}

// prepareRelayRoutes 候选路由（按转发模型过滤）、源分组倍率、选路偏好与重试策略。
func (rt *Runtime) prepareRelayRoutes(req *relayRequest) *gatewayError {
	c, kind := req.c, req.in.Kind
	routes, err := rt.cachedRoutes(req.group.ID)
	if err != nil || len(routes) == 0 {
		return newGatewayError(http.StatusServiceUnavailable, "api_error", "no routes configured")
	}
	req.groupMapping = cachedModelMapping(req.group.ModelMappingJSON)
	groupMappedModel, _ := ResolveModel(req.model, req.groupMapping)
	// 转发模型：只留下接这个模型的路由（同 new-api 渠道的模型列表）
	if routes = filterRoutesForModel(routes, req.model, groupMappedModel); len(routes) == 0 {
		return newGatewayError(http.StatusNotFound, "invalid_request_error", fmt.Sprintf("model %q is not available: no route in this group forwards it", req.model))
	}
	req.routes = routes
	req.groupsByChannel = rt.loadGroupsByChannel(c.Request.Context(), routes)
	req.prefs = routePreferences{
		ModelRouteIDs: rt.modelRouteIDs(req.group, routes, req.model, groupMappedModel),
	}
	if req.in.TaskID != "" {
		req.prefs.PinnedRouteID = rt.tasks.lookup(req.group.ID, req.in.TaskID)
	}
	// 透传请求的查询串（分页、Gemini 参数等）原样带给上游；Gemini 的 ?key= 是网关密钥，不外传
	req.query = forwardQuery(c)
	req.policy = rt.retryPolicyFor(req.group)
	req.convCtx = withMediaFetchState(c.Request.Context())
	req.geminiPassthrough = protocol.IsPassthroughEndpoint(req.in.Path) && protocol.NormalizeKind(kind) == protocol.KindGemini
	return nil
}

// relayWithFailover 按候选顺序逐条路由尝试：同路由重试、换路由顺延，
// 直到成功、遇到不可重试的错误或次数用尽。成功时已回包并返回 nil；否则返回要回给客户端的错误。
func (rt *Runtime) relayWithFailover(req *relayRequest) *gatewayError {
	st := &failoverState{exclude: map[uint]struct{}{}}
	for {
		candidates := orderCandidates(req.routes, req.groupsByChannel, req.group, time.Now(), st.exclude, req.prefs)
		if len(candidates) == 0 {
			break
		}
		choice := newRouteChoice(req, candidates[0])
		// 接不住的路由直接跳过：不发请求、不占顺延次数
		if req.geminiPassthrough && !rt.geminiPassthroughCapable(&choice.route, choice.upstreamModel) {
			st.exclude[choice.route.ID] = struct{}{}
			if st.lastErr == nil && st.lastStatus == 0 {
				st.lastErr = fmt.Errorf("no gemini upstream route in this group can serve %s", req.in.Path)
			}
			continue
		}
		// 非首条路由 = 顺延；超过顺延次数则停止
		if st.routesTried > 0 && !st.canFailover(req.policy) {
			break
		}
		st.exclude[choice.route.ID] = struct{}{}
		if st.routesTried > 0 {
			st.failoversDone++
		}
		st.routesTried++
		choice.reasoningEffort = ApplyThinkingEnabledEffortFallback(
			req.reasoningEffort, req.thinkingEnabled, choice.upstreamModel, req.model,
		)
		choice.firstTokenTimeout = rt.routeFirstTokenTimeout(req, st)

		switch rt.tryRoute(req, choice, st) {
		case attemptDone:
			return st.reply
		case attemptGiveUp:
			return st.lastFailure()
		}
		// 同路由结束：判断能否顺延
		if !req.policy.enabled || !st.canFailover(req.policy) {
			break
		}
	}
	return st.lastFailure()
}

// routeFirstTokenTimeout 当前路由的首字超时。当前路由已进 exclude：失败后还有没有可顺延的其它路由；
// 没有下家时关闭首字超时，最后一枪老实等上游，而不是再掐 30s 直接 502。
func (rt *Runtime) routeFirstTokenTimeout(req *relayRequest, st *failoverState) time.Duration {
	remainingAfter := SortRoutes(req.routes, req.groupsByChannel, req.group.RateSortDirection, time.Now(), st.exclude)
	p := req.policy
	return effectiveFirstTokenTimeout(
		p.firstTokenTimeout,
		p.enabled, p.failoverEnabled,
		st.failoversDone, p.failoverMax,
		len(remainingAfter) > 0,
	)
}

// tryRoute 在一条路由上尝试：1 次首次 + sameRouteRetries 次重试。
func (rt *Runtime) tryRoute(req *relayRequest, choice *routeChoice, st *failoverState) attemptNext {
	maxTries := 1 + req.policy.sameRouteRetries
	for try := 0; try < maxTries; try++ {
		st.attemptNo++
		at := &relayAttempt{routeChoice: choice, no: st.attemptNo, kind: attemptKindPrimary}
		if try > 0 {
			at.kind = attemptKindRetry
		} else if st.routesTried > 1 {
			at.kind = attemptKindFailover
		}
		if next := rt.runAttempt(req, at, st, try == maxTries-1); next != attemptRetry {
			return next
		}
	}
	return attemptNextRoute
}

// forwardOnce 非流式转发一次（流式请求在 stream=true 时整包读完，供探测等场景）。
func (rt *Runtime) forwardOnce(ctx context.Context, call upstreamCall) (status int, respHeader http.Header, respBody []byte, firstTokenMS *int64, err error) {
	if call.target == nil {
		return 0, nil, nil, nil, errors.New("upstream target is nil")
	}

	// 可取消：首字超时需能打断卡住的 Do / body 读
	reqCtx, abortReq := context.WithCancel(ctx)
	defer abortReq()

	req, err := rt.buildUpstreamHTTPRequest(reqCtx, call)
	if err != nil {
		return 0, nil, nil, nil, err
	}
	firstTokenTimeout := call.firstTokenTimeout

	client := rt.httpClientForTarget(call.target.Channel, call.target.Provider)
	start := time.Now()
	resp, err := doHTTPWithFirstTokenDeadline(reqCtx, abortReq, client, req, start, firstTokenTimeout)
	if err != nil {
		return 0, nil, nil, nil, err
	}
	// 注意：超时断开时需主动 Close，正常路径 defer 关闭
	bodyClosed := false
	closeBody := func() {
		if !bodyClosed {
			bodyClosed = true
			_ = resp.Body.Close()
		}
	}
	defer closeBody()

	// 非 2xx 错误体整包读，不受首字超时约束
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, readErr := io.ReadAll(io.LimitReader(resp.Body, int64(rt.gatewayRuntime().UsageErrorBodyBytes)+1))
		if readErr != nil && len(data) == 0 {
			return resp.StatusCode, resp.Header.Clone(), nil, nil, readErr
		}
		return resp.StatusCode, resp.Header.Clone(), data, nil, nil
	}

	// 流式/非流式成功响应：首字节超时从 start 起算剩余预算（含等响应头时间）。
	var ft *int64
	var data []byte

	bodyWait, timedOut := remainingFirstTokenWait(start, firstTokenTimeout)
	if timedOut {
		closeBody()
		abortReq()
		ms := time.Since(start).Milliseconds()
		ft = &ms
		return 0, resp.Header.Clone(), nil, ft, fmt.Errorf("%w after %s", errFirstTokenTimeout, firstTokenTimeout)
	}

	firstChunk, firstErr := readFirstChunk(resp.Body, bodyWait)
	if firstErr != nil {
		if isFirstTokenTimeout(firstErr) {
			// 主动掐断连接，触发重试/顺延（上游可能已开始计费）
			closeBody()
			abortReq()
			ms := time.Since(start).Milliseconds()
			ft = &ms
			return 0, resp.Header.Clone(), nil, ft, fmt.Errorf("%w after %s", errFirstTokenTimeout, firstTokenTimeout)
		}
		return resp.StatusCode, resp.Header.Clone(), nil, nil, firstErr
	}
	if len(firstChunk) > 0 {
		ms := time.Since(start).Milliseconds()
		ft = &ms
		data = append(data, firstChunk...)
	}

	if call.stream {
		buf := make([]byte, 32*1024)
		for {
			n, readErr := resp.Body.Read(buf)
			if n > 0 {
				data = append(data, buf[:n]...)
			}
			if readErr != nil {
				if readErr == io.EOF {
					break
				}
				return resp.StatusCode, resp.Header.Clone(), data, ft, readErr
			}
		}
	} else {
		rest, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return resp.StatusCode, resp.Header.Clone(), data, ft, readErr
		}
		if len(rest) > 0 {
			data = append(data, rest...)
		}
	}
	return resp.StatusCode, resp.Header.Clone(), data, ft, nil
}
