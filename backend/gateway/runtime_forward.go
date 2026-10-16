// 数据面：非流式转发与上游目标解析。
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
	"github.com/bejix/upstream-ops/backend/storage"
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

func (rt *Runtime) handleForward(c *gin.Context, req forwardRequest) {
	path, kind := req.Path, req.Kind
	// 尽早生成/透传 request id，保证后续任意错误体都可带上
	reqID := rt.ensureGatewayRequestID(c)

	auth, err := rt.Authenticate(c)
	if err != nil {
		rt.writeAuthError(c, kind, err.Error())
		return
	}
	key, group := auth.Key, auth.Group

	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		rt.writeGatewayError(c, kind, http.StatusBadRequest, "invalid_request_error", "failed to read body")
		return
	}
	body, err = rt.applySystemPrompt(group, key, path, kind, body)
	if err != nil {
		rt.writeGatewayError(c, kind, http.StatusBadRequest, "invalid_request_error", "system prompt injection failed: "+err.Error())
		return
	}

	contentType := c.GetHeader("Content-Type")
	// 请求体只扫一遍：model、stream、服务等级、推理档位、thinking、计费参数一次拿齐
	facts := scanRequestFacts(path, body)
	requestedModel := strings.TrimSpace(req.Model)
	if requestedModel == "" {
		requestedModel = facts.model
	}
	if requestedModel == "" {
		// multipart（音频、图片编辑）的 model 在表单里
		requestedModel = requestModel(contentType, body)
	}
	beforeTier := body
	body, err = rt.applyServiceTierRules(group, key, kind, requestedModel, body)
	if err != nil {
		rt.writeGatewayError(c, kind, http.StatusForbidden, "permission_error", err.Error())
		return
	}
	if !sameBuffer(beforeTier, body) {
		// 服务等级规则改写了 body（去掉或强制 service_tier）：按改写后的重新取
		facts = scanRequestFacts(path, body)
	}
	stream := facts.stream
	if req.Stream != nil {
		stream = *req.Stream
	}
	billingInput := facts.billing
	serviceTier, reasoningEffort := facts.serviceTier, facts.reasoningEffort
	// thinking 是否开启：国产模型无 effort 档位时，按上游映射模型补默认 high（对齐 sub2api）
	thinkingEnabled := facts.thinkingEnabled
	rt.touchKey(key.ID, time.Now())

	routes, err := rt.cachedRoutes(group.ID)
	if err != nil || len(routes) == 0 {
		rt.writeGatewayError(c, kind, http.StatusServiceUnavailable, "api_error", "no routes configured")
		return
	}

	groupMapping := cachedModelMapping(group.ModelMappingJSON)
	groupMappedModel, _ := ResolveModel(requestedModel, groupMapping)
	// 转发模型：只留下接这个模型的路由（同 new-api 渠道的模型列表）
	if routes = filterRoutesForModel(routes, requestedModel, groupMappedModel); len(routes) == 0 {
		rt.writeGatewayError(c, kind, http.StatusNotFound, "invalid_request_error",
			fmt.Sprintf("model %q is not available: no route in this group forwards it", requestedModel))
		return
	}
	groupsByChannel := rt.loadGroupsByChannel(c.Request.Context(), routes)
	prefs := routePreferences{
		ModelRouteIDs: rt.modelRouteIDs(group, routes, requestedModel, groupMappedModel),
	}
	if req.TaskID != "" {
		prefs.PinnedRouteID = rt.tasks.lookup(group.ID, req.TaskID)
	}
	// 透传请求的查询串（分页、Gemini 参数等）原样带给上游；Gemini 的 ?key= 是网关密钥，不外传
	query := forwardQuery(c)

	// 组级重试策略
	retryEnabled := group.RetryEnabled
	sameRouteRetries := group.RetryCount
	failoverEnabled := group.FailoverEnabled
	failoverMax := group.FailoverMax
	cooldownSec := group.CooldownSeconds
	ftTimeoutSec := rt.clampFirstTokenTimeoutSec(group.FirstTokenTimeoutSec)
	var firstTokenTimeout time.Duration
	if ftTimeoutSec > 0 {
		firstTokenTimeout = time.Duration(ftTimeoutSec) * time.Second
	}
	if sameRouteRetries < 0 {
		sameRouteRetries = 0
	}
	if failoverMax < 0 {
		failoverMax = 0
	}
	if cooldownSec < 0 {
		cooldownSec = 0
	}
	// 重试总开关关闭：不重试、不顺延，失败直接回显
	if !retryEnabled {
		sameRouteRetries = 0
		failoverEnabled = false
		failoverMax = 0
	}

	exclude := map[uint]struct{}{}
	var lastStatus int
	var lastBody []byte
	var lastErr error
	// 已顺延次数（换到另一条路由的次数）
	failoversDone := 0
	// 全局尝试序号（写入 usage.attempt，同一 request_id 关联）
	attemptNo := 0
	routesTried := 0

	// 跨协议转换时下载的图片等媒体：整个请求共享一份缓存与预算，换路由不重复下载
	convCtx := withMediaFetchState(c.Request.Context())
	// Gemini 原生透传（embedContent / cachedContents 等）不跨协议，只能交给 Gemini 上游
	geminiPassthrough := protocol.IsPassthroughEndpoint(path) && protocol.NormalizeKind(kind) == protocol.KindGemini

	for {
		candidates := orderCandidates(routes, groupsByChannel, group, time.Now(), exclude, prefs)
		if len(candidates) == 0 {
			break
		}

		cand := candidates[0]
		route := cand.Route
		routeMapping := cachedModelMapping(route.ModelMappingJSON)
		upstreamModel, chain := ResolveModel(requestedModel, routeMapping, groupMapping)
		if upstreamModel == "" {
			upstreamModel = requestedModel
		}
		// 接不住的路由直接跳过：不发请求、不占顺延次数
		if geminiPassthrough && !rt.geminiPassthroughCapable(&route, upstreamModel) {
			exclude[route.ID] = struct{}{}
			if lastErr == nil && lastStatus == 0 {
				lastErr = fmt.Errorf("no gemini upstream route in this group can serve %s", path)
			}
			continue
		}

		// 非首条路由 = 顺延；超过顺延次数则停止
		if routesTried > 0 {
			if !failoverEnabled || failoversDone >= failoverMax {
				break
			}
		}
		exclude[route.ID] = struct{}{}
		if routesTried > 0 {
			failoversDone++
		}
		routesTried++
		// 映射后的上游模型可能是 kimi/glm 等；thinking 开了但无档位时补 high
		attemptReasoningEffort := ApplyThinkingEnabledEffortFallback(
			reasoningEffort, thinkingEnabled, upstreamModel, requestedModel,
		)

		// 当前路由已进 exclude：失败后是否还有可顺延的其它路由。
		// 没有下家时关闭首字超时，最后一枪老实等上游，而不是再掐 30s 直接 502。
		remainingAfter := SortRoutes(routes, groupsByChannel, group.RateSortDirection, time.Now(), exclude)
		attemptFTTimeout := rt.effectiveFirstTokenTimeout(
			firstTokenTimeout,
			retryEnabled, failoverEnabled,
			failoversDone, failoverMax,
			len(remainingAfter) > 0,
		)

		// 同一路由：1 次首次 + sameRouteRetries 次重试
		maxTriesOnRoute := 1 + sameRouteRetries
		for tryOnRoute := 0; tryOnRoute < maxTriesOnRoute; tryOnRoute++ {
			attemptNo++
			attemptKind := attemptKindPrimary
			if tryOnRoute > 0 {
				attemptKind = attemptKindRetry
			} else if routesTried > 1 {
				attemptKind = attemptKindFailover
			}

			target, resolveErr := rt.resolveUpstreamTarget(&route)
			if resolveErr != nil {
				lastErr = resolveErr
				errInfo := usageErrorInfo{
					Type:    "config",
					Summary: resolveErr.Error(),
					Detail:  fmt.Sprintf("config error\nroute_id: %d\nsource_kind: %s\nerror: %s\n", route.ID, route.NormalizeSourceKind(), resolveErr.Error()),
				}
				rt.recordUsage(key, group, &route, target, reqID, requestedModel, upstreamModel, chain, UsageTokens{}, cand.EffectiveRate, cand.BillingRate, stream, 0, false, errInfo, 0, nil, c, usageRecordMeta{
					InboundEndpoint: path, InboundProtocol: string(kind), ServiceTier: serviceTier, ReasoningEffort: attemptReasoningEffort,
					Attempt: attemptNo, AttemptKind: attemptKind,
				})
				// 配置错误：不再重试同路由，进入顺延判断
				break
			}
			// 转发：组+路由 UA 三选一（透传 / 组 / 自定义）
			rt.applyRouteUserAgent(target, group, &route)

			// 协议：路由显式优先；route=auto 且为 provider 时用 provider 默认
			routeProto := rt.normalizeUpstreamProtocol(route.UpstreamProtocol)
			if route.NormalizeSourceKind() == storage.GatewayRouteSourceProvider &&
				routeProto == storage.GatewayUpstreamProtocolAuto &&
				target.Provider != nil {
				if p := rt.normalizeProviderProtocol(target.Provider.UpstreamProtocol); p != storage.GatewayUpstreamProtocolAuto {
					routeProto = p
				}
			}
			upstreamKind := protocol.ResolveUpstream(routeProto, kind, upstreamModel)
			// 透传端点不跨协议：上游协议跟随入站（Gemini 透传的路由已在选路时过滤）
			if protocol.IsPassthroughEndpoint(path) {
				upstreamKind = protocol.NormalizeKind(kind)
			}

			// legacy completions 不跨协议
			if protocol.IsOpenAIFamily(kind) && strings.Contains(path, "/completions") && !strings.Contains(path, "/chat/") {
				if upstreamKind == protocolAnthropic || upstreamKind == protocol.KindOpenAIResponses || upstreamKind == protocol.KindGemini {
					rt.writeGatewayError(c, kind, http.StatusBadRequest, "invalid_request_error",
						"protocol conversion for /v1/completions is not supported; use /v1/chat/completions")
					return
				}
				upstreamKind = protocol.KindOpenAIChat
			}

			attemptBody := body
			if upstreamModel != requestedModel && isMultipartContentType(contentType) {
				attemptBody = rewriteMultipartModel(contentType, body, upstreamModel)
			}
			fwdBody, upstreamPath, plan, convErr := rt.planUpstreamRequest(convCtx, attemptBody, kind, upstreamKind, requestedModel, upstreamModel, stream, path)
			if convErr != nil {
				// 这条路由的上游协议接不住该请求：记一条配置错误，换下一条路由
				lastErr = convErr
				lastStatus = 0
				errInfo := usageErrorInfo{
					Type:    "config",
					Summary: convErr.Error(),
					Detail:  fmt.Sprintf("protocol convert failed\nroute_id: %d\ninbound: %s\nupstream: %s\nerror: %s\n", route.ID, kind, upstreamKind, convErr.Error()),
				}
				rt.recordUsage(key, group, &route, target, reqID, requestedModel, upstreamModel, chain, UsageTokens{}, cand.EffectiveRate, cand.BillingRate, stream, 0, false, errInfo, 0, nil, c, usageRecordMeta{
					InboundEndpoint: path, InboundProtocol: string(kind), UpstreamProtocol: string(upstreamKind), ProtocolConverted: true,
					ServiceTier: serviceTier, ReasoningEffort: attemptReasoningEffort, Attempt: attemptNo, AttemptKind: attemptKind,
				})
				// 请求里的图片 / 文件下载失败：换哪条路由都一样，直接回 400
				if mediaErr := mediaFetchFailure(convCtx); mediaErr != nil {
					rt.writeGatewayError(c, kind, http.StatusBadRequest, "invalid_request_error", "failed to load media from the request: "+mediaErr.Error())
					return
				}
				break
			}
			plan.GeminiArray = req.GeminiArray
			if query != "" && protocol.IsPassthroughEndpoint(path) {
				upstreamPath = appendQuery(upstreamPath, query)
			}

			upstreamFullURL := joinUpstreamURL(target.BaseURL, upstreamPath)
			usageMeta := usageRecordMeta{
				InboundEndpoint:   path,
				UpstreamEndpoint:  upstreamPath,
				InboundProtocol:   string(kind),
				UpstreamProtocol:  string(upstreamKind),
				ProtocolConverted: plan.Converted,
				ServiceTier:       serviceTier,
				ReasoningEffort:   attemptReasoningEffort,
				UpstreamURL:       upstreamFullURL,
				Attempt:           attemptNo,
				AttemptKind:       attemptKind,
				BillingInput:      billingInput,
			}

			start := time.Now()
			var (
				status             int
				respHeaders        http.Header
				respBody           []byte
				firstTokenMS       *int64
				fwdErr             error
				streamCommitted    bool
				streamTokens       UsageTokens
				streamErr          error
				clientDisconnected bool
			)
			if stream {
				// 真流式：边读上游 SSE 边写客户端。Committed 后禁止 retry/failover。
				res := rt.forwardStreamPlan(
					c.Request.Context(), c, target, upstreamPath, c.Request.Method, c.Request.Header, fwdBody,
					plan, attemptFTTimeout,
				)
				status = res.Status
				respHeaders = res.Headers
				respBody = res.Body
				firstTokenMS = res.FirstTokenMS
				fwdErr = res.Err
				streamCommitted = res.Committed
				streamTokens = res.Tokens
				streamErr = res.StreamErr
				clientDisconnected = res.ClientDisconnected
			} else {
				status, respHeaders, respBody, firstTokenMS, fwdErr = rt.forwardOnce(
					c.Request.Context(), c, target, upstreamPath, c.Request.Method, c.Request.Header, fwdBody, false, upstreamKind, attemptFTTimeout,
				)
			}
			duration := time.Since(start).Milliseconds()

			// 已向客户端写出 SSE：不能再重试/顺延，直接记账并结束本次请求。
			// 客户端在 commit 后、终端帧交付前断开：记 success=true + error_type=client。
			// 若已写出 [DONE]/message_stop 后客户端才关连接，forwardStream 会清掉 client 标记，记普通成功。
			if stream && streamCommitted {
				usageMeta.BillingInput = billingInputWithResponse(billingInput, respBody)
				onlyClientDisconnect := rt.isClientDisconnectAfterCommit(clientDisconnected, streamErr)
				// 仅客户端断开仍算业务成功；真实 stream 错误才算失败。
				success := streamErr == nil || onlyClientDisconnect
				errInfo := usageErrorInfo{}
				gwCfg := rt.gatewayRuntime()
				headerJSON := rt.formatDebugHeaders(respHeaders, gwCfg.UsageErrorHeadersJSONBytes, gwCfg.UsageErrorHeaderValueRunes)
				headerPlain := rt.formatHeadersPlain(respHeaders, gwCfg.UsageErrorHeaderValueRunes)
				if onlyClientDisconnect {
					var detail strings.Builder
					fmt.Fprintf(&detail,
						"client disconnected after stream commit\nmethod: %s\nurl: %s\nnote: 已继续读取上游以同步 usage/计费；上游可能仍完整计费\n",
						c.Request.Method, upstreamFullURL,
					)
					if streamErr != nil {
						fmt.Fprintf(&detail, "stream_err: %s\n", streamErr.Error())
					}
					fmt.Fprintf(&detail,
						"tokens: input=%d output=%d cache_read=%d cache_creation=%d\n",
						streamTokens.InputTokens, streamTokens.OutputTokens,
						streamTokens.CacheReadTokens, streamTokens.CacheCreationTokens,
					)
					rt.appendUpstreamHeadersToDetail(&detail, headerPlain)
					errInfo = usageErrorInfo{
						Type:            "client",
						Summary:         "客户端主动断开（已尽量同步上游用量）",
						Detail:          detail.String(),
						UpstreamHeaders: headerJSON,
					}
					if rt.Log != nil {
						rt.Log.Info("gateway stream client disconnected after commit",
							"request_id", reqID,
							"attempt", attemptNo,
							"route_id", route.ID,
							"upstream_url", upstreamFullURL,
							"input_tokens", streamTokens.InputTokens,
							"output_tokens", streamTokens.OutputTokens,
							"cache_read", streamTokens.CacheReadTokens,
						)
					}
				} else if streamErr != nil {
					var detail strings.Builder
					fmt.Fprintf(&detail,
						"stream error after commit\nmethod: %s\nurl: %s\nerror: %s\nclient_disconnected: %v\n",
						c.Request.Method, upstreamFullURL, streamErr.Error(), clientDisconnected,
					)
					rt.appendUpstreamHeadersToDetail(&detail, headerPlain)
					errInfo = usageErrorInfo{
						Type:            "transport",
						Summary:         streamErr.Error(),
						Detail:          detail.String(),
						UpstreamHeaders: headerJSON,
					}
				}
				if success {
					rt.noteRouteSuccess(&route)
				} else if rt.Log != nil {
					rt.Log.Warn("gateway stream ended with error after commit",
						"request_id", reqID,
						"attempt", attemptNo,
						"route_id", route.ID,
						"upstream_url", upstreamFullURL,
						"err", errInfo.Summary,
						"client_disconnected", clientDisconnected,
						"input_tokens", streamTokens.InputTokens,
						"output_tokens", streamTokens.OutputTokens,
					)
				}
				// status 通常为 200；保留已 drain 的 tokens（含客户端断开后的用量同步）。
				if status == 0 {
					status = http.StatusOK
				}
				rt.recordUsage(key, group, &route, target, reqID, requestedModel, upstreamModel, chain, streamTokens, cand.EffectiveRate, cand.BillingRate, stream, status, success, errInfo, duration, firstTokenMS, c, usageMeta)
				return
			}

			failed := fwdErr != nil || status >= 400
			if failed {
				gwCfg := rt.gatewayRuntime()
				errInfo := rt.buildUpstreamErrorInfoCfg(gwCfg, fwdErr, status, respHeaders, respBody, upstreamFullURL, c.Request.Method)
				if rt.isFirstTokenTimeout(fwdErr) {
					errInfo.Type = "transport"
					appliedSec := int(attemptFTTimeout / time.Second)
					if appliedSec <= 0 {
						appliedSec = ftTimeoutSec
					}
					errInfo.Summary = fmt.Sprintf("首字超时（%ds）：未在限定时间内收到上游首字节，已主动断开", appliedSec)
					var detail strings.Builder
					fmt.Fprintf(&detail,
						"first token timeout\nmethod: %s\nurl: %s\ntimeout: %ds\nwaited: %dms\nnote: 上游可能已开始计费；将走重试/顺延，可能增加费用\n",
						c.Request.Method, upstreamFullURL, appliedSec, duration,
					)
					// 保留已拿到的上游响应头（若有）
					rt.appendUpstreamHeadersToDetail(&detail, rt.formatHeadersPlain(respHeaders, gwCfg.UsageErrorHeaderValueRunes))
					if errInfo.UpstreamHeaders == "" {
						errInfo.UpstreamHeaders = rt.formatDebugHeaders(respHeaders, gwCfg.UsageErrorHeadersJSONBytes, gwCfg.UsageErrorHeaderValueRunes)
					}
					errInfo.Detail = detail.String()
				}
				// 客户端断开 / 请求 context 已取消：父 context 污染会导致后续重试/顺延全部变成
				// "context canceled"，且误伤路由冷却。此类错误应立刻停止，不再重试/顺延/冷却。
				clientCanceled := rt.isClientContextError(fwdErr, c) || clientDisconnected
				if clientCanceled {
					rt.annotateClientContextError(&errInfo, c, upstreamFullURL, c.Request.Method, fwdErr)
				}
				decision := rt.classifyFailure(group, status, respHeaders, respBody, fwdErr, req.TaskID != "", requestedModel, upstreamModel)
				if decision.Reason != "" && !clientCanceled {
					errInfo.Detail = strings.TrimRight(errInfo.Detail, "\n") + "\nfailover_decision: " + decision.Reason + "\n"
				}
				if fwdErr != nil {
					lastErr = fwdErr
				} else {
					lastStatus = status
					lastBody = rt.convertErrorBody(respBody, kind, upstreamKind, plan.Converted)
					lastErr = fmt.Errorf("upstream status %d: %s", status, errInfo.Summary)
				}

				// 不可重试的错误（默认 4xx、上下文超长等）：直接回显给客户端
				if decision.Action == failStop && !clientCanceled && fwdErr == nil {
					clientBody := rt.injectUpstreamOpsRequestID(lastBody, reqID)
					rt.copyResponseHeaders(c.Writer.Header(), respHeaders)
					c.Writer.Header().Del("Content-Length")
					c.Header("Content-Type", "application/json")
					rt.setGatewayRequestIDHeaders(c, reqID)
					c.Status(status)
					_, _ = c.Writer.Write(clientBody)
					rt.recordUsage(key, group, &route, target, reqID, requestedModel, upstreamModel, chain, UsageTokens{}, cand.EffectiveRate, cand.BillingRate, stream, status, false, errInfo, duration, firstTokenMS, c, usageMeta)
					return
				}

				lastTryOnRoute := tryOnRoute >= maxTriesOnRoute-1
				var cooldownUntil *time.Time
				switch {
				case clientCanceled:
					// 客户端取消不冷却
				case decision.Action == failDisableRoute:
					reason := errInfo.Summary
					if strings.TrimSpace(errInfo.Detail) != "" {
						reason = rt.truncateRunes(errInfo.Detail, 4000)
					}
					_ = rt.Routes.SetAutoDisabled(route.ID, reason, time.Now(), reqID)
					if rt.Log != nil {
						rt.Log.Warn("gateway route auto-disabled", "request_id", reqID, "route_id", route.ID, "status", status, "err", errInfo.Summary)
					}
				case decision.Action == failRetry && retryEnabled && lastTryOnRoute && cooldownSec > 0:
					// 最后一次同路由尝试失败才进入冷却；Retry-After 更长时以它为准
					cool := time.Duration(cooldownSec) * time.Second
					if decision.Cooldown > cool {
						cool = decision.Cooldown
					}
					until := time.Now().Add(cool)
					cooldownUntil = &until
					pauseReason := errInfo.Summary
					if strings.TrimSpace(errInfo.Detail) != "" {
						pauseReason = rt.truncateRunes(errInfo.Detail, 4000)
					}
					_ = rt.Routes.SetTempUnschedulable(route.ID, until, pauseReason, time.Now(), reqID)
				}
				usageMeta.CooldownUntil = cooldownUntil
				rt.recordUsage(key, group, &route, target, reqID, requestedModel, upstreamModel, chain, UsageTokens{}, cand.EffectiveRate, cand.BillingRate, stream, status, false, errInfo, duration, firstTokenMS, c, usageMeta)
				if rt.Log != nil {
					rt.Log.Warn("gateway upstream fail",
						"request_id", reqID,
						"attempt", attemptNo,
						"attempt_kind", attemptKind,
						"route_id", route.ID,
						"status", status,
						"upstream_url", upstreamFullURL,
						"err", errInfo.Summary,
						"decision", decision.Reason,
						"client_canceled", clientCanceled,
					)
				}
				// 客户端已取消：立刻结束，避免用已取消 context 继续打上游
				if clientCanceled || !retryEnabled {
					goto finishError
				}
				// 只有可重试错误才在同路由重试；其余直接顺延
				if decision.Action == failRetry && tryOnRoute < maxTriesOnRoute-1 {
					continue
				}
				break // 同路由结束，顺延
			}

			// 非流式成功：整包转换后写出
			tokens := rt.parseUsageByKind(respBody, false, upstreamKind)
			usageMeta.BillingInput = billingInputWithResponse(billingInput, respBody)
			clientBody := rt.convertResponseWithPlan(c.Request.Context(), respBody, plan)
			if plan.Converted && rt.Log != nil && len(clientBody) == 0 && len(respBody) > 0 {
				rt.Log.Warn("response convert produced empty body")
			}
			if protocol.IsPassthroughEndpoint(path) && c.Request.Method != http.MethodGet {
				rt.tasks.remember(group.ID, route.ID, taskIDsFromResponse(respBody))
			}
			rt.copyResponseHeaders(c.Writer.Header(), respHeaders)
			if plan.Converted {
				c.Writer.Header().Del("Content-Length")
				c.Header("Content-Type", "application/json")
			}
			c.Status(status)
			_, _ = c.Writer.Write(clientBody)
			// 成功：立刻恢复调度；连续成功达到阈值后自动清除错误残留展示
			rt.noteRouteSuccess(&route)
			rt.recordUsage(key, group, &route, target, reqID, requestedModel, upstreamModel, chain, tokens, cand.EffectiveRate, cand.BillingRate, stream, status, true, usageErrorInfo{}, duration, firstTokenMS, c, usageMeta)
			return
		}

		// 同路由结束：判断能否顺延
		if !retryEnabled || !failoverEnabled || failoversDone >= failoverMax {
			break
		}
	}

finishError:
	if lastStatus > 0 && len(lastBody) > 0 {
		out := rt.injectUpstreamOpsRequestID(lastBody, reqID)
		rt.setGatewayRequestIDHeaders(c, reqID)
		c.Header("Content-Type", "application/json")
		c.Writer.Header().Del("Content-Length")
		c.Status(lastStatus)
		_, _ = c.Writer.Write(out)
		return
	}
	msg := "all upstream routes failed"
	status := http.StatusBadGateway
	errType := "api_error"
	if lastErr != nil {
		msg = lastErr.Error()
		if isProtocolConvertError(lastErr) {
			status, errType = http.StatusBadRequest, "invalid_request_error"
		}
	}
	rt.writeGatewayError(c, kind, status, errType, msg)
}

// prepareUpstreamRequest 按入站/上游协议准备转发 body 与 path。

func (rt *Runtime) forwardOnce(
	ctx context.Context,
	c *gin.Context,
	target *upstreamTarget,
	path string,
	method string,
	inHeader http.Header,
	body []byte,
	stream bool,
	kind protocolKind,
	firstTokenTimeout time.Duration, // 0=关闭；从发起请求起算到首字节的总等待
) (status int, respHeader http.Header, respBody []byte, firstTokenMS *int64, err error) {
	if target == nil {
		return 0, nil, nil, nil, errors.New("upstream target is nil")
	}

	// 可取消：首字超时需能打断卡住的 Do / body 读
	reqCtx, abortReq := context.WithCancel(ctx)
	defer abortReq()

	req, err := rt.buildUpstreamHTTPRequest(reqCtx, target, path, method, inHeader, body, kind, stream)
	if err != nil {
		return 0, nil, nil, nil, err
	}

	client := rt.httpClientForTarget(target.Channel, target.Provider)
	start := time.Now()
	resp, err := rt.doHTTPWithFirstTokenDeadline(reqCtx, abortReq, client, req, start, firstTokenTimeout)
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

	bodyWait, timedOut := rt.remainingFirstTokenWait(start, firstTokenTimeout)
	if timedOut {
		closeBody()
		abortReq()
		ms := time.Since(start).Milliseconds()
		ft = &ms
		return 0, resp.Header.Clone(), nil, ft, fmt.Errorf("%w after %s", errFirstTokenTimeout, firstTokenTimeout)
	}

	firstChunk, firstErr := rt.readFirstChunk(resp.Body, bodyWait)
	if firstErr != nil {
		if rt.isFirstTokenTimeout(firstErr) {
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

	if stream {
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
	_ = c
	return resp.StatusCode, resp.Header.Clone(), data, ft, nil
}

// remainingFirstTokenWait 计算从 start 起算，首字超时还剩多少时间。
// configured<=0 表示关闭：left=0 且 timedOut=false（调用方按「无超时」处理）。
// 已耗尽：timedOut=true。

func (rt *Runtime) resolveUpstreamTarget(route *storage.GatewayRoute) (*upstreamTarget, error) {
	if route == nil {
		return nil, errors.New("route is nil")
	}
	if route.NormalizeSourceKind() == storage.GatewayRouteSourceProvider {
		if rt.Providers == nil {
			return nil, errors.New("providers not configured")
		}
		if route.GatewayProviderID == 0 {
			return nil, errors.New("gateway_provider_id is required")
		}
		p, err := rt.cachedProvider(route.GatewayProviderID)
		if err != nil {
			return nil, fmt.Errorf("provider not found: %w", err)
		}
		if !p.Enabled {
			return nil, fmt.Errorf("provider %q is disabled", p.Name)
		}
		secret, err := rt.Cipher.Decrypt(p.APIKeyCipher)
		if err != nil || strings.TrimSpace(secret) == "" {
			return nil, errors.New("provider api key missing or decrypt failed")
		}
		return &upstreamTarget{
			BaseURL:  strings.TrimRight(p.BaseURL, "/"),
			APIKey:   secret,
			Provider: p,
		}, nil
	}
	ch, err := rt.cachedChannel(route.SourceChannelID)
	if err != nil {
		return nil, fmt.Errorf("channel not found: %w", err)
	}
	secret, err := rt.Cipher.Decrypt(route.SourceAPIKeyCipher)
	if err != nil || strings.TrimSpace(secret) == "" {
		return nil, errors.New("missing upstream api key; run ensure-keys")
	}
	return &upstreamTarget{
		BaseURL: strings.TrimRight(ch.SiteURL, "/"),
		APIKey:  secret,
		Channel: ch,
	}, nil
}
