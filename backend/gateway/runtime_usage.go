// 数据面：用量落库与计费。
package gateway

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/pricing"
	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

// recordUsage 计费并写一条用量日志；成功时累加密钥已用额度。
func (rt *Runtime) recordUsage(c *gin.Context, u usageRecord) {
	priceModel := u.upstreamModel
	if priceModel == "" {
		priceModel = u.requestedModel
	}
	// 拆成互斥桶再计费 / 落库：OpenAI（Chat / Responses）的输入 token 含缓存读写，要扣掉（对齐 sub2api）；
	// Anthropic 的 input_tokens 本来就不含缓存，Gemini 解析时已扣过，再扣一次输入就少算了
	tokens := u.tokens
	if protocol.IsOpenAIFamily(u.upstreamKind) {
		tokens = pricing.SplitOpenAIUsageBuckets(tokens)
	}
	price, priced := rt.Pricing.Lookup(priceModel)
	unpriced := !priced && priceModel != ""
	if unpriced && u.success {
		rt.warnUnpricedModel(priceModel, u)
	}
	if price.BillingMode == pricing.BillingModeToken && u.meta.BillingInput.EndpointMode != "" {
		price.BillingMode = u.meta.BillingInput.EndpointMode
	}
	cost := pricing.CalculateCostWithBillingInput(price, tokens, u.rate, u.billingRate, u.meta.ServiceTier, u.meta.BillingInput)
	if !u.success {
		// 没成功不向密钥收费：实际费用记 0，与额度累加一致。按次 / 按张 / 按时长的价格只在成功时成立，
		// 失败尝试连标准价也记 0；按 token 计价的失败（如流中途出错）保留已消耗 token 的标准价，便于对账上游消耗
		if cost.BillingMode != string(pricing.BillingModeToken) {
			cost = pricing.CostBreakdown{BillingMode: cost.BillingMode}
		}
		cost.ActualCost = 0
	}
	reqType := storage.GatewayRequestTypeSync
	if u.stream {
		reqType = storage.GatewayRequestTypeStream
	}
	billingMode := cost.BillingMode
	if billingMode == "" {
		billingMode = string(pricing.BillingModeToken)
	}
	sourceGroupName := strings.TrimSpace(u.route.SourceGroupName)
	if sourceGroupName == "" || isSourceGroupIDPlaceholder(sourceGroupName) {
		// 运行时再解析一次：旧路由可能只有 id、name 为空
		if u.route.SourceGroupID != nil && *u.route.SourceGroupID > 0 && u.route.SourceChannelID > 0 {
			if gs := rt.loadGroupsByChannel(context.Background(), []storage.GatewayRoute{*u.route}); len(gs) > 0 {
				tmp := *u.route
				enrichRouteSourceGroupName(&tmp, gs[u.route.SourceChannelID])
				if n := strings.TrimSpace(tmp.SourceGroupName); n != "" && !isSourceGroupIDPlaceholder(n) {
					sourceGroupName = n
				}
			}
		}
	}
	if sourceGroupName == "" && u.route.SourceGroupID != nil {
		sourceGroupName = fmt.Sprintf("id:%d", *u.route.SourceGroupID)
	}
	var channelID uint
	var providerID uint
	providerName := ""
	sourceKeyName := strings.TrimSpace(u.route.SourceAPIKeyName)
	sourceKeyID := u.route.SourceAPIKeyID
	if u.target != nil {
		if u.target.Channel != nil {
			channelID = u.target.Channel.ID
		}
		if u.target.Provider != nil {
			providerID = u.target.Provider.ID
			providerName = u.target.Provider.Name
			if sourceKeyName == "" {
				sourceKeyName = u.target.Provider.APIKeyHint
			}
		}
	} else if u.route != nil {
		channelID = u.route.SourceChannelID
		providerID = u.route.GatewayProviderID
	}
	attempt := u.meta.Attempt
	if attempt <= 0 {
		attempt = 1
	}
	attemptKind := strings.TrimSpace(u.meta.AttemptKind)
	if attemptKind == "" {
		attemptKind = attemptKindPrimary
	}
	item := &storage.GatewayUsageLog{
		GatewayGroupID:    u.group.ID,
		GatewayKeyID:      u.key.ID,
		RouteID:           u.route.ID,
		ChannelID:         channelID,
		GatewayProviderID: providerID,
		ProviderName:      providerName,
		// 路由快照：保存路由换 id 后历史记录仍可展示
		SourceAPIKeyID:        sourceKeyID,
		SourceAPIKeyName:      sourceKeyName,
		SourceGroupID:         u.route.SourceGroupID,
		SourceGroupName:       sourceGroupName,
		RequestID:             u.requestID,
		Attempt:               attempt,
		AttemptKind:           attemptKind,
		CooldownUntil:         u.meta.CooldownUntil,
		RequestedModel:        u.requestedModel,
		UpstreamModel:         u.upstreamModel,
		ModelMappingChain:     u.chain,
		InboundEndpoint:       u.meta.InboundEndpoint,
		UpstreamEndpoint:      u.meta.UpstreamEndpoint,
		InboundProtocol:       u.meta.InboundProtocol,
		UpstreamProtocol:      u.meta.UpstreamProtocol,
		ProtocolConverted:     u.meta.ProtocolConverted,
		RequestType:           reqType,
		ServiceTier:           u.meta.ServiceTier,
		ReasoningEffort:       u.meta.ReasoningEffort,
		BillingMode:           billingMode,
		Unpriced:              unpriced,
		InputTokens:           tokens.InputTokens,
		OutputTokens:          tokens.OutputTokens,
		CacheCreationTokens:   tokens.CacheCreationTokens,
		CacheReadTokens:       tokens.CacheReadTokens,
		CacheCreation5mTokens: tokens.CacheCreation5mTokens,
		CacheCreation1hTokens: tokens.CacheCreation1hTokens,
		ImageOutputTokens:     tokens.ImageOutputTokens,
		ReasoningTokens:       tokens.ReasoningTokens,
		InputCost:             cost.InputCost,
		OutputCost:            cost.OutputCost,
		CacheCreationCost:     cost.CacheCreationCost,
		CacheReadCost:         cost.CacheReadCost,
		ImageOutputCost:       cost.ImageOutputCost,
		TotalCost:             cost.TotalCost,
		ActualCost:            cost.ActualCost,
		AccountStatsCost:      cost.TotalCost,
		RateMultiplier:        u.rate,
		BillingRateMultiplier: u.billingRate,
		// 账户侧统计用换算后的有效倍率（与路由计费倍率一致），而非独立默认 1
		AccountRateMultiplier: u.rate,
		Stream:                u.stream,
		StatusCode:            u.status,
		Success:               u.success,
		ErrorMessage:          truncateRunes(u.errInfo.Summary, rt.gatewayRuntime().UsageErrorMsgRunes),
		ErrorType:             u.errInfo.Type,
		ErrorDetail:           u.errInfo.Detail,
		UpstreamURL:           u.meta.UpstreamURL,
		UpstreamErrorBody:     u.errInfo.UpstreamBody,
		UpstreamErrorHeaders:  u.errInfo.UpstreamHeaders,
		DurationMS:            u.durationMS,
		FirstTokenMS:          u.firstTokenMS,
		IPAddress:             c.ClientIP(),
		UserAgent:             c.Request.UserAgent(),
		CreatedAt:             time.Now(),
	}
	rt.saveUsageLog(item)
	if u.success && cost.ActualCost > 0 {
		rt.addKeyQuotaUsed(u.key.ID, cost.ActualCost)
	}
}

// buildUpstreamErrorInfo 从转发失败结果拼装可落库的详细错误（截断上限用默认值，便于单测）。

// unpricedWarnEvery 同一个模型的「找不到价格」告警最多这么久打一次，避免日志刷屏。
const unpricedWarnEvery = 10 * time.Minute

// unpricedWarner 记录每个模型上次告警的时间。
type unpricedWarner struct {
	mu   sync.Mutex
	last map[string]time.Time
}

// shouldWarn 距上次告警超过 unpricedWarnEvery 时返回 true 并记下这次。
func (w *unpricedWarner) shouldWarn(model string, now time.Time) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if at, ok := w.last[model]; ok && now.Sub(at) < unpricedWarnEvery {
		return false
	}
	if w.last == nil {
		w.last = map[string]time.Time{}
	}
	w.last[model] = now
	return true
}

// warnUnpricedModel 成功的请求找不到模型价格：费用按 0 记、不扣额度，打告警提醒补价格。
func (rt *Runtime) warnUnpricedModel(model string, u usageRecord) {
	if rt.Log == nil || !rt.unpricedWarn.shouldWarn(model, time.Now()) {
		return
	}
	rt.Log.Warn("gateway model has no price; usage is recorded with zero cost — add a price override",
		"model", model,
		"requested_model", u.requestedModel,
		"group_id", u.group.ID,
		"route_id", u.route.ID,
		"request_id", u.requestID,
	)
}
