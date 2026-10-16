// 数据面：用量落库与计费。
package gateway

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

// recordUsage 计费并写一条用量日志；成功时累加密钥已用额度。
func (rt *Runtime) recordUsage(c *gin.Context, u usageRecord) {
	priceModel := u.upstreamModel
	if priceModel == "" {
		priceModel = u.requestedModel
	}
	// 对齐 sub2api RecordUsage：OpenAI 总输入含 cache 明细 → 拆互斥桶再计费/落库
	tokens := SplitOpenAIUsageBuckets(u.tokens)
	pricing := rt.Pricing.Resolve(priceModel)
	if pricing.BillingMode == BillingModeToken && u.meta.BillingInput.EndpointMode != "" {
		pricing.BillingMode = u.meta.BillingInput.EndpointMode
	}
	cost := CalculateCostWithBillingInput(pricing, tokens, u.rate, u.billingRate, u.meta.ServiceTier, u.meta.BillingInput)
	reqType := storage.GatewayRequestTypeSync
	if u.stream {
		reqType = storage.GatewayRequestTypeStream
	}
	billingMode := cost.BillingMode
	if billingMode == "" {
		billingMode = string(BillingModeToken)
	}
	sourceGroupName := strings.TrimSpace(u.route.SourceGroupName)
	if sourceGroupName == "" || rt.isSourceGroupIDPlaceholder(sourceGroupName) {
		// 运行时再解析一次：旧路由可能只有 id、name 为空
		if u.route.SourceGroupID != nil && *u.route.SourceGroupID > 0 && u.route.SourceChannelID > 0 {
			if gs := rt.loadGroupsByChannel(context.Background(), []storage.GatewayRoute{*u.route}); len(gs) > 0 {
				tmp := *u.route
				rt.enrichRouteSourceGroupName(&tmp, gs[u.route.SourceChannelID])
				if n := strings.TrimSpace(tmp.SourceGroupName); n != "" && !rt.isSourceGroupIDPlaceholder(n) {
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
		ErrorMessage:          rt.truncateRunes(u.errInfo.Summary, rt.gatewayRuntime().UsageErrorMsgRunes),
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
