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

func (rt *Runtime) recordUsage(
	key *storage.GatewayKey,
	group *storage.GatewayGroup,
	route *storage.GatewayRoute,
	target *upstreamTarget,
	reqID, requestedModel, upstreamModel, chain string,
	tokens UsageTokens,
	rate, billingRate float64,
	stream bool,
	status int,
	success bool,
	errInfo usageErrorInfo,
	durationMS int64,
	firstTokenMS *int64,
	c *gin.Context,
	meta usageRecordMeta,
) {
	priceModel := upstreamModel
	if priceModel == "" {
		priceModel = requestedModel
	}
	// 对齐 sub2api RecordUsage：OpenAI 总输入含 cache 明细 → 拆互斥桶再计费/落库
	tokens = SplitOpenAIUsageBuckets(tokens)
	pricing := rt.Pricing.Resolve(priceModel)
	if pricing.BillingMode == BillingModeToken && meta.BillingInput.EndpointMode != "" {
		pricing.BillingMode = meta.BillingInput.EndpointMode
	}
	cost := CalculateCostWithBillingInput(pricing, tokens, rate, billingRate, meta.ServiceTier, meta.BillingInput)
	reqType := storage.GatewayRequestTypeSync
	if stream {
		reqType = storage.GatewayRequestTypeStream
	}
	billingMode := cost.BillingMode
	if billingMode == "" {
		billingMode = string(BillingModeToken)
	}
	sourceGroupName := strings.TrimSpace(route.SourceGroupName)
	if sourceGroupName == "" || rt.isSourceGroupIDPlaceholder(sourceGroupName) {
		// 运行时再解析一次：旧路由可能只有 id、name 为空
		if route.SourceGroupID != nil && *route.SourceGroupID > 0 && route.SourceChannelID > 0 {
			if gs := rt.loadGroupsByChannel(context.Background(), []storage.GatewayRoute{*route}); len(gs) > 0 {
				tmp := *route
				rt.enrichRouteSourceGroupName(&tmp, gs[route.SourceChannelID])
				if n := strings.TrimSpace(tmp.SourceGroupName); n != "" && !rt.isSourceGroupIDPlaceholder(n) {
					sourceGroupName = n
				}
			}
		}
	}
	if sourceGroupName == "" && route.SourceGroupID != nil {
		sourceGroupName = fmt.Sprintf("id:%d", *route.SourceGroupID)
	}
	var channelID uint
	var providerID uint
	providerName := ""
	sourceKeyName := strings.TrimSpace(route.SourceAPIKeyName)
	sourceKeyID := route.SourceAPIKeyID
	if target != nil {
		if target.Channel != nil {
			channelID = target.Channel.ID
		}
		if target.Provider != nil {
			providerID = target.Provider.ID
			providerName = target.Provider.Name
			if sourceKeyName == "" {
				sourceKeyName = target.Provider.APIKeyHint
			}
		}
	} else if route != nil {
		channelID = route.SourceChannelID
		providerID = route.GatewayProviderID
	}
	attempt := meta.Attempt
	if attempt <= 0 {
		attempt = 1
	}
	attemptKind := strings.TrimSpace(meta.AttemptKind)
	if attemptKind == "" {
		attemptKind = attemptKindPrimary
	}
	item := &storage.GatewayUsageLog{
		GatewayGroupID:    group.ID,
		GatewayKeyID:      key.ID,
		RouteID:           route.ID,
		ChannelID:         channelID,
		GatewayProviderID: providerID,
		ProviderName:      providerName,
		// 路由快照：保存路由换 id 后历史记录仍可展示
		SourceAPIKeyID:        sourceKeyID,
		SourceAPIKeyName:      sourceKeyName,
		SourceGroupID:         route.SourceGroupID,
		SourceGroupName:       sourceGroupName,
		RequestID:             reqID,
		Attempt:               attempt,
		AttemptKind:           attemptKind,
		CooldownUntil:         meta.CooldownUntil,
		RequestedModel:        requestedModel,
		UpstreamModel:         upstreamModel,
		ModelMappingChain:     chain,
		InboundEndpoint:       meta.InboundEndpoint,
		UpstreamEndpoint:      meta.UpstreamEndpoint,
		InboundProtocol:       meta.InboundProtocol,
		UpstreamProtocol:      meta.UpstreamProtocol,
		ProtocolConverted:     meta.ProtocolConverted,
		RequestType:           reqType,
		ServiceTier:           meta.ServiceTier,
		ReasoningEffort:       meta.ReasoningEffort,
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
		RateMultiplier:        rate,
		BillingRateMultiplier: billingRate,
		// 账户侧统计用换算后的有效倍率（与路由计费倍率一致），而非独立默认 1
		AccountRateMultiplier: rate,
		Stream:                stream,
		StatusCode:            status,
		Success:               success,
		ErrorMessage:          rt.truncateRunes(errInfo.Summary, rt.gatewayRuntime().UsageErrorMsgRunes),
		ErrorType:             errInfo.Type,
		ErrorDetail:           errInfo.Detail,
		UpstreamURL:           meta.UpstreamURL,
		UpstreamErrorBody:     errInfo.UpstreamBody,
		UpstreamErrorHeaders:  errInfo.UpstreamHeaders,
		DurationMS:            durationMS,
		FirstTokenMS:          firstTokenMS,
		IPAddress:             c.ClientIP(),
		UserAgent:             c.Request.UserAgent(),
		CreatedAt:             time.Now(),
	}
	rt.saveUsageLog(item)
	if success && cost.ActualCost > 0 {
		rt.addKeyQuotaUsed(key.ID, cost.ActualCost)
	}
}

// buildUpstreamErrorInfo 从转发失败结果拼装可落库的详细错误（截断上限用默认值，便于单测）。
