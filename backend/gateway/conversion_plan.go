// 每次上游尝试的协议转换方案：选择 relaykit / 旧转换器，并贯穿请求、响应、流式三条路径。
package gateway

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
)

// conversionPlan 一次上游尝试的转换方案。请求转换时建立，响应 / 流式转换沿用同一份状态
// （relaykit 的工具编码与推理设置、旧转换器的 Responses 工具桥都记在这里）。
type conversionPlan struct {
	Inbound   protocolKind
	Upstream  protocolKind
	Model     string // 映射后的上游模型
	Stream    bool
	Converted bool
	// Engine relaykit | legacy；未转换时为空
	Engine string
	Bridge protocol.ResponsesToolBridge
	RK     *protocol.Conversion
	// GeminiArray 客户端是 Gemini 且未带 alt=sse：流式响应按 JSON 数组逐元素写出
	GeminiArray bool
}

// errProtocolConvert 请求转换失败（如 relaykit 拒绝把有状态的 Responses 请求降级到 Chat）。
// 只说明这条路由的上游协议接不住该请求，可以换下一条路由。
type errProtocolConvert struct{ err error }

func (e *errProtocolConvert) Error() string { return "protocol convert failed: " + e.err.Error() }
func (e *errProtocolConvert) Unwrap() error { return e.err }

func isProtocolConvertError(err error) bool {
	var target *errProtocolConvert
	return errors.As(err, &target)
}

// planUpstreamRequest 按入站 / 上游协议准备转发 body 与 path，并给出后续响应转换方案。
func (svc *core) planUpstreamRequest(
	ctx context.Context,
	body []byte,
	inbound, upstream protocolKind,
	originModel, upstreamModel string,
	stream bool,
	inboundPath string,
) ([]byte, string, *conversionPlan, error) {
	in := protocol.NormalizeKind(inbound)
	up := protocol.NormalizeKind(upstream)
	plan := &conversionPlan{Inbound: in, Upstream: up, Model: upstreamModel, Stream: stream}
	upstreamPath := protocol.UpstreamPath(up, inboundPath, upstreamModel, stream)

	// 透传端点：不做协议转换；只有 body 原本带 model 时才改写，避免给不认识 model 的接口塞字段
	if protocol.IsPassthroughEndpoint(inboundPath) {
		// Gemini embedContent / batchEmbedContents：模型在 URL 上，换成映射后的上游模型；
		// body 里的 model（批量时是 requests[].model）按 Gemini 要求写成 models/{model} 并与 URL 一致
		if up == protocol.KindGemini && upstreamModel != "" && strings.HasPrefix(inboundPath, "/v1beta/models/") {
			m := strings.TrimPrefix(upstreamModel, "models/")
			if i := strings.LastIndex(inboundPath, ":"); i > 0 {
				upstreamPath = "/v1beta/models/" + m + inboundPath[i:]
			}
			return rewriteGeminiEmbedModels(body, "models/"+m), upstreamPath, plan, nil
		}
		fwd := body
		if ExtractModelFromBody(body) != "" {
			fwd = RewriteModelInBody(body, upstreamModel)
		}
		return fwd, upstreamPath, plan, nil
	}

	if !protocol.NeedsBodyConvert(in, up) {
		fwd := body
		if up != protocol.KindGemini {
			fwd = RewriteModelInBody(body, upstreamModel)
		}
		if stream && up == protocol.KindOpenAIChat {
			fwd = EnsureStreamUsageOption(fwd, stream)
		}
		return fwd, upstreamPath, plan, nil
	}

	plan.Converted = true
	var fwd []byte
	if protocol.UseRelayKitFor(in, up) && !needsLegacyToolBridge(in, up, body) {
		rk := protocol.NewConversion(in, up, originModel, upstreamModel, stream)
		out, err := rk.Request(ctx, body)
		if err != nil {
			return nil, "", nil, &errProtocolConvert{err: err}
		}
		plan.RK = rk
		plan.Engine = protocol.EngineRelayKit
		fwd = out
	} else {
		out, bridge, err := svc.prepareLegacyUpstreamRequest(RewriteModelInBody(body, upstreamModel), in, up, upstreamModel, stream)
		if err != nil {
			return nil, "", nil, &errProtocolConvert{err: err}
		}
		plan.Bridge = bridge
		plan.Engine = protocol.EngineLegacy
		fwd = out
	}
	if stream && up == protocol.KindOpenAIChat {
		fwd = EnsureStreamUsageOption(fwd, stream)
	}
	return fwd, upstreamPath, plan, nil
}

// rewriteGeminiEmbedModels 把 Gemini embedding 请求体里已有的 model 与 requests[].model 改成 model；
// 不新增字段，其余内容原样保留。
func rewriteGeminiEmbedModels(body []byte, model string) []byte {
	var obj map[string]json.RawMessage
	if json.Unmarshal(body, &obj) != nil {
		return body
	}
	quoted, _ := json.Marshal(model)
	changed := false
	if _, ok := obj["model"]; ok {
		obj["model"] = quoted
		changed = true
	}
	if raw, ok := obj["requests"]; ok {
		var reqs []map[string]json.RawMessage
		if json.Unmarshal(raw, &reqs) == nil {
			for _, r := range reqs {
				if _, ok := r["model"]; ok {
					r["model"] = quoted
					changed = true
				}
			}
			if out, err := json.Marshal(reqs); err == nil {
				obj["requests"] = out
			}
		}
	}
	if !changed {
		return body
	}
	out, err := json.Marshal(obj)
	if err != nil {
		return body
	}
	return out
}

// needsLegacyToolBridge relaykit 不实现 tool_search 往返；带 tool_search 的 Responses 请求
// 降级到 Chat / Anthropic 时继续用旧转换器的工具桥。
func needsLegacyToolBridge(in, up protocolKind, body []byte) bool {
	if in != protocol.KindOpenAIResponses || (up != protocol.KindOpenAIChat && up != protocol.KindAnthropic) {
		return false
	}
	if !bytes.Contains(body, []byte("tool_search")) {
		return false
	}
	var req struct {
		Tools []struct {
			Type string `json:"type"`
		} `json:"tools"`
		Input json.RawMessage `json:"input"`
	}
	if json.Unmarshal(body, &req) != nil {
		return true
	}
	for _, t := range req.Tools {
		if t.Type == "tool_search" {
			return true
		}
	}
	var items []struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(req.Input, &items) == nil {
		for _, it := range items {
			if it.Type == "tool_search_call" || it.Type == "tool_search_output" {
				return true
			}
		}
	}
	return false
}

// defaultStreamPlan 供未经 planUpstreamRequest 的调用方（测试 / 旧入口）构造流式方案。
func defaultStreamPlan(inbound, upstream protocolKind, model string, converted bool) *conversionPlan {
	in := protocol.NormalizeKind(inbound)
	up := protocol.NormalizeKind(upstream)
	plan := &conversionPlan{Inbound: in, Upstream: up, Model: model, Stream: true, Converted: converted}
	if !converted {
		return plan
	}
	if protocol.UseRelayKitFor(in, up) {
		plan.RK = protocol.NewConversion(in, up, "", model, true)
		plan.Engine = protocol.EngineRelayKit
	} else {
		plan.Engine = protocol.EngineLegacy
	}
	return plan
}

// convertResponseWithPlan 非流式响应转回客户端协议；relaykit 失败时退回旧转换器。
func (rt *Runtime) convertResponseWithPlan(ctx context.Context, respBody []byte, plan *conversionPlan) []byte {
	if plan == nil || !plan.Converted || len(respBody) == 0 {
		return respBody
	}
	if plan.RK != nil {
		out, err := plan.RK.Response(ctx, respBody)
		if err == nil && len(out) > 0 {
			return out
		}
		if rt.Log != nil {
			rt.Log.Warn("relaykit response convert failed", "inbound", plan.Inbound, "upstream", plan.Upstream, "err", err)
		}
		if plan.Inbound == protocol.KindGemini || plan.Upstream == protocol.KindGemini {
			return respBody
		}
		return rt.convertUpstreamResponse(respBody, plan.Inbound, plan.Upstream, plan.Model, false, true)
	}
	return rt.convertUpstreamResponseWithToolBridge(respBody, plan.Inbound, plan.Upstream, plan.Model, false, true, plan.Bridge)
}

// newStreamConverter 创建上游 SSE → 客户端 SSE 的增量转换器；未转换时返回 nil（原样透传）。
func (rt *Runtime) newStreamConverter(ctx context.Context, plan *conversionPlan) protocol.StreamConverter {
	if plan == nil || !plan.Converted {
		return nil
	}
	if plan.RK != nil {
		conv, err := plan.RK.NewStream(ctx, newStreamID(plan.Inbound))
		if err == nil {
			return conv
		}
		if rt != nil && rt.Log != nil {
			rt.Log.Warn("relaykit stream init failed", "inbound", plan.Inbound, "upstream", plan.Upstream, "err", err)
		}
		if plan.Inbound == protocol.KindGemini || plan.Upstream == protocol.KindGemini {
			return nil
		}
	}
	return legacyStreamConverter(plan.Inbound, plan.Upstream, plan.Model, plan.Bridge)
}

type legacyStream struct {
	feed  func(eventName, data string) [][]byte
	close func() [][]byte
}

func (s legacyStream) Feed(eventName, data string) [][]byte { return s.feed(eventName, data) }
func (s legacyStream) Close() [][]byte                      { return s.close() }

// legacyStreamConverter 旧转换器的六个方向。
func legacyStreamConverter(in, up protocolKind, model string, bridge protocol.ResponsesToolBridge) protocol.StreamConverter {
	isChat := func(k protocolKind) bool { return k == protocol.KindOpenAIChat || k == protocol.KindOpenAI }
	switch {
	case isChat(in) && up == protocol.KindAnthropic:
		s := protocol.NewAnthropicToOpenAIStream(model)
		return legacyStream{feed: s.Feed, close: s.Close}
	case in == protocol.KindAnthropic && isChat(up):
		s := protocol.NewOpenAIToAnthropicStream(model)
		return legacyStream{
			feed: func(_ string, data string) [][]byte {
				var frames [][]byte
				if !s.Started {
					frames = append(frames, s.EnsureStarted()...)
				}
				return append(frames, s.FeedData(data)...)
			},
			close: s.Close,
		}
	case in == protocol.KindAnthropic && up == protocol.KindOpenAIResponses:
		s := protocol.NewResponsesToAnthropicStream(model)
		return legacyStream{feed: s.Feed, close: s.Close}
	case isChat(in) && up == protocol.KindOpenAIResponses:
		s := protocol.NewResponsesToOpenAIStream(model)
		return legacyStream{feed: s.Feed, close: s.Close}
	case in == protocol.KindOpenAIResponses && up == protocol.KindAnthropic:
		s := protocol.NewAnthropicToResponsesStreamWithToolBridge(model, bridge)
		return legacyStream{feed: s.Feed, close: s.Close}
	case in == protocol.KindOpenAIResponses && isChat(up):
		s := protocol.NewChatToResponsesStreamWithToolBridge(model, bridge)
		return legacyStream{feed: func(_ string, data string) [][]byte { return s.FeedData(data) }, close: s.Close}
	}
	return nil
}

func newStreamID(client protocolKind) string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	suffix := hex.EncodeToString(b[:])
	switch protocol.NormalizeKind(client) {
	case protocol.KindOpenAIResponses:
		return "resp_" + suffix
	case protocol.KindAnthropic:
		return "msg_" + suffix
	default:
		return fmt.Sprintf("chatcmpl-%s", suffix)
	}
}
