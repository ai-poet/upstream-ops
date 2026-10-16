// 数据面：上游请求构建与流式转发入口（缓冲 / 增量转换的读写见 runtime_stream_relay.go）。
package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

// buildUpstreamHTTPRequest 构建上游 HTTP 请求（forwardOnce / forwardStreamPlan / WS 握手共用）。
func (rt *Runtime) buildUpstreamHTTPRequest(ctx context.Context, call upstreamCall) (*http.Request, error) {
	target, inHeader, body, stream := call.target, call.header, call.body, call.stream
	if target == nil {
		return nil, errors.New("upstream target is nil")
	}
	apiKey := target.APIKey
	fullURL := joinUpstreamURL(target.BaseURL, call.path)

	upKind := protocol.NormalizeKind(call.kind)
	if stream && (upKind == protocol.KindOpenAIChat || upKind == protocol.KindOpenAI) {
		body = EnsureStreamUsageOption(body, true)
	}

	req, err := http.NewRequestWithContext(ctx, call.method, fullURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	for _, h := range []string{"Content-Type", "Accept", "User-Agent"} {
		if v := inHeader.Get(h); v != "" {
			req.Header.Set(h, v)
		}
	}
	// 请求相关 ID 原样转发上游；网关只在响应侧写 X-Upstream-Ops-Request-Id
	copyClientRequestIDHeaders(req.Header, inHeader)
	switch upKind {
	case protocol.KindAnthropic:
		if v := inHeader.Get("anthropic-version"); v != "" {
			req.Header.Set("anthropic-version", v)
		}
		if v := inHeader.Get("anthropic-beta"); v != "" {
			req.Header.Set("anthropic-beta", v)
		}
	case protocol.KindGemini:
		if v := inHeader.Get("x-goog-api-client"); v != "" {
			req.Header.Set("x-goog-api-client", v)
		}
	default:
		if v := inHeader.Get("OpenAI-Beta"); v != "" {
			req.Header.Set("OpenAI-Beta", v)
		}
	}
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	authStyle := storage.GatewayProviderAuthBoth
	if target.Provider != nil {
		authStyle = normalizeProviderAuthStyle(target.Provider.AuthStyle)
	}
	// Gemini 上游的密钥头是 x-goog-api-key（NewAPI / Sub2API 的 /v1beta 也认）
	keyHeader := "x-api-key"
	if upKind == protocol.KindGemini {
		keyHeader = "x-goog-api-key"
	}
	switch authStyle {
	case storage.GatewayProviderAuthBearer:
		req.Header.Set("Authorization", "Bearer "+apiKey)
	case storage.GatewayProviderAuthXAPIKey:
		req.Header.Set(keyHeader, apiKey)
	default:
		if upKind == protocol.KindGemini {
			// Google 原生端点会把 Authorization: Bearer 当 OAuth 令牌校验，API key 放这里会被拒成 401；
			// 默认只发 x-goog-api-key（要 Bearer 的上游可把鉴权方式显式设为 bearer）
			req.Header.Set(keyHeader, apiKey)
			break
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		if upKind == protocol.KindAnthropic {
			req.Header.Set(keyHeader, apiKey)
		}
	}
	if upKind == protocol.KindAnthropic {
		if req.Header.Get("anthropic-version") == "" {
			req.Header.Set("anthropic-version", "2023-06-01")
		}
	}
	if target.Provider != nil && strings.TrimSpace(target.Provider.ExtraHeadersJSON) != "" {
		var extra map[string]string
		if json.Unmarshal([]byte(target.Provider.ExtraHeadersJSON), &extra) == nil {
			for hk, hv := range extra {
				if strings.TrimSpace(hk) != "" && strings.TrimSpace(hv) != "" {
					req.Header.Set(hk, hv)
				}
			}
		}
	}
	// 组+路由 UA 策略：非空则覆盖客户端/ExtraHeaders 中的 UA；空则保持透传。
	// 转发 / 模型测试 / 拉模型共用 resolve（见 applyRouteUserAgent）。
	if ua := strings.TrimSpace(target.UserAgentOverride); ua != "" {
		req.Header.Set("User-Agent", ua)
	}
	if stream {
		req.Header.Set("Accept", "text/event-stream")
	}
	return req, nil
}

// errClientDisconnected 客户端主动断开/取消；已 commit 时仍会 drain 上游以同步 usage。
var errClientDisconnected = errors.New("client disconnected")

// forwardStreamPlan 真流式转发：边读上游 SSE 边写客户端（读写过程见 runtime_stream_relay.go）。
// 首字超时从发起上游请求起算（含等响应头），与 usage.first_token_ms 同一时钟。
//
// 上游请求不绑定客户端 context：客户端中途断开后继续读完上游 SSE，
// 尽量拿到完整 usage/计费（对齐 sub2api “continue draining for billing”）。
func (rt *Runtime) forwardStreamPlan(ctx context.Context, c *gin.Context, call upstreamCall, plan *conversionPlan) streamAttemptResult {
	call.kind, call.stream = plan.Upstream, true
	clientCtx := context.Background()
	if c != nil && c.Request != nil && c.Request.Context() != nil {
		clientCtx = c.Request.Context()
	}
	// 剥离客户端取消：否则 client disconnect 会连带取消 upstream body，usage 永远为 0
	upBase := context.Background()
	if ctx != nil {
		upBase = context.WithoutCancel(ctx)
	}
	gwCfg := rt.gatewayRuntime()
	upCtx, upCancel := context.WithTimeout(upBase, gwCfg.ForwardTimeout())

	// 可取消：仅首字超时 / 未 commit 的客户端断开时 abort
	reqCtx, abortReq := context.WithCancel(upCtx)

	// 结束时关掉上游响应体、取消上游请求。收到协议终态后不立刻关：HTTP/2 上在上游的 END_STREAM 到达前
	// 关 body / 取消请求，Go 会给上游发 RST_STREAM，每条流都被我们重置，上游的防滥用规则会回
	// GOAWAY ENHANCE_YOUR_CALM 把整条连接（连同复用它的所有流）断掉（线上 2026-10-16 实测）。
	// 所以终态之后让读协程在后台把剩下的读到 EOF，最多等 upstreamDrainGrace，客户端这边照常立刻收尾。
	var upstreamBody io.Closer
	var drainUntilEOF <-chan struct{}
	defer func() {
		release := func(drained bool) {
			if upstreamBody != nil {
				_ = upstreamBody.Close()
			}
			abortReq()
			upCancel()
			if !drained {
				rt.metrics.upstreamStreamReset()
			}
		}
		if drainUntilEOF == nil {
			release(true)
			return
		}
		go func() {
			timer := time.NewTimer(upstreamDrainGrace)
			defer timer.Stop()
			select {
			case <-drainUntilEOF:
				release(true)
			case <-timer.C:
				release(false)
			}
		}()
	}()

	req, err := rt.buildUpstreamHTTPRequest(reqCtx, call)
	if err != nil {
		return streamAttemptResult{Err: err}
	}

	client := rt.httpClientForTarget(call.target.Channel, call.target.Provider)
	start := time.Now()
	resp, err := doHTTPWithFirstTokenDeadline(reqCtx, abortReq, client, req, start, call.firstTokenTimeout)
	if err != nil {
		return streamAttemptResult{Err: err}
	}
	upstreamBody = resp.Body

	s := &streamRelay{
		rt:                rt,
		c:                 c,
		plan:              plan,
		abortReq:          abortReq,
		start:             start,
		firstTokenTimeout: call.firstTokenTimeout,
		result:            streamAttemptResult{Status: resp.StatusCode, Headers: resp.Header.Clone()},
	}
	// 非 2xx：整包读错误体，不 commit，供 retry/failover（错误体不受首字超时约束）
	if s.result.Status < 200 || s.result.Status >= 300 {
		errBody, readErr := io.ReadAll(io.LimitReader(resp.Body, maxUpstreamErrorBodyBytes))
		if readErr != nil && len(errBody) == 0 {
			s.result.Err = readErr
		} else {
			s.result.Body = errBody
		}
		return s.result
	}
	// 响应头已到：若整体首字预算已耗尽，直接按首字超时断开
	if _, timedOut := remainingFirstTokenWait(start, call.firstTokenTimeout); timedOut {
		return s.firstTokenTimedOut()
	}
	if !protocol.SupportsIncrementalStream(plan.Inbound, plan.Upstream, plan.Converted) {
		return s.relayBuffered(resp.Body)
	}
	result := s.relayIncremental(upCtx, clientCtx, resp.Body)
	if s.sawTerminal {
		drainUntilEOF = s.readerExit
	}
	return result
}

// upstreamDrainGrace 收到协议终态后，最多等上游这么久把流正常结束（见 forwardStreamPlan）。
var upstreamDrainGrace = 3 * time.Second

// joinUpstreamURL 拼上游 URL。方舟（Seedance）的 /api/v3/... 路径在 base 已以 /api/v3 或 /v3 结尾时
// 去掉重复的版本段，避免 /api/v3/api/v3/...。
func joinUpstreamURL(base, path string) string {
	base = strings.TrimRight(base, "/")
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	if strings.HasPrefix(path, "/api/v3/") {
		lower := strings.ToLower(base)
		if strings.HasSuffix(lower, "/api/v3") || strings.HasSuffix(lower, "/v3") {
			path = strings.TrimPrefix(path, "/api/v3")
		}
	}
	return base + path
}
