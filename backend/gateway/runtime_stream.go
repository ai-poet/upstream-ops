// 数据面：流式转发（缓冲 / 增量转换）。
package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

// buildUpstreamHTTPRequest 构建上游 HTTP 请求。
func (rt *Runtime) buildUpstreamHTTPRequest(
	ctx context.Context,
	target *upstreamTarget,
	path string,
	method string,
	inHeader http.Header,
	body []byte,
	kind protocolKind,
	stream bool,
) (*http.Request, error) {
	if target == nil {
		return nil, errors.New("upstream target is nil")
	}
	apiKey := target.APIKey
	fullURL := joinUpstreamURL(target.BaseURL, path)

	upKind := protocol.NormalizeKind(kind)
	if stream && (upKind == protocol.KindOpenAIChat || upKind == protocol.KindOpenAI) {
		body = EnsureStreamUsageOption(body, true)
	}

	req, err := http.NewRequestWithContext(ctx, method, fullURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	for _, h := range []string{"Content-Type", "Accept", "User-Agent"} {
		if v := inHeader.Get(h); v != "" {
			req.Header.Set(h, v)
		}
	}
	// 请求相关 ID 原样转发上游；网关只在响应侧写 X-Upstream-Ops-Request-Id
	rt.copyClientRequestIDHeaders(req.Header, inHeader)
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
		authStyle = rt.normalizeProviderAuthStyle(target.Provider.AuthStyle)
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

// forwardStream 真流式转发：边读上游 SSE 边写客户端。
// 首字超时从发起上游请求起算（含等响应头），与 usage.first_token_ms 同一时钟。
//
// 上游请求不绑定客户端 context：客户端中途断开后继续读完上游 SSE，
// 尽量拿到完整 usage/计费（对齐 sub2api “continue draining for billing”）。

func (rt *Runtime) forwardStream(
	ctx context.Context,
	c *gin.Context,
	target *upstreamTarget,
	path string,
	method string,
	inHeader http.Header,
	body []byte,
	inboundKind protocolKind,
	upstreamKind protocolKind,
	model string,
	converted bool,
	firstTokenTimeout time.Duration,
) streamAttemptResult {
	return rt.forwardStreamPlan(ctx, c, target, path, method, inHeader, body, defaultStreamPlan(inboundKind, upstreamKind, model, converted), firstTokenTimeout)
}

func (rt *Runtime) forwardStreamPlan(
	ctx context.Context,
	c *gin.Context,
	target *upstreamTarget,
	path string,
	method string,
	inHeader http.Header,
	body []byte,
	plan *conversionPlan,
	firstTokenTimeout time.Duration,
) streamAttemptResult {
	upstreamKind := plan.Upstream
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
	defer upCancel()

	// 可取消：仅首字超时 / 未 commit 的客户端断开时 abort
	reqCtx, abortReq := context.WithCancel(upCtx)
	defer abortReq()

	req, err := rt.buildUpstreamHTTPRequest(reqCtx, target, path, method, inHeader, body, upstreamKind, true)
	if err != nil {
		return streamAttemptResult{Err: err}
	}

	client := rt.httpClientForTarget(target.Channel, target.Provider)
	start := time.Now()
	resp, err := rt.doHTTPWithFirstTokenDeadline(reqCtx, abortReq, client, req, start, firstTokenTimeout)
	if err != nil {
		return streamAttemptResult{Err: err}
	}
	defer func() { _ = resp.Body.Close() }()

	headers := resp.Header.Clone()
	status := resp.StatusCode

	// 非 2xx：整包读错误体，不 commit，供 retry/failover（错误体不受首字超时约束）
	if status < 200 || status >= 300 {
		errBody, readErr := io.ReadAll(io.LimitReader(resp.Body, int64(gwCfg.UsageErrorBodyBytes)+1))
		if readErr != nil && len(errBody) == 0 {
			return streamAttemptResult{Status: status, Headers: headers, Err: readErr}
		}
		return streamAttemptResult{Status: status, Headers: headers, Body: errBody}
	}

	// 响应头已到：若整体首字预算已耗尽，直接按首字超时断开
	if _, timedOut := rt.remainingFirstTokenWait(start, firstTokenTimeout); timedOut {
		ms := time.Since(start).Milliseconds()
		ft := &ms
		return streamAttemptResult{
			Status: status, Headers: headers, FirstTokenMS: ft,
			Err: fmt.Errorf("%w after %s", errFirstTokenTimeout, firstTokenTimeout),
		}
	}

	incremental := protocol.SupportsIncrementalStream(plan.Inbound, plan.Upstream, plan.Converted)
	if !incremental {
		return rt.forwardStreamBufferedPlan(c, resp, start, firstTokenTimeout, plan, headers, status)
	}
	return rt.forwardStreamIncrementalPlan(upCtx, clientCtx, abortReq, c, resp, start, firstTokenTimeout, plan, headers, status)
}

// forwardStreamBuffered 仅兜底：三协议互转已走增量真流；此处保留给未知协议或未实现转换器。
// 行为：先读完再转换，再一次性写出（首写出前仍可 failover）——即「假流」，应尽量避免进入。

func (rt *Runtime) forwardStreamBuffered(
	c *gin.Context,
	resp *http.Response,
	start time.Time,
	firstTokenTimeout time.Duration,
	inbound, upstream protocolKind,
	model string,
	converted bool,
	headers http.Header,
	status int,
) streamAttemptResult {
	return rt.forwardStreamBufferedPlan(c, resp, start, firstTokenTimeout, defaultStreamPlan(inbound, upstream, model, converted), headers, status)
}

func (rt *Runtime) forwardStreamBufferedPlan(
	c *gin.Context,
	resp *http.Response,
	start time.Time,
	firstTokenTimeout time.Duration,
	plan *conversionPlan,
	headers http.Header,
	status int,
) streamAttemptResult {
	inbound, upstream, model, converted, bridge := plan.Inbound, plan.Upstream, plan.Model, plan.Converted, plan.Bridge
	var ft *int64
	// 从请求发起起算剩余首字预算（含已花费的等响应头时间）
	bodyWait, timedOut := rt.remainingFirstTokenWait(start, firstTokenTimeout)
	if timedOut {
		ms := time.Since(start).Milliseconds()
		ft = &ms
		return streamAttemptResult{Status: status, Headers: headers, FirstTokenMS: ft, Err: fmt.Errorf("%w after %s", errFirstTokenTimeout, firstTokenTimeout)}
	}
	firstChunk, firstErr := rt.readFirstChunk(resp.Body, bodyWait)
	if firstErr != nil {
		if rt.isFirstTokenTimeout(firstErr) {
			ms := time.Since(start).Milliseconds()
			ft = &ms
			return streamAttemptResult{Status: status, Headers: headers, FirstTokenMS: ft, Err: fmt.Errorf("%w after %s", errFirstTokenTimeout, firstTokenTimeout)}
		}
		return streamAttemptResult{Status: status, Headers: headers, Err: firstErr}
	}
	var data []byte
	if len(firstChunk) > 0 {
		ms := time.Since(start).Milliseconds()
		ft = &ms
		data = append(data, firstChunk...)
	}
	rest, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return streamAttemptResult{Status: status, Headers: headers, Body: data, FirstTokenMS: ft, Err: readErr}
	}
	data = append(data, rest...)

	tokens := rt.parseUsageByKind(data, true, upstream)
	clientBody := rt.convertUpstreamResponseWithToolBridge(data, inbound, upstream, model, true, converted, bridge)
	if len(clientBody) == 0 {
		clientBody = data
	}

	if err := rt.commitSSEHeaders(c, headers); err != nil {
		return streamAttemptResult{Status: status, Headers: headers, Body: clientBody, FirstTokenMS: ft, Tokens: tokens, Err: err}
	}
	if _, err := c.Writer.Write(clientBody); err != nil {
		return streamAttemptResult{
			Status: status, Headers: headers, FirstTokenMS: ft, Tokens: tokens,
			Committed: true, ClientDisconnected: true, StreamErr: errClientDisconnected,
		}
	}
	rt.flushWriter(c)
	if ft == nil {
		ms := time.Since(start).Milliseconds()
		ft = &ms
	}
	// 缓冲路径整包写出成功即视为下游完整交付（含转换后的终端帧）。
	return streamAttemptResult{
		Status: status, Headers: headers, FirstTokenMS: ft, Tokens: tokens,
		Committed: true, DownstreamComplete: true,
	}
}

// forwardStreamIncremental 同协议透传或 Anthropic↔OpenAI 增量转换。
// upCtx：上游生命周期（与客户端取消解耦）；clientCtx：检测客户端断开。
// abortReq：仅未 commit 或首字超时时中止上游；已 commit 的客户端断开不调用，以便 drain 计费。

func (rt *Runtime) forwardStreamIncremental(
	upCtx context.Context,
	clientCtx context.Context,
	abortReq context.CancelFunc,
	c *gin.Context,
	resp *http.Response,
	start time.Time,
	firstTokenTimeout time.Duration,
	inbound, upstream protocolKind,
	model string,
	converted bool,
	headers http.Header,
	status int,
) streamAttemptResult {
	return rt.forwardStreamIncrementalPlan(upCtx, clientCtx, abortReq, c, resp, start, firstTokenTimeout, defaultStreamPlan(inbound, upstream, model, converted), headers, status)
}

func (rt *Runtime) forwardStreamIncrementalPlan(
	upCtx context.Context,
	clientCtx context.Context,
	abortReq context.CancelFunc,
	c *gin.Context,
	resp *http.Response,
	start time.Time,
	firstTokenTimeout time.Duration,
	plan *conversionPlan,
	headers http.Header,
	status int,
) streamAttemptResult {
	if upCtx == nil {
		upCtx = context.Background()
	}
	if clientCtx == nil {
		clientCtx = context.Background()
	}
	result := streamAttemptResult{Status: status, Headers: headers}
	clientKind := protocol.NormalizeKind(plan.Inbound)
	upKind := protocol.NormalizeKind(plan.Upstream)
	gwCfg := rt.gatewayRuntime()
	maxLineSize := gwCfg.StreamMaxLineSize()

	// conv 为 nil 表示同协议透传
	conv := rt.newStreamConverter(upCtx, plan)
	// Gemini 客户端未带 alt=sse 时按 JSON 数组逐元素写出
	var geminiArray *geminiArrayEncoder
	if clientKind == protocol.KindGemini && plan.GeminiArray {
		geminiArray = &geminiArrayEncoder{}
	}
	// Gemini 上游没有 [DONE]：见过 finishReason 后 EOF 才算正常结束
	geminiUpstreamFinished := false
	// 数组模式下已向客户端写出带 finishReason 的元素（数组本身可能还没闭合）
	geminiFinishSent := false

	type scanEvent struct {
		line string
		err  error
	}
	events := make(chan scanEvent, 32)
	done := make(chan struct{})
	var lastReadAt int64
	atomic.StoreInt64(&lastReadAt, time.Now().UnixNano())

	go func() {
		defer close(events)
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 0, 64*1024), maxLineSize)
		for scanner.Scan() {
			atomic.StoreInt64(&lastReadAt, time.Now().UnixNano())
			line := strings.TrimRight(scanner.Text(), "\r")
			select {
			case events <- scanEvent{line: line}:
			case <-done:
				return
			}
		}
		if err := scanner.Err(); err != nil {
			select {
			case events <- scanEvent{err: err}:
			case <-done:
			}
		}
	}()
	defer close(done)

	// 首字超时：从 start（发起上游请求）起算剩余时间，与 first_token_ms 同一时钟。
	// 注意：不是「响应头之后再等 N 秒」。
	var firstTimer *time.Timer
	var firstCh <-chan time.Time
	if firstTokenTimeout > 0 {
		left, timedOut := rt.remainingFirstTokenWait(start, firstTokenTimeout)
		if timedOut {
			ms := time.Since(start).Milliseconds()
			result.FirstTokenMS = &ms
			result.Err = fmt.Errorf("%w after %s", errFirstTokenTimeout, firstTokenTimeout)
			return result
		}
		firstTimer = time.NewTimer(left)
		defer firstTimer.Stop()
		firstCh = firstTimer.C
	}

	keepalive := gwCfg.StreamKeepalive()
	idleTimeout := gwCfg.StreamIdleTimeout()
	var keepTicker *time.Ticker
	if keepalive > 0 {
		keepTicker = time.NewTicker(keepalive)
		defer keepTicker.Stop()
	}
	var idleTicker *time.Ticker
	if idleTimeout > 0 {
		idleTicker = time.NewTicker(time.Second)
		defer idleTicker.Stop()
	}

	var (
		pendingLines     []string
		sawUpstreamData  bool
		errorEventSent   bool
		lastDownstreamAt = time.Now()
		usageBuf         bytes.Buffer
		tokens           UsageTokens
	)
	clientDone := clientCtx.Done()

	stopFirstTimer := func() {
		if firstTimer != nil {
			if !firstTimer.Stop() {
				select {
				case <-firstTimer.C:
				default:
				}
			}
			firstCh = nil
		}
	}

	writeFrames := func(frames [][]byte) error {
		if len(frames) == 0 {
			return nil
		}
		if !result.Committed {
			if err := rt.commitSSEHeaders(c, headers); err != nil {
				return err
			}
			if geminiArray != nil {
				// Gemini REST 非 SSE 流式：JSON 数组
				c.Header("Content-Type", "application/json; charset=utf-8")
			}
			result.Committed = true
			ms := time.Since(start).Milliseconds()
			result.FirstTokenMS = &ms
			stopFirstTimer()
		}
		if result.ClientDisconnected {
			return nil
		}
		if geminiArray != nil {
			frames = geminiArray.encode(frames)
		}
		for _, f := range frames {
			if len(f) == 0 {
				continue
			}
			if _, err := c.Writer.Write(f); err != nil {
				result.ClientDisconnected = true
				return err
			}
			if geminiArray != nil {
				// 数组模式：写出 "]" 才算完整交付；带 finishReason 的元素只记下已给出终态
				if geminiArray.closed {
					result.DownstreamComplete = true
				} else if rt.sseFrameIsTerminal(f) {
					geminiFinishSent = true
				}
			} else if rt.sseFrameIsTerminal(f) {
				result.DownstreamComplete = true
			}
		}
		rt.flushWriter(c)
		lastDownstreamAt = time.Now()
		return nil
	}

	processEvent := func(lines []string) error {
		if len(lines) == 0 {
			return nil
		}
		eventName, data := rt.parseSSEEventLines(lines)
		// 旁路 usage：累积原始 data
		if data != "" && data != "[DONE]" {
			usageBuf.WriteString(data)
			usageBuf.WriteByte('\n')
			rt.mergeStreamUsage(&tokens, data, upKind)
		}

		if upKind == protocol.KindGemini && geminiChunkFinished(data) {
			geminiUpstreamFinished = true
		}
		var frames [][]byte
		switch {
		case conv != nil:
			frames = conv.Feed(eventName, data)
		default:
			// 透传完整 event。commit 前丢弃纯注释/空事件，避免上游 :PING 过早
			// 提交响应头后无法再 retry/failover，并污染 first_token 统计。
			if !result.Committed && !rt.sseEventHasPayload(lines) {
				return nil
			}
			var b strings.Builder
			for _, ln := range lines {
				b.WriteString(ln)
				b.WriteByte('\n')
			}
			b.WriteByte('\n')
			frames = [][]byte{[]byte(b.String())}
		}
		return writeFrames(frames)
	}

	sendTerminalError := func(errType, msg string) {
		if errorEventSent || result.ClientDisconnected {
			return
		}
		if !result.Committed {
			return
		}
		errorEventSent = true
		if geminiArray != nil {
			// 已给出 finishReason：客户端已拿到完整回答，只闭合数组（与 SSE 模式终态后不再补错误一致）
			frames := [][]byte{geminiErrorFrame(errType, msg), geminiArrayCloseFrame}
			if geminiFinishSent {
				frames = frames[1:]
			}
			if err := writeFrames(frames); err == nil {
				result.DownstreamComplete = true
			}
			return
		}
		if err := rt.writeStreamTerminalError(c, clientKind, errType, msg); err == nil {
			result.DownstreamComplete = true
		}
	}

	// 转换器遇到无法解析 / 转换的上游事件：之后的输出不可信，按失败结束（未提交时本次尝试失败，可重试 / 顺延）
	failOnConvertError := func() bool {
		ce, ok := conv.(interface{ Err() error })
		if !ok || ce.Err() == nil {
			return false
		}
		convErr := ce.Err()
		if rt.Log != nil {
			rt.Log.Warn("gateway stream convert failed", "inbound", plan.Inbound, "upstream", plan.Upstream, "err", convErr)
		}
		result.Body = append([]byte(nil), usageBuf.Bytes()...)
		result.Tokens = rt.finalizeStreamTokens(tokens, result.Body, upKind)
		msg := "stream convert failed: " + convErr.Error()
		if !result.Committed {
			result.Err = errors.New(msg)
			if abortReq != nil {
				abortReq()
			}
			return true
		}
		if !result.ClientDisconnected {
			sendTerminalError("upstream_error", msg)
		}
		result.StreamErr = errors.New(msg)
		rt.finalizeStreamClientDisconnect(&result)
		return true
	}

	closeConverters := func() {
		var frames [][]byte
		if conv != nil {
			frames = conv.Close()
		}
		if geminiArray != nil {
			_ = writeFrames(frames)
			_ = writeFrames([][]byte{geminiArrayCloseFrame})
			return
		}
		_ = writeFrames(frames)
	}

	for {
		var keepCh <-chan time.Time
		if keepTicker != nil && result.Committed && !result.ClientDisconnected {
			keepCh = keepTicker.C
		}
		var idleCh <-chan time.Time
		if idleTicker != nil {
			idleCh = idleTicker.C
		}

		select {
		case <-upCtx.Done():
			// 上游整体超时（非客户端取消）
			if !result.Committed {
				result.Err = upCtx.Err()
				if abortReq != nil {
					abortReq()
				}
				return result
			}
			result.Tokens = rt.finalizeStreamTokens(tokens, usageBuf.Bytes(), upKind)
			if result.ClientDisconnected {
				// 客户端已断，上游超时：用已收集 usage 落库；若已交付终端帧则不当 client 失败
				rt.finalizeStreamClientDisconnect(&result)
				return result
			}
			msg := "upstream stream canceled"
			if err := upCtx.Err(); err != nil {
				msg = err.Error()
			}
			sendTerminalError("stream_timeout", msg)
			result.StreamErr = errors.New(msg)
			return result

		case <-clientDone:
			// 客户端断开 / 取消
			if !result.Committed {
				// 尚未向客户端提交：中止上游，允许上层记账为客户端取消（不 drain 计费，也无半截响应）
				result.ClientDisconnected = true
				if abortReq != nil {
					abortReq()
				}
				if err := clientCtx.Err(); err != nil {
					result.Err = err
				} else {
					result.Err = errClientDisconnected
				}
				return result
			}
			// 已 commit：不 abort 上游，标记后继续主循环 drain，对齐 sub2api billing drain。
			// 若已写出终端帧，结束时会清掉 client 标签（正常收尾关连接）。
			if !result.ClientDisconnected {
				result.ClientDisconnected = true
			}
			clientDone = nil
			continue

		case <-firstCh:
			// 仅在尚未收到「有效」上游载荷时触发（ping/注释不算）
			if !sawUpstreamData && !result.Committed {
				result.Err = fmt.Errorf("%w after %s", errFirstTokenTimeout, firstTokenTimeout)
				ms := time.Since(start).Milliseconds()
				result.FirstTokenMS = &ms
				return result
			}

		case <-keepCh:
			if result.ClientDisconnected || !result.Committed {
				continue
			}
			if time.Since(lastDownstreamAt) < keepalive {
				continue
			}
			ping := rt.streamKeepaliveFrame(clientKind)
			if geminiArray != nil {
				// JSON 数组里只能插空白
				ping = []byte("\r\n")
			}
			if _, err := c.Writer.Write(ping); err != nil {
				result.ClientDisconnected = true
				continue
			}
			rt.flushWriter(c)
			lastDownstreamAt = time.Now()

		case <-idleCh:
			if !sawUpstreamData {
				continue
			}
			lastRead := time.Unix(0, atomic.LoadInt64(&lastReadAt))
			if time.Since(lastRead) < idleTimeout {
				continue
			}
			msg := fmt.Sprintf("upstream stream idle for %s", idleTimeout)
			if result.Committed {
				sendTerminalError("stream_timeout", msg)
				result.StreamErr = errors.New(msg)
				result.Tokens = rt.finalizeStreamTokens(tokens, usageBuf.Bytes(), upKind)
				return result
			}
			result.Err = errors.New(msg)
			return result

		case ev, ok := <-events:
			if !ok {
				// 上游结束：没有协议终态时必须按失败处理，不能由转换器补一个成功帧。
				if len(pendingLines) > 0 {
					if err := processEvent(pendingLines); err != nil && !result.ClientDisconnected {
						result.StreamErr = err
					}
					pendingLines = pendingLines[:0]
				}
				if failOnConvertError() {
					return result
				}
				result.Body = append([]byte(nil), usageBuf.Bytes()...)
				result.Tokens = rt.finalizeStreamTokens(tokens, result.Body, upKind)
				if geminiUpstreamFinished && result.Committed && !result.DownstreamComplete {
					// Gemini 上游以 EOF 结束：补齐客户端终态（转换器收尾 / 数组闭合）
					closeConverters()
					if conv == nil && geminiArray == nil {
						result.DownstreamComplete = true
					}
				}
				if !result.DownstreamComplete {
					msg := "upstream stream closed before terminal event"
					if !result.Committed {
						result.Err = errors.New(msg)
						return result
					}
					if !result.ClientDisconnected {
						sendTerminalError("stream_incomplete", msg)
					}
					result.StreamErr = errors.New(msg)
					rt.finalizeStreamClientDisconnect(&result)
					return result
				}
				if !result.Committed {
					// 空流
					result.Body = usageBuf.Bytes()
					result.Tokens = rt.finalizeStreamTokens(tokens, result.Body, upKind)
					// 仍写出转换收尾（可能只有 [DONE]）
					closeConverters()
					if !result.Committed {
						// 无任何输出：视为成功空流，commit 空 SSE 结束
						if err := rt.commitSSEHeaders(c, headers); err == nil {
							if geminiArray != nil {
								c.Header("Content-Type", "application/json; charset=utf-8")
							}
							result.Committed = true
							if conv == nil && geminiArray == nil && clientKind != protocol.KindGemini {
								doneFrame := []byte("data: [DONE]\n\n")
								if _, werr := c.Writer.Write(doneFrame); werr == nil {
									result.DownstreamComplete = true
								}
								rt.flushWriter(c)
							}
						}
					}
					result.Tokens = rt.finalizeStreamTokens(tokens, result.Body, upKind)
					rt.finalizeStreamClientDisconnect(&result)
					return result
				}
				// 已 commit：收尾转换（客户端已断则 writeFrames 会跳过写出）
				closeConverters()
				result.Tokens = rt.finalizeStreamTokens(tokens, usageBuf.Bytes(), upKind)
				rt.finalizeStreamClientDisconnect(&result)
				return result
			}
			if ev.err != nil {
				if !result.Committed {
					if errors.Is(ev.err, bufio.ErrTooLong) {
						result.Err = fmt.Errorf("upstream SSE line exceeded %d bytes", maxLineSize)
					} else {
						result.Err = ev.err
					}
					return result
				}
				// 已 commit：尽量落已收集 usage；中途 client 断开优先标记 client
				result.Tokens = rt.finalizeStreamTokens(tokens, usageBuf.Bytes(), upKind)
				if result.ClientDisconnected {
					rt.finalizeStreamClientDisconnect(&result)
					return result
				}
				msg := "upstream stream disconnected"
				if errors.Is(ev.err, bufio.ErrTooLong) {
					msg = fmt.Sprintf("upstream SSE line exceeded %d bytes", maxLineSize)
				}
				sendTerminalError("stream_read_error", msg)
				result.StreamErr = ev.err
				return result
			}

			line := ev.line
			if strings.TrimSpace(line) == "" {
				// 完整 SSE event 结束。commit 前的纯注释/ping 不计入首字、不停首字计时器，
				// 否则上游 :PING 会误取消 2s 首字超时，真实内容 7s 才到却无法顺延。
				if !result.Committed && conv == nil && !rt.sseEventHasPayload(pendingLines) {
					pendingLines = pendingLines[:0]
					continue
				}
				// 有效事件：才算「有首字」
				if !sawUpstreamData {
					sawUpstreamData = true
					stopFirstTimer()
					if result.FirstTokenMS == nil {
						ms := time.Since(start).Milliseconds()
						result.FirstTokenMS = &ms
					}
				}
				eventName, eventData := rt.parseSSEEventLines(pendingLines)
				terminal, success, terminalMessage := rt.upstreamSSETerminal(eventName, eventData, upKind)
				if err := processEvent(pendingLines); err != nil && !result.ClientDisconnected {
					// write error already flagged disconnect
				}
				pendingLines = pendingLines[:0]
				if failOnConvertError() {
					return result
				}
				if terminal {
					result.Body = append([]byte(nil), usageBuf.Bytes()...)
					result.Tokens = rt.finalizeStreamTokens(tokens, result.Body, upKind)
					if success {
						closeConverters()
					} else {
						if terminalMessage == "" {
							terminalMessage = "upstream response failed"
						}
						if !result.ClientDisconnected && !result.DownstreamComplete {
							sendTerminalError("upstream_error", terminalMessage)
						}
						result.StreamErr = errors.New(terminalMessage)
					}
					rt.finalizeStreamClientDisconnect(&result)
					return result
				}
				continue
			}
			pendingLines = append(pendingLines, line)
		}
	}
}

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
