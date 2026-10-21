// 数据面：流式转发的读写（读上游 SSE → 转换 → 写客户端 → 收尾结算）。
//
// 以前是一个 500 行的函数，6 个闭包共享十几个局部变量；现在状态放在 streamRelay 上，
// 主循环（relayIncremental）只等事件并分派，每类事件一个方法。
package gateway

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/pricing"
	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/gin-gonic/gin"
)

// streamRelay 一次流式转发的状态。result 随过程填充，结束时原样返回。
type streamRelay struct {
	rt                *Runtime
	c                 *gin.Context
	plan              *conversionPlan
	abortReq          context.CancelFunc // 仅未 commit 或首字超时时中止上游；已 commit 的客户端断开不调用，以便 drain 计费
	start             time.Time          // 发起上游请求的时间（首字超时与 first_token_ms 的起点）
	firstTokenTimeout time.Duration
	result            streamAttemptResult
	// stopForwardDeadline 收到第一段有效上游数据时调用：结束转发总超时（之后只看数据间隔）
	stopForwardDeadline func()

	// 以下仅增量转发使用
	clientKind, upKind protocolKind
	maxLineSize        int
	keepalive          time.Duration
	idleTimeout        time.Duration
	conv               protocol.StreamConverter // nil 表示同协议透传
	geminiArray        *geminiArrayEncoder      // Gemini 客户端未带 alt=sse 时按 JSON 数组逐元素写出

	// 上游侧
	readerExit             chan struct{} // 读协程退出（读到 EOF / 出错）时关闭
	sawTerminal            bool          // 见过上游协议终态（之后上游很快会结束流，见 forwardStreamPlan）
	lastReadAt             atomic.Int64  // 最近一次读到上游数据（UnixNano，读协程写）
	pendingLines           []string      // 当前 SSE event 已读到的行
	sawUpstreamData        bool          // 收到过有效载荷（ping / 注释不算）
	geminiUpstreamFinished bool          // Gemini 上游没有 [DONE]：见过 finishReason 后 EOF 才算正常结束
	// chatUpstreamFinished Chat Completions 上游见过 finish_reason 或 usage 块：之后不带 [DONE] 直接 EOF 也算讲完了
	// （sub2api openai_raw_stream_truncation.go：只认 [DONE] 会误伤跑完最后一帧就关连接的兼容上游）
	chatUpstreamFinished bool
	usageBuf             boundedLineBuffer // 旁路保留上游原始 data 的头尾，结束时补算用量（见 stream_usage_buffer.go）
	tokens               pricing.UsageTokens

	// 客户端侧
	geminiFinishSent bool // 数组模式下已写出带 finishReason 的元素（数组本身可能还没闭合）
	// held 首个可见输出之前攒着的客户端帧（上游 Responses 的 response.created 等，见 stream_preoutput.go）
	held             [][]byte
	errorEventSent   bool
	lastDownstreamAt time.Time
	firstTimer       *time.Timer
	firstCh          <-chan time.Time
}

// sseLine 读协程交给主循环的一行（或读错误）。
type sseLine struct {
	line string
	err  error
}

// firstTokenTimedOut 首字超时：记下已等待时长，本次尝试失败（未提交时可重试 / 顺延）。
func (s *streamRelay) firstTokenTimedOut() streamAttemptResult {
	ms := time.Since(s.start).Milliseconds()
	s.result.FirstTokenMS = &ms
	s.result.Err = fmt.Errorf("%w after %s", errFirstTokenTimeout, s.firstTokenTimeout)
	return s.result
}

// markFirstToken 记首字时间。
func (s *streamRelay) markFirstToken() {
	ms := time.Since(s.start).Milliseconds()
	s.result.FirstTokenMS = &ms
}

// relayBuffered 仅兜底：三协议互转已走增量真流；此处保留给未知协议或未实现转换器。
// 行为：先读完再转换，再一次性写出（首写出前仍可 failover）——即「假流」，应尽量避免进入。
func (s *streamRelay) relayBuffered(body io.Reader) streamAttemptResult {
	c, plan, r := s.c, s.plan, &s.result
	// 从请求发起起算剩余首字预算（含已花费的等响应头时间）
	bodyWait, timedOut := remainingFirstTokenWait(s.start, s.firstTokenTimeout)
	if timedOut {
		return s.firstTokenTimedOut()
	}
	firstChunk, firstErr := readFirstChunk(body, bodyWait)
	if firstErr != nil {
		if isFirstTokenTimeout(firstErr) {
			return s.firstTokenTimedOut()
		}
		r.Err = firstErr
		return *r
	}
	var data []byte
	if len(firstChunk) > 0 {
		s.markFirstToken()
		data = append(data, firstChunk...)
	}
	rest, readErr := io.ReadAll(body)
	if readErr != nil {
		r.Body, r.Err = data, readErr
		return *r
	}
	data = append(data, rest...)

	r.Tokens = parseUsageByKind(data, true, plan.Upstream)
	clientBody := convertUpstreamResponseWithToolBridge(data, plan.Inbound, plan.Upstream, plan.Model, true, plan.Converted, plan.Bridge)
	if len(clientBody) == 0 {
		clientBody = data
	}
	if err := commitSSEHeaders(c, r.Headers); err != nil {
		r.Body, r.Err = clientBody, err
		return *r
	}
	r.Committed = true
	if _, err := c.Writer.Write(clientBody); err != nil {
		r.ClientDisconnected, r.StreamErr = true, errClientDisconnected
		return *r
	}
	flushWriter(c)
	if r.FirstTokenMS == nil {
		s.markFirstToken()
	}
	// 缓冲路径整包写出成功即视为下游完整交付（含转换后的终端帧）。
	r.DownstreamComplete = true
	return *r
}

// relayIncremental 同协议透传或跨协议增量转换：边读边写。
// upCtx：上游生命周期（与客户端取消解耦）；clientCtx：检测客户端断开。
func (s *streamRelay) relayIncremental(upCtx, clientCtx context.Context, body io.Reader) streamAttemptResult {
	rt := s.rt
	gwCfg := rt.gatewayRuntime()
	s.clientKind = protocol.NormalizeKind(s.plan.Inbound)
	s.upKind = protocol.NormalizeKind(s.plan.Upstream)
	s.maxLineSize = gwCfg.StreamMaxLineSize()
	s.conv = rt.newStreamConverter(upCtx, s.plan)
	if s.clientKind == protocol.KindGemini && s.plan.GeminiArray {
		s.geminiArray = &geminiArrayEncoder{}
	}

	events := make(chan sseLine, 32)
	done := make(chan struct{})
	s.readerExit = make(chan struct{})
	s.lastReadAt.Store(time.Now().UnixNano())
	go s.readUpstream(body, events, done)
	defer close(done)

	// 首字超时：从 start（发起上游请求）起算剩余时间，与 first_token_ms 同一时钟。
	// 注意：不是「响应头之后再等 N 秒」。
	if s.firstTokenTimeout > 0 {
		left, timedOut := remainingFirstTokenWait(s.start, s.firstTokenTimeout)
		if timedOut {
			return s.firstTokenTimedOut()
		}
		s.firstTimer = time.NewTimer(left)
		defer s.firstTimer.Stop()
		s.firstCh = s.firstTimer.C
	}

	s.keepalive = gwCfg.StreamKeepalive()
	s.idleTimeout = gwCfg.StreamIdleTimeout()
	var keepTicker, idleTicker *time.Ticker
	if s.keepalive > 0 {
		keepTicker = time.NewTicker(s.keepalive)
		defer keepTicker.Stop()
	}
	if s.idleTimeout > 0 {
		idleTicker = time.NewTicker(time.Second)
		defer idleTicker.Stop()
	}
	s.lastDownstreamAt = time.Now()
	clientDone := clientCtx.Done()

	for {
		var keepCh, idleCh <-chan time.Time
		// 已提交：按客户端协议发保活帧；未提交（还没有可见输出）：发 SSE 注释保活，不影响重试
		if keepTicker != nil && !s.result.ClientDisconnected && (s.result.Committed || s.geminiArray == nil) {
			keepCh = keepTicker.C
		}
		if idleTicker != nil {
			idleCh = idleTicker.C
		}

		select {
		case <-upCtx.Done():
			// 上游整体超时（非客户端取消）
			return s.onUpstreamCanceled(upCtx)

		case <-clientDone:
			if !s.result.Committed {
				return s.onClientGoneBeforeCommit(clientCtx)
			}
			// 已 commit：不 abort 上游，标记后继续主循环 drain，对齐 sub2api billing drain。
			// 若已写出终端帧，结束时会清掉 client 标签（正常收尾关连接）。
			s.result.ClientDisconnected = true
			clientDone = nil

		case <-s.firstCh:
			// 仅在尚未收到「有效」上游载荷时触发（ping/注释不算）
			if !s.sawUpstreamData && !s.result.Committed {
				return s.firstTokenTimedOut()
			}

		case <-keepCh:
			s.sendKeepalive()

		case <-idleCh:
			if s.upstreamIdle() {
				return s.onUpstreamIdle()
			}

		case ev, ok := <-events:
			switch {
			case !ok:
				return s.onUpstreamEOF()
			case ev.err != nil:
				return s.onReadError(ev.err)
			case s.onLine(ev.line):
				return s.result
			}
		}
	}
}

// readUpstream 读协程：按行读上游 SSE 交给主循环；主循环结束（done 关闭）后退出。
func (s *streamRelay) readUpstream(body io.Reader, events chan<- sseLine, done <-chan struct{}) {
	defer close(s.readerExit)
	defer close(events)
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), s.maxLineSize)
	for scanner.Scan() {
		s.lastReadAt.Store(time.Now().UnixNano())
		line := strings.TrimRight(scanner.Text(), "\r")
		select {
		case events <- sseLine{line: line}:
		case <-done:
			// 主循环已结束：剩下的读完丢掉，让上游按 END_STREAM 正常结束这条流（body 由调用方限时关闭）
			_, _ = io.Copy(io.Discard, body)
			return
		}
	}
	if err := scanner.Err(); err != nil {
		select {
		case events <- sseLine{err: err}:
		case <-done:
		}
	}
}

// ---------- 事件处理：返回 true 或 streamAttemptResult 表示本次转发结束 ----------

// onLine 一行上游 SSE：攒到空行算一个完整 event，处理后若是协议终态则结束。
func (s *streamRelay) onLine(line string) (finished bool) {
	if strings.TrimSpace(line) != "" {
		s.pendingLines = append(s.pendingLines, line)
		return false
	}
	// 完整 SSE event 结束。commit 前的纯注释/ping 不计入首字、不停首字计时器，
	// 否则上游 :PING 会误取消 2s 首字超时，真实内容 7s 才到却无法顺延。
	if !s.result.Committed && s.conv == nil && !sseEventHasPayload(s.pendingLines) {
		s.pendingLines = s.pendingLines[:0]
		return false
	}
	// 有效事件：才算「有首字」
	if !s.sawUpstreamData {
		s.sawUpstreamData = true
		s.stopFirstTimer()
		if s.stopForwardDeadline != nil {
			s.stopForwardDeadline()
		}
		if s.result.FirstTokenMS == nil {
			s.markFirstToken()
		}
	}
	eventName, eventData := parseSSEEventLines(s.pendingLines)
	// 首个可见输出之前上游在流里报错：可重试的记为本次尝试失败（走重试 / 换路由），客户端什么都没收到
	if !s.result.Committed {
		if status, body, ok := preOutputStreamFailure(eventName, eventData, s.upKind); ok {
			s.sawTerminal = true
			s.usageBuf.WriteLine(eventData)
			mergeStreamUsage(&s.tokens, eventData, s.upKind)
			s.pendingLines, s.held = s.pendingLines[:0], nil
			s.collectUsage()
			s.result.Status, s.result.Body = status, body
			return true
		}
	}
	terminal, success, terminalMessage := upstreamSSETerminal(eventName, eventData, s.upKind)
	if terminal {
		s.sawTerminal = true
	}
	// 写出失败已标记客户端断开，这里不用再处理
	_ = s.processEvent(s.pendingLines, eventName, eventData)
	s.pendingLines = s.pendingLines[:0]
	if s.failOnConvertError() {
		return true
	}
	if !terminal {
		return false
	}
	s.collectUsage()
	if success {
		s.closeConverters()
	} else {
		if terminalMessage == "" {
			terminalMessage = "upstream response failed"
		}
		if !s.result.ClientDisconnected && !s.result.DownstreamComplete {
			s.sendTerminalError("upstream_error", terminalMessage)
		}
		s.result.StreamErr = errors.New(terminalMessage)
	}
	finalizeStreamClientDisconnect(&s.result)
	return true
}

// onUpstreamEOF 上游关闭连接：没有协议终态时必须按失败处理，不能由转换器补一个成功帧。
func (s *streamRelay) onUpstreamEOF() streamAttemptResult {
	r := &s.result
	if len(s.pendingLines) > 0 {
		eventName, eventData := parseSSEEventLines(s.pendingLines)
		if err := s.processEvent(s.pendingLines, eventName, eventData); err != nil && !r.ClientDisconnected {
			r.StreamErr = err
		}
		s.pendingLines = s.pendingLines[:0]
	}
	if s.failOnConvertError() {
		return *r
	}
	s.collectUsage()
	if (s.geminiUpstreamFinished || s.chatUpstreamFinished) && r.Committed && !r.DownstreamComplete {
		// Gemini 上游以 EOF 结束：补齐客户端终态（转换器收尾 / 数组闭合）
		s.closeConverters()
		if s.conv == nil && s.geminiArray == nil {
			r.DownstreamComplete = true
		}
	}
	// 只有写出过才可能已完整交付，所以这里未提交的流一定走失败
	if !r.DownstreamComplete {
		msg := "upstream stream closed before terminal event"
		if !r.Committed {
			r.Err = errors.New(msg)
			return *r
		}
		if !r.ClientDisconnected {
			s.sendTerminalError("stream_incomplete", msg)
		}
		r.StreamErr = errors.New(msg)
		finalizeStreamClientDisconnect(r)
		return *r
	}
	// 已 commit：收尾转换（客户端已断则 writeFrames 会跳过写出）
	s.closeConverters()
	r.Tokens = finalizeStreamTokens(s.tokens, s.usageBuf.Bytes(), s.upKind)
	finalizeStreamClientDisconnect(r)
	return *r
}

// onReadError 读上游出错：未提交时本次尝试失败；已提交时落已收集的用量并给客户端补错误终态。
func (s *streamRelay) onReadError(err error) streamAttemptResult {
	r := &s.result
	tooLong := errors.Is(err, bufio.ErrTooLong)
	if !r.Committed {
		r.Err = err
		if tooLong {
			r.Err = fmt.Errorf("upstream SSE line exceeded %d bytes", s.maxLineSize)
		}
		return *r
	}
	// 已 commit：尽量落已收集 usage；中途 client 断开优先标记 client
	r.Tokens = finalizeStreamTokens(s.tokens, s.usageBuf.Bytes(), s.upKind)
	if r.ClientDisconnected {
		finalizeStreamClientDisconnect(r)
		return *r
	}
	msg := "upstream stream disconnected"
	if cause := streamDisconnectCause(err); cause != "" {
		msg += ": " + cause
	}
	if tooLong {
		msg = fmt.Sprintf("upstream SSE line exceeded %d bytes", s.maxLineSize)
	}
	s.sendTerminalError("stream_read_error", msg)
	r.StreamErr = err
	return *r
}

// onUpstreamCanceled 上游整体超时（ForwardTimeout）。
func (s *streamRelay) onUpstreamCanceled(upCtx context.Context) streamAttemptResult {
	r := &s.result
	if !r.Committed {
		r.Err = context.Cause(upCtx)
		s.abort()
		return *r
	}
	r.Tokens = finalizeStreamTokens(s.tokens, s.usageBuf.Bytes(), s.upKind)
	if r.ClientDisconnected {
		// 客户端已断，上游超时：用已收集 usage 落库；若已交付终端帧则不当 client 失败
		finalizeStreamClientDisconnect(r)
		return *r
	}
	msg := "upstream stream canceled"
	if err := context.Cause(upCtx); err != nil {
		msg = err.Error()
	}
	s.sendTerminalError("stream_timeout", msg)
	r.StreamErr = errors.New(msg)
	return *r
}

// onClientGoneBeforeCommit 尚未向客户端提交时客户端断开：中止上游，
// 由上层记为客户端取消（不 drain 计费，也无半截响应）。
func (s *streamRelay) onClientGoneBeforeCommit(clientCtx context.Context) streamAttemptResult {
	r := &s.result
	r.ClientDisconnected = true
	s.abort()
	r.Err = errClientDisconnected
	if err := clientCtx.Err(); err != nil {
		r.Err = err
	}
	return *r
}

// upstreamIdle 已收到过数据、但超过 idleTimeout 没再读到上游。
func (s *streamRelay) upstreamIdle() bool {
	if !s.sawUpstreamData {
		return false
	}
	lastRead := time.Unix(0, s.lastReadAt.Load())
	return time.Since(lastRead) >= s.idleTimeout
}

// onUpstreamIdle 上游空闲超时：已提交时给客户端补错误终态。
func (s *streamRelay) onUpstreamIdle() streamAttemptResult {
	r := &s.result
	msg := fmt.Sprintf("upstream stream idle for %s", s.idleTimeout)
	if !r.Committed {
		r.Err = errors.New(msg)
		return *r
	}
	s.sendTerminalError("stream_timeout", msg)
	r.StreamErr = errors.New(msg)
	r.Tokens = finalizeStreamTokens(s.tokens, s.usageBuf.Bytes(), s.upKind)
	return *r
}

// sendKeepalive 已提交且一段时间没写出时，给客户端发保活帧。
func (s *streamRelay) sendKeepalive() {
	if s.result.ClientDisconnected || time.Since(s.lastDownstreamAt) < s.keepalive {
		return
	}
	if !s.result.Committed {
		// 还没有可见输出：只写 SSE 注释（Gemini 数组模式不能写），不算提交，失败仍可重试
		if s.geminiArray == nil && writeStreamPrelude(s.c, s.result.Headers) == nil {
			s.lastDownstreamAt = time.Now()
		}
		return
	}
	ping := streamKeepaliveFrame(s.clientKind)
	if s.geminiArray != nil {
		// JSON 数组里只能插空白
		ping = []byte("\r\n")
	}
	if _, err := s.c.Writer.Write(ping); err != nil {
		s.result.ClientDisconnected = true
		return
	}
	flushWriter(s.c)
	s.lastDownstreamAt = time.Now()
}

// ---------- 转换与写出 ----------

// processEvent 一个完整的上游 event：旁路记用量，转换（或原样透传）后写给客户端。
func (s *streamRelay) processEvent(lines []string, eventName, data string) error {
	if len(lines) == 0 {
		return nil
	}
	// 旁路 usage：累积原始 data
	if data != "" && data != "[DONE]" {
		s.usageBuf.WriteLine(data)
		mergeStreamUsage(&s.tokens, data, s.upKind)
	}
	if s.upKind == protocol.KindGemini && geminiChunkFinished(data) {
		s.geminiUpstreamFinished = true
	}
	if s.upKind == protocol.KindOpenAIChat && chatChunkFinished(data) {
		s.chatUpstreamFinished = true
	}
	if s.conv != nil {
		return s.emit(s.conv.Feed(eventName, data), eventName, data)
	}
	// 网关替客户端要的 usage 帧：上面已记下用量，不转给客户端
	if s.plan.HideUsageChunks && usageOnlyChunk(data) {
		return nil
	}
	// 透传完整 event。commit 前丢弃纯注释/空事件，避免上游 :PING 过早
	// 提交响应头后无法再 retry/failover，并污染 first_token 统计。
	if !s.result.Committed && !sseEventHasPayload(lines) {
		return nil
	}
	var b strings.Builder
	for _, ln := range lines {
		b.WriteString(ln)
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	return s.emit([][]byte{[]byte(b.String())}, eventName, data)
}

// emit 写出一个上游事件对应的客户端帧。上游是 Responses 且还没有可见输出时先攒着（见 stream_preoutput.go），
// 这段时间里上游断开仍可重试 / 换路由。
func (s *streamRelay) emit(frames [][]byte, eventName, data string) error {
	if !s.result.Committed && s.upKind == protocol.KindOpenAIResponses && !responsesEventStartsOutput(eventName, data) {
		s.held = append(s.held, frames...)
		return nil
	}
	return s.writeFrames(frames)
}

// writeFrames 写给客户端；第一次写出时提交响应头（之后不能再重试 / 顺延）并记首字。
func (s *streamRelay) writeFrames(frames [][]byte) error {
	if len(frames) == 0 {
		return nil
	}
	r, c := &s.result, s.c
	if !r.Committed && len(s.held) > 0 {
		frames = append(s.held, frames...)
		s.held = nil
	}
	if !r.Committed {
		if err := commitSSEHeaders(c, r.Headers); err != nil {
			return err
		}
		if s.geminiArray != nil {
			// Gemini REST 非 SSE 流式：JSON 数组
			c.Header("Content-Type", "application/json; charset=utf-8")
		}
		r.Committed = true
		s.markFirstToken()
		s.stopFirstTimer()
	}
	if r.ClientDisconnected {
		return nil
	}
	if s.geminiArray != nil {
		frames = s.geminiArray.encode(frames)
	}
	for _, f := range frames {
		if len(f) == 0 {
			continue
		}
		if _, err := c.Writer.Write(f); err != nil {
			r.ClientDisconnected = true
			return err
		}
		if s.geminiArray != nil {
			// 数组模式：写出 "]" 才算完整交付；带 finishReason 的元素只记下已给出终态
			if s.geminiArray.closed {
				r.DownstreamComplete = true
			} else if sseFrameIsTerminal(f) {
				s.geminiFinishSent = true
			}
		} else if sseFrameIsTerminal(f) {
			r.DownstreamComplete = true
		}
	}
	flushWriter(c)
	s.lastDownstreamAt = time.Now()
	return nil
}

// sendTerminalError 已提交的流出错：按客户端协议补一个错误终态（只补一次）。
func (s *streamRelay) sendTerminalError(errType, msg string) {
	r := &s.result
	if s.errorEventSent || r.ClientDisconnected || !r.Committed {
		return
	}
	s.errorEventSent = true
	if s.geminiArray != nil {
		// 已给出 finishReason：客户端已拿到完整回答，只闭合数组（与 SSE 模式终态后不再补错误一致）
		frames := [][]byte{geminiErrorFrame(errType, msg), geminiArrayCloseFrame}
		if s.geminiFinishSent {
			frames = frames[1:]
		}
		if err := s.writeFrames(frames); err == nil {
			r.DownstreamComplete = true
		}
		return
	}
	if err := writeStreamTerminalError(s.c, s.clientKind, errType, msg); err == nil {
		r.DownstreamComplete = true
	}
}

// failOnConvertError 转换器遇到无法解析 / 转换的上游事件：之后的输出不可信，按失败结束
// （未提交时本次尝试失败，可重试 / 顺延）。
func (s *streamRelay) failOnConvertError() bool {
	ce, ok := s.conv.(interface{ Err() error })
	if !ok || ce.Err() == nil {
		return false
	}
	convErr := ce.Err()
	if s.rt.Log != nil {
		s.rt.Log.Warn("gateway stream convert failed", "inbound", s.plan.Inbound, "upstream", s.plan.Upstream, "err", convErr)
	}
	s.collectUsage()
	msg := "stream convert failed: " + convErr.Error()
	if !s.result.Committed {
		s.result.Err = errors.New(msg)
		s.abort()
		return true
	}
	if !s.result.ClientDisconnected {
		s.sendTerminalError("upstream_error", msg)
	}
	s.result.StreamErr = errors.New(msg)
	finalizeStreamClientDisconnect(&s.result)
	return true
}

// closeConverters 转换器收尾帧（Gemini 数组模式再闭合数组）。
func (s *streamRelay) closeConverters() {
	var frames [][]byte
	if s.conv != nil {
		frames = s.conv.Close()
	}
	_ = s.writeFrames(frames)
	if s.geminiArray != nil {
		_ = s.writeFrames([][]byte{geminiArrayCloseFrame})
	}
}

// collectUsage 把旁路收集的上游原始 data 存进结果，并据此补算用量。
func (s *streamRelay) collectUsage() {
	s.result.Body = append([]byte(nil), s.usageBuf.Bytes()...)
	s.result.Tokens = finalizeStreamTokens(s.tokens, s.result.Body, s.upKind)
}

func (s *streamRelay) stopFirstTimer() {
	if s.firstTimer == nil {
		return
	}
	if !s.firstTimer.Stop() {
		select {
		case <-s.firstTimer.C:
		default:
		}
	}
	s.firstCh = nil
}

func (s *streamRelay) abort() {
	if s.abortReq != nil {
		s.abortReq()
	}
}
