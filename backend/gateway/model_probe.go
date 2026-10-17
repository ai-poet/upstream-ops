// 管理面：渠道（路由）× 模型连通性测试。
//
// 照 new-api controller/channel-test.go 的做法：按端点类型造一个最小请求（"hi"），
// 经这条路由的真实上游（同一套目标解析、UA、模型映射、协议转换）发出去，计时并校验响应：
//   - 端点可选（new-api 的 endpoint_type）；不选时按模型猜：生图模型走 Images，Claude 走 Messages，
//     GPT / o 系 / codex 走 Responses（线上 GPT 流量几乎全是 Responses），其余走 Chat。
//   - 流式可选（new-api 的 stream）；默认开，额外量出首字延迟。生图不支持流式。
//   - 200 也要看内容：响应体里带 error 算失败（new-api detectErrorFromTestResponseBody）；
//     流式至少要有一个数据事件（validateStreamTestResponseBody），这里再严一点：
//     Responses / Anthropic 要等到协议终态，Chat 要 [DONE] 或带 finish_reason 的块，中途断开算失败。
//
// 与 new-api 的差别：不记用量日志、不改路由的健康状态（测试失败不会暂停路由）。
package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/tidwall/gjson"
)

// 测试端点（入站请求的形态）；空表示按模型自动选。
const (
	probeEndpointChat      = "chat"
	probeEndpointResponses = "responses"
	probeEndpointAnthropic = "anthropic"
	probeEndpointGemini    = "gemini"
	probeEndpointImages    = "images"
)

const (
	// 文本测试的整体超时：线上 Claude（Kiro 类分组）首字常在 30s 以上，30s 会把慢但可用的渠道判成失败
	probeTextTimeout = 60 * time.Second
	// 生图一次常要 20~60s
	probeImageTimeout = 180 * time.Second
	// 回复摘要的长度（字符）
	probeReplyMaxRunes = 120
	// 流式测试最多读这么多字节（"hi" 的回复远小于它，防上游异常时无限读）
	probeStreamMaxBytes = 4 << 20
)

// probeOptions 一次测试的端点与流式选择。
type probeOptions struct {
	Endpoint string // probeEndpoint*；空 = 按模型自动选
	Stream   *bool  // nil = 默认（生图关，其余开）
}

// normalizeProbeEndpoint 规范化前端传来的端点名；不认识的报错。
func normalizeProbeEndpoint(s string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "auto":
		return "", nil
	case "chat", "openai", "openai_chat", "chat_completions":
		return probeEndpointChat, nil
	case "responses", "openai_responses", "openai-response":
		return probeEndpointResponses, nil
	case "anthropic", "messages", "claude":
		return probeEndpointAnthropic, nil
	case "gemini":
		return probeEndpointGemini, nil
	case "images", "image", "image-generation", "images_generations":
		return probeEndpointImages, nil
	}
	return "", fmt.Errorf("unknown test endpoint %q", s)
}

// looksLikeImageGenerationModel 走 /v1/images/generations 的生图模型（gpt-image-*、dall-e-*）。
func looksLikeImageGenerationModel(model string) bool {
	m := normalizeModelName(model)
	if i := strings.LastIndex(m, "/"); i >= 0 {
		m = m[i+1:]
	}
	return strings.HasPrefix(m, "gpt-image") || strings.HasPrefix(m, "chatgpt-image") || strings.HasPrefix(m, "dall-e")
}

// looksLikeOpenAIResponsesModel OpenAI 自家的 GPT / o 系 / codex 模型（客户端基本都用 Responses 调）。
func looksLikeOpenAIResponsesModel(model string) bool {
	m := normalizeModelName(model)
	if i := strings.LastIndex(m, "/"); i >= 0 {
		m = m[i+1:]
	}
	if strings.Contains(m, "codex") {
		return true
	}
	if strings.HasPrefix(m, "gpt-") && !strings.HasPrefix(m, "gpt-oss") {
		return true
	}
	return strings.HasPrefix(m, "o1") || strings.HasPrefix(m, "o3") || strings.HasPrefix(m, "o4")
}

// isOpenAIReasoningChatModel Chat 接口里不接 max_tokens 的推理模型（o 系、GPT-5 及以后）。
func isOpenAIReasoningChatModel(model string) bool {
	m := normalizeModelName(model)
	if strings.HasPrefix(m, "o1") || strings.HasPrefix(m, "o3") || strings.HasPrefix(m, "o4") {
		return true
	}
	rest, ok := strings.CutPrefix(m, "gpt-")
	if !ok || rest == "" {
		return false
	}
	// gpt-5 / gpt-5.6-sol / gpt-6-astra …：主版本号 ≥ 5
	major := 0
	for _, r := range rest {
		if r < '0' || r > '9' {
			break
		}
		major = major*10 + int(r-'0')
	}
	return major >= 5
}

// defaultProbeEndpoint 没选端点时按模型猜客户端会怎么调它。
func defaultProbeEndpoint(model string) string {
	switch {
	case looksLikeImageGenerationModel(model):
		return probeEndpointImages
	case protocol.LooksLikeClaudeModel(model):
		return probeEndpointAnthropic
	case looksLikeOpenAIResponsesModel(model):
		return probeEndpointResponses
	}
	return probeEndpointChat
}

// probeInbound 端点对应的入站协议与入站路径。
func probeInbound(endpoint, model string, stream bool) (protocolKind, string) {
	switch endpoint {
	case probeEndpointResponses:
		return protocol.KindOpenAIResponses, "/v1/responses"
	case probeEndpointAnthropic:
		return protocolAnthropic, "/v1/messages"
	case probeEndpointGemini:
		m := strings.TrimPrefix(strings.TrimSpace(model), "models/")
		if stream {
			return protocol.KindGemini, "/v1beta/models/" + m + ":streamGenerateContent"
		}
		return protocol.KindGemini, "/v1beta/models/" + m + ":generateContent"
	case probeEndpointImages:
		return protocol.KindOpenAIChat, "/v1/images/generations"
	}
	return protocol.KindOpenAIChat, "/v1/chat/completions"
}

// buildProbeBody 测试请求体（形态同 new-api buildTestRequest）。
func buildProbeBody(endpoint, model string, stream bool) []byte {
	var body map[string]any
	switch endpoint {
	case probeEndpointResponses:
		body = map[string]any{
			"model":  model,
			"input":  []map[string]any{{"role": "user", "content": "hi"}},
			"stream": stream,
		}
	case probeEndpointAnthropic:
		// max_tokens 用 sub2api 账号测试的 1024：上游（如 sub2api 的 OAuth 账号）可能自己加 thinking，
		// 太小会被拒「max_tokens 须大于 budget_tokens」；它只是上限，"hi" 的回复用不了这么多
		body = map[string]any{
			"model":      model,
			"max_tokens": 1024,
			"messages":   []map[string]any{{"role": "user", "content": "hi"}},
			"stream":     stream,
		}
	case probeEndpointGemini:
		body = map[string]any{
			"contents":         []map[string]any{{"role": "user", "parts": []map[string]any{{"text": "hi"}}}},
			"generationConfig": map[string]any{"maxOutputTokens": 3000},
		}
	case probeEndpointImages:
		body = map[string]any{
			"model":  model,
			"prompt": "a cute cat",
			"n":      1,
			"size":   "1024x1024",
		}
		// gpt-image 系按质量计价：测连通性用最低档
		if strings.Contains(normalizeModelName(model), "gpt-image") {
			body["quality"] = "low"
		}
	default:
		body = map[string]any{
			"model":    model,
			"messages": []map[string]any{{"role": "user", "content": "hi"}},
			"stream":   stream,
		}
		lower := normalizeModelName(model)
		switch {
		case isOpenAIReasoningChatModel(model):
			// 推理模型不接 max_tokens；上限又会被推理吃掉，干脆不设（new-api 设 max_completion_tokens=16，
			// 推理模型常因此回空）
		case protocol.LooksLikeGeminiModel(model):
			body["max_tokens"] = 3000
		case strings.Contains(lower, "thinking") && !strings.Contains(lower, "claude"):
			body["max_tokens"] = 50
		default:
			body["max_tokens"] = 16
		}
	}
	b, _ := json.Marshal(body)
	return b
}

// probeResponse 测试响应的结论。
type probeResponse struct {
	ok            bool
	err           string
	firstTokenMS  *int64
	reply         string
	responseModel string
}

// probeSend 发出测试请求并校验响应。流式走 probeStream；非流式整包读回后看内容。
func (rt *Runtime) probeSend(ctx context.Context, call upstreamCall, endpoint string, start time.Time) (status int, out probeResponse) {
	if call.stream {
		return rt.probeStream(ctx, call, start)
	}
	status, _, body, _, err := rt.forwardOnce(ctx, call)
	if err != nil {
		out.err = probeTransportError(ctx, err)
		return status, out
	}
	if status < 200 || status >= 300 {
		out.err = probeStatusError(status, body)
		return status, out
	}
	out = probeJSONResult(body, call.kind, endpoint)
	return status, out
}

// probeStatusError 非 2xx：尽量取上游的错误消息。
func probeStatusError(status int, body []byte) string {
	msg := truncateProbeError(body, 240)
	if msg == "" {
		msg = fmt.Sprintf("upstream status %d", status)
	}
	return msg
}

// probeTransportError 连接 / 读取错误；超时说清楚是测试超时。
func probeTransportError(ctx context.Context, err error) string {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return "test timed out"
	}
	if cause := streamDisconnectCause(err); cause != "" {
		return cause
	}
	return err.Error()
}

// probeErrorMessage 响应体（或 SSE 数据）里带 error 时的消息；没有返回空（同 new-api detectErrorMessageFromJSONBytes）。
func probeErrorMessage(payload []byte) string {
	b := bytes.TrimSpace(payload)
	if len(b) == 0 || (b[0] != '{' && b[0] != '[') || !gjson.ValidBytes(b) {
		return ""
	}
	errVal := gjson.GetBytes(b, "error")
	if !errVal.Exists() || errVal.Type == gjson.Null {
		return ""
	}
	msg := gjson.GetBytes(b, "error.message").String()
	if msg == "" {
		msg = gjson.GetBytes(b, "error.error.message").String()
	}
	if msg == "" && errVal.Type == gjson.String {
		msg = errVal.String()
	}
	if msg == "" {
		msg = errVal.Raw
	}
	msg = strings.TrimSpace(msg)
	if msg == "" {
		return "upstream returned error payload"
	}
	return clipRunes(msg, 240)
}

// probeJSONResult 非流式 2xx 的响应体：带 error 算失败；取回复摘要与上游报的模型名。
func probeJSONResult(body []byte, kind protocolKind, endpoint string) probeResponse {
	var out probeResponse
	if msg := probeErrorMessage(body); msg != "" {
		out.err = msg
		return out
	}
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		out.err = "empty response body"
		return out
	}
	if !gjson.ValidBytes(trimmed) {
		out.err = "response is not JSON: " + clipRunes(string(trimmed), 120)
		return out
	}
	if endpoint == probeEndpointImages {
		first := gjson.GetBytes(trimmed, "data.0")
		if !first.Exists() || (first.Get("b64_json").String() == "" && first.Get("url").String() == "") {
			out.err = "no image in response"
			return out
		}
		out.ok = true
		if first.Get("b64_json").String() != "" {
			out.reply = "已返回图片（b64_json）"
		} else {
			out.reply = "已返回图片（url）"
		}
		out.responseModel = gjson.GetBytes(trimmed, "model").String()
		return out
	}
	out.ok = true
	switch protocol.NormalizeKind(kind) {
	case protocol.KindOpenAIResponses:
		var sb strings.Builder
		for _, item := range gjson.GetBytes(trimmed, "output").Array() {
			for _, part := range item.Get("content").Array() {
				if t := part.Get("text").String(); t != "" {
					sb.WriteString(t)
				}
			}
		}
		out.reply = sb.String()
		out.responseModel = gjson.GetBytes(trimmed, "model").String()
	case protocol.KindAnthropic:
		var sb strings.Builder
		for _, part := range gjson.GetBytes(trimmed, "content").Array() {
			if part.Get("type").String() == "text" {
				sb.WriteString(part.Get("text").String())
			}
		}
		out.reply = sb.String()
		out.responseModel = gjson.GetBytes(trimmed, "model").String()
	case protocol.KindGemini:
		out.reply = geminiCandidateText(gjson.ParseBytes(trimmed))
		out.responseModel = gjson.GetBytes(trimmed, "modelVersion").String()
	default:
		out.reply = gjson.GetBytes(trimmed, "choices.0.message.content").String()
		out.responseModel = gjson.GetBytes(trimmed, "model").String()
	}
	out.reply = clipRunes(strings.TrimSpace(out.reply), probeReplyMaxRunes)
	return out
}

func geminiCandidateText(v gjson.Result) string {
	var sb strings.Builder
	for _, part := range v.Get("candidates.0.content.parts").Array() {
		if part.Get("thought").Bool() {
			continue
		}
		sb.WriteString(part.Get("text").String())
	}
	return sb.String()
}

// probeStreamState 流式测试边读边记：首字时间、回复、终态。
type probeStreamState struct {
	kind          protocolKind
	start         time.Time
	firstTokenMS  *int64
	reply         strings.Builder
	replyRunes    int
	responseModel string
	sawData       bool
	chatFinished  bool
	terminal      bool
	failure       string
}

func (s *probeStreamState) addReply(text string) {
	if text == "" || s.replyRunes >= probeReplyMaxRunes {
		return
	}
	s.reply.WriteString(text)
	s.replyRunes += utf8.RuneCountInString(text)
}

// onEvent 处理一个 SSE 事件；返回 true 表示已到终态（成功或失败），不用再解析后面的数据。
func (s *probeStreamState) onEvent(eventName, data string) bool {
	data = strings.TrimSpace(data)
	if data == "" {
		return false
	}
	s.sawData = true
	if data == "[DONE]" {
		s.terminal = true
		return true
	}
	if msg := probeErrorMessage([]byte(data)); msg != "" && !strings.HasPrefix(gjson.Get(data, "type").String(), "response.") {
		// Chat / Gemini 流中的 {"error":...} 块；Responses 的 response.failed 交给下面按协议判
		s.failure = msg
		return true
	}
	if terminal, success, message := upstreamSSETerminal(eventName, data, s.kind); terminal {
		s.terminal = true
		if !success {
			if message == "" {
				message = "upstream stream failed"
			}
			s.failure = clipRunes(message, 240)
		}
		s.captureModel(eventName, data)
		return true
	}
	if s.firstTokenMS == nil && probeEventStartsOutput(s.kind, eventName, data) {
		ms := time.Since(s.start).Milliseconds()
		s.firstTokenMS = &ms
	}
	s.captureModel(eventName, data)
	s.addReply(probeEventText(s.kind, eventName, data))
	if protocol.NormalizeKind(s.kind) == protocol.KindOpenAIChat || protocol.NormalizeKind(s.kind) == protocol.KindOpenAI {
		if chatChunkFinished(data) {
			s.chatFinished = true
		}
	}
	return false
}

func (s *probeStreamState) captureModel(eventName, data string) {
	if s.responseModel != "" || !gjson.Valid(data) {
		return
	}
	for _, path := range []string{"model", "response.model", "message.model", "modelVersion"} {
		if m := strings.TrimSpace(gjson.Get(data, path).String()); m != "" {
			s.responseModel = m
			return
		}
	}
}

// probeEventStartsOutput 这个事件是不是模型的第一个输出（算首字）。
func probeEventStartsOutput(kind protocolKind, eventName, data string) bool {
	switch protocol.NormalizeKind(kind) {
	case protocol.KindOpenAIResponses:
		return responsesEventStartsOutput(eventName, data)
	case protocol.KindAnthropic:
		typ := strings.TrimSpace(gjson.Get(data, "type").String())
		if typ == "" {
			typ = strings.TrimSpace(eventName)
		}
		return typ == "content_block_delta"
	case protocol.KindGemini:
		return gjson.Get(data, "candidates.0.content.parts").Exists()
	default:
		for _, choice := range gjson.Get(data, "choices").Array() {
			delta := choice.Get("delta")
			if delta.Get("content").String() != "" || delta.Get("reasoning_content").String() != "" ||
				delta.Get("reasoning").String() != "" || delta.Get("tool_calls").Exists() {
				return true
			}
		}
		return false
	}
}

// probeEventText 事件里的可见文本增量。
func probeEventText(kind protocolKind, eventName, data string) string {
	switch protocol.NormalizeKind(kind) {
	case protocol.KindOpenAIResponses:
		typ := strings.TrimSpace(gjson.Get(data, "type").String())
		if typ == "" {
			typ = strings.TrimSpace(eventName)
		}
		if typ == "response.output_text.delta" {
			return gjson.Get(data, "delta").String()
		}
	case protocol.KindAnthropic:
		if gjson.Get(data, "delta.type").String() == "text_delta" {
			return gjson.Get(data, "delta.text").String()
		}
	case protocol.KindGemini:
		return geminiCandidateText(gjson.Parse(data))
	default:
		return gjson.Get(data, "choices.0.delta.content").String()
	}
	return ""
}

// finish 读到 EOF（或终态）后给结论。
func (s *probeStreamState) finish() probeResponse {
	out := probeResponse{
		firstTokenMS:  s.firstTokenMS,
		reply:         clipRunes(strings.TrimSpace(s.reply.String()), probeReplyMaxRunes),
		responseModel: s.responseModel,
	}
	switch {
	case s.failure != "":
		out.err = s.failure
	case !s.sawData:
		out.err = "stream response has no data event"
	case s.terminal || s.chatFinished:
		out.ok = true
	case protocol.NormalizeKind(s.kind) == protocol.KindGemini:
		// Gemini 流以 EOF 正常结束
		out.ok = true
	default:
		out.err = "stream ended before completion"
	}
	return out
}

// probeStream 流式测试：读 SSE 直到协议终态，量首字；终态后把剩下的读到 EOF 再关
// （同 forwardStreamPlan：不在 HTTP/2 流还开着时关 body，免得上游按 RST 计数回 GOAWAY）。
func (rt *Runtime) probeStream(ctx context.Context, call upstreamCall, start time.Time) (int, probeResponse) {
	var out probeResponse
	call.stream = true
	req, err := rt.buildUpstreamHTTPRequest(ctx, call)
	if err != nil {
		out.err = err.Error()
		return 0, out
	}
	client := rt.httpClientForTarget(call.target.Channel, call.target.Provider)
	resp, err := client.Do(req)
	if err != nil {
		out.err = probeTransportError(ctx, err)
		return 0, out
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, maxUpstreamErrorBodyBytes))
		out.err = probeStatusError(resp.StatusCode, data)
		return resp.StatusCode, out
	}
	// 上游没按流式回（JSON 整包）：按非流式校验
	if ct := strings.ToLower(resp.Header.Get("Content-Type")); ct != "" && !strings.Contains(ct, "event-stream") {
		data, err := io.ReadAll(io.LimitReader(resp.Body, probeStreamMaxBytes))
		if err != nil {
			out.err = probeTransportError(ctx, err)
			return resp.StatusCode, out
		}
		out = probeJSONResult(data, call.kind, "")
		return resp.StatusCode, out
	}

	st := &probeStreamState{kind: call.kind, start: start}
	reader := bufio.NewReaderSize(io.LimitReader(resp.Body, probeStreamMaxBytes), 64<<10)
	var lines []string
	done := false
	for !done {
		line, readErr := reader.ReadString('\n')
		if line != "" {
			trimmed := strings.TrimRight(line, "\r\n")
			if strings.TrimSpace(trimmed) == "" {
				if len(lines) > 0 {
					name, data := parseSSEEventLines(lines)
					lines = lines[:0]
					done = st.onEvent(name, data)
				}
			} else {
				lines = append(lines, trimmed)
			}
		}
		if readErr != nil {
			if !done && len(lines) > 0 {
				name, data := parseSSEEventLines(lines)
				done = st.onEvent(name, data)
			}
			if !done && !errors.Is(readErr, io.EOF) {
				st.failure = "stream disconnected: " + probeTransportError(ctx, readErr)
			}
			break
		}
	}
	if done {
		drainProbeBody(resp.Body)
	}
	return resp.StatusCode, st.finish()
}

// drainProbeBody 终态后最多等 upstreamDrainGrace 让上游把流正常结束。
func drainProbeBody(body io.ReadCloser) {
	finished := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(body, probeStreamMaxBytes))
		close(finished)
	}()
	grace := upstreamDrainGrace
	select {
	case <-finished:
	case <-time.After(grace):
	}
}

// clipRunes 按字符截断（clipString 按字节，会切坏中文）。
func clipRunes(s string, max int) string {
	if max <= 0 || utf8.RuneCountInString(s) <= max {
		return s
	}
	r := []rune(s)
	return string(r[:max]) + "…"
}
