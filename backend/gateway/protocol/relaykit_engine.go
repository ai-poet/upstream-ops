// relaykit 协议转换引擎：把网关的原始 JSON / SSE 和 relaykit 的 DTO 互转。
//
// relaykit（github.com/QuantumNous/new-api/relaykit，AGPL-3.0）负责四种协议
// （OpenAI Chat / OpenAI Responses / Anthropic Messages / Gemini）之间的请求、
// 非流式响应与流式事件转换；HTTP 收发、SSE 读写、选路与计费仍由网关完成。
package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/relayconvert"
	"github.com/QuantumNous/new-api/relaykit/relayconvert/convmeta"
	"github.com/QuantumNous/new-api/relaykit/types"
)

// Engine 协议转换引擎名。
const (
	EngineRelayKit = "relaykit"
	EngineLegacy   = "legacy"
)

var legacyEngine atomic.Bool

// SetEngine 热切换协议转换引擎；未知值按 relaykit 处理。
// Gemini 只有 relaykit 能转，选 legacy 时涉及 Gemini 的转换仍走 relaykit。
func SetEngine(name string) {
	legacyEngine.Store(strings.EqualFold(strings.TrimSpace(name), EngineLegacy))
}

// UsingRelayKit 当前三协议互转是否走 relaykit。
func UsingRelayKit() bool { return !legacyEngine.Load() }

// UseRelayKitFor 该方向的转换是否走 relaykit。
func UseRelayKitFor(inbound, upstream Kind) bool {
	if NormalizeKind(inbound) == KindGemini || NormalizeKind(upstream) == KindGemini {
		return true
	}
	return UsingRelayKit()
}

// DefaultClaudeMaxTokens 源请求没有 max_tokens 时转 Claude 使用的值。
const DefaultClaudeMaxTokens = 8192

func relayFormat(k Kind) (types.RelayFormat, error) {
	switch NormalizeKind(k) {
	case KindOpenAIChat:
		return types.RelayFormatOpenAI, nil
	case KindOpenAIResponses:
		return types.RelayFormatOpenAIResponses, nil
	case KindAnthropic:
		return types.RelayFormatClaude, nil
	case KindGemini:
		return types.RelayFormatGemini, nil
	default:
		return "", fmt.Errorf("protocol %q is not convertible", k)
	}
}

// Conversion 一次上游尝试的转换会话。请求与响应转换共用同一个 meta：
// Responses 工具编码、推理设置、Claude 流式状态都记在里面，响应转换按它还原。
type Conversion struct {
	Inbound  Kind
	Upstream Kind
	// Steps 实际经过的转换器（如 oai_responses_to_oai_chat → oai_chat_to_claude_messages）
	Steps []string
	// Quality relaykit 给出的质量等级（good / fair / discouraged）
	Quality string
	// Diagnostics 请求阶段的损耗诊断（code@path）
	Diagnostics []string

	meta *convmeta.Values
	// explicitReasoning 源请求是否显式带了推理设置；没带时去掉 relaykit 按源模型补的默认值
	explicitReasoning bool
}

// NewConversion 创建转换会话。originModel 为客户端请求的模型，upstreamModel 为映射后的上游模型。
func NewConversion(inbound, upstream Kind, originModel, upstreamModel string, stream bool) *Conversion {
	return &Conversion{
		Inbound:  NormalizeKind(inbound),
		Upstream: NormalizeKind(upstream),
		meta: &convmeta.Values{
			OriginModelName:   originModel,
			UpstreamModelName: upstreamModel,
			// relaykit 只在声明了渠道信息时才把上游模型名写进转换结果（如 Gemini → Chat 的 model）
			ChannelMetaAttached: true,
			IsStream:            stream,
			Options: &convmeta.Options{
				Claude: convmeta.ClaudeOptions{
					DefaultMaxTokens: func(string) int { return DefaultClaudeMaxTokens },
				},
			},
		},
	}
}

// Request 把入站请求 body 转成上游协议 body。
func (cv *Conversion) Request(ctx context.Context, body []byte) ([]byte, error) {
	target, err := relayFormat(cv.Upstream)
	if err != nil {
		return nil, err
	}
	req, err := decodeRequest(cv.Inbound, body)
	if err != nil {
		return nil, err
	}
	cv.explicitReasoning = HasExplicitReasoning(cv.Inbound, body)
	cv.prepareRequest(req)
	// Gemini 请求没有 model 字段（模型在 URL 上）：relaykit 经 Chat 中转时中间那份 Chat 请求也没有 model，
	// 下一跳（如 Chat → Responses）会拒绝。这里自己拆成两跳，中间补上上游模型。
	if cv.Inbound == KindGemini && cv.Upstream != KindOpenAIChat {
		hop, err := relayconvert.ConvertRequest(ctx, cv.meta, types.RelayFormatOpenAI, req)
		if err != nil {
			return nil, err
		}
		chat, ok := hop.Value.(*dto.GeneralOpenAIRequest)
		if !ok {
			return nil, fmt.Errorf("unexpected intermediate request %T", hop.Value)
		}
		cv.prepareRequest(chat)
		if !cv.explicitReasoning {
			// 第一跳按源 Gemini 模型补了默认思考；带到第二跳会被渲染成 Claude thinking，
			// 连带抬高 max_tokens、丢掉 temperature / top_p / top_k（finishRequest 只删得掉 thinking 本身）
			chat.ReasoningEffort = ""
			chat.ReasoningConversion = nil
		}
		for _, step := range hop.Steps {
			cv.Steps = append(cv.Steps, step.Converter)
		}
		req = chat
	}
	result, err := relayconvert.ConvertRequest(ctx, cv.meta, target, req)
	if err != nil {
		return nil, err
	}
	cv.Quality = string(result.Quality)
	for _, step := range result.Steps {
		cv.Steps = append(cv.Steps, step.Converter)
	}
	for _, d := range result.Diagnostics {
		cv.Diagnostics = append(cv.Diagnostics, diagnosticLabel(d))
	}
	out, err := json.Marshal(result.Value)
	if err != nil {
		return nil, err
	}
	return cv.finishRequest(out), nil
}

// prepareRequest 把上游模型名与 stream 标志写进源 DTO（relaykit 按 DTO 里的 Model 生成上游请求）。
func (cv *Conversion) prepareRequest(req any) {
	model := cv.meta.UpstreamModelName
	stream := cv.meta.IsStream
	switch r := req.(type) {
	case *dto.GeneralOpenAIRequest:
		if model != "" {
			r.Model = model
		}
		r.Stream = &stream
	case *dto.OpenAIResponsesRequest:
		if model != "" {
			r.Model = model
		}
		r.Stream = &stream
	case *dto.ClaudeRequest:
		if model != "" {
			r.Model = model
		}
		r.Stream = &stream
	}
}

// finishRequest 按上游协议修正 relaykit 输出里与网关约定不一致的字段。
func (cv *Conversion) finishRequest(out []byte) []byte {
	if cv.Upstream == KindGemini {
		// Gemini 的模型和是否流式都在 URL 上，body 里不能出现 model / stream（未知字段会 400）
		return deleteJSONKeys(out, "model", "stream")
	}
	if !cv.explicitReasoning {
		// relaykit 会按源模型（如 gemini-2.5-pro 默认开思考）补推理档位；
		// 上游换成别的模型族时这个默认值没有意义，还可能被不支持推理的模型拒绝。
		switch cv.Upstream {
		case KindOpenAIChat:
			out = deleteJSONKeys(out, "reasoning_effort")
		case KindOpenAIResponses:
			out = deleteJSONKeys(out, "reasoning")
		case KindAnthropic:
			out = deleteJSONKeys(out, "thinking", "output_config")
		}
	}
	if !cv.meta.IsStream {
		out = deleteJSONKeys(out, "stream_options")
	}
	if model := cv.meta.UpstreamModelName; model != "" {
		out = setJSONString(out, "model", model)
	}
	return out
}

// HasExplicitReasoning 源请求是否显式带了推理 / 思考设置。
func HasExplicitReasoning(kind Kind, body []byte) bool {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(body, &obj); err != nil {
		return false
	}
	present := func(key string) bool {
		raw, ok := obj[key]
		return ok && len(raw) > 0 && string(raw) != "null"
	}
	switch NormalizeKind(kind) {
	case KindOpenAIChat:
		return present("reasoning_effort") || present("reasoning") || present("thinking")
	case KindOpenAIResponses:
		return present("reasoning")
	case KindAnthropic:
		return present("thinking") || present("output_config")
	case KindGemini:
		var cfg struct {
			GenerationConfig struct {
				ThinkingConfig json.RawMessage `json:"thinkingConfig"`
				ThinkingSnake  json.RawMessage `json:"thinking_config"`
			} `json:"generationConfig"`
		}
		if json.Unmarshal(body, &cfg) != nil {
			return false
		}
		set := func(raw json.RawMessage) bool { return len(raw) > 0 && string(raw) != "null" }
		return set(cfg.GenerationConfig.ThinkingConfig) || set(cfg.GenerationConfig.ThinkingSnake)
	}
	return false
}

// Response 把上游非流式响应转成客户端协议。
func (cv *Conversion) Response(ctx context.Context, body []byte) ([]byte, error) {
	target, err := relayFormat(cv.Inbound)
	if err != nil {
		return nil, err
	}
	resp, err := decodeResponse(cv.Upstream, body)
	if err != nil {
		return nil, err
	}
	result, err := relayconvert.ConvertResponse(ctx, cv.meta, target, resp)
	if err != nil {
		return nil, err
	}
	out, err := json.Marshal(result.Value)
	if err != nil {
		return nil, err
	}
	return stripUsageInternals(out), nil
}

// StreamConverter 增量 SSE 转换器：每个上游事件产出若干条已编码的客户端 SSE 帧。
type StreamConverter interface {
	Feed(eventName, data string) [][]byte
	Close() [][]byte
}

// NewStream 创建上游流 → 客户端流的增量转换器。
func (cv *Conversion) NewStream(ctx context.Context, id string) (StreamConverter, error) {
	from, err := relayFormat(cv.Upstream)
	if err != nil {
		return nil, err
	}
	to, err := relayFormat(cv.Inbound)
	if err != nil {
		return nil, err
	}
	// 与旧转换器一致：流式事件里回显上游模型名
	model := cv.meta.UpstreamModelName
	if model == "" {
		model = cv.meta.OriginModelName
	}
	state, err := relayconvert.NewResponseStreamState(from, to, relayconvert.ResponseStreamOptions{
		ID:                 id,
		Model:              model,
		IncludeUsage:       true,
		EmitSequenceNumber: true,
	})
	if err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return &relayStream{ctx: ctx, cv: cv, state: state, id: id, model: model, created: time.Now().Unix()}, nil
}

type relayStream struct {
	ctx      context.Context
	cv       *Conversion
	state    *relayconvert.ResponseStreamState
	closed   bool
	doneSent bool
	// Chat 块的 id / model / created：relaykit 的部分方向（Claude → Chat）只在首块填，这里统一补齐
	id      string
	model   string
	created int64
	// err 第一个解析 / 转换失败的上游事件；之后的输出不再可信，由网关以错误终态结束流
	err error
}

// Err 第一个无法解析或转换的上游事件（nil 表示目前一切正常）。
func (s *relayStream) Err() error { return s.err }

func (s *relayStream) Feed(eventName, data string) [][]byte {
	if s.closed || s.err != nil {
		return nil
	}
	data = strings.TrimSpace(data)
	if data == "" || data == "[DONE]" {
		return nil
	}
	chunk, ok, err := decodeStreamChunk(s.cv.Upstream, eventName, data)
	if err != nil {
		s.err = err
		return nil
	}
	if !ok {
		return nil
	}
	results, err := relayconvert.ConvertStreamResponseChunk(s.ctx, s.cv.meta, s.state, chunk)
	if err != nil {
		s.err = fmt.Errorf("convert %s stream event %q: %w", s.cv.Upstream, eventName, err)
		return nil
	}
	return s.encode(results)
}

func (s *relayStream) Close() [][]byte {
	if s.closed {
		return nil
	}
	s.closed = true
	results, _ := relayconvert.FinalizeStreamResponse(s.ctx, s.cv.meta, s.state)
	frames := s.encode(results)
	if s.cv.Inbound == KindOpenAIChat && !s.doneSent {
		s.doneSent = true
		frames = append(frames, []byte("data: [DONE]\n\n"))
	}
	return frames
}

func (s *relayStream) encode(results []relayconvert.ResponseResult) [][]byte {
	var frames [][]byte
	for _, r := range results {
		s.fillChatChunk(r.Value)
		if f := EncodeStreamValue(s.cv.Inbound, r.Value); len(f) > 0 {
			frames = append(frames, f)
		}
	}
	return frames
}

// fillChatChunk 给缺 id / model / created 的 Chat 流式块补上本次流的值（与旧转换器每块都带一致）。
func (s *relayStream) fillChatChunk(v any) {
	chunk, ok := v.(*dto.ChatCompletionsStreamResponse)
	if !ok || chunk == nil {
		return
	}
	if chunk.Id == "" {
		chunk.Id = s.id
	}
	if chunk.Model == "" {
		chunk.Model = s.model
	}
	if chunk.Created == 0 {
		chunk.Created = s.created
	}
}

// EncodeStreamValue 把 relaykit 的流式 DTO 编码成客户端协议的 SSE 帧。
func EncodeStreamValue(client Kind, value any) []byte {
	if value == nil {
		return nil
	}
	event := ""
	switch v := value.(type) {
	case relayconvert.ChatToResponsesStreamEvent:
		event, value = v.Type, v.Payload
	case *relayconvert.ChatToResponsesStreamEvent:
		if v == nil {
			return nil
		}
		event, value = v.Type, v.Payload
	case dto.ResponsesStreamResponse:
		event = v.Type
	case *dto.ResponsesStreamResponse:
		if v == nil {
			return nil
		}
		event = v.Type
	case dto.ClaudeResponse:
		event = v.Type
	case *dto.ClaudeResponse:
		if v == nil {
			return nil
		}
		event = v.Type
	}
	raw, err := json.Marshal(value)
	if err != nil || len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	raw = stripUsageInternals(raw)
	var b bytes.Buffer
	switch NormalizeKind(client) {
	case KindAnthropic, KindOpenAIResponses:
		if event != "" {
			b.WriteString("event: ")
			b.WriteString(event)
			b.WriteByte('\n')
		}
	}
	b.WriteString("data: ")
	b.Write(raw)
	b.WriteString("\n\n")
	return b.Bytes()
}

func decodeRequest(kind Kind, body []byte) (any, error) {
	var (
		req any
		err error
	)
	switch NormalizeKind(kind) {
	case KindOpenAIChat:
		var r dto.GeneralOpenAIRequest
		err = json.Unmarshal(body, &r)
		req = &r
	case KindOpenAIResponses:
		var r dto.OpenAIResponsesRequest
		err = json.Unmarshal(body, &r)
		req = &r
	case KindAnthropic:
		var r dto.ClaudeRequest
		err = json.Unmarshal(body, &r)
		req = &r
	case KindGemini:
		var r dto.GeminiChatRequest
		err = json.Unmarshal(body, &r)
		req = &r
	default:
		return nil, fmt.Errorf("protocol %q is not convertible", kind)
	}
	if err != nil {
		return nil, fmt.Errorf("invalid %s request: %w", NormalizeKind(kind), err)
	}
	return req, nil
}

func decodeResponse(kind Kind, body []byte) (any, error) {
	var (
		resp any
		err  error
	)
	switch NormalizeKind(kind) {
	case KindOpenAIChat:
		var r dto.OpenAITextResponse
		err = json.Unmarshal(body, &r)
		resp = &r
	case KindOpenAIResponses:
		var r dto.OpenAIResponsesResponse
		err = json.Unmarshal(body, &r)
		resp = &r
	case KindAnthropic:
		var r dto.ClaudeResponse
		err = json.Unmarshal(body, &r)
		resp = &r
	case KindGemini:
		var r dto.GeminiChatResponse
		err = json.Unmarshal(body, &r)
		resp = &r
	default:
		return nil, fmt.Errorf("protocol %q is not convertible", kind)
	}
	if err != nil {
		return nil, fmt.Errorf("invalid %s response: %w", NormalizeKind(kind), err)
	}
	return resp, nil
}

// decodeStreamChunk 把一条上游 SSE data 解析成 relaykit 的流式 DTO。
// 错误事件与心跳不送进转换器（ok=false）：终态错误由网关按客户端协议写出。
// 是 JSON 却解析不成 DTO 时返回 err：继续转换会悄悄丢内容。
func decodeStreamChunk(kind Kind, eventName, data string) (chunk any, ok bool, err error) {
	raw := []byte(data)
	if len(raw) == 0 || (raw[0] != '{' && raw[0] != '[') {
		return nil, false, nil
	}
	decodeErr := func(e error) error {
		return fmt.Errorf("decode %s stream event %q: %w", NormalizeKind(kind), eventName, e)
	}
	switch NormalizeKind(kind) {
	case KindOpenAIChat:
		var r dto.ChatCompletionsStreamResponse
		if e := json.Unmarshal(raw, &r); e != nil {
			return nil, false, decodeErr(e)
		}
		if r.Id == "" && len(r.Choices) == 0 && r.Usage == nil {
			return nil, false, nil
		}
		return &r, true, nil
	case KindOpenAIResponses:
		var r dto.ResponsesStreamResponse
		if e := json.Unmarshal(raw, &r); e != nil {
			return nil, false, decodeErr(e)
		}
		if r.Type == "" {
			r.Type = strings.TrimSpace(eventName)
		}
		if r.Type == "" || r.Type == "error" {
			return nil, false, nil
		}
		return &r, true, nil
	case KindAnthropic:
		var r dto.ClaudeResponse
		if e := json.Unmarshal(raw, &r); e != nil {
			return nil, false, decodeErr(e)
		}
		if r.Type == "" {
			r.Type = strings.TrimSpace(eventName)
		}
		switch r.Type {
		case "", "ping", "error":
			return nil, false, nil
		}
		return &r, true, nil
	case KindGemini:
		// Gemini 流中的错误块（{"error":{...}}）：不送进转换器，否则产出空块并提交 200
		if bytes.Contains(raw, []byte(`"error"`)) {
			var probe struct {
				Error json.RawMessage `json:"error"`
			}
			if json.Unmarshal(raw, &probe) == nil && len(probe.Error) > 0 && string(probe.Error) != "null" {
				return nil, false, nil
			}
		}
		var r dto.GeminiChatResponse
		if e := json.Unmarshal(raw, &r); e != nil {
			return nil, false, decodeErr(e)
		}
		return &r, true, nil
	}
	return nil, false, nil
}

func diagnosticLabel(d types.ConversionDiagnostic) string {
	label := strings.TrimSpace(d.Code)
	if p := strings.TrimSpace(d.Path); p != "" {
		label += "@" + p
	}
	return label
}

// usageInternalKeys relaykit 写在 usage 里的宿主侧旁路字段（计费旁路、用量来源、Claude 缓存细分），
// 不属于任何公开协议：usage_source 还会暴露上游是哪家。只在 usage 所在位置删，
// 不碰工具参数等用户数据里的同名键。
var usageInternalKeys = []string{
	"billing_usage", "usage_semantic", "usage_source",
	"claude_cache_creation_5_m_tokens", "claude_cache_creation_1_h_tokens",
}

// usagePaths 各协议 usage 所在位置：Chat / Responses / Claude 的顶层 usage、Gemini 的 usageMetadata、
// Claude message_start 的 message.usage、Responses 终态事件的 response.usage。
var usagePaths = [][]string{{"usage"}, {"usageMetadata"}, {"message", "usage"}, {"response", "usage"}}

func stripUsageInternals(raw []byte) []byte {
	hit := false
	for _, k := range usageInternalKeys {
		if bytes.Contains(raw, []byte(`"`+k+`"`)) {
			hit = true
			break
		}
	}
	if !hit {
		return raw
	}
	var root map[string]json.RawMessage
	if json.Unmarshal(raw, &root) != nil {
		return raw
	}
	changed := false
	for _, path := range usagePaths {
		if stripAtPath(root, path) {
			changed = true
		}
	}
	if !changed {
		return raw
	}
	out, err := json.Marshal(root)
	if err != nil {
		return raw
	}
	return out
}

// stripAtPath 在 obj 的 path 处（最后一段是 usage 对象）删掉内部字段；只解析沿途这几层。
func stripAtPath(obj map[string]json.RawMessage, path []string) bool {
	child, ok := obj[path[0]]
	if !ok || len(child) == 0 || child[0] != '{' {
		return false
	}
	var m map[string]json.RawMessage
	if json.Unmarshal(child, &m) != nil {
		return false
	}
	changed := false
	if len(path) == 1 {
		for _, k := range usageInternalKeys {
			if _, ok := m[k]; ok {
				delete(m, k)
				changed = true
			}
		}
	} else {
		changed = stripAtPath(m, path[1:])
	}
	if !changed {
		return false
	}
	out, err := json.Marshal(m)
	if err != nil {
		return false
	}
	obj[path[0]] = out
	return true
}

func setJSONString(body []byte, key, value string) []byte {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(body, &obj); err != nil {
		return body
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return body
	}
	obj[key] = raw
	out, err := json.Marshal(obj)
	if err != nil {
		return body
	}
	return out
}

func deleteJSONKeys(body []byte, keys ...string) []byte {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(body, &obj); err != nil {
		return body
	}
	changed := false
	for _, k := range keys {
		if _, ok := obj[k]; ok {
			delete(obj, k)
			changed = true
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

// ErrNotConvertible 该方向无转换器。
var ErrNotConvertible = errors.New("protocol conversion not supported")
