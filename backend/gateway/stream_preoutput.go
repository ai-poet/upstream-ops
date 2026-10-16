// 数据面：流式转发在「首个可见输出」之前的处理，照 sub2api 的做法：
//
//   - 上游是 OpenAI Responses 时，response.created / response.in_progress 这类生命周期事件先攒着不写给客户端，
//     等第一个真正的输出事件到了再一起写出（sub2api openai_gateway_passthrough.go 的 pendingLines、
//     openAIStreamDataStartsClientOutput）。在此之前上游断开，客户端一个字节的内容都没收到，
//     本次尝试按未提交处理，可以同路由重试或换路由——以前收到 response.created 就提交，
//     之后推理阶段上游一断，只能给客户端回 "upstream stream disconnected"。
//   - 攒着的时候给客户端写 SSE 注释保活（sub2api startOpenAISSEKeepalive）：会提交 200 响应头，
//     但不算输出，不影响重试；否则推理几分钟不出字，中间的代理会按空闲超时把连接掐掉。
//   - 断流时告诉客户端断在哪一类错误上（sub2api sanitizeStreamError），不带地址。
package gateway

import (
	"context"
	"errors"
	"io"
	"net"
	"regexp"
	"strings"
	"syscall"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// responsesEventStartsOutput 上游 Responses 事件是不是「客户端可见的输出」。
// 移植自 sub2api openAIStreamDataStartsClientOutput / openAIStreamAddedEventStartsClientOutput。
// 与 sub2api 的一处差别：response.failed 与 error 事件这里一律算输出（原样写出），
// sub2api 会把可重试的失败事件也攒着并换号——那要再移植一整套失败事件分类，本次不做。
func responsesEventStartsOutput(eventName, data string) bool {
	trimmed := strings.TrimSpace(data)
	if trimmed == "" {
		return false
	}
	eventType := strings.TrimSpace(eventName)
	if eventType == "" && gjson.Valid(trimmed) {
		eventType = strings.TrimSpace(gjson.Get(trimmed, "type").String())
	}
	switch eventType {
	case "response.created", "response.in_progress", "keepalive":
		return false
	case "response.output_item.added", "response.content_part.added", "response.reasoning_summary_part.added":
		return responsesAddedEventStartsOutput([]byte(trimmed), eventType)
	}
	return true
}

// responsesAddedEventStartsOutput 「新增」类事件只有带了内容才算输出（空的推理块、空文本块不算）。
func responsesAddedEventStartsOutput(payload []byte, eventType string) bool {
	if !gjson.ValidBytes(payload) {
		return true
	}
	switch eventType {
	case "response.output_item.added":
		item := gjson.GetBytes(payload, "item")
		if !item.IsObject() {
			return true
		}
		switch strings.TrimSpace(item.Get("type").String()) {
		case "reasoning":
			if item.Get("encrypted_content").String() != "" {
				return true
			}
			summary := item.Get("summary")
			if !summary.IsArray() {
				return false
			}
			for _, part := range summary.Array() {
				if strings.TrimSpace(part.Get("type").String()) != "summary_text" || part.Get("text").String() != "" {
					return true
				}
			}
			return false
		case "message":
			content := item.Get("content")
			if !content.IsArray() {
				return false
			}
			for _, part := range content.Array() {
				switch strings.TrimSpace(part.Get("type").String()) {
				case "output_text":
					if part.Get("text").String() != "" {
						return true
					}
				case "refusal":
					if part.Get("refusal").String() != "" {
						return true
					}
				default:
					return true
				}
			}
			return false
		case "function_call":
			return item.Get("arguments").String() != ""
		case "custom_tool_call":
			return item.Get("input").String() != ""
		case "compaction":
			return item.Get("encrypted_content").String() != ""
		default:
			return true
		}
	case "response.content_part.added":
		part := gjson.GetBytes(payload, "part")
		if !part.IsObject() {
			return true
		}
		switch strings.TrimSpace(part.Get("type").String()) {
		case "output_text":
			return part.Get("text").String() != ""
		case "refusal":
			return part.Get("refusal").String() != ""
		default:
			return true
		}
	case "response.reasoning_summary_part.added":
		part := gjson.GetBytes(payload, "part")
		if !part.IsObject() || strings.TrimSpace(part.Get("type").String()) != "summary_text" {
			return true
		}
		return part.Get("text").String() != ""
	}
	return true
}

// preludeContextKey 本请求已经写出过「提交前保活」（SSE 响应头 + 注释行）。
// 存在 gin context 上：同一请求的重试 / 换路由共用一个响应。
const preludeContextKey = "gateway_stream_prelude_sent"

// preOutputKeepaliveFrame 提交前的保活：SSE 注释，各协议的 SSE 解析器都会忽略。
var preOutputKeepaliveFrame = []byte(": keepalive\n\n")

func streamPreludeSent(c *gin.Context) bool {
	if c == nil {
		return false
	}
	v, ok := c.Get(preludeContextKey)
	return ok && v == true
}

// writeStreamPrelude 首个可见输出之前给客户端写一条保活注释；第一次写时提交 SSE 响应头。
// 写出的字节不算输出：之后上游失败仍可重试 / 换路由，最终失败时 writeError 在流里补错误终态。
func writeStreamPrelude(c *gin.Context, upstream map[string][]string) error {
	if !streamPreludeSent(c) {
		if err := commitSSEHeaders(c, upstream); err != nil {
			return err
		}
		c.Set(preludeContextKey, true)
	}
	if _, err := c.Writer.Write(preOutputKeepaliveFrame); err != nil {
		return err
	}
	flushWriter(c)
	return nil
}

var (
	http2StreamErrorCode = regexp.MustCompile(`stream error: stream id \d+; ([a-z_]+)`)
	http2GoAwayCode      = regexp.MustCompile(`errcode=([a-z_]+)`)
)

// streamDisconnectCause 上游读流出错的原因分类（给客户端看，不带地址 / 端口）。
// 移植自 sub2api sanitizeStreamError；另加 HTTP/2 的 GOAWAY 与流错误码：net/http 自带的 HTTP/2 实现
// 用的是未导出的错误类型，只能按文本匹配（sub2api openai_codex_models_service.go 同样按文本判断）。
func streamDisconnectCause(err error) string {
	if err == nil {
		return ""
	}
	switch {
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "unexpected EOF"
	case errors.Is(err, io.EOF):
		return "EOF"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline exceeded"
	case errors.Is(err, syscall.ECONNRESET):
		return "connection reset by peer"
	case errors.Is(err, syscall.ECONNABORTED):
		return "connection aborted"
	case errors.Is(err, syscall.ETIMEDOUT):
		return "connection timed out"
	case errors.Is(err, syscall.EPIPE):
		return "broken pipe"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "connection refused"
	}
	msg := strings.ToLower(err.Error())
	if m := http2StreamErrorCode.FindStringSubmatch(msg); m != nil {
		return "http2 stream error " + strings.ToUpper(m[1])
	}
	if strings.Contains(msg, "goaway") {
		if m := http2GoAwayCode.FindStringSubmatch(msg); m != nil {
			return "http2 GOAWAY " + strings.ToUpper(m[1])
		}
		return "http2 GOAWAY"
	}
	if strings.Contains(msg, "http2: client connection lost") {
		return "http2 connection lost"
	}
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			if netErr.Op != "" {
				return netErr.Op + " timeout"
			}
			return "i/o timeout"
		}
		if netErr.Op != "" {
			return netErr.Op + " network error"
		}
	}
	return "upstream connection error"
}
