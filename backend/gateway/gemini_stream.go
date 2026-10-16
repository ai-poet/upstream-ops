// Gemini 流式线格式：alt=sse 时为 SSE；否则为逐元素写出的 JSON 数组（REST 默认）。
package gateway

import (
	"bytes"
	"encoding/json"
	"strings"
)

// geminiArrayCloseFrame 数组模式下的收尾标记（由 geminiArrayEncoder 翻译成 "]"）。
var geminiArrayCloseFrame = []byte("\x00gemini-array-close")

// geminiArrayEncoder 把 "data: {...}\n\n" 形式的 SSE 帧改写成 JSON 数组元素：
// 首个元素前写 "["，之后的元素以 ",\r\n" 分隔，收尾写 "]"。
type geminiArrayEncoder struct {
	started bool
	closed  bool
}

func (e *geminiArrayEncoder) encode(frames [][]byte) [][]byte {
	out := make([][]byte, 0, len(frames))
	for _, f := range frames {
		if e.closed {
			break
		}
		if bytes.Equal(f, geminiArrayCloseFrame) {
			var b bytes.Buffer
			if !e.started {
				b.WriteByte('[')
				e.started = true
			}
			b.WriteString("]\r\n")
			e.closed = true
			out = append(out, b.Bytes())
			continue
		}
		for _, payload := range sseDataPayloads(f) {
			var b bytes.Buffer
			if !e.started {
				b.WriteByte('[')
				e.started = true
			} else {
				b.WriteString(",\r\n")
			}
			b.Write(payload)
			out = append(out, b.Bytes())
		}
	}
	return out
}

// sseDataPayloads 取出一帧 SSE 里每个事件的 data（忽略 event / 注释 / [DONE]）。
func sseDataPayloads(frame []byte) [][]byte {
	var out [][]byte
	for _, block := range bytes.Split(frame, []byte("\n\n")) {
		var data []string
		for _, line := range strings.Split(string(block), "\n") {
			line = strings.TrimRight(line, "\r")
			if strings.HasPrefix(line, "data:") {
				data = append(data, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
			}
		}
		joined := strings.TrimSpace(strings.Join(data, "\n"))
		if joined == "" || joined == "[DONE]" {
			continue
		}
		out = append(out, []byte(joined))
	}
	return out
}

// geminiChunkFinished Gemini 流式块是否已给出终态（finishReason 或 promptFeedback.blockReason）。
func geminiChunkFinished(data string) bool {
	if !strings.Contains(data, "finishReason") && !strings.Contains(data, "blockReason") &&
		!strings.Contains(data, "finish_reason") && !strings.Contains(data, "block_reason") {
		return false
	}
	var chunk struct {
		Candidates []struct {
			FinishReason      string `json:"finishReason"`
			FinishReasonSnake string `json:"finish_reason"`
		} `json:"candidates"`
		PromptFeedback *struct {
			BlockReason      string `json:"blockReason"`
			BlockReasonSnake string `json:"block_reason"`
		} `json:"promptFeedback"`
	}
	if json.Unmarshal([]byte(data), &chunk) != nil {
		return false
	}
	for _, c := range chunk.Candidates {
		if c.FinishReason != "" || c.FinishReasonSnake != "" {
			return true
		}
	}
	if pf := chunk.PromptFeedback; pf != nil && (pf.BlockReason != "" || pf.BlockReasonSnake != "") {
		return true
	}
	return false
}
