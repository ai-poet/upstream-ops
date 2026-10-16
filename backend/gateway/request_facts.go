// 数据面：请求体字段提取（只扫一遍，不反序列化整个 body）。
//
// 以前 model / stream / service_tier / 推理档位 / thinking / 计费参数各自 json.Unmarshal 一次，
// 计费参数还整包解析成 map[string]any；长上下文（几百 KB）的请求每个要解析 7 次以上，
// 光这一项就占处理请求 CPU 的约 28%。现在用 gjson 顺序扫一遍顶层字段，嵌套值直接跳过。
package gateway

import (
	"bytes"
	"strings"

	"github.com/bejix/upstream-ops/backend/gateway/pricing"
	"github.com/tidwall/gjson"
)

// requestFacts 转发路径要的请求字段。
type requestFacts struct {
	model           string
	stream          bool
	serviceTier     string
	reasoningEffort string
	thinkingEnabled bool
	billing         pricing.BillingInput
}

// looksLikeJSONObject body 第一个非空白字符是 '{'（multipart 等非 JSON 请求直接跳过）。
func looksLikeJSONObject(body []byte) bool {
	trim := bytes.TrimLeft(body, " \t\r\n")
	return len(trim) > 0 && trim[0] == '{'
}

// scanRequestFacts 扫一遍顶层字段，算出 model、stream、服务等级、推理档位、thinking 与计费参数。
// path 用于判断图片 / 视频 / Responses 端点的计费模式；不需要计费参数时传空串。
func scanRequestFacts(path string, body []byte) requestFacts {
	f := requestFacts{billing: baseBillingInput(path)}
	if !looksLikeJSONObject(body) {
		return f
	}
	var reasoning, outputConfig, thinking, tools, input gjson.Result
	var reasoningEffort, size, resolution string
	gjson.ParseBytes(body).ForEach(func(key, value gjson.Result) bool {
		switch key.Str {
		case "model":
			if value.Type == gjson.String {
				f.model = strings.TrimSpace(value.Str)
			}
		case "stream":
			f.stream = value.Type == gjson.True
		case "service_tier":
			if value.Type == gjson.String {
				f.serviceTier = strings.TrimSpace(value.Str)
			}
		case "reasoning_effort":
			if value.Type == gjson.String {
				reasoningEffort = value.Str
			}
		case "reasoning":
			reasoning = value
		case "output_config":
			outputConfig = value
		case "thinking":
			thinking = value
		case "n":
			if n := int(value.Int()); value.Type == gjson.Number && n > 0 {
				f.billing.RequestCount = n
			}
		case "size":
			if value.Type == gjson.String {
				size = strings.TrimSpace(value.Str)
			}
		case "resolution":
			if value.Type == gjson.String {
				resolution = strings.TrimSpace(value.Str)
			}
		case "duration":
			if value.Type == gjson.Number {
				f.billing.VideoDurationSeconds = int(value.Int())
			}
		case "tools":
			tools = value
		case "input":
			input = value
		}
		return true
	})
	// size 优先，没有再看 resolution（与以前的取值顺序一致）
	f.billing.SizeTier = size
	if f.billing.SizeTier == "" {
		f.billing.SizeTier = resolution
	}

	thinkingType := strings.ToLower(strings.TrimSpace(thinking.Get("type").String()))
	f.thinkingEnabled = thinkingType == "enabled" || thinkingType == "adaptive"
	f.reasoningEffort = reasoningEffortFrom(f.model, reasoning.Get("effort").String(), reasoningEffort,
		outputConfig.Get("effort").String(), f.thinkingEnabled)

	if f.billing.EndpointMode == pricing.BillingModeVideo && f.billing.SizeTier == "" {
		f.billing.SizeTier = "480p"
	}
	if strings.Contains(path, "/responses") && responsesRequestHasImageGenerationJSON(tools, input) {
		f.billing.EndpointMode = pricing.BillingModeImage
	}
	return f
}

func baseBillingInput(path string) pricing.BillingInput {
	input := pricing.BillingInput{RequestCount: 1}
	if strings.Contains(path, "/images/") {
		input.EndpointMode = pricing.BillingModeImage
	}
	if strings.Contains(path, "/videos/") {
		input.EndpointMode = pricing.BillingModeVideo
	}
	return input
}

// reasoningEffortFrom 推理档位的取值优先级（对齐 sub2api，见 ExtractMetaFromBody 注释）。
func reasoningEffortFrom(model, nestedEffort, flatEffort, outputEffort string, thinkingEnabled bool) string {
	if e := normalizeOpenAIReasoningEffort(nestedEffort); e != "" {
		return e
	}
	if e := normalizeOpenAIReasoningEffort(flatEffort); e != "" {
		return e
	}
	if e := normalizeClaudeOutputEffort(outputEffort); e != "" {
		return e
	}
	if e := deriveReasoningEffortFromModel(model); e != "" {
		return e
	}
	if thinkingEnabled {
		return defaultEffortForThinkingEnabled(model)
	}
	return ""
}

// responsesRequestHasImageGenerationJSON Responses 请求是否带图片生成工具（顶层 tools，或 input 里的 additional_tools）。
func responsesRequestHasImageGenerationJSON(tools, input gjson.Result) bool {
	hasTool := func(list gjson.Result) bool {
		found := false
		list.ForEach(func(_, tool gjson.Result) bool {
			typ := strings.TrimSpace(tool.Get("type").String())
			if typ == "image_generation" || (typ == "namespace" && strings.TrimSpace(tool.Get("name").String()) == "image_gen") {
				found = true
				return false
			}
			return true
		})
		return found
	}
	if tools.IsArray() && hasTool(tools) {
		return true
	}
	if !input.IsArray() {
		return false
	}
	found := false
	input.ForEach(func(_, entry gjson.Result) bool {
		if strings.TrimSpace(entry.Get("type").String()) == "additional_tools" && hasTool(entry.Get("tools")) {
			found = true
			return false
		}
		return true
	})
	return found
}

// sameBuffer 两个切片是同一块内存（规则没改写 body 时原样返回）。
func sameBuffer(a, b []byte) bool {
	return len(a) == len(b) && (len(a) == 0 || &a[0] == &b[0])
}
