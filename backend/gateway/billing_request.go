package gateway

import (
	"encoding/json"
	"strings"
)

// billingInputFromRequest 请求里的计费参数（张数、尺寸、视频时长、是否 Responses 生图）。
func billingInputFromRequest(path string, body []byte) BillingInput {
	return scanRequestFacts(path, body).billing
}

func billingInputWithResponse(input BillingInput, body []byte) BillingInput {
	for _, raw := range responseJSONObjects(body) {
		if data, ok := raw["data"].([]any); ok && len(data) > 0 {
			input.RequestCount = len(data)
		}
		response, _ := raw["response"].(map[string]any)
		if response == nil {
			response = raw
		}
		if count, size := responsesImageOutputCount(response); count > 0 {
			input.RequestCount = count
			if size != "" {
				input.SizeTier = size
			}
		}
		if usage, _ := response["tool_usage"].(map[string]any); usage != nil {
			if imageGen, _ := usage["image_gen"].(map[string]any); imageGen != nil {
				if count := jsonInt(imageGen["images"]); count > 0 {
					input.RequestCount = count
				}
			}
		}
	}
	return input
}

func responseJSONObjects(body []byte) []map[string]any {
	var out []map[string]any
	appendObject := func(raw []byte) {
		var obj map[string]any
		if json.Unmarshal(raw, &obj) == nil {
			out = append(out, obj)
		}
	}
	appendObject(body)
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "data:") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		}
		if strings.HasPrefix(line, "{") {
			appendObject([]byte(line))
		}
	}
	return out
}

func responsesImageOutputCount(response map[string]any) (int, string) {
	output, _ := response["output"].([]any)
	count := 0
	size := ""
	for _, raw := range output {
		item, _ := raw.(map[string]any)
		if item == nil || item["type"] != "image_generation_call" {
			continue
		}
		if jsonString(item["result"]) == "" && jsonString(item["b64_json"]) == "" && jsonString(item["url"]) == "" {
			continue
		}
		count++
		if size == "" {
			size = jsonString(item["size"])
		}
	}
	return count, size
}

func jsonString(v any) string {
	s, _ := v.(string)
	return strings.TrimSpace(s)
}

func jsonInt(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	default:
		return 0
	}
}
