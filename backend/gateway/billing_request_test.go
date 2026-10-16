package gateway

import (
	"testing"

	"github.com/bejix/upstream-ops/backend/gateway/pricing"
)

// Responses 带生图工具：请求阶段只记出图模型，真的出了图才按图片计费（对齐 sub2api）。
func TestBillingInputWithResponsesImageOutput(t *testing.T) {
	input := billingInputFromRequest("/v1/responses", []byte(`{"model":"gpt-6.1-sol","tools":[{"type":"image_generation"}]}`))
	if input.EndpointMode != "" || input.ImageModel != "gpt-image-2" {
		t.Fatalf("请求阶段不应定为图片计费：%+v", input)
	}
	// 没出图：照常按 token
	noImage := input.WithResponse([]byte(`{"type":"response.completed","response":{"output":[{"type":"message","content":[{"type":"output_text","text":"hi"}]}],"usage":{"input_tokens":10,"output_tokens":2}}}`))
	if noImage.EndpointMode != "" {
		t.Fatalf("没出图却按图片计费：%+v", noImage)
	}
	body := []byte(`{"type":"response.completed","response":{"output":[{"type":"image_generation_call","result":"image-1","size":"1024x1024"},{"type":"image_generation_call","result":"image-2"}],"tool_usage":{"image_gen":{"images":2}}}}`)
	input = input.WithResponse(body)
	if input.EndpointMode != pricing.BillingModeImage || input.RequestCount != 2 || input.SizeTier != "1024x1024" {
		t.Fatalf("input=%+v", input)
	}
	// 工具里写了 model / size 时按工具的
	tool := billingInputFromRequest("/v1/responses", []byte(`{"model":"gpt-6.1-sol","tools":[{"type":"function","name":"f"},{"type":"image_generation","model":"gpt-image-1.5","size":"1536x1024"}]}`))
	if tool.ImageModel != "gpt-image-1.5" || tool.SizeTier != "1536x1024" {
		t.Fatalf("tool=%+v", tool)
	}
}

func TestParseOpenAIUsage_ResponsesTerminalImageUsage(t *testing.T) {
	body := []byte(`{"type":"response.completed","response":{"usage":{"input_tokens":10,"output_tokens":20},"tool_usage":{"image_gen":{"output_tokens_details":{"image_tokens":12}}}}}`)
	u := ParseOpenAIUsage(body)
	if u.InputTokens != 10 || u.OutputTokens != 20 || u.ImageOutputTokens != 12 {
		t.Fatalf("usage=%+v", u)
	}
}

func TestResponsesWebSocketHelpers(t *testing.T) {
	url, err := responsesWebSocketURL("https://api.example.com/base/")
	if err != nil || url != "wss://api.example.com/base/v1/responses" {
		t.Fatalf("url=%q err=%v", url, err)
	}
	terminal, success, typ, msg := wsResponsesTerminal([]byte(`{"type":"response.failed","response":{"error":{"code":"server_error","message":"busy"}}}`))
	if !terminal || success || typ != "server_error" || msg != "busy" {
		t.Fatalf("terminal=%v success=%v type=%q message=%q", terminal, success, typ, msg)
	}
}

// billingInputFromRequest 请求里的计费参数（张数、尺寸、视频时长、是否 Responses 生图）。
func billingInputFromRequest(path string, body []byte) pricing.BillingInput {
	return scanRequestFacts(path, body).billing
}
