package gateway

import (
	"testing"

	"github.com/bejix/upstream-ops/backend/gateway/pricing"
)

func TestBillingInputWithResponsesImageOutput(t *testing.T) {
	input := billingInputFromRequest("/v1/responses", []byte(`{"tools":[{"type":"image_generation"}]}`))
	if input.EndpointMode != pricing.BillingModeImage {
		t.Fatalf("mode=%q", input.EndpointMode)
	}
	body := []byte(`{"type":"response.completed","response":{"output":[{"type":"image_generation_call","result":"image-1","size":"1024x1024"},{"type":"image_generation_call","result":"image-2"}],"tool_usage":{"image_gen":{"images":2}}}}`)
	input = input.WithResponse(body)
	if input.RequestCount != 2 || input.SizeTier != "1024x1024" {
		t.Fatalf("input=%+v", input)
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
