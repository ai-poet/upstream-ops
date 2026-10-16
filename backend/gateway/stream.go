// 流式转发结果类型。
package gateway

import (
	"net/http"

	"github.com/bejix/upstream-ops/backend/gateway/pricing"
)

// streamAttemptResult 单次流式转发结果。
// Committed=true 表示已向客户端写出有效 SSE，禁止 retry/failover。
// DownstreamComplete=true 表示已成功向客户端写出流式终端帧（[DONE] / message_stop 等）；
// 此后客户端关连接属于正常收尾，不应记 error_type=client。
type streamAttemptResult struct {
	Status             int
	Headers            http.Header
	Body               []byte
	FirstTokenMS       *int64
	Tokens             pricing.UsageTokens
	Committed          bool
	ClientDisconnected bool
	DownstreamComplete bool
	StreamErr          error
	Err                error
}
