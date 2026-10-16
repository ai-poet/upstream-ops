package gateway

// 测试沿用的展开参数写法：直接给出目标、路径与协议发一次上游请求。
// 生产代码统一传 upstreamCall（见 relay_context.go）。

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

func (s *Service) buildUpstreamHTTPRequest(ctx context.Context, target *upstreamTarget, path, method string, inHeader http.Header, body []byte, kind protocolKind, stream bool) (*http.Request, error) {
	return s.runtime().buildUpstreamHTTPRequest(ctx, upstreamCall{
		target: target, path: path, method: method, header: inHeader, body: body, kind: kind, stream: stream,
	})
}

func (s *Service) forwardStream(ctx context.Context, c *gin.Context, target *upstreamTarget, path, method string, inHeader http.Header, body []byte, inbound, upstream protocolKind, model string, converted bool, firstTokenTimeout time.Duration) streamAttemptResult {
	call := upstreamCall{target: target, path: path, method: method, header: inHeader, body: body, firstTokenTimeout: firstTokenTimeout}
	return s.runtime().forwardStreamPlan(ctx, c, call, defaultStreamPlan(inbound, upstream, model, converted))
}
