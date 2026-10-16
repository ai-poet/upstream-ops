// 共用：上游 User-Agent 解析与默认值（转发、模型测试、拉模型）。
package gateway

import (
	"strings"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/storage"
)

func (svc *core) defaultUpstreamUserAgent() string {
	if svc == nil {
		return config.DefaultUpstreamUserAgent
	}
	svc.mu.RLock()
	ua := strings.TrimSpace(svc.upstream.UserAgent)
	svc.mu.RUnlock()
	if ua == "" {
		return config.DefaultUpstreamUserAgent
	}
	return ua
}

// resolveAdminUserAgent 模型测试 / 拉模型：组+路由策略，空则默认 UA。
func (svc *core) resolveAdminUserAgent(group *storage.GatewayGroup, route *storage.GatewayRoute) string {
	return withDefaultUserAgent(ResolveRouteUserAgentFrom(group, route), svc.defaultUpstreamUserAgent())
}

// applyRouteUserAgentForAdmin 模型测试等无客户端路径：空则填默认 UA。
func (svc *core) applyRouteUserAgentForAdmin(target *upstreamTarget, group *storage.GatewayGroup, route *storage.GatewayRoute) {
	if target == nil {
		return
	}
	target.UserAgentOverride = svc.resolveAdminUserAgent(group, route)
}
