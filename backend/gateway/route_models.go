// 路由「转发模型」：每条路由只接自己声明的模型（同 new-api 渠道的模型列表），空表示不限。
package gateway

import (
	"strings"

	"github.com/bejix/upstream-ops/backend/storage"
)

// normalizeModelName 比较用的模型名：去空白、小写、去掉 Gemini 的 models/ 前缀。
func normalizeModelName(m string) string {
	return strings.ToLower(strings.TrimPrefix(strings.TrimSpace(m), "models/"))
}

// modelPatternMatch pattern 末尾为 * 时按前缀匹配，否则整名匹配（都不区分大小写）。
func modelPatternMatch(pattern, model string) bool {
	p := normalizeModelName(pattern)
	m := normalizeModelName(model)
	if p == "" || m == "" {
		return false
	}
	if prefix, ok := strings.CutSuffix(p, "*"); ok {
		return strings.HasPrefix(m, prefix)
	}
	return p == m
}

// routeSupportsModel 路由的转发模型是否允许 models 中任一模型（请求模型、组级映射后的模型）；
// 没配转发模型的路由什么模型都接。
func routeSupportsModel(route *storage.GatewayRoute, models ...string) bool {
	if route == nil || len(route.SupportedModels) == 0 {
		return true
	}
	for _, m := range models {
		for _, p := range route.SupportedModels {
			if modelPatternMatch(p, m) {
				return true
			}
		}
	}
	return false
}

// routeDeclaresModel 路由在转发模型里显式写了这个模型（用于按模型优先排序）；没配清单的路由不算声明。
func routeDeclaresModel(route *storage.GatewayRoute, models ...string) bool {
	return route != nil && len(route.SupportedModels) > 0 && routeSupportsModel(route, models...)
}

// filterRoutesForModel 只留下转发模型允许该模型的路由。请求不带模型（任务查询、cachedContents 列表等）时不过滤。
func filterRoutesForModel(routes []storage.GatewayRoute, models ...string) []storage.GatewayRoute {
	hasModel := false
	for _, m := range models {
		if strings.TrimSpace(m) != "" {
			hasModel = true
			break
		}
	}
	if !hasModel {
		return routes
	}
	out := make([]storage.GatewayRoute, 0, len(routes))
	for i := range routes {
		if routeSupportsModel(&routes[i], models...) {
			out = append(out, routes[i])
		}
	}
	return out
}

// applySupportedModels 路由同步模型时只保留转发模型允许的：整名条目直接列入（上游 /v1/models 不列也算），
// 通配条目从上游清单里筛。没配清单时原样返回。
func applySupportedModels(route *storage.GatewayRoute, upstream []string) []string {
	if route == nil || len(route.SupportedModels) == 0 {
		return upstream
	}
	var out []string
	seen := map[string]struct{}{}
	add := func(m string) {
		k := normalizeModelName(m)
		if k == "" {
			return
		}
		if _, ok := seen[k]; ok {
			return
		}
		seen[k] = struct{}{}
		out = append(out, strings.TrimSpace(m))
	}
	for _, p := range route.SupportedModels {
		if !strings.HasSuffix(strings.TrimSpace(p), "*") {
			add(p)
		}
	}
	for _, m := range upstream {
		if routeSupportsModel(route, m) {
			add(m)
		}
	}
	return out
}

// exactSupportedModels 转发模型里的整名条目（不含通配）。
func exactSupportedModels(route *storage.GatewayRoute) []string {
	if route == nil {
		return nil
	}
	var out []string
	for _, p := range route.SupportedModels {
		if p = strings.TrimSpace(p); p != "" && !strings.HasSuffix(p, "*") {
			out = append(out, p)
		}
	}
	return out
}
