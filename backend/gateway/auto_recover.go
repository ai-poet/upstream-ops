// 自动禁用路由的恢复探测：按系统设置的间隔对被禁用的路由发一次最小请求，成功即恢复调度。
package gateway

import (
	"context"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
)

// StartAutoRecover 启动恢复探测后台任务（重复调用无副作用）。
func (s *Service) StartAutoRecover() {
	if s == nil || s.Routes == nil {
		return
	}
	s.autoRecoverMu.Lock()
	defer s.autoRecoverMu.Unlock()
	if s.autoRecoverStop != nil {
		return
	}
	stop := make(chan struct{})
	s.autoRecoverStop = stop
	go s.autoRecoverLoop(stop)
}

func (s *Service) stopAutoRecover() {
	s.autoRecoverMu.Lock()
	defer s.autoRecoverMu.Unlock()
	if s.autoRecoverStop != nil {
		close(s.autoRecoverStop)
		s.autoRecoverStop = nil
	}
}

func (s *Service) autoRecoverLoop(stop chan struct{}) {
	for {
		interval := time.Duration(s.gatewayRuntime().AutoRecoverMinutes) * time.Minute
		timer := time.NewTimer(interval)
		select {
		case <-stop:
			timer.Stop()
			return
		case <-timer.C:
			s.RecoverAutoDisabledRoutes(context.Background())
		}
	}
}

// RecoverAutoDisabledRoutes 对所有被自动禁用的路由各探测一次，返回恢复的条数。
func (s *Service) RecoverAutoDisabledRoutes(ctx context.Context) int {
	routes, err := s.Routes.ListAutoDisabled()
	if err != nil || len(routes) == 0 {
		return 0
	}
	recovered := 0
	groups := map[uint]*storage.GatewayGroup{}
	for _, route := range routes {
		group, ok := groups[route.GatewayGroupID]
		if !ok {
			group, _ = s.Groups.FindByID(route.GatewayGroupID)
			groups[route.GatewayGroupID] = group
		}
		if group == nil {
			continue
		}
		model := s.probeModelForRoute(ctx, group, route)
		if model == "" {
			// 找不到可探测的模型：路由会一直保持禁用，需要人工「清除暂停」
			if s.Log != nil {
				s.Log.Warn("gateway auto-disabled route has no probe model; clear it manually after fixing the upstream",
					"route_id", route.ID, "group_id", route.GatewayGroupID)
			}
			continue
		}
		res := s.Admin.probeRouteModel(ctx, group, route, model, ParseModelMapping(group.ModelMappingJSON))
		if !res.OK {
			if s.Log != nil {
				s.Log.Info("gateway auto-disabled route still failing", "route_id", route.ID, "model", model, "status", res.StatusCode, "err", res.Error)
			}
			continue
		}
		if err := s.Routes.ClearAutoDisabled(route.ID); err == nil {
			recovered++
			_ = s.Routes.NoteSuccessForPauseError(route.ID)
			if s.Log != nil {
				s.Log.Info("gateway auto-disabled route recovered", "route_id", route.ID, "model", model)
			}
		}
	}
	return recovered
}

// probeModelForRoute 选一个这条路由声明过的模型；都没有时用组模型清单的第一个，
// 清单也是空的（组没同步过模型）再问上游 /v1/models。
func (s *Service) probeModelForRoute(ctx context.Context, group *storage.GatewayGroup, route storage.GatewayRoute) string {
	// 转发模型里写明的模型最可靠：这条路由本来就只接它们
	if exact := exactSupportedModels(&route); len(exact) > 0 {
		return exact[0]
	}
	items := s.ParseModelsJSON(group.ModelsJSON)
	for _, item := range items {
		if !routeSupportsModel(&route, item.ID) {
			continue
		}
		for _, src := range item.Sources {
			if src.RouteID == route.ID {
				return item.ID
			}
		}
	}
	for k := range ParseModelMapping(route.ModelMappingJSON) {
		if k = strings.TrimSpace(k); k != "" && k != "*" {
			return k
		}
	}
	for _, item := range items {
		if routeSupportsModel(&route, item.ID) {
			return item.ID
		}
	}
	return s.firstUpstreamModel(ctx, group, route)
}

// firstUpstreamModel 拉这条路由上游的 /v1/models，返回第一个模型；失败返回空。
func (s *Service) firstUpstreamModel(ctx context.Context, group *storage.GatewayGroup, route storage.GatewayRoute) string {
	target, err := s.Admin.resolveUpstreamTarget(&route)
	if err != nil {
		return ""
	}
	ch := target.Channel
	if ch == nil {
		// 直连渠道：用 provider base 拉
		ch = &storage.Channel{SiteURL: target.BaseURL}
	}
	fetchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	models, err := s.fetchUpstreamModels(fetchCtx, ch, target.APIKey, s.resolveAdminUserAgent(group, &route))
	if err != nil {
		if s.Log != nil {
			s.Log.Info("gateway auto-disabled route: list upstream models failed", "route_id", route.ID, "err", err)
		}
		return ""
	}
	for _, m := range models {
		if m = strings.TrimSpace(m); m != "" && routeSupportsModel(&route, m) {
			return m
		}
	}
	return ""
}
