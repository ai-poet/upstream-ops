// 数据面：源分组列表缓存。
package gateway

import (
	"context"
	"sync"
	"time"

	"github.com/bejix/upstream-ops/backend/connector"
	"github.com/bejix/upstream-ops/backend/storage"
)

func (rt *Runtime) loadGroupsByChannel(ctx context.Context, routes []storage.GatewayRoute) map[uint][]connector.APIKeyGroup {
	out := make(map[uint][]connector.APIKeyGroup)
	if rt.ChannelAPI == nil {
		return out
	}
	ids := make([]uint, 0)
	seen := make(map[uint]struct{})
	for _, r := range routes {
		if r.NormalizeSourceKind() == storage.GatewayRouteSourceProvider {
			continue
		}
		if r.SourceChannelID == 0 {
			continue
		}
		if _, ok := seen[r.SourceChannelID]; ok {
			continue
		}
		seen[r.SourceChannelID] = struct{}{}
		ids = append(ids, r.SourceChannelID)
	}
	if len(ids) == 0 {
		return out
	}

	// 先吃缓存：新鲜的直接用；过期的也先用旧值、在后台刷新（请求路径不再同步等上游管理接口）；
	// 从没拉过的才同步拉。同一渠道同时只有一个拉取在跑，并发请求等它的结果。
	ttl := rt.gatewayRuntime().ModelsCacheTTL()
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	miss := make([]uint, 0, len(ids))
	stale := make([]uint, 0)
	now := time.Now()
	rt.channelGroupsCacheMu.Lock()
	if rt.channelGroupsCache == nil {
		rt.channelGroupsCache = map[uint]channelGroupsCacheEntry{}
	}
	for _, id := range ids {
		if ent, ok := rt.channelGroupsCache[id]; ok {
			out[id] = ent.groups
			if now.Sub(ent.at) >= ttl {
				stale = append(stale, id)
			}
			continue
		}
		miss = append(miss, id)
	}
	rt.channelGroupsCacheMu.Unlock()

	for _, id := range stale {
		rt.refreshChannelGroupsAsync(id)
	}
	if len(miss) == 0 {
		return out
	}
	if len(miss) == 1 {
		out[miss[0]] = rt.fetchChannelGroups(ctx, miss[0])
		return out
	}

	// 保存路由 / 运行时倍率排序：按渠道并发拉源分组，缩短批量等待。
	sem := make(chan struct{}, rt.gatewayRuntime().RouteBatchConcurrency)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, id := range miss {
		id := id
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				mu.Lock()
				out[id] = nil
				mu.Unlock()
				return
			}
			groups := rt.fetchChannelGroups(ctx, id)
			mu.Lock()
			out[id] = groups
			mu.Unlock()
		}()
	}
	wg.Wait()
	return out
}

// channelGroupsCall 一次进行中的源分组拉取；同一渠道的并发调用共享结果。
type channelGroupsCall struct {
	done   chan struct{}
	groups []connector.APIKeyGroup
}

// fetchChannelGroups 拉一个渠道的源分组并写缓存；同一渠道已有拉取在跑时等它的结果，不重复打上游。
// 拉取失败时缓存 nil（选路回落到路由上保存的倍率），下个 TTL 再试。
func (rt *Runtime) fetchChannelGroups(ctx context.Context, id uint) []connector.APIKeyGroup {
	rt.channelGroupsCacheMu.Lock()
	if rt.channelGroupsInflight == nil {
		rt.channelGroupsInflight = map[uint]*channelGroupsCall{}
	}
	if call, ok := rt.channelGroupsInflight[id]; ok {
		rt.channelGroupsCacheMu.Unlock()
		select {
		case <-call.done:
			return call.groups
		case <-ctx.Done():
			return nil
		}
	}
	call := &channelGroupsCall{done: make(chan struct{})}
	rt.channelGroupsInflight[id] = call
	rt.channelGroupsCacheMu.Unlock()

	groups, err := rt.ChannelAPI.ListAPIKeyGroups(ctx, id)
	if err != nil {
		groups = nil
	}
	call.groups = groups
	rt.channelGroupsCacheMu.Lock()
	delete(rt.channelGroupsInflight, id)
	if rt.channelGroupsCache == nil {
		rt.channelGroupsCache = map[uint]channelGroupsCacheEntry{}
	}
	rt.channelGroupsCache[id] = channelGroupsCacheEntry{at: time.Now(), groups: groups}
	rt.channelGroupsCacheMu.Unlock()
	close(call.done)
	return groups
}

// refreshChannelGroupsAsync 后台刷新一个渠道的源分组（已有拉取在跑时什么都不做）。
func (rt *Runtime) refreshChannelGroupsAsync(id uint) {
	rt.channelGroupsCacheMu.Lock()
	_, running := rt.channelGroupsInflight[id]
	rt.channelGroupsCacheMu.Unlock()
	if running {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		rt.fetchChannelGroups(ctx, id)
	}()
}

func (rt *Runtime) storeChannelGroupsCache(channelID uint, groups []connector.APIKeyGroup) {
	rt.channelGroupsCacheMu.Lock()
	defer rt.channelGroupsCacheMu.Unlock()
	if rt.channelGroupsCache == nil {
		rt.channelGroupsCache = map[uint]channelGroupsCacheEntry{}
	}
	rt.channelGroupsCache[channelID] = channelGroupsCacheEntry{at: time.Now(), groups: groups}
}

// InvalidateChannelGroupsCache 清空源分组缓存（倍率扫描后调用，保证下次重排拿到新 ratio）。

func (rt *Runtime) InvalidateChannelGroupsCache() {
	rt.channelGroupsCacheMu.Lock()
	defer rt.channelGroupsCacheMu.Unlock()
	rt.channelGroupsCache = map[uint]channelGroupsCacheEntry{}
}

// upstreamTarget 解析后的上游目标（监控渠道或直连 Provider）。
