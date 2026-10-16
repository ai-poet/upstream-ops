// 共用：源分组列表缓存（数据面算倍率选路，管理面排序 / 保存路由）。
package gateway

import (
	"context"
	"sync"
	"time"

	"github.com/bejix/upstream-ops/backend/connector"
	"github.com/bejix/upstream-ops/backend/storage"
)

func (svc *core) loadGroupsByChannel(ctx context.Context, routes []storage.GatewayRoute) map[uint][]connector.APIKeyGroup {
	out := make(map[uint][]connector.APIKeyGroup)
	if svc.ChannelAPI == nil {
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
	ttl := svc.gatewayRuntime().ModelsCacheTTL()
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	miss := make([]uint, 0, len(ids))
	stale := make([]uint, 0)
	now := time.Now()
	svc.channelGroupsCacheMu.Lock()
	if svc.channelGroupsCache == nil {
		svc.channelGroupsCache = map[uint]channelGroupsCacheEntry{}
	}
	for _, id := range ids {
		if ent, ok := svc.channelGroupsCache[id]; ok {
			out[id] = ent.groups
			if now.Sub(ent.at) >= ttl {
				stale = append(stale, id)
			}
			continue
		}
		miss = append(miss, id)
	}
	svc.channelGroupsCacheMu.Unlock()

	for _, id := range stale {
		svc.refreshChannelGroupsAsync(id)
	}
	if len(miss) == 0 {
		return out
	}
	if len(miss) == 1 {
		out[miss[0]] = svc.fetchChannelGroups(ctx, miss[0])
		return out
	}

	// 保存路由 / 运行时倍率排序：按渠道并发拉源分组，缩短批量等待。
	sem := make(chan struct{}, svc.gatewayRuntime().RouteBatchConcurrency)
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
			groups := svc.fetchChannelGroups(ctx, id)
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
func (svc *core) fetchChannelGroups(ctx context.Context, id uint) []connector.APIKeyGroup {
	svc.channelGroupsCacheMu.Lock()
	if svc.channelGroupsInflight == nil {
		svc.channelGroupsInflight = map[uint]*channelGroupsCall{}
	}
	if call, ok := svc.channelGroupsInflight[id]; ok {
		svc.channelGroupsCacheMu.Unlock()
		select {
		case <-call.done:
			return call.groups
		case <-ctx.Done():
			return nil
		}
	}
	call := &channelGroupsCall{done: make(chan struct{})}
	svc.channelGroupsInflight[id] = call
	svc.channelGroupsCacheMu.Unlock()

	groups, err := svc.ChannelAPI.ListAPIKeyGroups(ctx, id)
	if err != nil {
		groups = nil
	}
	call.groups = groups
	svc.channelGroupsCacheMu.Lock()
	delete(svc.channelGroupsInflight, id)
	if svc.channelGroupsCache == nil {
		svc.channelGroupsCache = map[uint]channelGroupsCacheEntry{}
	}
	svc.channelGroupsCache[id] = channelGroupsCacheEntry{at: time.Now(), groups: groups}
	svc.channelGroupsCacheMu.Unlock()
	close(call.done)
	return groups
}

// refreshChannelGroupsAsync 后台刷新一个渠道的源分组（已有拉取在跑时什么都不做）。
func (svc *core) refreshChannelGroupsAsync(id uint) {
	svc.channelGroupsCacheMu.Lock()
	_, running := svc.channelGroupsInflight[id]
	svc.channelGroupsCacheMu.Unlock()
	if running {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		svc.fetchChannelGroups(ctx, id)
	}()
}

func (svc *core) storeChannelGroupsCache(channelID uint, groups []connector.APIKeyGroup) {
	svc.channelGroupsCacheMu.Lock()
	defer svc.channelGroupsCacheMu.Unlock()
	if svc.channelGroupsCache == nil {
		svc.channelGroupsCache = map[uint]channelGroupsCacheEntry{}
	}
	svc.channelGroupsCache[channelID] = channelGroupsCacheEntry{at: time.Now(), groups: groups}
}

// InvalidateChannelGroupsCache 清空源分组缓存（倍率扫描后调用，保证下次重排拿到新 ratio）。

func (svc *core) InvalidateChannelGroupsCache() {
	svc.channelGroupsCacheMu.Lock()
	defer svc.channelGroupsCacheMu.Unlock()
	svc.channelGroupsCache = map[uint]channelGroupsCacheEntry{}
}

// upstreamTarget 解析后的上游目标（监控渠道或直连 Provider）。
