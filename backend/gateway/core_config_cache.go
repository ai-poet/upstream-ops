// 共用：热路径配置缓存（数据面每个请求读，管理面解析上游目标也读）。
//
// 鉴权（密钥、分组）、选路（路由）、取上游目标（直连渠道、监控渠道）每个请求都要读，以前每次都查库；
// SQLite 只有一个连接，这些读和用量写排在同一个队列里，是并发吞吐的主要瓶颈。
// 现在读一次缓存在内存：
//   - 密钥 / 分组 / 路由 / 直连渠道：存储层网关配置版本（任何写入后加一）变了就立即重载，另有 TTL 兜底
//     （多个实例共用一个数据库时，别的实例的改动最多滞后一个 TTL）；
//   - 监控渠道由监控模块频繁更新余额等字段，不参与版本号，只按 TTL 刷新（网关只用到地址、代理开关）。
//
// 同一个 key 的并发加载合并成一次查库；查库出错不缓存。
package gateway

import (
	"sync"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
)

const (
	configCacheTTL     = 10 * time.Second
	channelCacheTTL    = 30 * time.Second
	configCacheMaxKeys = 10000
)

type cacheEntry[V any] struct {
	val     V
	version uint64
	at      time.Time
}

type cacheCall[V any] struct {
	done chan struct{}
	val  V
	err  error
}

// configCache 带版本与 TTL 的小缓存。versioned=false 时只看 TTL。
type configCache[K comparable, V any] struct {
	ttl       time.Duration
	versioned bool

	mu       sync.Mutex
	entries  map[K]cacheEntry[V]
	inflight map[K]*cacheCall[V]
}

func newConfigCache[K comparable, V any](ttl time.Duration, versioned bool) *configCache[K, V] {
	return &configCache[K, V]{ttl: ttl, versioned: versioned, entries: map[K]cacheEntry[V]{}, inflight: map[K]*cacheCall[V]{}}
}

func (c *configCache[K, V]) get(key K, load func() (V, error)) (V, error) {
	// 读库前取版本：加载期间若有写入，缓存条目带着旧版本，下次读就会重载
	version := storage.GatewayConfigVersion()
	now := time.Now()

	c.mu.Lock()
	if e, ok := c.entries[key]; ok && now.Sub(e.at) < c.ttl && (!c.versioned || e.version == version) {
		c.mu.Unlock()
		return e.val, nil
	}
	if call, ok := c.inflight[key]; ok {
		c.mu.Unlock()
		<-call.done
		return call.val, call.err
	}
	call := &cacheCall[V]{done: make(chan struct{})}
	c.inflight[key] = call
	c.mu.Unlock()

	call.val, call.err = load()

	c.mu.Lock()
	delete(c.inflight, key)
	if call.err == nil {
		if len(c.entries) >= configCacheMaxKeys {
			c.entries = map[K]cacheEntry[V]{}
		}
		c.entries[key] = cacheEntry[V]{val: call.val, version: version, at: now}
	}
	c.mu.Unlock()
	close(call.done)
	return call.val, call.err
}

// runtimeConfigCache 数据面用到的各类配置缓存。
type runtimeConfigCache struct {
	keys      *configCache[string, storage.GatewayKey]
	groups    *configCache[uint, storage.GatewayGroup]
	routes    *configCache[uint, []storage.GatewayRoute]
	providers *configCache[uint, storage.GatewayProvider]
	channels  *configCache[uint, storage.Channel]
}

func (svc *core) configCache() *runtimeConfigCache {
	svc.configCacheOnce.Do(func() {
		svc.cfgCache = &runtimeConfigCache{
			keys:      newConfigCache[string, storage.GatewayKey](configCacheTTL, true),
			groups:    newConfigCache[uint, storage.GatewayGroup](configCacheTTL, true),
			routes:    newConfigCache[uint, []storage.GatewayRoute](configCacheTTL, true),
			providers: newConfigCache[uint, storage.GatewayProvider](configCacheTTL, true),
			channels:  newConfigCache[uint, storage.Channel](channelCacheTTL, false),
		}
	})
	return svc.cfgCache
}

// 以下返回值都是缓存的副本，调用方可以放心修改。

func (svc *core) cachedKeyByHash(hash string) (*storage.GatewayKey, error) {
	k, err := svc.configCache().keys.get(hash, func() (storage.GatewayKey, error) {
		item, err := svc.Keys.FindByHash(hash)
		if err != nil {
			return storage.GatewayKey{}, err
		}
		return *item, nil
	})
	if err != nil {
		return nil, err
	}
	return &k, nil
}

func (svc *core) cachedGroup(id uint) (*storage.GatewayGroup, error) {
	g, err := svc.configCache().groups.get(id, func() (storage.GatewayGroup, error) {
		item, err := svc.Groups.FindByID(id)
		if err != nil {
			return storage.GatewayGroup{}, err
		}
		return *item, nil
	})
	if err != nil {
		return nil, err
	}
	return &g, nil
}

func (svc *core) cachedRoutes(groupID uint) ([]storage.GatewayRoute, error) {
	list, err := svc.configCache().routes.get(groupID, func() ([]storage.GatewayRoute, error) {
		return svc.Routes.ListByGroupID(groupID)
	})
	if err != nil {
		return nil, err
	}
	return append([]storage.GatewayRoute(nil), list...), nil
}

func (svc *core) cachedProvider(id uint) (*storage.GatewayProvider, error) {
	p, err := svc.configCache().providers.get(id, func() (storage.GatewayProvider, error) {
		item, err := svc.Providers.FindByID(id)
		if err != nil {
			return storage.GatewayProvider{}, err
		}
		return *item, nil
	})
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func (svc *core) cachedChannel(id uint) (*storage.Channel, error) {
	ch, err := svc.configCache().channels.get(id, func() (storage.Channel, error) {
		item, err := svc.Channels.FindByID(id)
		if err != nil {
			return storage.Channel{}, err
		}
		return *item, nil
	})
	if err != nil {
		return nil, err
	}
	return &ch, nil
}
