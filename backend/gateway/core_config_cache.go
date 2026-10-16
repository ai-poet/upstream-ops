// 共用：热路径配置缓存（数据面每个请求读，管理面解析上游目标也读）。
//
// 鉴权（密钥、分组）、选路（路由）、取上游目标（直连渠道、监控渠道）每个请求都要读，以前每次都查库；
// SQLite 只有一个连接，这些读和用量写排在同一个队列里，是并发吞吐的主要瓶颈。
// 现在读一次缓存在内存：
//   - 密钥 / 分组 / 路由 / 直连渠道：存储层网关配置版本（任何写入后加一）变了就立即重载，另有 TTL 兜底
//     （多个实例共用一个数据库时，别的实例的改动最多滞后一个 TTL）；
//   - 监控渠道由监控模块频繁更新余额等字段，不参与版本号，只按 TTL 刷新（网关只用到地址、代理开关）。
//
// 同一个 key 的并发加载合并成一次查库；查库出错不缓存（见 internal/cfgcache）。
package gateway

import (
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/internal/cfgcache"
	"github.com/bejix/upstream-ops/backend/storage"
)

const (
	configCacheTTL  = 10 * time.Second
	channelCacheTTL = 30 * time.Second
)

// runtimeConfigCache 数据面用到的各类配置缓存。
type runtimeConfigCache struct {
	keys      *cfgcache.Cache[string, storage.GatewayKey]
	groups    *cfgcache.Cache[uint, storage.GatewayGroup]
	routes    *cfgcache.Cache[uint, []storage.GatewayRoute]
	providers *cfgcache.Cache[uint, storage.GatewayProvider]
	channels  *cfgcache.Cache[uint, storage.Channel]
}

func (svc *core) configCache() *runtimeConfigCache {
	svc.configCacheOnce.Do(func() {
		svc.cfgCache = &runtimeConfigCache{
			keys:      cfgcache.New[string, storage.GatewayKey](configCacheTTL, true),
			groups:    cfgcache.New[uint, storage.GatewayGroup](configCacheTTL, true),
			routes:    cfgcache.New[uint, []storage.GatewayRoute](configCacheTTL, true),
			providers: cfgcache.New[uint, storage.GatewayProvider](configCacheTTL, true),
			channels:  cfgcache.New[uint, storage.Channel](channelCacheTTL, false),
		}
	})
	return svc.cfgCache
}

// 以下返回值都是缓存的副本，调用方可以放心修改。

func (svc *core) cachedKeyByHash(hash string) (*storage.GatewayKey, error) {
	k, err := svc.configCache().keys.Get(hash, func() (storage.GatewayKey, error) {
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
	g, err := svc.configCache().groups.Get(id, func() (storage.GatewayGroup, error) {
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
	list, err := svc.configCache().routes.Get(groupID, func() ([]storage.GatewayRoute, error) {
		return svc.Routes.ListByGroupID(groupID)
	})
	if err != nil {
		return nil, err
	}
	return append([]storage.GatewayRoute(nil), list...), nil
}

func (svc *core) cachedProvider(id uint) (*storage.GatewayProvider, error) {
	p, err := svc.configCache().providers.Get(id, func() (storage.GatewayProvider, error) {
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
	ch, err := svc.configCache().channels.Get(id, func() (storage.Channel, error) {
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
