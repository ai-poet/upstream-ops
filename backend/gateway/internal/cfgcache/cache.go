// Package cfgcache 网关配置的小缓存：按存储层网关配置版本与 TTL 失效。
//
// 本进程的任何网关配置写入都会让版本号加一，带版本的缓存条目随即失效；
// TTL 兜底多实例共用一个数据库时别的实例的改动（最多滞后一个 TTL）。
// 同一个 key 的并发加载合并成一次；加载出错不缓存。
package cfgcache

import (
	"sync"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
)

// maxKeys 条目上限；满了整体清空（配置条目有限，正常到不了）。
const maxKeys = 10000

type entry[V any] struct {
	val     V
	version uint64
	at      time.Time
}

type call[V any] struct {
	done chan struct{}
	val  V
	err  error
}

// Cache 带版本与 TTL 的缓存。versioned=false 时只看 TTL。
type Cache[K comparable, V any] struct {
	ttl       time.Duration
	versioned bool

	mu       sync.Mutex
	entries  map[K]entry[V]
	inflight map[K]*call[V]
}

// New 新建缓存。
func New[K comparable, V any](ttl time.Duration, versioned bool) *Cache[K, V] {
	return &Cache[K, V]{ttl: ttl, versioned: versioned, entries: map[K]entry[V]{}, inflight: map[K]*call[V]{}}
}

// Get 命中且未过期时返回缓存值，否则用 load 加载（同一 key 并发只加载一次）。
func (c *Cache[K, V]) Get(key K, load func() (V, error)) (V, error) {
	// 读库前取版本：加载期间若有写入，缓存条目带着旧版本，下次读就会重载
	version := storage.GatewayConfigVersion()
	now := time.Now()

	c.mu.Lock()
	if e, ok := c.entries[key]; ok && now.Sub(e.at) < c.ttl && (!c.versioned || e.version == version) {
		c.mu.Unlock()
		return e.val, nil
	}
	if cl, ok := c.inflight[key]; ok {
		c.mu.Unlock()
		<-cl.done
		return cl.val, cl.err
	}
	cl := &call[V]{done: make(chan struct{})}
	c.inflight[key] = cl
	c.mu.Unlock()

	cl.val, cl.err = load()

	c.mu.Lock()
	delete(c.inflight, key)
	if cl.err == nil {
		if len(c.entries) >= maxKeys {
			c.entries = map[K]entry[V]{}
		}
		c.entries[key] = entry[V]{val: cl.val, version: version, at: now}
	}
	c.mu.Unlock()
	close(cl.done)
	return cl.val, cl.err
}
