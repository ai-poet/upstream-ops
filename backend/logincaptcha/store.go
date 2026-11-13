// Package logincaptcha 提供后台登录图形验证码的生成与一次性校验。
package logincaptcha

import (
	"errors"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultTTL 是登录验证码的默认有效期。
	DefaultTTL = 5 * time.Minute
	// cleanupInterval 是过期验证码的后台清理周期。
	cleanupInterval = time.Minute
)

type entry struct {
	answer    string
	expiresAt time.Time
}

// Store 是带 TTL 的内存验证码存储，并实现 base64Captcha.Store。
// Store 独立于 auth.Service，配置热更新重建鉴权服务时不会丢失验证码。
type Store struct {
	mu      sync.Mutex
	entries map[string]entry
	ttl     time.Duration
	now     func() time.Time

	done      chan struct{}
	closeOnce sync.Once
}

// NewStore 创建内存 Store，并启动过期条目清理协程。
func NewStore(ttl time.Duration) *Store {
	return newStore(ttl, time.Now, cleanupInterval)
}

func newStore(ttl time.Duration, now func() time.Time, cleanEvery time.Duration) *Store {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	s := &Store{
		entries: make(map[string]entry),
		ttl:     ttl,
		now:     now,
		done:    make(chan struct{}),
	}
	if cleanEvery > 0 {
		go s.cleanupLoop(cleanEvery)
	}
	return s
}

// Set 保存验证码答案，并顺便清理已经过期的条目。
func (s *Store) Set(id, answer string) error {
	if id == "" {
		return errors.New("captcha id is empty")
	}
	now := s.now()
	s.mu.Lock()
	s.removeExpiredLocked(now)
	s.entries[id] = entry{answer: answer, expiresAt: now.Add(s.ttl)}
	s.mu.Unlock()
	return nil
}

// Get 返回验证码答案。clear 为 true 时无论答案是否匹配都会删除条目。
func (s *Store) Get(id string, clear bool) string {
	if id == "" {
		return ""
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.entries[id]
	if !ok {
		return ""
	}
	expired := !now.Before(item.expiresAt)
	if clear || expired {
		delete(s.entries, id)
	}
	if expired {
		return ""
	}
	return item.answer
}

// Verify 校验验证码。clear 为 true 时验证码只能消费一次，错误答案也会消费。
func (s *Store) Verify(id, answer string, clear bool) bool {
	if id == "" || strings.TrimSpace(answer) == "" {
		return false
	}
	stored := s.Get(id, clear)
	return stored != "" && strings.EqualFold(strings.TrimSpace(stored), strings.TrimSpace(answer))
}

// Close 停止后台清理协程。
func (s *Store) Close() {
	s.closeOnce.Do(func() { close(s.done) })
}

func (s *Store) cleanupLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.mu.Lock()
			s.removeExpiredLocked(s.now())
			s.mu.Unlock()
		case <-s.done:
			return
		}
	}
}

func (s *Store) removeExpiredLocked(now time.Time) {
	for id, item := range s.entries {
		if !now.Before(item.expiresAt) {
			delete(s.entries, id)
		}
	}
}
