package cfgcache

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 同一个 key 的并发加载合并成一次查库。
func TestCacheDedupesConcurrentLoads(t *testing.T) {
	c := New[string, int](time.Minute, true)
	var loads atomic.Int32
	release := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := c.Get("k", func() (int, error) {
				loads.Add(1)
				<-release
				return 42, nil
			})
			if err != nil || v != 42 {
				t.Errorf("get = %v %v", v, err)
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	if loads.Load() != 1 {
		t.Fatalf("loads = %d, want 1", loads.Load())
	}
	if v, _ := c.Get("k", func() (int, error) { loads.Add(1); return 0, nil }); v != 42 || loads.Load() != 1 {
		t.Fatalf("cached value not reused")
	}
}
