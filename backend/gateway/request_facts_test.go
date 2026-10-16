package gateway

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/connector"
	"github.com/bejix/upstream-ops/backend/storage"
)

func TestScanRequestFacts(t *testing.T) {
	f := scanRequestFacts("/v1/images/generations", []byte(`{"resolution":"1024x1024","model":" gpt-image-1 ","n":3,"size":"2048x2048","stream":true,"service_tier":"flex","reasoning":{"effort":"high"},"reasoning_effort":"low"}`))
	if f.model != "gpt-image-1" || !f.stream || f.serviceTier != "flex" || f.reasoningEffort != "high" {
		t.Fatalf("facts %+v", f)
	}
	// size 优先于 resolution，与字段顺序无关
	if f.billing.RequestCount != 3 || f.billing.SizeTier != "2048x2048" || f.billing.EndpointMode != BillingModeImage {
		t.Fatalf("billing %+v", f.billing)
	}
	if got := scanRequestFacts("", []byte(`{"resolution":"720p"}`)).billing.SizeTier; got != "720p" {
		t.Fatalf("resolution fallback %q", got)
	}
	// Responses：input 里的 additional_tools 带生图工具也按图片计费
	resp := scanRequestFacts("/v1/responses", []byte(`{"model":"gpt-5","input":[{"role":"user","content":"draw"},{"type":"additional_tools","tools":[{"type":"image_generation"}]}]}`))
	if resp.billing.EndpointMode != BillingModeImage {
		t.Fatalf("responses image tool not detected: %+v", resp.billing)
	}
	// thinking 开启 + 国产模型：推理档位按 high 记
	th := scanRequestFacts("", []byte(`{"model":"kimi-k2-thinking","thinking":{"type":"enabled"}}`))
	if !th.thinkingEnabled {
		t.Fatalf("thinking not detected: %+v", th)
	}
	// 非 JSON（multipart）一律为空
	if mp := scanRequestFacts("", []byte("--boundary\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\nwhisper-1")); mp.model != "" || mp.stream {
		t.Fatalf("multipart facts %+v", mp)
	}
	// 类型不对的字段不当真
	if bad := scanRequestFacts("", []byte(`{"model":123,"stream":"true"}`)); bad.model != "" || bad.stream {
		t.Fatalf("wrong-typed fields %+v", bad)
	}
}

// 原地改写：不再整包解码重编码，大整数不丢精度，字段顺序不变；已是目标值时原样返回。
func TestBodyRewritesKeepNumbersAndOrder(t *testing.T) {
	body := []byte(`{"seed":12345678901234567890,"model":"a","messages":[]}`)
	out := RewriteModelInBody(body, "b")
	if string(out) != `{"seed":12345678901234567890,"model":"b","messages":[]}` {
		t.Fatalf("rewrite: %s", out)
	}
	if same := RewriteModelInBody(out, "b"); !sameBuffer(same, out) {
		t.Fatal("rewrite to the same model should return the body unchanged")
	}

	s := EnsureStreamUsageOption([]byte(`{"seed":12345678901234567890,"stream":true,"stream_options":null}`), true)
	if !strings.Contains(string(s), `12345678901234567890`) || !strings.Contains(string(s), `"stream_options":{"include_usage":true}`) {
		t.Fatalf("stream options: %s", s)
	}
	if again := EnsureStreamUsageOption(s, true); !sameBuffer(again, s) {
		t.Fatal("second EnsureStreamUsageOption call should be a no-op")
	}
	if off := EnsureStreamUsageOption([]byte(`{"stream":false}`), true); string(off) != `{"stream":false}` {
		t.Fatalf("explicit stream:false must stay: %s", off)
	}
}

// fakeGroupsAPI 统计源分组拉取次数。
type fakeGroupsAPI struct {
	fakeKeyAPI
	calls   atomic.Int32
	delay   time.Duration
	version atomic.Int32
}

func (f *fakeGroupsAPI) ListAPIKeyGroups(context.Context, uint) ([]connector.APIKeyGroup, error) {
	f.calls.Add(1)
	time.Sleep(f.delay)
	return []connector.APIKeyGroup{{Name: "g", Ratio: float64(f.version.Load())}}, nil
}

// 源分组：过期时先用旧值、后台刷新；同时缺缓存的并发请求只打一次上游。
func TestChannelGroupsStaleWhileRevalidate(t *testing.T) {
	api := &fakeGroupsAPI{delay: 50 * time.Millisecond}
	api.version.Store(1)
	svc := NewService(nil, nil, nil, nil, nil, nil, api, nil, nil)
	routes := []storage.GatewayRoute{{ID: 1, SourceChannelID: 7}}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got := svc.loadGroupsByChannel(context.Background(), routes)[7]; len(got) != 1 {
				t.Errorf("first load: %v", got)
			}
		}()
	}
	wg.Wait()
	if n := api.calls.Load(); n != 1 {
		t.Fatalf("concurrent misses fetched %d times, want 1", n)
	}

	// 让缓存过期：读到的仍是旧值，且不等上游
	svc.channelGroupsCacheMu.Lock()
	ent := svc.channelGroupsCache[7]
	ent.at = time.Now().Add(-time.Hour)
	svc.channelGroupsCache[7] = ent
	svc.channelGroupsCacheMu.Unlock()
	api.version.Store(2)

	start := time.Now()
	got := svc.loadGroupsByChannel(context.Background(), routes)[7]
	if time.Since(start) > 20*time.Millisecond || got[0].Ratio != 1 {
		t.Fatalf("stale read should be immediate and return the old value: %v after %v", got, time.Since(start))
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if g := svc.loadGroupsByChannel(context.Background(), routes)[7]; len(g) == 1 && g[0].Ratio == 2 {
			if n := api.calls.Load(); n != 2 {
				t.Fatalf("background refresh fetched %d times in total, want 2", n)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("background refresh never landed")
}
