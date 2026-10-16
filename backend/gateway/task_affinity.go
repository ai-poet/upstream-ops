// 异步任务亲和：任务在哪条路由创建，后续查询 / 下载 / 取消就优先回到哪条路由。
//
// 图片异步任务、视频生成、Seedance 任务、批量图像、自定义语音、Gemini cachedContents
// 的 id 只在创建它的上游有效。记录只在内存里；重启后丢失时，查询按正常顺序逐条尝试，
// 404 不冷却直接换下一条路由（见 classifyFailure 的 task_not_found）。
package gateway

import (
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	taskAffinityTTL = 7 * 24 * time.Hour
	taskAffinityCap = 100_000
)

type taskAffinityEntry struct {
	routeID uint
	expires time.Time
}

type taskAffinity struct {
	mu sync.Mutex
	m  map[string]taskAffinityEntry
}

func newTaskAffinity() *taskAffinity {
	return &taskAffinity{m: map[string]taskAffinityEntry{}}
}

func taskAffinityKey(groupID uint, id string) string {
	return strconv.FormatUint(uint64(groupID), 10) + ":" + id
}

func (t *taskAffinity) remember(groupID uint, routeID uint, ids []string) {
	if t == nil || routeID == 0 || len(ids) == 0 {
		return
	}
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.m)+len(ids) > taskAffinityCap {
		t.evictLocked(now)
	}
	for _, id := range ids {
		if id = strings.TrimSpace(id); id != "" {
			t.m[taskAffinityKey(groupID, id)] = taskAffinityEntry{routeID: routeID, expires: now.Add(taskAffinityTTL)}
		}
	}
}

func (t *taskAffinity) lookup(groupID uint, id string) uint {
	if t == nil || strings.TrimSpace(id) == "" {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.m[taskAffinityKey(groupID, strings.TrimSpace(id))]
	if !ok {
		return 0
	}
	if time.Now().After(e.expires) {
		delete(t.m, taskAffinityKey(groupID, strings.TrimSpace(id)))
		return 0
	}
	return e.routeID
}

// evictLocked 先清过期；仍超限时丢掉最早到期的一成。
func (t *taskAffinity) evictLocked(now time.Time) {
	for k, e := range t.m {
		if now.After(e.expires) {
			delete(t.m, k)
		}
	}
	if len(t.m) < taskAffinityCap {
		return
	}
	drop := len(t.m) / 10
	cutoff := now.Add(taskAffinityTTL)
	for _, e := range t.m {
		if e.expires.Before(cutoff) {
			cutoff = e.expires
		}
	}
	// 粗粒度：从最早到期往后推，直到丢够
	for step := time.Hour; drop > 0 && step <= taskAffinityTTL; step *= 2 {
		limit := cutoff.Add(step)
		for k, e := range t.m {
			if drop <= 0 {
				break
			}
			if e.expires.Before(limit) {
				delete(t.m, k)
				drop--
			}
		}
	}
}

// taskIDsFromResponse 从创建任务的响应里取可作为后续查询凭据的 id。
func taskIDsFromResponse(body []byte) []string {
	trim := strings.TrimSpace(string(body))
	if trim == "" || (trim[0] != '{' && trim[0] != '[') {
		return nil
	}
	var root any
	if json.Unmarshal(body, &root) != nil {
		return nil
	}
	var ids []string
	collect := func(m map[string]any) {
		for _, k := range []string{"id", "task_id", "taskId", "request_id", "requestId", "batch_id", "voice_id", "name"} {
			if s, ok := m[k].(string); ok && strings.TrimSpace(s) != "" {
				ids = append(ids, strings.TrimSpace(s))
				// Gemini cachedContents 的 name 形如 "cachedContents/abc"，查询路径里只有 abc
				if k == "name" {
					if i := strings.LastIndex(s, "/"); i >= 0 && i < len(s)-1 {
						ids = append(ids, s[i+1:])
					}
				}
			}
		}
	}
	switch v := root.(type) {
	case map[string]any:
		collect(v)
		switch data := v["data"].(type) {
		case map[string]any:
			collect(data)
		case []any:
			for _, item := range data {
				if m, ok := item.(map[string]any); ok {
					collect(m)
				}
			}
		}
	case []any:
		for _, item := range v {
			if m, ok := item.(map[string]any); ok {
				collect(m)
			}
		}
	}
	return ids
}
