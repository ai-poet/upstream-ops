// 数据面：会话粘性与「路由缺模型」记忆。
//
// 会话粘性（照 sub2api 的 sticky session：gateway_service.go GenerateSessionHash、
// openai_gateway_scheduling.go GenerateSessionHash，TTL 1 小时）：同一对话成功走过哪条路由，
// 下一轮优先回到那条。以前每轮都按优先级从头选，一轮失败顺延到别的路由后，下一轮又回到原来的路由：
//   - Claude 历史里的 thinking 签名是别家账号签的，原路由回 400（线上一天 1117 个请求，平均白打 9 次）；
//   - 上游的提示词缓存也跟着丢。
//
// 路由缺模型记忆：路由回「模型不存在 / 无可用渠道」后 10 分钟内，这个模型的请求把它排到最后，
// 不再每个请求都先去碰一次（线上一条路由一天对同一个模型回了 1197 次 404）。
//
// 两者都只在内存里，重启后丢失，丢了只是退回按优先级选路。
package gateway

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/gjson"
)

const (
	sessionAffinityTTL = time.Hour
	sessionAffinityCap = 200_000
	modelGapTTL        = 10 * time.Minute
	modelGapCap        = 50_000
)

// ttlRouteMap 键 → 路由 id，带过期时间与容量上限。
type ttlRouteMap struct {
	mu  sync.Mutex
	m   map[string]ttlRouteEntry
	cap int
}

type ttlRouteEntry struct {
	routeID uint
	expires time.Time
}

func (t *ttlRouteMap) set(key string, routeID uint, ttl time.Duration, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.m == nil {
		t.m = map[string]ttlRouteEntry{}
	}
	if _, exists := t.m[key]; !exists && len(t.m) >= t.cap {
		t.evictLocked(now)
	}
	t.m[key] = ttlRouteEntry{routeID: routeID, expires: now.Add(ttl)}
}

func (t *ttlRouteMap) get(key string, now time.Time) (uint, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.m[key]
	if !ok {
		return 0, false
	}
	if now.After(e.expires) {
		delete(t.m, key)
		return 0, false
	}
	return e.routeID, true
}

func (t *ttlRouteMap) delete(key string) {
	t.mu.Lock()
	delete(t.m, key)
	t.mu.Unlock()
}

// evictLocked 先清过期；仍满时随机丢一成（map 遍历顺序随机）。
func (t *ttlRouteMap) evictLocked(now time.Time) {
	for k, e := range t.m {
		if now.After(e.expires) {
			delete(t.m, k)
		}
	}
	if len(t.m) < t.cap {
		return
	}
	drop := max(len(t.m)/10, 1)
	for k := range t.m {
		if drop <= 0 {
			break
		}
		delete(t.m, k)
		drop--
	}
}

// sessionAffinity 会话 → 上次成功的路由。键里带组与网关密钥：不同租户的会话不会撞到一起。
type sessionAffinity struct{ ttlRouteMap }

func newSessionAffinity() *sessionAffinity {
	return &sessionAffinity{ttlRouteMap{cap: sessionAffinityCap}}
}

func sessionAffinityKey(groupID, keyID uint, session string) string {
	return strconv.FormatUint(uint64(groupID), 10) + ":" + strconv.FormatUint(uint64(keyID), 10) + ":" + session
}

func (s *sessionAffinity) lookup(groupID, keyID uint, session string) uint {
	if s == nil || session == "" {
		return 0
	}
	id, _ := s.get(sessionAffinityKey(groupID, keyID, session), time.Now())
	return id
}

// bind 成功后绑定 / 续期（每次成功都续 1 小时，同 sub2api RefreshSessionTTL）。
func (s *sessionAffinity) bind(groupID, keyID uint, session string, routeID uint) {
	if s == nil || session == "" || routeID == 0 {
		return
	}
	s.set(sessionAffinityKey(groupID, keyID, session), routeID, sessionAffinityTTL, time.Now())
}

// modelGaps (路由, 模型) → 最近回过「模型不存在」。
type modelGaps struct{ ttlRouteMap }

func newModelGaps() *modelGaps {
	return &modelGaps{ttlRouteMap{cap: modelGapCap}}
}

func modelGapKey(routeID uint, model string) string {
	return strconv.FormatUint(uint64(routeID), 10) + ":" + normalizeModelName(model)
}

func (g *modelGaps) note(routeID uint, model string) {
	if g == nil || routeID == 0 || strings.TrimSpace(model) == "" {
		return
	}
	g.set(modelGapKey(routeID, model), routeID, modelGapTTL, time.Now())
}

func (g *modelGaps) clear(routeID uint, model string) {
	if g == nil || routeID == 0 || strings.TrimSpace(model) == "" {
		return
	}
	g.delete(modelGapKey(routeID, model))
}

// routesMissing 候选路由里最近回过「没有这个模型」的。
func (g *modelGaps) routesMissing(routeIDs []uint, model string) map[uint]struct{} {
	if g == nil || strings.TrimSpace(model) == "" {
		return nil
	}
	now := time.Now()
	var out map[uint]struct{}
	for _, id := range routeIDs {
		if _, ok := g.get(modelGapKey(id, model), now); ok {
			if out == nil {
				out = map[uint]struct{}{}
			}
			out[id] = struct{}{}
		}
	}
	return out
}

// sessionHeaderNames 客户端显式带的会话标识（sub2api explicitOpenAIHeaderSessionNames）：
// Codex 的 session_id / session-id、conversation_id，OpenCode、CodeBuddy 的会话头。
var sessionHeaderNames = []string{
	"session-id",
	"session_id",
	"conversation_id",
	"X-Session-Affinity",
	"X-Session-Id",
	"X-OpenCode-Session",
	"X-Conversation-ID",
}

// legacyClaudeUserID Claude Code 旧版 metadata.user_id：user_<64hex>_account_<uuid>_session_<uuid>。
var legacyClaudeUserID = regexp.MustCompile(`^user_([a-fA-F0-9]{64})_account_([a-fA-F0-9-]*)_session_([a-fA-F0-9-]{36})$`)

// claudeSessionID 从 metadata.user_id 取会话 id（sub2api ParseMetadataUserID：新版是 JSON 字符串，旧版是拼接串）。
func claudeSessionID(userID string) string {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return ""
	}
	if userID[0] == '{' {
		if gjson.Get(userID, "device_id").String() == "" {
			return ""
		}
		return strings.TrimSpace(gjson.Get(userID, "session_id").String())
	}
	if m := legacyClaudeUserID.FindStringSubmatch(userID); m != nil {
		return m[3]
	}
	return ""
}

// sessionKeyFor 同一对话的标识（哈希后返回，空表示认不出）。优先级同 sub2api：
// 显式会话头 → Claude metadata.user_id 里的会话 → prompt_cache_key → 内容兜底
// （模型 + 工具 + 系统提示 / instructions + 第一条用户消息：多轮对话里这些不变）。
func sessionKeyFor(header http.Header, body []byte) string {
	for _, name := range sessionHeaderNames {
		if v := strings.TrimSpace(header.Get(name)); v != "" {
			return hashSessionSeed("h:" + v)
		}
	}
	if len(body) == 0 || !gjson.ValidBytes(body) {
		return ""
	}
	if sid := claudeSessionID(gjson.GetBytes(body, "metadata.user_id").String()); sid != "" {
		return hashSessionSeed("c:" + sid)
	}
	if v := strings.TrimSpace(gjson.GetBytes(body, "prompt_cache_key").String()); v != "" {
		return hashSessionSeed("p:" + v)
	}
	if seed := contentSessionSeed(body); seed != "" {
		return hashSessionSeed("b:" + seed)
	}
	return ""
}

// contentSessionSeed 没有显式会话标识时的内容兜底（sub2api deriveOpenAIContentSessionSeed 的思路，
// 同时认 Anthropic 的 system / messages、OpenAI 的 instructions / messages / input、Gemini 的 contents）。
// 找不到用户消息时返回空：不拿整个请求体当会话，免得每个请求都成了新会话还占内存。
func contentSessionSeed(body []byte) string {
	root := gjson.ParseBytes(body)
	first := firstUserText(root)
	if first == "" {
		return ""
	}
	var b strings.Builder
	b.WriteString("model=")
	b.WriteString(root.Get("model").String())
	if tools := root.Get("tools"); tools.Exists() {
		b.WriteString("|tools=")
		b.WriteString(tools.Raw)
	}
	if v := root.Get("instructions").String(); v != "" {
		b.WriteString("|instructions=")
		b.WriteString(v)
	}
	if sys := root.Get("system"); sys.Exists() {
		b.WriteString("|system=")
		b.WriteString(sys.Raw)
	}
	if sys := root.Get("systemInstruction"); sys.Exists() {
		b.WriteString("|system=")
		b.WriteString(sys.Raw)
	}
	for _, m := range root.Get("messages").Array() {
		if r := m.Get("role").String(); r == "system" || r == "developer" {
			b.WriteString("|system=")
			b.WriteString(m.Get("content").Raw)
			continue
		}
		break
	}
	b.WriteString("|first_user=")
	b.WriteString(first)
	return b.String()
}

// firstUserText 第一条用户消息（原始 JSON 片段）。
func firstUserText(root gjson.Result) string {
	for _, path := range []string{"messages", "input"} {
		arr := root.Get(path)
		if path == "input" && arr.Type == gjson.String {
			return arr.Raw
		}
		for _, m := range arr.Array() {
			if m.Get("role").String() == "user" {
				return m.Get("content").Raw
			}
		}
	}
	for _, c := range root.Get("contents").Array() {
		if role := c.Get("role").String(); role == "" || role == "user" {
			return c.Get("parts").Raw
		}
	}
	return ""
}

func hashSessionSeed(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(sum[:16])
}
