// 故障转移策略：失败分类、可重试状态码、Retry-After、自动禁用判定与候选路由排序。
//
// 参考 new-api 的做法（可配置重试状态码、鉴权 / 额度类错误自动禁用并定时探测恢复、
// 优先级从高到低、同优先级可随机），在 upstream-ops 已有的同路由重试 / 顺延 / 冷却之上补齐：
//   - 模型不存在 / 端点不支持：不冷却，直接换下一条路由（即使没开 4xx 顺延）；
//   - 上下文超长：换哪条路由都一样，直接回显；
//   - 429 / 503 的 Retry-After 用作冷却时长。
package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/connector"
	"github.com/bejix/upstream-ops/backend/storage"
)

// 负载均衡模式
const (
	// LoadBalanceOrdered 同优先级按倍率方向、position 固定先后（默认）
	LoadBalanceOrdered = "ordered"
	// LoadBalanceRandom 同优先级的路由随机打散，失败再顺延到同级其它路由、再到下一优先级
	LoadBalanceRandom = "random"
)

func normalizeLoadBalanceMode(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case LoadBalanceRandom, "weighted":
		return LoadBalanceRandom
	}
	return LoadBalanceOrdered
}

// maxRetryAfterCooldown Retry-After 用作冷却时的上限，避免上游给出离谱的等待时间把路由长期挂起。
const maxRetryAfterCooldown = 10 * time.Minute

type statusRange struct{ lo, hi int }

// parseStatusCodeRanges 解析 "429,500-503,520-599" 形式的状态码列表（区间两侧可带空格：「500 - 503」）。
func parseStatusCodeRanges(raw string) ([]statusRange, error) {
	var out []statusRange
	// 先把区间里的空格收紧，再按逗号 / 分号 / 空白切分
	raw = statusRangeDash.ReplaceAllString(raw, "-")
	for _, part := range strings.FieldsFunc(raw, func(r rune) bool { return r == ',' || r == ';' || unicode.IsSpace(r) }) {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		lo, hi := part, part
		if i := strings.Index(part, "-"); i > 0 {
			lo, hi = strings.TrimSpace(part[:i]), strings.TrimSpace(part[i+1:])
		}
		a, err1 := strconv.Atoi(lo)
		b, err2 := strconv.Atoi(hi)
		if err1 != nil || err2 != nil || a < 100 || b > 599 || a > b {
			return nil, fmt.Errorf("invalid status code range %q", part)
		}
		out = append(out, statusRange{lo: a, hi: b})
	}
	return out, nil
}

var statusRangeDash = regexp.MustCompile(`\s*-\s*`)

// maxStatusCodeRangesLen 规范化结果的最大长度（retry_status_codes 列宽 256）。
const maxStatusCodeRangesLen = 256

// NormalizeStatusCodeRanges 校验并规范化状态码列表（空串合法，表示用默认规则）：
// 排序、合并重叠与相邻的区间（"500,501,502" → "500-502"）。
func NormalizeStatusCodeRanges(raw string) (string, error) {
	ranges, err := parseStatusCodeRanges(raw)
	if err != nil {
		return "", err
	}
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].lo < ranges[j].lo })
	var merged []statusRange
	for _, r := range ranges {
		if n := len(merged); n > 0 && r.lo <= merged[n-1].hi+1 {
			merged[n-1].hi = max(merged[n-1].hi, r.hi)
			continue
		}
		merged = append(merged, r)
	}
	parts := make([]string, 0, len(merged))
	for _, r := range merged {
		if r.lo == r.hi {
			parts = append(parts, strconv.Itoa(r.lo))
		} else {
			parts = append(parts, fmt.Sprintf("%d-%d", r.lo, r.hi))
		}
	}
	out := strings.Join(parts, ",")
	if len(out) > maxStatusCodeRangesLen {
		return "", fmt.Errorf("status code list too long (max %d characters after merging)", maxStatusCodeRangesLen)
	}
	return out, nil
}

// NormalizeAutoDisableStatusCodes 校验系统设置里的自动禁用状态码：空串（用默认 401）、
// none（不按状态码禁用）或状态码列表。
func NormalizeAutoDisableStatusCodes(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if strings.EqualFold(raw, config.GatewayAutoDisableStatusCodesNone) || strings.EqualFold(raw, "off") {
		return config.GatewayAutoDisableStatusCodesNone, nil
	}
	return NormalizeStatusCodeRanges(raw)
}

func statusInRanges(code int, ranges []statusRange) bool {
	for _, r := range ranges {
		if code >= r.lo && code <= r.hi {
			return true
		}
	}
	return false
}

// failureAction 一次失败尝试之后怎么走。
type failureAction int

const (
	// failStop 不重试、不顺延，把上游错误回显给客户端
	failStop failureAction = iota
	// failRetry 同路由重试，耗尽后冷却并顺延
	failRetry
	// failSkipRoute 这条路由接不住该请求（模型不存在 / 端点不支持 / 任务不在此上游 / 协议转换不支持）：
	// 不重试、不冷却，直接顺延
	failSkipRoute
	// failDisableRoute 鉴权 / 额度类错误：自动禁用路由并顺延
	failDisableRoute
)

type failureDecision struct {
	Action failureAction
	// Cooldown >0 时覆盖组默认冷却（Retry-After）
	Cooldown time.Duration
	// Reason 分类标签，写进用量明细
	Reason string
}

// classifyFailure 判定一次失败尝试的走向。fwdErr 为传输层错误（已排除客户端取消）。
// echoes 是请求里由客户端决定、上游可能原样回显的值（请求模型、映射后的上游模型），
// 自动禁用的关键词匹配会先把它们从错误信息里去掉。
func (rt *Runtime) classifyFailure(group *storage.GatewayGroup, status int, headers http.Header, body []byte, fwdErr error, taskLookup bool, echoes ...string) failureDecision {
	if fwdErr != nil {
		if isProtocolConvertError(fwdErr) {
			return failureDecision{Action: failSkipRoute, Reason: "protocol_convert"}
		}
		return failureDecision{Action: failRetry, Reason: "transport"}
	}
	if status >= 200 && status < 400 {
		return failureDecision{Action: failStop}
	}
	msg := failureText(body)

	if isContextLengthError(status, msg) {
		return failureDecision{Action: failStop, Reason: "context_length"}
	}
	if taskLookup && status == http.StatusNotFound {
		return failureDecision{Action: failSkipRoute, Reason: "task_not_found"}
	}
	// 模型不存在 / 端点不支持先于自动禁用：这类错误常回显请求里的模型名，不能拿来禁路由
	if isRouteCapabilityError(status, msg) {
		return failureDecision{Action: failSkipRoute, Reason: "model_unavailable"}
	}
	if group != nil && group.AutoDisableEnabled && rt.matchesAutoDisable(status, body, echoes) {
		return failureDecision{Action: failDisableRoute, Reason: "auto_disable"}
	}
	if rt.isRetryableStatus(group, status) {
		d := failureDecision{Action: failRetry, Reason: "http_" + strconv.Itoa(status)}
		if status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable {
			d.Cooldown = retryAfter(headers, time.Now())
		}
		return d
	}
	return failureDecision{Action: failStop, Reason: "http_" + strconv.Itoa(status)}
}

// isRetryableStatus 组配置了 RetryStatusCodes 时按其判定；否则沿用默认：429、5xx，开 FailoverOn4xx 再加全部 4xx。
func (rt *Runtime) isRetryableStatus(group *storage.GatewayGroup, status int) bool {
	if group != nil && strings.TrimSpace(group.RetryStatusCodes) != "" {
		if ranges, err := parseStatusCodeRanges(group.RetryStatusCodes); err == nil && len(ranges) > 0 {
			return statusInRanges(status, ranges)
		}
	}
	failoverOn4xx := group != nil && group.FailoverOn4xx
	return rt.isFailoverStatus(status, failoverOn4xx)
}

// matchesAutoDisable 状态码或错误信息命中系统设置里的自动禁用规则。
//
// 错误信息常回显客户端输入（new-api 的「对于模型 insufficient_quota 无可用渠道」、
// OpenAI 的「Invalid value: 'invalid_api_key'」），任何持有网关密钥的人都能借此禁掉路由。
// 所以关键词按下面的规则匹配：
//   - 上游给出的错误码 / 错误类型（由上游生成，不含用户输入）：任何关键词命中即禁用；
//   - 错误信息：只看 4xx，先去掉请求模型名与引号里的内容（回显的参数值）；
//     短语型关键词（含空格或非 ASCII 字符）直接匹配，单词型关键词（如 insufficient_quota）
//     只在鉴权 / 额度类状态码（401、402、403、429）上匹配。
func (rt *Runtime) matchesAutoDisable(status int, body []byte, echoes []string) bool {
	cfg := rt.gatewayRuntime()
	if codes := cfg.AutoDisableStatusCodes; codes != config.GatewayAutoDisableStatusCodesNone {
		if ranges, err := parseStatusCodeRanges(codes); err == nil && statusInRanges(status, ranges) {
			return true
		}
	}
	code, typ, msg := upstreamErrorFields(body)
	ids := strings.ToLower(code + "\n" + typ)
	msg = strings.ToLower(msg)
	for _, e := range echoes {
		if e = strings.ToLower(strings.TrimSpace(e)); e != "" {
			msg = strings.ReplaceAll(msg, e, " ")
		}
	}
	msg = quotedSegment.ReplaceAllString(msg, " ")
	msgEligible := status >= 400 && status < 500
	for _, kw := range cfg.AutoDisableKeywords {
		kw = strings.ToLower(strings.TrimSpace(kw))
		if kw == "" {
			continue
		}
		if strings.Contains(ids, kw) {
			return true
		}
		if msgEligible && (isPhraseKeyword(kw) || autoDisableKeywordStatus(status)) && strings.Contains(msg, kw) {
			return true
		}
	}
	return false
}

// quotedSegment 错误信息里引号括起来的片段，多为回显的请求参数值。
var quotedSegment = regexp.MustCompile("'[^']*'|\"[^\"]*\"|`[^`]*`|“[^”]*”|「[^」]*」")

// isPhraseKeyword 含空格或非 ASCII 字符的关键词：一整句提示，难以借参数值回显伪造。
func isPhraseKeyword(kw string) bool {
	for _, r := range kw {
		if r == ' ' || r > unicode.MaxASCII {
			return true
		}
	}
	return false
}

// autoDisableKeywordStatus 单词型关键词只在这些状态码的错误信息上用于自动禁用。
func autoDisableKeywordStatus(status int) bool {
	switch status {
	case http.StatusUnauthorized, http.StatusPaymentRequired, http.StatusForbidden, http.StatusTooManyRequests:
		return true
	}
	return false
}

// upstreamErrorFields 取上游错误体的错误码、错误类型与错误信息。
// 兼容 OpenAI / Anthropic（error 对象）、Gemini（error.status）与扁平的 {message,type,code}；
// 不是 JSON 时把原文（截断）当作错误信息。
func upstreamErrorFields(body []byte) (code, typ, msg string) {
	var m map[string]any
	if json.Unmarshal(bytes.TrimSpace(body), &m) != nil {
		snippet := body
		if len(snippet) > 4096 {
			snippet = snippet[:4096]
		}
		return "", "", string(snippet)
	}
	src := m
	if errObj, ok := m["error"].(map[string]any); ok {
		src = errObj
	} else if s, ok := m["error"].(string); ok {
		msg = s
	}
	if msg == "" {
		msg = stringValue(src["message"])
	}
	typ = strings.TrimSpace(stringValue(src["type"]) + " " + stringValue(src["status"]))
	switch v := src["code"].(type) {
	case string:
		code = v
	case float64:
		code = strconv.Itoa(int(v))
	}
	return code, typ, msg
}

// failureText 错误体的小写文本（上游 message + 原文片段），用于关键词匹配。
func failureText(body []byte) string {
	_, msg, _ := extractUpstreamError(body)
	snippet := body
	if len(snippet) > 4096 {
		snippet = snippet[:4096]
	}
	return strings.ToLower(msg + "\n" + string(snippet))
}

var contextLengthPhrases = []string{
	"context_length_exceeded",
	"maximum context length",
	"context length exceeded",
	"prompt is too long",
	"input is too long",
	"exceeds the context window",
	"exceeds the maximum number of tokens",
	"input token count",
	"超出最大上下文",
	"上下文长度超",
}

func isContextLengthError(status int, msg string) bool {
	if status != http.StatusBadRequest && status != http.StatusRequestEntityTooLarge && status != http.StatusUnprocessableEntity {
		return false
	}
	for _, p := range contextLengthPhrases {
		if strings.Contains(msg, p) {
			return true
		}
	}
	return false
}

var modelUnavailablePhrases = []string{
	"model_not_found",
	"model not found",
	"no such model",
	"unknown model",
	"invalid model",
	"unsupported model",
	"not supported model",
	"model is not supported",
	"model not supported",
	"model_not_supported",
	"no available channel",
	"no available account",
	"无可用渠道",
	"模型不存在",
	"不支持的模型",
	"不支持该模型",
}

// isRouteCapabilityError 这条路由没有该模型 / 不支持该端点：换一条路由大概率能成。
func isRouteCapabilityError(status int, msg string) bool {
	switch status {
	case http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusNotImplemented:
		return true
	}
	if status < 400 {
		return false
	}
	for _, p := range modelUnavailablePhrases {
		if strings.Contains(msg, p) {
			return true
		}
	}
	// OpenAI: "The model `x` does not exist or you do not have access to it."
	return strings.Contains(msg, "model") && strings.Contains(msg, "does not exist")
}

// retryAfter 解析 Retry-After（秒数或 HTTP 日期），上限 maxRetryAfterCooldown；无效返回 0。
func retryAfter(h http.Header, now time.Time) time.Duration {
	if h == nil {
		return 0
	}
	v := strings.TrimSpace(h.Get("Retry-After"))
	if v == "" {
		if ms := strings.TrimSpace(h.Get("Retry-After-Ms")); ms != "" {
			if n, err := strconv.ParseFloat(ms, 64); err == nil && n > 0 {
				return clampRetryAfter(time.Duration(n * float64(time.Millisecond)))
			}
		}
		return 0
	}
	if n, err := strconv.ParseFloat(v, 64); err == nil {
		if n <= 0 {
			return 0
		}
		return clampRetryAfter(time.Duration(n * float64(time.Second)))
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := t.Sub(now); d > 0 {
			return clampRetryAfter(d)
		}
	}
	return 0
}

func clampRetryAfter(d time.Duration) time.Duration {
	if d > maxRetryAfterCooldown {
		return maxRetryAfterCooldown
	}
	return d
}

// routePreferences 选路偏好：只调整顺序，不剔除路由（偏好路由都失败后仍会顺延到其它路由）。
type routePreferences struct {
	// ModelRouteIDs 模型清单里声明了该模型的路由；nil 表示不区分
	ModelRouteIDs map[uint]struct{}
	// PinnedRouteID 异步任务创建时所在的路由，查询 / 取消时优先回到它
	PinnedRouteID uint
}

// orderCandidates 可调度路由的尝试顺序：
//  1. 优先级从高到低，同优先级按倍率方向 + position（SortRoutes）；
//  2. random 模式下，同优先级的一档内随机打散；
//  3. 声明了该模型的路由排前面；
//  4. 任务亲和路由放最前。
func orderCandidates(
	routes []storage.GatewayRoute,
	groupsByChannel map[uint][]connector.APIKeyGroup,
	group *storage.GatewayGroup,
	now time.Time,
	exclude map[uint]struct{},
	prefs routePreferences,
) []ScoredRoute {
	direction := ""
	if group != nil {
		direction = group.RateSortDirection
	}
	cands := SortRoutes(routes, groupsByChannel, direction, now, exclude)
	if len(cands) <= 1 {
		return cands
	}
	if group != nil && normalizeLoadBalanceMode(group.LoadBalanceMode) == LoadBalanceRandom {
		shufflePriorityTiers(cands)
	}
	if len(prefs.ModelRouteIDs) > 0 {
		sort.SliceStable(cands, func(i, j int) bool {
			_, a := prefs.ModelRouteIDs[cands[i].Route.ID]
			_, b := prefs.ModelRouteIDs[cands[j].Route.ID]
			return a && !b
		})
	}
	if prefs.PinnedRouteID != 0 {
		for i := range cands {
			if cands[i].Route.ID == prefs.PinnedRouteID {
				pinned := cands[i]
				copy(cands[1:i+1], cands[:i])
				cands[0] = pinned
				break
			}
		}
	}
	return cands
}

// shufflePriorityTiers 同优先级的连续一段随机打散（候选已按优先级降序排好）。
func shufflePriorityTiers(cands []ScoredRoute) {
	for start := 0; start < len(cands); {
		end := start + 1
		for end < len(cands) && cands[end].Route.Weight == cands[start].Route.Weight {
			end++
		}
		if end-start > 1 {
			tier := cands[start:end]
			rand.Shuffle(len(tier), func(i, j int) { tier[i], tier[j] = tier[j], tier[i] })
		}
		start = end
	}
}

// modelRouteIDs 组模型清单里声明了 requested（或组级映射后的模型）的路由。
// 清单里没有该模型、或只有手工条目没有来源时返回 nil（不区分路由）。
func (svc *Service) modelRouteIDs(group *storage.GatewayGroup, routes []storage.GatewayRoute, models ...string) map[uint]struct{} {
	if group == nil || !group.ModelAwareRouting {
		return nil
	}
	wanted := map[string]struct{}{}
	for _, m := range models {
		if m = strings.ToLower(strings.TrimSpace(m)); m != "" {
			wanted[m] = struct{}{}
		}
	}
	if len(wanted) == 0 {
		return nil
	}
	ids := map[uint]struct{}{}
	channels := map[uint]struct{}{}
	for _, item := range svc.ParseModelsJSON(group.ModelsJSON) {
		if _, ok := wanted[strings.ToLower(item.ID)]; !ok {
			continue
		}
		for _, src := range item.Sources {
			if src.RouteID > 0 {
				ids[src.RouteID] = struct{}{}
			} else if src.ChannelID > 0 {
				channels[src.ChannelID] = struct{}{}
			}
		}
		for _, ch := range item.ChannelIDs {
			channels[ch] = struct{}{}
		}
	}
	// 清单项没有路由级来源时（旧数据只有 channel_ids）按渠道匹配；须在循环前判定，
	// 否则第一条渠道路由加入后，其余同样声明了该模型的路由就被漏掉
	byChannel := len(ids) == 0
	for _, r := range routes {
		if _, ok := channels[r.SourceChannelID]; ok && r.SourceChannelID > 0 && byChannel {
			ids[r.ID] = struct{}{}
		}
		// 路由级映射或转发模型显式写了该模型，也算声明
		for k := range ParseModelMapping(r.ModelMappingJSON) {
			if _, ok := wanted[strings.ToLower(strings.TrimSpace(k))]; ok {
				ids[r.ID] = struct{}{}
			}
		}
		if routeDeclaresModel(&r, models...) {
			ids[r.ID] = struct{}{}
		}
	}
	if len(ids) == 0 {
		return nil
	}
	return ids
}

var errNoRoutes = errors.New("no schedulable routes")
