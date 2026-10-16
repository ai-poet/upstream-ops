// 数据面：公开 HTTP 端点（models / count_tokens / gemini 等）。
package gateway

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

// HandleModels 返回 OpenAI 风格模型列表（GET /v1/models、/models）。
func (rt *Runtime) HandleModels(c *gin.Context) {
	_ = ensureGatewayRequestID(c)
	auth, err := rt.Authenticate(c)
	if err != nil {
		writeAuthError(c, protocolOpenAI, err.Error())
		return
	}
	rt.touchKey(auth.Key.ID, time.Now())
	payload, err := rt.groupModelsPayload(c, auth.Group)
	if err != nil {
		writeGatewayError(c, protocolOpenAI, http.StatusInternalServerError, "api_error", err.Error())
		return
	}
	c.Data(http.StatusOK, "application/json", payload)
}

// HandleModel 返回单个模型（GET /v1/models/:model）。
func (rt *Runtime) HandleModel(c *gin.Context) {
	_ = ensureGatewayRequestID(c)
	auth, err := rt.Authenticate(c)
	if err != nil {
		writeAuthError(c, protocolOpenAI, err.Error())
		return
	}
	id := strings.TrimPrefix(strings.TrimSpace(c.Param("model")), "/")
	ids, err := rt.groupModelIDs(c, auth.Group)
	if err != nil {
		writeGatewayError(c, protocolOpenAI, http.StatusInternalServerError, "api_error", err.Error())
		return
	}
	for _, m := range ids {
		if m == id {
			c.JSON(http.StatusOK, gin.H{"id": m, "object": "model", "created": time.Now().Unix(), "owned_by": "upstream-ops"})
			return
		}
	}
	writeGatewayError(c, protocolOpenAI, http.StatusNotFound, "invalid_request_error", fmt.Sprintf("model %q not found", id))
}

// groupModelIDs 组对外的模型 id 列表（与 /v1/models 同源、同缓存）。
func (rt *Runtime) groupModelIDs(c *gin.Context, group *storage.GatewayGroup) ([]string, error) {
	payload, err := rt.groupModelsPayload(c, group)
	if err != nil {
		return nil, err
	}
	var list struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(payload, &list); err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(list.Data))
	for _, m := range list.Data {
		ids = append(ids, m.ID)
	}
	return ids, nil
}

// groupModelsPayload 组模型列表的 OpenAI JSON（带 TTL 缓存）。
func (rt *Runtime) groupModelsPayload(c *gin.Context, group *storage.GatewayGroup) ([]byte, error) {
	rt.modelsCacheMu.Lock()
	if ent, ok := rt.modelsCache[group.ID]; ok && time.Since(ent.at) < rt.gatewayRuntime().ModelsCacheTTL() {
		body := ent.body
		rt.modelsCacheMu.Unlock()
		return body, nil
	}
	rt.modelsCacheMu.Unlock()

	type modelObj struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		Created int64  `json:"created"`
		OwnedBy string `json:"owned_by"`
	}
	seen := map[string]struct{}{}
	data := make([]modelObj, 0)
	add := func(id string) {
		id = strings.TrimSpace(id)
		if id == "" {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		data = append(data, modelObj{
			ID:      id,
			Object:  "model",
			Created: time.Now().Unix(),
			OwnedBy: "upstream-ops",
		})
	}

	mode := group.ModelsMode
	if mode == "" {
		mode = storage.GatewayModelsModeAuto
	}
	stored := rt.cachedModelsJSON(group.ModelsJSON)

	if mode == storage.GatewayModelsModeManual {
		for _, it := range stored {
			add(it.ID)
		}
	} else {
		// auto / hybrid: live aggregate
		routes, err := rt.cachedRoutes(group.ID)
		if err != nil {
			return nil, err
		}
		groupsByChannel := rt.loadGroupsByChannel(c.Request.Context(), routes)
		candidates := SortRoutes(routes, groupsByChannel, group.RateSortDirection, time.Now(), nil)
		groupMapping := cachedModelMapping(group.ModelMappingJSON)

		for _, cand := range candidates {
			route := cand.Route
			// 与 pullRouteModels / 转发一致：监控渠道与直连 provider 都走 resolveUpstreamTarget。
			// 旧逻辑只 FindByID(SourceChannelID)，直连路由 channel_id=0 会被整段跳过 → /v1/models 空列表。
			target, err := rt.resolveUpstreamTarget(&route)
			if err != nil {
				continue
			}
			ch := target.Channel
			if ch == nil {
				label := ""
				if target.Provider != nil {
					label = target.Provider.Name
				}
				ch = &storage.Channel{
					Name:    label,
					SiteURL: target.BaseURL,
				}
			}
			// 拉模型：组+路由 UA，空则默认 UA（与模型测试一致；转发仍透传客户端）
			models, err := rt.fetchUpstreamModels(c.Request.Context(), ch, target.APIKey, rt.resolveAdminUserAgent(group, &route))
			// 转发模型里写明的模型总是列出（上游 /v1/models 不列、或拉取失败也一样）
			for _, m := range exactSupportedModels(&route) {
				add(m)
			}
			if err != nil {
				continue
			}
			routeMapping := cachedModelMapping(route.ModelMappingJSON)
			for _, m := range models {
				id := m
				for src, dst := range routeMapping {
					if dst == m && src != "*" {
						id = src
						break
					}
				}
				for src, dst := range groupMapping {
					if dst == m && src != "*" {
						id = src
						break
					}
				}
				// 配了转发模型的路由只列它接的模型
				if !routeSupportsModel(&route, id, m) {
					continue
				}
				add(id)
			}
		}
		if mode == storage.GatewayModelsModeHybrid {
			for _, it := range stored {
				if it.Source == "custom" {
					add(it.ID)
				}
			}
		}
	}

	payload, _ := json.Marshal(gin.H{"object": "list", "data": data})
	rt.modelsCacheMu.Lock()
	rt.modelsCache[group.ID] = modelsCacheEntry{at: time.Now(), body: payload}
	rt.modelsCacheMu.Unlock()
	return payload, nil
}

// HandleCountTokens POST /v1/messages/count_tokens
// 对齐 sub2api：优先透传上游；失败则本地粗估。

func (rt *Runtime) HandleCountTokens(c *gin.Context) {
	_ = ensureGatewayRequestID(c)
	auth, err := rt.Authenticate(c)
	if err != nil {
		writeAuthError(c, protocolAnthropic, err.Error())
		return
	}
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		writeGatewayError(c, protocolAnthropic, http.StatusBadRequest, "invalid_request_error", "failed to read body")
		return
	}
	body, err = rt.applySystemPrompt(auth.Group, auth.Key, "/v1/messages/count_tokens", protocol.KindAnthropic, body)
	if err != nil {
		writeGatewayError(c, protocolAnthropic, http.StatusBadRequest, "invalid_request_error", "system prompt injection failed: "+err.Error())
		return
	}
	// 尝试转发到第一条可用路由的 /v1/messages/count_tokens（只看转发模型接这个模型的路由）
	routes, _ := rt.cachedRoutes(auth.Group.ID)
	if model := ExtractModelFromBody(body); model != "" {
		mapped, _ := ResolveModel(model, cachedModelMapping(auth.Group.ModelMappingJSON))
		routes = filterRoutesForModel(routes, model, mapped)
	}
	groupsByChannel := rt.loadGroupsByChannel(c.Request.Context(), routes)
	cands := SortRoutes(routes, groupsByChannel, auth.Group.RateSortDirection, time.Now(), nil)
	for _, cand := range cands {
		route := cand.Route
		target, rerr := rt.resolveUpstreamTarget(&route)
		if rerr != nil {
			continue
		}
		applyRouteUserAgent(target, auth.Group, &route)
		status, _, respBody, _, ferr := rt.forwardOnce(c.Request.Context(), upstreamCall{
			target: target, path: "/v1/messages/count_tokens", method: http.MethodPost,
			header: c.Request.Header, body: body, kind: protocol.KindAnthropic,
		})
		if ferr == nil && status >= 200 && status < 300 && len(respBody) > 0 {
			c.Data(status, "application/json", respBody)
			return
		}
	}
	// 本地粗估（字符/4）
	inputTokens := estimateTokenCount(body)
	c.JSON(http.StatusOK, gin.H{
		"input_tokens": inputTokens,
		"type":         "message_count_tokens_result",
	})
}

// HandleUsage 用量相关端点。
func (rt *Runtime) HandleUsage(c *gin.Context) {
	_ = ensureGatewayRequestID(c)
	auth, err := rt.Authenticate(c)
	if err != nil {
		writeAuthError(c, protocolOpenAI, err.Error())
		return
	}
	if rt.Usage == nil {
		c.JSON(http.StatusOK, gin.H{"object": "list", "data": []any{}})
		return
	}
	page, err := rt.Usage.List(storage.GatewayUsageQuery{
		GatewayKeyID: auth.Key.ID,
		Page:         1,
		PageSize:     50,
	})
	if err != nil {
		writeGatewayError(c, protocolOpenAI, http.StatusInternalServerError, "api_error", err.Error())
		return
	}
	stats, _ := rt.Usage.Stats(storage.GatewayUsageQuery{GatewayKeyID: auth.Key.ID})
	c.JSON(http.StatusOK, gin.H{
		"object": "usage",
		"key_id": auth.Key.ID,
		"stats":  stats,
		"recent": page.Items,
	})
}

// JSON / Header 中的网关请求 ID 字段（与使用记录 request_id 一致，便于排查）
const (
	ctxKeyUpstreamOpsRequestID  = "upstream_ops_request_id"
	headerUpstreamOpsRequestID  = "X-Upstream-Ops-Request-Id"
	jsonKeyUpstreamOpsRequestID = "upstream_ops_request_id"
)

// clientRequestIDHeaders 客户端请求相关 ID 头：原样透传到上游，网关不改写、不采纳为 usage.request_id。
var clientRequestIDHeaders = []string{
	"X-Request-Id",
	"X-Client-Request-Id",
	"X-Openai-Request-Id",
	"X-Correlation-Id",
	"Request-Id",
}

// copyClientRequestIDHeaders 把入站请求相关 ID 原样写到上游请求；不碰 X-Upstream-Ops-Request-Id。
