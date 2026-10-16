// 数据面：Gemini 原生端点（/v1beta、/antigravity/v1beta）。
//
// generateContent / streamGenerateContent 按 Gemini 协议入站，经 relaykit 转到路由的上游协议
// （Gemini 上游直接透传）；countTokens 优先问 Gemini 上游，失败本地估算；
// embedContent / batchEmbedContents / cachedContents 只透传给 Gemini 上游。
package gateway

import (
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

var geminiModelSegment = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._\-]*$`)

// parseGeminiModelAction 拆 "{model}:{action}"（兼容 "{model}/{action}" 与 "models/" 前缀）。
func parseGeminiModelAction(raw string) (model, action string) {
	raw = strings.TrimPrefix(strings.TrimSpace(raw), "/")
	raw = strings.TrimPrefix(raw, "models/")
	if i := strings.LastIndex(raw, ":"); i >= 0 {
		return raw[:i], raw[i+1:]
	}
	if i := strings.LastIndex(raw, "/"); i >= 0 {
		return raw[:i], raw[i+1:]
	}
	return raw, ""
}

// HandleGeminiGenerate POST /v1beta/models/{model}:{action}
func (rt *Runtime) HandleGeminiGenerate(c *gin.Context) {
	_ = ensureGatewayRequestID(c)
	model, action := parseGeminiModelAction(c.Param("modelAction"))
	if !geminiModelSegment.MatchString(model) {
		writeError(c, protocol.KindGemini, newGatewayError(http.StatusBadRequest, "invalid_request_error", "invalid model name"))
		return
	}
	path := "/v1beta/models/" + model + ":" + action
	switch strings.ToLower(action) {
	case "generatecontent":
		stream := false
		rt.handleForward(c, forwardRequest{Path: path, Kind: protocol.KindGemini, Model: model, Stream: &stream})
	case "streamgeneratecontent":
		stream := true
		rt.handleForward(c, forwardRequest{
			Path: path, Kind: protocol.KindGemini, Model: model, Stream: &stream,
			GeminiArray: !strings.EqualFold(c.Query("alt"), "sse"),
		})
	case "counttokens":
		rt.handleGeminiCountTokens(c, model)
	case "embedcontent", "batchembedcontents":
		rt.handleForward(c, forwardRequest{Path: path, Kind: protocol.KindGemini, Model: model})
	default:
		writeError(c, protocol.KindGemini, newGatewayError(http.StatusNotFound, "not_found_error", fmt.Sprintf("unsupported action %q", action)))
	}
}

// HandleGeminiCachedContents /v1beta/cachedContents[/:id]：透传给 Gemini 上游，按 id 回到创建它的路由。
func (rt *Runtime) HandleGeminiCachedContents(c *gin.Context) {
	id := strings.TrimPrefix(c.Param("id"), "/")
	path := "/v1beta/cachedContents"
	if id != "" {
		if !geminiModelSegment.MatchString(id) {
			writeError(c, protocol.KindGemini, newGatewayError(http.StatusBadRequest, "invalid_request_error", "invalid cached content id"))
			return
		}
		path += "/" + id
	}
	rt.handleForward(c, forwardRequest{Path: path, Kind: protocol.KindGemini, TaskID: id})
}

// handleGeminiCountTokens 依次问 Gemini 上游的 countTokens；都不行时按字符粗估。
func (rt *Runtime) handleGeminiCountTokens(c *gin.Context, model string) {
	auth, err := rt.Authenticate(c)
	if err != nil {
		writeError(c, protocol.KindGemini, authError(err.Error()))
		return
	}
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		writeError(c, protocol.KindGemini, newGatewayError(http.StatusBadRequest, "invalid_request_error", "failed to read body"))
		return
	}
	routes, _ := rt.cachedRoutes(auth.Group.ID)
	groupMapping := cachedModelMapping(auth.Group.ModelMappingJSON)
	mapped, _ := ResolveModel(model, groupMapping)
	routes = filterRoutesForModel(routes, model, mapped)
	groupsByChannel := rt.loadGroupsByChannel(c.Request.Context(), routes)
	cands := orderCandidates(routes, groupsByChannel, auth.Group, time.Now(), nil, routePreferences{
		ModelRouteIDs: rt.modelRouteIDs(auth.Group, routes, model),
	})
	for _, cand := range cands {
		route := cand.Route
		upstreamModel, _ := ResolveModel(model, cachedModelMapping(route.ModelMappingJSON), groupMapping)
		if upstreamModel == "" {
			upstreamModel = model
		}
		target, rerr := rt.resolveUpstreamTarget(&route)
		if rerr != nil {
			continue
		}
		if routeUpstreamKind(&route, target, protocol.KindGemini, upstreamModel) != protocol.KindGemini {
			continue
		}
		applyRouteUserAgent(target, auth.Group, &route)
		status, _, respBody, _, ferr := rt.forwardOnce(c.Request.Context(), upstreamCall{
			target: target, path: "/v1beta/models/" + upstreamModel + ":countTokens", method: http.MethodPost,
			header: c.Request.Header, body: body, kind: protocol.KindGemini,
		})
		if ferr == nil && status >= 200 && status < 300 && len(respBody) > 0 {
			c.Data(status, "application/json", respBody)
			return
		}
	}
	c.JSON(http.StatusOK, gin.H{"totalTokens": estimateTokenCount(body)})
}

// routeUpstreamKind 路由（含直连 provider 默认）解析出的上游协议。
func routeUpstreamKind(route *storage.GatewayRoute, target *upstreamTarget, inbound protocolKind, upstreamModel string) protocolKind {
	routeProto := normalizeUpstreamProtocol(route.UpstreamProtocol)
	if route.NormalizeSourceKind() == storage.GatewayRouteSourceProvider &&
		routeProto == storage.GatewayUpstreamProtocolAuto && target != nil && target.Provider != nil {
		if p := normalizeProviderProtocol(target.Provider.UpstreamProtocol); p != storage.GatewayUpstreamProtocolAuto {
			routeProto = p
		}
	}
	return protocol.ResolveUpstream(routeProto, inbound, upstreamModel)
}

// geminiPassthroughCapable 路由能否接 Gemini 原生透传（embedContent / cachedContents 等）：
// 协议显式为 gemini（含直连渠道的默认协议），或为 auto 且模型像 Gemini 系。
// 不带模型的透传（如 GET cachedContents）交给 auto 路由碰运气。
func (rt *Runtime) geminiPassthroughCapable(route *storage.GatewayRoute, upstreamModel string) bool {
	proto := normalizeUpstreamProtocol(route.UpstreamProtocol)
	if proto == storage.GatewayUpstreamProtocolAuto &&
		route.NormalizeSourceKind() == storage.GatewayRouteSourceProvider &&
		rt.Providers != nil && route.GatewayProviderID > 0 {
		if p, err := rt.cachedProvider(route.GatewayProviderID); err == nil && p != nil {
			if pp := normalizeProviderProtocol(p.UpstreamProtocol); pp != storage.GatewayUpstreamProtocolAuto {
				proto = pp
			}
		}
	}
	switch proto {
	case storage.GatewayUpstreamProtocolGemini:
		return true
	case storage.GatewayUpstreamProtocolAuto:
		return upstreamModel == "" || protocol.LooksLikeGeminiModel(upstreamModel)
	}
	return false
}

// HandleGeminiModels GET /v1beta/models 与 /v1beta/models/{model}
func (rt *Runtime) HandleGeminiModels(c *gin.Context) {
	_ = ensureGatewayRequestID(c)
	auth, err := rt.Authenticate(c)
	if err != nil {
		writeError(c, protocol.KindGemini, authError(err.Error()))
		return
	}
	ids, err := rt.groupModelIDs(c, auth.Group)
	if err != nil {
		writeError(c, protocol.KindGemini, newGatewayError(http.StatusInternalServerError, "api_error", err.Error()))
		return
	}
	describe := func(id string) gin.H {
		return gin.H{
			"name":                       "models/" + id,
			"baseModelId":                id,
			"version":                    "001",
			"displayName":                id,
			"supportedGenerationMethods": []string{"generateContent", "streamGenerateContent", "countTokens"},
		}
	}
	if want, _ := parseGeminiModelAction(c.Param("model")); want != "" {
		for _, id := range ids {
			if id == want {
				c.JSON(http.StatusOK, describe(id))
				return
			}
		}
		writeError(c, protocol.KindGemini, newGatewayError(http.StatusNotFound, "not_found_error", fmt.Sprintf("model %q not found", want)))
		return
	}
	models := make([]gin.H, 0, len(ids))
	for _, id := range ids {
		models = append(models, describe(id))
	}
	c.JSON(http.StatusOK, gin.H{"models": models})
}
