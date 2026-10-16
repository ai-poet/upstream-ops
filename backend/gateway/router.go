package gateway

import (
	"net/http"
	"net/url"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/gin-gonic/gin"
)

// endpointsPayload 对齐 CLIProxyAPI / sub2api 的服务说明形态。
// 注意：不要占用 GET /gateway —— 那是前端 SPA 管理页路由。
func endpointsPayload() gin.H {
	return gin.H{
		"message": "UpstreamOps Gateway",
		"endpoints": []string{
			// OpenAI Chat
			"POST /v1/chat/completions",
			"POST /chat/completions",
			"POST /v1/completions",
			// OpenAI Responses
			"POST /v1/responses",
			"POST /v1/responses/*",
			"GET  /v1/responses (WebSocket)",
			"POST /responses",
			"POST /responses/*",
			"GET  /responses (WebSocket)",
			// Anthropic
			"POST /v1/messages",
			"POST /v1/messages/count_tokens",
			"POST /messages/count_tokens",
			// Models / Usage
			"GET  /v1/models",
			"GET  /v1/models/:model",
			"GET  /models",
			"GET  /v1/usage",
			// Embeddings / Moderations / Rerank / Audio
			"POST /v1/embeddings",
			"POST /v1/moderations",
			"POST /v1/rerank",
			"POST /v1/audio/speech",
			"POST /v1/audio/transcriptions",
			"POST /v1/audio/translations",
			// Images
			"POST /v1/images/generations",
			"POST /v1/images/edits",
			"POST /v1/images/generations/async",
			"POST /v1/images/edits/async",
			"GET  /v1/images/tasks/:task_id",
			"POST /v1/images/batches",
			// Videos
			"POST /v1/videos",
			"POST /v1/videos/generations",
			"POST /v1/videos/edits",
			"POST /v1/videos/extensions",
			"GET  /v1/videos/:request_id",
			"GET  /v1/videos/:request_id/content",
			// Seedance / Ark
			"POST /api/v3/contents/generations/tasks",
			"GET  /api/v3/contents/generations/tasks/:task_id",
			// Voice / Search
			"POST /v1/tts",
			"POST /v1/stt",
			"GET  /v1/custom-voices",
			"POST /v1/web_search",
			"POST /v1/x_search",
			"POST /v1/alpha/search",
			// Codex
			"POST /backend-api/codex/responses",
			"POST /backend-api/codex/responses/*",
			"GET  /backend-api/codex/responses (WebSocket)",
			"GET  /backend-api/codex/models",
			// Gemini
			"GET  /v1beta/models",
			"GET  /v1beta/models/:model",
			"POST /v1beta/models/{model}:generateContent",
			"POST /v1beta/models/{model}:streamGenerateContent",
			"POST /v1beta/models/{model}:countTokens",
			"POST /v1beta/models/{model}:embedContent",
			"*    /v1beta/cachedContents",
			// Antigravity
			"POST /antigravity/v1/messages",
			"GET  /antigravity/v1/models",
		},
	}
}

// RegisterPublic 注册网关公开路由（不走管理端鉴权）。
func RegisterPublic(r *gin.Engine, s *Service) {
	r.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, endpointsPayload())
	})
	r.GET("/v1", func(c *gin.Context) {
		c.JSON(http.StatusOK, endpointsPayload())
	})
	registerAllGatewayRoutes(r, s)
}

// RegisterPublicWithoutRoot 注册网关路由但不占用 GET /（有 SPA 时使用）。
func RegisterPublicWithoutRoot(r *gin.Engine, s *Service) {
	r.GET("/v1", func(c *gin.Context) {
		c.JSON(http.StatusOK, endpointsPayload())
	})
	registerAllGatewayRoutes(r, s)
}

func registerAllGatewayRoutes(r *gin.Engine, s *Service) {
	rt := s.runtime()
	chat, resp, anth := protocol.KindOpenAIChat, protocol.KindOpenAIResponses, protocol.KindAnthropic

	// fwd 固定上游 path 的转发
	fwd := func(path string, kind protocolKind) gin.HandlerFunc {
		return func(c *gin.Context) { rt.HandleForward(c, path, kind) }
	}
	// task 带任务 id 的查询 / 下载 / 取消：上游 path 由 build 拼出（参数经 pathParam 转义），按 id 回到创建任务的路由
	task := func(param string, build func(c *gin.Context) string) gin.HandlerFunc {
		return func(c *gin.Context) {
			for _, p := range c.Params {
				if !validTaskPathParam(p.Value) {
					writeError(c, chat, newGatewayError(http.StatusBadRequest, "invalid_request_error", "invalid "+p.Key))
					return
				}
			}
			rt.HandleTaskForward(c, build(c), chat, c.Param(param))
		}
	}

	// 无 /v1 前缀的别名与 /v1 共用一张表（sub2api 同款）。
	for _, prefix := range []string{"/v1", ""} {
		g := r.Group(prefix)

		// Models / Usage
		g.GET("/models", rt.HandleModels)
		g.GET("/models/:model", rt.HandleModel)

		// OpenAI Chat / Responses
		g.POST("/chat/completions", fwd("/v1/chat/completions", chat))
		g.POST("/responses", fwd("/v1/responses", resp))
		g.POST("/responses/*subpath", func(c *gin.Context) {
			rt.HandleForward(c, "/v1/responses"+c.Param("subpath"), resp)
		})
		g.GET("/responses", rt.HandleResponsesWebSocket)

		// Anthropic count_tokens（sub2api 同样提供无前缀别名）
		g.POST("/messages/count_tokens", rt.HandleCountTokens)

		// 透传类
		g.POST("/embeddings", fwd("/v1/embeddings", chat))
		g.POST("/moderations", fwd("/v1/moderations", chat))
		g.POST("/rerank", fwd("/v1/rerank", chat))
		g.POST("/audio/speech", fwd("/v1/audio/speech", chat))
		g.POST("/audio/transcriptions", fwd("/v1/audio/transcriptions", chat))
		g.POST("/audio/translations", fwd("/v1/audio/translations", chat))
		g.POST("/alpha/search", fwd("/v1/alpha/search", chat))
		g.POST("/web_search", fwd("/v1/web_search", chat))
		g.POST("/x_search", fwd("/v1/x_search", chat))

		// Images（含异步任务）
		g.POST("/images/generations", fwd("/v1/images/generations", chat))
		g.POST("/images/edits", fwd("/v1/images/edits", chat))
		g.POST("/images/generations/async", fwd("/v1/images/generations/async", chat))
		g.POST("/images/edits/async", fwd("/v1/images/edits/async", chat))
		g.GET("/images/tasks/:task_id", task("task_id", func(c *gin.Context) string {
			return "/v1/images/tasks/" + pathParam(c, "task_id")
		}))

		// Videos
		g.POST("/videos", fwd("/v1/videos", chat))
		g.POST("/videos/generations", fwd("/v1/videos/generations", chat))
		g.POST("/videos/edits", fwd("/v1/videos/edits", chat))
		g.POST("/videos/extensions", fwd("/v1/videos/extensions", chat))
		g.GET("/videos/:request_id", task("request_id", func(c *gin.Context) string {
			return "/v1/videos/" + pathParam(c, "request_id")
		}))
		g.GET("/videos/:request_id/content", task("request_id", func(c *gin.Context) string {
			return "/v1/videos/" + pathParam(c, "request_id") + "/content"
		}))
		for _, kind := range []string{"generations", "edits", "extensions"} {
			kind := kind
			g.GET("/videos/"+kind+"/:request_id", task("request_id", func(c *gin.Context) string {
				return "/v1/videos/" + kind + "/" + pathParam(c, "request_id")
			}))
			g.GET("/videos/"+kind+"/:request_id/content", task("request_id", func(c *gin.Context) string {
				return "/v1/videos/" + kind + "/" + pathParam(c, "request_id") + "/content"
			}))
		}

		// Voice
		g.POST("/tts", fwd("/v1/tts", chat))
		g.POST("/stt", fwd("/v1/stt", chat))
		g.GET("/custom-voices", fwd("/v1/custom-voices", chat))
		g.POST("/custom-voices", fwd("/v1/custom-voices", chat))
		voice := task("voice_id", func(c *gin.Context) string { return "/v1/custom-voices/" + pathParam(c, "voice_id") })
		g.GET("/custom-voices/:voice_id", voice)
		g.PATCH("/custom-voices/:voice_id", voice)
		g.DELETE("/custom-voices/:voice_id", voice)
		g.GET("/custom-voices/:voice_id/audio", task("voice_id", func(c *gin.Context) string {
			return "/v1/custom-voices/" + pathParam(c, "voice_id") + "/audio"
		}))
	}

	// ---------- 只有 /v1 前缀的端点 ----------
	v1 := r.Group("/v1")
	{
		v1.GET("/usage", rt.HandleUsage)
		v1.POST("/completions", fwd("/v1/completions", chat))
		v1.POST("/messages", fwd("/v1/messages", anth))

		// 批量图像（sub2api 也只开 /v1）
		v1.POST("/images/batches", fwd("/v1/images/batches", chat))
		v1.GET("/images/batches", fwd("/v1/images/batches", chat))
		v1.GET("/images/batches/models", fwd("/v1/images/batches/models", chat))
		batch := func(suffix string) gin.HandlerFunc {
			return task("id", func(c *gin.Context) string { return "/v1/images/batches/" + pathParam(c, "id") + suffix })
		}
		v1.GET("/images/batches/:id", batch(""))
		v1.GET("/images/batches/:id/items", batch("/items"))
		v1.GET("/images/batches/:id/items/:custom_id/content", task("id", func(c *gin.Context) string {
			return "/v1/images/batches/" + pathParam(c, "id") + "/items/" + pathParam(c, "custom_id") + "/content"
		}))
		v1.GET("/images/batches/:id/download", batch("/download"))
		v1.POST("/images/batches/:id/cancel", batch("/cancel"))
		v1.DELETE("/images/batches/:id", batch(""))
		v1.DELETE("/images/batches/:id/outputs", batch("/outputs"))
	}

	// ---------- Seedance / 火山方舟异步视频任务 ----------
	// 客户端四种前缀都收，上游一律用方舟的 /api/v3/...（base 已带 /api/v3 时由 joinUpstreamURL 去重）
	seedance := func(c *gin.Context) string { return seedanceTasksPath + "/" + pathParam(c, "task_id") }
	for _, prefix := range []string{"/api/v3", "/v3", "/v1", ""} {
		base := prefix + "/contents/generations/tasks"
		r.POST(base, fwd(seedanceTasksPath, chat))
		r.GET(base+"/:task_id", task("task_id", seedance))
		r.DELETE(base+"/:task_id", task("task_id", seedance))
	}

	// ---------- Claude Code 遥测：吞掉即可（sub2api 同款），不需要鉴权 ----------
	r.POST("/api/event_logging/batch", func(c *gin.Context) { c.Status(http.StatusOK) })

	// ---------- Codex ----------
	codex := r.Group("/backend-api/codex")
	{
		codex.POST("/responses", fwd("/v1/responses", resp))
		codex.POST("/responses/*subpath", func(c *gin.Context) {
			rt.HandleForward(c, "/v1/responses"+c.Param("subpath"), resp)
		})
		codex.GET("/responses", rt.HandleResponsesWebSocket)
		codex.POST("/alpha/search", fwd("/v1/alpha/search", chat))
		codex.GET("/models", rt.HandleModels)
	}

	// ---------- Gemini v1beta ----------
	for _, prefix := range []string{"/v1beta", "/antigravity/v1beta"} {
		g := r.Group(prefix)
		g.GET("/models", rt.HandleGeminiModels)
		g.GET("/models/*model", rt.HandleGeminiModels)
		g.POST("/models/*modelAction", rt.HandleGeminiGenerate)
	}
	cached := r.Group("/v1beta/cachedContents")
	{
		cached.POST("", rt.HandleGeminiCachedContents)
		cached.GET("", rt.HandleGeminiCachedContents)
		cached.GET("/:id", rt.HandleGeminiCachedContents)
		cached.PATCH("/:id", rt.HandleGeminiCachedContents)
		cached.DELETE("/:id", rt.HandleGeminiCachedContents)
	}

	// ---------- Antigravity ----------
	r.GET("/antigravity/models", rt.HandleModels)
	ag := r.Group("/antigravity/v1")
	{
		ag.POST("/messages", fwd("/v1/messages", anth))
		ag.POST("/messages/count_tokens", rt.HandleCountTokens)
		ag.GET("/models", rt.HandleModels)
		ag.GET("/usage", rt.HandleUsage)
	}
}

// seedanceTasksPath 方舟（Seedance）视频任务的上游路径。
const seedanceTasksPath = "/api/v3/contents/generations/tasks"

// pathParam 取路径参数并转义后拼进上游 path：gin 会把 %3F 解码成 "?"，原样拼接会变成上游的查询串。
func pathParam(c *gin.Context, name string) string {
	return url.PathEscape(c.Param(name))
}

// validTaskPathParam 任务 id 等路径参数不能为空，也不能是 "." / ".."（会被上游当成上一级路径）。
func validTaskPathParam(v string) bool {
	return v != "" && v != "." && v != ".."
}
