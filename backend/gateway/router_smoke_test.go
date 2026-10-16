package gateway

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// 路由表能完整注册（gin 对静态段 / 参数段冲突会 panic），且关键端点都能命中。
func TestRegisterPublicRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	RegisterPublicWithoutRoot(r, &Service{})
	want := map[string]bool{}
	for _, ri := range r.Routes() {
		want[ri.Method+" "+ri.Path] = true
	}
	for _, route := range []string{
		"GET /v1/models/:model",
		"GET /models",
		"POST /messages/count_tokens",
		"POST /v1/images/generations/async",
		"GET /v1/images/tasks/:task_id",
		"POST /v1/videos",
		"GET /v1/videos/:request_id/content",
		"GET /v1/videos/generations/:request_id",
		"POST /v1/tts",
		"DELETE /v1/custom-voices/:voice_id",
		"POST /v1/web_search",
		"POST /api/v3/contents/generations/tasks",
		"GET /contents/generations/tasks/:task_id",
		"POST /v1beta/cachedContents",
		"POST /api/event_logging/batch",
		"POST /v1/audio/transcriptions",
	} {
		if !want[route] {
			t.Errorf("route %s not registered", route)
		}
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/event_logging/batch", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("event logging sink status %d", w.Code)
	}
}
