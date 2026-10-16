// 数据面：网关密钥鉴权。
package gateway

import (
	"errors"
	"strings"

	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

// Authenticate 鉴权并返回 AuthResult。
func (rt *Runtime) Authenticate(c *gin.Context) (*AuthResult, error) {
	raw := strings.TrimSpace(c.GetHeader("Authorization"))
	if strings.HasPrefix(strings.ToLower(raw), "bearer ") {
		raw = strings.TrimSpace(raw[7:])
	} else {
		raw = strings.TrimSpace(c.GetHeader("x-api-key"))
	}
	if raw == "" {
		// Gemini SDK / CLI 用 x-goog-api-key
		raw = strings.TrimSpace(c.GetHeader("x-goog-api-key"))
	}
	if raw == "" && isGeminiNativePath(c.Request.URL.Path) {
		// 与 Google API 一致：/v1beta 下允许 ?key=（其它路径不接受查询串里的密钥，避免泄露进日志）
		raw = strings.TrimSpace(c.Query("key"))
	}
	if raw == "" {
		return nil, errors.New("missing api key")
	}
	key, err := rt.cachedKeyByHash(HashAPIKey(raw))
	if err != nil {
		return nil, errors.New("invalid api key")
	}
	if key.Status != storage.GatewayKeyStatusActive {
		return nil, errors.New("api key disabled")
	}
	if err := rt.checkIPAccess(c.ClientIP(), key.IPWhitelistJSON, key.IPBlacklistJSON); err != nil {
		return nil, err
	}
	// 已用额度 = 库里的 + 后台还没写回的增量
	if key.Quota > 0 && key.QuotaUsed+rt.pendingKeyQuota(key.ID) >= key.Quota {
		return nil, errors.New("api key quota exceeded")
	}
	group, err := rt.cachedGroup(key.GroupID)
	if err != nil {
		return nil, errors.New("gateway group not found")
	}
	if group.Status != storage.GatewayGroupStatusActive {
		return nil, errors.New("gateway group disabled")
	}
	return &AuthResult{Key: key, Group: group}, nil
}

// isGeminiNativePath Gemini 原生端点（/v1beta、/antigravity/v1beta）。
func isGeminiNativePath(path string) bool {
	return strings.HasPrefix(path, "/v1beta/") || path == "/v1beta" ||
		strings.HasPrefix(path, "/antigravity/v1beta/") || path == "/antigravity/v1beta"
}
