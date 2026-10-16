// 直连渠道协议、鉴权样式归一化与密钥提示。
package gateway

import (
	"strings"

	"github.com/bejix/upstream-ops/backend/storage"
)

func normalizeUpstreamProtocol(v string) string {
	up := strings.ToLower(strings.TrimSpace(v))
	switch up {
	case storage.GatewayUpstreamProtocolOpenAI,
		"chat", "chat_completions":
		// 历史 openai / 别名 → 明确的 chat
		return storage.GatewayUpstreamProtocolOpenAIChat
	case storage.GatewayUpstreamProtocolOpenAIChat,
		storage.GatewayUpstreamProtocolOpenAIResponses,
		storage.GatewayUpstreamProtocolAnthropic,
		storage.GatewayUpstreamProtocolGemini:
		return up
	case "responses":
		return storage.GatewayUpstreamProtocolOpenAIResponses
	case "claude":
		return storage.GatewayUpstreamProtocolAnthropic
	case "google":
		return storage.GatewayUpstreamProtocolGemini
	default:
		return storage.GatewayUpstreamProtocolAuto
	}
}

func normalizeProviderProtocol(v string) string {
	return normalizeUpstreamProtocol(v)
}

func normalizeProviderAuthStyle(v string) string {
	s := strings.ToLower(strings.TrimSpace(v))
	switch s {
	case storage.GatewayProviderAuthBearer, storage.GatewayProviderAuthXAPIKey:
		return s
	default:
		return storage.GatewayProviderAuthBoth
	}
}

func apiKeyHint(secret string) string {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return ""
	}
	if len(secret) <= 8 {
		return secret[:1] + "…"
	}
	return secret[:4] + "…" + secret[len(secret)-4:]
}
