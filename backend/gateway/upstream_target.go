// 共用：上游转发目标（baseURL / key / channel / provider）与按路由解析。
package gateway

import (
	"errors"
	"fmt"
	"strings"

	"github.com/bejix/upstream-ops/backend/storage"
)

type upstreamTarget struct {
	BaseURL  string
	APIKey   string
	Channel  *storage.Channel
	Provider *storage.GatewayProvider
	// UserAgentOverride 非空时覆盖发往上游的 User-Agent（组+路由策略解析结果）。
	UserAgentOverride string
}

// resolveUpstreamTarget 路由对应的上游地址与密钥（直连 provider 或监控渠道）。
func (svc *core) resolveUpstreamTarget(route *storage.GatewayRoute) (*upstreamTarget, error) {
	if route == nil {
		return nil, errors.New("route is nil")
	}
	if route.NormalizeSourceKind() == storage.GatewayRouteSourceProvider {
		if svc.Providers == nil {
			return nil, errors.New("providers not configured")
		}
		if route.GatewayProviderID == 0 {
			return nil, errors.New("gateway_provider_id is required")
		}
		p, err := svc.cachedProvider(route.GatewayProviderID)
		if err != nil {
			return nil, fmt.Errorf("provider not found: %w", err)
		}
		if !p.Enabled {
			return nil, fmt.Errorf("provider %q is disabled", p.Name)
		}
		secret, err := svc.Cipher.Decrypt(p.APIKeyCipher)
		if err != nil || strings.TrimSpace(secret) == "" {
			return nil, errors.New("provider api key missing or decrypt failed")
		}
		return &upstreamTarget{
			BaseURL:  strings.TrimRight(p.BaseURL, "/"),
			APIKey:   secret,
			Provider: p,
		}, nil
	}
	ch, err := svc.cachedChannel(route.SourceChannelID)
	if err != nil {
		return nil, fmt.Errorf("channel not found: %w", err)
	}
	secret, err := svc.Cipher.Decrypt(route.SourceAPIKeyCipher)
	if err != nil || strings.TrimSpace(secret) == "" {
		return nil, errors.New("missing upstream api key; run ensure-keys")
	}
	return &upstreamTarget{
		BaseURL: strings.TrimRight(ch.SiteURL, "/"),
		APIKey:  secret,
		Channel: ch,
	}, nil
}
