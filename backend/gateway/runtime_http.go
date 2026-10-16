// 数据面：代理、超时与按目标构建 HTTP Client。
package gateway

import (
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/storage"
)

func (rt *Runtime) gatewayRuntime() config.GatewayConfig {
	rt.mu.RLock()
	cfg, ready := rt.gatewayCfg, rt.gatewayCfgReady
	rt.mu.RUnlock()
	if ready {
		return cfg
	}
	return cfg.WithDefaults()
}

func (rt *Runtime) proxyURLForChannel(ch *storage.Channel) string {
	return rt.proxyURLForTarget(ch, nil)
}

// proxyURLForTarget 监控渠道或直连 Provider 任一方开启 proxy_enabled，且全局代理启用时返回代理 URL。

func (rt *Runtime) proxyURLForTarget(ch *storage.Channel, provider *storage.GatewayProvider) string {
	rt.mu.RLock()
	pc := rt.proxyConfig
	rt.mu.RUnlock()
	if !pc.Enabled {
		return ""
	}
	useProxy := false
	if ch != nil && ch.ProxyEnabled {
		useProxy = true
	}
	if provider != nil && provider.ProxyEnabled {
		useProxy = true
	}
	if !useProxy {
		return ""
	}
	u, err := pc.ActiveURL()
	if err != nil {
		return ""
	}
	return u
}

func (rt *Runtime) httpClientForChannel(ch *storage.Channel) *http.Client {
	return rt.httpClientForTarget(ch, nil)
}

// httpClientForTarget 转发用的 HTTP 客户端：按「代理 + 转发超时」复用，同一上游的连接（含 HTTP/2）在请求间共享。
// 以前每个请求新建 Transport，每次都要重新 TCP + TLS 握手，用完的空闲连接还要挂 90 秒才释放。
func (rt *Runtime) httpClientForTarget(ch *storage.Channel, provider *storage.GatewayProvider) *http.Client {
	// 网关转发超时由 gateway.forwardTimeoutSeconds 控制（与监控上游 timeout 分离）。
	timeout := rt.gatewayRuntime().ForwardTimeout()
	proxy := rt.proxyURLForTarget(ch, provider)
	key := proxy + "|" + timeout.String()

	rt.upstreamClientsMu.Lock()
	defer rt.upstreamClientsMu.Unlock()
	if c := rt.upstreamClients[key]; c != nil {
		return c
	}
	if rt.upstreamClients == nil {
		rt.upstreamClients = map[string]*http.Client{}
	}
	c := newUpstreamClient(proxy, timeout)
	rt.upstreamClients[key] = c
	return c
}

func newUpstreamClient(proxy string, timeout time.Duration) *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		// 同一上游的并发连接都留着复用（标准库默认每个 host 只留 2 个空闲连接）
		MaxIdleConns:          1000,
		MaxIdleConnsPerHost:   200,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
	}
	if proxy != "" {
		if u, err := url.Parse(proxy); err == nil {
			transport.Proxy = http.ProxyURL(u)
		}
	}
	return &http.Client{Timeout: timeout, Transport: transport}
}

// resetUpstreamClients 代理或转发超时变了：丢掉缓存的客户端并关掉它们的空闲连接。
func (s *Service) resetUpstreamClients() {
	s.upstreamClientsMu.Lock()
	old := s.upstreamClients
	s.upstreamClients = nil
	s.upstreamClientsMu.Unlock()
	for _, c := range old {
		c.CloseIdleConnections()
	}
}

// ---------- key helpers ----------
