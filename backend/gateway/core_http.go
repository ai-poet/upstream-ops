// 共用：运行时配置、代理、超时与按目标构建 HTTP Client，拉上游模型列表。
package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/connector"
	"github.com/bejix/upstream-ops/backend/storage"
)

func (svc *core) gatewayRuntime() config.GatewayConfig {
	svc.mu.RLock()
	cfg, ready := svc.gatewayCfg, svc.gatewayCfgReady
	svc.mu.RUnlock()
	if ready {
		return cfg
	}
	return cfg.WithDefaults()
}

func (svc *core) proxyURLForChannel(ch *storage.Channel) string {
	return svc.proxyURLForTarget(ch, nil)
}

// proxyURLForTarget 监控渠道或直连 Provider 任一方开启 proxy_enabled，且全局代理启用时返回代理 URL。

func (svc *core) proxyURLForTarget(ch *storage.Channel, provider *storage.GatewayProvider) string {
	svc.mu.RLock()
	pc := svc.proxyConfig
	svc.mu.RUnlock()
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

func (svc *core) httpClientForChannel(ch *storage.Channel) *http.Client {
	return svc.httpClientForTarget(ch, nil)
}

// httpClientForTarget 转发用的 HTTP 客户端：按「代理 + 转发超时」复用，同一上游的连接（含 HTTP/2）在请求间共享。
// 以前每个请求新建 Transport，每次都要重新 TCP + TLS 握手，用完的空闲连接还要挂 90 秒才释放。
func (svc *core) httpClientForTarget(ch *storage.Channel, provider *storage.GatewayProvider) *http.Client {
	// 网关转发超时由 gateway.forwardTimeoutSeconds 控制（与监控上游 timeout 分离）。
	timeout := svc.gatewayRuntime().ForwardTimeout()
	proxy := svc.proxyURLForTarget(ch, provider)
	key := proxy + "|" + timeout.String()

	svc.upstreamClientsMu.Lock()
	defer svc.upstreamClientsMu.Unlock()
	if c := svc.upstreamClients[key]; c != nil {
		return c
	}
	if svc.upstreamClients == nil {
		svc.upstreamClients = map[string]*http.Client{}
	}
	c := newUpstreamClient(proxy, timeout)
	svc.upstreamClients[key] = c
	return c
}

const (
	upstreamHTTP2PingInterval = 15 * time.Second
	upstreamHTTP2PingTimeout  = 15 * time.Second
)

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
	// HTTP/2 连接健康探测（sub2api http_upstream.go enableHTTP2KeepAlive）：连接空闲 15 秒发 PING，
	// 15 秒没回就关掉。被代理 / NAT 静默掐断的死连接不再被新请求复用，请求不会挂到 TCP 重传超时。
	// 用 sub2api 给 OpenAI 中转站的 15s/15s，而不是长流的 10s/5s：中转站回 PING 慢，太短会误判。
	transport.HTTP2 = &http.HTTP2Config{
		SendPingTimeout: upstreamHTTP2PingInterval,
		PingTimeout:     upstreamHTTP2PingTimeout,
	}
	if proxy != "" {
		if u, err := url.Parse(proxy); err == nil {
			transport.Proxy = http.ProxyURL(u)
		}
	}
	return &http.Client{Timeout: timeout, Transport: transport}
}

// websocketHTTPClient 给 WebSocket 拨号用的 client：coder/websocket 要求 Timeout 为 0（由 context 控制），
// 返回共享 client 的副本（同一个 Transport，连接池照旧复用），不改共享 client 本身。
func websocketHTTPClient(base *http.Client) *http.Client {
	c := *base
	c.Timeout = 0
	return &c
}

// resetUpstreamClients 代理或转发超时变了：丢掉缓存的客户端并关掉它们的空闲连接。
func (svc *core) resetUpstreamClients() {
	svc.upstreamClientsMu.Lock()
	old := svc.upstreamClients
	svc.upstreamClients = nil
	svc.upstreamClientsMu.Unlock()
	for _, c := range old {
		c.CloseIdleConnections()
	}
}

// ---------- key helpers ----------

// upstreamModelsTimeout 拉上游 /v1/models 的超时（只作用于这一次请求的 context）。
var upstreamModelsTimeout = 30 * time.Second

// fetchUpstreamModels 拉取上游 /v1/models。
// userAgent 为组+路由解析结果；空则回落默认 UA（无客户端可透传）。
//
// 用的是和转发共享的 client（连接池复用），所以超时只能加在本次请求的 context 上，
// 绝不能改 client.Timeout：那会让之后所有经它转发的流在 30 秒处被掐断。
func (svc *core) fetchUpstreamModels(ctx context.Context, ch *storage.Channel, apiKey, userAgent string) ([]string, error) {
	client := svc.httpClientForChannel(ch)
	ctx, cancel := context.WithTimeout(ctx, upstreamModelsTimeout)
	defer cancel()
	url := strings.TrimRight(ch.SiteURL, "/") + "/v1/models"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build GET %s: %w", url, err)
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", withDefaultUserAgent(userAgent, svc.defaultUpstreamUserAgent()))
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("GET %s: read body: %w", url, err)
	}
	if resp.StatusCode >= 400 {
		// 保留 URL + 状态码 + 上游 body 摘要，便于模型同步结果页排查（如 base_url 多写/少写 /v1）
		return nil, fmt.Errorf("GET %s: %w", url, connector.HTTPStatusError(resp.StatusCode, body))
	}
	var parsed struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		snippet := svc.extractUpstreamErrorSnippet(body)
		if snippet == "" {
			snippet = truncateRunes(string(body), 200)
		}
		if snippet == "" {
			return nil, fmt.Errorf("GET %s: HTTP %d invalid models JSON: %w", url, resp.StatusCode, err)
		}
		return nil, fmt.Errorf("GET %s: HTTP %d invalid models JSON (%v): %s", url, resp.StatusCode, err, snippet)
	}
	out := make([]string, 0, len(parsed.Data))
	for _, d := range parsed.Data {
		if strings.TrimSpace(d.ID) != "" {
			out = append(out, d.ID)
		}
	}
	return out, nil
}
