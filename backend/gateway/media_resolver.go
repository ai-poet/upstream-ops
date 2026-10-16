// relaykit 跨协议转换图片 / 文件时的媒体解析：下载 URL 或解析 data URL，返回 base64。
//
// 下载由网关代客户端发起，URL 来自请求体，必须防 SSRF：只允许 http(s)，
// 拨号时拒绝回环 / 私网 / 链路本地 / 组播等地址（DNS 重绑定也在拨号阶段拦下），
// 限制大小与耗时。
package gateway

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/QuantumNous/new-api/relaykit/relayconvert"
	"github.com/QuantumNous/new-api/relaykit/types"
)

const (
	mediaMaxBytes = 20 << 20
	mediaTimeout  = 30 * time.Second
	// 单次客户端请求里最多下载的媒体链接数与总字节数（跨所有上游尝试共享）
	mediaMaxDownloadsPerRequest = 16
	mediaMaxBytesPerRequest     = 50 << 20
)

// mediaFetchState 一次客户端请求的媒体下载状态：同一 URL 只下载一次（换路由重试时复用），
// 限制下载数与总字节，并记下第一个下载失败，供转发循环直接回 400 而不是逐条路由重试。
type mediaFetchState struct {
	mu        sync.Mutex
	results   map[string]mediaResult
	downloads int
	bytes     int64
	failure   error
}

type mediaResult struct {
	data, mime string
	err        error
}

type mediaFetchCtxKey struct{}

// withMediaFetchState 给一次客户端请求挂上媒体下载状态。
func withMediaFetchState(ctx context.Context) context.Context {
	return context.WithValue(ctx, mediaFetchCtxKey{}, &mediaFetchState{results: map[string]mediaResult{}})
}

func mediaFetchStateFrom(ctx context.Context) *mediaFetchState {
	if ctx == nil {
		return nil
	}
	s, _ := ctx.Value(mediaFetchCtxKey{}).(*mediaFetchState)
	return s
}

// mediaFetchFailure 本次请求里第一个媒体下载失败（没有返回 nil）。
func mediaFetchFailure(ctx context.Context) error {
	s := mediaFetchStateFrom(ctx)
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failure
}

var registerMediaResolverOnce sync.Once

func registerMediaResolver() {
	registerMediaResolverOnce.Do(func() {
		relayconvert.SetMediaResolver(relayconvert.MediaResolver{
			GetBase64Data:        resolveMediaBase64,
			DecodeBase64FileData: decodeDataURL,
		})
	})
}

func init() { registerMediaResolver() }

var errBlockedAddress = errors.New("media url resolves to a non-public address")

var mediaHTTPClient = &http.Client{
	Timeout: mediaTimeout,
	Transport: &http.Transport{
		Proxy: nil,
		DialContext: (&net.Dialer{
			Timeout: 10 * time.Second,
			Control: func(_, address string, _ syscall.RawConn) error {
				host, _, err := net.SplitHostPort(address)
				if err != nil {
					return err
				}
				ip := net.ParseIP(host)
				if ip == nil || !isPublicIP(ip) {
					return errBlockedAddress
				}
				return nil
			},
		}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
		MaxIdleConns:          16,
		IdleConnTimeout:       30 * time.Second,
	},
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 3 {
			return errors.New("too many redirects")
		}
		if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
			return fmt.Errorf("redirect to unsupported scheme %q", req.URL.Scheme)
		}
		return nil
	},
}

func isPublicIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsInterfaceLocalMulticast() || ip.IsMulticast() {
		return false
	}
	// 100.64.0.0/10（运营商 NAT）与 IPv4 映射的保留段
	if v4 := ip.To4(); v4 != nil {
		if v4[0] == 100 && v4[1]&0xc0 == 64 {
			return false
		}
		if v4[0] == 0 || v4[0] >= 240 {
			return false
		}
		return true
	}
	// 能把流量转进 IPv4 内网的 IPv6 段（NAT64 / 6to4）与废弃的站点本地地址
	for _, n := range blockedIPv6Nets {
		if n.Contains(ip) {
			return false
		}
	}
	return true
}

var blockedIPv6Nets = func() []*net.IPNet {
	var out []*net.IPNet
	for _, cidr := range []string{"64:ff9b::/96", "64:ff9b:1::/48", "2002::/16", "fec0::/10"} {
		_, n, _ := net.ParseCIDR(cidr)
		out = append(out, n)
	}
	return out
}()

// resolveMediaBase64 返回 (base64 数据, MIME 类型)。
func resolveMediaBase64(ctx context.Context, source types.FileSource, _ ...string) (string, string, error) {
	if source == nil {
		return "", "", errors.New("media source is nil")
	}
	raw := strings.TrimSpace(source.GetRawData())
	if !source.IsURL() {
		if strings.HasPrefix(raw, "data:") {
			mt, data, err := decodeDataURL(raw)
			return data, mt, err
		}
		mt := ""
		if b, ok := source.(*types.Base64Source); ok {
			mt = b.MimeType
		}
		if mt == "" {
			mt = sniffBase64MimeType(raw)
		}
		return raw, mt, nil
	}
	if strings.HasPrefix(raw, "data:") {
		mt, data, err := decodeDataURL(raw)
		return data, mt, err
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", "", fmt.Errorf("unsupported media url %q", raw)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	state := mediaFetchStateFrom(ctx)
	if state == nil {
		// 没有请求级状态（如后台探测）：单次下载，只受单个文件上限约束
		return downloadMedia(ctx, u, mediaMaxBytes)
	}
	key := u.String()
	state.mu.Lock()
	defer state.mu.Unlock()
	if r, ok := state.results[key]; ok {
		return r.data, r.mime, r.err
	}
	var r mediaResult
	switch {
	case state.downloads >= mediaMaxDownloadsPerRequest:
		r.err = fmt.Errorf("too many media urls in one request (max %d)", mediaMaxDownloadsPerRequest)
	case state.bytes >= mediaMaxBytesPerRequest:
		r.err = fmt.Errorf("media in one request exceeds %d bytes", mediaMaxBytesPerRequest)
	default:
		state.downloads++
		limit := min(int64(mediaMaxBytes), mediaMaxBytesPerRequest-state.bytes)
		r.data, r.mime, r.err = downloadMedia(ctx, u, limit)
		// base64 长度换算回原始字节数计入预算
		state.bytes += int64(len(r.data)) * 3 / 4
	}
	if r.err != nil && state.failure == nil {
		state.failure = r.err
	}
	state.results[key] = r
	return r.data, r.mime, r.err
}

// downloadMedia 下载一个媒体 URL，最多读 limit 字节，返回 (base64 数据, MIME 类型)。
func downloadMedia(ctx context.Context, u *url.URL, limit int64) (string, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", "", err
	}
	resp, err := mediaHTTPClient.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("download media: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("download media: HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return "", "", fmt.Errorf("download media: %w", err)
	}
	if int64(len(data)) > limit {
		return "", "", fmt.Errorf("media larger than %d bytes", limit)
	}
	mt, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if mt == "" || mt == "application/octet-stream" || mt == "binary/octet-stream" {
		mt = http.DetectContentType(data)
		if i := strings.Index(mt, ";"); i >= 0 {
			mt = mt[:i]
		}
	}
	return base64.StdEncoding.EncodeToString(data), mt, nil
}

// decodeDataURL 解析 data:<mime>;base64,<data>，返回 (MIME 类型, base64 数据)。
// 没有 data: 前缀时按裸 base64 处理并嗅探类型。
func decodeDataURL(s string) (string, string, error) {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "data:") {
		if s == "" {
			return "", "", errors.New("empty base64 data")
		}
		return sniffBase64MimeType(s), s, nil
	}
	meta, data, ok := strings.Cut(strings.TrimPrefix(s, "data:"), ",")
	if !ok {
		return "", "", errors.New("malformed data url")
	}
	mt := meta
	if i := strings.Index(meta, ";"); i >= 0 {
		mt = meta[:i]
	}
	if mt == "" {
		mt = sniffBase64MimeType(data)
	}
	if !strings.Contains(meta, ";base64") {
		// 非 base64 的 data URL（URL 编码文本）
		text, err := url.PathUnescape(data)
		if err != nil {
			return "", "", err
		}
		return mt, base64.StdEncoding.EncodeToString([]byte(text)), nil
	}
	return mt, data, nil
}

func sniffBase64MimeType(b64 string) string {
	head := b64
	if len(head) > 1024 {
		head = head[:1024]
	}
	head = head[:len(head)-len(head)%4]
	raw, err := base64.StdEncoding.DecodeString(head)
	if err != nil || len(raw) == 0 {
		return "application/octet-stream"
	}
	mt := http.DetectContentType(raw)
	if i := strings.Index(mt, ";"); i >= 0 {
		mt = mt[:i]
	}
	return mt
}
