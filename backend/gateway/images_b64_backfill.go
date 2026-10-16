// 数据面：生图结果 url 转 base64（移植自 sub2api openai_images_b64_backfill.go）。
//
// 路由开启 images_url_to_b64_json 后，Images 端点（/v1/images/generations、/v1/images/edits）的非流式成功响应里，
// b64_json 缺失或为空、但带 url 的图片项，由网关下载 url 内容并以 base64 回填 b64_json（url 原样保留），
// 兼容只认 b64_json 的客户端（如 Codex）。客户端显式要求 response_format=url 时不回填；单项失败只记日志，原样返回。
//
// 下载防 SSRF：只允许 http(s)；拨号时拒绝回环 / 私网 / 链路本地等地址（含每一跳重定向与 DNS 重绑定，见 media_resolver.go）；
// 单张最多 20MB、60 秒；内容前 512 字节须嗅探为 png / jpeg / webp / gif，响应头不作为依据。
// 与 sub2api 的一处差别：sub2api 经账号代理下载，这里直连（代理场景下拨号目标是代理本身，无法按目标地址做私网拦截）。
package gateway

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	imageBackfillMaxBytes = 20 << 20
	imageBackfillTimeout  = 60 * time.Second
)

// imageBackfillClient 下载图片用的客户端（与跨协议媒体下载同一套私网拦截）；测试可替换。
var imageBackfillClient = mediaHTTPClient

var imageBackfillContentTypes = map[string]struct{}{
	"image/png": {}, "image/jpeg": {}, "image/webp": {}, "image/gif": {},
}

// isImagesResultEndpoint 同步返回图片结果的 Images 端点（异步任务 / 批量另有自己的响应形状）。
func isImagesResultEndpoint(path string) bool {
	return path == "/v1/images/generations" || path == "/v1/images/edits"
}

// clientWantsImageURL 客户端显式要求 response_format=url（JSON 或 multipart 表单）。
func clientWantsImageURL(contentType string, body []byte) bool {
	if isMultipartContentType(contentType) {
		return strings.EqualFold(strings.TrimSpace(multipartFieldValue(contentType, body, "response_format")), "url")
	}
	return strings.EqualFold(strings.TrimSpace(gjson.GetBytes(body, "response_format").String()), "url")
}

// backfillImagesB64JSON 对 Images 非流式响应做 url → b64_json 回填，返回（可能改写后的）响应体。
func (rt *Runtime) backfillImagesB64JSON(ctx context.Context, requestID string, body []byte) []byte {
	if len(body) == 0 || !gjson.ValidBytes(body) {
		return body
	}
	items := gjson.GetBytes(body, "data")
	if !items.IsArray() {
		return body
	}
	for index, item := range items.Array() {
		if !item.IsObject() || strings.TrimSpace(item.Get("b64_json").String()) != "" {
			continue
		}
		rawURL := strings.TrimSpace(item.Get("url").String())
		if rawURL == "" {
			continue
		}
		encoded, err := fetchImageURLBase64(ctx, rawURL)
		if err == nil {
			var updated []byte
			if updated, err = sjson.SetBytes(body, fmt.Sprintf("data.%d.b64_json", index), encoded); err == nil {
				body = updated
				continue
			}
		}
		if rt.Log != nil {
			rt.Log.Warn("images b64_json backfill skipped", "request_id", requestID, "index", index, "err", err)
		}
	}
	return body
}

// fetchImageURLBase64 取图片 url 内容的标准 base64：data: url 直接取载荷，其余按上面的限制下载。
func fetchImageURLBase64(ctx context.Context, rawURL string) (string, error) {
	if strings.HasPrefix(strings.ToLower(rawURL), "data:") {
		_, data, err := decodeDataURL(rawURL)
		if err != nil {
			return "", err
		}
		if _, err := base64.StdEncoding.DecodeString(data); err != nil {
			return "", errors.New("data url payload is not valid base64")
		}
		return data, nil
	}
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("unsupported image url scheme")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), imageBackfillTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "image/*,*/*;q=0.8")
	resp, err := imageBackfillClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("download image: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return "", fmt.Errorf("download image: unexpected status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, imageBackfillMaxBytes+1))
	if err != nil {
		return "", fmt.Errorf("read image body: %w", err)
	}
	if len(data) > imageBackfillMaxBytes {
		return "", fmt.Errorf("downloaded image exceeds %d bytes", imageBackfillMaxBytes)
	}
	if len(data) == 0 {
		return "", errors.New("download image: empty body")
	}
	ct := http.DetectContentType(data)
	if i := strings.Index(ct, ";"); i >= 0 {
		ct = ct[:i]
	}
	if _, ok := imageBackfillContentTypes[ct]; !ok {
		return "", errors.New("download image: content is not an allowed image format")
	}
	return base64.StdEncoding.EncodeToString(data), nil
}
