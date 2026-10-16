// 请求侧辅助：multipart 表单里的模型名、透传查询串。
package gateway

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"
)

func isMultipartContentType(contentType string) bool {
	mt, _, err := mime.ParseMediaType(contentType)
	return err == nil && strings.EqualFold(mt, "multipart/form-data")
}

// requestModel 取请求里的模型名：JSON body 的 model，或 multipart 表单（图片编辑 / 语音转写）的 model 字段。
func requestModel(contentType string, body []byte) string {
	if !isMultipartContentType(contentType) {
		return ExtractModelFromBody(body)
	}
	return multipartFieldValue(contentType, body, "model")
}

// multipartFieldValue multipart 表单里某个普通字段（非文件）的值；没有或解析失败返回空。
func multipartFieldValue(contentType string, body []byte, name string) string {
	_, params, err := mime.ParseMediaType(contentType)
	if err != nil || params["boundary"] == "" {
		return ""
	}
	r := multipart.NewReader(bytes.NewReader(body), params["boundary"])
	for {
		part, err := r.NextPart()
		if err != nil {
			return ""
		}
		if part.FormName() == name && part.FileName() == "" {
			v, _ := io.ReadAll(io.LimitReader(part, 512))
			return strings.TrimSpace(string(v))
		}
	}
}

// rewriteMultipartModel 改写 multipart 表单里的 model 字段，沿用原 boundary（Content-Type 不变）。
// 解析失败时原样返回。
func rewriteMultipartModel(contentType string, body []byte, model string) []byte {
	_, params, err := mime.ParseMediaType(contentType)
	if err != nil || params["boundary"] == "" {
		return body
	}
	r := multipart.NewReader(bytes.NewReader(body), params["boundary"])
	var out bytes.Buffer
	w := multipart.NewWriter(&out)
	if err := w.SetBoundary(params["boundary"]); err != nil {
		return body
	}
	found := false
	for {
		part, err := r.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return body
		}
		dst, err := w.CreatePart(part.Header)
		if err != nil {
			return body
		}
		if part.FormName() == "model" && part.FileName() == "" {
			found = true
			_, _ = io.Copy(io.Discard, part)
			_, _ = dst.Write([]byte(model))
			continue
		}
		if _, err := io.Copy(dst, part); err != nil {
			return body
		}
	}
	if err := w.Close(); err != nil || !found {
		return body
	}
	return out.Bytes()
}

// forwardQuery 入站查询串去掉网关自己的鉴权参数（Gemini ?key= / ?api_key=），其余参数的顺序与编码原样保留。
func forwardQuery(c *gin.Context) string {
	if c == nil || c.Request == nil || c.Request.URL == nil || c.Request.URL.RawQuery == "" {
		return ""
	}
	parts := strings.Split(c.Request.URL.RawQuery, "&")
	kept := make([]string, 0, len(parts))
	for _, part := range parts {
		name, _, _ := strings.Cut(part, "=")
		if decoded, err := url.QueryUnescape(name); err == nil {
			name = decoded
		}
		if part == "" || name == "key" || name == "api_key" {
			continue
		}
		kept = append(kept, part)
	}
	return strings.Join(kept, "&")
}

func appendQuery(path, query string) string {
	if query == "" {
		return path
	}
	if strings.Contains(path, "?") {
		// 已有 ?alt=sse 之类：合并，避免重复键
		base, existing, _ := strings.Cut(path, "?")
		merged, err := url.ParseQuery(existing)
		if err != nil {
			return path + "&" + query
		}
		extra, err := url.ParseQuery(query)
		if err != nil {
			return path
		}
		for k, vs := range extra {
			if _, ok := merged[k]; !ok {
				merged[k] = vs
			}
		}
		return base + "?" + merged.Encode()
	}
	return path + "?" + query
}

// readRequestBody 读客户端请求体；超过网关设置的上限（gateway.maxRequestBodyBytes）回 413，
// 不再整包读进内存——以前不限长，一个超大请求就能把进程内存打满。
func (svc *core) readRequestBody(c *gin.Context) ([]byte, *gatewayError) {
	if c.Request.Body == nil {
		return nil, nil
	}
	limit := svc.gatewayRuntime().MaxRequestBody()
	body, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, limit))
	if err == nil {
		return body, nil
	}
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		return nil, newGatewayError(http.StatusRequestEntityTooLarge, "request_too_large",
			fmt.Sprintf("request body exceeds the gateway limit of %d bytes", limit))
	}
	return nil, newGatewayError(http.StatusBadRequest, "invalid_request_error", "failed to read body")
}
