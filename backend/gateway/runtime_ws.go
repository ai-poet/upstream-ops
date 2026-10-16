package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/pricing"
	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/coder/websocket"
	"github.com/gin-gonic/gin"
)

// wsResponsesPath Responses WebSocket 的入站与上游端点。
const wsResponsesPath = "/v1/responses"

// HandleResponsesWebSocket forwards native Responses WebSocket sessions. A
// WebSocket session cannot safely change protocol mid-flight, so only routes
// whose resolved upstream protocol is Responses are eligible.
func (rt *Runtime) HandleResponsesWebSocket(c *gin.Context) {
	reqID := ensureGatewayRequestID(c)
	if !strings.EqualFold(c.GetHeader("Upgrade"), "websocket") {
		// 对齐 sub2api：GET 只用于 WebSocket 升级
		c.JSON(http.StatusUpgradeRequired, gin.H{
			"message":                   "Use POST /v1/responses for Responses API",
			"websocket":                 "supported",
			jsonKeyUpstreamOpsRequestID: reqID,
		})
		return
	}

	auth, err := rt.Authenticate(c)
	if err != nil {
		writeError(c, protocolOpenAI, authError(err.Error()))
		return
	}
	if rt.Routes == nil {
		writeError(c, protocolOpenAI, newGatewayError(http.StatusServiceUnavailable, "api_error", "routes not configured"))
		return
	}

	client, err := websocket.Accept(c.Writer, c.Request, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	defer client.CloseNow()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	msgType, first, err := client.Read(ctx)
	if err != nil {
		return
	}
	if msgType != websocket.MessageText {
		writeWSResponsesFailed(ctx, client, "invalid_request_error", "first WebSocket message must be response.create")
		return
	}
	requestedModel, err := wsResponsesCreateModel(first)
	if err != nil {
		writeWSResponsesFailed(ctx, client, "invalid_request_error", err.Error())
		return
	}

	req := &relayRequest{
		c:      c,
		in:     forwardRequest{Path: wsResponsesPath, Kind: protocol.KindOpenAIResponses},
		id:     reqID,
		key:    auth.Key,
		group:  auth.Group,
		model:  requestedModel,
		stream: true,
	}
	first, err = rt.applyResponsesWebSocketSystemPrompt(req.group, req.key, first)
	if err != nil {
		writeWSResponsesFailed(ctx, client, "invalid_request_error", "system prompt injection failed: "+err.Error())
		return
	}
	rt.touchKey(req.key.ID, time.Now())
	at, errCode, errMsg := rt.selectWSResponsesRoute(req)
	if at == nil {
		writeWSResponsesFailed(ctx, client, errCode, errMsg)
		return
	}

	upURL, err := responsesWebSocketURL(at.target.BaseURL)
	if err != nil {
		writeWSResponsesFailed(ctx, client, "api_error", err.Error())
		return
	}
	handshake, err := rt.buildUpstreamHTTPRequest(ctx, upstreamCall{
		target: at.target, path: wsResponsesPath, method: http.MethodGet,
		header: c.Request.Header, kind: protocol.KindOpenAIResponses,
	})
	if err != nil {
		writeWSResponsesFailed(ctx, client, "api_error", err.Error())
		return
	}
	upHTTP := rt.httpClientForTarget(at.target.Channel, at.target.Provider)
	upHTTP.Timeout = 0
	upstream, _, err := websocket.Dial(ctx, upURL, &websocket.DialOptions{HTTPClient: upHTTP, HTTPHeader: handshake.Header})
	if err != nil {
		writeWSResponsesFailed(ctx, client, "upstream_error", "failed to connect upstream: "+err.Error())
		return
	}
	defer upstream.CloseNow()

	first = RewriteModelInBody(first, at.upstreamModel)
	if err := upstream.Write(ctx, websocket.MessageText, first); err != nil {
		writeWSResponsesFailed(ctx, client, "upstream_error", "failed to send response.create: "+err.Error())
		return
	}

	facts := scanRequestFacts(wsResponsesPath, first)
	req.serviceTier, req.reasoningEffort, req.billing = facts.serviceTier, facts.reasoningEffort, facts.billing
	at.meta = usageRecordMeta{
		InboundEndpoint: wsResponsesPath, UpstreamEndpoint: wsResponsesPath,
		InboundProtocol: string(protocol.KindOpenAIResponses), UpstreamProtocol: string(protocol.KindOpenAIResponses),
		ServiceTier: req.serviceTier, ReasoningEffort: req.reasoningEffort, UpstreamURL: upURL,
		Attempt: 1, AttemptKind: attemptKindPrimary, BillingInput: req.billing,
	}
	rt.relayResponsesWebSocket(ctx, &wsRelay{req: req, at: at, client: client, upstream: upstream})
}

// selectWSResponsesRoute 按倍率顺序取第一条上游协议为 Responses 的路由（会话中途不能换协议，也不顺延）。
// 没有可用路由时返回 nil 与回给客户端的错误码、说明。
func (rt *Runtime) selectWSResponsesRoute(req *relayRequest) (at *relayAttempt, errCode, errMsg string) {
	routes, err := rt.cachedRoutes(req.group.ID)
	if err != nil || len(routes) == 0 {
		return nil, "api_error", "no routes configured"
	}
	req.groupMapping = cachedModelMapping(req.group.ModelMappingJSON)
	groupMappedModel, _ := ResolveModel(req.model, req.groupMapping)
	// 转发模型：只留下接这个模型的路由
	if routes = filterRoutesForModel(routes, req.model, groupMappedModel); len(routes) == 0 {
		return nil, "invalid_request_error", fmt.Sprintf("model %q is not available: no route in this group forwards it", req.model)
	}
	groupsByChannel := rt.loadGroupsByChannel(req.c.Request.Context(), routes)
	for _, cand := range SortRoutes(routes, groupsByChannel, req.group.RateSortDirection, time.Now(), nil) {
		choice := newRouteChoice(req, cand)
		routeProto := normalizeUpstreamProtocol(choice.route.UpstreamProtocol)
		target, resolveErr := rt.resolveUpstreamTarget(&choice.route)
		if resolveErr != nil {
			continue
		}
		if choice.route.NormalizeSourceKind() == "provider" && routeProto == "auto" && target.Provider != nil {
			routeProto = normalizeProviderProtocol(target.Provider.UpstreamProtocol)
		}
		if protocol.ResolveUpstream(routeProto, protocol.KindOpenAIResponses, choice.upstreamModel) != protocol.KindOpenAIResponses {
			continue
		}
		applyRouteUserAgent(target, req.group, &choice.route)
		return &relayAttempt{
			routeChoice:  choice,
			no:           1,
			kind:         attemptKindPrimary,
			target:       target,
			upstreamKind: protocol.KindOpenAIResponses,
		}, "", ""
	}
	return nil, "api_error", "no native Responses WebSocket route configured"
}

// wsRelay 一次 Responses WebSocket 会话：客户端 ↔ 选中的上游，会话结束时记一条用量。
type wsRelay struct {
	req      *relayRequest
	at       *relayAttempt
	client   *websocket.Conn
	upstream *websocket.Conn

	started            time.Time
	body               bytes.Buffer // 上游原始事件（计费与补算用量）
	tokens             pricing.UsageTokens
	clientDisconnected bool
}

type wsMessage struct {
	typ  websocket.MessageType
	body []byte
	err  error
}

// relayResponsesWebSocket 双向转发：客户端消息注入系统提示词、改写模型后发上游；上游事件原样回给客户端，
// 遇到终态事件记账结束。客户端断开后继续读上游到终态，用量不丢。
func (rt *Runtime) relayResponsesWebSocket(ctx context.Context, ws *wsRelay) {
	upMessages := make(chan wsMessage, 32)
	clientMessages := make(chan wsMessage, 1)
	go wsReadLoop(ctx, ws.upstream, upMessages)
	go wsReadLoop(ctx, ws.client, clientMessages)

	ws.started = time.Now()
	clientCh := clientMessages
	for {
		select {
		case msg, ok := <-clientCh:
			if !ok || msg.err != nil {
				ws.clientDisconnected = true
				clientCh = nil
				continue
			}
			if msg.typ != websocket.MessageText {
				continue
			}
			payload, injectErr := rt.applyResponsesWebSocketSystemPrompt(ws.req.group, ws.req.key, msg.body)
			if injectErr != nil {
				rt.endResponsesWebSocket(ctx, ws, ws.tokens, false, "invalid_request_error", injectErr.Error(),
					"system prompt injection failed: "+injectErr.Error())
				return
			}
			payload = RewriteModelInBody(payload, ws.at.upstreamModel)
			if err := ws.upstream.Write(ctx, websocket.MessageText, payload); err != nil {
				rt.endResponsesWebSocket(ctx, ws, ws.tokens, false, "upstream_error", err.Error(), err.Error())
				return
			}

		case msg, ok := <-upMessages:
			if !ok || msg.err != nil {
				errText := "upstream WebSocket closed before terminal event"
				if msg.err != nil && !errors.Is(msg.err, context.Canceled) {
					errText = msg.err.Error()
				}
				rt.recordResponsesWebSocket(ws, ws.tokens, false, "transport", errText)
				if !ws.clientDisconnected {
					writeWSResponsesFailed(ctx, ws.client, "stream_incomplete", errText)
				}
				return
			}
			if msg.typ != websocket.MessageText {
				if !ws.clientDisconnected {
					_ = ws.client.Write(ctx, msg.typ, msg.body)
				}
				continue
			}
			ws.body.Write(msg.body)
			ws.body.WriteByte('\n')
			mergeStreamUsage(&ws.tokens, string(msg.body), protocol.KindOpenAIResponses)
			terminal, success, errType, errText := wsResponsesTerminal(msg.body)
			if !ws.clientDisconnected {
				if err := ws.client.Write(ctx, websocket.MessageText, msg.body); err != nil {
					ws.clientDisconnected = true
				}
			}
			if !terminal {
				continue
			}
			tokens := finalizeStreamTokens(ws.tokens, ws.body.Bytes(), protocol.KindOpenAIResponses)
			rt.recordResponsesWebSocket(ws, tokens, success, errType, errText)
			if success {
				rt.noteRouteSuccess(&ws.at.route)
			}
			return
		}
	}
}

// endResponsesWebSocket 客户端一侧出错结束会话：记账，客户端还在时回一个 response.failed。
func (rt *Runtime) endResponsesWebSocket(ctx context.Context, ws *wsRelay, tokens pricing.UsageTokens, success bool, errType, errText, clientMsg string) {
	rt.recordResponsesWebSocket(ws, tokens, success, errType, errText)
	if !ws.clientDisconnected {
		writeWSResponsesFailed(ctx, ws.client, errType, clientMsg)
	}
}

// recordResponsesWebSocket 记一条会话用量（会话结束时调用一次）。
func (rt *Runtime) recordResponsesWebSocket(ws *wsRelay, tokens pricing.UsageTokens, success bool, errType, errText string) {
	if rt.Usage == nil {
		return
	}
	ws.at.meta.BillingInput = ws.at.meta.BillingInput.WithResponse(ws.body.Bytes())
	status := http.StatusOK
	if !success {
		status = http.StatusBadGateway
	}
	rt.recordAttempt(ws.req, ws.at, usageOutcome{
		tokens:     tokens,
		status:     status,
		success:    success,
		errInfo:    usageErrorInfo{Type: errType, Summary: errText},
		durationMS: time.Since(ws.started).Milliseconds(),
	})
}

func wsReadLoop(ctx context.Context, conn *websocket.Conn, out chan<- wsMessage) {
	defer close(out)
	for {
		typ, body, err := conn.Read(ctx)
		if err != nil {
			select {
			case out <- wsMessage{err: err}:
			case <-ctx.Done():
			}
			return
		}
		select {
		case out <- wsMessage{typ: typ, body: body}:
		case <-ctx.Done():
			return
		}
	}
}

func wsResponsesCreateModel(body []byte) (string, error) {
	var event struct {
		Type  string `json:"type"`
		Model string `json:"model"`
	}
	if err := json.Unmarshal(body, &event); err != nil {
		return "", errors.New("invalid response.create message")
	}
	if event.Type != "response.create" {
		return "", errors.New("first WebSocket message must be response.create")
	}
	return strings.TrimSpace(event.Model), nil
}

func wsResponsesTerminal(body []byte) (terminal, success bool, errType, errText string) {
	var event struct {
		Type     string `json:"type"`
		Response struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		} `json:"response"`
	}
	if json.Unmarshal(body, &event) != nil {
		return false, false, "", ""
	}
	switch event.Type {
	case "response.completed", "response.done", "response.incomplete":
		return true, true, "", ""
	case "response.failed", "response.cancelled", "response.canceled":
		errType = event.Response.Error.Code
		if errType == "" {
			errType = "upstream_error"
		}
		errText = event.Response.Error.Message
		if errText == "" {
			errText = event.Type
		}
		return true, false, errType, errText
	default:
		return false, false, "", ""
	}
}

func writeWSResponsesFailed(ctx context.Context, conn *websocket.Conn, code, message string) {
	payload, _ := json.Marshal(map[string]any{
		"type": "response.failed",
		"response": map[string]any{
			"id": "resp_gateway_error", "object": "response", "status": "failed", "output": []any{},
			"error": map[string]any{"code": code, "message": message},
		},
	})
	_ = conn.Write(ctx, websocket.MessageText, payload)
}

func responsesWebSocketURL(base string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(base))
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("invalid upstream base URL")
	}
	switch strings.ToLower(u.Scheme) {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	case "ws", "wss":
	default:
		return "", fmt.Errorf("unsupported upstream URL scheme %q", u.Scheme)
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/v1/responses"
	u.RawQuery = ""
	return u.String(), nil
}
