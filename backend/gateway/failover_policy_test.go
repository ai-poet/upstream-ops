package gateway

import (
	"net/http"
	"testing"

	"github.com/bejix/upstream-ops/backend/storage"
)

func TestClassifyFailure_AutoDisable(t *testing.T) {
	rt := NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil).Runtime
	group := &storage.GatewayGroup{AutoDisableEnabled: true}

	cases := []struct {
		name    string
		status  int
		body    string
		echoes  []string
		disable bool
	}{
		// 用户可控的回显不能触发禁用
		{"new-api echoes model name", http.StatusServiceUnavailable,
			`{"error":{"message":"当前分组 default 下对于模型 insufficient_quota 无可用渠道","type":"new_api_error"}}`,
			[]string{"insufficient_quota"}, false},
		{"openai echoes param value", http.StatusBadRequest,
			`{"error":{"message":"Invalid value: 'invalid_api_key'. Supported values are: 'low', 'medium' and 'high'.","type":"invalid_request_error","param":"reasoning.effort","code":"invalid_value"}}`,
			nil, false},
		{"403 echoes model name", http.StatusForbidden,
			`{"error":{"message":"该令牌无权使用模型 account_deactivated","type":"new_api_error"}}`,
			[]string{"account_deactivated"}, false},
		{"5xx message never disables", http.StatusInternalServerError,
			`{"error":{"message":"your credit balance is too low (proxied)","type":"api_error"}}`,
			nil, false},
		// 真实的鉴权 / 额度错误仍然禁用
		{"401 status code", http.StatusUnauthorized, `{"error":{"message":"bad key"}}`, nil, true},
		{"openai insufficient_quota code", http.StatusTooManyRequests,
			`{"error":{"message":"You exceeded your current quota, please check your plan and billing details.","type":"insufficient_quota","code":"insufficient_quota"}}`,
			[]string{"gpt-4o"}, true},
		{"anthropic credit balance 400", http.StatusBadRequest,
			`{"type":"error","error":{"type":"invalid_request_error","message":"Your credit balance is too low to access the Anthropic API."}}`,
			nil, true},
		{"gemini key not valid 400", http.StatusBadRequest,
			`{"error":{"code":400,"message":"API key not valid. Please pass a valid API key.","status":"INVALID_ARGUMENT"}}`,
			nil, true},
		{"new-api token quota 403", http.StatusForbidden,
			`{"error":{"message":"该令牌额度已用尽 TokenStatusExhausted[sk-abc***xyz]","type":"new_api_error"}}`,
			nil, true},
		{"new-api 用户额度不足", http.StatusForbidden,
			`{"error":{"message":"用户额度不足, 剩余额度: $0.01","type":"new_api_error"}}`,
			nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := rt.classifyFailure(group, tc.status, nil, []byte(tc.body), nil, false, tc.echoes...)
			if got := d.Action == failDisableRoute; got != tc.disable {
				t.Fatalf("disable=%v want %v (decision %+v)", got, tc.disable, d)
			}
		})
	}

	// 组未开启自动禁用：401 也只是普通失败
	if d := rt.classifyFailure(&storage.GatewayGroup{}, http.StatusUnauthorized, nil, []byte(`{}`), nil, false); d.Action == failDisableRoute {
		t.Fatalf("auto-disable off but route disabled")
	}
}

func TestClassifyFailure_ModelUnavailableBeforeAutoDisable(t *testing.T) {
	rt := NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil).Runtime
	group := &storage.GatewayGroup{AutoDisableEnabled: true}
	d := rt.classifyFailure(group, http.StatusServiceUnavailable,
		nil, []byte(`{"error":{"message":"当前分组 default 下对于模型 insufficient_quota 无可用渠道"}}`), nil, false, "insufficient_quota")
	if d.Action != failSkipRoute {
		t.Fatalf("decision %+v, want skip route", d)
	}
}

// 旧数据只有 channel_ids：声明了该模型的每条渠道路由都要被偏好，而不只是第一条。
func TestModelRouteIDs_LegacyChannelIDs(t *testing.T) {
	svc := NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	group := &storage.GatewayGroup{ModelAwareRouting: true, ModelsJSON: `[{"id":"gpt-4o","source":"sync","channel_ids":[1,2]}]`}
	routes := []storage.GatewayRoute{{ID: 10, SourceChannelID: 1}, {ID: 20, SourceChannelID: 2}, {ID: 30, SourceChannelID: 3}}
	ids := svc.modelRouteIDs(group, routes, "gpt-4o")
	if _, ok := ids[10]; !ok {
		t.Fatalf("route 10 not preferred: %v", ids)
	}
	if _, ok := ids[20]; !ok {
		t.Fatalf("route 20 not preferred: %v", ids)
	}
	if _, ok := ids[30]; ok {
		t.Fatalf("route 30 does not declare the model: %v", ids)
	}
}
