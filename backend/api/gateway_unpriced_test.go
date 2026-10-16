package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

// 未定价模型接口：列出最近计费时找不到价格的模型，并标出现在是否已经补上价格。
func TestListGatewayUnpricedModels(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	usage := storage.NewGatewayUsageLogs(db)
	prices := storage.NewModelPriceOverrides(db)
	for _, model := range []string{"brand-new-model", "later-priced-model"} {
		if err := usage.Create(&storage.GatewayUsageLog{GatewayKeyID: 1, RouteID: 1, RequestedModel: model, UpstreamModel: model, Unpriced: true, Success: true, CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	// 事后补了价格覆盖
	if err := prices.Upsert(&storage.ModelPriceOverride{ModelName: "later-priced-model", BillingMode: "token", InputPricePerToken: 1e-6, OutputPricePerToken: 2e-6, Enabled: true, PricingTiersJSON: "[]"}); err != nil {
		t.Fatal(err)
	}
	svc := gateway.NewService(nil, nil, nil, usage, prices, nil, nil, nil, nil)

	r := gin.New()
	registerGatewayAdmin(r.Group("/api"), &Deps{Gateway: svc, GatewayUsage: usage, ModelPrices: prices})
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/gateway/prices/unpriced?days=7", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Days  int `json:"days"`
		Items []struct {
			Model     string `json:"model"`
			Requests  int64  `json:"requests"`
			PricedNow bool   `json:"priced_now"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	priced := map[string]bool{}
	for _, it := range resp.Items {
		priced[it.Model] = it.PricedNow
	}
	if resp.Days != 7 || len(resp.Items) != 2 || priced["brand-new-model"] || !priced["later-priced-model"] {
		t.Fatalf("resp = %s", rec.Body.String())
	}
}
