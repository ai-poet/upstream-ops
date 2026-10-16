package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/gateway"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

// 「从远程同步价格」：先同步网关价目表（远程表里有的模型直接有价格），
// 仍找不到的到参考价目表查参考价；同步与参考表都没配时回 409。
func TestSyncGatewayRemotePrices(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	usage := storage.NewGatewayUsageLogs(db)
	prices := storage.NewModelPriceOverrides(db)
	for _, model := range []string{"brand-new-model", "only-in-reference", "nowhere-model"} {
		if err := usage.Create(&storage.GatewayUsageLog{GatewayKeyID: 1, RouteID: 1, RequestedModel: model, UpstreamModel: model, Unpriced: true, Success: true, CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/prices.json":
			_, _ = w.Write([]byte(`{"vendor/brand-new-model":{"input_cost_per_token":0.000001,"output_cost_per_token":0.000002}}`))
		case "/reference.json":
			_, _ = w.Write([]byte(`{
				"zai/only-in-reference":{"input_cost_per_token":0.0000006,"output_cost_per_token":0.0000022,"cache_read_input_token_cost":0.00000011},
				"openrouter/z-ai/only-in-reference":{"input_cost_per_token":0.000001,"output_cost_per_token":0.000003}
			}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer remote.Close()

	svc := gateway.NewService(nil, nil, nil, usage, prices, nil, nil, nil, nil)
	r := gin.New()
	registerGatewayAdmin(r.Group("/api"), &Deps{Gateway: svc, GatewayUsage: usage, ModelPrices: prices})
	post := func() *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/gateway/prices/sync", nil))
		return rec
	}

	if rec := post(); rec.Code != http.StatusConflict {
		t.Fatalf("sync while disabled: %d %s", rec.Code, rec.Body.String())
	}

	svc.StartPricing(config.PricingConfig{
		Enabled: true, RemoteURL: remote.URL + "/prices.json", ReferenceURL: remote.URL + "/reference.json",
		DataDir: t.TempDir(), HashCheckIntervalMinutes: 60, UpdateIntervalHours: 24,
	})
	defer svc.Stop()
	rec := post()
	if rec.Code != http.StatusOK {
		t.Fatalf("sync: %d %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Sync struct {
			Models int `json:"models"`
		} `json:"sync"`
		Items []struct {
			Model     string `json:"model"`
			PricedNow bool   `json:"priced_now"`
			Reference *struct {
				SourceKey          string  `json:"source_key"`
				InputPricePerToken float64 `json:"input_price_per_token"`
			} `json:"reference"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	byModel := map[string]int{}
	for i, it := range resp.Items {
		byModel[it.Model] = i
	}
	brand, ref, nowhere := resp.Items[byModel["brand-new-model"]], resp.Items[byModel["only-in-reference"]], resp.Items[byModel["nowhere-model"]]
	if resp.Sync.Models != 1 || !brand.PricedNow || brand.Reference != nil {
		t.Fatalf("primary sync: %s", rec.Body.String())
	}
	if ref.PricedNow || ref.Reference == nil || ref.Reference.SourceKey != "zai/only-in-reference" || ref.Reference.InputPricePerToken != 6e-7 {
		t.Fatalf("reference (official provider first): %s", rec.Body.String())
	}
	if nowhere.PricedNow || nowhere.Reference != nil {
		t.Fatalf("unknown model: %s", rec.Body.String())
	}
}
