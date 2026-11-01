package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestParseGatewayUsageQueryIncludeSum(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		query string
		want  bool
	}{
		{query: "", want: false},
		{query: "include_sum=true", want: false},
		{query: "include_sum=false", want: true},
		{query: "include_sum=0", want: true},
	} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodGet, "/api/gateway/usage?"+tc.query, nil)
		q := parseGatewayUsageQuery(c)
		if q.SkipCostSum != tc.want {
			t.Fatalf("query %q: SkipCostSum = %v, want %v", tc.query, q.SkipCostSum, tc.want)
		}
	}
}

func TestCleanupGatewayUsageByIDs(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	usage := storage.NewGatewayUsageLogs(db)
	rows := []storage.GatewayUsageLog{
		{RequestID: "delete-1", RequestedModel: "gpt-4o", ActualCost: 1, CreatedAt: time.Now()},
		{RequestID: "delete-2", RequestedModel: "gpt-4o", ActualCost: 2, CreatedAt: time.Now()},
		{RequestID: "keep", RequestedModel: "gpt-4o", ActualCost: 4, CreatedAt: time.Now()},
	}
	for i := range rows {
		if err := usage.Create(&rows[i]); err != nil {
			t.Fatalf("create usage %d: %v", i, err)
		}
	}
	svc := gateway.NewService(nil, nil, nil, usage, nil, nil, nil, nil, nil)
	r := gin.New()
	registerGatewayAdmin(r.Group("/api"), &Deps{Gateway: svc, GatewayUsage: usage})

	body, err := json.Marshal(map[string]any{
		"ids":     []uint{rows[0].ID, rows[1].ID, rows[0].ID},
		"confirm": true,
	})
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/gateway/usage/cleanup", bytes.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Deleted int64 `json:"deleted"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Deleted != 2 {
		t.Fatalf("deleted = %d, want 2", resp.Deleted)
	}
	page, err := usage.List(storage.GatewayUsageQuery{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	if page.Total != 1 || len(page.Items) != 1 || page.Items[0].RequestID != "keep" {
		t.Fatalf("remaining usage = %+v", page)
	}
}

func TestCleanupGatewayUsageByIDsDryRunAndValidation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	usage := storage.NewGatewayUsageLogs(db)
	row := storage.GatewayUsageLog{RequestID: "keep", RequestedModel: "gpt-4o", CreatedAt: time.Now()}
	if err := usage.Create(&row); err != nil {
		t.Fatal(err)
	}
	svc := gateway.NewService(nil, nil, nil, usage, nil, nil, nil, nil, nil)
	r := gin.New()
	registerGatewayAdmin(r.Group("/api"), &Deps{Gateway: svc, GatewayUsage: usage})

	for _, tc := range []struct {
		name       string
		body       map[string]any
		wantStatus int
	}{
		{name: "dry run", body: map[string]any{"ids": []uint{row.ID}, "dry_run": true}, wantStatus: http.StatusOK},
		{name: "missing confirm", body: map[string]any{"ids": []uint{row.ID}}, wantStatus: http.StatusBadRequest},
		{name: "zero id", body: map[string]any{"ids": []uint{0}, "confirm": true}, wantStatus: http.StatusBadRequest},
		{name: "conflicting scope", body: map[string]any{"ids": []uint{row.ID}, "all": true, "confirm": true}, wantStatus: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, err := json.Marshal(tc.body)
			if err != nil {
				t.Fatal(err)
			}
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/gateway/usage/cleanup", bytes.NewReader(body)))
			if rec.Code != tc.wantStatus {
				t.Fatalf("status %d %s, want %d", rec.Code, rec.Body.String(), tc.wantStatus)
			}
			count, err := usage.CountAll()
			if err != nil {
				t.Fatal(err)
			}
			if count != 1 {
				t.Fatalf("usage count = %d, want 1", count)
			}
		})
	}
}
