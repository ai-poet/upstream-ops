package storage

import (
	"testing"
	"time"
)

// 未定价模型汇总：只算标记了 unpriced 的、时间窗内的记录；按计费用的模型名（上游模型优先）分组。
func TestListUnpricedModels(t *testing.T) {
	logs := NewGatewayUsageLogs(openTestDB(t))
	now := time.Now()
	add := func(requested, upstream string, unpriced, success bool, in, out int, at time.Time) {
		t.Helper()
		if err := logs.Create(&GatewayUsageLog{
			GatewayKeyID: 1, RouteID: 1, RequestedModel: requested, UpstreamModel: upstream,
			Unpriced: unpriced, Success: success, InputTokens: in, OutputTokens: out, CreatedAt: at,
		}); err != nil {
			t.Fatal(err)
		}
	}
	add("alias", "new-model", true, true, 100, 10, now.Add(-2*time.Hour))
	add("new-model", "", true, true, 50, 5, now.Add(-time.Hour)) // 没有上游模型时按请求模型
	add("alias", "new-model", true, false, 0, 0, now.Add(-30*time.Minute))
	add("other", "other", true, true, 1, 1, now.AddDate(0, 0, -40)) // 时间窗外
	add("gpt-4o", "gpt-4o", false, true, 1000, 100, now)            // 有价格

	got, err := logs.ListUnpricedModels(now.AddDate(0, 0, -30), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %+v", got)
	}
	s := got[0]
	if s.Model != "new-model" || s.Requests != 3 || s.SuccessRequests != 2 || s.InputTokens != 150 || s.OutputTokens != 15 {
		t.Fatalf("stat = %+v", s)
	}
	if d := s.LastSeenAt.Sub(now.Add(-30 * time.Minute)); d < -time.Second || d > time.Second {
		t.Fatalf("last seen = %v", s.LastSeenAt)
	}
}
