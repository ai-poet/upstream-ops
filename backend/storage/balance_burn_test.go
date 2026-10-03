package storage

import (
	"reflect"
	"testing"
	"time"
)

// burnSnapshots 从 base 开始，每 step 一条，依次写入 balances。
func burnSnapshots(base time.Time, step time.Duration, balances ...float64) []BalanceSnapshot {
	list := make([]BalanceSnapshot, len(balances))
	for i, b := range balances {
		list[i] = BalanceSnapshot{ChannelID: 1, Balance: b, SampledAt: base.Add(time.Duration(i) * step)}
	}
	return list
}

func TestEstimateBalanceBurn(t *testing.T) {
	base := time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)

	cases := []struct {
		name      string
		snapshots []BalanceSnapshot
		want      *BalanceBurn
	}{
		{"无快照", nil, nil},
		{"只有一条", burnSnapshots(base, time.Hour, 100), nil},
		{"跨度不足 1 小时", burnSnapshots(base, 15*time.Minute, 100, 99, 98, 97), nil},
		// 刚好 1 小时可以估算：1 小时消耗 1，日均 24。
		{"跨度刚好 1 小时", burnSnapshots(base, 30*time.Minute, 100, 99.5, 99), &BalanceBurn{DailyCost: 24, SpanHours: 1}},
		{"匀速消耗", burnSnapshots(base, 6*time.Hour, 100, 97.5, 95, 92.5, 90), &BalanceBurn{DailyCost: 10, SpanHours: 24}},
		// 充值的上涨不抵扣消耗：下降 20 + 10 = 30，跨度 12 小时 → 日均 60。
		{"中途充值", burnSnapshots(base, 4*time.Hour, 100, 80, 150, 140), &BalanceBurn{DailyCost: 60, SpanHours: 12}},
		{"只涨不跌", burnSnapshots(base, time.Hour, 100, 100, 120), &BalanceBurn{DailyCost: 0, SpanHours: 2}},
		// 暂停监控两天后恢复：中间没有采样，下降量摊到整段间隔里。
		{"采样间隔不均", []BalanceSnapshot{
			{Balance: 100, SampledAt: base},
			{Balance: 99, SampledAt: base.Add(time.Hour)},
			{Balance: 51, SampledAt: base.Add(48 * time.Hour)},
		}, &BalanceBurn{DailyCost: 24.5, SpanHours: 48}},
		// 结果保留 4 位小数：7 小时消耗 1 → 日均 3.428571…
		{"四舍五入", burnSnapshots(base, 7*time.Hour, 10, 9), &BalanceBurn{DailyCost: 3.4286, SpanHours: 7}},
	}
	for _, tc := range cases {
		got := EstimateBalanceBurn(tc.snapshots)
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: got %+v, want %+v", tc.name, got, tc.want)
		}
	}
}

func TestBalanceSnapshotsSinceAndUpdateBalanceBurn(t *testing.T) {
	db := openTestDB(t)
	rates := NewRates(db)
	channels := NewChannels(db)

	ch := Channel{Name: "demo", Type: ChannelTypeNewAPI, PasswordCipher: "x"}
	if err := channels.Create(&ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	other := Channel{Name: "other", Type: ChannelTypeNewAPI, PasswordCipher: "x"}
	if err := channels.Create(&other); err != nil {
		t.Fatalf("create other channel: %v", err)
	}

	base := time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)
	// 故意乱序写入，读取时按采样时间升序。
	for _, s := range []BalanceSnapshot{
		{ChannelID: ch.ID, Balance: 90, SampledAt: base.Add(2 * time.Hour)},
		{ChannelID: ch.ID, Balance: 100, SampledAt: base},
		{ChannelID: ch.ID, Balance: 120, SampledAt: base.Add(-time.Hour)}, // 早于 since，不返回
		{ChannelID: other.ID, Balance: 1, SampledAt: base.Add(time.Hour)}, // 其它渠道，不返回
		{ChannelID: ch.ID, Balance: 95, SampledAt: base.Add(time.Hour)},
	} {
		s := s
		if err := rates.AppendBalance(&s); err != nil {
			t.Fatalf("append balance: %v", err)
		}
	}

	list, err := rates.BalanceSnapshotsSince(ch.ID, base)
	if err != nil {
		t.Fatalf("BalanceSnapshotsSince: %v", err)
	}
	var balances []float64
	for _, s := range list {
		balances = append(balances, s.Balance)
	}
	if !reflect.DeepEqual(balances, []float64{100, 95, 90}) {
		t.Fatalf("balances = %v, want [100 95 90]", balances)
	}

	if err := channels.UpdateBalanceBurn(ch.ID, EstimateBalanceBurn(list)); err != nil {
		t.Fatalf("UpdateBalanceBurn: %v", err)
	}
	got, err := channels.FindByID(ch.ID)
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if got.BalanceDailyCost == nil || *got.BalanceDailyCost != 120 || got.BalanceCostSpanHours == nil || *got.BalanceCostSpanHours != 2 {
		t.Fatalf("burn = %v / %v, want 120 / 2", got.BalanceDailyCost, got.BalanceCostSpanHours)
	}

	// 样本不足时清空旧的估算。
	if err := channels.UpdateBalanceBurn(ch.ID, nil); err != nil {
		t.Fatalf("UpdateBalanceBurn(nil): %v", err)
	}
	got, err = channels.FindByID(ch.ID)
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if got.BalanceDailyCost != nil || got.BalanceCostSpanHours != nil {
		t.Fatalf("burn = %v / %v, want nil / nil", got.BalanceDailyCost, got.BalanceCostSpanHours)
	}
}
