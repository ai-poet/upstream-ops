package storage

import (
	"math"
	"time"
)

// BalanceBurnWindow 估算余额消耗速度时回看的时长。
// 只看最近 2 小时，让估算跟得上当前的消耗速度；窗口再长会把突发的大量消耗摊薄，
// 余额可能在估算出"快用完"之前就已经用完。
const BalanceBurnWindow = 2 * time.Hour

// minBalanceBurnSpan 样本首尾跨度的下限；更短的数据波动太大，不给出估算。
const minBalanceBurnSpan = time.Hour

// BalanceBurn 一段时间内余额消耗速度的估算结果。
type BalanceBurn struct {
	DailyCost float64 // 按样本期间的速度折算的日消耗（与余额同一显示单位）
	SpanHours float64 // 参与估算的样本首尾跨度（小时）
}

// EstimateBalanceBurn 用按 sampled_at 升序排列的余额快照估算消耗速度，结果折算成日消耗。
//
// 只累计相邻两次采样之间的余额下降：充值、兑换等导致的上涨直接跳过，不抵扣消耗，
// 所以中途充值不会把消耗算成负数。两次采样间隔不均时，下降量摊到整段间隔里。
//
// 少于两条快照、或首尾跨度不足 minBalanceBurnSpan 时返回 nil，表示样本不足。
// 有样本但余额从未下降时返回 DailyCost = 0，表示这段时间没有消耗。
func EstimateBalanceBurn(snapshots []BalanceSnapshot) *BalanceBurn {
	if len(snapshots) < 2 {
		return nil
	}
	span := snapshots[len(snapshots)-1].SampledAt.Sub(snapshots[0].SampledAt)
	if span < minBalanceBurnSpan {
		return nil
	}
	var consumed float64
	for i := 1; i < len(snapshots); i++ {
		if drop := snapshots[i-1].Balance - snapshots[i].Balance; drop > 0 {
			consumed += drop
		}
	}
	return &BalanceBurn{
		DailyCost: roundBurn(consumed / span.Hours() * 24),
		SpanHours: roundBurn(span.Hours()),
	}
}

// roundBurn 保留 4 位小数，与连接器换算余额时的精度一致。
func roundBurn(v float64) float64 {
	return math.Round(v*10000) / 10000
}
