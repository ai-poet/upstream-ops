package notify

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
)

// Policy 通知去抖策略。所有字段都是面向"少烦用户"取向：
//   - MinChangePct：涨跌幅小于阈值时跳过推送（仍写入 RateChangeLog 表）
//   - BalanceLowCooldown：同渠道 balance_low 在窗口内不重复发送
//   - BalanceDepletionLead：预计余额在多久内用完时发送 balance_depleting，<=0 表示关闭
//   - SendMaxAttempts：单条消息最多发送尝试次数（含首发），<=1 表示不重试
type Policy struct {
	NotificationPrefix                       string
	MinChangePct                             float64
	BalanceLowCooldown                       time.Duration
	BalanceDepletionLead                     time.Duration
	SubscriptionDailyRemainingThresholdPct   float64
	SubscriptionWeeklyRemainingThresholdPct  float64
	SubscriptionMonthlyRemainingThresholdPct float64
	SubscriptionExpiryThreshold              time.Duration
	SubscriptionAlertCooldown                time.Duration
	SendMaxAttempts                          int
}

// CooldownStore Dispatcher 用来判断某个 (channelID, event) 是否还在冷却窗口。
//
// 抽象成 interface 是为了让 dispatcher 不依赖具体存储；
// 生产实现是 *storage.Notifications.TryClaimCooldown；
// 测试时可以注入一个内存 stub。
type CooldownStore interface {
	TryClaimCooldown(channelID uint, event storage.NotificationEvent, cooldown time.Duration) (bool, error)
}

// RateChange 是一条待发送的倍率相关记录（去抖 / 合并的基本单元）。
type RateChange struct {
	GroupName string
	OldRatio  float64
	NewRatio  float64
	OldComp   float64
	NewComp   float64
	ChangedAt time.Time
}

// ChannelRateChange 为全渠道扫描保留来源渠道，供订阅规则筛选和汇总展示。
type ChannelRateChange struct {
	ChannelID   uint
	ChannelName string
	Change      RateChange
}

// RateNotificationBatch 是一次全渠道扫描中的全部分组变化。
type RateNotificationBatch struct {
	Changed []ChannelRateChange
	Added   []ChannelRateChange
	Removed []ChannelRateChange
}

func (b RateNotificationBatch) Empty() bool {
	return len(b.Changed)+len(b.Added)+len(b.Removed) == 0
}

// ChangePctAbove 涨跌幅是否达到阈值。
// minPct = 0 表示不过滤。OldRatio = 0 时按"新出现的分组"处理，永远算"达到阈值"。
func (rc RateChange) ChangePctAbove(minPct float64) bool {
	if minPct <= 0 {
		return true
	}
	if rc.OldRatio == 0 {
		return true
	}
	pct := math.Abs(rc.NewRatio-rc.OldRatio) / math.Abs(rc.OldRatio) * 100
	return pct >= minPct
}

// BuildRateNotificationBatchMessage 把整轮扫描的渠道变化合并为一条 CommonMark 消息。
func BuildRateNotificationBatchMessage(batch RateNotificationBatch) Message {
	if batch.Empty() {
		return Message{}
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# 渠道分组变动\n\n> 扫描时间：%s\n", time.Now().Format("2006-01-02 15:04"))
	writeRateNotificationSection(&b, "倍率变化", batch.Changed, func(c RateChange) string {
		return fmt.Sprintf("倍率：`%gx` %s `%gx`", c.OldRatio, arrowFor(c.OldRatio, c.NewRatio), c.NewRatio)
	})
	writeRateNotificationSection(&b, "新增分组", batch.Added, func(c RateChange) string {
		return fmt.Sprintf("倍率：`%gx`", c.NewRatio)
	})
	writeRateNotificationSection(&b, "移除分组", batch.Removed, func(c RateChange) string {
		return fmt.Sprintf("原倍率：`%gx`", c.OldRatio)
	})
	channels := make(map[uint]struct{})
	for _, items := range [][]ChannelRateChange{batch.Changed, batch.Added, batch.Removed} {
		for _, item := range items {
			channels[item.ChannelID] = struct{}{}
		}
	}
	total := len(batch.Changed) + len(batch.Added) + len(batch.Removed)
	return Message{
		Event:   storage.EventRateChanged,
		Subject: fmt.Sprintf("【渠道分组变动】%d 个渠道 · %d 项", len(channels), total),
		Body:    b.String(),
	}
}

func writeRateNotificationSection(b *strings.Builder, title string, items []ChannelRateChange, detail func(RateChange) string) {
	if len(items) == 0 {
		return
	}
	fmt.Fprintf(b, "\n## %s（%d）\n", title, len(items))
	lastChannelID := uint(0)
	for _, item := range items {
		if item.ChannelID != lastChannelID {
			fmt.Fprintf(b, "\n### %s\n", item.ChannelName)
			lastChannelID = item.ChannelID
		}
		fmt.Fprintf(b, "- **%s**：%s\n", item.Change.GroupName, detail(item.Change))
	}
}

func arrowFor(oldV, newV float64) string {
	switch {
	case newV > oldV:
		return "上涨"
	case newV < oldV:
		return "下调"
	default:
		return "调整"
	}
}
