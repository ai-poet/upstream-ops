package monitor

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/channel"
	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/notify"
	"github.com/bejix/upstream-ops/backend/storage"
	"gorm.io/gorm"
)

type balanceBurnFixture struct {
	db       *gorm.DB
	channels *storage.Channels
	rates    *storage.Rates
	svc      *Service
	ch       *storage.Channel

	mu       sync.Mutex
	webhooks []map[string]any // webhook 收到的通知
}

func (f *balanceBurnFixture) sent() []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]map[string]any(nil), f.webhooks...)
}

// newBalanceBurnFixture 准备一个余额恒为 2 的 NewAPI 渠道（quota 1000000 / quota_per_unit 500000）
// 和一个接收全部事件的 webhook 通知渠道。
func newBalanceBurnFixture(t *testing.T, policy notify.Policy) *balanceBurnFixture {
	t.Helper()
	db := openTestDB(t)
	f := &balanceBurnFixture{db: db, channels: storage.NewChannels(db), rates: storage.NewRates(db)}
	monitorLogs := storage.NewMonitorLogs(db)
	notifies := storage.NewNotifications(db)
	cipher, err := crypto.NewCipher("secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}

	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/status":
			_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"quota_per_unit":500000}}`))
		case "/api/user/self":
			_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"quota":1000000,"used_quota":500000}}`))
		case "/api/log/self/stat":
			_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"quota":0}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(apiSrv.Close)

	webhookSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode webhook: %v", err)
		}
		f.mu.Lock()
		f.webhooks = append(f.webhooks, body)
		f.mu.Unlock()
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(webhookSrv.Close)
	if err := notifies.CreateChannel(&storage.NotificationChannel{
		Name:         "webhook",
		Type:         storage.NotifyWebhook,
		ConfigCipher: mustEncrypt(t, cipher, `{"url":"`+webhookSrv.URL+`"}`),
	}); err != nil {
		t.Fatalf("create notify channel: %v", err)
	}

	f.ch = &storage.Channel{
		Name:           "demo",
		Type:           storage.ChannelTypeNewAPI,
		SiteURL:        apiSrv.URL,
		Username:       "u",
		PasswordCipher: mustEncrypt(t, cipher, `{"cookie":"session=1","user_id":"7"}`),
		CredentialMode: storage.CredentialModeToken,
		MonitorEnabled: true,
	}
	if err := f.channels.Create(f.ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	if policy.SendMaxAttempts == 0 {
		policy.SendMaxAttempts = 1
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	channelSvc := channel.NewService(f.channels, storage.NewAuthSessions(db), storage.NewCaptchas(db), f.rates, monitorLogs, cipher)
	dispatcher := notify.NewDispatcher(notifies, cipher, logger, policy)
	f.svc = NewService(f.channels, storage.NewUpstreamAnnouncements(db), f.rates, monitorLogs, channelSvc, dispatcher, logger)
	return f
}

// seedBalances 写入相对 now 的历史余额快照（ago 为距今时长）。
func (f *balanceBurnFixture) seedBalances(t *testing.T, now time.Time, history map[time.Duration]float64) {
	t.Helper()
	for ago, balance := range history {
		s := storage.BalanceSnapshot{ChannelID: f.ch.ID, Balance: balance, SampledAt: now.Add(-ago)}
		if err := f.rates.AppendBalance(&s); err != nil {
			t.Fatalf("append balance: %v", err)
		}
	}
}

func TestRefreshBalanceUpdatesBalanceBurn(t *testing.T) {
	f := newBalanceBurnFixture(t, notify.Policy{})

	burnOf := func() (*float64, *float64) {
		t.Helper()
		got, err := f.channels.FindByID(f.ch.ID)
		if err != nil {
			t.Fatalf("find channel: %v", err)
		}
		return got.BalanceDailyCost, got.BalanceCostSpanHours
	}

	// 第一次采样只有一条快照，样本不足，不估算。
	if err := f.svc.RefreshBalance(context.Background(), f.ch); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	if daily, span := burnOf(); daily != nil || span != nil {
		t.Fatalf("first refresh burn = %v / %v, want nil / nil", daily, span)
	}

	// 换成 12 小时前余额 5、6 小时前余额 3.5 的历史；8 天前的快照超出窗口，不参与估算。
	if err := f.db.Where("channel_id = ?", f.ch.ID).Delete(&storage.BalanceSnapshot{}).Error; err != nil {
		t.Fatalf("drop snapshots: %v", err)
	}
	f.seedBalances(t, time.Now(), map[time.Duration]float64{
		8 * 24 * time.Hour: 1000,
		12 * time.Hour:     5,
		6 * time.Hour:      3.5,
	})

	// 5 → 3.5 → 2：12 小时消耗 3，日均约 6。
	if err := f.svc.RefreshBalance(context.Background(), f.ch); err != nil {
		t.Fatalf("second refresh: %v", err)
	}
	daily, span := burnOf()
	if daily == nil || span == nil {
		t.Fatalf("second refresh burn = %v / %v, want values", daily, span)
	}
	if math.Abs(*daily-6) > 0.01 || math.Abs(*span-12) > 0.01 {
		t.Fatalf("second refresh burn = %v / %v, want ~6 / ~12", *daily, *span)
	}
	// 未开启即将用完提醒（BalanceDepletionLead = 0）。
	if got := f.sent(); len(got) != 0 {
		t.Fatalf("webhooks = %v, want none", got)
	}
}

// 近 7 天日均很低，只有最近 2 小时在大量消耗：按日均估算还能用一天多，按最近速度不到 1 小时就会用完。
func TestRefreshBalanceAlertsOnRecentBurn(t *testing.T) {
	f := newBalanceBurnFixture(t, notify.Policy{BalanceDepletionLead: time.Hour, BalanceLowCooldown: 10 * time.Minute})
	// 8.5 → 8 → 5 → 2（本次采样）：5 天消耗 6.5，日均 1.3，余额 2 约 37 小时；
	// 最近 2 小时 8 → 2，每小时约 3，余额 2 约 40 分钟。
	// 窗口从本次采样时间往前算，"2 小时前"的快照留 1 分钟余量，避免因测试执行耗时落到窗口外。
	f.seedBalances(t, time.Now(), map[time.Duration]float64{
		5 * 24 * time.Hour: 8.5,
		119 * time.Minute:  8,
		time.Hour:          5,
	})

	if err := f.svc.RefreshBalance(context.Background(), f.ch); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	sent := f.sent()
	if len(sent) != 1 {
		t.Fatalf("webhooks = %d, want 1: %v", len(sent), sent)
	}
	if sent[0]["event"] != string(storage.EventBalanceDepleting) {
		t.Fatalf("event = %v, want balance_depleting", sent[0]["event"])
	}
	text, _ := json.Marshal(sent[0])
	for _, want := range []string{"demo 余额预计", "当前余额：2.0000", "最近 2 小时每小时消耗约 3.0", "约 40 分钟后", "预计 1.0 小时内用完"} {
		if !strings.Contains(string(text), want) {
			t.Fatalf("webhook %s missing %q", text, want)
		}
	}

	// 冷却取提前量与余额不足冷却中较长的一个（1 小时），下一轮采样不重复提醒。
	if err := f.svc.RefreshBalance(context.Background(), f.ch); err != nil {
		t.Fatalf("second refresh: %v", err)
	}
	if got := len(f.sent()); got != 1 {
		t.Fatalf("webhooks after second refresh = %d, want 1", got)
	}
}

func TestCheckBalanceDepletion(t *testing.T) {
	burn := func(daily float64) *storage.BalanceBurn {
		return &storage.BalanceBurn{DailyCost: daily, SpanHours: 168}
	}
	cases := []struct {
		name         string
		lead         time.Duration
		balance      float64
		week, recent *storage.BalanceBurn
		want         bool
	}{
		// 日均 48（每小时 2），余额 1.5 → 45 分钟。
		{"按日均进入提醒", time.Hour, 1.5, burn(48), nil, true},
		{"刚好等于提前量", time.Hour, 2, burn(48), nil, true},
		{"还早", time.Hour, 2.5, burn(48), nil, false},
		{"关闭提醒", 0, 1.5, burn(48), nil, false},
		{"余额已用完", time.Hour, 0, burn(48), burn(48), false},
		{"样本不足", time.Hour, 1, nil, nil, false},
		{"没有消耗", time.Hour, 1, burn(0), burn(0), false},
		// 最近速度慢于日均时按日均：余额 1.5，日均 48 → 45 分钟。
		{"取较快的速度", time.Hour, 1.5, burn(48), burn(1), true},
		{"只有最近速度", time.Hour, 1.5, nil, burn(48), true},
		{"提前量较长", 24 * time.Hour, 40, burn(48), nil, true},
		// 余额很多、消耗极少：换算成 time.Duration 会溢出，必须先按小时比较。
		{"超长时间不溢出", time.Hour, 1e12, burn(1e-9), nil, false},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newBalanceBurnFixture(t, notify.Policy{BalanceDepletionLead: tc.lead})
			c := *f.ch
			c.ID += uint(i) // 各用例独立冷却
			f.svc.checkBalanceDepletion(context.Background(), &c, tc.balance, time.Now(), tc.week, tc.recent)
			if got := len(f.sent()) == 1; got != tc.want {
				t.Fatalf("sent = %v, want %v", f.sent(), tc.want)
			}
		})
	}
}
