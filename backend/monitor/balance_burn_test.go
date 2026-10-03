package monitor

import (
	"context"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/channel"
	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/notify"
	"github.com/bejix/upstream-ops/backend/storage"
)

func TestRefreshBalanceUpdatesBalanceBurn(t *testing.T) {
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	monitorLogs := storage.NewMonitorLogs(db)
	notifies := storage.NewNotifications(db)
	cipher, err := crypto.NewCipher("secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}

	// 余额 quota 1000000 / quota_per_unit 500000 = 2。
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
	defer apiSrv.Close()

	ch := &storage.Channel{
		Name:           "demo",
		Type:           storage.ChannelTypeNewAPI,
		SiteURL:        apiSrv.URL,
		Username:       "u",
		PasswordCipher: mustEncrypt(t, cipher, `{"cookie":"session=1","user_id":"7"}`),
		CredentialMode: storage.CredentialModeToken,
		MonitorEnabled: true,
	}
	if err := channels.Create(ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	channelSvc := channel.NewService(channels, storage.NewAuthSessions(db), storage.NewCaptchas(db), rates, monitorLogs, cipher)
	dispatcher := notify.NewDispatcher(notifies, cipher, slog.New(slog.NewTextHandler(io.Discard, nil)), notify.Policy{SendMaxAttempts: 1})
	svc := NewService(channels, storage.NewUpstreamAnnouncements(db), rates, monitorLogs, channelSvc, dispatcher, slog.New(slog.NewTextHandler(io.Discard, nil)))

	burnOf := func() (*float64, *float64) {
		t.Helper()
		got, err := channels.FindByID(ch.ID)
		if err != nil {
			t.Fatalf("find channel: %v", err)
		}
		return got.BalanceDailyCost, got.BalanceCostSpanHours
	}

	// 第一次采样只有一条快照，样本不足，不估算。
	if err := svc.RefreshBalance(context.Background(), ch); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	if daily, span := burnOf(); daily != nil || span != nil {
		t.Fatalf("first refresh burn = %v / %v, want nil / nil", daily, span)
	}

	// 补上 12 小时前余额 5、6 小时前余额 3.5 的历史；8 天前的快照超出窗口，不参与估算。
	now := time.Now()
	for _, s := range []storage.BalanceSnapshot{
		{ChannelID: ch.ID, Balance: 1000, SampledAt: now.Add(-8 * 24 * time.Hour)},
		{ChannelID: ch.ID, Balance: 5, SampledAt: now.Add(-12 * time.Hour)},
		{ChannelID: ch.ID, Balance: 3.5, SampledAt: now.Add(-6 * time.Hour)},
	} {
		s := s
		if err := rates.AppendBalance(&s); err != nil {
			t.Fatalf("append balance: %v", err)
		}
	}
	if err := db.Where("channel_id = ? AND sampled_at > ?", ch.ID, now.Add(-time.Minute)).Delete(&storage.BalanceSnapshot{}).Error; err != nil {
		t.Fatalf("drop first snapshot: %v", err)
	}

	// 5 → 3.5 → 2：12 小时消耗 3，日均约 6。
	if err := svc.RefreshBalance(context.Background(), ch); err != nil {
		t.Fatalf("second refresh: %v", err)
	}
	daily, span := burnOf()
	if daily == nil || span == nil {
		t.Fatalf("second refresh burn = %v / %v, want values", daily, span)
	}
	if math.Abs(*daily-6) > 0.01 || math.Abs(*span-12) > 0.01 {
		t.Fatalf("second refresh burn = %v / %v, want ~6 / ~12", *daily, *span)
	}
}
