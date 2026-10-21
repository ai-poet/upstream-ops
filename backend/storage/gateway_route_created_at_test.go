package storage

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"gorm.io/gorm"
)

// 线上 MySQL 是严格模式（NO_ZERO_DATE），UPDATE 把 created_at 写成零值会报
// Error 1292: Incorrect datetime value: '0000-00-00'。SQLite 不拒绝零值，这里用回调模拟同样的约束。
func rejectZeroRouteCreatedAt(db *gorm.DB) {
	_ = db.Callback().Update().Before("gorm:update").Register("test:reject_zero_created_at", func(tx *gorm.DB) {
		if r, ok := tx.Statement.Dest.(*GatewayRoute); ok && r.ID != 0 && r.CreatedAt.IsZero() {
			_ = tx.AddError(errors.New("Error 1292 (22007): Incorrect datetime value: '0000-00-00' for column 'created_at'"))
		}
	})
}

// 已有路由换了来源渠道（或直连渠道）：保存时沿用原来的创建时间，不能写回零值。
func TestGatewayRoutesSaveForGroupKeepsCreatedAtWhenSourceChanges(t *testing.T) {
	db, err := Open(DBConfig{Driver: DBDriverSQLite, Path: filepath.Join(t.TempDir(), "gw-route-created.db"), MaxOpenConns: 5, MaxIdleConns: 2})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	sqlDB, _ := db.DB()
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := AutoMigrate(db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	rejectZeroRouteCreatedAt(db)

	routes := NewGatewayRoutes(db)
	if err := routes.SaveForGroup(3, []GatewayRoute{
		{SourceKind: GatewayRouteSourceMonitor, SourceChannelID: 1, Weight: 1, Enabled: true},
		{SourceKind: GatewayRouteSourceMonitor, SourceChannelID: 2, Weight: 1, Enabled: true},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	before, _ := routes.ListByGroupID(3)
	created := map[uint]time.Time{before[0].ID: before[0].CreatedAt, before[1].ID: before[1].CreatedAt}

	// 前端回传的路由不带 created_at：第一条换监控渠道，第二条改成直连渠道
	if err := routes.SaveForGroup(3, []GatewayRoute{
		{ID: before[0].ID, SourceKind: GatewayRouteSourceMonitor, SourceChannelID: 9, Weight: 1, Enabled: true},
		{ID: before[1].ID, SourceKind: GatewayRouteSourceProvider, GatewayProviderID: 4, Weight: 1, Enabled: true},
	}); err != nil {
		t.Fatalf("save after changing sources: %v", err)
	}
	after, _ := routes.ListByGroupID(3)
	for _, r := range after {
		want, ok := created[r.ID]
		if !ok {
			t.Fatalf("route id changed: %+v", r)
		}
		if r.CreatedAt.IsZero() || !r.CreatedAt.Equal(want) {
			t.Fatalf("route %d created_at = %v, want %v", r.ID, r.CreatedAt, want)
		}
	}
	if after[0].SourceChannelID != 9 || after[1].GatewayProviderID != 4 {
		t.Fatalf("sources not updated: %+v", after)
	}
}
