package storage

import (
	"context"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestIsReadQuery(t *testing.T) {
	for q, want := range map[string]bool{
		"SELECT * FROM x":                       true,
		"  select count(*) from sqlite_master":  true,
		"(SELECT 1)":                            true,
		"INSERT INTO x VALUES (1) RETURNING id": false,
		"UPDATE x SET a = 1":                    false,
		"WITH t AS (SELECT 1) DELETE FROM x":    false,
		"PRAGMA table_info(x)":                  false,
	} {
		if got := isReadQuery(q); got != want {
			t.Fatalf("isReadQuery(%q) = %v, want %v", q, got, want)
		}
	}
}

// 读写分离：写事务进行中，读查询不用排队等写连接。
func TestSQLiteReadsNotBlockedByOpenWriteTx(t *testing.T) {
	db := openTestDB(t)
	keys := NewGatewayKeys(db)
	if err := keys.Create(&GatewayKey{GroupID: 1, Name: "k", KeyHash: "h1", KeyPrefix: "p", KeyCipher: "c", Status: GatewayKeyStatusActive}); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	txStarted := make(chan struct{})
	go func() {
		_ = db.Transaction(func(tx *gorm.DB) error {
			if err := tx.Model(&GatewayKey{}).Where("key_hash = ?", "h1").Update("name", "k2").Error; err != nil {
				return err
			}
			close(txStarted)
			<-release
			return nil
		})
	}()
	<-txStarted
	defer close(release)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var got GatewayKey
	start := time.Now()
	if err := db.WithContext(ctx).Where("key_hash = ?", "h1").First(&got).Error; err != nil {
		t.Fatalf("read during an open write transaction failed: %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("read waited %v for the writer", time.Since(start))
	}
	if got.Name != "k" {
		t.Fatalf("reader should see the last committed value, got %q", got.Name)
	}
}
