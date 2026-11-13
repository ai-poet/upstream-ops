package logincaptcha

import (
	"testing"
	"time"
)

func TestStoreExpiresCaptcha(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store := newStore(DefaultTTL, func() time.Time { return now }, 0)
	defer store.Close()

	if err := store.Set("captcha-id", "1234"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	now = now.Add(DefaultTTL)
	if store.Verify("captcha-id", "1234", true) {
		t.Fatal("expected expired captcha to be rejected")
	}
}

func TestStoreConsumesCorrectCaptchaOnce(t *testing.T) {
	store := newStore(DefaultTTL, time.Now, 0)
	defer store.Close()
	if err := store.Set("captcha-id", "a1b2"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if !store.Verify("captcha-id", "A1B2", true) {
		t.Fatal("expected correct captcha to pass case-insensitively")
	}
	if store.Verify("captcha-id", "a1b2", true) {
		t.Fatal("expected consumed captcha to be rejected")
	}
}

func TestStoreConsumesCaptchaOnWrongAnswer(t *testing.T) {
	store := newStore(DefaultTTL, time.Now, 0)
	defer store.Close()
	if err := store.Set("captcha-id", "1234"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if store.Verify("captcha-id", "0000", true) {
		t.Fatal("expected wrong captcha to be rejected")
	}
	if store.Verify("captcha-id", "1234", true) {
		t.Fatal("expected wrong answer to consume captcha")
	}
}

func TestStoreCleansUpExpiredEntries(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store := newStore(time.Minute, func() time.Time { return now }, 0)
	defer store.Close()
	if err := store.Set("expired", "1234"); err != nil {
		t.Fatalf("Set expired: %v", err)
	}
	now = now.Add(2 * time.Minute)
	if err := store.Set("fresh", "5678"); err != nil {
		t.Fatalf("Set fresh: %v", err)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if _, ok := store.entries["expired"]; ok {
		t.Fatal("expected expired entry to be cleaned up")
	}
	if _, ok := store.entries["fresh"]; !ok {
		t.Fatal("expected fresh entry to remain")
	}
}
