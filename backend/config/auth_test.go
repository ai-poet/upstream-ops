package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoginCaptchaDefaultsEnabled(t *testing.T) {
	cfg, err := LoadFile(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if !cfg.Auth.CaptchaEnabled {
		t.Fatal("expected login captcha to be enabled by default")
	}
}

func TestLoginCaptchaCanBeDisabled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("auth:\n  captchaEnabled: false\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if cfg.Auth.CaptchaEnabled {
		t.Fatal("expected login captcha to be disabled")
	}
}
