package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestConfigLoadAndValidate(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")

	content := `{
		"app_id": 12345,
		"app_hash": "dummy_hash",
		"bot_token": "dummy_bot_token",
		"gdrive_credentials_file": "credentials.json",
		"index_base_url": "https://dl2.duhost.workers.dev/",
		"owner_id": 1001,
		"allowed_user_ids": [1002, 1003],
		"max_concurrency": 5
	}`

	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}

	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.AppID != 12345 || cfg.AppHash != "dummy_hash" {
		t.Errorf("config parsed incorrectly: %+v", cfg)
	}

	if !cfg.IsAllowed(1001) || !cfg.IsAllowed(1002) {
		t.Errorf("authorization check failed")
	}

	if cfg.IsAllowed(9999) {
		t.Errorf("unauthorized user allowed")
	}
}
