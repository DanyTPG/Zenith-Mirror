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

func TestAllowedChatID(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")

	content := `{
		"app_id": 12345,
		"app_hash": "dummy_hash",
		"bot_token": "dummy_bot_token",
		"owner_id": 777,
		"allowed_chat_id": [103663594, -1002345678901, -987654]
	}`

	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}

	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !cfg.IsOwner(777) {
		t.Errorf("expected 777 to be owner")
	}
	if cfg.IsOwner(103663594) {
		t.Errorf("did not expect 103663594 to be owner")
	}

	// Direct user ID match
	if !cfg.IsChatAllowed(103663594) {
		t.Errorf("expected user 103663594 to be allowed")
	}

	// Supergroup matching: configured -1002345678901 vs MTProto ChannelID 2345678901
	if !cfg.IsChatAllowed(2345678901) {
		t.Errorf("expected channel 2345678901 to match -1002345678901")
	}
	if !cfg.IsChatAllowed(-1002345678901) {
		t.Errorf("expected -1002345678901 to match")
	}

	// Basic group matching: configured -987654 vs MTProto ChatID 987654
	if !cfg.IsChatAllowed(987654) {
		t.Errorf("expected chat 987654 to match -987654")
	}

	// Random unauthorized chat
	if cfg.IsChatAllowed(555555) {
		t.Errorf("unauthorized chat should not be allowed")
	}
}

func TestMultipleOwners(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")

	content := `{
		"app_id": 12345,
		"app_hash": "dummy_hash",
		"bot_token": "dummy_bot_token",
		"owner_id": [777, 888, 999],
		"allowed_chat_id": [103663594]
	}`

	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}

	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !cfg.IsOwner(777) || !cfg.IsOwner(888) || !cfg.IsOwner(999) {
		t.Errorf("expected all 777, 888, 999 to be owners")
	}
	if cfg.IsOwner(103663594) {
		t.Errorf("did not expect 103663594 to be owner")
	}
	if !cfg.IsAllowed(777) || !cfg.IsAllowed(888) || !cfg.IsAllowed(999) {
		t.Errorf("owners must be allowed")
	}
}
