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
		"app_hash": "abcdef",
		"gdrive_sa_file": "sa.json",
		"gdrive_folder_id": "folder_123",
		"owner_id": 9999
	}`

	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}

	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.MaxConcurrency != 3 {
		t.Errorf("expected default concurrency 3, got %d", cfg.MaxConcurrency)
	}

	if !cfg.IsAllowed(9999) {
		t.Errorf("owner should be allowed")
	}
	if cfg.IsAllowed(1111) {
		t.Errorf("unauthorized user should be rejected")
	}
}
