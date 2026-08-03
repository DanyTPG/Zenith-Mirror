package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

type Config struct {
	AppID                  int     `json:"app_id"`
	AppHash                string  `json:"app_hash"`
	BotToken               string  `json:"bot_token"`
	SessionFile            string  `json:"session_file"`
	GDriveCredentialsFile  string  `json:"gdrive_credentials_file"`
	GDriveTokenFile        string  `json:"gdrive_token_file"`
	GDriveFolderID         string  `json:"gdrive_folder_id"`
	IndexBaseURL           string  `json:"index_base_url"`
	DownloadMode           string  `json:"download_mode"`    // "stream" (zero-disk) or "parallel" (temp file, faster)
	DownloadThreads        int     `json:"download_threads"` // threads for parallel mode (default 4)
	OwnerID                int64   `json:"owner_id"`
	AllowedUserIDs         []int64 `json:"allowed_user_ids"`
	AuthorizedUsers        []int64 `json:"authorized_users"` // alias for AllowedUserIDs
	MaxConcurrency         int     `json:"max_concurrency"`
	LogFile                string  `json:"log_file"`
	StatusRefreshDelaySec  int     `json:"status_refresh_delay_sec"`
	StatusRefreshDelay     int     `json:"-"`
}

func LoadConfig(path string) (*Config, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("config file error: %w", err)
	}

	mode := info.Mode().Perm()
	if mode&0077 != 0 {
		fmt.Printf("WARNING: Config file %s is world-readable! Setting 0600 permissions.\n", path)
		_ = os.Chmod(path, 0600)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config JSON: %w", err)
	}

	if cfg.AppID == 0 || cfg.AppHash == "" || cfg.BotToken == "" {
		return nil, errors.New("app_id, app_hash, and bot_token are required in config")
	}

	if cfg.SessionFile == "" {
		cfg.SessionFile = "session.json"
	}
	if cfg.LogFile == "" {
		cfg.LogFile = "bot.log"
	}
	if cfg.GDriveCredentialsFile == "" {
		cfg.GDriveCredentialsFile = "credentials.json"
	}
	if cfg.GDriveTokenFile == "" {
		cfg.GDriveTokenFile = "gdrive_token.json"
	}
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = 3
	}
	if cfg.DownloadMode == "" {
		cfg.DownloadMode = "stream"
	}
	if cfg.DownloadThreads <= 0 {
		cfg.DownloadThreads = 16
	}
	cfg.StatusRefreshDelay = cfg.StatusRefreshDelaySec
	if cfg.StatusRefreshDelay <= 0 {
		cfg.StatusRefreshDelay = 5
	}

	return &cfg, nil
}

func (c *Config) IsAllowed(userID int64) bool {
	if c.OwnerID != 0 && userID == c.OwnerID {
		return true
	}
	for _, id := range c.AllowedUserIDs {
		if id == userID {
			return true
		}
	}
	return false
}
