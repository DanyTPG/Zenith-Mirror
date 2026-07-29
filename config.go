package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

type Config struct {
	AppID          int     `json:"app_id"`
	AppHash        string  `json:"app_hash"`
	BotToken       string  `json:"bot_token,omitempty"`
	PhoneNumber    string  `json:"phone_number,omitempty"`
	SessionFile    string  `json:"session_file"`
	GDriveSAFile   string  `json:"gdrive_sa_file"`
	GDriveFolderID string  `json:"gdrive_folder_id"`
	OwnerID        int64   `json:"owner_id"`
	AllowedUsers   []int64 `json:"allowed_users"`
	MaxConcurrency int     `json:"max_concurrency"`
}

func LoadConfig(path string) (*Config, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("config file stat error: %w", err)
	}

	// Perms check: warn/reject if world readable
	if info.Mode().Perm()&0004 != 0 {
		fmt.Fprintf(os.Stderr, "WARNING: Config file %s is world-readable! Setting 0600 permissions.\n", path)
		_ = os.Chmod(path, 0600)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("invalid json in config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return &cfg, nil
}

func (c *Config) Validate() error {
	if c.AppID == 0 {
		return errors.New("app_id is required")
	}
	if c.AppHash == "" {
		return errors.New("app_hash is required")
	}
	if c.GDriveSAFile == "" {
		return errors.New("gdrive_sa_file is required")
	}
	if c.GDriveFolderID == "" {
		return errors.New("gdrive_folder_id is required")
	}
	if c.OwnerID == 0 {
		return errors.New("owner_id is required")
	}
	if c.SessionFile == "" {
		c.SessionFile = "session.json"
	}
	if c.MaxConcurrency <= 0 {
		c.MaxConcurrency = 3
	}
	return nil
}

func (c *Config) IsAllowed(userID int64) bool {
	if userID == c.OwnerID {
		return true
	}
	for _, id := range c.AllowedUsers {
		if id == userID {
			return true
		}
	}
	return false
}
