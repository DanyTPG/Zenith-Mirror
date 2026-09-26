package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Int64List []int64

func (l *Int64List) UnmarshalJSON(data []byte) error {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" || trimmed == "null" {
		return nil
	}
	var single int64
	if err := json.Unmarshal(data, &single); err == nil {
		if single != 0 {
			*l = []int64{single}
		}
		return nil
	}
	var arr []int64
	if err := json.Unmarshal(data, &arr); err == nil {
		*l = arr
		return nil
	}
	return fmt.Errorf("expected int64 or []int64, got %s", trimmed)
}

type Config struct {
	mu                     sync.RWMutex
	AppID                  int       `json:"app_id"`
	AppHash                string    `json:"app_hash"`
	BotToken               string    `json:"bot_token"`
	SessionFile            string    `json:"session_file"`
	GDriveCredentialsFile  string    `json:"gdrive_credentials_file"`
	GDriveTokenFile        string    `json:"gdrive_token_file"`
	GDriveFolderID         string    `json:"gdrive_folder_id"`
	IndexBaseURL           string    `json:"index_base_url"`
	DownloadMode           string    `json:"download_mode"`    // "stream" (zero-disk) or "parallel" (temp file, faster)
	DownloadThreads        int       `json:"download_threads"` // threads for parallel mode (default 4)
	PartSize               int       `json:"part_size"`        // chunk size in bytes, multiple of 4096 (default 524288)
	MaxConcurrentDownloads int       `json:"max_concurrent_downloads"` // global cap on simultaneous file downloads (default 4)
	RPCDelay               time.Duration `json:"-"`          // rate limiter: min interval between RPCs
	RPCBurst               int       `json:"rpc_burst"`        // rate limiter: token bucket burst (default 5)
	RPCRatePerSec          float64   `json:"rpc_rate_per_sec"` // rate limiter: sustained RPCs/sec (default 10)
	OwnerID                Int64List `json:"owner_id"`
	OwnerIDs               Int64List `json:"owner_ids"`                // alias for OwnerID
	AllowedChatID          Int64List `json:"allowed_chat_id"`
	AllowedChatIDs         Int64List `json:"allowed_chat_ids"`         // alias for AllowedChatID
	AllowedUserIDs         Int64List `json:"allowed_user_ids"`         // legacy alias
	AuthorizedUsers        Int64List `json:"authorized_users"`        // legacy alias
	MaxConcurrency         int       `json:"max_concurrency"`
	LogFile                string    `json:"log_file"`
	StatusRefreshDelaySec  int       `json:"status_refresh_delay_sec"`
	StatusRefreshDelay     int       `json:"-"`
	TorrentDownloadDir     string    `json:"torrent_download_dir"`  // temp dir for torrent pieces (default "torrent_downloads")
	TorrentListenPort      int       `json:"torrent_listen_port"`   // DHT listen port (default 0 = random)
	JobStateFile           string    `json:"job_state_file"`        // persistence file for restart resume (default "jobs_state.json")
	FeedStateFile          string    `json:"feed_state_file"`       // persistence file for RSS/Atom feeds (default "feeds_state.json")
	FeedCheckIntervalSec   int       `json:"feed_check_interval_sec"` // RSS check interval in seconds (default 600)
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
		cfg.DownloadThreads = 4
	}
	if cfg.PartSize <= 0 || cfg.PartSize%4096 != 0 {
		cfg.PartSize = 512 * 1024
	}
	if cfg.MaxConcurrentDownloads <= 0 {
		cfg.MaxConcurrentDownloads = 4
	}
	if cfg.RPCBurst <= 0 {
		cfg.RPCBurst = 5
	}
	if cfg.RPCRatePerSec <= 0 {
		cfg.RPCRatePerSec = 10
	}
	cfg.RPCDelay = time.Duration(float64(time.Second) / cfg.RPCRatePerSec)
	cfg.StatusRefreshDelay = cfg.StatusRefreshDelaySec
	if cfg.StatusRefreshDelay <= 0 {
		cfg.StatusRefreshDelay = 5
	}
	if cfg.TorrentDownloadDir == "" {
		cfg.TorrentDownloadDir = "torrent_downloads"
	}
	if cfg.TorrentListenPort < 0 {
		cfg.TorrentListenPort = 0
	}
	if cfg.JobStateFile == "" {
		cfg.JobStateFile = "jobs_state.json"
	}
	if cfg.FeedStateFile == "" {
		cfg.FeedStateFile = "feeds_state.json"
	}
	if cfg.FeedCheckIntervalSec <= 0 {
		cfg.FeedCheckIntervalSec = 600
	}

	// Merge all allowed chat ID variations
	seen := make(map[int64]bool)
	var merged []int64
	addID := func(id int64) {
		if id != 0 && !seen[id] {
			seen[id] = true
			merged = append(merged, id)
		}
	}
	for _, id := range cfg.AllowedChatID {
		addID(id)
	}
	for _, id := range cfg.AllowedChatIDs {
		addID(id)
	}
	for _, id := range cfg.AllowedUserIDs {
		addID(id)
	}
	for _, id := range cfg.AuthorizedUsers {
		addID(id)
	}
	cfg.AllowedChatID = merged
	cfg.AllowedChatIDs = merged
	cfg.AllowedUserIDs = merged
	cfg.AuthorizedUsers = merged

	// Merge all owner ID variations
	seenOwner := make(map[int64]bool)
	var mergedOwners []int64
	addOwner := func(id int64) {
		if id != 0 && !seenOwner[id] {
			seenOwner[id] = true
			mergedOwners = append(mergedOwners, id)
		}
	}
	for _, id := range cfg.OwnerID {
		addOwner(id)
	}
	for _, id := range cfg.OwnerIDs {
		addOwner(id)
	}
	cfg.OwnerID = mergedOwners
	cfg.OwnerIDs = mergedOwners

	return &cfg, nil
}

func (c *Config) Reload(path string) error {
	newCfg, err := LoadConfig(path)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.OwnerID = newCfg.OwnerID
	c.OwnerIDs = newCfg.OwnerIDs
	c.AllowedChatID = newCfg.AllowedChatID
	c.AllowedChatIDs = newCfg.AllowedChatIDs
	c.AllowedUserIDs = newCfg.AllowedUserIDs
	c.AuthorizedUsers = newCfg.AuthorizedUsers
	c.MaxConcurrency = newCfg.MaxConcurrency
	c.StatusRefreshDelaySec = newCfg.StatusRefreshDelaySec
	c.StatusRefreshDelay = newCfg.StatusRefreshDelay
	c.FeedCheckIntervalSec = newCfg.FeedCheckIntervalSec

	return nil
}

func (c *Config) IsOwner(userID int64) bool {
	if userID == 0 {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, id := range c.OwnerID {
		if id == userID {
			return true
		}
	}
	return false
}

func (c *Config) IsChatAllowed(chatID int64) bool {
	if chatID == 0 {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, id := range c.AllowedChatID {
		if matchChatID(id, chatID) {
			return true
		}
	}
	return false
}

func (c *Config) IsAllowed(id int64) bool {
	return c.IsOwner(id) || c.IsChatAllowed(id)
}

func matchChatID(configured int64, peerID int64) bool {
	if configured == peerID {
		return true
	}
	if configured == -peerID || -configured == peerID {
		return true
	}
	sConfig := strconv.FormatInt(configured, 10)
	sPeer := strconv.FormatInt(peerID, 10)

	norm := func(s string) string {
		if strings.HasPrefix(s, "-100") {
			return s[4:]
		}
		if strings.HasPrefix(s, "-") {
			return s[1:]
		}
		return s
	}
	return norm(sConfig) == norm(sPeer)
}
