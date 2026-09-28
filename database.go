package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type UserPlan int

const (
	PlanAdmin     UserPlan = 0
	PlanFree      UserPlan = 1
	PlanTier2     UserPlan = 2
	PlanTier3     UserPlan = 3
	PlanUnlimited UserPlan = 4
)

const (
	ByteUnitKB = 1024
	ByteUnitMB = 1024 * ByteUnitKB
	ByteUnitGB = 1024 * ByteUnitMB
	ByteUnitTB = 1024 * ByteUnitGB
)

func (p UserPlan) String() string {
	switch p {
	case PlanAdmin:
		return "Admin (Unlimited)"
	case PlanFree:
		return "Free (10 GB / day, 50 GB / mo)"
	case PlanTier2:
		return "Tier 2 (20 GB / day, 200 GB / mo)"
	case PlanTier3:
		return "Tier 3 (200 GB / day, 2 TB / mo)"
	case PlanUnlimited:
		return "Unlimited"
	default:
		return "Free"
	}
}

func PlanLimits(plan UserPlan) (dailyLimit int64, monthlyLimit int64) {
	switch plan {
	case PlanAdmin, PlanUnlimited:
		return 0, 0 // 0 = unlimited
	case PlanTier2:
		return 20 * ByteUnitGB, 200 * ByteUnitGB
	case PlanTier3:
		return 200 * ByteUnitGB, 2 * ByteUnitTB
	case PlanFree:
		fallthrough
	default:
		return 10 * ByteUnitGB, 50 * ByteUnitGB
	}
}

type UserRecord struct {
	ID           int64      `json:"id"`
	Username     string     `json:"username"`
	FirstName    string     `json:"first_name"`
	AccessHash   int64      `json:"access_hash"`
	Plan         UserPlan   `json:"plan"`
	PlanExpiryAt *time.Time `json:"plan_expiry_at,omitempty"`
	UsedBytes    int64      `json:"used_bytes"`
	DailyBytes   int64      `json:"daily_bytes"`
	MonthlyBytes int64      `json:"monthly_bytes"`
	LastResetAt  time.Time  `json:"last_reset_at"`
	CreatedAt    time.Time  `json:"created_at"`
}

type DB struct {
	db  *sql.DB
	cfg *Config
	mu  sync.RWMutex
}

func OpenDB(path string, cfg *Config) (*DB, error) {
	if path == "" {
		path = "zenith.db"
	}

	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		_ = os.MkdirAll(dir, 0755)
	}

	dsn := fmt.Sprintf("%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)", path)
	sqlDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed opening sqlite db: %w", err)
	}

	// SQLite handles concurrent reads and single-writer in WAL mode cleanly
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)
	sqlDB.SetConnMaxLifetime(0)

	d := &DB{
		db:  sqlDB,
		cfg: cfg,
	}

	if err := d.initSchema(); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("failed initializing sqlite schema: %w", err)
	}

	// Migrate old JSON storage files if present
	d.migrateLegacyFiles()

	return d, nil
}

func (d *DB) Close() error {
	if d == nil || d.db == nil {
		return nil
	}
	return d.db.Close()
}

func (d *DB) initSchema() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS users (
			id INTEGER PRIMARY KEY,
			username TEXT NOT NULL DEFAULT '',
			first_name TEXT NOT NULL DEFAULT '',
			access_hash INTEGER NOT NULL DEFAULT 0,
			plan INTEGER NOT NULL DEFAULT 1,
			plan_expiry_at DATETIME,
			used_bytes INTEGER NOT NULL DEFAULT 0,
			daily_bytes INTEGER NOT NULL DEFAULT 0,
			monthly_bytes INTEGER NOT NULL DEFAULT 0,
			last_reset_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE TABLE IF NOT EXISTS jobs (
			id TEXT PRIMARY KEY,
			user_id INTEGER NOT NULL,
			type TEXT NOT NULL,
			kind TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'queued',
			file_name TEXT NOT NULL DEFAULT '',
			size INTEGER NOT NULL DEFAULT 0,
			raw_url TEXT NOT NULL DEFAULT '',
			magnet_uri TEXT NOT NULL DEFAULT '',
			torrent_bytes BLOB,
			target_json TEXT NOT NULL DEFAULT '',
			location_json TEXT NOT NULL DEFAULT '',
			error TEXT NOT NULL DEFAULT '',
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			completed_at DATETIME
		);`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs(status, created_at);`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_user_status ON jobs(user_id, status);`,
		`CREATE TABLE IF NOT EXISTS feeds (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			user_id INTEGER NOT NULL,
			name TEXT NOT NULL,
			mode TEXT NOT NULL,
			url TEXT NOT NULL,
			includes TEXT NOT NULL DEFAULT '',
			excludes TEXT NOT NULL DEFAULT '',
			last_guid TEXT NOT NULL DEFAULT '',
			last_title TEXT NOT NULL DEFAULT '',
			target_json TEXT NOT NULL DEFAULT '',
			paused INTEGER NOT NULL DEFAULT 0,
			last_checked DATETIME,
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE INDEX IF NOT EXISTS idx_feeds_user ON feeds(user_id);`,
	}

	for _, q := range queries {
		if _, err := d.db.Exec(q); err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) migrateLegacyFiles() {
	if d.cfg == nil {
		return
	}

	// 1. dm_users.json migration
	dmPath := d.cfg.DMUsersFile
	if dmPath == "" {
		dmPath = "dm_users.json"
	}
	if data, err := os.ReadFile(dmPath); err == nil && len(data) > 0 {
		var list []DMUser
		if err := json.Unmarshal(data, &list); err == nil && len(list) > 0 {
			slog.Info("migrating legacy dm_users.json into sqlite database", "count", len(list))
			for _, u := range list {
				plan := PlanFree
				if d.cfg.IsOwner(u.UserID) {
					plan = PlanAdmin
				}
				_, _ = d.db.Exec(`
					INSERT INTO users (id, username, first_name, access_hash, plan, created_at)
					VALUES (?, ?, ?, ?, ?, ?)
					ON CONFLICT(id) DO UPDATE SET
						username = CASE WHEN excluded.username != '' THEN excluded.username ELSE users.username END,
						first_name = CASE WHEN excluded.first_name != '' THEN excluded.first_name ELSE users.first_name END,
						access_hash = CASE WHEN excluded.access_hash != 0 THEN excluded.access_hash ELSE users.access_hash END,
						plan = CASE WHEN excluded.plan = 0 THEN 0 ELSE users.plan END;`,
					u.UserID, u.Username, u.FirstName, u.AccessHash, plan, u.StartedAt,
				)
			}
			_ = os.Rename(dmPath, dmPath+".migrated")
		}
	}

	// 2. feeds_state.json migration
	feedPath := d.cfg.FeedStateFile
	if feedPath == "" {
		feedPath = "feeds_state.json"
	}
	if data, err := os.ReadFile(feedPath); err == nil && len(data) > 0 {
		var count int
		_ = d.db.QueryRow("SELECT COUNT(*) FROM feeds").Scan(&count)
		if count == 0 {
			var list []*FeedSubscription
			if err := json.Unmarshal(data, &list); err == nil && len(list) > 0 {
				slog.Info("migrating legacy feeds_state.json into sqlite database", "count", len(list))
				for _, f := range list {
					_ = d.SaveFeed(f)
				}
			}
		}
		_ = os.Rename(feedPath, feedPath+".migrated")
	}

	// 3. jobs_state.json migration
	jobPath := d.cfg.JobStateFile
	if jobPath == "" {
		jobPath = "jobs_state.json"
	}
	if data, err := os.ReadFile(jobPath); err == nil && len(data) > 0 {
		var count int
		_ = d.db.QueryRow("SELECT COUNT(*) FROM jobs WHERE status IN ('queued', 'running')").Scan(&count)
		if count == 0 {
			var list []PersistedJob
			if err := json.Unmarshal(data, &list); err == nil && len(list) > 0 {
				slog.Info("migrating legacy jobs_state.json into sqlite database", "count", len(list))
				for _, j := range list {
					targetData, _ := json.Marshal(j.Target)
					var locData []byte
					if j.Location != nil {
						locData, _ = json.Marshal(j.Location)
					}
					_, _ = d.db.Exec(`
						INSERT INTO jobs (id, user_id, type, kind, status, file_name, size, raw_url, magnet_uri, torrent_bytes, target_json, location_json, created_at)
						VALUES (?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?)
						ON CONFLICT(id) DO NOTHING;`,
						j.ID, j.UserID, string(j.Type), j.Kind, j.FileName, j.Size, j.RawURL, j.MagnetURI, j.TorrentBytes, string(targetData), string(locData), j.CreatedAt,
					)
				}
			}
		}
		_ = os.Rename(jobPath, jobPath+".migrated")
	}
}

// -------------------------------------------------------------
// User & Accounting Operations
// -------------------------------------------------------------

func (d *DB) UpsertUser(userID, accessHash int64, username, firstName string) error {
	if userID == 0 {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	plan := PlanFree
	if d.cfg != nil && d.cfg.IsOwner(userID) {
		plan = PlanAdmin
	}

	_, err := d.db.Exec(`
		INSERT INTO users (id, username, first_name, access_hash, plan, last_reset_at, created_at)
		VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		ON CONFLICT(id) DO UPDATE SET
			username = CASE WHEN excluded.username != '' THEN excluded.username ELSE users.username END,
			first_name = CASE WHEN excluded.first_name != '' THEN excluded.first_name ELSE users.first_name END,
			access_hash = CASE WHEN excluded.access_hash != 0 THEN excluded.access_hash ELSE users.access_hash END,
			plan = CASE WHEN excluded.plan = 0 THEN 0 ELSE users.plan END;`,
		userID, username, firstName, accessHash, plan,
	)
	return err
}

func (d *DB) UnregisterDM(userID int64) error {
	if userID == 0 {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec("UPDATE users SET access_hash = 0 WHERE id = ?", userID)
	return err
}

func (d *DB) HasDMUser(userID int64) bool {
	if userID == 0 {
		return false
	}
	d.mu.RLock()
	defer d.mu.RUnlock()

	var accessHash int64
	err := d.db.QueryRow("SELECT access_hash FROM users WHERE id = ?", userID).Scan(&accessHash)
	return err == nil && accessHash != 0
}

func (d *DB) GetUser(userID int64) (*UserRecord, error) {
	if userID == 0 {
		return nil, sql.ErrNoRows
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	var u UserRecord
	var expiry sql.NullTime
	err := d.db.QueryRow(`
		SELECT id, username, first_name, access_hash, plan, plan_expiry_at,
		       used_bytes, daily_bytes, monthly_bytes, last_reset_at, created_at
		FROM users WHERE id = ?`, userID).Scan(
		&u.ID, &u.Username, &u.FirstName, &u.AccessHash, &u.Plan, &expiry,
		&u.UsedBytes, &u.DailyBytes, &u.MonthlyBytes, &u.LastResetAt, &u.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	if expiry.Valid {
		u.PlanExpiryAt = &expiry.Time
	}

	// Automatic quota cycle reset check
	now := time.Now().UTC()
	needsDailyReset := u.LastResetAt.UTC().Day() != now.Day() || u.LastResetAt.UTC().Month() != now.Month() || u.LastResetAt.UTC().Year() != now.Year()
	needsMonthlyReset := u.LastResetAt.UTC().Month() != now.Month() || u.LastResetAt.UTC().Year() != now.Year()

	if needsDailyReset || needsMonthlyReset {
		if needsDailyReset {
			u.DailyBytes = 0
		}
		if needsMonthlyReset {
			u.MonthlyBytes = 0
		}
		u.LastResetAt = now
		_, _ = d.db.Exec("UPDATE users SET daily_bytes = ?, monthly_bytes = ?, last_reset_at = ? WHERE id = ?",
			u.DailyBytes, u.MonthlyBytes, u.LastResetAt, u.ID)
	}

	// Plan expiration check: revert to PlanFree if expired
	if u.PlanExpiryAt != nil && u.PlanExpiryAt.Before(now) && u.Plan > PlanFree {
		u.Plan = PlanFree
		u.PlanExpiryAt = nil
		_, _ = d.db.Exec("UPDATE users SET plan = ?, plan_expiry_at = NULL WHERE id = ?", u.Plan, u.ID)
	}

	// Always guarantee owners retain PlanAdmin
	if d.cfg != nil && d.cfg.IsOwner(u.ID) && u.Plan != PlanAdmin {
		u.Plan = PlanAdmin
		_, _ = d.db.Exec("UPDATE users SET plan = 0 WHERE id = ?", u.ID)
	}

	return &u, nil
}

func (d *DB) SetUserPlan(userID int64, plan UserPlan, expiry *time.Time) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	var expVal interface{}
	if expiry != nil {
		expVal = *expiry
	}
	_, err := d.db.Exec("UPDATE users SET plan = ?, plan_expiry_at = ? WHERE id = ?", plan, expVal, userID)
	return err
}

func (d *DB) AddTraffic(userID int64, bytes int64) error {
	if userID == 0 || bytes <= 0 {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(`
		UPDATE users SET
			used_bytes = used_bytes + ?,
			daily_bytes = daily_bytes + ?,
			monthly_bytes = monthly_bytes + ?
		WHERE id = ?;`,
		bytes, bytes, bytes, userID,
	)
	return err
}

func (d *DB) CheckQuota(userID int64, additionalBytes int64) (bool, string) {
	if d.cfg != nil && d.cfg.IsOwner(userID) {
		return true, ""
	}

	user, err := d.GetUser(userID)
	if err != nil {
		if err == sql.ErrNoRows {
			return true, ""
		}
		return false, "Error checking user quota: " + err.Error()
	}

	dailyLimit, monthlyLimit := PlanLimits(user.Plan)
	if dailyLimit > 0 && user.DailyBytes+additionalBytes > dailyLimit {
		return false, fmt.Sprintf("Daily quota exceeded (%s / %s). Plan: %s",
			FormatBytes(user.DailyBytes), FormatBytes(dailyLimit), user.Plan.String())
	}
	if monthlyLimit > 0 && user.MonthlyBytes+additionalBytes > monthlyLimit {
		return false, fmt.Sprintf("Monthly quota exceeded (%s / %s). Plan: %s",
			FormatBytes(user.MonthlyBytes), FormatBytes(monthlyLimit), user.Plan.String())
	}

	return true, ""
}

func (d *DB) ListDMUsers() (map[int64]DMUser, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	rows, err := d.db.Query("SELECT id, username, first_name, access_hash, created_at FROM users WHERE access_hash != 0")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := make(map[int64]DMUser)
	for rows.Next() {
		var u DMUser
		if err := rows.Scan(&u.UserID, &u.Username, &u.FirstName, &u.AccessHash, &u.StartedAt); err == nil {
			res[u.UserID] = u
		}
	}
	return res, nil
}

// -------------------------------------------------------------
// Job Operations (State & History)
// -------------------------------------------------------------

func (d *DB) SaveJob(job *Job) error {
	if job == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	targetData, _ := json.Marshal(job.Target)
	var locData []byte
	if job.Location != nil {
		locData, _ = json.Marshal(job.Location)
	}

	status := strings.ToLower(string(job.State))
	if status == "" {
		status = "queued"
	}

	_, err := d.db.Exec(`
		INSERT INTO jobs (id, user_id, type, kind, status, file_name, size, raw_url, magnet_uri, torrent_bytes, target_json, location_json, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(id) DO UPDATE SET
			status = excluded.status,
			file_name = excluded.file_name,
			size = excluded.size,
			target_json = excluded.target_json,
			location_json = excluded.location_json;`,
		job.ID, job.UserID, string(job.Type), job.Kind, status, job.FileName, job.Size,
		job.RawURL, job.MagnetURI, job.TorrentBytes, string(targetData), string(locData),
	)
	return err
}

func (d *DB) CompleteJob(id string, status string, errMsg string, finalSize int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	status = strings.ToLower(status)
	if status == "" {
		status = "completed"
	}

	_, err := d.db.Exec(`
		UPDATE jobs SET
			status = ?,
			error = ?,
			size = CASE WHEN ? > 0 THEN ? ELSE size END,
			completed_at = CURRENT_TIMESTAMP
		WHERE id = ?;`,
		status, errMsg, finalSize, finalSize, id,
	)
	return err
}

func (d *DB) LoadActiveJobs() ([]PersistedJob, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	rows, err := d.db.Query(`
		SELECT id, user_id, type, kind, file_name, size, raw_url, magnet_uri, torrent_bytes, target_json, location_json, created_at
		FROM jobs
		WHERE status IN ('queued', 'running')
		ORDER BY created_at ASC;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []PersistedJob
	for rows.Next() {
		var pj PersistedJob
		var jobTypeStr, targetJSON, locJSON string
		err := rows.Scan(
			&pj.ID, &pj.UserID, &jobTypeStr, &pj.Kind, &pj.FileName, &pj.Size,
			&pj.RawURL, &pj.MagnetURI, &pj.TorrentBytes, &targetJSON, &locJSON, &pj.CreatedAt,
		)
		if err != nil {
			continue
		}
		pj.Type = JobType(jobTypeStr)
		if targetJSON != "" {
			_ = json.Unmarshal([]byte(targetJSON), &pj.Target)
		}
		if locJSON != "" {
			var loc StoredLocation
			if err := json.Unmarshal([]byte(locJSON), &loc); err == nil {
				pj.Location = &loc
			}
		}
		jobs = append(jobs, pj)
	}
	return jobs, nil
}

// -------------------------------------------------------------
// Feed Operations
// -------------------------------------------------------------

func (d *DB) SaveFeed(f *FeedSubscription) error {
	if f == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	targetData, _ := json.Marshal(f.Target)
	includesStr := strings.Join(f.Includes, ",")
	excludesStr := strings.Join(f.Excludes, ",")
	pausedInt := 0
	if f.Paused {
		pausedInt = 1
	}

	if f.ID <= 0 {
		res, err := d.db.Exec(`
			INSERT INTO feeds (user_id, name, mode, url, includes, excludes, last_guid, last_title, target_json, paused, last_checked, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP);`,
			f.UserID, f.Name, string(f.Mode), f.URL, includesStr, excludesStr, f.LastGUID, f.LastTitle, string(targetData), pausedInt, f.LastChecked,
		)
		if err != nil {
			return err
		}
		lastID, err := res.LastInsertId()
		if err == nil {
			f.ID = int(lastID)
		}
		return nil
	}

	_, err := d.db.Exec(`
		INSERT INTO feeds (id, user_id, name, mode, url, includes, excludes, last_guid, last_title, target_json, paused, last_checked, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(id) DO UPDATE SET
			name = excluded.name,
			mode = excluded.mode,
			url = excluded.url,
			includes = excluded.includes,
			excludes = excluded.excludes,
			last_guid = excluded.last_guid,
			last_title = excluded.last_title,
			target_json = excluded.target_json,
			paused = excluded.paused,
			last_checked = excluded.last_checked;`,
		f.ID, f.UserID, f.Name, string(f.Mode), f.URL, includesStr, excludesStr, f.LastGUID, f.LastTitle, string(targetData), pausedInt, f.LastChecked,
	)
	return err
}

func (d *DB) DeleteFeed(id int, userID int64, isOwner bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if isOwner {
		_, err := d.db.Exec("DELETE FROM feeds WHERE id = ?", id)
		return err
	}
	_, err := d.db.Exec("DELETE FROM feeds WHERE id = ? AND user_id = ?", id, userID)
	return err
}

func (d *DB) UpdateFeedCursor(id int, lastGUID, lastTitle string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec("UPDATE feeds SET last_guid = ?, last_title = ?, last_checked = CURRENT_TIMESTAMP WHERE id = ?", lastGUID, lastTitle, id)
	return err
}

func (d *DB) SetFeedPaused(id int, userID int64, isOwner bool, paused bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	pausedInt := 0
	if paused {
		pausedInt = 1
	}

	if isOwner {
		_, err := d.db.Exec("UPDATE feeds SET paused = ? WHERE id = ?", pausedInt, id)
		return err
	}
	_, err := d.db.Exec("UPDATE feeds SET paused = ? WHERE id = ? AND user_id = ?", pausedInt, id, userID)
	return err
}

func (d *DB) LoadFeeds() ([]*FeedSubscription, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	rows, err := d.db.Query(`
		SELECT id, user_id, name, mode, url, includes, excludes, last_guid, last_title, target_json, paused, last_checked, created_at
		FROM feeds
		ORDER BY id ASC;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var feeds []*FeedSubscription
	for rows.Next() {
		var f FeedSubscription
		var modeStr, includesStr, excludesStr, targetJSON string
		var pausedInt int
		var lastChecked sql.NullTime
		err := rows.Scan(
			&f.ID, &f.UserID, &f.Name, &modeStr, &f.URL, &includesStr, &excludesStr,
			&f.LastGUID, &f.LastTitle, &targetJSON, &pausedInt, &lastChecked, &f.CreatedAt,
		)
		if err != nil {
			continue
		}
		f.Mode = FeedMode(modeStr)
		if includesStr != "" {
			f.Includes = strings.Split(includesStr, ",")
		}
		if excludesStr != "" {
			f.Excludes = strings.Split(excludesStr, ",")
		}
		if targetJSON != "" {
			_ = json.Unmarshal([]byte(targetJSON), &f.Target)
		}
		f.Paused = pausedInt == 1
		if lastChecked.Valid {
			f.LastChecked = lastChecked.Time
		}
		feeds = append(feeds, &f)
	}
	return feeds, nil
}
