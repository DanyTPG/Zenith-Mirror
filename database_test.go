package main

import (
	"path/filepath"
	"testing"
	"time"
)

func TestDatabaseUsersAndQuotas(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_zenith.db")

	cfg := &Config{
		OwnerID:  []int64{100},
		OwnerIDs: []int64{100},
	}

	db, err := OpenDB(dbPath, cfg)
	if err != nil {
		t.Fatalf("failed opening test db: %v", err)
	}
	defer db.Close()

	// 1. Owner user
	err = db.UpsertUser(100, 111, "owner", "Boss")
	if err != nil {
		t.Fatalf("failed upserting owner: %v", err)
	}
	owner, err := db.GetUser(100)
	if err != nil {
		t.Fatalf("failed getting owner: %v", err)
	}
	if owner.Plan != PlanAdmin {
		t.Fatalf("expected owner plan %v, got %v", PlanAdmin, owner.Plan)
	}
	allowed, _ := db.CheckQuota(100, 100*ByteUnitGB)
	if !allowed {
		t.Fatalf("owner should have unlimited quota")
	}

	// 2. Free user (Plan 1: 10GB daily, 50GB monthly)
	err = db.UpsertUser(200, 222, "freeuser", "User")
	if err != nil {
		t.Fatalf("failed upserting free user: %v", err)
	}
	freeUser, err := db.GetUser(200)
	if err != nil {
		t.Fatalf("failed getting free user: %v", err)
	}
	if freeUser.Plan != PlanFree {
		t.Fatalf("expected plan free, got %v", freeUser.Plan)
	}

	// Quota check: 5GB should pass
	allowed, _ = db.CheckQuota(200, 5*ByteUnitGB)
	if !allowed {
		t.Fatalf("5GB should be within 10GB daily quota")
	}

	// Add 9GB traffic
	err = db.AddTraffic(200, 9*ByteUnitGB)
	if err != nil {
		t.Fatalf("failed adding traffic: %v", err)
	}

	// Quota check: additional 2GB should fail daily quota (9GB + 2GB > 10GB)
	allowed, reason := db.CheckQuota(200, 2*ByteUnitGB)
	if allowed {
		t.Fatalf("expected daily quota to fail, reason: %s", reason)
	}

	// Upgrade to Tier 2 (Plan 2: 20GB daily, 200GB monthly)
	err = db.SetUserPlan(200, PlanTier2, nil)
	if err != nil {
		t.Fatalf("failed setting plan: %v", err)
	}
	allowed, _ = db.CheckQuota(200, 2*ByteUnitGB)
	if !allowed {
		t.Fatalf("Tier 2 should permit additional 2GB (9GB + 2GB < 20GB)")
	}

	// Unregister user DM
	err = db.UnregisterDM(200)
	if err != nil {
		t.Fatalf("failed unregistering: %v", err)
	}
	if db.HasDMUser(200) {
		t.Fatalf("user should not have active DM access hash")
	}
}

func TestDatabaseJobsPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_jobs.db")

	db, err := OpenDB(dbPath, nil)
	if err != nil {
		t.Fatalf("failed opening test db: %v", err)
	}
	defer db.Close()

	job := &Job{
		ID:       "job-123",
		UserID:   200,
		Type:     JobTypeMirror,
		Kind:     "url_mirror",
		FileName: "test.iso",
		Size:     1024 * 1024,
		State:    StateRunning,
	}

	err = db.SaveJob(job)
	if err != nil {
		t.Fatalf("failed saving job: %v", err)
	}

	active, err := db.LoadActiveJobs()
	if err != nil {
		t.Fatalf("failed loading active jobs: %v", err)
	}
	if len(active) != 1 || active[0].ID != "job-123" {
		t.Fatalf("expected 1 active job with id job-123, got: %+v", active)
	}

	// Complete job
	err = db.CompleteJob("job-123", "completed", "", 1024*1024)
	if err != nil {
		t.Fatalf("failed completing job: %v", err)
	}

	activeAfter, err := db.LoadActiveJobs()
	if err != nil {
		t.Fatalf("failed loading active jobs: %v", err)
	}
	if len(activeAfter) != 0 {
		t.Fatalf("expected 0 active jobs after completion, got: %d", len(activeAfter))
	}
}

func TestDatabaseFeedsPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_feeds.db")

	db, err := OpenDB(dbPath, nil)
	if err != nil {
		t.Fatalf("failed opening test db: %v", err)
	}
	defer db.Close()

	feed := &FeedSubscription{
		UserID:    100,
		Name:      "TestFeed",
		Mode:      FeedModeMirror,
		URL:       "https://example.com/rss.xml",
		Includes:  []string{"1080p"},
		LastGUID:  "guid-1",
		LastTitle: "Title 1",
		CreatedAt: time.Now(),
	}

	err = db.SaveFeed(feed)
	if err != nil {
		t.Fatalf("failed saving feed: %v", err)
	}
	if feed.ID <= 0 {
		t.Fatalf("expected positive feed ID, got %d", feed.ID)
	}

	feeds, err := db.LoadFeeds()
	if err != nil {
		t.Fatalf("failed loading feeds: %v", err)
	}
	if len(feeds) != 1 || feeds[0].Name != "TestFeed" {
		t.Fatalf("expected 1 feed named TestFeed, got: %+v", feeds)
	}

	// Update cursor
	err = db.UpdateFeedCursor(feed.ID, "guid-2", "Title 2")
	if err != nil {
		t.Fatalf("failed updating cursor: %v", err)
	}

	// Pause feed
	err = db.SetFeedPaused(feed.ID, 100, false, true)
	if err != nil {
		t.Fatalf("failed pausing feed: %v", err)
	}

	feeds, _ = db.LoadFeeds()
	if !feeds[0].Paused || feeds[0].LastGUID != "guid-2" {
		t.Fatalf("expected paused feed with guid-2, got: %+v", feeds[0])
	}
}
