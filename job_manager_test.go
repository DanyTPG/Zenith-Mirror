package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestJobManagerConcurrencyAndCancel(t *testing.T) {
	jm := NewJobManager(2)
	ctx := context.Background()

	executed := make(chan string, 10)

	j1, err := jm.CreateJob(ctx, JobOptions{
		Type:     JobTypeMirror,
		Kind:     "url_mirror",
		FileName: "file1.bin",
		Size:     1000,
		UserID:   1001,
		Execute:  func(j *Job) { executed <- "j1" },
	})
	if err != nil {
		t.Fatalf("failed creating j1: %v", err)
	}

	j2, err := jm.CreateJob(ctx, JobOptions{
		Type:     JobTypeMirror,
		Kind:     "url_mirror",
		FileName: "file2.bin",
		Size:     2000,
		UserID:   1001,
		Execute:  func(j *Job) { executed <- "j2" },
	})
	if err != nil {
		t.Fatalf("failed creating j2: %v", err)
	}

	// 3rd job should be queued because max_concurrency = 2
	j3, err := jm.CreateJob(ctx, JobOptions{
		Type:     JobTypeMirror,
		Kind:     "url_mirror",
		FileName: "file3.bin",
		Size:     3000,
		UserID:   1001,
		Execute:  func(j *Job) { executed <- "j3" },
	})
	if err != nil {
		t.Fatalf("failed creating j3: %v", err)
	}

	if j1.State != StateRunning || j2.State != StateRunning {
		t.Errorf("j1 and j2 should be running")
	}

	if j3.State != StateQueued {
		t.Errorf("j3 should be queued, got %s", j3.State)
	}

	// Finish j1 -> j3 should be dequeued and executed
	jm.FinishJob(j1.ID)

	select {
	case id := <-executed:
		if id != "j3" {
			t.Errorf("expected j3 execution, got %s", id)
		}
	case <-time.After(1 * time.Second):
		t.Errorf("j3 was not executed after j1 finished")
	}

	// Test CancelAll
	count := jm.CancelAllJobs()
	if count < 1 {
		t.Errorf("expected at least 1 job cancelled, got %d", count)
	}
}

func TestJobManagerPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "jobs_state.json")

	jm1 := NewJobManager(2)
	jm1.SetStateFile(stateFile)

	ctx := context.Background()
	j, err := jm1.CreateJob(ctx, JobOptions{
		Type:     JobTypeMirror,
		Kind:     "url_mirror",
		FileName: "test.bin",
		Size:     12345,
		UserID:   999,
		RawURL:   "https://example.com/test.bin",
		Target:   JobTarget{PeerType: "user", UserID: 999, ReplyMsgID: 42},
	})
	if err != nil {
		t.Fatalf("failed creating job: %v", err)
	}
	jm1.SaveState()

	// Verify file exists and holds state
	records, err := jm1.LoadPersistedState()
	if err != nil {
		t.Fatalf("failed loading state: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].RawURL != "https://example.com/test.bin" {
		t.Errorf("unexpected raw url: %s", records[0].RawURL)
	}
	if records[0].Target.ReplyMsgID != 42 {
		t.Errorf("unexpected reply msg id: %d", records[0].Target.ReplyMsgID)
	}

	// Create a second manager instance simulating restart
	jm2 := NewJobManager(2)
	jm2.SetStateFile(stateFile)
	recLoaded, err := jm2.LoadPersistedState()
	if err != nil {
		t.Fatalf("failed loading state on jm2: %v", err)
	}
	if len(recLoaded) != 1 {
		t.Fatalf("expected 1 record on jm2, got %d", len(recLoaded))
	}

	// Finish job removes from state file
	jm1.FinishJob(j.ID)
	recordsAfter, err := jm1.LoadPersistedState()
	if err != nil {
		t.Fatalf("failed loading state after finish: %v", err)
	}
	if len(recordsAfter) != 0 {
		t.Errorf("expected 0 records after finish, got %d", len(recordsAfter))
	}
}
