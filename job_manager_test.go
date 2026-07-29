package main

import (
	"context"
	"testing"
	"time"
)

func TestJobManagerConcurrencyAndCancel(t *testing.T) {
	jm := NewJobManager(2)
	ctx := context.Background()

	executed := make(chan string, 10)

	j1, err := jm.CreateJob(ctx, JobTypeMirror, "file1.bin", 1000, 1001, func() { executed <- "j1" })
	if err != nil {
		t.Fatalf("failed creating j1: %v", err)
	}

	j2, err := jm.CreateJob(ctx, JobTypeMirror, "file2.bin", 2000, 1001, func() { executed <- "j2" })
	if err != nil {
		t.Fatalf("failed creating j2: %v", err)
	}

	// 3rd job should be queued because max_concurrency = 2
	j3, err := jm.CreateJob(ctx, JobTypeMirror, "file3.bin", 3000, 1001, func() { executed <- "j3" })
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
