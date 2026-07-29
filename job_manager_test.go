package main

import (
	"context"
	"testing"
)

func TestJobManagerConcurrencyAndCancel(t *testing.T) {
	jm := NewJobManager(2)

	j1, err := jm.CreateJob(context.Background(), JobTypeMirror, "file1.txt", 100, 1001)
	if err != nil {
		t.Fatalf("failed creating job 1: %v", err)
	}

	j2, err := jm.CreateJob(context.Background(), JobTypeLeech, "file2.txt", 200, 1001)
	if err != nil {
		t.Fatalf("failed creating job 2: %v", err)
	}

	_, err = jm.CreateJob(context.Background(), JobTypeMirror, "file3.txt", 300, 1001)
	if err == nil {
		t.Fatalf("expected concurrency error on 3rd job")
	}

	if len(jm.GetActiveJobs()) != 2 {
		t.Errorf("expected 2 active jobs")
	}

	if !jm.CancelJob(j1.ID) {
		t.Errorf("failed to cancel job 1")
	}

	jm.FinishJob(j1.ID)

	j3, err := jm.CreateJob(context.Background(), JobTypeMirror, "file3.txt", 300, 1001)
	if err != nil {
		t.Fatalf("failed creating job 3 after slot freed: %v", err)
	}

	jm.FinishJob(j2.ID)
	jm.FinishJob(j3.ID)

	if len(jm.GetActiveJobs()) != 0 {
		t.Errorf("expected 0 jobs remaining")
	}
}
