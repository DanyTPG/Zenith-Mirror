package main

import (
	"context"
	"fmt"

	"golang.org/x/sync/semaphore"
)

// DownloadManager caps the number of simultaneously running file downloads
// across ALL jobs. Telegram rate limits are per-account, not per-download:
// N files x M threads each = N*M concurrent chunk requests, which triggers
// FLOOD_WAIT. The semaphore bounds N.
type DownloadManager struct {
	sem *semaphore.Weighted
}

func NewDownloadManager(maxConcurrent int64) *DownloadManager {
	if maxConcurrent < 1 {
		maxConcurrent = 1
	}
	return &DownloadManager{sem: semaphore.NewWeighted(maxConcurrent)}
}

// Acquire blocks until a download slot is free. Callers must Release.
func (dm *DownloadManager) Acquire(ctx context.Context) error {
	if err := dm.sem.Acquire(ctx, 1); err != nil {
		return fmt.Errorf("acquire download slot: %w", err)
	}
	return nil
}

func (dm *DownloadManager) Release() {
	dm.sem.Release(1)
}
