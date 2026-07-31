package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/td/tg"
)

const (
	chunkSize      = 512 * 1024 // 512KB — Telegram max per request
	pipelineDepth  = 8          // outstanding requests per worker
)

// fastDownload uses raw upload.getFile RPCs with pipelining.
// Sends multiple outstanding requests concurrently, bypassing gotd/td's
// sequential reader bottleneck. Writes directly to file via WriteAt.
func fastDownload(ctx context.Context, api *tg.Client, location tg.InputFileLocationClass,
	totalSize int64, output *os.File, counter *atomicWriteAt) error {

	if totalSize <= 0 {
		return fmt.Errorf("unknown file size, cannot use fast download")
	}

	workers := pipelineDepth
	slog.Info("fast download starting", "size", totalSize, "workers", workers, "chunk", chunkSize)

	// Semaphore limits concurrent RPCs
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	var dlErr atomic.Value

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Launch workers, each processes a range of chunks
	totalChunks := (totalSize + chunkSize - 1) / chunkSize
	chunksPerWorker := (totalChunks + int64(workers) - 1) / int64(workers)

	for w := 0; w < workers; w++ {
		startChunk := int64(w) * chunksPerWorker
		endChunk := startChunk + chunksPerWorker
		if endChunk > totalChunks {
			endChunk = totalChunks
		}
		if startChunk >= totalChunks {
			break
		}

		wg.Add(1)
		go func(workerID int, startChunk, endChunk int64) {
			defer wg.Done()
			for chunkIdx := startChunk; chunkIdx < endChunk; chunkIdx++ {
				select {
				case <-ctx.Done():
					dlErr.Store(ctx.Err())
					return
				case sem <- struct{}{}:
				}

				offset := chunkIdx * int64(chunkSize)
				limit := chunkSize
				if offset+int64(limit) > totalSize {
					limit = int(totalSize - offset)
				}

				req := &tg.UploadGetFileRequest{
					Location: location,
					Offset:   offset,
					Limit:    limit,
				}
				req.SetPrecise(true)
				req.SetCDNSupported(true)

				resp, err := api.UploadGetFile(ctx, req)
				<-sem

				if err != nil {
					// Retry once on transient errors
					resp, err = api.UploadGetFile(ctx, req)
					if err != nil {
						dlErr.Store(fmt.Errorf("worker %d chunk %d: %w", workerID, chunkIdx, err))
						cancel()
						return
					}
				}

				switch r := resp.(type) {
				case *tg.UploadFile:
					if _, err := output.WriteAt(r.Bytes, offset); err != nil {
						dlErr.Store(fmt.Errorf("worker %d write: %w", workerID, err))
						cancel()
						return
					}
					atomic.AddInt64(&counter.written, int64(len(r.Bytes)))
				case *tg.UploadFileCDNRedirect:
					dlErr.Store(fmt.Errorf("CDN redirect not supported in fast download"))
					cancel()
					return
				}
			}
		}(w, startChunk, endChunk)
	}

	wg.Wait()

	if errVal := dlErr.Load(); errVal != nil {
		return errVal.(error)
	}

	slog.Info("fast download finished", "bytes", atomic.LoadInt64(&counter.written))
	return nil
}

// startProgressTracker starts a goroutine that updates job progress once per second.
func startProgressTracker(job *Job, counter *atomicWriteAt) (stop func()) {
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		start := time.Now()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				written := atomic.LoadInt64(&counter.written)
				elapsed := time.Since(start).Seconds()
				if elapsed > 0 {
					job.ReadBytes = written
					job.Speed = float64(written) / elapsed
					if job.Size > 0 {
						remaining := float64(job.Size-written) / job.Speed
						job.ETA = time.Duration(remaining * float64(time.Second))
					}
				}
			}
		}
	}()
	return func() { close(done) }
}
