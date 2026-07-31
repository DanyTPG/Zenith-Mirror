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
	rawChunkSize    = 512 * 1024 // 512KB — Telegram max
	rawPipelineDepth = 2         // chunks in-flight per worker
)

// rawParallelDownload bypasses gotd/td's downloader entirely.
// Each goroutine independently calls UploadGetFile with its own offset range.
// FILE_MIGRATE is handled per-call by client.API()'s invokeDirect.
// Multiple goroutines create multiple concurrent connections to the remote DC.
func rawParallelDownload(ctx context.Context, api *tg.Client, location tg.InputFileLocationClass, size int64, threads int, file *os.File) error {
	if size <= 0 {
		return fmt.Errorf("unknown file size %d", size)
	}

	totalChunks := int((size + rawChunkSize - 1) / int64(rawChunkSize))
	slog.Info("raw parallel download", "chunks", totalChunks, "threads", threads, "size", size)

	var (
		written   atomic.Int64
		firstErr  atomic.Value
		sem       = make(chan struct{}, threads*rawPipelineDepth)
		wg        sync.WaitGroup
	)

	for i := 0; i < totalChunks; i++ {
		// Check for early abort
		if firstErr.Load() != nil {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case sem <- struct{}{}:
		}

		offset := int64(i) * int64(rawChunkSize)
		end := offset + int64(rawChunkSize)
		if end > size {
			end = size
		}
		chunkLen := int(end - offset)

		wg.Add(1)
		go func(offset int64, chunkLen int, chunkIdx int) {
			defer wg.Done()
			defer func() { <-sem }()

			req := &tg.UploadGetFileRequest{
				Location: location,
				Offset:   offset,
				Limit:    chunkLen,
			}
			req.SetPrecise(true)

			resp, err := api.UploadGetFile(ctx, req)
			if err != nil {
				slog.Error("chunk download failed", "chunk", chunkIdx, "offset", offset, "error", err)
				firstErr.CompareAndSwap(nil, fmt.Errorf("chunk %d at offset %d: %w", chunkIdx, offset, err))
				return
			}

			switch r := resp.(type) {
			case *tg.UploadFile:
				n, err := file.WriteAt(r.Bytes, offset)
				if err != nil {
					firstErr.CompareAndSwap(nil, fmt.Errorf("write chunk %d: %w", chunkIdx, err))
					return
				}
				written.Add(int64(n))
			default:
				firstErr.CompareAndSwap(nil, fmt.Errorf("chunk %d: unexpected response type %T", chunkIdx, resp))
			}
		}(offset, chunkLen, i)
	}

	wg.Wait()

	if err := firstErr.Load(); err != nil {
		return err.(error)
	}

	downloaded := written.Load()
	slog.Info("raw parallel download complete", "bytes", downloaded)
	return nil
}

// rawDownloadProgress tracks progress for rawParallelDownload.
func rawDownloadProgress(ctx context.Context, file *os.File, job *Job, totalSize int64) {
	start := time.Now()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fi, err := file.Stat()
			if err != nil {
				continue
			}
			written := fi.Size()
			elapsed := time.Since(start).Seconds()
			if elapsed > 0 {
				job.ReadBytes = written
				job.Speed = float64(written) / elapsed
				if totalSize > 0 {
					remaining := float64(totalSize-written) / job.Speed
					job.ETA = time.Duration(remaining * float64(time.Second))
				}
			}
		}
	}
}
