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
	"github.com/gotd/td/tgerr"
)

const (
	rawChunkSize  = 512 * 1024
	rawMaxRetries = 5
)

// rawParallelDownload bypasses gotd/td's downloader entirely.
// Each goroutine independently calls UploadGetFile with its own offset range.
// FILE_MIGRATE handled per-call → multiple goroutines → multiple connections to remote DC.
// FLOOD_WAIT handled with retry + backoff.
func rawParallelDownload(ctx context.Context, api *tg.Client, location tg.InputFileLocationClass, size int64, threads int, file *os.File) error {
	if size <= 0 {
		return fmt.Errorf("unknown file size %d", size)
	}

	totalChunks := int((size + rawChunkSize - 1) / int64(rawChunkSize))
	slog.Info("raw parallel download", "chunks", totalChunks, "threads", threads, "size", size)

	var (
		written  atomic.Int64
		firstErr atomic.Value
		wg       sync.WaitGroup
	)

	sem := make(chan struct{}, threads)

	for i := 0; i < totalChunks; i++ {
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
		// Telegram requires Limit to be a multiple of 4096
		chunkLen := int(end - offset)
		if chunkLen%4096 != 0 {
			// Round down to nearest multiple of 4096, handle remainder later
			rounded := chunkLen &^ 4095
			if rounded > 0 {
				end = offset + int64(rounded)
				chunkLen = rounded
			}
		}

		wg.Add(1)
		go func(offset int64, chunkLen int, chunkIdx int) {
			defer wg.Done()
			defer func() { <-sem }()

			n, err := downloadChunkWithRetry(ctx, api, location, offset, chunkLen, chunkIdx, file)
			if err != nil {
				slog.Error("chunk download failed after retries", "chunk", chunkIdx, "offset", offset, "error", err)
				firstErr.CompareAndSwap(nil, fmt.Errorf("chunk %d at offset %d: %w", chunkIdx, offset, err))
				return
			}
			written.Add(int64(n))
		}(offset, chunkLen, i)
	}

	wg.Wait()

	if err := firstErr.Load(); err != nil {
		return err.(error)
	}

	// Handle remainder bytes (when file size not a multiple of 4096)
	if remainder := size % int64(rawChunkSize); remainder > 0 && remainder%4096 != 0 {
		if firstErr.Load() == nil {
			offset := size - remainder
			rounded := int(remainder) &^ 4095
			if rounded > 0 {
				n, err := downloadChunkWithRetry(ctx, api, location, offset, rounded, totalChunks, file)
				if err != nil {
					return fmt.Errorf("remainder chunk at offset %d: %w", offset, err)
				}
				written.Add(int64(n))
			}
		}
	}

	slog.Info("raw parallel download complete", "bytes", written.Load())
	return nil
}

// downloadChunkWithRetry downloads a single chunk with FLOOD_WAIT retry.
func downloadChunkWithRetry(ctx context.Context, api *tg.Client, location tg.InputFileLocationClass, offset int64, chunkLen int, chunkIdx int, file *os.File) (int, error) {
	for attempt := 0; attempt < rawMaxRetries; attempt++ {
		req := &tg.UploadGetFileRequest{
			Location: location,
			Offset:   offset,
			Limit:    chunkLen,
		}
		req.SetPrecise(true)

		resp, err := api.UploadGetFile(ctx, req)
		if err != nil {
			if d, ok := tgerr.AsFloodWait(err); ok {
				slog.Warn("flood wait, sleeping", "chunk", chunkIdx, "wait", d)
				select {
				case <-ctx.Done():
					return 0, ctx.Err()
				case <-time.After(d):
					continue
				}
			}
			return 0, err
		}

		switch r := resp.(type) {
		case *tg.UploadFile:
			n, err := file.WriteAt(r.Bytes, offset)
			if err != nil {
				return 0, fmt.Errorf("write chunk %d: %w", chunkIdx, err)
			}
			return n, nil
		default:
			return 0, fmt.Errorf("chunk %d: unexpected response type %T", chunkIdx, resp)
		}
	}

	return 0, fmt.Errorf("chunk %d: exceeded max retries (%d)", chunkIdx, rawMaxRetries)
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
