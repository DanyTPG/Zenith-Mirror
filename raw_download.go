package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

const (
	rawChunkSize  = 512 * 1024
	rawMaxRetries = 20
)

// rawParallelDownload is a true multi-connection downloader.
// The caller supplies a *pool-based* invoker (ts.client.DC(dc, threads)) — a
// pool that opens `threads` INDEPENDENT mtproto connections (each with its own
// TCP socket + auth key to the target DC). Each goroutine below calls
// UploadGetFile through that pool, so requests spread across distinct sockets.
//
// Why not gotd's built-in Parallel()? It funnels all chunk requests through a
// single shared Client (client.API() → one mtproto.Conn). All N workers share
// one TCP connection, so throughput caps around ~2MB/s regardless of threads.
// A real pool of N sockets → ~11MB/s.
func rawParallelDownload(ctx context.Context, api *tg.Client, location tg.InputFileLocationClass, size int64, threads int, partSize int, file *os.File) error {
	if size <= 0 {
		return fmt.Errorf("unknown file size %d", size)
	}
	if partSize <= 0 || partSize%4096 != 0 {
		partSize = rawChunkSize
	}

	totalChunks := int((size + int64(partSize) - 1) / int64(partSize))
	slog.Info("raw parallel download", "chunks", totalChunks, "threads", threads, "size", size, "part_size", partSize)

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

		offset := int64(i) * int64(partSize)
		end := offset + int64(partSize)
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

	// Handle remainder bytes (when file size not a multiple of partSize)
	if remainder := size % int64(partSize); remainder > 0 && remainder%4096 != 0 {
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
// Backoff: wait + jitter, doubling per attempt (1x, 2x, 4x... capped at 64x).
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
				// Exponential backoff: multiply the wait by 2^attempt,
				// capped so a huge ban doesn't stall forever. Jitter keeps
				// concurrent goroutines from waking in lockstep.
				mult := 1 << min(attempt, 6)
				sleep := d * time.Duration(mult)
				jitter := time.Duration(rand.Int63n(int64(d) / 2))
				sleep += jitter
				slog.Warn("flood wait, sleeping", "chunk", chunkIdx, "wait", d, "attempt", attempt, "sleep", sleep)
				select {
				case <-ctx.Done():
					return 0, ctx.Err()
				case <-time.After(sleep):
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
