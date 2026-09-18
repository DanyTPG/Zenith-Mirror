package main

import (
	"context"
	"fmt"
	"io"
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

// fetchChunkWithRetry downloads a single chunk with FLOOD_WAIT retry.
// Backoff: wait + jitter, doubling per attempt (1x, 2x, 4x... capped at 64x).
func fetchChunkWithRetry(ctx context.Context, api *tg.Client, location tg.InputFileLocationClass, offset int64, chunkLen int, chunkIdx int) ([]byte, error) {
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
				jitter := time.Duration(rand.Int63n(int64(d)/2 + 1))
				sleep += jitter
				slog.Warn("flood wait, sleeping", "chunk", chunkIdx, "wait", d, "attempt", attempt, "sleep", sleep)
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(sleep):
					continue
				}
			}
			return nil, err
		}

		switch r := resp.(type) {
		case *tg.UploadFile:
			return r.Bytes, nil
		default:
			return nil, fmt.Errorf("chunk %d: unexpected response type %T", chunkIdx, resp)
		}
	}

	return nil, fmt.Errorf("chunk %d: exceeded max retries (%d)", chunkIdx, rawMaxRetries)
}

// downloadChunkWithRetry downloads a single chunk and writes it directly to file.
func downloadChunkWithRetry(ctx context.Context, api *tg.Client, location tg.InputFileLocationClass, offset int64, chunkLen int, chunkIdx int, file *os.File) (int, error) {
	data, err := fetchChunkWithRetry(ctx, api, location, offset, chunkLen, chunkIdx)
	if err != nil {
		return 0, err
	}
	n, err := file.WriteAt(data, offset)
	if err != nil {
		return 0, fmt.Errorf("write chunk %d: %w", chunkIdx, err)
	}
	return n, nil
}

type pipelinedChunkResult struct {
	idx  int
	data []byte
	err  error
}

// rawPipelinedStream concurrently downloads chunks from Telegram using a pooled
// multi-connection client, buffering up to windowSize chunks in memory and writing
// them strictly sequentially to w (e.g. io.PipeWriter into Google Drive).
func rawPipelinedStream(
	ctx context.Context,
	api *tg.Client,
	location tg.InputFileLocationClass,
	size int64,
	threads int,
	partSize int,
	w io.Writer,
) error {
	if size <= 0 {
		return fmt.Errorf("unknown file size %d", size)
	}
	if partSize <= 0 || partSize%4096 != 0 {
		partSize = rawChunkSize
	}
	if threads <= 0 {
		threads = 4
	}

	totalChunks := int((size + int64(partSize) - 1) / int64(partSize))
	windowSize := threads * 2
	if windowSize < 4 {
		windowSize = 4
	}

	slog.Info("starting raw pipelined stream", "chunks", totalChunks, "threads", threads, "size", size, "part_size", partSize, "window", windowSize)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	windowSem := make(chan struct{}, windowSize)
	results := make(chan pipelinedChunkResult, windowSize)
	dispatch := make(chan int, threads)

	var wg sync.WaitGroup
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range dispatch {
				offset := int64(idx) * int64(partSize)
				end := offset + int64(partSize)
				if end > size {
					end = size
				}
				chunkLen := int(end - offset)
				reqLimit := (chunkLen + 4095) &^ 4095
				if reqLimit > partSize {
					reqLimit = partSize
				}

				data, err := fetchChunkWithRetry(ctx, api, location, offset, reqLimit, idx)
				if err == nil && len(data) > chunkLen {
					data = data[:chunkLen]
				}

				select {
				case <-ctx.Done():
					return
				case results <- pipelinedChunkResult{idx: idx, data: data, err: err}:
				}
			}
		}()
	}

	// Dispatcher loop
	go func() {
		defer close(dispatch)
		for i := 0; i < totalChunks; i++ {
			select {
			case <-ctx.Done():
				return
			case windowSem <- struct{}{}:
			}

			select {
			case <-ctx.Done():
				return
			case dispatch <- i:
			}
		}
	}()

	// Cleanup worker wait
	go func() {
		wg.Wait()
		close(results)
	}()

	buffered := make(map[int][]byte)
	nextIdx := 0

	for res := range results {
		if res.err != nil {
			cancel()
			return fmt.Errorf("chunk %d download failed: %w", res.idx, res.err)
		}

		buffered[res.idx] = res.data

		for {
			data, ok := buffered[nextIdx]
			if !ok {
				break
			}
			delete(buffered, nextIdx)

			if len(data) > 0 {
				if _, err := w.Write(data); err != nil {
					cancel()
					return fmt.Errorf("write chunk %d to stream: %w", nextIdx, err)
				}
			}

			<-windowSem
			nextIdx++

			if nextIdx == totalChunks {
				slog.Info("raw pipelined stream complete", "total_chunks", totalChunks, "bytes", size)
				return nil
			}
		}
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	if nextIdx < totalChunks {
		return fmt.Errorf("stream terminated early: wrote %d of %d chunks", nextIdx, totalChunks)
	}

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
