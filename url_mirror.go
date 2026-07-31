package main

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// executeURLMirrorParallel: multi-threaded HTTP range download to temp file, then stream to GDrive
func (ts *TelegramService) executeURLMirrorParallel(job *Job, rawURL string) (string, error) {
	// HEAD request to get size and check Range support
	client := &http.Client{Timeout: 30 * time.Second}
 headReq, err := http.NewRequestWithContext(job.Ctx, "HEAD", rawURL, nil)
	if err != nil {
		return "", fmt.Errorf("HEAD request failed: %w", err)
	}
	headReq.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Zenith-Mirror/1.0")

	headResp, err := client.Do(headReq)
	if err != nil {
		return "", fmt.Errorf("HEAD request failed: %w", err)
	}
	headResp.Body.Close()

	totalSize := headResp.ContentLength
	supportsRange := strings.Contains(strings.ToLower(headResp.Header.Get("Accept-Ranges")), "bytes")

	if totalSize <= 0 || !supportsRange {
		slog.Info("server does not support range requests, falling back to stream mode", "job_id", job.ID)
		return ts.executeURLMirrorStreamFallback(job, rawURL)
	}

	slog.Info("starting parallel URL download", "job_id", job.ID, "size", totalSize, "threads", ts.cfg.DownloadThreads)

	// Create temp file
	tmpFile, err := os.CreateTemp("", "zenith-url-dl-*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Pre-allocate file
	if err := tmpFile.Truncate(totalSize); err != nil {
		return "", fmt.Errorf("failed to pre-allocate temp file: %w", err)
	}

	// Progress tracking
	pwa := newProgressWriterAt(tmpFile, totalSize, func(read, total int64, speed float64, eta time.Duration) {
		job.ReadBytes = read
		job.Speed = speed
		job.ETA = eta
	})

	// Split into chunks
	threads := ts.cfg.DownloadThreads
	chunkSize := totalSize / int64(threads)

	var wg sync.WaitGroup
	var dlErr atomic.Value

	for i := 0; i < threads; i++ {
		start := int64(i) * chunkSize
		end := start + chunkSize - 1
		if i == threads-1 {
			end = totalSize - 1 // last thread takes remainder
		}

		wg.Add(1)
		go func(start, end int64, idx int) {
			defer wg.Done()

			req, err := http.NewRequestWithContext(job.Ctx, "GET", rawURL, nil)
			if err != nil {
				dlErr.Store(fmt.Errorf("thread %d: %w", idx, err))
				return
			}
			req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Zenith-Mirror/1.0")
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				dlErr.Store(fmt.Errorf("thread %d: %w", idx, err))
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				dlErr.Store(fmt.Errorf("thread %d: HTTP %s", idx, resp.Status))
				return
			}

			// Read into 512KB buffer, write at offset
			buf := make([]byte, 512*1024)
			offset := start
			for {
				n, readErr := resp.Body.Read(buf)
				if n > 0 {
					if _, writeErr := pwa.WriteAt(buf[:n], offset); writeErr != nil {
						dlErr.Store(fmt.Errorf("thread %d write: %w", idx, writeErr))
						return
					}
					offset += int64(n)
				}
				if readErr == io.EOF {
					break
				}
				if readErr != nil {
					dlErr.Store(fmt.Errorf("thread %d read: %w", idx, readErr))
					return
				}
			}
		}(start, end, i)
	}

	wg.Wait()

	if errVal := dlErr.Load(); errVal != nil {
		return "", errVal.(error)
	}

	downloadedSize, _ := tmpFile.Seek(0, io.SeekEnd)
	slog.Info("parallel URL download finished", "job_id", job.ID, "bytes", downloadedSize)

	// Seek back for upload
	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("failed to seek temp file: %w", err)
	}

	job.ReadBytes = 0
	job.Speed = 0
	job.ETA = 0
	job.Phase = PhaseUploading
	job.Status = "Uploading to Google Drive"

	progressReader := NewProgressReader(tmpFile, downloadedSize, func(read, total int64, speed float64, eta time.Duration) {
		job.ReadBytes = read
		job.Speed = speed
		job.ETA = eta
	})

	return ts.gdrive.UploadStream(job.Ctx, job.FileName, progressReader, downloadedSize)
}

// executeURLMirrorStreamFallback: single-threaded stream when server doesn't support Range
func (ts *TelegramService) executeURLMirrorStreamFallback(job *Job, rawURL string) (string, error) {
	req, err := http.NewRequestWithContext(job.Ctx, "GET", rawURL, nil)
	if err != nil {
		return "", fmt.Errorf("request failed: %w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Zenith-Mirror/1.0")

	client := &http.Client{Timeout: 0}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("download failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("HTTP %s", resp.Status)
	}

	job.Size = resp.ContentLength
	job.FileName = extractFileNameFromURL(rawURL)

	pr := NewProgressReader(resp.Body, job.Size, func(read, total int64, speed float64, eta time.Duration) {
		job.ReadBytes = read
		job.Speed = speed
		job.ETA = eta
	})

	job.Phase = PhaseUploading
	job.Status = "Uploading to Google Drive"

	return ts.gdrive.UploadStream(job.Ctx, job.FileName, pr, job.Size)
}

func extractFileNameFromURL(rawURL string) string {
	urlParts := strings.Split(rawURL, "/")
	name := urlParts[len(urlParts)-1]
	if name == "" {
		name = "downloaded_file.bin"
	}
	// Strip query params
	if idx := strings.IndexByte(name, '?'); idx >= 0 {
		name = name[:idx]
	}
	return name
}