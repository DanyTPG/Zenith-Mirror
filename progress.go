package main

import (
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"time"
)

type ProgressReader struct {
	reader     io.Reader
	totalBytes int64
	readBytes  int64
	startTime  time.Time
	onProgress func(read, total int64, speed float64, eta time.Duration)
	lastNotify time.Time
}

func NewProgressReader(r io.Reader, totalBytes int64, onProgress func(read, total int64, speed float64, eta time.Duration)) *ProgressReader {
	return &ProgressReader{
		reader:     r,
		totalBytes: totalBytes,
		startTime:  time.Now(),
		onProgress: onProgress,
	}
}

func (pr *ProgressReader) Read(p []byte) (int, error) {
	n, err := pr.reader.Read(p)
	if n > 0 {
		atomic.AddInt64(&pr.readBytes, int64(n))
		pr.notifyIfNeeded()
	}
	return n, err
}

func (pr *ProgressReader) notifyIfNeeded() {
	if pr.onProgress == nil {
		return
	}
	now := time.Now()
	if now.Sub(pr.lastNotify) < 1*time.Second {
		return
	}
	pr.lastNotify = now

	read := atomic.LoadInt64(&pr.readBytes)
	elapsed := now.Sub(pr.startTime).Seconds()
	if elapsed <= 0 {
		return
	}

	speed := float64(read) / elapsed
	var eta time.Duration
	if speed > 0 && pr.totalBytes > read {
		remainingBytes := pr.totalBytes - read
		eta = time.Duration(float64(remainingBytes)/speed) * time.Second
	}

	pr.onProgress(read, pr.totalBytes, speed, eta)
}

func FormatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	return fmt.Sprintf("%dh%dm%ds", int(d.Hours()), int(d.Minutes())%60, int(d.Seconds())%60)
}

// ProgressWriter wraps an io.Writer with progress tracking.
type ProgressWriter struct {
	w          io.Writer
	totalBytes int64
	readBytes  int64
	startTime  time.Time
	lastNotify time.Time
	onProgress func(read, total int64, speed float64, eta time.Duration)
}

func NewProgressWriter(w io.Writer, totalBytes int64, onProgress func(read, total int64, speed float64, eta time.Duration)) *ProgressWriter {
	return &ProgressWriter{w: w, totalBytes: totalBytes, startTime: time.Now(), onProgress: onProgress}
}

func (pw *ProgressWriter) Write(p []byte) (int, error) {
	n, err := pw.w.Write(p)
	if n > 0 {
		pw.readBytes += int64(n)
		now := time.Now()
		if now.Sub(pw.lastNotify) >= time.Second {
			pw.lastNotify = now
			elapsed := now.Sub(pw.startTime).Seconds()
			if elapsed > 0 && pw.onProgress != nil {
				speed := float64(pw.readBytes) / elapsed
				var eta time.Duration
				if speed > 0 && pw.readBytes < pw.totalBytes {
					eta = time.Duration(float64(pw.totalBytes-pw.readBytes) / speed * float64(time.Second))
				}
				pw.onProgress(pw.readBytes, pw.totalBytes, speed, eta)
			}
		}
	}
	return n, err
}

func RenderProgressBar(current, total int64, length int) string {
	if length <= 0 {
		length = 12
	}
	if total <= 0 {
		return "[" + strings.Repeat("□", length) + "]"
	}

	percent := float64(current) / float64(total)
	if percent > 1.0 {
		percent = 1.0
	}
	if percent < 0 {
		percent = 0
	}

	filledLength := int(percent * float64(length))
	bar := strings.Repeat("■", filledLength) + strings.Repeat("□", length-filledLength)
	return "[" + bar + "]"
}

func FormatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f%cB", float64(b)/float64(div), "KMGTPE"[exp])
}
