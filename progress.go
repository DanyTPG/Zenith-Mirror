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

func FormatBytes(bytes int64) string {
	if bytes <= 0 {
		return "0 B"
	}
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func RenderProgressBar(read, total int64, width int) string {
	if total <= 0 {
		return "[░░░░░░░░░░] 0%"
	}
	pct := float64(read) / float64(total)
	if pct > 1.0 {
		pct = 1.0
	}
	filledLen := int(pct * float64(width))
	if filledLen < 0 {
		filledLen = 0
	}
	emptyLen := width - filledLen

	bar := strings.Repeat("█", filledLen) + strings.Repeat("░", emptyLen)
	return fmt.Sprintf("[%s] %d%%", bar, int(pct*100))
}
