package main

import (
	"bytes"
	"io"
	"testing"
	"time"
)

func TestProgressReader(t *testing.T) {
	data := make([]byte, 1024*1024) // 1MB
	buf := bytes.NewReader(data)

	var lastRead int64
	pr := NewProgressReader(buf, int64(len(data)), func(read, total int64, speed float64, eta time.Duration) {
		lastRead = read
	})

	out := make([]byte, 512*1024)
	n, err := io.ReadFull(pr, out)
	if err != nil || n != 512*1024 {
		t.Fatalf("failed reading: %v, n=%d", err, n)
	}

	if atomicRead := pr.readBytes; atomicRead != 512*1024 {
		t.Errorf("expected 512KB read, got %d", atomicRead)
	}
	_ = lastRead

	if FormatBytes(1024) != "1.00 KB" {
		t.Errorf("FormatBytes failed, got %s", FormatBytes(1024))
	}
	if FormatBytes(1048576) != "1.00 MB" {
		t.Errorf("FormatBytes failed, got %s", FormatBytes(1048576))
	}
}
