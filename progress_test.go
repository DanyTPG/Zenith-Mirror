package main

import (
	"bytes"
	"io"
	"testing"
	"time"
)

func TestProgressReader(t *testing.T) {
	data := []byte("hello world, this is a test stream for progress reader")
	buf := bytes.NewReader(data)

	pr := NewProgressReader(buf, int64(len(data)), func(read, total int64, speed float64, eta time.Duration) {
	})

	out, err := io.ReadAll(pr)
	if err != nil {
		t.Fatalf("failed reading: %v", err)
	}

	if len(out) != len(data) {
		t.Errorf("expected %d bytes, got %d", len(data), len(out))
	}
}

func TestFormatBytes(t *testing.T) {
	if got := FormatBytes(1024); got != "1.00KB" {
		t.Errorf("FormatBytes failed, got %s", got)
	}
	if got := FormatBytes(1048576); got != "1.00MB" {
		t.Errorf("FormatBytes failed, got %s", got)
	}
}
