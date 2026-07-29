package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
)

type LeechPipeline struct {
	client *http.Client
	tg     *TelegramService
}

func NewLeechPipeline(tg *TelegramService) *LeechPipeline {
	return &LeechPipeline{
		client: &http.Client{
			Timeout: 0, // Streaming requests do not use hard request timeouts
		},
		tg: tg,
	}
}

func (lp *LeechPipeline) DownloadHTTP(ctx context.Context, rawURL string, headers map[string]string) (io.ReadCloser, int64, string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", rawURL, nil)
	if err != nil {
		return nil, 0, "", fmt.Errorf("invalid request url: %w", err)
	}

	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Zenith-Mirror/1.0")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := lp.client.Do(req)
	if err != nil {
		return nil, 0, "", fmt.Errorf("http request failed: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		resp.Body.Close()
		return nil, 0, "", fmt.Errorf("http error status: %s", resp.Status)
	}

	fileName := "downloaded_file"
	if cd := resp.Header.Get("Content-Disposition"); cd != "" {
		fileName = cd
	}

	return resp.Body, resp.ContentLength, fileName, nil
}
