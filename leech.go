package main

import (
	"context"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/tg"
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

func (lp *LeechPipeline) Download(client *tg.Client, location tg.InputFileLocationClass) *downloader.Builder {
	dl := downloader.NewDownloader().WithAllowCDN(true)
	return dl.Download(client, location)
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

	fileName := ExtractFileName(rawURL, resp.Header.Get("Content-Disposition"))

	return resp.Body, resp.ContentLength, fileName, nil
}

// ExtractFileName parses a filename from Content-Disposition header (RFC 6266/5987)
// with fallback to URL path and finally "downloaded_file.bin".
func ExtractFileName(rawURL string, cd string) string {
	if cd != "" {
		if fn := parseContentDispositionFilename(cd); fn != "" && fn != "." && fn != "/" {
			return fn
		}
	}
	if rawURL != "" {
		if u, err := url.Parse(rawURL); err == nil {
			p := strings.TrimRight(u.Path, "/")
			if p != "" {
				base := filepath.Base(p)
				if unescaped, err := url.PathUnescape(base); err == nil && unescaped != "" && unescaped != "." && unescaped != "/" {
					return unescaped
				}
				if base != "" && base != "." && base != "/" {
					return base
				}
			}
		}
	}
	return "downloaded_file.bin"
}

func parseContentDispositionFilename(cd string) string {
	_, params, err := mime.ParseMediaType(cd)
	if err == nil {
		if fnStar, ok := params["filename*"]; ok && fnStar != "" {
			parts := strings.SplitN(fnStar, "'", 3)
			val := fnStar
			if len(parts) == 3 {
				val = parts[2]
			}
			if decoded, err := url.PathUnescape(val); err == nil && decoded != "" {
				return filepath.Base(decoded)
			}
			if decoded, err := url.QueryUnescape(val); err == nil && decoded != "" {
				return filepath.Base(decoded)
			}
			return filepath.Base(val)
		}
		if fn, ok := params["filename"]; ok && fn != "" {
			return filepath.Base(fn)
		}
	}

	// Manual fallback if mime.ParseMediaType fails on malformed header
	lowerCD := strings.ToLower(cd)
	if idx := strings.Index(lowerCD, "filename*="); idx != -1 {
		val := cd[idx+len("filename*="):]
		if semi := strings.Index(val, ";"); semi != -1 {
			val = val[:semi]
		}
		val = strings.Trim(val, " \"'")
		parts := strings.SplitN(val, "'", 3)
		if len(parts) == 3 {
			val = parts[2]
		}
		if decoded, err := url.PathUnescape(val); err == nil && decoded != "" {
			return filepath.Base(decoded)
		}
		if decoded, err := url.QueryUnescape(val); err == nil && decoded != "" {
			return filepath.Base(decoded)
		}
		if val != "" {
			return filepath.Base(val)
		}
	}

	if idx := strings.Index(lowerCD, "filename="); idx != -1 {
		val := cd[idx+len("filename="):]
		if semi := strings.Index(val, ";"); semi != -1 {
			val = val[:semi]
		}
		val = strings.Trim(val, " \"'")
		if val != "" {
			return filepath.Base(val)
		}
	}

	return ""
}
