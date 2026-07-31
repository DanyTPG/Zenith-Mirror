package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

// detectFileDC makes a tiny request through a raw pool invoker (no auto-migration)
// to discover which DC holds the file via FILE_MIGRATE error.
// Returns the DC ID, or 0 if already on correct DC.
func detectFileDC(ctx context.Context, client *telegram.Client, location tg.InputFileLocationClass) (int, error) {
	// Use Pool(1) — raw invoker that does NOT auto-handle FILE_MIGRATE
	rawPool, err := client.Pool(1)
	if err != nil {
		return 0, fmt.Errorf("create probe pool: %w", err)
	}
	defer rawPool.Close()

	rawAPI := tg.NewClient(rawPool)
	req := &tg.UploadGetFileRequest{
		Location: location,
		Offset:   0,
		Limit:    4096,
	}
	req.SetPrecise(true)

	_, err = rawAPI.UploadGetFile(ctx, req)
	if err == nil {
		return 0, nil
	}

	if rpcErr, ok := tgerr.As(err); ok && rpcErr.Type == "FILE_MIGRATE" {
		return rpcErr.Argument, nil
	}

	return 0, err
}

// createDownloadPool creates a multi-connection pool to the correct DC for a file.
func createDownloadPool(ctx context.Context, client *telegram.Client, location tg.InputFileLocationClass, threads int64) (tg.Invoker, io.Closer, error) {
	dc, err := detectFileDC(ctx, client, location)
	if err != nil {
		return nil, nil, fmt.Errorf("detect file DC: %w", err)
	}

	if dc == 0 {
		slog.Info("file on primary DC, creating pool")
		pool, err := client.Pool(threads)
		if err != nil {
			return nil, nil, fmt.Errorf("create primary pool: %w", err)
		}
		return pool, pool, nil
	}

	slog.Info("file on remote DC, creating pool", "dc", dc, "threads", threads)

	pool, err := client.DC(ctx, dc, threads)
	if err != nil {
		return nil, nil, fmt.Errorf("create DC%d pool: %w", dc, err)
	}
	return pool, pool, nil
}