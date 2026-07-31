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

// detectFileDC makes a tiny request to discover which DC holds the file.
// Returns the DC ID from FILE_MIGRATE error, or 0 if already on correct DC.
func detectFileDC(ctx context.Context, api *tg.Client, location tg.InputFileLocationClass) (int, error) {
	req := &tg.UploadGetFileRequest{
		Location: location,
		Offset:   0,
		Limit:    4096, // minimum valid limit (must be multiple of 4KB)
	}
	req.SetPrecise(true)

	_, err := api.UploadGetFile(ctx, req)
	if err == nil {
		return 0, nil // file is on the current DC
	}

	if rpcErr, ok := tgerr.As(err); ok && rpcErr.Type == "FILE_MIGRATE" {
		return rpcErr.Argument, nil
	}

	return 0, err
}

// createDownloadPool creates a multi-connection pool to the correct DC for a file.
func createDownloadPool(ctx context.Context, client *telegram.Client, location tg.InputFileLocationClass, threads int64) (tg.Invoker, io.Closer, error) {
	dc, err := detectFileDC(ctx, client.API(), location)
	if err != nil {
		return nil, nil, fmt.Errorf("detect file DC: %w", err)
	}

	if dc == 0 {
		// File is on the primary DC — use Pool()
		pool, err := client.Pool(threads)
		if err != nil {
			return nil, nil, fmt.Errorf("create primary pool: %w", err)
		}
		return pool, pool, nil
	}

	slog.Info("file on remote DC, creating pool", "dc", dc, "threads", threads)

	// File is on a different DC — use DC() to create pool there
	pool, err := client.DC(ctx, dc, threads)
	if err != nil {
		return nil, nil, fmt.Errorf("create DC%d pool: %w", dc, err)
	}
	return pool, pool, nil
}