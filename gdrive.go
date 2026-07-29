package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

// DriveChunkSize must be a multiple of 256 KiB per Google Drive API requirements.
// Default to 8MB (32 * 256KiB) for stream performance.
const DriveChunkSize = 8 * 1024 * 1024

type GDriveService struct {
	service  *drive.Service
	folderID string
}

func NewGDriveService(ctx context.Context, saFile string, folderID string) (*GDriveService, error) {
	srv, err := drive.NewService(ctx, option.WithCredentialsFile(saFile))
	if err != nil {
		return nil, fmt.Errorf("failed to create drive service: %w", err)
	}

	return &GDriveService{
		service:  srv,
		folderID: folderID,
	}, nil
}

func (g *GDriveService) UploadStream(ctx context.Context, name string, reader io.Reader, size int64) (string, error) {
	fileMeta := &drive.File{
		Name:    name,
		Parents: []string{g.folderID},
	}

	call := g.service.Files.Create(fileMeta).Context(ctx)

	var opts []googleapi.MediaOption
	opts = append(opts, googleapi.ChunkSize(DriveChunkSize))

	call.Media(reader, opts...)

	res, err := call.Do()
	if err != nil {
		return "", fmt.Errorf("drive upload failed: %w", err)
	}

	slog.Info("gdrive upload complete", "file_id", res.Id, "name", name)
	return fmt.Sprintf("https://drive.google.com/file/d/%s/view", res.Id), nil
}
