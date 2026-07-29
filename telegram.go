package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/telegram/uploader"
)

type TelegramService struct {
	client     *telegram.Client
	downloader *downloader.Downloader
	uploader   *uploader.Uploader
	cfg        *Config
}

func NewTelegramService(cfg *Config) (*TelegramService, error) {
	storage := &session.FileStorage{
		Path: cfg.SessionFile,
	}

	opts := telegram.Options{
		SessionStorage: storage,
	}

	client := telegram.NewClient(cfg.AppID, cfg.AppHash, opts)
	dl := downloader.NewDownloader()
	ul := uploader.NewUploader(client.API())

	return &TelegramService{
		client:     client,
		downloader: dl,
		uploader:   ul,
		cfg:        cfg,
	}, nil
}

func (ts *TelegramService) Run(ctx context.Context, handler func(ctx context.Context) error) error {
	slog.Info("starting Telegram MTProto client session")
	return ts.client.Run(ctx, func(ctx context.Context) error {
		if ts.cfg.BotToken != "" {
			status, err := ts.client.Auth().Status(ctx)
			if err != nil {
				return fmt.Errorf("failed auth status: %w", err)
			}
			if !status.Authorized {
				slog.Info("logging in via bot token")
				if _, err := ts.client.Auth().Bot(ctx, ts.cfg.BotToken); err != nil {
					return fmt.Errorf("failed bot auth: %w", err)
				}
			}
		}
		return handler(ctx)
	})
}
