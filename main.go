package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/session"
	"github.com/gotd/td/tg"
)

func main() {
	cfgPath := flag.String("config", "config.json", "path to config file")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	slog.Info("starting Zenith-Mirror", "config_path", *cfgPath)

	cfg, err := LoadConfig(*cfgPath)
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	jm := NewJobManager(cfg.MaxConcurrency)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	gdrive, err := NewGDriveService(ctx, cfg)
	if err != nil {
		slog.Error("failed to initialize gdrive service", "error", err)
		os.Exit(1)
	}

	storage := &session.FileStorage{Path: cfg.SessionFile}
	dispatcher := tg.NewUpdateDispatcher()

	clientOpts := telegram.Options{
		SessionStorage: storage,
		UpdateHandler:  dispatcher,
		AllowCDN:       true,
	}
	client := telegram.NewClient(cfg.AppID, cfg.AppHash, clientOpts)

	ts := NewTelegramService(client, gdrive, jm, cfg)
	ts.RegisterHandlers(dispatcher)

	errCh := make(chan error, 1)
	go func() {
		errCh <- client.Run(ctx, func(ctx context.Context) error {
			slog.Info("Zenith-Mirror bot engine running and listening for commands")
			<-ctx.Done()
			return nil
		})
	}()

	select {
	case <-ctx.Done():
		slog.Info("shutdown signal received")
		jm.CancelAllJobs()
		slog.Info("graceful shutdown complete")
	case err := <-errCh:
		if err != nil {
			slog.Error("service exited with error", "error", err)
		}
	}
}
