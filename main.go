package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
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

	tg, err := NewTelegramService(cfg, jm, gdrive)
	if err != nil {
		slog.Error("failed to initialize telegram service", "error", err)
		os.Exit(1)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- tg.Run(ctx, func(ctx context.Context) error {
			slog.Info("Zenith-Mirror bot engine running and listening for commands")
			<-ctx.Done()
			return nil
		})
	}()

	select {
	case <-ctx.Done():
		slog.Info("shutdown signal received, cancelling active jobs...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = shutdownCtx
		slog.Info("graceful shutdown complete")
	case err := <-errCh:
		if err != nil {
			slog.Error("service exited with error", "error", err)
		}
	}
}
