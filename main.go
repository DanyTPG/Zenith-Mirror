package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/gotd/contrib/middleware/ratelimit"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/session"
	"github.com/gotd/td/tg"
	"golang.org/x/time/rate"
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

	// Token bucket rate limiter: proactive throttle on all RPCs.
	// ponytail: ratelimit.Wait blocks the caller's own goroutine (reentrancy-safe).
	// floodwait.NewWaiter() is NOT safe here — its single sender goroutine deadlocks
	// when an RPC triggered inside a download (auth.exportAuthorization during DC
	// migration) re-enters Handle() from the same call chain. Re-add only if
	// upstream fixes reentrancy; raw_download.go already backs off FLOOD_WAIT itself.
	rl := ratelimit.New(rate.Every(cfg.RPCDelay), cfg.RPCBurst)

	clientOpts := telegram.Options{
		SessionStorage: storage,
		UpdateHandler:  dispatcher,
		AllowCDN:       true,
		Middlewares: []telegram.Middleware{
			telegram.MiddlewareFunc(func(next tg.Invoker) telegram.InvokeFunc {
				return rl.Handle(next)
			}),
		},
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
		ts.ClosePools()
		slog.Info("graceful shutdown complete")
	case err := <-errCh:
		if err != nil {
			slog.Error("service exited with error", "error", err)
		}
	}
}
