package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/telegram/message"
	"github.com/gotd/td/telegram/message/styling"
	"github.com/gotd/td/telegram/uploader"
	"github.com/gotd/td/tg"
)

type TelegramService struct {
	client     *telegram.Client
	downloader *downloader.Downloader
	uploader   *uploader.Uploader
	sender     *message.Sender
	cfg        *Config
	gdrive     *GDriveService
	jm         *JobManager
	dispatcher *tg.UpdateDispatcher
}

func NewTelegramService(cfg *Config, jm *JobManager, gdrive *GDriveService) (*TelegramService, error) {
	storage := &session.FileStorage{
		Path: cfg.SessionFile,
	}

	dispatcher := tg.NewUpdateDispatcher()

	opts := telegram.Options{
		SessionStorage: storage,
		UpdateHandler:  dispatcher,
	}

	client := telegram.NewClient(cfg.AppID, cfg.AppHash, opts)
	dl := downloader.NewDownloader()
	api := client.API()
	ul := uploader.NewUploader(api)
	sender := message.NewSender(api)

	ts := &TelegramService{
		client:     client,
		downloader: dl,
		uploader:   ul,
		sender:     sender,
		cfg:        cfg,
		gdrive:     gdrive,
		jm:         jm,
		dispatcher: &dispatcher,
	}

	ts.registerRoutes()

	return ts, nil
}

func (ts *TelegramService) registerRoutes() {
	ts.dispatcher.OnNewMessage(func(ctx context.Context, entities tg.Entities, update *tg.UpdateNewMessage) error {
		msg, ok := update.Message.(*tg.Message)
		if !ok || msg.Out {
			return nil
		}

		var senderID int64
		if peer, ok := msg.PeerID.(*tg.PeerUser); ok {
			senderID = peer.UserID
		}

		if !ts.cfg.IsAllowed(senderID) {
			slog.Warn("unauthorized user attempted command", "user_id", senderID)
			return nil
		}

		text := strings.TrimSpace(msg.Message)
		if strings.HasPrefix(text, "/status") {
			return ts.handleStatus(ctx, entities, update, msg)
		} else if strings.HasPrefix(text, "/cancel") {
			return ts.handleCancel(ctx, entities, update, msg, text)
		} else if strings.HasPrefix(text, "/mirror") {
			return ts.handleMirror(ctx, entities, update, msg)
		} else if strings.HasPrefix(text, "/leech") {
			return ts.handleLeech(ctx, entities, update, msg, text)
		}

		return nil
	})
}

func (ts *TelegramService) handleStatus(ctx context.Context, entities tg.Entities, update *tg.UpdateNewMessage, msg *tg.Message) error {
	jobs := ts.jm.GetActiveJobs()
	if len(jobs) == 0 {
		_, err := ts.sender.Reply(entities, update).Text(ctx, "No active transfer jobs.")
		return err
	}

	statusText := "Active Jobs:\n\n"
	for _, j := range jobs {
		statusText += fmt.Sprintf("• %s [%s]\n  File: %s\n  Size: %s | Progress: %s\n\n",
			j.ID, j.Type, j.FileName, FormatBytes(j.Size), FormatBytes(j.ReadBytes))
	}

	_, err := ts.sender.Reply(entities, update).Text(ctx, statusText)
	return err
}

func (ts *TelegramService) handleCancel(ctx context.Context, entities tg.Entities, update *tg.UpdateNewMessage, msg *tg.Message, text string) error {
	parts := strings.Fields(text)
	if len(parts) < 2 {
		_, err := ts.sender.Reply(entities, update).Text(ctx, "Usage: /cancel <job_id>")
		return err
	}
	jobID := parts[1]
	if ts.jm.CancelJob(jobID) {
		_, err := ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("Cancelled job %s.", jobID))
		return err
	}
	_, err := ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("Job %s not found.", jobID))
	return err
}

func (ts *TelegramService) handleMirror(ctx context.Context, entities tg.Entities, update *tg.UpdateNewMessage, msg *tg.Message) error {
	_, err := ts.sender.Reply(entities, update).StyledText(ctx, styling.Bold("Mirror command received."), styling.Plain(" Send or reply to media to mirror to Google Drive."))
	return err
}

func (ts *TelegramService) handleLeech(ctx context.Context, entities tg.Entities, update *tg.UpdateNewMessage, msg *tg.Message, text string) error {
	parts := strings.Fields(text)
	if len(parts) < 2 {
		_, err := ts.sender.Reply(entities, update).Text(ctx, "Usage: /leech <url>")
		return err
	}
	rawURL := parts[1]
	_, err := ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("Starting leech for %s...", rawURL))
	return err
}

func (ts *TelegramService) Run(ctx context.Context, handler func(ctx context.Context) error) error {
	slog.Info("starting Telegram MTProto client session with UpdateHandler")
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
