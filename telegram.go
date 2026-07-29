package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

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

		slog.Info("received telegram message", "sender_id", senderID, "text", msg.Message)

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
			return ts.handleLeech(ctx, entities, update, msg, text, senderID)
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
		statusText += fmt.Sprintf("• %s [%s]\n  File: %s\n  Size: %s | Progress: %s\n  Status: %s\n\n",
			j.ID, j.Type, j.FileName, FormatBytes(j.Size), FormatBytes(j.ReadBytes), j.Status)
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

func (ts *TelegramService) handleLeech(ctx context.Context, entities tg.Entities, update *tg.UpdateNewMessage, msg *tg.Message, text string, userID int64) error {
	parts := strings.Fields(text)
	if len(parts) < 2 {
		_, err := ts.sender.Reply(entities, update).Text(ctx, "Usage: /leech <url>")
		return err
	}
	rawURL := parts[1]

	job, err := ts.jm.CreateJob(ctx, JobTypeLeech, rawURL, 0, userID)
	if err != nil {
		slog.Error("failed creating leech job", "error", err)
		_, replyErr := ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("Error creating job: %v", err))
		return replyErr
	}

	slog.Info("leech job created", "job_id", job.ID, "url", rawURL)
	_, _ = ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("Starting leech [%s] for %s...", job.ID, rawURL))

	go ts.executeLeechJob(job, rawURL, entities, update)

	return nil
}

func (ts *TelegramService) executeLeechJob(job *Job, rawURL string, entities tg.Entities, update *tg.UpdateNewMessage) {
	defer ts.jm.FinishJob(job.ID)

	slog.Info("executing leech job", "job_id", job.ID, "url", rawURL)

	req, err := http.NewRequestWithContext(job.Ctx, "GET", rawURL, nil)
	if err != nil {
		slog.Error("invalid url for leech", "job_id", job.ID, "error", err)
		return
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Zenith-Mirror/1.0")

	client := &http.Client{Timeout: 0}
	resp, err := client.Do(req)
	if err != nil {
		slog.Error("leech download request failed", "job_id", job.ID, "error", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		slog.Error("leech download non-200 status", "job_id", job.ID, "status", resp.Status)
		return
	}

	job.Size = resp.ContentLength
	job.FileName = "downloaded_file"
	job.Status = "Downloading"

	pr := NewProgressReader(resp.Body, job.Size, func(read, total int64, speed float64, eta time.Duration) {
		job.ReadBytes = read
		job.Speed = speed
		job.ETA = eta
		slog.Info("leech progress", "job_id", job.ID, "read", FormatBytes(read), "total", FormatBytes(total), "speed_kbps", speed/1024)
	})

	// Stream upload to Telegram
	slog.Info("uploading stream to telegram", "job_id", job.ID, "size", FormatBytes(job.Size))
	job.Status = "Uploading to Telegram"

	uploadedFile, err := ts.uploader.FromReader(job.Ctx, job.FileName, pr)
	if err != nil {
		slog.Error("telegram upload failed", "job_id", job.ID, "error", err)
		return
	}

	document := message.UploadedDocument(uploadedFile).Filename(job.FileName)
	if _, err := ts.sender.Reply(entities, update).Media(job.Ctx, document); err != nil {
		slog.Error("failed sending media message", "job_id", job.ID, "error", err)
		return
	}

	job.Status = "Completed"
	slog.Info("leech job completed successfully", "job_id", job.ID)
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
