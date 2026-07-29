package main

import (
	"context"
	"fmt"
	"io"
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
			return ts.handleMirror(ctx, entities, update, msg, senderID)
		} else if strings.HasPrefix(text, "/leech") {
			return ts.handleLeech(ctx, entities, update, msg, text, senderID)
		}

		return nil
	})
}

func (ts *TelegramService) handleStatus(ctx context.Context, entities tg.Entities, update *tg.UpdateNewMessage, msg *tg.Message) error {
	text := ts.buildStatusText()
	_, err := ts.sender.Reply(entities, update).StyledText(ctx, styling.Plain(text))
	return err
}

func (ts *TelegramService) buildStatusText() string {
	jobs := ts.jm.GetActiveJobs()
	if len(jobs) == 0 {
		return "No active transfer jobs."
	}

	statusText := "Zenith-Mirror Active Transfers:\n\n"
	for _, j := range jobs {
		bar := RenderProgressBar(j.ReadBytes, j.Size, 10)
		etaStr := "N/A"
		if j.ETA > 0 {
			etaStr = j.ETA.Round(time.Second).String()
		}
		statusText += fmt.Sprintf("• %s [%s]\n  File: %s\n  %s\n  Size: %s / %s | Speed: %s/s | ETA: %s\n  Status: %s\n\n",
			j.ID, j.Type, j.FileName, bar, FormatBytes(j.ReadBytes), FormatBytes(j.Size), FormatBytes(int64(j.Speed)), etaStr, j.Status)
	}
	return statusText
}

func (ts *TelegramService) startLiveStatusUpdater(jobCtx context.Context, entities tg.Entities, update *tg.UpdateNewMessage) {
	delay := ts.cfg.StatusRefreshDelay
	if delay <= 0 {
		delay = 5
	}
	ticker := time.NewTicker(time.Duration(delay) * time.Second)
	defer ticker.Stop()

	initialText := ts.buildStatusText()
	updates, err := ts.sender.Reply(entities, update).StyledText(jobCtx, styling.Plain(initialText))
	if err != nil {
		slog.Error("failed sending live status message", "error", err)
		return
	}

	var statusMsgID int
	if u, ok := updates.(*tg.Updates); ok {
		for _, up := range u.GetUpdates() {
			if newMsg, ok := up.(*tg.UpdateNewMessage); ok {
				statusMsgID = newMsg.Message.GetID()
				break
			}
		}
	} else if uShort, ok := updates.(*tg.UpdateShortMessage); ok {
		statusMsgID = uShort.ID
	}

	var lastText string = initialText

	for {
		select {
		case <-jobCtx.Done():
			// When all jobs complete, delete status message
			if statusMsgID > 0 {
				_, _ = ts.client.API().MessagesDeleteMessages(context.Background(), &tg.MessagesDeleteMessagesRequest{
					Revoke: true,
					ID:     []int{statusMsgID},
				})
			}
			return
		case <-ticker.C:
			currentText := ts.buildStatusText()
			if currentText != lastText && statusMsgID > 0 {
				_, _ = ts.sender.Reply(entities, update).Edit(statusMsgID).StyledText(jobCtx, styling.Plain(currentText))
				lastText = currentText
			}
		}
	}
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

func (ts *TelegramService) handleMirror(ctx context.Context, entities tg.Entities, update *tg.UpdateNewMessage, msg *tg.Message, userID int64) error {
	if msg.ReplyTo == nil {
		_, err := ts.sender.Reply(entities, update).Text(ctx, "Reply to a media/file message with /mirror to upload it to Google Drive.")
		return err
	}

	job, err := ts.jm.CreateJob(ctx, JobTypeMirror, "telegram_media", 0, userID)
	if err != nil {
		_, replyErr := ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("Error creating job: %v", err))
		return replyErr
	}

	go ts.startLiveStatusUpdater(job.Ctx, entities, update)
	go ts.executeMirrorJob(job, entities, update)

	return nil
}

func (ts *TelegramService) executeMirrorJob(job *Job, entities tg.Entities, update *tg.UpdateNewMessage) {
	defer ts.jm.FinishJob(job.ID)

	slog.Info("executing mirror job", "job_id", job.ID)
	job.Status = "Mirroring Telegram Media to Google Drive"

	pr, pw := io.Pipe()

	progressReader := NewProgressReader(pr, job.Size, func(read, total int64, speed float64, eta time.Duration) {
		job.ReadBytes = read
		job.Speed = speed
		job.ETA = eta
	})

	go func() {
		defer pw.Close()
	}()

	driveURL, err := ts.gdrive.UploadStream(job.Ctx, job.FileName, progressReader, job.Size)
	if err != nil {
		slog.Error("gdrive upload failed", "job_id", job.ID, "error", err)
		job.Status = fmt.Sprintf("Failed: %v", err)
		return
	}

	job.Status = "Completed"
	slog.Info("mirror job completed", "job_id", job.ID, "drive_url", driveURL)
	_, _ = ts.sender.Reply(entities, update).Text(context.Background(), fmt.Sprintf("✅ Mirror Complete!\nGoogle Drive Link: %s", driveURL))
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

	go ts.startLiveStatusUpdater(job.Ctx, entities, update)
	go ts.executeLeechJob(job, rawURL, entities, update)

	return nil
}

func (ts *TelegramService) executeLeechJob(job *Job, rawURL string, entities tg.Entities, update *tg.UpdateNewMessage) {
	defer ts.jm.FinishJob(job.ID)

	slog.Info("executing leech job", "job_id", job.ID, "url", rawURL)

	req, err := http.NewRequestWithContext(job.Ctx, "GET", rawURL, nil)
	if err != nil {
		slog.Error("invalid url for leech", "job_id", job.ID, "error", err)
		job.Status = fmt.Sprintf("Failed: %v", err)
		return
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Zenith-Mirror/1.0")

	client := &http.Client{Timeout: 0}
	resp, err := client.Do(req)
	if err != nil {
		slog.Error("leech download request failed", "job_id", job.ID, "error", err)
		job.Status = fmt.Sprintf("Failed: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		slog.Error("leech download non-200 status", "job_id", job.ID, "status", resp.Status)
		job.Status = fmt.Sprintf("HTTP Error %s", resp.Status)
		return
	}

	job.Size = resp.ContentLength
	urlParts := strings.Split(rawURL, "/")
	job.FileName = urlParts[len(urlParts)-1]
	if job.FileName == "" {
		job.FileName = "downloaded_file.bin"
	}
	job.Status = "Downloading & Uploading to Telegram"

	pr := NewProgressReader(resp.Body, job.Size, func(read, total int64, speed float64, eta time.Duration) {
		job.ReadBytes = read
		job.Speed = speed
		job.ETA = eta
	})

	slog.Info("uploading stream to telegram", "job_id", job.ID, "size", FormatBytes(job.Size))

	uploadedFile, err := ts.uploader.FromReader(job.Ctx, job.FileName, pr)
	if err != nil {
		slog.Error("telegram upload failed", "job_id", job.ID, "error", err)
		job.Status = fmt.Sprintf("Upload Failed: %v", err)
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
