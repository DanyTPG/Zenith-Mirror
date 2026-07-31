package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/telegram/message"
	"github.com/gotd/td/telegram/message/styling"
	"github.com/gotd/td/telegram/uploader"
	"github.com/gotd/td/tg"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/mem"
	netstat "github.com/shirou/gopsutil/v3/net"
)

type TelegramService struct {
	client       *telegram.Client
	downloader   *downloader.Downloader
	uploader     *uploader.Uploader
	sender       *message.Sender
	cfg          *Config
	gdrive       *GDriveService
	jm           *JobManager
	dispatcher   *tg.UpdateDispatcher
	startTime    time.Time
	lastStatusMu sync.Mutex
	lastStatusID int
	lastStatusFn context.CancelFunc
}

type ProgressWriter struct {
	writer     io.Writer
	totalBytes int64
	readBytes  int64
	startTime  time.Time
	onProgress func(read, total int64, speed float64, eta time.Duration)
	lastNotify time.Time
}

func NewProgressWriter(w io.Writer, totalBytes int64, onProgress func(read, total int64, speed float64, eta time.Duration)) *ProgressWriter {
	return &ProgressWriter{
		writer:     w,
		totalBytes: totalBytes,
		startTime:  time.Now(),
		onProgress: onProgress,
	}
}

func (pw *ProgressWriter) Write(p []byte) (int, error) {
	n, err := pw.writer.Write(p)
	if n > 0 {
		pw.readBytes += int64(n)
		pw.notifyIfNeeded()
	}
	return n, err
}

func (pw *ProgressWriter) notifyIfNeeded() {
	if pw.onProgress == nil {
		return
	}
	now := time.Now()
	if now.Sub(pw.lastNotify) < 1*time.Second {
		return
	}
	pw.lastNotify = now

	read := pw.readBytes
	elapsed := now.Sub(pw.startTime).Seconds()
	if elapsed <= 0 {
		return
	}

	speed := float64(read) / elapsed
	var eta time.Duration
	if speed > 0 && pw.totalBytes > read {
		remainingBytes := pw.totalBytes - read
		eta = time.Duration(float64(remainingBytes)/speed) * time.Second
	}

	pw.onProgress(read, pw.totalBytes, speed, eta)
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
	dl := downloader.NewDownloader().
		WithPartSize(512 * 1024). // 512KB parts — max allowed by Telegram, fewer round-trips
		WithAllowCDN(true)        // use CDN nodes when available
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
		startTime:  time.Now(),
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
		if text == "/start" {
			return ts.handleStart(ctx, entities, update)
		} else if strings.HasPrefix(text, "/status") {
			return ts.handleStatus(ctx, entities, update, msg)
		} else if text == "/stats" {
			return ts.handleStats(ctx, entities, update)
		} else if text == "/restart" {
			return ts.handleRestart(ctx, entities, update)
		} else if text == "/cancel-all" {
			return ts.handleCancelAll(ctx, entities, update)
		} else if strings.HasPrefix(text, "/cancel") {
			return ts.handleCancel(ctx, entities, update, msg, text)
		} else if strings.HasPrefix(text, "/mirror") || strings.HasPrefix(text, "/m") {
			return ts.handleMirror(ctx, entities, update, msg, text, senderID)
		} else if strings.HasPrefix(text, "/leech") {
			return ts.handleLeech(ctx, entities, update, msg, text, senderID)
		}

		return nil
	})
}

func (ts *TelegramService) handleStart(ctx context.Context, entities tg.Entities, update *tg.UpdateNewMessage) error {
	opts := []styling.StyledTextOption{
		styling.Bold("⚡ Zenith-Mirror Bot"),
		styling.Plain("\n\n"),
		styling.Bold("Available Commands:"),
		styling.Plain("\n"),
		styling.Plain("• `/mirror` or `/m [-i count]` (Reply to media to upload to Google Drive)\n"),
		styling.Plain("• `/leech <url>` (Download direct link to Telegram)\n"),
		styling.Plain("• `/status` (Check active jobs & queue)\n"),
		styling.Plain("• `/stats` (View system & bot statistics)\n"),
		styling.Plain("• `/cancel <job_id>` (Cancel specific transfer)\n"),
		styling.Plain("• `/cancel-all` (Cancel all transfers and flush queue)\n"),
		styling.Plain("• `/restart` (Restart bot service)"),
	}
	_, err := ts.sender.Reply(entities, update).StyledText(ctx, opts...)
	return err
}

func (ts *TelegramService) handleRestart(ctx context.Context, entities tg.Entities, update *tg.UpdateNewMessage) error {
	_, _ = ts.sender.Reply(entities, update).Text(ctx, "🔄 Restarting Zenith-Mirror...")
	go func() {
		time.Sleep(1 * time.Second)
		slog.Info("restarting process via /restart command")
		os.Exit(0)
	}()
	return nil
}

func (ts *TelegramService) handleStats(ctx context.Context, entities tg.Entities, update *tg.UpdateNewMessage) error {
	botUptime := formatDuration(time.Since(ts.startTime))

	osUptimeStr := "N/A"
	if hostInfo, err := host.Info(); err == nil {
		osUptimeStr = formatDuration(time.Duration(hostInfo.Uptime) * time.Second)
	}

	totalDisk, usedDisk, freeDisk, diskPercent := "N/A", "N/A", "N/A", 0.0
	if d, err := disk.Usage("/"); err == nil {
		totalDisk = FormatBytes(int64(d.Total))
		usedDisk = FormatBytes(int64(d.Used))
		freeDisk = FormatBytes(int64(d.Free))
		diskPercent = d.UsedPercent
	}

	netSent, netRecv := "N/A", "N/A"
	if ioCounters, err := netstat.IOCounters(false); err == nil && len(ioCounters) > 0 {
		netSent = FormatBytes(int64(ioCounters[0].BytesSent))
		netRecv = FormatBytes(int64(ioCounters[0].BytesRecv))
	}

	cpuPercent := 0.0
	if percents, err := cpu.Percent(0, false); err == nil && len(percents) > 0 {
		cpuPercent = percents[0]
	}

	physCores := runtime.NumCPU()
	totalCores := runtime.NumCPU()
	if counts, err := cpu.Counts(false); err == nil && counts > 0 {
		physCores = counts
	}
	if counts, err := cpu.Counts(true); err == nil && counts > 0 {
		totalCores = counts
	}

	memTotal, memFree, memUsed, memPercent := "N/A", "N/A", "N/A", 0.0
	swapTotal, swapUsed, swapPercent := "N/A", "N/A", 0.0
	if v, err := mem.VirtualMemory(); err == nil {
		memTotal = FormatBytes(int64(v.Total))
		memFree = FormatBytes(int64(v.Free))
		memUsed = FormatBytes(int64(v.Used))
		memPercent = v.UsedPercent
	}
	if s, err := mem.SwapMemory(); err == nil {
		swapTotal = FormatBytes(int64(s.Total))
		swapUsed = FormatBytes(int64(s.Used))
		swapPercent = s.UsedPercent
	}

	opts := []styling.StyledTextOption{
		styling.Bold("Bot Uptime:"), styling.Plain(fmt.Sprintf(" %s\n", botUptime)),
		styling.Bold("OS Uptime:"), styling.Plain(fmt.Sprintf(" %s\n\n", osUptimeStr)),
		styling.Bold("Total Disk Space:"), styling.Plain(fmt.Sprintf(" %s\n", totalDisk)),
		styling.Bold("Used:"), styling.Plain(fmt.Sprintf(" %s | ", usedDisk)),
		styling.Bold("Free:"), styling.Plain(fmt.Sprintf(" %s\n\n", freeDisk)),
		styling.Bold("Upload:"), styling.Plain(fmt.Sprintf(" %s\n", netSent)),
		styling.Bold("Download:"), styling.Plain(fmt.Sprintf(" %s\n\n", netRecv)),
		styling.Bold("CPU:"), styling.Plain(fmt.Sprintf(" %.1f%%\n", cpuPercent)),
		styling.Bold("RAM:"), styling.Plain(fmt.Sprintf(" %.1f%%\n", memPercent)),
		styling.Bold("DISK:"), styling.Plain(fmt.Sprintf(" %.1f%%\n\n", diskPercent)),
		styling.Bold("Physical Cores:"), styling.Plain(fmt.Sprintf(" %d\n", physCores)),
		styling.Bold("Total Cores:"), styling.Plain(fmt.Sprintf(" %d\n\n", totalCores)),
		styling.Bold("SWAP:"), styling.Plain(fmt.Sprintf(" %s (%s) | ", swapTotal, swapUsed)),
		styling.Bold("Used:"), styling.Plain(fmt.Sprintf(" %.1f%%\n", swapPercent)),
		styling.Bold("Memory Total:"), styling.Plain(fmt.Sprintf(" %s\n", memTotal)),
		styling.Bold("Memory Free:"), styling.Plain(fmt.Sprintf(" %s\n", memFree)),
		styling.Bold("Memory Used:"), styling.Plain(fmt.Sprintf(" %s", memUsed)),
	}

	_, err := ts.sender.Reply(entities, update).StyledText(ctx, opts...)
	return err
}

func formatDuration(d time.Duration) string {
	days := int(d.Hours()) / 24
	hours := int(d.Hours()) % 24
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60

	if days > 0 {
		return fmt.Sprintf("%dd%dh%dm%ds", days, hours, minutes, seconds)
	}
	if hours > 0 {
		return fmt.Sprintf("%dh%dm%ds", hours, minutes, seconds)
	}
	if minutes > 0 {
		return fmt.Sprintf("%dm%ds", minutes, seconds)
	}
	return fmt.Sprintf("%ds", seconds)
}

func (ts *TelegramService) deleteLastStatus() {
	ts.lastStatusMu.Lock()
	id := ts.lastStatusID
	fn := ts.lastStatusFn
	ts.lastStatusMu.Unlock()
	if fn != nil {
		fn()
	}
	if id <= 0 {
		return
	}
	slog.Info("deleting previous status message", "status_msg_id", id)
	_, err := ts.client.API().MessagesDeleteMessages(context.Background(), &tg.MessagesDeleteMessagesRequest{
		Revoke: true,
		ID:     []int{id},
	})
	if err != nil {
		slog.Error("failed to delete previous status message", "status_msg_id", id, "error", err)
	}
}

func (ts *TelegramService) setLastStatus(id int, cancelFn context.CancelFunc) {
	ts.lastStatusMu.Lock()
	ts.lastStatusID = id
	ts.lastStatusFn = cancelFn
	ts.lastStatusMu.Unlock()
}

func (ts *TelegramService) clearLastStatusIf(id int) {
	ts.lastStatusMu.Lock()
	if ts.lastStatusID == id {
		ts.lastStatusID = 0
		ts.lastStatusFn = nil
	}
	ts.lastStatusMu.Unlock()
}

func (ts *TelegramService) handleStatus(ctx context.Context, entities tg.Entities, update *tg.UpdateNewMessage, msg *tg.Message) error {
	ts.deleteLastStatus()
	opts := ts.buildStatusStyledText()
	updates, err := ts.sender.Reply(entities, update).StyledText(ctx, opts...)
	if err != nil {
		return err
	}
	ts.setLastStatus(extractMsgIDFromUpdates(updates), nil)
	return nil
}

func (ts *TelegramService) buildStatusStyledText() []styling.StyledTextOption {
	jobs := ts.jm.GetActiveJobs()
	if len(jobs) == 0 {
		return []styling.StyledTextOption{styling.Plain("No active transfer jobs.")}
	}

	var options []styling.StyledTextOption

	for i, j := range jobs {
		if j.State == StateQueued {
			options = append(options, styling.Bold(fmt.Sprintf("%d.Queued:", i+1)))
			options = append(options, styling.Plain(" "))
			options = append(options, styling.Code(j.FileName))
			options = append(options, styling.Plain(fmt.Sprintf("\nSize: %s | Status: Waiting in Queue\n", FormatBytes(j.Size))))
			options = append(options, styling.Code(fmt.Sprintf("/cancel %s", j.ID)))
			options = append(options, styling.Plain("\n\n"))
			continue
		}

		bar := RenderProgressBar(j.ReadBytes, j.Size, 12)
		pct := 0.0
		if j.Size > 0 {
			pct = (float64(j.ReadBytes) / float64(j.Size)) * 100
		}

		etaStr := "N/A"
		if j.ETA > 0 {
			etaStr = formatDuration(j.ETA)
		}

		phaseName := "Download"
		if j.Phase == PhaseUploading {
			phaseName = "Upload"
		}

		options = append(options, styling.Bold(fmt.Sprintf("%d.%s:", i+1, phaseName)))
		options = append(options, styling.Plain(" "))
		options = append(options, styling.Code(j.FileName))
		options = append(options, styling.Plain(fmt.Sprintf("\n%s %.2f%%\n", bar, pct)))
		options = append(options, styling.Bold("Processed:"))
		options = append(options, styling.Plain(fmt.Sprintf(" %s of %s\n", FormatBytes(j.ReadBytes), FormatBytes(j.Size))))
		options = append(options, styling.Bold("Speed:"))
		options = append(options, styling.Plain(fmt.Sprintf(" %s/s | ", FormatBytes(int64(j.Speed)))))
		options = append(options, styling.Bold("ETA:"))
		options = append(options, styling.Plain(fmt.Sprintf(" %s\n", etaStr)))
		options = append(options, styling.Code(fmt.Sprintf("/cancel %s", j.ID)))
		options = append(options, styling.Plain("\n\n"))
	}

	cpuPercent := 0.0
	if percents, err := cpu.Percent(0, false); err == nil && len(percents) > 0 {
		cpuPercent = percents[0]
	}

	freeDisk := "N/A"
	if d, err := disk.Usage("/"); err == nil {
		freeDisk = FormatBytes(int64(d.Free))
	}

	memPercent := 0.0
	if v, err := mem.VirtualMemory(); err == nil {
		memPercent = v.UsedPercent
	}

	osUptimeStr := "N/A"
	if hostInfo, err := host.Info(); err == nil {
		osUptimeStr = formatDuration(time.Duration(hostInfo.Uptime) * time.Second)
	}

	var totalDLSpeed, totalULSpeed float64
	for _, j := range jobs {
		if j.State == StateQueued {
			continue
		}
		if j.Phase == PhaseDownloading {
			totalDLSpeed += j.Speed
		} else {
			totalULSpeed += j.Speed
		}
	}

	options = append(options, styling.Plain("\n"))
	options = append(options, styling.Bold("Total DL:"))
	options = append(options, styling.Plain(fmt.Sprintf(" %s/s | ", FormatBytes(int64(totalDLSpeed)))))
	options = append(options, styling.Bold("Total UL:"))
	options = append(options, styling.Plain(fmt.Sprintf(" %s/s\n", FormatBytes(int64(totalULSpeed)))))

	options = append(options, styling.Bold("CPU:"))
	options = append(options, styling.Plain(fmt.Sprintf(" %.1f%% | ", cpuPercent)))
	options = append(options, styling.Bold("FREE:"))
	options = append(options, styling.Plain(fmt.Sprintf(" %s\n", freeDisk)))
	options = append(options, styling.Bold("RAM:"))
	options = append(options, styling.Plain(fmt.Sprintf(" %.1f%% | ", memPercent)))
	options = append(options, styling.Bold("UPTIME:"))
	options = append(options, styling.Plain(fmt.Sprintf(" %s", osUptimeStr)))

	return options
}

func extractMsgIDFromUpdates(updates tg.UpdatesClass) int {
	switch u := updates.(type) {
	case *tg.Updates:
		for _, up := range u.GetUpdates() {
			switch unm := up.(type) {
			case *tg.UpdateNewMessage:
				if m, ok := unm.Message.(*tg.Message); ok {
					return m.ID
				}
			case *tg.UpdateNewChannelMessage:
				if m, ok := unm.Message.(*tg.Message); ok {
					return m.ID
				}
			}
		}
	case *tg.UpdateShortMessage:
		return u.ID
	case *tg.UpdateShortSentMessage:
		return u.ID
	}
	return 0
}

func (ts *TelegramService) startLiveStatusUpdater(jobCtx context.Context, entities tg.Entities, update *tg.UpdateNewMessage, msg *tg.Message) {
	ts.deleteLastStatus()

	statusCtx, statusCancel := context.WithCancel(jobCtx)

	delay := ts.cfg.StatusRefreshDelay
	if delay <= 0 {
		delay = 5
	}
	ticker := time.NewTicker(time.Duration(delay) * time.Second)
	defer ticker.Stop()

	initialOpts := ts.buildStatusStyledText()
	updates, err := ts.sender.Reply(entities, update).StyledText(statusCtx, initialOpts...)
	if err != nil {
		slog.Error("failed sending live status message", "error", err)
		statusCancel()
		return
	}

	statusMsgID := extractMsgIDFromUpdates(updates)
	ts.setLastStatus(statusMsgID, statusCancel)
	slog.Info("created live status message", "status_msg_id", statusMsgID)

	for {
		select {
		case <-statusCtx.Done():
			ts.clearLastStatusIf(statusMsgID)
			if statusMsgID > 0 {
				slog.Info("deleting completed job live status message", "status_msg_id", statusMsgID)
				_, delErr := ts.client.API().MessagesDeleteMessages(context.Background(), &tg.MessagesDeleteMessagesRequest{
					Revoke: true,
					ID:     []int{statusMsgID},
				})
				if delErr != nil {
					slog.Error("failed to delete live status message", "status_msg_id", statusMsgID, "error", delErr)
				}
			}
			return
		case <-ticker.C:
			currentOpts := ts.buildStatusStyledText()
			if statusMsgID > 0 {
				slog.Info("editing live status message", "status_msg_id", statusMsgID)
				_, editErr := ts.sender.Reply(entities, update).Edit(statusMsgID).StyledText(statusCtx, currentOpts...)
				if editErr != nil {
					slog.Error("failed editing live status message", "status_msg_id", statusMsgID, "error", editErr)
				}
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

func (ts *TelegramService) handleCancelAll(ctx context.Context, entities tg.Entities, update *tg.UpdateNewMessage) error {
	count := ts.jm.CancelAllJobs()
	_, err := ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("Cancelled all %d active and queued jobs.", count))
	return err
}

func (ts *TelegramService) handleMirror(ctx context.Context, entities tg.Entities, update *tg.UpdateNewMessage, msg *tg.Message, text string, userID int64) error {
	parts := strings.Fields(text)

	// Check if a direct URL was passed: /mirror <url> or /m <url>
	var rawURL string
	for _, part := range parts[1:] {
		if strings.HasPrefix(part, "http://") || strings.HasPrefix(part, "https://") {
			rawURL = part
			break
		}
	}

	if rawURL != "" {
		urlParts := strings.Split(rawURL, "/")
		fileName := urlParts[len(urlParts)-1]
		if fileName == "" {
			fileName = "downloaded_file.bin"
		}

		var jobRef *Job
		execFunc := func() {
			ts.executeURLMirrorJob(jobRef, rawURL, entities, update)
		}

		job, err := ts.jm.CreateJob(ctx, JobTypeMirror, fileName, 0, userID, execFunc)
		if err != nil {
			slog.Error("failed creating URL mirror job", "error", err)
			_, replyErr := ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("Error creating job: %v", err))
			return replyErr
		}
		jobRef = job

		slog.Info("url mirror job created", "job_id", job.ID, "url", rawURL)
		go ts.startLiveStatusUpdater(job.Ctx, entities, update, msg)
		return nil
	}

	if msg.ReplyTo == nil {
		_, err := ts.sender.Reply(entities, update).Text(ctx, "Usage: /mirror <url> OR reply to a media message with /mirror [-i count] to upload to Google Drive.")
		return err
	}

	replyHeader, ok := msg.ReplyTo.(*tg.MessageReplyHeader)
	if !ok || replyHeader.ReplyToMsgID == 0 {
		_, err := ts.sender.Reply(entities, update).Text(ctx, "Could not find replied message ID.")
		return err
	}

	count := 1
	for i, part := range parts {
		if part == "-i" && i+1 < len(parts) {
			if parsed, err := strconv.Atoi(parts[i+1]); err == nil && parsed > 0 {
				count = parsed
			}
		}
	}

	startMsgID := replyHeader.ReplyToMsgID
	queuedJobs := 0

	for offset := 0; offset < count; offset++ {
		targetID := startMsgID + offset

		res, err := ts.client.API().MessagesGetMessages(ctx, []tg.InputMessageClass{
			&tg.InputMessageID{ID: targetID},
		})
		if err != nil {
			slog.Error("failed fetching message for batch mirror", "msg_id", targetID, "error", err)
			continue
		}

		var messagesSlice []tg.MessageClass
		switch m := res.(type) {
		case *tg.MessagesMessages:
			messagesSlice = m.Messages
		case *tg.MessagesMessagesSlice:
			messagesSlice = m.Messages
		case *tg.MessagesChannelMessages:
			messagesSlice = m.Messages
		}

		if len(messagesSlice) == 0 {
			continue
		}

		targetMsg, ok := messagesSlice[0].(*tg.Message)
		if !ok || targetMsg.Media == nil {
			continue
		}

		fileName, fileSize, location, err := extractMediaInfo(targetMsg.Media)
		if err != nil {
			continue
		}

		var jobRef *Job
		execFunc := func() {
			ts.executeMirrorJob(jobRef, location, entities, update)
		}

		job, err := ts.jm.CreateJob(ctx, JobTypeMirror, fileName, fileSize, userID, execFunc)
		if err != nil {
			slog.Warn("could not create mirror job", "msg_id", targetID, "error", err)
			continue
		}
		jobRef = job

		queuedJobs++
	}

	if queuedJobs == 0 {
		_, err := ts.sender.Reply(entities, update).Text(ctx, "No downloadable media files found in the requested range.")
		return err
	}

	// Start a single live status updater for the entire batch
	statusCtx, statusCancel := context.WithCancel(ctx)
	go func() {
		for {
			time.Sleep(2 * time.Second)
			if ts.jm.GetActiveJobCount() == 0 {
				statusCancel()
				return
			}
		}
	}()
	go ts.startLiveStatusUpdater(statusCtx, entities, update, msg)

	return nil
}

func extractMediaInfo(media tg.MessageMediaClass) (string, int64, tg.InputFileLocationClass, error) {
	switch m := media.(type) {
	case *tg.MessageMediaDocument:
		doc, ok := m.Document.AsNotEmpty()
		if !ok {
			return "", 0, nil, fmt.Errorf("empty document")
		}
		name := "document.bin"
		for _, attr := range doc.Attributes {
			if filenameAttr, ok := attr.(*tg.DocumentAttributeFilename); ok {
				name = filenameAttr.FileName
				break
			}
		}
		loc := &tg.InputDocumentFileLocation{
			ID:            doc.ID,
			AccessHash:    doc.AccessHash,
			FileReference: doc.FileReference,
		}
		return name, doc.Size, loc, nil
	case *tg.MessageMediaPhoto:
		photo, ok := m.Photo.AsNotEmpty()
		if !ok {
			return "", 0, nil, fmt.Errorf("empty photo")
		}
		loc := &tg.InputPhotoFileLocation{
			ID:            photo.ID,
			AccessHash:    photo.AccessHash,
			FileReference: photo.FileReference,
			ThumbSize:     "x",
		}
		return "photo.jpg", 0, loc, nil
	default:
		return "", 0, nil, fmt.Errorf("unsupported media type")
	}
}

func (ts *TelegramService) executeURLMirrorJob(job *Job, rawURL string, entities tg.Entities, update *tg.UpdateNewMessage) {
	defer ts.jm.FinishJob(job.ID)

	slog.Info("executing URL mirror job", "job_id", job.ID, "url", rawURL, "mode", ts.cfg.DownloadMode)
	job.Phase = PhaseDownloading
	job.Status = "Downloading from HTTP URL"
	job.FileName = extractFileNameFromURL(rawURL)

	var driveURL string
	var err error

	if ts.cfg.DownloadMode == "parallel" {
		driveURL, err = ts.executeURLMirrorParallel(job, rawURL)
	} else {
		driveURL, err = ts.executeURLMirrorStreamFallback(job, rawURL)
	}
	if err != nil {
		slog.Error("gdrive upload failed for URL mirror", "job_id", job.ID, "error", err)
		job.Status = fmt.Sprintf("Failed: %v", err)
		return
	}

	job.Status = "Completed"
	slog.Info("URL mirror job completed", "job_id", job.ID, "drive_url", driveURL)

	baseURL := ts.cfg.IndexBaseURL
	if baseURL != "" && !strings.HasSuffix(baseURL, "/") {
		baseURL += "/"
	}
	indexURL := baseURL + url.PathEscape(job.FileName)

	completionOpts := []styling.StyledTextOption{
		styling.Bold("✅ Mirror Complete!\n\n"),
		styling.Bold("File:"), styling.Plain(" "), styling.Code(job.FileName), styling.Plain("\n"),
		styling.Bold("Google Drive Link:"), styling.Plain(fmt.Sprintf(" %s\n", driveURL)),
	}
	if ts.cfg.IndexBaseURL != "" {
		completionOpts = append(completionOpts, styling.Bold("Index Link:"), styling.Plain(fmt.Sprintf(" %s", indexURL)))
	}

	_, _ = ts.sender.Reply(entities, update).StyledText(context.Background(), completionOpts...)
}

func (ts *TelegramService) executeMirrorJob(job *Job, location tg.InputFileLocationClass, entities tg.Entities, update *tg.UpdateNewMessage) {

	slog.Info("executing mirror job", "job_id", job.ID, "file_name", job.FileName, "file_size", job.Size, "mode", ts.cfg.DownloadMode)
	job.Phase = PhaseDownloading
	job.Status = "Streaming from Telegram to Google Drive"

	var driveURL string
	var err error

	if ts.cfg.DownloadMode == "parallel" {
		driveURL, err = ts.executeMirrorParallel(job, location)
	} else {
		driveURL, err = ts.executeMirrorStream(job, location)
	}

	if err != nil {
		slog.Error("mirror job failed", "job_id", job.ID, "error", err)
		job.Status = fmt.Sprintf("Failed: %v", err)
		return
	}

	job.Status = "Completed"
	slog.Info("mirror job completed", "job_id", job.ID, "drive_url", driveURL)

	baseURL := ts.cfg.IndexBaseURL
	if baseURL != "" && !strings.HasSuffix(baseURL, "/") {
		baseURL += "/"
	}
	indexURL := baseURL + url.PathEscape(job.FileName)

	completionOpts := []styling.StyledTextOption{
		styling.Bold("✅ Mirror Complete!\n\n"),
		styling.Bold("File:"), styling.Plain(" "), styling.Code(job.FileName), styling.Plain("\n"),
		styling.Bold("Google Drive Link:"), styling.Plain(fmt.Sprintf(" %s\n", driveURL)),
	}
	if ts.cfg.IndexBaseURL != "" {
		completionOpts = append(completionOpts, styling.Bold("Index Link:"), styling.Plain(fmt.Sprintf(" %s", indexURL)))
	}

	_, _ = ts.sender.Reply(entities, update).StyledText(context.Background(), completionOpts...)
}

// executeMirrorStream: zero-disk pipe streaming (download + upload simultaneously)
func (ts *TelegramService) executeMirrorStream(job *Job, location tg.InputFileLocationClass) (string, error) {
	pr, pw := io.Pipe()

	progressWriter := NewProgressWriter(pw, job.Size, func(read, total int64, speed float64, eta time.Duration) {
		job.ReadBytes = read
		job.Speed = speed
		job.ETA = eta
	})

	go func() {
		defer pw.Close()
		slog.Info("starting telegram media stream download", "job_id", job.ID)
		written, err := ts.downloader.Download(ts.client.API(), location).Stream(job.Ctx, progressWriter)
		if err != nil {
			slog.Error("telegram media download stream error", "job_id", job.ID, "error", err)
			pw.CloseWithError(err)
		} else {
			slog.Info("telegram media download stream finished", "job_id", job.ID, "bytes_written", written)
			job.Phase = PhaseUploading
			job.Status = "Uploading to Google Drive"
		}
	}()

	return ts.gdrive.UploadStream(job.Ctx, job.FileName, pr, job.Size)
}

// atomicWriteAt wraps io.WriterAt with a single atomic counter for progress tracking.
// Minimal overhead — no callbacks, no time checks per write.
type atomicWriteAt struct {
	file    *os.File
	written int64
}

func (a *atomicWriteAt) WriteAt(p []byte, off int64) (int, error) {
	n, err := a.file.WriteAt(p, off)
	atomic.AddInt64(&a.written, int64(n))
	return n, err
}

func (a *atomicWriteAt) Write(p []byte) (int, error) {
	n, err := a.file.Write(p)
	atomic.AddInt64(&a.written, int64(n))
	return n, err
}

// progressWriterAt wraps an io.WriterAt to track total bytes written (for parallel downloads)
// Uses atomic ops instead of mutex to avoid contention across threads.
type progressWriterAt struct {
	dest       io.WriterAt
	totalBytes int64
	written    int64
	startTime  time.Time
	onProgress func(read, total int64, speed float64, eta time.Duration)
	lastNotify int64 // unix nano, atomic
}

func newProgressWriterAt(dest io.WriterAt, totalBytes int64, onProgress func(read, total int64, speed float64, eta time.Duration)) *progressWriterAt {
	return &progressWriterAt{
		dest:       dest,
		totalBytes: totalBytes,
		startTime:  time.Now(),
		onProgress: onProgress,
	}
}

func (pw *progressWriterAt) WriteAt(p []byte, off int64) (int, error) {
	n, err := pw.dest.WriteAt(p, off)
	if n > 0 {
		written := atomic.AddInt64(&pw.written, int64(n))
		now := time.Now().UnixNano()
		last := atomic.LoadInt64(&pw.lastNotify)
		if now-last >= int64(1*time.Second) && atomic.CompareAndSwapInt64(&pw.lastNotify, last, now) {
			elapsed := now - pw.startTime.UnixNano()
			if elapsed > 0 && pw.onProgress != nil {
				speed := float64(written) / (float64(elapsed) / float64(time.Second))
				var eta time.Duration
				if speed > 0 && pw.totalBytes > written {
					eta = time.Duration(float64(pw.totalBytes-written)/speed) * time.Second
				}
				pw.onProgress(written, pw.totalBytes, speed, eta)
			}
		}
	}
	return n, err
}

// executeMirrorParallel: multi-threaded download to temp file, then stream to GDrive
func (ts *TelegramService) executeMirrorParallel(job *Job, location tg.InputFileLocationClass) (string, error) {
	tmpFile, err := os.CreateTemp("", "zenith-dl-*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	threads := ts.cfg.DownloadThreads
	slog.Info("starting parallel telegram download", "job_id", job.ID, "threads", threads, "tmp", tmpFile.Name())

	// Wrap temp file with byte counter for progress tracking
	counter := &atomicWriteAt{file: tmpFile}

	// Progress tracking goroutine — reads counter once per second
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		start := time.Now()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				written := atomic.LoadInt64(&counter.written)
				elapsed := time.Since(start).Seconds()
				if elapsed > 0 {
					job.ReadBytes = written
					job.Speed = float64(written) / elapsed
					if job.Size > 0 {
						remaining := float64(job.Size-written) / job.Speed
						job.ETA = time.Duration(remaining * float64(time.Second))
					}
				}
			}
		}
	}()

	// Try parallel first, fall back to stream if threads=1
	if threads > 1 {
		_, err = ts.downloader.Download(ts.client.API(), location).
			WithThreads(threads).
			WithVerify(false).
			Parallel(job.Ctx, counter)
	} else {
		_, err = ts.downloader.Download(ts.client.API(), location).
			WithVerify(false).
			Stream(job.Ctx, counter)
	}

	close(done)

	if err != nil {
		return "", fmt.Errorf("parallel download failed: %w", err)
	}

	downloadedSize, _ := tmpFile.Seek(0, io.SeekEnd)
	slog.Info("parallel download finished", "job_id", job.ID, "bytes", downloadedSize)

	// Seek back to start for upload
	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("failed to seek temp file: %w", err)
	}

	job.ReadBytes = 0
	job.Speed = 0
	job.ETA = 0
	job.Phase = PhaseUploading
	job.Status = "Uploading to Google Drive"

	// Stream temp file into GDrive with progress tracking
	progressReader := NewProgressReader(tmpFile, downloadedSize, func(read, total int64, speed float64, eta time.Duration) {
		job.ReadBytes = read
		job.Speed = speed
		job.ETA = eta
	})

	return ts.gdrive.UploadStream(job.Ctx, job.FileName, progressReader, downloadedSize)
}

func (ts *TelegramService) handleLeech(ctx context.Context, entities tg.Entities, update *tg.UpdateNewMessage, msg *tg.Message, text string, userID int64) error {
	parts := strings.Fields(text)
	if len(parts) < 2 {
		_, err := ts.sender.Reply(entities, update).Text(ctx, "Usage: /leech <url>")
		return err
	}
	rawURL := parts[1]

	var jobRef *Job
	execFunc := func() {
		ts.executeLeechJob(jobRef, rawURL, entities, update)
	}

	job, err := ts.jm.CreateJob(ctx, JobTypeLeech, rawURL, 0, userID, execFunc)
	if err != nil {
		slog.Error("failed creating leech job", "error", err)
		_, replyErr := ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("Error creating job: %v", err))
		return replyErr
	}
	jobRef = job

	slog.Info("leech job created", "job_id", job.ID, "url", rawURL)

	go ts.startLiveStatusUpdater(job.Ctx, entities, update, msg)

	return nil
}

func (ts *TelegramService) executeLeechJob(job *Job, rawURL string, entities tg.Entities, update *tg.UpdateNewMessage) {
	defer ts.jm.FinishJob(job.ID)

	slog.Info("executing leech job", "job_id", job.ID, "url", rawURL)
	job.Phase = PhaseDownloading
	job.Status = "Downloading from HTTP"

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

	pr := NewProgressReader(resp.Body, job.Size, func(read, total int64, speed float64, eta time.Duration) {
		job.ReadBytes = read
		job.Speed = speed
		job.ETA = eta
	})

	slog.Info("uploading stream to telegram", "job_id", job.ID, "size", FormatBytes(job.Size))
	job.Phase = PhaseUploading
	job.Status = "Uploading to Telegram"

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
