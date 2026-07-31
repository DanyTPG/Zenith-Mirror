package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/message"
	"github.com/gotd/td/telegram/message/styling"
	"github.com/gotd/td/tg"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"
)

type TelegramService struct {
	client       *telegram.Client
	sender       *message.Sender
	gdrive       *GDriveService
	downloader   *LeechPipeline
	jm           *JobManager
	cfg          *Config
	startTime    time.Time
	lastStatusID int
	lastStatusFn context.CancelFunc
	lastStatusMu sync.Mutex
}

func NewTelegramService(client *telegram.Client, gdrive *GDriveService, jm *JobManager, cfg *Config) *TelegramService {
	ts := &TelegramService{
		client:    client,
		sender:    message.NewSender(client.API()),
		gdrive:    gdrive,
		jm:        jm,
		cfg:       cfg,
		startTime: time.Now(),
	}
	ts.downloader = NewLeechPipeline(ts)
	return ts
}

func (ts *TelegramService) RegisterHandlers(dispatcher tg.UpdateDispatcher) {
	dispatcher.OnNewMessage(func(ctx context.Context, entities tg.Entities, update *tg.UpdateNewMessage) error {
		msg, ok := update.Message.(*tg.Message)
		if !ok || msg.Out {
			return nil
		}

		text := strings.TrimSpace(msg.Message)
		if text == "" {
			return nil
		}

		userID := ts.getUserID(msg)
		if !ts.isAuthorized(userID) {
			slog.Warn("unauthorized user access attempt", "user_id", userID, "text", text)
			_, err := ts.sender.Reply(entities, update).Text(ctx, "Unauthorized user.")
			return err
		}

		if strings.HasPrefix(text, "/start") {
			_, err := ts.sender.Reply(entities, update).Text(ctx, "Welcome to Zenith Mirror! Send /help for commands.")
			return err
		}

		if strings.HasPrefix(text, "/help") {
			helpText := "Available commands:\n" +
				"/mirror <url> OR reply to media with /mirror [-i count] - Mirror file to Google Drive\n" +
				"/leech <url> - Leech direct link to Telegram\n" +
				"/status - View active transfer jobs\n" +
				"/cancel <id> - Cancel an active job\n" +
				"/stats - View system performance and resource usage\n" +
				"/help - View this message"
			_, err := ts.sender.Reply(entities, update).Text(ctx, helpText)
			return err
		}

		if strings.HasPrefix(text, "/stats") {
			return ts.handleStats(ctx, entities, update)
		}

		if strings.HasPrefix(text, "/status") {
			return ts.handleStatus(ctx, entities, update)
		}

		if strings.HasPrefix(text, "/cancel") {
			return ts.handleCancel(ctx, entities, update, text)
		}

		if strings.HasPrefix(text, "/mirror") {
			return ts.handleMirror(ctx, entities, update, msg, text, userID)
		}

		if strings.HasPrefix(text, "/leech") {
			return ts.handleLeech(ctx, entities, update, msg, text, userID)
		}

		return nil
	})
}

func (ts *TelegramService) getUserID(msg *tg.Message) int64 {
	if msg.FromID != nil {
		if peerUser, ok := msg.FromID.(*tg.PeerUser); ok {
			return peerUser.UserID
		}
	}
	return 0
}

func (ts *TelegramService) isAuthorized(userID int64) bool {
	if len(ts.cfg.AuthorizedUsers) == 0 {
		return true
	}
	for _, id := range ts.cfg.AuthorizedUsers {
		if id == userID {
			return true
		}
	}
	return false
}

func (ts *TelegramService) handleCancel(ctx context.Context, entities tg.Entities, update *tg.UpdateNewMessage, text string) error {
	parts := strings.Fields(text)
	if len(parts) < 2 {
		_, err := ts.sender.Reply(entities, update).Text(ctx, "Usage: /cancel <job_id>")
		return err
	}
	jobID := parts[1]
	if ts.jm.CancelJob(jobID) {
		_, err := ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("Job %s cancelled.", jobID))
		return err
	}
	_, err := ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("Job %s not found or already finished.", jobID))
	return err
}

func (ts *TelegramService) handleStats(ctx context.Context, entities tg.Entities, update *tg.UpdateNewMessage) error {
	botUptime := formatDuration(time.Since(ts.startTime))

	osUptimeStr := "N/A"
	if uptime, err := host.Uptime(); err == nil {
		osUptimeStr = formatDuration(time.Duration(uptime) * time.Second)
	}

	totalDisk, usedDisk, freeDisk := "N/A", "N/A", "N/A"
	diskPercent := 0.0
	if usage, err := disk.Usage("/"); err == nil {
		totalDisk = FormatBytes(int64(usage.Total))
		usedDisk = FormatBytes(int64(usage.Used))
		freeDisk = FormatBytes(int64(usage.Free))
		diskPercent = usage.UsedPercent
	}

	netSent, netRecv := "N/A", "N/A"
	if counters, err := net.IOCounters(false); err == nil && len(counters) > 0 {
		netSent = FormatBytes(int64(counters[0].BytesSent))
		netRecv = FormatBytes(int64(counters[0].BytesRecv))
	}

	cpuPercent := 0.0
	if percents, err := cpu.Percent(0, false); err == nil && len(percents) > 0 {
		cpuPercent = percents[0]
	}

	physCores, _ := cpu.Counts(false)
	totalCores, _ := cpu.Counts(true)

	memTotal, memFree, memUsed := "N/A", "N/A", "N/A"
	memPercent := 0.0
	swapTotal, swapUsed := "N/A", "N/A"
	swapPercent := 0.0

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

func (ts *TelegramService) handleStatus(ctx context.Context, entities tg.Entities, update *tg.UpdateNewMessage) error {
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

	memPercent := 0.0
	if v, err := mem.VirtualMemory(); err == nil {
		memPercent = v.UsedPercent
	}

	diskPercent := 0.0
	if usage, err := disk.Usage("/"); err == nil {
		diskPercent = usage.UsedPercent
	}

	options = append(options, styling.Bold("CPU:"), styling.Plain(fmt.Sprintf(" %.1f%% | ", cpuPercent)))
	options = append(options, styling.Bold("RAM:"), styling.Plain(fmt.Sprintf(" %.1f%% | ", memPercent)))
	options = append(options, styling.Bold("DISK:"), styling.Plain(fmt.Sprintf(" %.1f%%", diskPercent)))

	return options
}

func (ts *TelegramService) startLiveStatusUpdater(ctx context.Context, entities tg.Entities, update *tg.UpdateNewMessage, userMsg *tg.Message) {
	ts.deleteLastStatus()

	opts := ts.buildStatusStyledText()
	updates, err := ts.sender.Reply(entities, update).StyledText(ctx, opts...)
	if err != nil {
		slog.Error("failed sending initial live status message", "error", err)
		return
	}

	msgID := extractMsgIDFromUpdates(updates)
	if msgID <= 0 {
		slog.Warn("could not extract message ID for live status update")
		return
	}

	statusCtx, cancel := context.WithCancel(ctx)
	ts.setLastStatus(msgID, cancel)

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	channelID, accessHash := extractPeerChannelInfo(userMsg.PeerID)

	for {
		select {
		case <-statusCtx.Done():
			ts.clearLastStatusIf(msgID)
			return
		case <-ticker.C:
			activeJobs := ts.jm.GetActiveJobs()
			if len(activeJobs) == 0 {
				ts.deleteLastStatus()
				return
			}

			newOpts := ts.buildStatusStyledText()
			peer := ts.buildInputPeer(userMsg.PeerID, channelID, accessHash)

			_, editErr := ts.sender.To(peer).Edit(msgID).StyledText(statusCtx, newOpts...)
			if editErr != nil {
				slog.Error("failed updating status message", "msg_id", msgID, "error", editErr)
			}
		}
	}
}

func (ts *TelegramService) buildInputPeer(peer tg.PeerClass, channelID int64, accessHash int64) tg.InputPeerClass {
	switch p := peer.(type) {
	case *tg.PeerUser:
		return &tg.InputPeerUser{UserID: p.UserID}
	case *tg.PeerChat:
		return &tg.InputPeerChat{ChatID: p.ChatID}
	case *tg.PeerChannel:
		return &tg.InputPeerChannel{ChannelID: p.ChannelID, AccessHash: accessHash}
	default:
		return &tg.InputPeerSelf{}
	}
}

func extractPeerChannelInfo(peer tg.PeerClass) (int64, int64) {
	if p, ok := peer.(*tg.PeerChannel); ok {
		return p.ChannelID, 0
	}
	return 0, 0
}

func extractMsgIDFromUpdates(updates tg.UpdatesClass) int {
	switch u := updates.(type) {
	case *tg.Updates:
		for _, update := range u.Updates {
			if newMsg, ok := update.(*tg.UpdateNewMessage); ok {
				if msg, ok := newMsg.Message.(*tg.Message); ok {
					return msg.ID
				}
			}
		}
	case *tg.UpdateShortSentMessage:
		return u.ID
	}
	return 0
}

func (ts *TelegramService) handleMirror(ctx context.Context, entities tg.Entities, update *tg.UpdateNewMessage, msg *tg.Message, text string, userID int64) error {
	parts := strings.Fields(text)

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

type progressWriterAt struct {
	dest       io.WriterAt
	totalBytes int64
	written    int64
	startTime  time.Time
	onProgress func(read, total int64, speed float64, eta time.Duration)
	lastNotify int64
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

func (ts *TelegramService) executeMirrorParallel(job *Job, location tg.InputFileLocationClass) (string, error) {
	tmpFile, err := os.CreateTemp("", "zenith-dl-*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	threads := ts.cfg.DownloadThreads
	slog.Info("starting parallel telegram download", "job_id", job.ID, "threads", threads, "tmp", tmpFile.Name())

	ctx, cancel := context.WithCancel(job.Ctx)
	defer cancel()

	go rawDownloadProgress(ctx, tmpFile, job, job.Size)

	invoker, poolCloser, err := createDownloadPool(ctx, ts.client, location, int64(threads))
	if err != nil {
		cancel()
		return "", fmt.Errorf("create download pool: %w", err)
	}
	defer poolCloser.Close()

	api := tg.NewClient(invoker)
	err = rawParallelDownload(ctx, api, location, job.Size, threads, tmpFile)

	cancel()

	if err != nil {
		return "", fmt.Errorf("parallel download failed: %w", err)
	}

	downloadedSize, _ := tmpFile.Seek(0, io.SeekEnd)
	slog.Info("parallel download finished", "job_id", job.ID, "bytes", downloadedSize)

	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("failed to seek temp file: %w", err)
	}

	job.ReadBytes = 0
	job.Speed = 0
	job.ETA = 0
	job.Phase = PhaseUploading
	job.Status = "Uploading to Google Drive"

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
	job.Status = "Downloading HTTP link"

	body, contentLength, fileName, err := ts.downloader.DownloadHTTP(job.Ctx, rawURL, nil)
	if err != nil {
		slog.Error("leech download failed", "job_id", job.ID, "error", err)
		job.Status = fmt.Sprintf("Failed: %v", err)
		return
	}
	defer body.Close()

	job.FileName = fileName
	job.Size = contentLength

	progressReader := NewProgressReader(body, contentLength, func(read, total int64, speed float64, eta time.Duration) {
		job.ReadBytes = read
		job.Speed = speed
		job.ETA = eta
	})

	uploader := ts.client.API()
	job.Phase = PhaseUploading
	job.Status = "Uploading to Telegram"

	var uploadErr error
	if ts.cfg.DownloadMode == "parallel" && contentLength > 0 {
		_, uploadErr = ts.executeLeechParallel(job, uploader, progressReader, fileName, contentLength)
	} else {
		_, uploadErr = ts.executeLeechStream(job, uploader, progressReader, fileName, contentLength)
	}

	if uploadErr != nil {
		slog.Error("leech upload failed", "job_id", job.ID, "error", uploadErr)
		job.Status = fmt.Sprintf("Failed: %v", uploadErr)
		return
	}

	job.Status = "Completed"
	slog.Info("leech job completed", "job_id", job.ID)
}

func (ts *TelegramService) executeLeechStream(job *Job, api *tg.Client, reader io.Reader, fileName string, size int64) (tg.InputFileClass, error) {
	u := telegram.NewUploader(api)
	return u.FromReader(job.Ctx, fileName, reader)
}

func (ts *TelegramService) executeLeechParallel(job *Job, api *tg.Client, reader io.Reader, fileName string, size int64) (tg.InputFileClass, error) {
	u := telegram.NewUploader(api).WithThreads(ts.cfg.DownloadThreads)
	return u.FromReader(job.Ctx, fileName, reader)
}

func (ts *TelegramService) executeURLMirrorStreamFallback(job *Job, rawURL string) (string, error) {
	body, contentLength, fileName, err := ts.downloader.DownloadHTTP(job.Ctx, rawURL, nil)
	if err != nil {
		return "", err
	}
	defer body.Close()

	if job.FileName == "" || job.FileName == "downloaded_file.bin" {
		job.FileName = fileName
	}
	job.Size = contentLength

	progressReader := NewProgressReader(body, contentLength, func(read, total int64, speed float64, eta time.Duration) {
		job.ReadBytes = read
		job.Speed = speed
		job.ETA = eta
	})

	return ts.gdrive.UploadStream(job.Ctx, job.FileName, progressReader, contentLength)
}

func (ts *TelegramService) executeURLMirrorParallel(job *Job, rawURL string) (string, error) {
	return ts.executeURLMirrorStreamFallback(job, rawURL)
}

func extractFileNameFromURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "downloaded_file.bin"
	}
	parts := strings.Split(u.Path, "/")
	if len(parts) > 0 && parts[len(parts)-1] != "" {
		return parts[len(parts)-1]
	}
	return "downloaded_file.bin"
}
