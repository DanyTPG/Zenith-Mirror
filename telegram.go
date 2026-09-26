package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/message"
	"github.com/gotd/td/telegram/message/markup"
	"github.com/gotd/td/telegram/message/styling"
	"github.com/gotd/td/telegram/uploader"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"
)

type TelegramService struct {
	client         *telegram.Client
	sender         *message.Sender
	gdrive         *GDriveService
	torrentSvc     *TorrentService
	downloader     *LeechPipeline
	jm             *JobManager
	dm             *DownloadManager
	cfg            *Config
	startTime      time.Time
	lastStatusID   int
	lastStatusPeer tg.InputPeerClass
	lastStatusFn   context.CancelFunc
	lastStatusMu   sync.Mutex
	// Pool cache: one pool per remote DC, shared across jobs
	poolMu    sync.Mutex
	poolCache map[int]struct {
		invoker tg.Invoker
		closer  io.Closer
	}
	feedMgr *FeedManager
}

func NewTelegramService(client *telegram.Client, gdrive *GDriveService, jm *JobManager, cfg *Config) *TelegramService {
	ts := &TelegramService{
		client:    client,
		sender:    message.NewSender(client.API()),
		gdrive:    gdrive,
		jm:        jm,
		dm:        NewDownloadManager(int64(cfg.MaxConcurrentDownloads)),
		cfg:       cfg,
		startTime: time.Now(),
		poolCache: make(map[int]struct {
			invoker tg.Invoker
			closer  io.Closer
		}),
	}
	ts.feedMgr = NewFeedManager(cfg.FeedStateFile, ts)
	ts.downloader = NewLeechPipeline(ts)
	return ts
}

func (ts *TelegramService) SetTorrentService(svc *TorrentService) {
	ts.torrentSvc = svc
}

func (ts *TelegramService) RegisterHandlers(dispatcher tg.UpdateDispatcher) {
	dispatcher.OnNewMessage(func(ctx context.Context, entities tg.Entities, update *tg.UpdateNewMessage) error {
		msg, ok := update.Message.(*tg.Message)
		if !ok || msg.Out {
			return nil
		}
		return ts.handleIncomingMessage(ctx, entities, update, msg)
	})

	dispatcher.OnNewChannelMessage(func(ctx context.Context, entities tg.Entities, update *tg.UpdateNewChannelMessage) error {
		msg, ok := update.Message.(*tg.Message)
		if !ok || msg.Out {
			return nil
		}
		return ts.handleIncomingMessage(ctx, entities, update, msg)
	})

	dispatcher.OnBotCallbackQuery(func(ctx context.Context, entities tg.Entities, update *tg.UpdateBotCallbackQuery) error {
		return ts.handleBotCallbackQuery(ctx, entities, update)
	})
}

func (ts *TelegramService) handleIncomingMessage(ctx context.Context, entities tg.Entities, update message.AnswerableMessageUpdate, msg *tg.Message) error {
	text := strings.TrimSpace(msg.Message)
	if text == "" {
		return nil
	}

	userID := ts.getUserID(msg)
	authorized, isDM := ts.checkAuthorization(msg)
	if !authorized {
		if isDM {
			slog.Warn("unauthorized DM access attempt", "user_id", userID, "text", text)
			_, err := ts.sender.Reply(entities, update).Text(ctx, "You are not authorized to use this bot.")
			return err
		}
		slog.Debug("ignoring message from unauthorized group/channel", "peer", msg.PeerID, "user_id", userID)
		return nil
	}

	if strings.HasPrefix(text, "/start") {
		_, err := ts.sender.Reply(entities, update).Text(ctx, "Welcome to Zenith Mirror! Send /help for commands.")
		return err
	}

	if strings.HasPrefix(text, "/help") {
		helpText := "Available commands:\n" +
			"/mirror <url> OR reply to media with /mirror [-i count] - Mirror file to Google Drive\n" +
			"/mirror magnet:?xt=... OR reply to .torrent with /mirror - Torrent to Drive\n" +
			"/leech <url> OR magnet:?xt=... - Leech to Telegram\n" +
			"/feed - Manage RSS/Atom release feeds\n" +
			"/feed add [mirror|leech|notify] [NAME] [URL] [+include] [-exclude]\n" +
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

	if strings.HasPrefix(text, "/log") {
		return ts.handleLog(ctx, entities, update)
	}

	if strings.HasPrefix(text, "/status") {
		return ts.handleStatus(ctx, entities, update, msg)
	}

	if strings.HasPrefix(text, "/cancelall") {
		return ts.handleCancelAll(ctx, entities, update, msg)
	}

	if strings.HasPrefix(text, "/cancel") {
		return ts.handleCancel(ctx, entities, update, msg, text)
	}

	if strings.HasPrefix(text, "/mirror") || strings.HasPrefix(text, "/m ") || text == "/m" {
		return ts.handleMirror(ctx, entities, update, msg, text, userID)
	}

	if strings.HasPrefix(text, "/leech") {
		return ts.handleLeech(ctx, entities, update, msg, text, userID)
	}

	if strings.HasPrefix(text, "/feed") {
		return ts.handleFeed(ctx, entities, update, msg, text, userID)
	}

	return nil
}

func (ts *TelegramService) getUserID(msg *tg.Message) int64 {
	if msg.FromID != nil {
		if peerUser, ok := msg.FromID.(*tg.PeerUser); ok {
			return peerUser.UserID
		}
	}
	if msg.PeerID != nil {
		if peerUser, ok := msg.PeerID.(*tg.PeerUser); ok {
			return peerUser.UserID
		}
	}
	return 0
}

func (ts *TelegramService) checkAuthorization(msg *tg.Message) (bool, bool) {
	userID := ts.getUserID(msg)
	isOwner := ts.cfg.IsOwner(userID)

	switch p := msg.PeerID.(type) {
	case *tg.PeerUser:
		// Direct Message: user ID must match owner or be in allowed_chat_id
		isAllowed := isOwner || ts.cfg.IsChatAllowed(p.UserID) || (userID != 0 && ts.cfg.IsChatAllowed(userID))
		return isAllowed, true
	case *tg.PeerChat:
		// Group Chat: reply to anyone if group chat ID is allowed (or sender is owner)
		isAllowed := isOwner || ts.cfg.IsChatAllowed(p.ChatID)
		return isAllowed, false
	case *tg.PeerChannel:
		// Supergroup or channel: reply to anyone if channel ID is allowed (or sender is owner)
		isAllowed := isOwner || ts.cfg.IsChatAllowed(p.ChannelID)
		return isAllowed, false
	default:
		return isOwner, false
	}
}

func (ts *TelegramService) isAuthorized(userID int64) bool {
	return ts.cfg.IsAllowed(userID)
}

func (ts *TelegramService) handleCancel(ctx context.Context, entities tg.Entities, update message.AnswerableMessageUpdate, msg *tg.Message, text string) error {
	parts := strings.Fields(text)
	if len(parts) < 2 {
		_, err := ts.sender.Reply(entities, update).Text(ctx, "Usage: /cancel <job_id>")
		return err
	}
	jobID := parts[1]
	if ts.jm.CancelJob(jobID) {
		_, err := ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("Job %s cancelled.", jobID))
		ts.deleteLastStatus()
		if ts.jm.GetActiveJobCount() > 0 {
			go ts.startLiveStatusUpdater(ts.extractJobTarget(msg, entities))
		} else {
			opts := ts.buildStatusStyledText()
			updates, _ := ts.sender.Reply(entities, update).StyledText(context.Background(), opts...)
			channelID, accessHash := extractPeerChannelInfo(msg.PeerID, entities)
			peer := ts.buildInputPeer(msg.PeerID, channelID, accessHash)
			ts.setLastStatus(extractMsgIDFromUpdates(updates), peer, nil)
		}
		return err
	}
	_, err := ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("Job %s not found or already finished.", jobID))
	return err
}

func (ts *TelegramService) handleCancelAll(ctx context.Context, entities tg.Entities, update message.AnswerableMessageUpdate, msg *tg.Message) error {
	cancelled := ts.jm.CancelAllJobs()
	_, err := ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("Cancelled %d job(s).", cancelled))
	if cancelled > 0 {
		ts.deleteLastStatus()
		opts := ts.buildStatusStyledText()
		updates, _ := ts.sender.Reply(entities, update).StyledText(context.Background(), opts...)
		channelID, accessHash := extractPeerChannelInfo(msg.PeerID, entities)
		peer := ts.buildInputPeer(msg.PeerID, channelID, accessHash)
		ts.setLastStatus(extractMsgIDFromUpdates(updates), peer, nil)
	}
	return err
}

func (ts *TelegramService) handleStats(ctx context.Context, entities tg.Entities, update message.AnswerableMessageUpdate) error {
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
	peer := ts.lastStatusPeer
	fn := ts.lastStatusFn
	ts.lastStatusID = 0
	ts.lastStatusPeer = nil
	ts.lastStatusFn = nil
	ts.lastStatusMu.Unlock()
	if fn != nil {
		fn()
	}
	if id <= 0 {
		return
	}
	slog.Info("deleting previous status message", "status_msg_id", id)
	if ch, ok := peer.(*tg.InputPeerChannel); ok {
		_, err := ts.client.API().ChannelsDeleteMessages(context.Background(), &tg.ChannelsDeleteMessagesRequest{
			Channel: &tg.InputChannel{ChannelID: ch.ChannelID, AccessHash: ch.AccessHash},
			ID:      []int{id},
		})
		if err != nil {
			slog.Error("failed to delete previous channel status message", "status_msg_id", id, "error", err)
		}
		return
	}
	_, err := ts.client.API().MessagesDeleteMessages(context.Background(), &tg.MessagesDeleteMessagesRequest{
		Revoke: true,
		ID:     []int{id},
	})
	if err != nil {
		slog.Error("failed to delete previous status message", "status_msg_id", id, "error", err)
	}
}

func (ts *TelegramService) setLastStatus(id int, peer tg.InputPeerClass, cancelFn context.CancelFunc) {
	ts.lastStatusMu.Lock()
	ts.lastStatusID = id
	ts.lastStatusPeer = peer
	ts.lastStatusFn = cancelFn
	ts.lastStatusMu.Unlock()
}

func (ts *TelegramService) clearLastStatusIf(id int) {
	ts.lastStatusMu.Lock()
	if ts.lastStatusID == id {
		ts.lastStatusID = 0
		ts.lastStatusPeer = nil
		ts.lastStatusFn = nil
	}
	ts.lastStatusMu.Unlock()
}

// handleLog sends the last 10 lines of the log file in monospace.
func (ts *TelegramService) handleLog(ctx context.Context, entities tg.Entities, update message.AnswerableMessageUpdate) error {
	data, err := os.ReadFile(ts.cfg.LogFile)
	if err != nil {
		_, rErr := ts.sender.Reply(entities, update).Text(ctx, "no log file")
		return rErr
	}

	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) > 10 {
		lines = lines[len(lines)-10:]
	}

	var opts []styling.StyledTextOption
	for i, l := range lines {
		opts = append(opts, styling.Code(l))
		if i < len(lines)-1 {
			opts = append(opts, styling.Plain("\n"))
		}
	}
	_, err = ts.sender.Reply(entities, update).StyledText(ctx, opts...)
	return err
}

func (ts *TelegramService) handleStatus(ctx context.Context, entities tg.Entities, update message.AnswerableMessageUpdate, msg *tg.Message) error {
	channelID, accessHash := extractPeerChannelInfo(msg.PeerID, entities)
	peer := ts.buildInputPeer(msg.PeerID, channelID, accessHash)

	if ts.jm.GetActiveJobCount() == 0 {
		// No jobs — just send a static snapshot.
		ts.deleteLastStatus()
		opts := ts.buildStatusStyledText()
		updates, err := ts.sender.Reply(entities, update).StyledText(ctx, opts...)
		if err != nil {
			return err
		}
		ts.setLastStatus(extractMsgIDFromUpdates(updates), peer, nil)
		return nil
	}

	// Jobs active — run the same live updater the jobs use, so the status
	// keeps refreshing until all jobs finish, then deletes itself.
	ts.startLiveStatusUpdater(ts.extractJobTarget(msg, entities))
	return nil
}

func (ts *TelegramService) buildStatusStyledText() []styling.StyledTextOption {
	jobs := ts.jm.GetActiveJobs()
	if len(jobs) == 0 {
		return []styling.StyledTextOption{styling.Plain("No active transfer jobs.")}
	}

	var options []styling.StyledTextOption

	// Split into active (running) and queued to keep message well within 4096-char limit
	var running []*Job
	var queued []*Job
	for _, j := range jobs {
		if j.State == StateRunning {
			running = append(running, j)
		} else {
			queued = append(queued, j)
		}
	}

	// 1. Render all running jobs (active progress)
	for i, j := range running {
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
		options = append(options, styling.Plain(fmt.Sprintf(" %s", etaStr)))
		if j.IsTorrent {
			options = append(options, styling.Plain(fmt.Sprintf(" | Peers: %d Seeds: %d", j.Peers, j.Seeds)))
		}
		options = append(options, styling.Plain("\n"))
		options = append(options, styling.Code(fmt.Sprintf("/cancel %s", j.ID)))
		options = append(options, styling.Plain("\n\n"))
	}

	// 2. Render queued jobs up to a cap (max 4 shown), summarize the rest
	const maxQueuedShown = 4
	queuedShown := len(queued)
	if queuedShown > maxQueuedShown {
		queuedShown = maxQueuedShown
	}

	for i := 0; i < queuedShown; i++ {
		j := queued[i]
		idx := len(running) + i + 1
		options = append(options, styling.Bold(fmt.Sprintf("%d.Queued:", idx)))
		options = append(options, styling.Plain(" "))
		options = append(options, styling.Code(j.FileName))
		options = append(options, styling.Plain(fmt.Sprintf("\nSize: %s | Status: Waiting in Queue\n", FormatBytes(j.Size))))
		options = append(options, styling.Code(fmt.Sprintf("/cancel %s", j.ID)))
		options = append(options, styling.Plain("\n\n"))
	}

	if len(queued) > maxQueuedShown {
		remaining := len(queued) - maxQueuedShown
		options = append(options, styling.Italic(fmt.Sprintf("... and %d more queued jobs (total queue: %d)\n\n", remaining, len(queued))))
	}

	cpuPercent := 0.0
	if percents, err := cpu.Percent(0, false); err == nil && len(percents) > 0 {
		cpuPercent = percents[0]
	}

	memPercent := 0.0
	var memFree uint64
	if v, err := mem.VirtualMemory(); err == nil {
		memPercent = v.UsedPercent
		memFree = v.Free
	}

	diskPercent := 0.0
	_ = diskPercent
	if usage, err := disk.Usage("/"); err == nil {
		diskPercent = usage.UsedPercent
	}

	botUptime := formatDuration(time.Since(ts.startTime))
	osUptimeStr := botUptime
	if uptime, err := host.Uptime(); err == nil {
		osUptimeStr = formatDuration(time.Duration(uptime) * time.Second)
	}

	// Total DL/UL speeds
	var totalDL, totalUL float64
	for _, j := range jobs {
		if j.State == StateRunning {
			if j.Phase == PhaseUploading {
				totalUL += j.Speed
			} else {
				totalDL += j.Speed
			}
		}
	}

	options = append(options, styling.Plain("\n"))
	options = append(options, styling.Bold("Total DL:"))
	options = append(options, styling.Plain(fmt.Sprintf(" %s/s | ", FormatBytes(int64(totalDL)))))
	options = append(options, styling.Bold("Total UL:"))
	options = append(options, styling.Plain(fmt.Sprintf(" %s/s\n", FormatBytes(int64(totalUL)))))
	options = append(options, styling.Bold("CPU:"), styling.Plain(fmt.Sprintf(" %.1f%% | ", cpuPercent)))
	options = append(options, styling.Bold("FREE:"), styling.Plain(fmt.Sprintf(" %s\n", FormatBytes(int64(memFree)))))
	options = append(options, styling.Bold("RAM:"), styling.Plain(fmt.Sprintf(" %.1f%% | ", memPercent)))
	options = append(options, styling.Bold("UPTIME:"), styling.Plain(fmt.Sprintf(" %s", osUptimeStr)))

	return options
}

func (ts *TelegramService) startLiveStatusUpdater(target JobTarget) {
	ts.deleteLastStatus()

	peer := ts.inputPeerFromTarget(target)
	opts := ts.buildStatusStyledText()
	updates, err := ts.targetSender(target).StyledText(context.Background(), opts...)
	if err != nil {
		slog.Error("failed sending initial live status message", "error", err)
		return
	}

	msgID := extractMsgIDFromUpdates(updates)
	if msgID <= 0 {
		slog.Warn("could not extract message ID for live status update")
		return
	}

	statusCtx, cancel := context.WithCancel(context.Background())
	ts.setLastStatus(msgID, peer, cancel)

	statusDelay := time.Duration(ts.cfg.StatusRefreshDelay) * time.Second
	if statusDelay <= 0 {
		statusDelay = 3 * time.Second
	}
	ticker := time.NewTicker(statusDelay)
	defer ticker.Stop()

	var editAllowedAt time.Time

	for {
		select {
		case <-statusCtx.Done():
			return
		case <-ticker.C:
			activeJobs := ts.jm.GetActiveJobs()
			if len(activeJobs) == 0 {
				slog.Info("no active jobs, deleting status", "msg_id", msgID)
				ts.deleteLastStatus()
				return
			}

			// If a previous edit earned a long flood wait, skip edits entirely
			// until it elapses — re-requesting the banned method every 30s can
			// refresh/extend the ban server-side.
			if !editAllowedAt.IsZero() && time.Now().Before(editAllowedAt) {
				continue
			}

			newOpts := ts.buildStatusStyledText()

			_, editErr := ts.sender.To(peer).Edit(msgID).StyledText(statusCtx, newOpts...)
			if editErr != nil {
				// Back off on FLOOD_WAIT, but cap it — otherwise a big wait
				// (e.g. 8h) freezes the updater and it never notices job completion.
				if d, ok := tgerr.AsFloodWait(editErr); ok {
					statusDelay = d + time.Second
					if statusDelay > 30*time.Second {
						statusDelay = 30 * time.Second
					}
					// Long ban: park edits until the ban lapses.
					if d > 2*time.Minute {
						editAllowedAt = time.Now().Add(d + 5*time.Second)
						slog.Warn("long flood wait on status edit, pausing edits", "wait", d)
					}
					ticker.Reset(statusDelay)
				}
				slog.Error("failed updating status message", "msg_id", msgID, "error", editErr)
			} else {
				// Reset delay on success
				if statusDelay != time.Duration(ts.cfg.StatusRefreshDelay)*time.Second && ts.cfg.StatusRefreshDelay > 0 {
					statusDelay = time.Duration(ts.cfg.StatusRefreshDelay) * time.Second
					ticker.Reset(statusDelay)
				} else if ts.cfg.StatusRefreshDelay <= 0 && statusDelay != 3*time.Second {
					statusDelay = 3 * time.Second
					ticker.Reset(statusDelay)
				}
			}

			// Check after edit — job may have finished during edit
			if len(ts.jm.GetActiveJobs()) == 0 {
				slog.Info("no active jobs after edit, deleting status", "msg_id", msgID)
				ts.deleteLastStatus()
				return
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

func (ts *TelegramService) extractJobTarget(msg *tg.Message, entities tg.Entities) JobTarget {
	target := JobTarget{
		ReplyMsgID: msg.ID,
		UserID:     ts.getUserID(msg),
	}
	switch p := msg.PeerID.(type) {
	case *tg.PeerUser:
		target.PeerType = "user"
		target.UserID = p.UserID
	case *tg.PeerChat:
		target.PeerType = "chat"
		target.ChatID = p.ChatID
	case *tg.PeerChannel:
		target.PeerType = "channel"
		target.ChannelID = p.ChannelID
		if ch, ok := entities.Channels[p.ChannelID]; ok {
			target.AccessHash = ch.AccessHash
		}
	}
	return target
}

func (ts *TelegramService) inputPeerFromTarget(target JobTarget) tg.InputPeerClass {
	switch target.PeerType {
	case "user":
		return &tg.InputPeerUser{UserID: target.UserID}
	case "chat":
		return &tg.InputPeerChat{ChatID: target.ChatID}
	case "channel":
		return &tg.InputPeerChannel{ChannelID: target.ChannelID, AccessHash: target.AccessHash}
	default:
		return &tg.InputPeerSelf{}
	}
}

func (ts *TelegramService) targetSender(target JobTarget) *message.Builder {
	peer := ts.inputPeerFromTarget(target)
	builder := ts.sender.To(peer).CloneBuilder()
	if target.ReplyMsgID > 0 {
		builder = builder.Reply(target.ReplyMsgID)
	}
	return builder
}

func (ts *TelegramService) sendTargetFailure(target JobTarget, name string, action string, err error) {
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}
	if name == "" {
		name = "Job"
	}
	if action == "" {
		action = "Operation"
	}
	msg := fmt.Sprintf("❌ %s Failed: %s\nReason: %v", action, name, err)
	slog.Warn("notifying user of operation failure", "action", action, "name", name, "error", err)

	if target.PeerType != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, sendErr := ts.targetSender(target).Text(ctx, msg)
		if sendErr != nil {
			slog.Error("failed sending failure notification to user", "error", sendErr)
		}
	}
}

func (ts *TelegramService) sendJobFailure(job *Job, err error) {
	if job == nil || err == nil || errors.Is(err, context.Canceled) || job.Status == "Cancelled" || job.State == StateCancelled {
		return
	}
	name := job.FileName
	action := string(job.Type)
	ts.sendTargetFailure(job.Target, name, action, err)
}

func locationToStored(loc tg.InputFileLocationClass) *StoredLocation {
	if loc == nil {
		return nil
	}
	switch l := loc.(type) {
	case *tg.InputDocumentFileLocation:
		return &StoredLocation{
			Type:          "doc",
			ID:            l.ID,
			AccessHash:    l.AccessHash,
			FileReference: l.FileReference,
		}
	case *tg.InputPhotoFileLocation:
		return &StoredLocation{
			Type:          "photo",
			ID:            l.ID,
			AccessHash:    l.AccessHash,
			FileReference: l.FileReference,
			ThumbSize:     l.ThumbSize,
		}
	}
	return nil
}

func storedToLocation(s *StoredLocation) tg.InputFileLocationClass {
	if s == nil {
		return nil
	}
	switch s.Type {
	case "doc":
		return &tg.InputDocumentFileLocation{
			ID:            s.ID,
			AccessHash:    s.AccessHash,
			FileReference: s.FileReference,
		}
	case "photo":
		return &tg.InputPhotoFileLocation{
			ID:            s.ID,
			AccessHash:    s.AccessHash,
			FileReference: s.FileReference,
			ThumbSize:     s.ThumbSize,
		}
	}
	return nil
}

func extractPeerChannelInfo(peer tg.PeerClass, entities tg.Entities) (int64, int64) {
	if p, ok := peer.(*tg.PeerChannel); ok {
		var accessHash int64
		if ch, ok := entities.Channels[p.ChannelID]; ok {
			accessHash = ch.AccessHash
		}
		return p.ChannelID, accessHash
	}
	return 0, 0
}

func extractMsgIDFromUpdates(updates tg.UpdatesClass) int {
	switch u := updates.(type) {
	case *tg.Updates:
		for _, update := range u.Updates {
			switch newMsg := update.(type) {
			case *tg.UpdateNewMessage:
				if msg, ok := newMsg.Message.(*tg.Message); ok {
					return msg.ID
				}
			case *tg.UpdateNewChannelMessage:
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

func (ts *TelegramService) handleMirror(ctx context.Context, entities tg.Entities, update message.AnswerableMessageUpdate, msg *tg.Message, text string, userID int64) error {
	// Torrent via magnet in command text?
	if ts.torrentSvc != nil {
		for _, part := range strings.Fields(text) {
			if strings.HasPrefix(part, "magnet:?") {
				return ts.handleTorrentMirror(ctx, entities, update, msg, part, nil, userID)
			}
		}
	}

	parts := strings.Fields(text)

	var rawURL string
	for _, part := range parts[1:] {
		if strings.HasPrefix(part, "http://") || strings.HasPrefix(part, "https://") {
			rawURL = part
			break
		}
	}

	if rawURL != "" {
		// If URL is a .torrent link, fetch bytes and handle as torrent mirror
		if ts.torrentSvc != nil && (strings.HasSuffix(strings.ToLower(rawURL), ".torrent") || strings.Contains(strings.ToLower(rawURL), ".torrent?")) {
			body, _, _, err := ts.downloader.DownloadHTTP(ctx, rawURL, nil)
			if err == nil {
				data, dErr := io.ReadAll(body)
				body.Close()
				if dErr == nil && len(data) > 0 {
					return ts.handleTorrentMirror(ctx, entities, update, msg, "", data, userID)
				}
			}
		}

		fileName := ExtractFileName(rawURL, "")

		target := ts.extractJobTarget(msg, entities)
		var jobRef *Job
		execFunc := func() {
			ts.executeURLMirrorJob(jobRef, rawURL)
		}

		job, err := ts.jm.CreateJob(ctx, JobTypeMirror, fileName, 0, userID, execFunc)
		if err != nil {
			slog.Error("failed creating URL mirror job", "error", err)
			_, replyErr := ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("Error creating job: %v", err))
			return replyErr
		}
		job.Kind = "url_mirror"
		job.Target = target
		job.RawURL = rawURL
		ts.jm.SaveState()
		jobRef = job

		slog.Info("url mirror job created", "job_id", job.ID, "url", rawURL)
		go ts.startLiveStatusUpdater(target)
		return nil
	}

	// Reply to .torrent file? Handle as torrent mirror (single file, no batch).
	if ts.torrentSvc != nil && msg.ReplyTo != nil {
		if rh, ok := msg.ReplyTo.(*tg.MessageReplyHeader); ok && rh.ReplyToMsgID != 0 {
			// Try fetch replied message to see if it is a .torrent document
			channelID, accessHash := extractPeerChannelInfo(msg.PeerID, entities)
			var res tg.MessagesMessagesClass
			var err error
			if channelID != 0 {
				res, err = ts.client.API().ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{Channel: &tg.InputChannel{ChannelID: channelID, AccessHash: accessHash}, ID: []tg.InputMessageClass{&tg.InputMessageID{ID: rh.ReplyToMsgID}}})
			} else {
				res, err = ts.client.API().MessagesGetMessages(ctx, []tg.InputMessageClass{&tg.InputMessageID{ID: rh.ReplyToMsgID}})
			}
			if err == nil {
				var slice []tg.MessageClass
				switch m := res.(type) {
				case *tg.MessagesMessages: slice = m.Messages
				case *tg.MessagesMessagesSlice: slice = m.Messages
				case *tg.MessagesChannelMessages: slice = m.Messages
				}
				if len(slice) > 0 {
					if targetMsg, ok := slice[0].(*tg.Message); ok && targetMsg.Media != nil {
						if docMedia, ok := targetMsg.Media.(*tg.MessageMediaDocument); ok {
							if doc, ok := docMedia.Document.AsNotEmpty(); ok {
								for _, attr := range doc.Attributes {
									if fn, ok := attr.(*tg.DocumentAttributeFilename); ok {
										if strings.HasSuffix(strings.ToLower(fn.FileName), ".torrent") {
											loc := &tg.InputDocumentFileLocation{ID: doc.ID, AccessHash: doc.AccessHash, FileReference: doc.FileReference}
											data, dErr := ts.downloadDocumentBytes(ctx, loc, doc.Size)
											if dErr == nil {
												return ts.handleTorrentMirror(ctx, entities, update, msg, "", data, userID)
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
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

	channelID, accessHash := extractPeerChannelInfo(msg.PeerID, entities)

	for offset := 0; offset < count; offset++ {
		targetID := startMsgID + offset

		var res tg.MessagesMessagesClass
		var err error

		if channelID != 0 {
			res, err = ts.client.API().ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
				Channel: &tg.InputChannel{ChannelID: channelID, AccessHash: accessHash},
				ID:      []tg.InputMessageClass{&tg.InputMessageID{ID: targetID}},
			})
		} else {
			res, err = ts.client.API().MessagesGetMessages(ctx, []tg.InputMessageClass{
				&tg.InputMessageID{ID: targetID},
			})
		}

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

		target := ts.extractJobTarget(msg, entities)
		var jobRef *Job
		execFunc := func() {
			ts.executeMirrorJob(jobRef, location)
		}

		job, err := ts.jm.CreateJob(ctx, JobTypeMirror, fileName, fileSize, userID, execFunc)
		if err != nil {
			slog.Warn("could not create mirror job", "msg_id", targetID, "error", err)
			continue
		}
		job.Kind = "tg_mirror"
		job.Target = target
		job.Location = locationToStored(location)
		ts.jm.SaveState()
		jobRef = job

		queuedJobs++
	}

	if queuedJobs == 0 {
		_, err := ts.sender.Reply(entities, update).Text(ctx, "No downloadable media files found in the requested range.")
		return err
	}

	go ts.startLiveStatusUpdater(ts.extractJobTarget(msg, entities))

	return nil
}

// sendMirrorCompletion sends the completion message with Name/Size/Type and a copy button for the index link.
func (ts *TelegramService) sendMirrorCompletion(ctx context.Context, job *Job, driveURL string) {
	baseURL := ts.cfg.IndexBaseURL
	if baseURL != "" && !strings.HasSuffix(baseURL, "/") {
		baseURL += "/"
	}
	indexURL := baseURL + url.PathEscape(job.FileName)

	// Detect MIME type from filename
	mimeType := "unknown"
	if idx := strings.LastIndex(job.FileName, "."); idx >= 0 {
		ext := job.FileName[idx:]
		if t := mime.TypeByExtension(ext); t != "" {
			mimeType = t
		}
	}

	completionOpts := []styling.StyledTextOption{
		styling.Bold("Name: "), styling.Code(job.FileName), styling.Plain("\n"),
		styling.Bold("Size: "), styling.Plain(FormatBytes(job.Size)), styling.Plain("\n"),
		styling.Bold("Type: "), styling.Plain(mimeType),
	}

	// Build reply with inline keyboard if we have an index URL
	var replyMarkup tg.ReplyMarkupClass
	if indexURL != "" {
		copyBtn := &tg.KeyboardButtonCopy{
			Text:     "Index Link",
			CopyText: indexURL,
		}
		replyMarkup = markup.InlineKeyboard(
			tg.KeyboardButtonRow{Buttons: []tg.KeyboardButtonClass{copyBtn}},
		)
	}

	builder := ts.targetSender(job.Target)
	if replyMarkup != nil {
		builder = builder.Markup(replyMarkup)
	}
	_, _ = builder.StyledText(ctx, completionOpts...)
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

func (ts *TelegramService) executeURLMirrorJob(job *Job, rawURL string) {
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
		ts.sendJobFailure(job, err)
		return
	}

	job.Status = "Completed"
	slog.Info("URL mirror job completed", "job_id", job.ID, "drive_url", driveURL)

	ts.sendMirrorCompletion(context.Background(), job, driveURL)
}

func (ts *TelegramService) executeMirrorJob(job *Job, location tg.InputFileLocationClass) {
	defer ts.jm.FinishJob(job.ID)

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
		ts.sendJobFailure(job, err)
		return
	}

	job.Status = "Completed"
	slog.Info("mirror job completed", "job_id", job.ID, "drive_url", driveURL)

	ts.sendMirrorCompletion(context.Background(), job, driveURL)
}

func (ts *TelegramService) executeMirrorStream(job *Job, location tg.InputFileLocationClass) (string, error) {
	pr, pw := io.Pipe()

	progressWriter := NewProgressWriter(pw, job.Size, func(read, total int64, speed float64, eta time.Duration) {
		job.ReadBytes = read
		job.Speed = speed
		job.ETA = eta
	})

	threads := ts.cfg.DownloadThreads
	if threads <= 0 {
		threads = 4
	}

	go func() {
		defer pw.Close()

		if err := ts.dm.Acquire(job.Ctx); err != nil {
			pw.CloseWithError(err)
			return
		}
		defer ts.dm.Release()

		slog.Info("starting pipelined telegram media stream download", "job_id", job.ID, "threads", threads)

		api := ts.client.API()
		invoker, _, poolErr := ts.getOrCreatePool(job.Ctx, location, threads)
		if poolErr != nil {
			slog.Warn("pool creation failed, using single connection", "error", poolErr)
		} else if invoker != nil {
			api = tg.NewClient(invoker)
		}

		var err error
		if job.Size > 0 {
			err = rawPipelinedStream(job.Ctx, api, location, job.Size, threads, ts.cfg.PartSize, progressWriter)
		} else {
			_, err = ts.client.Download(location).Stream(job.Ctx, progressWriter)
		}

		if err != nil {
			slog.Error("telegram media download stream error", "job_id", job.ID, "error", err)
			pw.CloseWithError(err)
		} else {
			slog.Info("telegram media download stream finished", "job_id", job.ID)
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

// getOrCreatePool returns a cached pool for the given DC, or creates one.
// The pool opens `threads` independent mtproto connections to the target DC —
// this is the multi-socket engine that makes rawParallelDownload fast.
// Pools are cached per DC so concurrent jobs share one multi-connection pool
// instead of each racing to auth-export to the same DC.
func (ts *TelegramService) getOrCreatePool(ctx context.Context, location tg.InputFileLocationClass, threads int) (tg.Invoker, io.Closer, error) {
	var dc int
	var err error
	if location != nil {
		dc, err = detectFileDC(ctx, ts.client, location)
		if err != nil {
			return nil, nil, fmt.Errorf("detect file DC: %w", err)
		}
	}

	ts.poolMu.Lock()
	defer ts.poolMu.Unlock()

	if cached, ok := ts.poolCache[dc]; ok {
		return cached.invoker, cached.closer, nil
	}

	var invoker tg.Invoker
	var closer io.Closer
	if dc == 0 {
		invoker, err = ts.client.Pool(int64(threads))
	} else {
		slog.Info("creating pool to remote DC", "dc", dc, "threads", threads)
		invoker, err = ts.client.DC(ctx, dc, int64(threads))
	}
	if err != nil {
		return nil, nil, fmt.Errorf("create DC%d pool: %w", dc, err)
	}
	closer, ok := invoker.(io.Closer)
	if !ok {
		closer = io.NopCloser(nil)
	}

	ts.poolCache[dc] = struct {
		invoker tg.Invoker
		closer  io.Closer
	}{invoker, closer}
	return invoker, closer, nil
}

// ClosePools closes all cached DC pools on bot shutdown.
func (ts *TelegramService) ClosePools() {
	ts.poolMu.Lock()
	defer ts.poolMu.Unlock()

	for dc, p := range ts.poolCache {
		slog.Info("closing cached DC pool", "dc", dc)
		p.closer.Close()
	}
	ts.poolCache = make(map[int]struct {
		invoker tg.Invoker
		closer  io.Closer
	})
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

	// Global cap on concurrent file downloads across all jobs (per-account limit).
	// Released immediately when rawParallelDownload finishes, before GDrive upload.
	if err := ts.dm.Acquire(ctx); err != nil {
		return "", fmt.Errorf("acquire download slot: %w", err)
	}

	// Try to create multi-connection pool to remote DC for faster downloads.
	// Note: poolCloser is NOT closed per-job because pools are cached in ts.poolCache
	// and shared across concurrent jobs. Closing it here would kill active jobs!
	api := ts.client.API()
	invoker, _, poolErr := ts.getOrCreatePool(ctx, location, threads)
	if poolErr != nil {
		slog.Warn("pool creation failed, using single connection", "error", poolErr)
	} else {
		api = tg.NewClient(invoker)
	}

	err = rawParallelDownload(ctx, api, location, job.Size, threads, ts.cfg.PartSize, tmpFile)
	ts.dm.Release()

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

func (ts *TelegramService) handleLeech(ctx context.Context, entities tg.Entities, update message.AnswerableMessageUpdate, msg *tg.Message, text string, userID int64) error {
	if ts.torrentSvc != nil {
		for _, part := range strings.Fields(text) {
			if strings.HasPrefix(part, "magnet:?") {
				return ts.handleTorrentLeech(ctx, entities, update, msg, part, nil, userID)
			}
		}
		if msg.ReplyTo != nil {
			if rh, ok := msg.ReplyTo.(*tg.MessageReplyHeader); ok && rh.ReplyToMsgID != 0 {
				channelID, accessHash := extractPeerChannelInfo(msg.PeerID, entities)
				var res tg.MessagesMessagesClass
				var err error
				if channelID != 0 {
					res, err = ts.client.API().ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{Channel: &tg.InputChannel{ChannelID: channelID, AccessHash: accessHash}, ID: []tg.InputMessageClass{&tg.InputMessageID{ID: rh.ReplyToMsgID}}})
				} else {
					res, err = ts.client.API().MessagesGetMessages(ctx, []tg.InputMessageClass{&tg.InputMessageID{ID: rh.ReplyToMsgID}})
				}
				if err == nil {
					var slice []tg.MessageClass
					switch m := res.(type) {
					case *tg.MessagesMessages: slice = m.Messages
					case *tg.MessagesMessagesSlice: slice = m.Messages
					case *tg.MessagesChannelMessages: slice = m.Messages
					}
					if len(slice) > 0 {
						if targetMsg, ok := slice[0].(*tg.Message); ok && targetMsg.Media != nil {
							if docMedia, ok := targetMsg.Media.(*tg.MessageMediaDocument); ok {
								if doc, ok := docMedia.Document.AsNotEmpty(); ok {
									for _, attr := range doc.Attributes {
										if fn, ok := attr.(*tg.DocumentAttributeFilename); ok {
											if strings.HasSuffix(strings.ToLower(fn.FileName), ".torrent") {
												loc := &tg.InputDocumentFileLocation{ID: doc.ID, AccessHash: doc.AccessHash, FileReference: doc.FileReference}
												if data, dErr := ts.downloadDocumentBytes(ctx, loc, doc.Size); dErr == nil {
													return ts.handleTorrentLeech(ctx, entities, update, msg, "", data, userID)
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}
	parts := strings.Fields(text)
	if len(parts) < 2 {
		_, err := ts.sender.Reply(entities, update).Text(ctx, "Usage: /leech <url>")
		return err
	}
	rawURL := parts[1]

	// If URL is a .torrent link, fetch bytes and handle as torrent leech
	if ts.torrentSvc != nil && (strings.HasSuffix(strings.ToLower(rawURL), ".torrent") || strings.Contains(strings.ToLower(rawURL), ".torrent?")) {
		body, _, _, err := ts.downloader.DownloadHTTP(ctx, rawURL, nil)
		if err == nil {
			data, dErr := io.ReadAll(body)
			body.Close()
			if dErr == nil && len(data) > 0 {
				return ts.handleTorrentLeech(ctx, entities, update, msg, "", data, userID)
			}
		}
	}

	target := ts.extractJobTarget(msg, entities)
	var jobRef *Job
	execFunc := func() {
		ts.executeLeechJob(jobRef, rawURL)
	}

	fileName := ExtractFileName(rawURL, "")
	job, err := ts.jm.CreateJob(ctx, JobTypeLeech, fileName, 0, userID, execFunc)
	if err != nil {
		slog.Error("failed creating leech job", "error", err)
		_, replyErr := ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("Error creating job: %v", err))
		return replyErr
	}
	job.Kind = "url_leech"
	job.Target = target
	job.RawURL = rawURL
	ts.jm.SaveState()
	jobRef = job

	slog.Info("leech job created", "job_id", job.ID, "url", rawURL)
	go ts.startLiveStatusUpdater(target)
	return nil
}

func (ts *TelegramService) executeLeechJob(job *Job, rawURL string) {
	defer ts.jm.FinishJob(job.ID)

	slog.Info("executing leech job", "job_id", job.ID, "url", rawURL)
	job.Phase = PhaseDownloading
	job.Status = "Downloading HTTP link"

	body, contentLength, fileName, err := ts.downloader.DownloadHTTP(job.Ctx, rawURL, nil)
	if err != nil {
		slog.Error("leech download failed", "job_id", job.ID, "error", err)
		job.Status = fmt.Sprintf("Failed: %v", err)
		ts.sendJobFailure(job, err)
		return
	}
	defer body.Close()

	job.FileName = fileName
	job.Size = contentLength
	ts.jm.SaveState()

	if contentLength > 2*1024*1024*1024 {
		sizeErr := fmt.Errorf("file size (%s) exceeds Telegram 2GB limit", FormatBytes(contentLength))
		slog.Error("leech file exceeds 2GB limit", "job_id", job.ID, "size", contentLength)
		job.Status = "Failed: exceeds 2GB"
		ts.sendJobFailure(job, sizeErr)
		return
	}

	progressReader := NewProgressReader(body, contentLength, func(read, total int64, speed float64, eta time.Duration) {
		job.ReadBytes = read
		job.Speed = speed
		job.ETA = eta
	})

	uploader := ts.client.API()
	job.Phase = PhaseUploading
	job.Status = "Uploading to Telegram"

	var inputFile tg.InputFileClass
	var uploadErr error
	if ts.cfg.DownloadMode == "parallel" && contentLength > 0 {
		inputFile, uploadErr = ts.executeLeechParallel(job, uploader, progressReader, fileName, contentLength)
	} else {
		inputFile, uploadErr = ts.executeLeechStream(job, uploader, progressReader, fileName, contentLength)
	}

	if uploadErr != nil {
		slog.Error("leech upload failed", "job_id", job.ID, "error", uploadErr)
		job.Status = fmt.Sprintf("Failed: %v", uploadErr)
		ts.sendJobFailure(job, uploadErr)
		return
	}

	// Send uploaded file to the Telegram chat
	mediaOpt := buildMediaOption(inputFile, fileName)
	_, sendErr := ts.targetSender(job.Target).Media(context.Background(), mediaOpt)
	if sendErr != nil {
		slog.Error("failed sending uploaded file to chat", "job_id", job.ID, "file", fileName, "error", sendErr)
		job.Status = fmt.Sprintf("Failed delivering to chat: %v", sendErr)
		ts.sendJobFailure(job, sendErr)
		return
	}

	job.Status = "Completed"
	slog.Info("leech job completed", "job_id", job.ID)
}

func buildMediaOption(inputFile tg.InputFileClass, fileName string) message.MediaOption {
	ext := strings.ToLower(filepath.Ext(fileName))
	mimeType := mime.TypeByExtension(ext)
	caption := styling.Plain(fileName)

	// Explicit video extensions
	isVideo := strings.HasPrefix(mimeType, "video/") ||
		ext == ".mkv" || ext == ".mp4" || ext == ".avi" || ext == ".mov" ||
		ext == ".webm" || ext == ".flv" || ext == ".wmv" || ext == ".m4v" || ext == ".ts"

	if isVideo {
		if mimeType == "" {
			mimeType = "video/mp4"
			if ext == ".mkv" {
				mimeType = "video/x-matroska"
			} else if ext == ".webm" {
				mimeType = "video/webm"
			}
		}
		doc := message.UploadedDocument(inputFile, caption).Filename(fileName).MIME(mimeType)
		return doc.Video().SupportsStreaming()
	}

	// Explicit audio extensions
	isAudio := strings.HasPrefix(mimeType, "audio/") ||
		ext == ".mp3" || ext == ".m4a" || ext == ".flac" || ext == ".aac" ||
		ext == ".ogg" || ext == ".opus" || ext == ".wav" || ext == ".wma"

	if isAudio {
		if mimeType == "" {
			mimeType = "audio/mpeg"
			if ext == ".flac" {
				mimeType = "audio/flac"
			} else if ext == ".m4a" {
				mimeType = "audio/mp4"
			} else if ext == ".ogg" || ext == ".opus" {
				mimeType = "audio/ogg"
			} else if ext == ".wav" {
				mimeType = "audio/wav"
			}
		}
		doc := message.UploadedDocument(inputFile, caption).Filename(fileName).MIME(mimeType)
		return doc.Audio()
	}

	// Default fallback: document with exact filename and MIME
	doc := message.UploadedDocument(inputFile, caption).Filename(fileName)
	if mimeType != "" {
		doc = doc.MIME(mimeType)
	}
	return doc
}

func (ts *TelegramService) executeLeechStream(job *Job, api *tg.Client, reader io.Reader, fileName string, size int64) (tg.InputFileClass, error) {
	return ts.executeLeechParallel(job, api, reader, fileName, size)
}

func (ts *TelegramService) executeLeechParallel(job *Job, api *tg.Client, reader io.Reader, fileName string, size int64) (tg.InputFileClass, error) {
	threads := ts.cfg.DownloadThreads
	if threads <= 0 {
		threads = 4
	}

	uploadAPI := api
	invoker, closer, err := ts.getOrCreatePool(job.Ctx, nil, threads)
	if err == nil && invoker != nil {
		uploadAPI = tg.NewClient(invoker)
		_ = closer
	} else {
		slog.Warn("failed creating upload pool, using default client", "error", err)
	}

	u := uploader.NewUploader(uploadAPI).WithThreads(threads)
	if ts.cfg.PartSize > 0 {
		u = u.WithPartSize(ts.cfg.PartSize)
	}
	if size > 0 {
		return u.Upload(job.Ctx, uploader.NewUpload(fileName, reader, size))
	}
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

func (ts *TelegramService) RecoverJobs(ctx context.Context) {
	records, err := ts.jm.LoadPersistedState()
	if err != nil {
		slog.Error("failed loading persisted jobs for recovery", "error", err)
		return
	}
	if len(records) == 0 {
		return
	}

	slog.Info("recovering unfinished jobs from previous session", "count", len(records))

	for _, rec := range records {
		r := rec
		switch r.Kind {
		case "url_mirror", "url_leech":
			if r.RawURL == "" {
				ts.sendTargetFailure(r.Target, r.FileName, string(r.Type), errors.New("missing source URL"))
				ts.jm.RemovePersistedJob(r.ID)
				continue
			}
			probeCtx, probeCancel := context.WithTimeout(ctx, 8*time.Second)
			req, pErr := http.NewRequestWithContext(probeCtx, http.MethodHead, r.RawURL, nil)
			var resp *http.Response
			if pErr == nil {
				req.Header.Set("User-Agent", "Mozilla/5.0")
				resp, pErr = http.DefaultClient.Do(req)
			}
			probeCancel()
			if pErr != nil || (resp != nil && resp.StatusCode >= 400 && resp.StatusCode != http.StatusMethodNotAllowed) {
				var failReason error
				if pErr != nil {
					failReason = pErr
				} else if resp != nil {
					failReason = fmt.Errorf("source HTTP %s", resp.Status)
				}
				ts.sendTargetFailure(r.Target, r.FileName, string(r.Type), fmt.Errorf("source URL unreachable on recovery: %w", failReason))
				ts.jm.RemovePersistedJob(r.ID)
				if resp != nil && resp.Body != nil {
					_ = resp.Body.Close()
				}
				continue
			}
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}

		case "torrent_mirror", "torrent_leech":
			if ts.torrentSvc == nil {
				ts.sendTargetFailure(r.Target, r.FileName, string(r.Type), errors.New("torrent engine is disabled"))
				ts.jm.RemovePersistedJob(r.ID)
				continue
			}
			if r.MagnetURI == "" && len(r.TorrentBytes) == 0 {
				ts.sendTargetFailure(r.Target, r.FileName, string(r.Type), errors.New("missing torrent source data"))
				ts.jm.RemovePersistedJob(r.ID)
				continue
			}

		case "tg_mirror":
			if r.Location == nil {
				ts.sendTargetFailure(r.Target, r.FileName, string(r.Type), errors.New("missing media file location"))
				ts.jm.RemovePersistedJob(r.ID)
				continue
			}
		}

		notice := fmt.Sprintf("🔄 Bot restarted. Resuming job %s: %s", r.ID, r.FileName)
		noticeCtx, noticeCancel := context.WithTimeout(ctx, 10*time.Second)
		_, _ = ts.targetSender(r.Target).Text(noticeCtx, notice)
		noticeCancel()

		go ts.startLiveStatusUpdater(r.Target)

		var jobRef *Job
		var execFunc func()
		switch r.Kind {
		case "url_mirror":
			execFunc = func() { ts.executeURLMirrorJob(jobRef, r.RawURL) }
		case "url_leech":
			execFunc = func() { ts.executeLeechJob(jobRef, r.RawURL) }
		case "tg_mirror":
			execFunc = func() { ts.executeMirrorJob(jobRef, storedToLocation(r.Location)) }
		case "torrent_mirror":
			execFunc = func() { ts.executeTorrentMirrorJob(jobRef, r.MagnetURI, r.TorrentBytes) }
		case "torrent_leech":
			execFunc = func() { ts.executeTorrentLeechJob(jobRef, r.MagnetURI, r.TorrentBytes) }
		default:
			ts.sendTargetFailure(r.Target, r.FileName, string(r.Type), fmt.Errorf("unknown job kind: %s", r.Kind))
			ts.jm.RemovePersistedJob(r.ID)
			continue
		}

		job, err := ts.jm.CreateRecoveredJob(ctx, r, execFunc)
		if err != nil {
			slog.Error("failed recreating recovered job", "job_id", r.ID, "error", err)
			ts.sendTargetFailure(r.Target, r.FileName, string(r.Type), fmt.Errorf("failed recovering job: %w", err))
			ts.jm.RemovePersistedJob(r.ID)
			continue
		}
		jobRef = job
	}
}

func (ts *TelegramService) StartFeedWorker(ctx context.Context, interval time.Duration) {
	if ts.feedMgr != nil {
		go ts.feedMgr.StartBackgroundPoller(ctx, interval)
	}
}

func (ts *TelegramService) handleBotCallbackQuery(ctx context.Context, entities tg.Entities, u *tg.UpdateBotCallbackQuery) error {
	data := string(u.Data)
	if strings.HasPrefix(data, "feed:") {
		return ts.handleFeedCallback(ctx, entities, u)
	}
	return nil
}

func (ts *TelegramService) handleFeedCallback(ctx context.Context, entities tg.Entities, u *tg.UpdateBotCallbackQuery) error {
	data := string(u.Data)
	parts := strings.Split(data, ":")
	if len(parts) < 2 {
		return nil
	}

	action := parts[1]
	isOwner := ts.cfg.IsOwner(u.UserID)
	toast := ""

	switch action {
	case "pause":
		if len(parts) >= 3 {
			id, _ := strconv.Atoi(parts[2])
			feed, err := ts.feedMgr.PauseFeed(id, u.UserID, isOwner)
			if err != nil {
				toast = err.Error()
			} else {
				toast = fmt.Sprintf("Paused #%d (%s)", feed.ID, feed.Name)
			}
		}
	case "resume":
		if len(parts) >= 3 {
			id, _ := strconv.Atoi(parts[2])
			feed, err := ts.feedMgr.ResumeFeed(id, u.UserID, isOwner)
			if err != nil {
				toast = err.Error()
			} else {
				toast = fmt.Sprintf("Resumed #%d (%s)", feed.ID, feed.Name)
			}
		}
	case "del":
		if len(parts) >= 3 {
			id, _ := strconv.Atoi(parts[2])
			feed, err := ts.feedMgr.DeleteFeed(id, u.UserID, isOwner)
			if err != nil {
				toast = err.Error()
			} else {
				toast = fmt.Sprintf("Deleted #%d (%s)", feed.ID, feed.Name)
			}
		}
	case "run":
		if len(parts) >= 3 {
			feed := ts.feedMgr.GetFeed(parts[2], u.UserID, isOwner)
			if feed == nil {
				toast = "Feed not found"
			} else {
				toast = fmt.Sprintf("Checking #%d (%s)...", feed.ID, feed.Name)
				go func() {
					checkCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
					defer cancel()
					_, _ = ts.feedMgr.CheckFeed(checkCtx, feed)
				}()
			}
		}
	case "list":
		toast = "Refreshed."
	}

	req := &tg.MessagesSetBotCallbackAnswerRequest{
		QueryID: u.QueryID,
	}
	if toast != "" {
		req.SetMessage(toast)
	}
	_, _ = ts.client.API().MessagesSetBotCallbackAnswer(ctx, req)

	channelID, accessHash := extractPeerChannelInfo(u.Peer, entities)
	peer := ts.buildInputPeer(u.Peer, channelID, accessHash)
	text, feedMarkup := ts.buildFeedListMessage(u.UserID, isOwner)
	builder := ts.sender.To(peer).CloneBuilder()
	if feedMarkup != nil {
		builder = builder.Markup(feedMarkup)
	}
	_, _ = builder.Edit(u.MsgID).Text(ctx, text)
	return nil
}

func (ts *TelegramService) handleFeed(ctx context.Context, entities tg.Entities, update message.AnswerableMessageUpdate, msg *tg.Message, text string, userID int64) error {
	args := strings.Fields(text)
	isOwner := ts.cfg.IsOwner(userID)

	if len(args) == 1 || args[1] == "help" {
		feeds := ts.feedMgr.ListFeeds(userID, isOwner)
		if len(feeds) > 0 {
			msgText, feedMarkup := ts.buildFeedListMessage(userID, isOwner)
			builder := ts.sender.Reply(entities, update)
			if feedMarkup != nil {
				builder = builder.Markup(feedMarkup)
			}
			_, err := builder.Text(ctx, msgText)
			return err
		}

		helpMsg := "📡 RSS / Atom Feed Automation\n\n" +
			"Commands:\n" +
			"• /feed add [mirror|leech|notify] [NAME] [URL] [+include] [-exclude]\n" +
			"• /feed list - View active feeds with interactive buttons\n" +
			"• /feed pause <id|name> - Pause a feed\n" +
			"• /feed resume <id|name> - Resume a paused feed\n" +
			"• /feed delete <id|name> - Delete a feed\n" +
			"• /feed check [id|name] - Trigger immediate scan\n\n" +
			"Filter Syntax:\n" +
			"• +1080p,2160p = Must contain 1080p OR 2160p\n" +
			"• +Remux = Must also contain Remux\n" +
			"• -CAM,TeleSync = Exclude if either matches\n\n" +
			"Example:\n" +
			"/feed add mirror movies https://feed.com/rss +1080p,2160p +Remux -CAM,TeleSync"
		_, err := ts.sender.Reply(entities, update).Text(ctx, helpMsg)
		return err
	}

	sub := strings.ToLower(args[1])
	switch sub {
	case "list":
		msgText, feedMarkup := ts.buildFeedListMessage(userID, isOwner)
		builder := ts.sender.Reply(entities, update)
		if feedMarkup != nil {
			builder = builder.Markup(feedMarkup)
		}
		_, err := builder.Text(ctx, msgText)
		return err

	case "add":
		// Format: /feed add [mirror|leech|notify] [NAME] [URL] [+include] [-exclude]
		if len(args) < 5 {
			usage := "Usage: /feed add [mirror|leech|notify] [NAME] [URL] [+include] [-exclude]\n\n" +
				"Example:\n" +
				"/feed add mirror movies https://feed.com/rss +1080p,2160p +Remux -CAM,TeleSync"
			_, err := ts.sender.Reply(entities, update).Text(ctx, usage)
			return err
		}

		mode := FeedMode(strings.ToLower(args[2]))
		name := args[3]
		rawURL := args[4]

		var includes []string
		var excludes []string
		for i := 5; i < len(args); i++ {
			tok := args[i]
			if strings.HasPrefix(tok, "+") {
				clean := strings.TrimPrefix(tok, "+")
				if clean != "" {
					includes = append(includes, clean)
				}
			} else if strings.HasPrefix(tok, "-") {
				clean := strings.TrimPrefix(tok, "-")
				if clean != "" {
					excludes = append(excludes, clean)
				}
			}
		}

		target := ts.extractJobTarget(msg, entities)
		feed, err := ts.feedMgr.AddFeed(ctx, mode, name, rawURL, includes, excludes, userID, target)
		if err != nil {
			_, replyErr := ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("❌ Failed to add feed: %v", err))
			return replyErr
		}

		replyText := fmt.Sprintf("✅ Feed Added: #%d %s\nMode: %s\nURL: %s\nIncludes: %v\nExcludes: %v\n\nBaseline snapshot set to newest item: %q.\nFuture matches will trigger automatically.",
			feed.ID, feed.Name, strings.ToUpper(string(feed.Mode)), feed.URL, feed.Includes, feed.Excludes, feed.LastTitle)

		var rows []tg.KeyboardButtonRow
		btnPause := markup.Callback(fmt.Sprintf("⏸ Pause #%d", feed.ID), []byte(fmt.Sprintf("feed:pause:%d", feed.ID)))
		btnRun := markup.Callback(fmt.Sprintf("⚡ Check #%d", feed.ID), []byte(fmt.Sprintf("feed:run:%d", feed.ID)))
		btnDel := markup.Callback(fmt.Sprintf("🗑 Del #%d", feed.ID), []byte(fmt.Sprintf("feed:del:%d", feed.ID)))
		rows = append(rows, markup.Row(btnPause, btnRun, btnDel))

		builder := ts.sender.Reply(entities, update).Markup(markup.InlineKeyboard(rows...))
		_, err = builder.Text(ctx, replyText)
		return err

	case "pause":
		if len(args) < 3 {
			_, err := ts.sender.Reply(entities, update).Text(ctx, "Usage: /feed pause <id|name>")
			return err
		}
		targetFeed := ts.feedMgr.GetFeed(args[2], userID, isOwner)
		if targetFeed == nil {
			_, err := ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("Feed %q not found.", args[2]))
			return err
		}
		f, err := ts.feedMgr.PauseFeed(targetFeed.ID, userID, isOwner)
		if err != nil {
			_, replyErr := ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("Error: %v", err))
			return replyErr
		}
		_, err = ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("⏸ Paused feed #%d (%s).", f.ID, f.Name))
		return err

	case "resume":
		if len(args) < 3 {
			_, err := ts.sender.Reply(entities, update).Text(ctx, "Usage: /feed resume <id|name>")
			return err
		}
		targetFeed := ts.feedMgr.GetFeed(args[2], userID, isOwner)
		if targetFeed == nil {
			_, err := ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("Feed %q not found.", args[2]))
			return err
		}
		f, err := ts.feedMgr.ResumeFeed(targetFeed.ID, userID, isOwner)
		if err != nil {
			_, replyErr := ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("Error: %v", err))
			return replyErr
		}
		_, err = ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("▶ Resumed feed #%d (%s).", f.ID, f.Name))
		return err

	case "delete", "del", "remove", "rm":
		if len(args) < 3 {
			_, err := ts.sender.Reply(entities, update).Text(ctx, "Usage: /feed delete <id|name>")
			return err
		}
		targetFeed := ts.feedMgr.GetFeed(args[2], userID, isOwner)
		if targetFeed == nil {
			_, err := ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("Feed %q not found.", args[2]))
			return err
		}
		f, err := ts.feedMgr.DeleteFeed(targetFeed.ID, userID, isOwner)
		if err != nil {
			_, replyErr := ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("Error: %v", err))
			return replyErr
		}
		_, err = ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("🗑 Deleted feed #%d (%s).", f.ID, f.Name))
		return err

	case "check", "run":
		if len(args) >= 3 {
			targetFeed := ts.feedMgr.GetFeed(args[2], userID, isOwner)
			if targetFeed == nil {
				_, err := ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("Feed %q not found.", args[2]))
				return err
			}
			count, err := ts.feedMgr.CheckFeed(ctx, targetFeed)
			if err != nil {
				_, replyErr := ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("❌ Check failed for #%d: %v", targetFeed.ID, err))
				return replyErr
			}
			_, err = ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("⚡ Feed #%d checked. %d new item(s) matched and queued.", targetFeed.ID, count))
			return err
		}

		userFeeds := ts.feedMgr.ListFeeds(userID, isOwner)
		if len(userFeeds) == 0 {
			_, err := ts.sender.Reply(entities, update).Text(ctx, "No feeds to check.")
			return err
		}
		totalMatches := 0
		for _, f := range userFeeds {
			if !f.Paused {
				m, _ := ts.feedMgr.CheckFeed(ctx, f)
				totalMatches += m
			}
		}
		_, err := ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("⚡ Checked %d active feeds. Total %d new matches queued.", len(userFeeds), totalMatches))
		return err

	default:
		_, err := ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("Unknown subcommand %q. Use /feed help", sub))
		return err
	}
}

func (ts *TelegramService) buildFeedListMessage(userID int64, isOwner bool) (string, tg.ReplyMarkupClass) {
	feeds := ts.feedMgr.ListFeeds(userID, isOwner)
	if len(feeds) == 0 {
		text := "📡 No RSS/Feed subscriptions found.\n\n" +
			"Add one with:\n" +
			"/feed add [mirror|leech|notify] [NAME] [URL] [+include] [-exclude]"
		return text, nil
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("📡 RSS / Feed Subscriptions (%d total)\n\n", len(feeds)))

	var rows []tg.KeyboardButtonRow
	for _, f := range feeds {
		status := "🟢 Active"
		if f.Paused {
			status = "⏸ Paused"
		}

		checkedStr := "Never"
		if !f.LastChecked.IsZero() {
			checkedStr = time.Since(f.LastChecked).Round(time.Second).String() + " ago"
		}

		filterStr := ""
		if len(f.Includes) > 0 {
			filterStr += fmt.Sprintf(" +%s", strings.Join(f.Includes, " +"))
		}
		if len(f.Excludes) > 0 {
			filterStr += fmt.Sprintf(" -%s", strings.Join(f.Excludes, " -"))
		}
		if filterStr == "" {
			filterStr = " None"
		}

		sb.WriteString(fmt.Sprintf("#%d %s [%s]\nStatus: %s | Checked: %s\nFilters:%s\nURL: %s\n\n",
			f.ID, f.Name, strings.ToUpper(string(f.Mode)), status, checkedStr, filterStr, f.URL))

		var btnPause tg.KeyboardButtonClass
		if f.Paused {
			btnPause = markup.Callback(fmt.Sprintf("▶ Resume #%d", f.ID), []byte(fmt.Sprintf("feed:resume:%d", f.ID)))
		} else {
			btnPause = markup.Callback(fmt.Sprintf("⏸ Pause #%d", f.ID), []byte(fmt.Sprintf("feed:pause:%d", f.ID)))
		}
		btnRun := markup.Callback(fmt.Sprintf("⚡ Check #%d", f.ID), []byte(fmt.Sprintf("feed:run:%d", f.ID)))
		btnDel := markup.Callback(fmt.Sprintf("🗑 Del #%d", f.ID), []byte(fmt.Sprintf("feed:del:%d", f.ID)))

		rows = append(rows, markup.Row(btnPause, btnRun, btnDel))
	}

	btnRefresh := markup.Callback("🔄 Refresh", []byte(fmt.Sprintf("feed:list:%d", userID)))
	rows = append(rows, markup.Row(btnRefresh))

	return strings.TrimSpace(sb.String()), markup.InlineKeyboard(rows...)
}

func (ts *TelegramService) notifyFeedMatch(f *FeedSubscription, item FeedItem) {
	msg := fmt.Sprintf("📢 Feed Alert: %s\n\nTitle: %s\nLink: %s", f.Name, item.Title, item.Link)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _ = ts.targetSender(f.Target).Text(ctx, msg)
}

func (ts *TelegramService) createFeedMirrorJob(ctx context.Context, f *FeedSubscription, title, link string) {
	if link == "" {
		return
	}
	slog.Info("creating feed mirror job", "feed_id", f.ID, "feed_name", f.Name, "link", link)

	if strings.HasPrefix(link, "magnet:?") || strings.HasSuffix(strings.ToLower(link), ".torrent") {
		if ts.torrentSvc == nil {
			ts.sendTargetFailure(f.Target, title, "Feed Torrent", errors.New("torrent engine disabled"))
			return
		}
		var magnetURI string
		var torrentBytes []byte
		if strings.HasPrefix(link, "magnet:?") {
			magnetURI = link
		} else {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, link, nil)
			if err == nil {
				req.Header.Set("User-Agent", "Mozilla/5.0")
				resp, err := http.DefaultClient.Do(req)
				if err == nil && resp.StatusCode == http.StatusOK {
					torrentBytes, _ = io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
					resp.Body.Close()
				}
			}
		}

		var jobRef *Job
		execFn := func() {
			ts.executeTorrentMirrorJob(jobRef, magnetURI, torrentBytes)
		}
		job, err := ts.jm.CreateJob(ctx, JobTypeMirror, title, 0, f.UserID, execFn)
		if err != nil {
			ts.sendTargetFailure(f.Target, title, "Feed Mirror", err)
			return
		}
		job.IsTorrent = true
		job.Kind = "torrent_mirror"
		job.Target = f.Target
		job.MagnetURI = magnetURI
		job.TorrentBytes = torrentBytes
		ts.jm.SaveState()
		jobRef = job
		go ts.startLiveStatusUpdater(f.Target)
		return
	}

	var jobRef *Job
	execFn := func() {
		ts.executeURLMirrorJob(jobRef, link)
	}
	job, err := ts.jm.CreateJob(ctx, JobTypeMirror, title, 0, f.UserID, execFn)
	if err != nil {
		ts.sendTargetFailure(f.Target, title, "Feed Mirror", err)
		return
	}
	job.Kind = "url_mirror"
	job.Target = f.Target
	job.RawURL = link
	ts.jm.SaveState()
	jobRef = job
	go ts.startLiveStatusUpdater(f.Target)
}

func (ts *TelegramService) createFeedLeechJob(ctx context.Context, f *FeedSubscription, title, link string) {
	if link == "" {
		return
	}
	slog.Info("creating feed leech job", "feed_id", f.ID, "feed_name", f.Name, "link", link)

	if strings.HasPrefix(link, "magnet:?") || strings.HasSuffix(strings.ToLower(link), ".torrent") {
		if ts.torrentSvc == nil {
			ts.sendTargetFailure(f.Target, title, "Feed Torrent", errors.New("torrent engine disabled"))
			return
		}
		var magnetURI string
		var torrentBytes []byte
		if strings.HasPrefix(link, "magnet:?") {
			magnetURI = link
		} else {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, link, nil)
			if err == nil {
				req.Header.Set("User-Agent", "Mozilla/5.0")
				resp, err := http.DefaultClient.Do(req)
				if err == nil && resp.StatusCode == http.StatusOK {
					torrentBytes, _ = io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
					resp.Body.Close()
				}
			}
		}

		var jobRef *Job
		execFn := func() {
			ts.executeTorrentLeechJob(jobRef, magnetURI, torrentBytes)
		}
		job, err := ts.jm.CreateJob(ctx, JobTypeLeech, title, 0, f.UserID, execFn)
		if err != nil {
			ts.sendTargetFailure(f.Target, title, "Feed Leech", err)
			return
		}
		job.IsTorrent = true
		job.Kind = "torrent_leech"
		job.Target = f.Target
		job.MagnetURI = magnetURI
		job.TorrentBytes = torrentBytes
		ts.jm.SaveState()
		jobRef = job
		go ts.startLiveStatusUpdater(f.Target)
		return
	}

	var jobRef *Job
	execFn := func() {
		ts.executeLeechJob(jobRef, link)
	}
	job, err := ts.jm.CreateJob(ctx, JobTypeLeech, title, 0, f.UserID, execFn)
	if err != nil {
		ts.sendTargetFailure(f.Target, title, "Feed Leech", err)
		return
	}
	job.Kind = "url_leech"
	job.Target = f.Target
	job.RawURL = link
	ts.jm.SaveState()
	jobRef = job
	go ts.startLiveStatusUpdater(f.Target)
}

