package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/gotd/td/telegram/message"
	"github.com/gotd/td/tg"
)

func (ts *TelegramService) handleTorrentMirror(ctx context.Context, entities tg.Entities, update message.AnswerableMessageUpdate, msg *tg.Message, magnetURI string, torrentBytes []byte, userID int64) error {
	if ts.torrentSvc == nil || ts.torrentSvc.client == nil {
		_, err := ts.sender.Reply(entities, update).Text(ctx, "Torrent service disabled (missing config).")
		return err
	}
	name := magnetURI
	if name == "" && len(torrentBytes) > 0 {
		if mi, err := metainfo.Load(bytes.NewReader(torrentBytes)); err == nil {
			if info, err := mi.UnmarshalInfo(); err == nil {
				name = info.BestName()
			}
		}
		if name == "" {
			name = "torrent"
		}
	}
	if name == "" {
		name = "torrent"
	}
	// Truncate magnet for display
	displayName := name
	if strings.HasPrefix(displayName, "magnet:?") {
		if len(displayName) > 60 {
			displayName = displayName[:60] + "..."
		}
	}
	var jobRef *Job
	execFn := func() {
		ts.executeTorrentMirrorJob(jobRef, magnetURI, torrentBytes, entities, update)
	}
	job, err := ts.jm.CreateJob(ctx, JobTypeMirror, displayName, 0, userID, execFn)
	if err != nil {
		slog.Error("failed creating torrent mirror job", "error", err)
		_, _ = ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("Error creating job: %v", err))
		return err
	}
	job.IsTorrent = true
	jobRef = job
	slog.Info("torrent mirror job created", "job_id", job.ID, "name", displayName)
	go ts.startLiveStatusUpdater(job.Ctx, entities, update, msg)
	return nil
}

func (ts *TelegramService) handleTorrentLeech(ctx context.Context, entities tg.Entities, update message.AnswerableMessageUpdate, msg *tg.Message, magnetURI string, torrentBytes []byte, userID int64) error {
	if ts.torrentSvc == nil || ts.torrentSvc.client == nil {
		_, err := ts.sender.Reply(entities, update).Text(ctx, "Torrent service disabled (missing config).")
		return err
	}
	name := magnetURI
	if name == "" && len(torrentBytes) > 0 {
		if mi, err := metainfo.Load(bytes.NewReader(torrentBytes)); err == nil {
			if info, err := mi.UnmarshalInfo(); err == nil {
				name = info.BestName()
			}
		}
		if name == "" {
			name = "torrent"
		}
	}
	if name == "" {
		name = "torrent"
	}
	displayName := name
	if strings.HasPrefix(displayName, "magnet:?") && len(displayName) > 60 {
		displayName = displayName[:60] + "..."
	}
	var jobRef *Job
	execFn := func() {
		ts.executeTorrentLeechJob(jobRef, magnetURI, torrentBytes, entities, update)
	}
	job, err := ts.jm.CreateJob(ctx, JobTypeLeech, displayName, 0, userID, execFn)
	if err != nil {
		slog.Error("failed creating torrent leech job", "error", err)
		_, _ = ts.sender.Reply(entities, update).Text(ctx, fmt.Sprintf("Error creating job: %v", err))
		return err
	}
	job.IsTorrent = true
	jobRef = job
	slog.Info("torrent leech job created", "job_id", job.ID, "name", displayName)
	go ts.startLiveStatusUpdater(job.Ctx, entities, update, msg)
	return nil
}

func (ts *TelegramService) downloadDocumentBytes(ctx context.Context, loc tg.InputFileLocationClass, size int64) ([]byte, error) {
	pr, pw := io.Pipe()
	go func() {
		_, err := ts.client.Download(loc).Stream(ctx, pw)
		if err != nil {
			_ = pw.CloseWithError(err)
		} else {
			_ = pw.Close()
		}
	}()
	if size > 10*1024*1024 {
		return nil, fmt.Errorf("file too large for torrent payload: %d bytes", size)
	}
	data, err := io.ReadAll(pr)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (ts *TelegramService) executeTorrentMirrorJob(job *Job, magnetURI string, torrentBytes []byte, entities tg.Entities, update message.AnswerableMessageUpdate) {
	defer ts.jm.FinishJob(job.ID)
	job.IsTorrent = true
	job.Phase = PhaseDownloading
	job.Status = "Fetching metadata..."

	var t *torrent.Torrent
	var err error
	if magnetURI != "" {
		t, err = ts.torrentSvc.AddMagnet(magnetURI)
	} else {
		t, err = ts.torrentSvc.AddTorrentBytes(torrentBytes)
	}
	if err != nil {
		slog.Error("torrent add failed", "job_id", job.ID, "error", err)
		job.Status = fmt.Sprintf("Failed: %v", err)
		return
	}
	defer ts.torrentSvc.Drop(t)

	go func() {
		<-job.Ctx.Done()
	}()

	// Wait for metadata
	job.Status = "Resolving metadata (DHT/trackers)..."
	if err := ts.torrentSvc.WaitForInfo(job.Ctx, t); err != nil {
		slog.Error("torrent metadata failed", "job_id", job.ID, "error", err)
		job.Status = fmt.Sprintf("Failed metadata: %v", err)
		return
	}
	job.FileName = t.Name()
	job.Size = t.Length()
	tryHash := t.InfoHash().HexString()
	job.TorrentHash = tryHash
	slog.Info("torrent metadata ready", "job_id", job.ID, "name", job.FileName, "size", job.Size, "hash", tryHash)

	// Disk space check
	if job.Size > 0 {
		if err := checkDiskSpace(ts.cfg.TorrentDownloadDir, job.Size); err != nil {
			slog.Error("disk space check failed", "job_id", job.ID, "error", err)
			job.Status = fmt.Sprintf("Failed: %v", err)
			return
		}
	}

	t.DownloadAll()
	job.Status = "Downloading from swarm"

	start := time.Now()
	var lastBytes int64
	var lastTime = start
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-job.Ctx.Done():
			job.Status = "Cancelled"
			slog.Info("torrent mirror cancelled", "job_id", job.ID)
			cleanupTorrentData(ts.cfg.TorrentDownloadDir, t)
			return
		case <-ticker.C:
			completed := t.BytesCompleted()
			stats := t.Stats()
			job.ReadBytes = completed
			job.Seeds = stats.ConnectedSeeders
			job.Peers = stats.ActivePeers
			now := time.Now()
			elapsed := now.Sub(lastTime).Seconds()
			if elapsed >= 0.9 {
				delta := completed - lastBytes
				if elapsed > 0 {
					speed := float64(delta) / elapsed
					job.Speed = speed
					if speed > 0 && job.Size > completed {
						remain := job.Size - completed
						job.ETA = time.Duration(float64(remain)/speed) * time.Second
					}
				}
				lastBytes = completed
				lastTime = now
			}
			// Completion check
			if t.Complete().Bool() || (job.Size > 0 && completed >= job.Size) {
				goto done
			}
		}
	}
done:
	job.ReadBytes = t.Length()
	job.Speed = 0
	job.ETA = 0
	stats := t.Stats()
	job.Seeds = stats.ConnectedSeeders
	job.Peers = stats.ActivePeers
	slog.Info("torrent download complete", "job_id", job.ID, "name", job.FileName)

	// Upload to GDrive
	job.Phase = PhaseUploading
	job.Status = "Uploading to Google Drive"
	job.ReadBytes = 0
	job.Size = t.Length() // Total size of all files combined
	job.FileName = t.Name()

	files := t.Files()
	if len(files) == 0 {
		slog.Error("torrent has no files", "job_id", job.ID)
		job.Status = "Failed: no files in torrent"
		cleanupTorrentData(ts.cfg.TorrentDownloadDir, t)
		return
	}

	folderCache := make(map[string]string)
	resolveFolder := func(relDir string) (string, error) {
		if relDir == "" || relDir == "." {
			return ts.gdrive.folderID, nil
		}
		if cached, ok := folderCache[relDir]; ok {
			return cached, nil
		}
		parts := strings.Split(filepath.ToSlash(relDir), "/")
		currentParent := ts.gdrive.folderID
		accum := ""
		for _, part := range parts {
			if part == "" || part == "." {
				continue
			}
			if accum == "" {
				accum = part
			} else {
				accum = accum + "/" + part
			}
			if cached, ok := folderCache[accum]; ok {
				currentParent = cached
				continue
			}
			id, err := ts.gdrive.EnsureFolder(job.Ctx, part, currentParent)
			if err != nil {
				return "", err
			}
			folderCache[accum] = id
			currentParent = id
		}
		return currentParent, nil
	}

	var overallUploaded int64
	var lastURL string
	var topFolderID string

	if len(files) > 1 {
		// Multi-file torrent: create a root folder on Drive for the torrent
		var err error
		topFolderID, err = resolveFolder(t.Name())
		if err != nil {
			slog.Error("failed to create root folder for torrent", "name", t.Name(), "error", err)
			topFolderID = ts.gdrive.folderID
		}
	}

	for idx, f := range files {
		select {
		case <-job.Ctx.Done():
			job.Status = "Cancelled"
			cleanupTorrentData(ts.cfg.TorrentDownloadDir, t)
			return
		default:
		}
		diskPath := filepath.Join(ts.cfg.TorrentDownloadDir, f.Path())
		info, err := os.Stat(diskPath)
		if err != nil {
			slog.Error("torrent file missing on disk", "job_id", job.ID, "path", diskPath, "error", err)
			continue
		}
		fileSize := info.Size()
		fileName := filepath.Base(diskPath)

		// Compute parent folder in Drive
		targetFolderID := ts.gdrive.folderID
		if len(files) > 1 {
			// f.Path() is <torrent_name>/dir1/dir2/file.ext
			relFromRoot, _ := filepath.Rel(t.Name(), f.Path())
			dirPart := filepath.Dir(relFromRoot)
			if dirPart != "." && dirPart != "" {
				fullRel := filepath.Join(t.Name(), dirPart)
				parentID, fErr := resolveFolder(fullRel)
				if fErr == nil {
					targetFolderID = parentID
				} else {
					targetFolderID = topFolderID
				}
			} else {
				targetFolderID = topFolderID
			}
		}

		job.Status = fmt.Sprintf("Uploading file %d/%d: %s", idx+1, len(files), fileName)

		fh, err := os.Open(diskPath)
		if err != nil {
			slog.Error("open torrent file failed", "job_id", job.ID, "path", diskPath, "error", err)
			continue
		}
		pr2 := NewProgressReader(fh, fileSize, func(read, total int64, speed float64, eta time.Duration) {
			currentTotal := atomic.LoadInt64(&overallUploaded) + read
			job.ReadBytes = currentTotal
			job.Speed = speed
			if speed > 0 && job.Size > currentTotal {
				remain := job.Size - currentTotal
				job.ETA = time.Duration(float64(remain)/speed) * time.Second
			}
		})
		driveURL, err := ts.gdrive.UploadStreamToFolder(job.Ctx, fileName, pr2, fileSize, targetFolderID)
		_ = fh.Close()
		if err != nil {
			slog.Error("gdrive upload failed for torrent file", "job_id", job.ID, "file", fileName, "error", err)
			continue
		}
		lastURL = driveURL
		atomic.AddInt64(&overallUploaded, fileSize)
		slog.Info("torrent file uploaded", "job_id", job.ID, "file", fileName, "url", driveURL)
	}

	job.Status = "Completed"
	job.ReadBytes = job.Size
	slog.Info("torrent mirror job completed", "job_id", job.ID)
	cleanupTorrentData(ts.cfg.TorrentDownloadDir, t)

	// Send ONE final completion message for the whole torrent
	completionURL := lastURL
	if len(files) > 1 && topFolderID != "" {
		completionURL = fmt.Sprintf("https://drive.google.com/drive/folders/%s", topFolderID)
	}
	ts.sendMirrorCompletion(context.Background(), entities, update, job, completionURL)
}

func (ts *TelegramService) executeTorrentLeechJob(job *Job, magnetURI string, torrentBytes []byte, entities tg.Entities, update message.AnswerableMessageUpdate) {
	defer ts.jm.FinishJob(job.ID)
	job.IsTorrent = true
	job.Phase = PhaseDownloading
	job.Status = "Fetching metadata..."

	var t *torrent.Torrent
	var err error
	if magnetURI != "" {
		t, err = ts.torrentSvc.AddMagnet(magnetURI)
	} else {
		t, err = ts.torrentSvc.AddTorrentBytes(torrentBytes)
	}
	if err != nil {
		slog.Error("torrent add failed", "job_id", job.ID, "error", err)
		job.Status = fmt.Sprintf("Failed: %v", err)
		return
	}
	defer ts.torrentSvc.Drop(t)

	job.Status = "Resolving metadata (DHT/trackers)..."
	if err := ts.torrentSvc.WaitForInfo(job.Ctx, t); err != nil {
		slog.Error("torrent metadata failed", "job_id", job.ID, "error", err)
		job.Status = fmt.Sprintf("Failed metadata: %v", err)
		return
	}
	job.FileName = t.Name()
	job.Size = t.Length()
	job.TorrentHash = t.InfoHash().HexString()
	slog.Info("torrent leech metadata ready", "job_id", job.ID, "name", job.FileName, "size", job.Size)

	if job.Size > 0 {
		if err := checkDiskSpace(ts.cfg.TorrentDownloadDir, job.Size); err != nil {
			slog.Error("disk space check failed", "job_id", job.ID, "error", err)
			job.Status = fmt.Sprintf("Failed: %v", err)
			return
		}
	}
	t.DownloadAll()
	job.Status = "Downloading from swarm"

	start := time.Now()
	var lastBytes int64
	var lastTime = start
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-job.Ctx.Done():
			job.Status = "Cancelled"
			cleanupTorrentData(ts.cfg.TorrentDownloadDir, t)
			return
		case <-ticker.C:
			completed := t.BytesCompleted()
			stats := t.Stats()
			job.ReadBytes = completed
			job.Seeds = stats.ConnectedSeeders
			job.Peers = stats.ActivePeers
			now := time.Now()
			elapsed := now.Sub(lastTime).Seconds()
			if elapsed >= 0.9 {
				delta := completed - lastBytes
				if elapsed > 0 {
					speed := float64(delta) / elapsed
					job.Speed = speed
					if speed > 0 && job.Size > completed {
						remain := job.Size - completed
						job.ETA = time.Duration(float64(remain)/speed) * time.Second
					}
				}
				lastBytes = completed
				lastTime = now
			}
			if t.Complete().Bool() || (job.Size > 0 && completed >= job.Size) {
				goto doneLeech
			}
		}
	}
doneLeech:
	job.ReadBytes = t.Length()
	slog.Info("torrent leech download complete", "job_id", job.ID)

	job.Phase = PhaseUploading
	job.Status = "Uploading to Telegram"
	job.ReadBytes = 0
	job.Size = t.Length()
	job.FileName = t.Name()

	files := t.Files()
	if len(files) == 0 {
		job.Status = "Failed: no files in torrent"
		cleanupTorrentData(ts.cfg.TorrentDownloadDir, t)
		return
	}
	api := ts.client.API()
	var leechUploaded int64
	for idx, f := range files {
		select {
		case <-job.Ctx.Done():
			job.Status = "Cancelled"
			cleanupTorrentData(ts.cfg.TorrentDownloadDir, t)
			return
		default:
		}
		diskPath := filepath.Join(ts.cfg.TorrentDownloadDir, f.Path())
		info, err := os.Stat(diskPath)
		if err != nil {
			slog.Error("torrent file missing", "job_id", job.ID, "path", diskPath, "error", err)
			continue
		}
		fileSize := info.Size()
		if fileSize > 2*1024*1024*1024 {
			slog.Warn("file exceeds Telegram bot limit, skipping", "job_id", job.ID, "file", f.DisplayPath(), "size", fileSize)
			_, _ = ts.sender.Reply(entities, update).Text(context.Background(), fmt.Sprintf("Skipping %s (%s) — exceeds 2GB Telegram limit.", f.DisplayPath(), FormatBytes(fileSize)))
			continue
		}
		fileName := filepath.Base(diskPath)
		job.Status = fmt.Sprintf("Uploading file %d/%d to Telegram: %s", idx+1, len(files), fileName)

		fh, err := os.Open(diskPath)
		if err != nil {
			slog.Error("open torrent file failed", "job_id", job.ID, "path", diskPath, "error", err)
			continue
		}
		pr2 := NewProgressReader(fh, fileSize, func(read, total int64, speed float64, eta time.Duration) {
			currentTotal := atomic.LoadInt64(&leechUploaded) + read
			job.ReadBytes = currentTotal
			job.Speed = speed
			if speed > 0 && job.Size > currentTotal {
				remain := job.Size - currentTotal
				job.ETA = time.Duration(float64(remain)/speed) * time.Second
			}
		})
		var uploadErr error
		if fileSize > 0 {
			_, uploadErr = ts.executeLeechParallel(job, api, pr2, fileName, fileSize)
		} else {
			_, uploadErr = ts.executeLeechStream(job, api, pr2, fileName, fileSize)
		}
		_ = fh.Close()
		if uploadErr != nil {
			slog.Error("telegram upload failed for torrent file", "job_id", job.ID, "file", fileName, "error", uploadErr)
			_, _ = ts.sender.Reply(entities, update).Text(context.Background(), fmt.Sprintf("Failed uploading %s: %v", fileName, uploadErr))
			continue
		}
		atomic.AddInt64(&leechUploaded, fileSize)
		slog.Info("torrent file leeched to Telegram", "job_id", job.ID, "file", fileName)
	}
	job.Status = "Completed"
	job.ReadBytes = job.Size
	slog.Info("torrent leech job completed", "job_id", job.ID)
	cleanupTorrentData(ts.cfg.TorrentDownloadDir, t)
}

func checkDiskSpace(dir string, need int64) error {
	_ = os.MkdirAll(dir, 0755)
	p := dir
	if _, err := os.Stat(p); err != nil {
		p = "/"
	}
	return checkDiskSpaceImpl(p, need)
}

func cleanupTorrentData(baseDir string, t *torrent.Torrent) {
	if t == nil || baseDir == "" {
		return
	}
	name := t.Name()
	if name == "" {
		return
	}
	target := filepath.Join(baseDir, name)
	absBase, _ := filepath.Abs(baseDir)
	absTarget, _ := filepath.Abs(target)
	if !strings.HasPrefix(absTarget, absBase) {
		slog.Warn("cleanup safety: refusing to delete outside base", "target", absTarget, "base", absBase)
		return
	}
	if _, err := os.Stat(target); err == nil {
		if err := os.RemoveAll(target); err != nil {
			slog.Warn("cleanup torrent data failed", "path", target, "error", err)
		} else {
			slog.Info("cleaned torrent data", "path", target)
		}
	}
}
