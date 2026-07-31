# Zenith-Mirror Development Plan

## Architecture Overview

```
              ┌──────────────────────────────────────────────────────┐
              │                    Zenith-Mirror                     │
              │                                                      │
 Telegram ───►│  Command Dispatcher                                  │
              │  /mirror (/m)  /leech  /status  /stats               │
              │  /cancel <id>  /cancelall  /help                     │
              │            │                                         │
              │      Auth/ACL Check — owner/whitelist gate           │
              └───────────────┬────────────────┬───────────────┬─────┘
                              │                │               │
             ┌────────────────▼───┐        ┌───▼────────────────┐    │
             │    /mirror Pipeline│        │     /leech Pipeline│    │
             └────────┬───────────┘        └───┬────────────────┘    │
                      │                        │                    │
                      ▼                        ▼                    │
        Telegram MTProto Download       HTTP Direct Download        │
       ┌─────────────────────────┐     (`net/http` stdlib)          │
       │ Pool (remote DC) or     │     + Range-resume               │
       │ Raw Parallel Download   │                                  │
       │ (bypasses gotd/td mutex)│                                  │
       └────────┬────────────────┘                                  │
                │                                                    │
                ▼                                                    │
        Progress Tracker (atomic byte counter)                      │
                │                                                    │
        ┌───────┴───────┐                                           │
        │               │                                           │
        ▼               ▼                                           │
  Google Drive     Telegram Upload                                  │
  (OAuth2)        (`uploader.NewUploader`)                          │
        │               │                                           │
        └───────┬───────┘                                           │
                ▼                                                    │
        Job Manager: FIFO queue, max concurrency cap                │
        Status Updater: live editing, auto-delete on completion     │
```

## Current State

### Implemented Features

1. **Config & Auth**
   - JSON config with permission enforcement (`0600`)
   - Bot token authentication (no phone auth needed)
   - Owner/whitelist ACL gate on all commands

2. **Download Engine**
   - **Raw parallel download**: N goroutines independently call `UploadGetFile()` via `client.API()`, each handling `FILE_MIGRATE` auto-migration. Bypasses gotd/td's `offsetMux` mutex bottleneck → ~11MB/s achieved.
   - **Pool caching**: `client.DC(ctx, dc, threads)` creates multi-connection pool to remote DC, cached per DC for reuse across batch jobs.
   - **FLOOD_WAIT handling**: `tgerr.AsFloodWait()` with retry + backoff, max 32 retries.
   - **Chunk alignment**: Last chunk rounded to multiple of 4096 (Telegram requirement).
   - **Download modes**: `parallel` (temp file + atomicWriteAt) or `stream` (io.Pipe zero-disk).

3. **Upload**
   - Google Drive OAuth2 resumable upload
   - Telegram upload via `uploader.NewUploader` for leech

4. **Status & Progress**
   - Single in-place edited message with `□`/`■` progress bars
   - Per-job: filename, progress, processed/total, speed, ETA, `/cancel` button
   - System: Total DL/UL speeds, CPU, RAM, FREE memory, OS uptime
   - FLOOD_WAIT backoff on status edits
   - Auto-delete on completion or context cancellation
   - `lastStatusID`/`lastStatusFn` properly cleared on delete

5. **Job Management**
   - FIFO queue with configurable `max_concurrency`
   - `/cancel <job_id>` and `/cancelall`
   - Job context cancellation on cancel

6. **Completion Message**
   - `Name:` (monospace) / `Size:` / `Type:` (MIME detected from extension)
   - Inline "Index Link" copy-to-clipboard button (`KeyboardButtonCopy`)

7. **Batch Support**
   - `/mirror -i N` / `/m -i N`: mirrors N consecutive files from a message
   - Single status updater for entire batch
   - Pool reused across all jobs in batch

### Commands

| Command | Description |
|---------|-------------|
| `/mirror` / `/m` | Mirror Telegram media to Google Drive |
| `/m <url>` / `/mirror <url>` | Mirror HTTP URL to Google Drive |
| `/mirror -i N` | Mirror N consecutive files |
| `/leech <url>` | Download URL → Telegram |
| `/status` | Current transfer status |
| `/stats` | System stats |
| `/cancel <job_id>` | Cancel specific job |
| `/cancelall` | Cancel all jobs |
| `/help` | Help message |

### Key Technical Decisions

- **Why raw parallel**: gotd/td's downloader serializes all RPCs through `offsetMux` mutex on a single TCP connection. Even 16 goroutines = 1 RPC at a time. Raw `UploadGetFile` from multiple goroutines creates multiple connections → ~11MB/s.
- **Why pool caching**: `client.DC(ctx, dc, N)` does auth key transfer which fails if done concurrently by multiple jobs. Cache once, reuse.
- **Why OAuth2**: Google Service Accounts have 0 storage quota.
- **Why chunk alignment**: Telegram rejects `Limit` not multiple of 4096 with `LIMIT_INVALID`.

### Files

| File | Purpose |
|------|---------|
| `main.go` | Entry point, client setup, dispatcher |
| `config.go` | Config struct, loader, validation |
| `telegram.go` | Command handlers, status updater, completion messages |
| `raw_download.go` | Raw parallel downloader (16 goroutines, FLOOD_WAIT handling) |
| `pool_download.go` | DC detection, multi-connection pool creation |
| `gdrive.go` | OAuth2, streaming upload to Google Drive |
| `leech.go` | LeechPipeline, HTTP download |
| `url_mirror.go` | URL mirror logic, HTTP range support |
| `progress.go` | FormatBytes, FormatDuration, RenderProgressBar, NewProgressWriter |
| `job_manager.go` | Job struct, FIFO queue, concurrency control |
| `pool_download.go` | DC detection, pool creation and caching |

### Remaining Work

- [ ] Graceful shutdown (SIGTERM → cancel all jobs)
- [ ] Leech completion message with copy button
- [ ] Split-upload for files >4GB (Telegram limit)
- [ ] Resume support for interrupted uploads
- [ ] Rate limiting for status edits during high concurrency
