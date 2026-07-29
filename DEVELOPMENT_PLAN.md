# Zenith-Mirror Development Plan

## Architecture Overview

```
              ┌────────────────────────────────────────────────┐
              │                Zenith-Mirror                   │
              │                                                │
 Telegram ───►│  Command Dispatcher (/mirror, /leech, /status) │
              └───────────────┬────────────────┬───────────────┘
                              │                │
             ┌────────────────▼───┐        ┌───▼────────────────┐
             │    /mirror Pipeline│        │     /leech Pipeline│
             └────────┬───────────┘        └───┬────────────────┘
                      │                        │
                      ▼                        ▼
        Telegram MTProto Stream      HTTP Direct Stream
       (`github.com/gotd/td`)        (`net/http` stdlib)
                      │                        │
                      ▼                        ▼
             Progress Tracker         Progress Tracker
             (`io.Reader` pipe)       (`io.Reader` pipe)
                      │                        │
                      ▼                        ▼
             Google Drive API           Telegram Upload
       (`google.golang.org/api`)    (`github.com/gotd/td`)
```

---

## Phase 1: Foundation & Configuration
1. **Dependencies:**
   - `github.com/gotd/td`: High-performance native Go MTProto client (supports full 2GB+ downloads/uploads).
   - `google.golang.org/api/drive/v3`: Google Drive API v3.
2. **Config Loader:**
   - Single JSON / `.env` file for API credentials (`APP_ID`, `APP_HASH`, `BOT_TOKEN` or `PHONE_NUMBER`, `GDRIVE_SA_FILE`, `GDRIVE_FOLDER_ID`).

---

## Phase 2: Live Status & Progress Engine
1. **Zero-Disk Pipe:**
   - Implement `ProgressReader` (wraps `io.Reader`) to calculate byte rate, percentage, and ETA dynamically without storing files on local disk.
2. **Task Manager:**
   - Thread-safe in-memory map tracking active jobs.
   - Throttled Telegram editor (updates status message every 2–3s to prevent Telegram API rate limits).

---

## Phase 3: Core Commands
1. **`/mirror` (Telegram ➔ Google Drive):**
   - Intercept media message or reply.
   - Open MTProto download stream -> pipe directly into GDrive API upload call.
   - Edit status to show final Google Drive shareable link.
2. **`/leech` (URL ➔ Telegram):**
   - Parse target URL from command `/leech <url>`.
   - Open HTTP stream -> pipe directly into MTProto upload call.
   - Send uploaded media to chat with final completion stats.

---

## Phase 4: Verification & Build
1. Build static binary (`go build`).
2. Run self-checks on progress calculations.
3. Validate memory usage (<20MB idle).
