# Zenith-Mirror Development Plan (Revised)

Changes from the original plan are marked **[NEW]**. Everything else is carried over unchanged.

## Architecture Overview

```
              ┌──────────────────────────────────────────────────────┐
              │                    Zenith-Mirror                     │
              │                                                      │
 Telegram ───►│  Command Dispatcher (/mirror, /leech, /status,       │
              │                      /cancel [NEW])                  │
              │            │                                         │
              │      Auth/ACL Check [NEW] — owner/whitelist gate     │
              └───────────────┬────────────────┬───────────────┬─────┘
                              │                │               │
             ┌────────────────▼───┐        ┌───▼────────────────┐    │
             │    /mirror Pipeline│        │     /leech Pipeline│    │
             └────────┬───────────┘        └───┬────────────────┘    │
                      │                        │                    │
                      ▼                        ▼                    │
        Telegram MTProto Stream      HTTP Direct Stream              │
       (`github.com/gotd/td`)        (`net/http` stdlib)             │
       + FloodWait middleware [NEW]  + UA/Referer, redirects,        │
                      │              Range-resume [NEW]              │
                      ▼                        ▼                    │
             Progress Tracker         Progress Tracker               │
        (`io.Reader` pipe, chunk-     (`io.Reader` pipe)              │
         aligned to Drive's 256KiB    │                              │
         boundary [NEW])              │                              │
                      │                        │                    │
                      ▼                        ▼                    │
             Google Drive API           Telegram Upload              │
      (resumable session [NEW])     (`github.com/gotd/td`,          │
       google.golang.org/api        split-upload if >limit [NEW])   │
                      │                        │                    │
                      └────────────┬───────────┘                    │
                                   ▼                                 │
                    Job Manager: context.Context per job,            │
                    max concurrency cap, cleanup on exit [NEW]  ◄────┘
```

---

## Phase 1: Foundation & Configuration

1. **Dependencies:**
   - `github.com/gotd/td`: native Go MTProto client (full 2GB+ down/upload support).
   - `google.golang.org/api/drive/v3`: Google Drive API v3.
   - **[NEW]** `github.com/gotd/td/telegram/auth/floodwait`: flood-wait middleware.

2. **Config Loader:**
   - Single JSON / `.env` file for API credentials (`APP_ID`, `APP_HASH`, `BOT_TOKEN` or `PHONE_NUMBER`, `GDRIVE_SA_FILE`, `GDRIVE_FOLDER_ID`).
   - **[NEW]** `OWNER_ID` / `ALLOWED_USER_IDS` — whitelist for who may invoke commands.
   - **[NEW]** Enforce `0600` permissions on the config and service-account files at startup; refuse to run (or warn loudly) if they're world-readable. Never commit these to the repo.

3. **[NEW] Session Persistence:**
   - Since large-file transfers require a logged-in *user* session (not just a bot token), implement first-run phone/code/2FA login and persist the session via `session.FileStorage` (or a small SQLite-backed storage) so the process doesn't need re-auth on every restart.

4. **[NEW] Auth Middleware:**
   - Wire `floodwait.NewSimpleWaiter()` (or equivalent) into the MTProto client from the start — sustained transfer load will trigger `FLOOD_WAIT_X` errors, and this needs to be handled transparently rather than surfaced as a crash.

---

## Phase 2: Live Status & Progress Engine

1. **Zero-Disk Pipe:**
   - Implement `ProgressReader` (wraps `io.Reader`) to calculate byte rate, percentage, and ETA dynamically without storing files on local disk.
   - **[NEW]** Align pipe chunk sizes to Google Drive's resumable-upload requirement (multiples of 256KiB, except the final chunk) so streaming and resumability both work correctly.

2. **Task Manager:**
   - Thread-safe in-memory map tracking active jobs (`sync.Map` or mutex-guarded map).
   - Throttled Telegram editor (updates status message every 2–3s to prevent Telegram API rate limits); skip redundant edits when content hasn't changed.
   - **[NEW]** Each job carries a `context.Context` / `context.CancelFunc` pair, enabling both user-initiated `/cancel` and process-wide graceful shutdown (SIGTERM aborts in-flight jobs cleanly instead of leaving orphaned Drive upload sessions).
   - **[NEW]** Cap maximum concurrent jobs (configurable) to avoid saturating host bandwidth or triggering IP-level flood bans.
   - **[NEW]** Remove completed/failed/cancelled jobs from the map promptly — an unbounded map is a slow memory leak.

---

## Phase 3: Core Commands

1. **`/mirror` (Telegram ➔ Google Drive):**
   - Intercept media message or reply.
   - Open MTProto download stream → pipe directly into GDrive API upload call.
   - **[NEW]** Use Drive's resumable upload session explicitly (not simple upload), so a dropped connection mid-transfer can resume instead of restarting from byte 0.
   - Edit status to show final Google Drive shareable link.

2. **`/leech` (URL ➔ Telegram):**
   - Parse target URL from command `/leech <url>`.
   - Open HTTP stream → pipe directly into MTProto upload call.
   - **[NEW]** Set a custom `User-Agent`/`Referer` (many direct-download hosts reject bare requests) and follow redirects with a sane cap.
   - **[NEW]** Support HTTP `Range` requests to resume a partially-downloaded source file after a dropped connection.
   - **[NEW]** Split-upload logic for source files exceeding the Telegram upload limit (2GB standard / 4GB Premium) — split into parts or fail with a clear error message rather than silently truncating.
   - Send uploaded media to chat with final completion stats.

3. **`/status`:**
   - List active jobs with progress from the Task Manager (as originally planned).

4. **[NEW] `/cancel <job_id>`:**
   - Cancel an in-flight job via its context; clean up any partial Drive upload session or Telegram upload state.

5. **[NEW] Authorization:**
   - Gate all commands behind the owner/whitelist check from Phase 1 config — without this, the bot is an open relay for your Drive quota and bandwidth to anyone who finds it.

---

## Phase 4: Verification & Build

1. Build static binary (`go build`).
2. Run self-checks on progress calculations.
   - **[NEW]** Unit tests specifically for `ProgressReader`'s byte-rate/ETA math (zero-bytes-read first tick, division-by-zero edge cases).
3. Validate memory usage (<20MB idle).
4. **[NEW]** Structured logging (`log/slog` stdlib) — needed immediately once flood-wait, quota, or network errors start showing up in real use.
5. **[NEW]** Dockerfile for containerized deployment, consistent with the rest of the self-hosted stack.
6. **[NEW]** Graceful shutdown: SIGTERM triggers context cancellation across all active jobs before process exit.

---

## Recommended Development Order
1. Configuration & validation (`config.go`)
2. Telegram authentication / session persistence
3. Task manager & cancellation contexts
4. Progress engine (`ProgressReader` + ETA math + tests)
5. `/mirror` command (MTProto -> Drive resumable pipe)
6. `/leech` command (HTTP -> MTProto pipe)
7. Logging (`log/slog`), graceful shutdown, Dockerfile
