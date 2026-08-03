# Zenith-Mirror

Resource-efficient, fast Telegram MTProto mirror & leech stream bot built in Go.

## Features

- **`/mirror`** (`/m`): Download Telegram media via MTProto (up to 2GB+) and upload to Google Drive. Supports batch with `-i N` for consecutive files.
- **`/m <url>`**: Mirror a direct HTTP URL to Google Drive.
- **`/leech`**: Download via HTTP URL and send to Telegram chat.
- **Live status message**: Auto-updating progress bars, throughput, ETA, and system stats.
- **Job queueing**: Beyond `max_concurrency`, jobs are queued. `/cancel-all` to abort all.
- **Raw parallel download**: Bypasses gotd/td single-connection bottleneck — achieves ~11MB/s via multiple independent `UploadGetFile` RPCs.
- **FLOOD_WAIT handling**: Automatic retry with backoff on Telegram rate limits.
- **Pool caching**: Multi-connection pools to remote DCs are cached and reused across batch jobs.
- **Copy-to-clipboard**: Completion messages include an inline button for the index link.

## Commands

| Command | Description |
|---------|-------------|
| `/mirror` or `/m` | Reply to a media message to mirror it to Google Drive |
| `/m <url>` or `/mirror <url>` | Mirror a direct URL to Google Drive |
| `/m -i N` or `/mirror -i N` | Mirror N consecutive files from a message |
| `/leech <url>` | Download URL and send to Telegram chat |
| `/status` | Show current system status |
| `/stats` | Show system stats (CPU, RAM, uptime) |
| `/cancel <job_id>` | Cancel a specific job |
| `/cancelall` | Cancel all active and queued jobs |
| `/help` | Show help message |

## Prerequisites

- Go 1.22+
- Telegram App API ID & API Hash ([my.telegram.org](https://my.telegram.org))
- Google Drive OAuth2 credentials (`credentials.json` + `gdrive_token.json`)
- Bot token from [@BotFather](https://t.me/BotFather)

## Quick Start

1. Clone the repo:
   ```bash
   git clone git@github.com:DanyTPG/Zenith-Mirror.git
   cd Zenith-Mirror
   ```

2. Copy and edit the config:
   ```bash
   cp example.config.json config.json
   # Edit config.json with your credentials
   ```

3. Build and run:
   ```bash
   go build -o zenith-mirror .
   ./zenith-mirror -config config.json
   ```

## Configuration

See `example.config.json` for all available options.

| Field | Description | Default |
|-------|-------------|---------|
| `app_id` | Telegram API ID | *required* |
| `app_hash` | Telegram API Hash | *required* |
| `bot_token` | Bot token from BotFather | *required* |
| `gdrive_credentials_file` | OAuth2 client credentials JSON | `credentials.json` |
| `gdrive_token_file` | OAuth2 refresh token JSON | `gdrive_token.json` |
| `gdrive_folder_id` | Google Drive folder ID for uploads | - |
| `index_base_url` | Base URL for direct file access links | - |
| `download_mode` | `parallel` (fast, temp file) or `stream` (zero-disk) | `stream` |
| `download_threads` | Threads for parallel download | `4` |
| `part_size` | Chunk size in bytes (must be multiple of 4096) | `524288` |
| `max_concurrent_downloads` | Global cap on simultaneous file downloads | `4` |
| `rpc_burst` | Rate limiter token bucket burst | `5` |
| `rpc_rate_per_sec` | Rate limiter sustained RPCs/sec | `10` |
| `owner_id` | Telegram user ID of the bot owner | - |
| `allowed_user_ids` | Additional authorized user IDs | - |
| `max_concurrency` | Max simultaneous transfers | `3` |
| `status_refresh_delay_sec` | Status message update interval (seconds) | `5` |

## Docker

```bash
docker build -t zenith-mirror .
docker run -d \
  -v $(pwd)/config.json:/app/config.json \
  -v $(pwd)/credentials.json:/app/credentials.json \
  -v $(pwd)/gdrive_token.json:/app/gdrive_token.json \
  --name zenith-mirror \
  zenith-mirror
```

## Architecture

- **Raw parallel download**: Each goroutine independently calls `UploadGetFile()` with its own chunk range, bypassing gotd/td's `offsetMux` mutex that serializes RPCs. `client.API()` auto-handles `FILE_MIGRATE` for remote DCs.
- **Pool caching**: Multi-connection pools to remote DCs (`client.DC()`) are created once and reused across all jobs, avoiding concurrent auth transfer conflicts.
- **Zero-disk streaming**: `io.Pipe` between download and upload (stream mode).
- **Parallel mode**: Downloads to temp file with `atomicWriteAt`, then uploads. Faster for large files.
- **Status**: Single in-place edited message with `□`/`■` progress bars, auto-deleted on completion.

## License

Private — not for redistribution.
