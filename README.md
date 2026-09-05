# Zenith-Mirror

Resource-efficient, fast Telegram MTProto mirror & leech bot with embedded BitTorrent and Google Drive support built in Go.

## Features

- **`/mirror`** (`/m`): Download Telegram media via MTProto (up to 2GB+) and upload to Google Drive. Supports batch with `-i N` for consecutive files.
- **`/mirror <url>`**: Mirror a direct HTTP URL to Google Drive.
- **`/mirror <magnet>`**: Download BitTorrent swarm (magnet link) and upload files to Google Drive.
- **Reply to `.torrent` with `/mirror`**: Parse uploaded `.torrent` file, fetch from BitTorrent swarm, and upload files to Google Drive.
- **`/leech <url>`**: Download direct HTTP URL and send files directly to Telegram chat.
- **`/leech <magnet>`** or **Reply to `.torrent` with `/leech`**: Download torrent swarm and send files directly to Telegram chat.
- **Embedded BitTorrent client**: 100% native Go client (`anacrolix/torrent`) with DHT, PEX, tracker support, and disk pre-checks — no external `aria2` or `libtorrent` daemon required.
- **Live status message**: Auto-updating progress bars, throughput, ETA, seeders/peers for torrents, and system stats.
- **Job queueing**: Beyond `max_concurrency`, jobs are queued. `/cancelall` to abort all.
- **Raw parallel download**: Bypasses gotd/td single-connection bottleneck — achieves ~11MB/s via multiple independent `UploadGetFile` RPCs.
- **FLOOD_WAIT handling**: Automatic retry with exponential backoff on Telegram rate limits.
- **Pool caching**: Multi-connection pools to remote DCs are cached and reused across batch jobs.
- **Copy-to-clipboard**: Completion messages include an inline button for the index link.

## Commands

| Command | Description |
|---------|-------------|
| `/mirror` or `/m` | Reply to a media message to mirror it to Google Drive |
| `/m <url>` or `/mirror <url>` | Mirror a direct HTTP link to Google Drive |
| `/m <magnet>` or `/mirror <magnet>` | Mirror a torrent magnet link to Google Drive |
| Reply to `.torrent` with `/m` or `/mirror` | Mirror files inside a `.torrent` file to Google Drive |
| `/m -i N` or `/mirror -i N` | Mirror N consecutive files from a message |
| `/leech <url>` | Download URL and send to Telegram chat |
| `/leech <magnet>` | Download torrent magnet and send files to Telegram chat |
| Reply to `.torrent` with `/leech` | Download files in `.torrent` and send to Telegram chat |
| `/status` | Show current system status and active transfer progress |
| `/stats` | Show system performance (CPU, RAM, uptime, disk) |
| `/cancel <job_id>` | Cancel a specific job |
| `/cancelall` | Cancel all active and queued jobs |
| `/help` | Show help message |

## Prerequisites

- Go 1.22+
- Telegram App API ID & API Hash ([my.telegram.org](https://my.telegram.org))
- Google Drive OAuth2 credentials (`credentials.json` + `gdrive_token.json`) or Service Account
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

   *Or download precompiled standalone binaries for Linux (`amd64`, `arm64`, `armv7`), Android (`arm64` Termux), and Windows directly from the [Releases](https://github.com/DanyTPG/Zenith-Mirror/releases/latest) page.*

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
| `torrent_download_dir` | Local scratch directory for torrent pieces | `torrent_downloads` |
| `torrent_listen_port` | Inbound BitTorrent TCP/UDP listening port (0 for random) | `0` |
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

- **BitTorrent Engine**: Integrated embedded Go client (`anacrolix/torrent`). Downloads pieces to scratch storage, exposes live swarm metrics (Peers/Seeds/Speed/ETA), uploads completed files sequentially to Google Drive or Telegram, and automatically cleans up disk pieces on completion or cancellation.
- **Raw parallel download**: Each goroutine independently calls `UploadGetFile()` with its own chunk range, bypassing gotd/td's `offsetMux` mutex that serializes RPCs. `client.API()` auto-handles `FILE_MIGRATE` for remote DCs.
- **Pool caching**: Multi-connection pools to remote DCs (`client.DC()`) are created once and reused across all jobs, avoiding concurrent auth transfer conflicts.
- **Zero-disk streaming**: `io.Pipe` between download and upload (stream mode).
- **Parallel mode**: Downloads to temp file with `atomicWriteAt`, then uploads. Faster for large files.
- **Status**: Single in-place edited message with progress bars, throughput, ETA, peer counters, and `/cancel` button, auto-deleted on completion.

## License

Private — not for redistribution.
