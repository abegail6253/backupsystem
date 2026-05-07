# BackupSys

A Windows desktop backup system with a PyQt6 tray app, incremental backups, encryption, compression, and multi-destination support (local, SFTP, FTP/FTPS, SMB/CIFS, HTTPS API, WebDAV/Nextcloud/ownCloud, Google Drive, and rclone).

---

## Requirements

- Python 3.11+
- Windows 10/11 (Linux/macOS partially supported — no system tray icon available, so the app runs in window mode: main window stays open, closing it minimizes to taskbar instead of exiting, and a quit button is available in the UI)

## Installation

**Option A — Guided setup (recommended for first-time installs):**
```bash
python setup_wizard.py
```
The wizard checks your Python version, installs all requirements, creates a starter `config.json`, and optionally adds BackupSys to Windows startup.

**Option B — Manual:**
```bash
pip install -r requirements_desktop.txt
```

## Running

```bash
python desktop_app.py
```

A tray icon appears in the system notification area. Right-click it to open the dashboard or settings.

## Building a standalone .exe

```bash
pip install pyinstaller>=6.0.0
python build_exe.py
```

Output: `dist/BackupSystem/BackupSystem.exe` — copy the entire `dist/BackupSystem/` folder to any Windows PC.

---

## Project Structure

| File | Purpose |
|------|---------|
| `desktop_app.py` | PyQt6 tray app — UI, scheduler, backup worker threads |
| `backup_engine.py` | Core backup logic — snapshot, diff, copy, encrypt, compress, restore, validate |
| `config_manager.py` | Load/save `config.json`, watch CRUD, snapshot & queue persistence |
| `watcher.py` | File-system watching via watchdog (polling fallback for network shares) |
| `transport_utils.py` | Remote upload helpers — SFTP, FTP/FTPS, SMB, HTTPS, WebDAV/Nextcloud/ownCloud, rclone; upload + download for all destinations |
| `notification_utils.py` | Email (SMTP/STARTTLS/SSL) and webhook notification helpers |
| `credential_store.py` | OS keyring wrapper for SFTP/FTP/SMB/SMTP passwords (falls back to config.json) |
| `integrity_scheduler.py` | Weekly background backup integrity checker |
| `backupsys_cli.py` | Headless CLI — run backups, list watches, validate, keygen (no GUI required) |
| `setup_wizard.py` | Guided first-run setup — installs deps, writes config, adds to startup |
| `build_exe.py` | PyInstaller packaging script |
| `scripts/create_release_zip.py` | Create release ZIP with config template and installer |
| `scripts/` | Build and release scripts |
| `tests/` | pytest unit tests — run with `pytest tests/` |
| `config.json` | Runtime configuration (auto-generated on first run) |
| `CHANGELOG.md` | Version history |

---

## Configuration

Settings are stored in `config.json` next to the `.py` files (or in `BACKUPSYS_DATA_DIR` if set).

Key global settings:

| Key | Default | Description |
|-----|---------|-------------|
| `destination` | `./backups` | Local path where backup folders are written |
| `dest_type` | `local` | `local`, `sftp`, `ftp`, `ftps`, `smb`, `https`, `webdav`, `rclone`, `cloud` (Google Drive) |
| `auto_backup` | `false` | Enable timed automatic backups |
| `interval_min` | `30` | Minutes between auto-backups |
| `retention_days` | `30` | Delete backups older than N days |
| `compression_enabled` | `false` / `0` | Compression level: `false` or `0` = off, `1` = fast (zlib level 1), `6` = balanced (default when enabled via UI), `9` = maximum compression. Integer levels can be set directly in `config.json` or via the CLI/API. |
| `max_backup_mbps` | `0` | Throttle backup I/O (0 = unlimited) |
| `idle_threshold_cpu` | `0` | Defer auto-backups while CPU% exceeds this value (0 = always run, requires psutil) |
| `pause_on_metered` | `false` | Pause auto-backups while on a metered network connection (Windows only; also settable via `BACKUPSYS_PAUSE_ON_METERED=1`) |
| `backup_schedule_times` | `[]` | List of `{"time": "HH:MM", "days": <bitmask>}` entries — see **Scheduled backups** below |
| `backup_window_start` | `""` | Earliest time (HH:MM) that auto-backups may run. Leave blank to disable the window restriction. |
| `backup_window_end` | `""` | Latest time (HH:MM) that auto-backups may run. Backups that start within the window may complete past it. Both `backup_window_start` and `backup_window_end` must be set to enable windowing. Configurable in **Settings → General → Backup window**. |
| `force_full_interval_days` | `0` | Force a full (non-incremental) backup every N days across all watches. `0` = disabled. Per-watch overrides take priority — see per-watch fields below. |

#### Compression levels

The `compression_enabled` field accepts both a boolean and an integer compression level:

| Value | Meaning |
|-------|---------|
| `false` / `0` | No compression (default) |
| `true` / `6` | Balanced — good size reduction, moderate CPU (default when toggled on in the UI) |
| `1` | Fast — minimal CPU overhead, ~10–20% smaller files |
| `9` | Maximum — best compression ratio, highest CPU cost |

Set the level directly in `config.json` or per-watch in the `watches` array:

```json
{
  "compression_enabled": 1,
  "watches": [
    { "name": "Heavy Data", "compression": 9, ... },
    { "name": "Logs", "compression": 1, ... }
  ]
}
```

The global `compression_enabled` value is the fallback when a watch's `compression` field is `false` or `0`.

### Scheduled backups

`backup_schedule_times` (global) and `schedule_times` (per-watch) are lists of scheduled fire-times with optional day-of-week filtering.

Each entry is a dict:

```json
{ "time": "02:00", "days": 31 }
```

`days` is a **7-bit bitmask**: bit 0 = Monday, bit 1 = Tuesday … bit 6 = Sunday.  
Common values: `127` = every day (default), `31` = Mon–Fri (weekdays), `96` = Sat–Sun (weekends).

The UI exposes this as a row of **Mon / Tue / Wed / Thu / Fri / Sat / Sun** checkboxes alongside each HH:MM entry in **Settings → Run at times** and **Edit Watch → Schedule times**.

Plain `"HH:MM"` strings written by older versions of BackupSys are still accepted and treated as `days: 127` (every day).

### Cross-watch file search

Open **History → 🔍 File Search** to search for any filename across every watch's backup snapshots at once.  Type a partial filename or path fragment, optionally filter by a single watch, and press **Search**.  Results show the watch name, backup date, full relative path, file size, and the backup directory — sorted by any column.  The search scans all local MANIFEST.json files (and any per-watch destination folders) in one pass.



### Config Import

Import a previously exported config file via **Settings → Config → Import Config**. This merges the imported settings into your current config without overwriting existing watches or credentials.

### Remote Destinations

Configure under `dest_sftp`, `dest_ftp`, `dest_smb`, or `dest_https` in `config.json`, or via the Settings UI.

**SFTP**
```json
"dest_sftp": { "host": "192.168.1.10", "port": 22, "username": "user", "password": "pass", "remote_path": "/backups" }
```

**FTP/FTPS**
```json
"dest_ftp": { "host": "ftp.example.com", "port": 21, "username": "user", "password": "pass", "use_tls": true }
```

**SMB**
```json
"dest_smb": { "server": "nas", "share": "backups", "username": "user", "password": "pass", "remote_path": "" }
```

**HTTPS API**
```json
"dest_https": { "url": "https://api.example.com/backup", "token": "Bearer xxx", "verify_ssl": true }
```


**WebDAV / Nextcloud / ownCloud**
```json
"dest_webdav": {
  "url": "https://nextcloud.example.com",
  "username": "user",
  "webdav_root": "/remote.php/dav/files/user/",
  "remote_path": "/backups",
  "verify_ssl": true
}
```

Store the password via the Settings UI (saved to OS keyring) — never paste it in `config.json`.

> **Nextcloud DAV root:** `/remote.php/dav/files/<USERNAME>/`  
> **ownCloud DAV root:** `/remote.php/webdav/`  
> **OneDrive DAV root:** `https://d.docs.live.net/<CID>/` where CID is your OneDrive CID (visible at onedrive.live.com)  
> **Plain WebDAV:** leave `webdav_root` empty.

Example OneDrive config:
```json
"dest_webdav": {
  "url": "https://d.docs.live.net/1234567890abcdef",
  "username": "user@example.com",
  "webdav_root": "",
  "remote_path": "/backups",
  "verify_ssl": true
}
```

Install `webdavclient3` for the best Nextcloud compatibility:
```bash
pip install webdavclient3
```
Without it, BackupSys falls back to the built-in `urllib` client automatically.

All remote destinations support live connection testing via the Settings UI "Test Connection" buttons.

**rclone**

Requires [rclone](https://rclone.org/downloads/) installed and a named remote already configured on your system (`rclone config`). BackupSys calls rclone as a subprocess — no extra Python packages needed.

```json
"dest_rclone": {
  "remote": "myremote",
  "path": "/backups"
}
```

Set `dest_type` to `rclone` and the remote name to any remote shown by `rclone listremotes`. rclone supports 70+ providers including NAS, SFTP, Storj, Mega, pCloud, and others — configure the remote once in rclone, then point BackupSys at it.

> **Retention:** BackupSys retention policies (retention_days, max_backups) are applied to rclone destinations as of v1.1.8. Older remote backups are pruned automatically after each successful backup, the same as for local destinations.

**Google Drive**

Connect via **Settings → Cloud → Connect Google Drive**. The OAuth flow opens your browser; tokens are saved to `.user_cloud_tokens.json` on your PC — not in `config.json`.

Set `dest_type` to `cloud` to activate Google Drive uploads. The folder can be chosen during the connection flow or changed later in Settings → Cloud.

```json
"dest_cloud": {
  "provider": "gdrive",
  "folder_id": "",
  "folder_name": "My Drive (root)"
}
```

> `folder_id` and `folder_name` are written automatically by the connection flow. Do not edit them manually.

---

## Environment Variables

Copy `.env.example` to `.env` and fill in your values.

> ⚠ **Never commit `.env` to Git.** It's already in `.gitignore`. If credentials were accidentally shared, regenerate them immediately at [Google Cloud Console](https://console.cloud.google.com/apis/credentials).

| Variable | Purpose |
|----------|---------|
| `BACKUPSYS_DATA_DIR` | Override data directory for config, snapshots, logs |
| `BACKUPSYS_EMAIL_PASSWORD` | SMTP password (avoids storing it in `config.json`) |
| `BACKUPSYS_ENCRYPT_KEY_<WATCH_ID>` | Per-watch Fernet encryption key |
| `BACKUPSYS_ENCRYPT_KEY_DEFAULT` | Fallback key for watches without an explicit key |
| `GDRIVE_CLIENT_ID` | Google Drive OAuth client ID |
| `GDRIVE_CLIENT_SECRET` | Google Drive OAuth client secret |
| `BACKUPSYS_WEBHOOK_URL` | Webhook URL override (avoids storing it in `config.json`) |
| `BACKUPSYS_PAUSE_ON_METERED` | Set to `1` to pause auto-backups on metered connections |
| `BACKUPSYS_PORTABLE` | Set to `1` for portable mode (same as creating `portable.flag`) |

Generate an encryption key:
```bash
python -c "from backup_engine import generate_encryption_key; print(generate_encryption_key())"
```

---

## Email Notifications

Configure under **Settings → Email Notifications** or directly in `config.json`:

```json
"email_config": {
  "enabled": true,
  "smtp_host": "smtp.gmail.com",
  "smtp_port": 587,
  "smtp_use_ssl": false,
  "username": "you@gmail.com",
  "password": "",
  "from_addr": "you@gmail.com",
  "to_addr": "alerts@example.com",
  "notify_on_success": false,
  "notify_on_failure": true
}
```

For Gmail, use an [App Password](https://support.google.com/accounts/answer/185833) and store it via `BACKUPSYS_EMAIL_PASSWORD` instead of in `config.json`.

---

## Webhooks

Set `webhook_url` to any endpoint that accepts a POST with `Content-Type: application/json`. Compatible with Slack, Discord, n8n, Zapier, Make, and custom REST APIs.

`webhook_on_success: false` (default) — only failed backups trigger the webhook. Set to `true` to also notify on success.

---

## Push Notifications (ntfy.sh)

BackupSys can send instant push notifications to your phone via [ntfy.sh](https://ntfy.sh) (free, open-source) or any self-hosted ntfy server.

**Setup:**
1. Install the [ntfy app](https://ntfy.sh) on iOS or Android.
2. Subscribe to a topic name of your choice (e.g. `my-backupsys-alerts`).  
   Use a hard-to-guess topic name as a lightweight access control measure.
3. Configure under **Settings → Notifications → Push Notifications** or in `config.json`:

```json
"ntfy_config": {
  "enabled": true,
  "server": "https://ntfy.sh",
  "topic": "my-backupsys-alerts",
  "token": "",
  "priority": "default",
  "notify_on_success": false,
  "notify_on_failure": true
}
```

| Field | Default | Description |
|-------|---------|-------------|
| `server` | `https://ntfy.sh` | ntfy server URL. Set to your self-hosted instance if applicable. |
| `topic` | `""` | Topic name to publish to. Required. |
| `token` | `""` | Bearer auth token for [protected topics](https://docs.ntfy.sh/publish/#access-tokens). Leave empty for public topics. |
| `priority` | `"default"` | Message priority: `min`, `low`, `default`, `high`, `urgent`. Failures default to `high`. |
| `notify_on_success` | `false` | Also send a push on successful backups. |
| `notify_on_failure` | `true` | Send a push when a backup fails (default on). |

Failure notifications are sent at `high` priority regardless of the configured priority so they break through Do Not Disturb on supported devices.



Passwords for SFTP, FTP, SMB, and SMTP are sensitive.  By default BackupSys
falls back to storing them in `config.json`, but the recommended approach is
to use the OS-native credential vault via the `keyring` package:

```bash
pip install keyring
```

Once installed, passwords saved through the Settings UI are stored in:
- **Windows** — Windows Credential Manager
- **macOS** — macOS Keychain
- **Linux** — SecretService (GNOME Keyring / KWallet)

If `keyring` is not installed the app works exactly as before — passwords are
read from `config.json` and no errors are raised.

---


---

## CLI / Headless Mode

`backupsys_cli.py` lets you run backups without a display, GUI, or running Qt application.
Useful for servers, WSL, SSH sessions, and Windows Task Scheduler.

```bash
# List all configured watches
python backupsys_cli.py list

# Back up a specific watch by name or ID
python backupsys_cli.py backup --watch "My Documents"
python backupsys_cli.py backup --watch w_abc123

# Back up ALL active watches (exit code 1 if any fail with --strict)
python backupsys_cli.py backup --all --strict

# Show last 20 backup results
python backupsys_cli.py history --limit 20

# Validate the most-recent backup for a watch
python backupsys_cli.py validate --watch "My Documents"
python backupsys_cli.py validate --all

# Restore the latest backup to a folder
python backupsys_cli.py restore --watch "My Documents" --target C:\Restored
python backupsys_cli.py restore --watch "My Documents" --target C:\Restored --backup-id bk_abc123
python backupsys_cli.py restore --watch "My Documents" --target C:\Restored --full-chain
python backupsys_cli.py restore --watch "My Documents" --target C:\Restored --no-overwrite

# Generate a new encryption key
python backupsys_cli.py keygen

# Rotate the encryption key for an existing backup directory
# (re-encrypts every .enc file in-place; updates the manifest)
python backupsys_cli.py rotate-key --backup-dir /path/to/backup/dir
python backupsys_cli.py rotate-key --backup-dir /path/to/backup/dir --yes  # skip confirmation

# Print active config (secrets redacted)
python backupsys_cli.py config

# Dry run — preview what would be backed up without copying anything
python backupsys_cli.py dry-run --watch "My Documents"
python backupsys_cli.py dry-run --all --verbose
```

All destination types (SFTP, FTP, HTTPS, WebDAV, Google Drive) are supported via the same `config.json` the GUI uses.

### Per-watch advanced fields (set via Settings → Edit Watch)

| Field | Default | Description |
|-------|---------|-------------|
| `max_backups` | `0` | Keep only this many versioned backups (0 = unlimited) |
| `max_file_size_mb` | `0` | Skip individual files larger than N MB (0 = no limit) |
| `max_backup_bytes` | `0` | Refuse new backups if this watch already uses more than N bytes of storage (0 = no limit) |
| `retention_days` | `0` | Override global retention for this watch |
| `interval_min` | `0` | Override global backup interval for this watch |
| `skip_auto_backup` | `false` | Exclude from scheduled auto-backups (manual only) |
| `compression` | `0` | Per-watch compression level: `0` = use global, `1` = fast, `6` = balanced, `9` = maximum. See [Compression levels](#compression-levels) above. |
| `force_full_interval_days` | `0` | Force a full backup every N days for this watch specifically. `0` = inherit global setting. `-1` = exempt this watch even when the global setting is active. |
| `drive_trigger_label` | `""` | Volume label of a USB / external drive that should trigger this watch automatically when connected (case-insensitive). Leave empty to disable. |
| `drive_trigger_serial` | `""` | Windows volume serial (8-char hex, e.g. `ABCD1234`) of a drive that triggers this watch. Either label **or** serial match fires the backup. Find the serial with `vol D:` in CMD. |

#### Drive trigger quick-start

1. Open **Settings → Watches → Edit Watch**.
2. Fill in **Drive trigger (label)** with the volume label of your backup drive (e.g. `MY_BACKUP`).  
   Find the label in Explorer — it appears under the drive icon — or run `vol D:` in CMD.
3. Save. Plug in the drive at any time; BackupSys will start the backup automatically within 3 seconds.

Use **serial** instead of label when two drives have the same label, or when the label might change:

```json
{ "drive_trigger_serial": "A1B2C3D4" }
```

Both fields can be set simultaneously; either match fires the backup.

---

## Portable Mode

To run BackupSys entirely from a single self-contained folder (e.g. a USB drive):

1. Create an empty file named `portable.flag` next to `desktop_app.py`.
2. Restart BackupSys.

All config, snapshots, logs, and the backup queue will be stored inside the app folder instead of using `BACKUPSYS_DATA_DIR`.  To disable, delete `portable.flag` and restart.

You can also set `BACKUPSYS_PORTABLE=1` as an environment variable for the same effect.

---

## Encryption

Per-watch AES-256-GCM streaming encryption (upgraded from Fernet in v1.1.0).
Files of **any size** are encrypted in 1 MB chunks with ~2 MB constant RAM overhead — the old 200 MB limit is gone.

The same 44-character key format is used as before; existing keys continue to work.
Old Fernet-encrypted backups from BackupSys v1.0.x are decrypted automatically — no migration needed.

Set `encrypt_key` on a watch in the Settings UI, or supply it via `BACKUPSYS_ENCRYPT_KEY_<ID>`.

Generate a key:
```bash
python backupsys_cli.py keygen
# or
python -c "from backup_engine import generate_encryption_key; print(generate_encryption_key())"
```

> **Legacy note (v1.0.x):** Earlier versions used Fernet (AES-128-CBC + HMAC-SHA256), which loaded entire files into RAM and had a 200 MB hard limit. The current engine uses streaming AES-256-GCM with no file-size limit. Existing Fernet-encrypted backups are read transparently — no action required.

---

## Restore

From the backup history UI, select a backup and click **Restore**. For incremental backups, use **Restore Full Chain** to replay all snapshots in order up to a chosen point in time.

## Running Tests

```bash
pip install pytest
pytest tests/
```

The test suite covers `backup_engine`, `config_manager`, and
`credential_store`.  Tests are self-contained and use temporary directories —
no real files, servers, or email accounts are needed.

---

## Security

### API CORS

CORS is **disabled by default** — no `Access-Control-Allow-Origin` headers are sent and a warning is logged on startup. You must opt in explicitly via the `ALLOWED_ORIGINS` environment variable:

```
ALLOWED_ORIGINS=*                                         # open wildcard
ALLOWED_ORIGINS=https://dashboard.example.com,http://localhost:3000  # restricted
```

**Rationale:** every mutating request requires a valid `X-BackupSys-Signature` HMAC-SHA256 header, so CORS is not the primary enforcement layer. However, silently defaulting to `*` is a footgun for users who expose the API on a public IP without realising it. Requiring an explicit opt-in surfaces the choice rather than hiding it.

The variable accepts a comma-separated list; only the exact origins listed will receive `Access-Control-Allow-Origin` headers.

### HMAC signature replay protection

The `X-BackupSys-Signature` header authenticates the request body using HMAC-SHA256. Every request must also include an `X-BackupSys-Timestamp` header (Unix epoch float); the server rejects requests whose timestamp deviates from server time by more than ±60 seconds, limiting the replay window to that narrow gap.

> ⚠️ **Public-endpoint warning:** if you deploy `backupsys_api.py` on a publicly reachable host (Railway, Render, Fly.io, a VPS, etc.) you **must** treat replay protection as an active concern, not a theoretical one. Any request logged in transit — by a proxy, CDN, or load balancer — can be re-submitted verbatim by anyone who reads those logs.

**Threat model:** for a private backup tool running on a trusted LAN this risk is low — an attacker would first need to intercept traffic. If you expose the API over the internet, apply all of the following:

- **TLS everywhere** — prevents interception in the first place (enforced by Railway/Render/Fly.io automatically)
- **Rotate `BACKUPSYS_API_KEY` regularly** — limits the replay window; treat it like a password
- **Network-layer controls** — firewall allowlist, VPN, or Cloudflare Access in front of the API
- **Keep request logs private** — do not expose raw access logs to untrusted parties
- **`X-BackupSys-Timestamp` window check** — every authenticated request must include a `X-BackupSys-Timestamp` header containing the current Unix epoch (float). The server rejects any request whose timestamp deviates from server time by more than ±60 seconds, preventing captured requests from being replayed outside that window.
- **Set `BACKUPSYS_TRUSTED_PROXY=true` when behind a reverse proxy** — Railway, Render, Fly.io, nginx, and similar platforms forward the real client IP in the `X-Forwarded-For` header. Enable this flag so the rate limiter sees individual client IPs rather than the proxy's address. Leave it unset (the default) for direct deployments — trusting `X-Forwarded-For` unconditionally allows IP spoofing.

### Rate limiter — single-worker limitation

> ⚠️ **The built-in rate limiter is single-worker only.** It stores state in process memory (`_event_rl_store`). If you run more than one gunicorn worker (`--workers N`) or scale to multiple instances (Railway autoscale, Render horizontal scale, etc.), each worker enforces the limit independently. The effective limit becomes **workers × EVENT_RATE_LIMIT**, silently multiplying how many requests a client can make.

The Procfile defaults to `--workers ${WEB_CONCURRENCY:-1}` for this reason. **Do not raise `WEB_CONCURRENCY` above 1 without replacing the in-process store.**

To support multiple workers or instances, swap the `store: dict` parameter in `_sliding_window_allow()` for a Redis-backed alternative:

```python
# pip install redis
import redis
r = redis.Redis.from_url(os.environ["REDIS_URL"])

def _sliding_window_allow_redis(key: str, limit: int, window_sec: int) -> bool:
    pipe = r.pipeline()
    now = time.time()
    cutoff = now - window_sec
    pipe.zremrangebyscore(key, "-inf", cutoff)
    pipe.zadd(key, {str(now): now})
    pipe.zcard(key)
    pipe.expire(key, window_sec + 1)
    _, _, count, _ = pipe.execute()
    return count <= limit
```

Replace the two `_sliding_window_allow(...)` calls in `_event_rate_limit_check()` with calls to `_sliding_window_allow_redis()` and remove the `_event_rl_lock` / `_event_rl_store` globals.

### Encryption key rotation

If you suspect an encryption key has been compromised, rotate it without reinstalling:

```bash
python backupsys_cli.py rotate-key --backup-dir /path/to/affected/backup/dir
```

This re-encrypts every `.enc` file in the directory with a new key and updates the manifest. Keep a safe copy of both keys until you have verified that the rotated backups restore correctly. The `rotate_encryption_key()` function is also importable from `backup_engine.py` for scripted rotation.

## Known Limitations

### Server-side restore is not implemented

`POST /restore` returns HTTP 501 in this version.  Server-side (headless) restore is **not yet available** from the API.

**Restore still works** via the desktop app and CLI — these enumerate files with `GET /manifest?backup_dir=<dir>`, download each one with `GET /files/<path>`, and reconstruct the backup locally.  From the CLI:

```bash
python backupsys_cli.py restore --watch "My Documents" --target C:\Restored
```

A future release will add a proper `POST /restore` endpoint that queues a restore command for the desktop agent, enabling fully headless / remote-triggered restores.  See the comment block above the `/restore` route in `backupsys_api.py` for the planned contract.

### In-process rate limiter is single-worker only

See [Rate limiter — single-worker limitation](#rate-limiter--single-worker-limitation) above.

### GDrive token is refreshed in-memory only

`google-auth` refreshes expired access tokens automatically, but the refreshed token is **not written back** to `config.json`.  After a server restart the OAuth library will silently perform one extra refresh round-trip before the first request succeeds.  This is not a data-loss bug, but it means the token stored in `config.json` is always one cycle behind after the first expiry.  A future release will persist refreshed tokens to disk.

## Logs

Rotating log files are written to `logs/backupsys.log` (2 MB × 5 files) inside `BACKUPSYS_DATA_DIR`.