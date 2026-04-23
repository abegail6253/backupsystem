# BackupSys

A Windows desktop backup system with a PyQt5 tray app, incremental backups, encryption, compression, and multi-destination support (local, SFTP, FTP, SMB, HTTPS, Google Drive).

---

## Requirements

- Python 3.8+
- Windows 10/11 (Linux/macOS partially supported — no tray icon)

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

For SMB on Linux/macOS, uncomment `smbprotocol` in `requirements_desktop.txt`.

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
| `desktop_app.py` | PyQt5 tray app — UI, scheduler, backup worker threads |
| `backup_engine.py` | Core backup logic — snapshot, diff, copy, encrypt, compress, restore, validate |
| `config_manager.py` | Load/save `config.json`, watch CRUD, snapshot & queue persistence |
| `watcher.py` | File-system watching via watchdog (polling fallback for network shares) |
| `transport_utils.py` | Remote upload helpers — SFTP, FTP/FTPS, SMB, HTTPS, WebDAV/Nextcloud |
| `notification_utils.py` | Email (SMTP/STARTTLS/SSL) and webhook notification helpers |
| `credential_store.py` | OS keyring wrapper for SFTP/FTP/SMB/SMTP passwords (falls back to config.json) |
| `integrity_scheduler.py` | Weekly background backup integrity checker |
| `connect_cloud.py` | Google Drive OAuth token management |
| `backupsys_cli.py` | Headless CLI — run backups, list watches, validate, keygen (no GUI required) |
| `setup_wizard.py` | Guided first-run setup — installs deps, writes config, adds to startup |
| `backupsys_api.py` | Flask API for remote OTP auth (deploy to Railway) |
| `build_exe.py` | PyInstaller packaging script |
| `create_release_zip.py` | Builds a clean, secret-free source release zip |
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
| `dest_type` | `local` | `local`, `sftp`, `ftp`, `smb`, `https`, `webdav`, `cloud` |
| `auto_backup` | `false` | Enable timed automatic backups |
| `interval_min` | `30` | Minutes between auto-backups |
| `retention_days` | `30` | Delete backups older than N days |
| `compression_enabled` | `false` | gzip-compress all files |
| `max_backup_mbps` | `0` | Throttle backup I/O (0 = unlimited) |
| `idle_threshold_cpu` | `0` | Defer auto-backups while CPU% exceeds this value (0 = always run, requires psutil) |

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

---

## Environment Variables

Copy `.env.example` to `.env` (or rename `_env` → `.env`) and fill in your values.

> ⚠ **Never commit `.env` to Git.** It's already in `.gitignore`. If credentials were accidentally shared, regenerate them immediately at [Google Cloud Console](https://console.cloud.google.com/apis/credentials).

| Variable | Purpose |
|----------|---------|
| `BACKUPSYS_DATA_DIR` | Override data directory for config, snapshots, logs |
| `BACKUPSYS_EMAIL_PASSWORD` | SMTP password (avoids storing it in `config.json`) |
| `BACKUPSYS_ENCRYPT_KEY_<WATCH_ID>` | Per-watch Fernet encryption key |
| `BACKUPSYS_ENCRYPT_KEY_DEFAULT` | Fallback key for watches without an explicit key |
| `GDRIVE_CLIENT_ID` | Google Drive OAuth client ID |
| `GDRIVE_CLIENT_SECRET` | Google Drive OAuth client secret |

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

## Credential Store (Keyring)

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

# Generate a new encryption key
python backupsys_cli.py keygen

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

The test suite covers `backup_engine`, `backupsys_api`, `config_manager`, and
`credential_store`.  Tests are self-contained and use temporary directories —
no real files, servers, or email accounts are needed.

---

## Logs

Rotating log files are written to `logs/backupsys.log` (2 MB × 5 files) inside `BACKUPSYS_DATA_DIR`.