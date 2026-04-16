# BackupSys

A Windows desktop backup system with a PyQt5 tray app, incremental backups, encryption, compression, and multi-destination support (local, SFTP, FTP, SMB, HTTPS, Google Drive).

---

## Requirements

- Python 3.8+
- Windows 10/11 (Linux/macOS partially supported — no tray icon)

## Installation

```bash
pip install -r requirements_desktop.txt
```

For SFTP support (already in requirements):
```bash
pip install paramiko>=3.0.0
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
| `transport_utils.py` | Remote upload helpers — SFTP, FTP/FTPS, SMB, HTTPS |
| `notification_utils.py` | Email (SMTP/STARTTLS/SSL) and webhook notification helpers |
| `live_dest_tests.py` | Comprehensive testing suite for all remote destination protocols |
| `sftp_repro.py` | SFTP debugging and reproduction script |
| `build_exe.py` | PyInstaller packaging script |
| `config.json` | Runtime configuration (auto-generated on first run) |
| `INTEGRATION_PATCH.py` | Reference: shows how transport/notification modules were integrated into `backup_engine.py` |

---

## Configuration

Settings are stored in `config.json` next to the `.py` files (or in `BACKUPSYS_DATA_DIR` if set).

Key global settings:

| Key | Default | Description |
|-----|---------|-------------|
| `destination` | `./backups` | Local path where backup folders are written |
| `dest_type` | `local` | `local`, `sftp`, `ftp`, `smb`, `https`, `cloud` |
| `auto_backup` | `false` | Enable timed automatic backups |
| `interval_min` | `30` | Minutes between auto-backups |
| `retention_days` | `30` | Delete backups older than N days |
| `compression_enabled` | `false` | gzip-compress all files |
| `max_backup_mbps` | `0` | Throttle backup I/O (0 = unlimited) |

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

## Encryption

Per-watch Fernet (AES-128-CBC + HMAC-SHA256) encryption. Set `encrypt_key` on a watch in the Settings UI, or supply it via `BACKUPSYS_ENCRYPT_KEY_<ID>`.

**Limits:** Fernet loads the entire file into RAM. Files larger than 200 MB cannot be encrypted (raise `FERNET_MAX_BYTES` in `backup_engine.py` if needed, or use compression first to reduce file sizes).

---

## Restore

From the backup history UI, select a backup and click **Restore**. For incremental backups, use **Restore Full Chain** to replay all snapshots in order up to a chosen point in time.

## Testing Remote Destinations

Test all remote destination protocols with local servers:

```bash
python live_dest_tests.py
```

This starts local test servers for SFTP, FTP, HTTPS, and SMB, then runs comprehensive upload tests for each protocol. Useful for validating remote destination configurations before deploying.

For SFTP debugging specifically:

```bash
python sftp_repro.py
```

---

## Logs

Rotating log files are written to `logs/backupsys.log` (2 MB × 5 files) inside `BACKUPSYS_DATA_DIR`.