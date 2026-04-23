# Changelog

All notable changes to BackupSys are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

---

## [1.1.0] — 2026-04-23 (patch: improvements)

### Added
- **Single-file restore** — in the backup preview dialog, select any file and click
  "Restore Selected File" to restore only that one file to a folder of your choice.
  Handles encrypted and compressed backups automatically.

- **Pre/post backup script hooks per watch** — `pre_backup_cmd` and `post_backup_cmd`
  fields on each watch (Settings → Edit Watch → Backup Hooks).  The pre-hook runs before
  the backup starts; failure aborts the backup.  The post-hook runs regardless of outcome;
  failure is logged but does not change the backup result.  Post-hook receives
  `BACKUPSYS_STATUS`, `BACKUPSYS_WATCH`, and `BACKUPSYS_WATCH_ID` environment variables.

- **Google Drive folder picker** — after OAuth, users are prompted to choose which Drive
  folder receives their backups instead of always uploading to the root.  A "Browse…"
  button in Settings → Cloud allows changing the folder without re-authenticating.

- **Backup size estimate** — before a manual backup starts, a background thread estimates
  how many files and bytes will be transferred and logs the result ("📊 Estimate:  ~N
  file(s)  ·  ~X MB  (incremental)").  Runs concurrently and never blocks the backup.

- **Backup window stop time** — `backup_window_end` global setting (HH:MM).  Auto-backups
  that would *start* after this time are silently skipped until the next day.  Pair with
  "Run at times" to confine backups to a specific window (e.g. 02:00–06:00).
  Configurable in Settings → General → "Stop by".

- **Config export** — Settings → General → "Export Config…" saves a redacted copy of
  `config.json` (passwords stripped) to a path of your choice.  Use this to back up your
  watch list or transfer settings to another machine.

- **Per-watch last-backup status indicator** — every watch card now shows a coloured icon
  (✔ green / ✘ red / — grey) next to the "Last backup" line indicating whether the most
  recent run succeeded or failed.  Updates live when a backup completes.

- **Crash notifications** — if the app crashes with an unhandled exception, it attempts to
  fire the configured email and webhook notifications before showing the error dialog and
  exiting.  This means overnight crashes are reported the same way backup failures are.

- **v1.0.x → v1.1.0 migration notice** — first launch after upgrading shows a one-time
  dialog listing everything that changed and confirming no manual migration is needed.

- **IP-based rate limiting on all Flask API endpoints** — `send-otp`, `verify-otp`,
  `gdrive/exchange`, and `gdrive/refresh` are now rate-limited per IP using a sliding
  window.  Limits are configurable via environment variables
  (`BACKUPSYS_RATE_WINDOW`, `BACKUPSYS_RATE_MAX_OTP`, etc.).

### Fixed
- `smbprotocol` is now an unconditional dependency (was commented out, causing silent
  SMB failures on fresh installs on any platform).
- `webdavclient3` is now a required dependency rather than optional; the stdlib urllib
  fallback was insufficient for Nextcloud edge cases.
- `pywin32` comment updated to clarify the `sys_platform` marker handles platform
  detection automatically — no manual uncomment required.

### Changed
- `config.template.json` now includes `backup_schedule_times`, `backup_window_end`, and
  `dest_webdav` sections so new installs have a complete starting configuration.

---

## [1.1.0] — 2026-04-23

### Added
- **Dry-run / preview mode** — `backup_engine.run_backup(..., dry_run=True)` scans the
  source and builds the full change list without copying any files.  CLI: `python
  backupsys_cli.py dry-run --watch "Name" [--verbose]`.  GUI: new **Dry Run** button on
  every watch card opens a summary dialog showing files that *would* be copied.

- **File size exclusion per watch** — `max_file_size_mb` field on each watch.  Files
  larger than the limit are silently skipped and reported in `failed_files` so you can
  see what was omitted.  Configurable in Settings → Edit Watch → "Skip files over".

- **Per-watch storage quota** — `max_backup_bytes` field.  Before launching a new backup
  the engine checks total disk usage for that watch; if the quota is exceeded the backup
  is refused with a clear tray notification and log entry.  Configurable in Settings →
  Edit Watch → "Storage quota".

- **System idle detection** — `idle_threshold_cpu` global setting (0–100%).  Auto-backups
  are deferred while CPU usage exceeds the threshold; they resume automatically once the
  system is idle again.  Requires `psutil` (already in requirements).  Configurable in
  Settings → General → "Idle threshold".

- **Post-upload file-count verification** — SFTP and FTP upload functions now compare the
  number of files confirmed uploaded against the number of local files.  A mismatch is
  logged as a warning and returned in the result dict (`warning` key) so the UI can
  surface it to the user.

- **pywin32 now installed by default** — uncommented in `requirements_desktop.txt` so VSS
  (Volume Shadow Copy) works out-of-the-box on a fresh install.  Previously VSS failed
  silently because pywin32 was commented out.

- **Improved tray notifications** — backup failure now uses `QSystemTrayIcon.Warning` (amber
  icon) instead of `Information` (blue icon) and shows for 5 seconds.  Success notifications
  now include the backup size.  Cancelled backups get a separate neutral message.

### Changed
- **VSS failure messages upgraded from DEBUG to WARNING** — users now see a clear log line
  explaining that pywin32 or Administrator privileges are required for VSS, rather than a
  silent debug entry.

- **Duplicate README encryption section removed** — the confusing "Encryption (Legacy —
  see above)" section has been merged into a single `## Encryption` section with a concise
  legacy note at the bottom.

### WebDAV / Nextcloud / ownCloud destination

- **Streaming AES-256-GCM encryption** — `_encrypt_file` / `_decrypt_file` in
  `backup_engine.py` completely replaced.  New format (`BACKENC1` magic + per-chunk
  AES-256-GCM) handles files of **any size** with ~2 MB constant RAM overhead.
  Key is re-derived via HKDF-SHA256 so the same user-visible key works for both
  old and new files.  Auto-detect on decrypt: legacy Fernet files written by
  earlier versions decrypt transparently.  The 200 MB hard limit is gone.

- **VSS (Volume Shadow Copy Service)** — `backup_engine.py` now attempts to create
  a Windows VSS shadow copy before reading any directory source.  Locked / open
  files (Outlook `.pst`, browser SQLite databases, running application databases)
  are read from the snapshot rather than the live filesystem.  Falls back silently
  if VSS is unavailable (non-Windows, insufficient privileges, or VSS service not
  running).  Shadow copies are always deleted in the `finally` block.

- **Backup resume on interruption** — `run_backup()` writes a `_resume_{watch_id}.json`
  checkpoint after each successful file copy.  On the next run, if the checkpoint
  and partial backup directory both exist, already-copied files are skipped and the
  run continues from where it left off.  On success the checkpoint is deleted.

- **Portable mode** — Create an empty `portable.flag` file next to `desktop_app.py`
  (or set `BACKUPSYS_PORTABLE=1`) to store all data (config, snapshots, logs, queue)
  inside the app folder.  A Settings panel shows whether portable mode is active and
  explains how to enable it.  `config_manager.py` exposes `_IS_PORTABLE` and
  `_DATA_DIR` for introspection.

- **CLI / headless mode** — New `backupsys_cli.py` with sub-commands:
  `list`, `config`, `keygen`, `history`, `backup --watch <name|id>`,
  `backup --all [--strict]`, `validate --watch <name|id>`, `validate --all`.
  Loads `.env` automatically; supports `Ctrl-C` / `SIGINT` cancellation;
  routes all destination types (SFTP, FTP, HTTPS, WebDAV, Google Drive) using
  the same `backup_engine.run_backup()` path as the GUI.

- **Auto-update check** — `MainWindow` checks the GitHub Releases API 10 seconds
  after startup in a background daemon thread.  If a newer version is found, a
  non-blocking tray balloon is shown.  All network errors are silently ignored.
  Replace the placeholder URL in `_check_for_updates()` with your actual repo.

- **Unit tests** — three new test modules added to `tests/`:
  - `test_transport_utils.py` — SFTP, FTP, WebDAV, HTTPS upload functions with
    mocked connections; metadata exclusion; cleanup helpers.
  - `test_notification_utils.py` — `build_backup_email` for all statuses;
    SMTP success/failure (STARTTLS + SSL); webhook send/error/payload; `test_email`.
  - `test_watcher.py` — event buffering, duplicate-path deduplication, buffer cap,
    exclude patterns, on_change callback safety, `WatcherManager` add/remove/flush/stop.

- **"Remove from startup" UI note** — Settings → General now explains that unchecking
  the startup checkbox removes BackupSys from Windows login items.

### Fixed
- **CRLF line endings in `notification_utils.py`** — converted to LF to match the
  rest of the project.
- **Stale "S3/cloud credentials" comment** in `config_manager.py` line 95 updated to
  "Google Drive OAuth credentials per-watch".
- **Plaintext password fields in `config.template.json`** — `"password": ""` replaced
  with `"__password_note"` guidance strings for SFTP, FTP, SMB, email, and WebDAV
  sections.  Users are directed to the OS keyring / env-var alternatives.
- **WebDAV dest_type written correctly on save** — `_save_settings()` now explicitly
  maps combo index 6 to `"webdav"` in `cfg["dest_type"]`.

### Changed
- `requirements_desktop.txt` — added commented-out `webdavclient3>=3.14.6`;
  expanded `pywin32` comment to mention VSS as the primary motivation.
- `config_manager.py` — portable-mode path resolution now uses a single `_DATA_DIR`
  constant that all four path constants (`CONFIG_PATH`, `QUEUE_PATH`,
  `HISTORY_PATH`, `SNAPSHOTS_DIR`) derive from.  `dest_webdav: {}` added to
  `DEFAULT_CONFIG`.
- `backup_engine.run_backup()` — `source` argument is remapped to the VSS shadow
  path before `build_snapshot()` is called, so the snapshot reflects the shadow
  copy's view of the filesystem rather than potentially-locked live files.
- `desktop_app.py BackupWorker` — `cloud_config` is now built and passed into
  `run_backup()` for SFTP, FTP, HTTPS, WebDAV, and Google Drive destinations instead
  of passing `None` and handling uploads separately after the call.

---

## [1.0.1] — 2026-04-23

### Fixed
- **Version mismatch** — `APP_VERSION` in `desktop_app.py` was `"2.0"`;
  aligned to `"1.0.0"` to match `create_release_zip.py`.
- **Duplicate upload code removed** — `desktop_app.py` contained ~400 lines
  of SFTP / FTP / SMB / HTTPS upload logic that duplicated `transport_utils.py`.
  All upload functions in `desktop_app.py` are now thin wrappers that delegate
  directly to `transport_utils`; bug fixes only need to happen in one place.
- **HTTPS upload memory limit removed** — `upload_to_https()` previously
  loaded entire files into RAM (hard limit: 200 MB). It now streams each file
  in 256 KB chunks using `http.client`, so arbitrarily large backups work.
- **Fernet error message improved** — when a file exceeds the Fernet in-memory
  encryption limit the error message now includes actionable UI steps (which
  settings panel to open, which field to change) instead of generic advice.

### Added
- **`credential_store.py`** — OS-native keyring integration (Windows Credential
  Manager / macOS Keychain / Linux SecretService) for SFTP, FTP, SMB, and SMTP
  passwords.  Falls back silently to `config.json` if `keyring` is not installed.
  `transport_utils.py` now resolves passwords through this store automatically.
- **Unit test suite** (`tests/`) — pytest-based tests covering:
  - `backup_engine`: `hash_file`, `build_snapshot`, `diff_snapshots`,
    encrypt/decrypt round-trips, `cleanup_old_backups`, `BackupThrottler`,
    `safe_path`, `_fix_path`.
  - `backupsys_api`: OTP send/verify flow, rate limiting, attempt lockout,
    HMAC helpers, DB store helpers.
  - `config_manager`: load/save, watch CRUD, snapshot persistence, defaults.
  - `credential_store`: get/set/delete, keyring-unavailable fallback,
    convenience helpers for each destination type.
- **`schedule_type` / `schedule_time` fields** added to `config.template.json`
  to document the time-of-day scheduling feature already present in the app.
- **`__password_note` fields** added to `config.template.json` under every
  destination that stores a password, directing users to env-var / keyring
  alternatives instead of plaintext storage.

### Changed
- `requirements_desktop.txt` — added `keyring>=24.0.0` (optional but
  recommended), added `pytest>=8.0.0` under a `[dev]` comment.
- `create_release_zip.py` — added `credential_store.py` and `tests/` to the
  `SAFE_SOURCE_FILES` allowlist; added a pre-flight check that `CHANGELOG.md`
  exists before packaging.
- `config.template.json` — removed plaintext `"password"` fields from
  `dest_sftp`, `dest_ftp`, `dest_smb`, replaced with `__password_note`
  guidance strings.

---

## [1.0.0] — 2026-04-22

### Added
- Initial release.
- PyQt5 system tray app with dashboard, per-watch cards, backup history window.
- Incremental snapshot-based backups with full-chain restore.
- Per-watch Fernet (AES-128-CBC + HMAC-SHA256) encryption with 200 MB RAM
  guard and 50 MB soft warning.
- gzip compression (runs before encryption to maximise size reduction).
- I/O throttling via sliding-window `BackupThrottler` (configurable MB/s).
- Multi-destination support: local, SFTP, FTP/FTPS, SMB/CIFS, HTTPS API,
  Google Drive (OAuth 2.0).
- File-system watching via `watchdog` with polling fallback for network shares.
- Interval-based and time-of-day (`backup_schedule_times`) auto-backup
  scheduler — tick every 5 s, fires within ±5 s of scheduled time.
- Per-watch `skip_auto_backup` flag to opt individual watches out of global
  auto-backup without deactivating them.
- Email notifications (SMTP / STARTTLS / SSL) with App Password support.
- Webhook notifications (JSON POST) — compatible with Slack, Discord, n8n,
  Zapier, Make, and custom endpoints.
- `backupsys_api.py` Flask backend deployable to Railway — HMAC-stored OTPs,
  rate limiting (1 OTP/60 s), attempt lockout (5 tries), SQLite persistence.
- `IntegrityScheduler` — weekly background backup validation with configurable
  interval and UI result display.
- Admin panel with password protection (PBKDF2-HMAC-SHA256 + 16-byte salt,
  260 000 iterations), connection test buttons for every destination type.
- `setup_wizard.py` — guided first-run helper that checks Python version,
  installs requirements, writes starter `config.json`, optionally adds to
  Windows startup.
- `build_exe.py` — PyInstaller packaging to standalone `.exe`.
- `create_release_zip.py` — allowlist-based release packager that blocks
  every file that could contain secrets or runtime state.
- Rotating log files (2 MB × 5 files) to `logs/backupsys.log`.
- Single-instance lock to prevent duplicate tray apps.
- Weak API key warning shown at startup if `BACKUPSYS_API_KEY` is unset or
  looks like a placeholder.
- `.env` / `_env` auto-loader at startup (before `config_manager.load()`).
- `privacy.html` privacy policy.


### Fixed
- **Version mismatch** — `APP_VERSION` in `desktop_app.py` was `"2.0"`;
  aligned to `"1.0.0"` to match `create_release_zip.py`.
- **Duplicate upload code removed** — `desktop_app.py` contained ~400 lines
  of SFTP / FTP / SMB / HTTPS upload logic that duplicated `transport_utils.py`.
  All upload functions in `desktop_app.py` are now thin wrappers that delegate
  directly to `transport_utils`; bug fixes only need to happen in one place.
- **HTTPS upload memory limit removed** — `upload_to_https()` previously
  loaded entire files into RAM (hard limit: 200 MB). It now streams each file
  in 256 KB chunks using `http.client`, so arbitrarily large backups work.
- **Fernet error message improved** — when a file exceeds the Fernet in-memory
  encryption limit the error message now includes actionable UI steps (which
  settings panel to open, which field to change) instead of generic advice.

### Added
- **`credential_store.py`** — OS-native keyring integration (Windows Credential
  Manager / macOS Keychain / Linux SecretService) for SFTP, FTP, SMB, and SMTP
  passwords.  Falls back silently to `config.json` if `keyring` is not installed.
  `transport_utils.py` now resolves passwords through this store automatically.
- **Unit test suite** (`tests/`) — pytest-based tests covering:
  - `backup_engine`: `hash_file`, `build_snapshot`, `diff_snapshots`,
    encrypt/decrypt round-trips, `cleanup_old_backups`, `BackupThrottler`,
    `safe_path`, `_fix_path`.
  - `backupsys_api`: OTP send/verify flow, rate limiting, attempt lockout,
    HMAC helpers, DB store helpers.
  - `config_manager`: load/save, watch CRUD, snapshot persistence, defaults.
  - `credential_store`: get/set/delete, keyring-unavailable fallback,
    convenience helpers for each destination type.
- **`schedule_type` / `schedule_time` fields** added to `config.template.json`
  to document the time-of-day scheduling feature already present in the app.
- **`__password_note` fields** added to `config.template.json` under every
  destination that stores a password, directing users to env-var / keyring
  alternatives instead of plaintext storage.

### Changed
- `requirements_desktop.txt` — added `keyring>=24.0.0` (optional but
  recommended), added `pytest>=8.0.0` under a `[dev]` comment.
- `create_release_zip.py` — added `credential_store.py` and `tests/` to the
  `SAFE_SOURCE_FILES` allowlist; added a pre-flight check that `CHANGELOG.md`
  exists before packaging.
- `config.template.json` — removed plaintext `"password"` fields from
  `dest_sftp`, `dest_ftp`, `dest_smb`, replaced with `__password_note`
  guidance strings.

---

## [1.0.0] — 2026-04-22

### Added
- Initial release.
- PyQt5 system tray app with dashboard, per-watch cards, backup history window.
- Incremental snapshot-based backups with full-chain restore.
- Per-watch Fernet (AES-128-CBC + HMAC-SHA256) encryption with 200 MB RAM
  guard and 50 MB soft warning.
- gzip compression (runs before encryption to maximise size reduction).
- I/O throttling via sliding-window `BackupThrottler` (configurable MB/s).
- Multi-destination support: local, SFTP, FTP/FTPS, SMB/CIFS, HTTPS API,
  Google Drive (OAuth 2.0).
- File-system watching via `watchdog` with polling fallback for network shares.
- Interval-based and time-of-day (`backup_schedule_times`) auto-backup
  scheduler — tick every 5 s, fires within ±5 s of scheduled time.
- Per-watch `skip_auto_backup` flag to opt individual watches out of global
  auto-backup without deactivating them.
- Email notifications (SMTP / STARTTLS / SSL) with App Password support.
- Webhook notifications (JSON POST) — compatible with Slack, Discord, n8n,
  Zapier, Make, and custom endpoints.
- `backupsys_api.py` Flask backend deployable to Railway — HMAC-stored OTPs,
  rate limiting (1 OTP/60 s), attempt lockout (5 tries), SQLite persistence.
- `IntegrityScheduler` — weekly background backup validation with configurable
  interval and UI result display.
- Admin panel with password protection (PBKDF2-HMAC-SHA256 + 16-byte salt,
  260 000 iterations), connection test buttons for every destination type.
- `setup_wizard.py` — guided first-run helper that checks Python version,
  installs requirements, writes starter `config.json`, optionally adds to
  Windows startup.
- `build_exe.py` — PyInstaller packaging to standalone `.exe`.
- `create_release_zip.py` — allowlist-based release packager that blocks
  every file that could contain secrets or runtime state.
- Rotating log files (2 MB × 5 files) to `logs/backupsys.log`.
- Single-instance lock to prevent duplicate tray apps.
- Weak API key warning shown at startup if `BACKUPSYS_API_KEY` is unset or
  looks like a placeholder.
- `.env` / `_env` auto-loader at startup (before `config_manager.load()`).
- `privacy.html` privacy policy.
