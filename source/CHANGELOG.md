# Changelog

All notable changes to BackupSys are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

---

## [1.1.8] — 2026-05-04

### Security & Reliability
- **`/backup/upload` rate limiting** — The upload endpoint now has its own
  sliding-window rate limiter (default 30 req / 60 s per IP), independent of
  the event endpoint.  Tune via `BACKUPSYS_UPLOAD_RATE_LIMIT` and
  `BACKUPSYS_UPLOAD_RATE_WINDOW_SEC` environment variables.

- **Flask `MAX_CONTENT_LENGTH` enforced at stream level** — Previously the
  250 MB per-file cap was checked via `request.content_length`, which clients
  using chunked transfer encoding could bypass by omitting the header entirely.
  `app.config['MAX_CONTENT_LENGTH']` is now set so Werkzeug enforces the limit
  before the view function runs.  A JSON `@app.errorhandler(413)` is registered
  so chunked-upload rejections return `{"error": "..."}` instead of an HTML page.

- **Total server-side storage quota** — New `BACKUPSYS_STORAGE_QUOTA_BYTES`
  environment variable (default: 0 = unlimited).  When set, the upload handler
  sums all bytes in `FILES_DIR` and rejects new uploads with HTTP 507 once the
  quota is reached, preventing a single client from filling the disk with
  thousands of small files.

- **`WEB_CONCURRENCY` > 1 startup warning** — If a hosting platform injects
  `WEB_CONCURRENCY` > 1 into the environment, the server now logs a loud
  WARNING at startup naming the actual multiplied effective rate limit, rather
  than silently allowing proportionally more traffic per client.

- **`.env` auto-load for API server** — `backupsys_api.py` now calls
  `load_dotenv()` at import time (via `python-dotenv`, added to
  `requirements_api.txt`).  Values in a `.env` file are picked up without
  needing to `source` it manually before starting gunicorn.

- **`pyproject.toml` desktop extras completed** — `pip install backupsys[desktop]`
  previously silently omitted `watchdog`, `psutil`, `python-dotenv`, `keyring`,
  `webdavclient3`, and `pywin32` (Windows).  All are now listed in
  `[project.optional-dependencies].desktop` with correct platform markers.

### Known Limitations
- **Server-side restore not implemented** — `POST /restore` returns HTTP 501.
  Restore is desktop/CLI-only in this version: use `GET /manifest` +
  `GET /files/<path>` to reconstruct a backup locally.  A future release will
  add a headless restore path.  This limitation is now documented in the README
  under *Known Limitations*.

### Added
- **Day-of-week scheduling** — `backup_schedule_times` (global) and `schedule_times`
  (per-watch) now store `{"time": "HH:MM", "days": <bitmask>}` objects instead of
  plain strings.  The `days` field is a 7-bit bitmask (bit 0 = Monday … bit 6 = Sunday;
  127 = every day).  The Settings and Edit Watch UIs now show a row of Mon–Sun
  checkboxes alongside each HH:MM entry so you can restrict a fire-time to weekdays,
  weekends, or any custom combination.  Existing plain `"HH:MM"` entries in
  `config.json` are migrated transparently on first load (treated as `days: 127`).

- **Cross-watch file search** — A new **🔍 File Search** tab in the History window
  lets you search for any filename or path fragment across every watch's backup
  snapshots simultaneously.  The search scans all `MANIFEST.json` files in the
  configured destination(s) and returns the watch name, backup date, relative
  path, file size, and backup directory for each hit.  Results are sortable by
  any column.  Optionally filter to a single watch via the dropdown.  Per-watch
  destination overrides are also searched.

- **`backupsys_api.py`** — Flask backend deployable to Railway, Render, or
  Fly.io.  Provides authenticated backup-event ingestion (`POST /backup/event`),
  event history (`GET /backup/events`), admin stats (`GET /admin/stats`), an
  OTP flow (`POST /otp/request` + `/otp/verify`), and a connectivity ping
  (`POST /ping`).  Features: HMAC-SHA256 request signing, 1 OTP/60 s rate
  limiting, 5-attempt lockout (15 min), rotating event log (1 000 rows),
  SQLite persistence with WAL mode, per-request API log table.  Replaces the
  stub that was described in the v1.0.0 changelog but never shipped.

### Fixed
- **`GITHUB_REPO` / `GITHUB_RELEASES_URL` constants in `desktop_app.py`** —
  The GitHub repo URL was previously hardcoded as a string literal at line 5910.
  Moved to two named constants at the top of the constants block so the repo
  can be changed without hunting through the codebase.  `GITHUB_REPO` is the
  `owner/repo` slug; `GITHUB_RELEASES_URL` is derived from it automatically.

- **rclone retention now actually runs** — `cleanup_remote_backups()` in
  `transport_utils.py` previously returned a static "skipped" result for
  rclone destinations.  Replaced with a new `cleanup_rclone_backups()` function
  that uses `rclone lsd` to list backup folders, identifies folders older than
  `retention_days` by their `YYYYMMDD_HHMMSS` name prefix, and deletes them
  with `rclone purge`.  Partial failures (per-folder purge errors) are
  collected and reported without aborting the rest of the cleanup.  The amber
  "retention not supported" warning label in the global settings is updated to
  a blue informational note reflecting the new behaviour.

- **Per-watch bandwidth override in Edit Watch dialog** — The bandwidth throttle
  schedule was previously only configurable in global settings.  The Edit Watch
  dialog now includes a dedicated "Bandwidth Override" group with a max MB/s
  spinner (0 = use global) and a per-watch schedule table (start/end/limit
  rows).  `BackupWorker` now picks the per-watch values when `max_backup_mbps >
  0`, falling back to global otherwise.  Values are persisted to the watch dict
  as `max_backup_mbps` and `bandwidth_schedule`.

- **Backup Queue panel in History window** — `backup_queue.json` was persisted
  by `config_manager.py` but had no GUI representation.  A third "Queue" tab is
  now shown in the History window displaying all pending items (watch name,
  triggered-by reason, queued-at timestamp).  The tab label shows the live item
  count.  A Refresh button re-reads the JSON from disk.  `MainWindow._open_history()`
  now passes the current queue to `HistoryWindow`.

- **Flask API `machine_id` tracking** — All desktop clients previously shared
  one `BACKUPSYS_API_KEY` with no way to distinguish which machine sent an
  event.  `_send_webhook()` now includes a `machine_id` field (resolved via
  `socket.gethostname()`) in every payload.  `POST /backup/event` accepts and
  stores `machine_id`.  `GET /backup/events` supports a `?machine_id=` query
  filter.  `GET /admin/stats` now returns a `machines` array with per-machine
  totals, failure counts, and last-seen timestamps.  A zero-downtime SQLite
  migration (`ALTER TABLE ADD COLUMN`) ensures existing databases are upgraded
  on first start without data loss.

- **`_is_excluded` in `backup_engine.py` did not honour `!`-prefixed include
  patterns** — `watcher.py` supports a whitelist syntax where patterns
  prefixed with `!` mean "only back up files matching this name/glob".  The
  backup engine's `_is_excluded()` had no knowledge of this prefix and treated
  `!*.docx` as a literal exclude glob, causing the watcher and the engine to
  disagree on which files were in scope.  Fixed: `_is_excluded()` now splits
  patterns into `include_only` (prefix `!` stripped) and `exclude_only` lists.
  When any include-only pattern is present the function operates in whitelist
  mode — files are excluded unless they match at least one include rule.
  Otherwise the original blacklist behaviour is unchanged.  This fix is
  consistent with the `watcher.py` logic and applies to `build_snapshot()`,
  the watcher fast-path, and `estimate_backup_size()`.

## [1.1.5] — 2026-04-24

### Fixed
- **`AttributeError: 'HistoryWindow' object has no attribute '_filter'`** — clicking
  "Clear dates" in the History window crashed the app. `_clear_dates()` called
  `self._filter()` which does not exist; the correct method is `self._filter_changes()`.
  One-line rename fix.

- **`RuntimeError: wrapped C/C++ object of type QComboBox has been deleted`** — opening
  the Admin panel crashed immediately on `_load_values()` when trying to call
  `self.dest_type_combo.setCurrentIndex(idx)`.  Root cause: `_build_ui()` created the
  `QTabWidget` and all intermediate container widgets (`general_inner`, `general`,
  `watches_tab`, `cloud_tab`, `notif_inner`, `notif_scroll`) as bare local variables.
  When `_build_ui()` returned, Python's garbage collector was free to delete these objects
  because no Python-level reference kept them alive — even though Qt's C++ side still
  owned the widget tree.  This invalidated the C++ wrapper for `dest_type_combo` (which
  lives inside `general_inner`) before `_load_values()` even ran.

  Fix: all seven intermediate container objects are now stored on `self` (`self._tabs`,
  `self._general_inner`, `self._general_scroll`, `self._watches_tab`, `self._cloud_tab`,
  `self._notif_inner`, `self._notif_scroll`) so Python keeps their wrappers alive for
  the lifetime of the dialog.  Local aliases (e.g. `tabs = self._tabs`) are kept so the
  rest of `_build_ui()` is unchanged.

## [1.1.4] — 2026-04-24

### Fixed
- **`AttributeError: 'HistoryWindow' object has no attribute 'table'`** — clicking the
  **History** button crashed the app immediately.  `HistoryWindow._build_ui()` was
  missing three things:

  1. **`self.tabs = QTabWidget()` never created** — `_build_change_history_tab()` calls
     `self.tabs.addTab(...)` but the `QTabWidget` instance was never instantiated in
     `_build_ui()`, so any access to `self.tabs` raised `AttributeError`.

  2. **`_build_change_history_tab()` never called** — `_build_ui()` called only
     `_build_backup_history_tab()`.  Because `_build_change_history_tab()` is what
     creates `self.table`, it was never set on the object, causing the crash when
     `_populate_changes()` tried to use it immediately after.

  3. **`_build_backup_history_tab()` never added its tab** — the method built its
     widget but never called `self.tabs.addTab(tab, "Backup History")`, so the Backup
     History tab would have been silently absent from the UI even if the crash were
     otherwise avoided.

  Fix: `_build_ui()` now creates `self.tabs`, adds it to the layout, and calls both
  builder methods in order (Change History first, Backup History second).
  `_build_backup_history_tab()` now concludes with `self.tabs.addTab(tab, "Backup
  History")`.

## [1.1.3] — 2026-04-24

### Fixed
- **`NameError: name 'QPlainTextEdit' is not defined`** — clicking the **Logs** button
  crashed the app with an unhandled exception because `QPlainTextEdit` was used in
  `LogViewerDialog._build_ui()` (line 8595) but was never included in the top-level
  `from PyQt5.QtWidgets import (...)` block.  Added to the main import.

- **`QDateEdit` and `QDate` also missing from imports** — the backup history filter UI
  (`HistoryDialog`) instantiates `QDateEdit` and calls `QDate.currentDate()` / `QDate(y,
  m, d)` (lines 7922–7934, 8222–8223).  Neither was in the import block: `QDateEdit`
  belongs in `PyQt5.QtWidgets` and `QDate` belongs in `PyQt5.QtCore`.  Both added.
  Without this fix, opening the History panel would have triggered the same crash class.


### Fixed
- **`config.template.json` still missing `dest_rclone` block** — the v1.1.1 changelog
  entry claimed this was fixed, but the block was absent from the released file.
  Added `dest_rclone` with `remote`, `path`, and a `__note` explaining how to obtain
  the remote name from `rclone config`.

- **Google Drive missing from watch-level destination combo** — the `dest_type_combo`
  in Settings listed Local, SMB, SFTP, FTPS, FTP, HTTPS, rclone, and WebDAV, but Google
  Drive was accessible only through the separate Settings → Cloud tab, creating an
  inconsistent and confusing experience.  Added "Google Drive" as index 8 in the combo.
  Selecting it shows a concise panel directing users to the Cloud tab for OAuth setup and
  watch assignment.  The `idx_map` (load) and `dest_map` (save) are updated accordingly,
  and the "cloud"/"gdrive" dest_type values now correctly map to index 8 so existing
  configs round-trip without resetting.

- **README destination list incomplete** — the header and `dest_type` config table
  omitted FTP/FTPS, WebDAV/Nextcloud/ownCloud, rclone, and Google Drive.  Both are now
  updated to list every supported destination type.


## [1.1.1] — 2026-04-24

### Fixed
- **CLI: `rclone` destination silently ignored** — `backupsys_cli.py` cmd_backup now
  correctly builds the `rclone` cloud config from `dest_rclone` in `config.json` when
  `dest_type` is `rclone`.  Previously the rclone config was never passed to
  `backup_engine.run_backup`, so backups ran locally and the rclone upload step was
  never reached.

- **Remote retention skipped for WebDAV and rclone** — `cleanup_remote_backups()` in
  `transport_utils.py` handled SFTP / FTP / SMB retention but fell into a bare `else`
  for all other types, returning `ok=True, deleted=0` for WebDAV and rclone.  Both now
  have explicit named cases with informative skip messages (matching the existing HTTPS
  behaviour), and will no longer silently suppress retention warnings.

- **`config.template.json` missing `dest_rclone` block** — every other destination
  (SFTP, FTP, SMB, HTTPS, WebDAV) had a template section; rclone was absent.  Added
  `dest_rclone` with `remote`, `path`, and a `__note` explaining where to get the
  remote name from `rclone config`.

- **Pre-existing syntax errors** — two bugs introduced upstream were corrected:
  a stray `)` on line 1178 of `backup_engine.py` that prematurely closed the
  `run_backup()` signature, and escaped single-quote sequences (`\'`) inside
  double-quoted f-strings in `desktop_app.py` that caused a parse error.

### Added
- **CLI `restore` command** — headless / SSH / WSL / Task Scheduler users can now
  restore backups without opening the GUI:
  ```
  python backupsys_cli.py restore --watch "My Documents" --target C:\Restored
  python backupsys_cli.py restore --watch "My Documents" --target C:\Restored --backup-id bk_abc123
  python backupsys_cli.py restore --watch "My Documents" --target C:\Restored --full-chain
  python backupsys_cli.py restore --watch "My Documents" --target C:\Restored --no-overwrite
  ```
  Supports single-snapshot and full incremental-chain restores, encrypted and
  compressed backups, and the same `--watch` name/ID resolution used by `backup`.

- **`APP_VERSION` out of sync with CHANGELOG** — `desktop_app.py` reported
  `"1.1.0"` while the changelog had advanced to `1.1.5`. The in-app version
  label, update-check comparison, and User-Agent header were all showing a
  stale version. Bumped to `"1.1.5"`.

- **`is_metered_connection()` false positives on public Wi-Fi** — the previous
  implementation queried `Get-NetConnectionProfile.NetworkCategory` and treated
  `"Public"` as metered. `NetworkCategory` is a *Windows Firewall profile*
  setting (Public / Private / Domain) and has nothing to do with data billing.
  Any coffee-shop or hotel Wi-Fi would match, causing auto-backups to be silently
  skipped on unlimited connections. Fixed by switching to the correct WinRT API:
  `NetworkInformation.GetInternetConnectionProfile().GetConnectionCost()`.
  A connection is now only considered metered when `NetworkCostType` is
  `Fixed` (data-capped) or `Variable` (pay-per-byte).

- **`config.template.json` missing `dest_rclone` block** — rclone is a fully
  supported destination type but the config template had no example entry.
  Added a `dest_rclone` block with `remote`, `path`, and a `__examples` map
  covering common rclone remotes.

- **`setup_wizard.py` used legacy SFTP key names** — the wizard wrote
  `dest_sftp` using old keys (`user`, `pass`, `path`, `keyfile`, `key_pass`)
  that differed from the canonical names in `config.template.json` and the
  Settings UI (`username`, `remote_path`, `key_path`, `key_passphrase`).
  Fixed to use canonical names. Also expanded the previously empty `dest_ftp`,
  `dest_smb`, and `dest_https` stubs, and added missing `dest_webdav` and
  `dest_rclone` blocks so wizard-generated configs match `config.template.json`.

- **README stale `.env` instruction** — the Environment Variables section
  referenced `"rename _env → .env"`. No `_env` file is included in releases;
  the file ships as `.env.example`. Removed the stale reference.

- **Duplicate `[1.0.0]` section in CHANGELOG** — the initial-release entry
  appeared twice. The second copy was a stale paste artifact. Removed.

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

## [1.0.1] — 2026-04-23

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

## [1.0.2] — 2026-04-23

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