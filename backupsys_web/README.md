Yes! The README has outdated info in a few spots. Here's the fully updated README.md:
markdown# 🛡️ BackupSys Web — v1.9

**A web dashboard for real file backups. Runs locally, open in any browser.**

---

## ⚡ Quick Start
```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the server
python app.py

# 3. Open your browser
http://localhost:5000
```

---

## 📁 Project Structure
```
backupsys_web/
├── app.py               ← Flask backend + all API routes
├── backup_engine.py     ← File copy, SHA-256 hashing, diff, validate
├── watcher.py           ← Real-time change detection (watchdog / polling fallback)
├── config_manager.py    ← Atomic config reads/writes (config.json)
├── config.json          ← Your settings & watch targets (auto-created)
├── requirements.txt     ← flask, flask-limiter, watchdog, cryptography, psutil
├── templates/
│   └── index.html       ← Full SPA frontend
├── static/
│   ├── style.css        ← All styles
│   └── app.js           ← All frontend JS
└── backups/             ← Created automatically on first backup
```

---

## 🌐 API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET  | `/` | Dashboard UI |
| GET  | `/api/dashboard` | Stats + recent backups |
| GET  | `/api/watches` | List watch targets |
| POST | `/api/watches` | Add a watch target |
| PATCH | `/api/watches/<id>` | Update name/tags/notes |
| DELETE | `/api/watches/<id>` | Remove a watch target |
| POST | `/api/watches/<id>/pause` | Pause or resume |
| POST | `/api/watches/<id>/scan` | Scan for pending changes |
| POST | `/api/watches/<id>/filediff` | Line diff for a specific file |
| GET  | `/api/watches/<id>/dry-run` | Preview backup without copying |
| GET  | `/api/watches/<id>/stats` | Per-watch backup statistics |
| POST | `/api/watches/<id>/duplicate` | Duplicate a watch target |
| POST | `/api/backup/<id>` | Start a backup |
| GET  | `/api/backup/<id>/status` | Poll backup progress |
| POST | `/api/backup/<id>/cancel` | Cancel a running backup |
| POST | `/api/backup/all` | Backup all active watches |
| POST | `/api/backup/all/cancel` | Cancel all running backups |
| GET  | `/api/backup/all/status` | Status of all running backups |
| GET  | `/api/backup/<id>/browse` | Browse files inside a backup |
| GET  | `/api/history` | Full backup history (filterable) |
| GET  | `/api/history/watches` | Distinct watches with backups |
| GET  | `/api/history/stats` | Aggregate history statistics |
| DELETE | `/api/history/<id>` | Delete a single backup |
| POST | `/api/history/<id>/annotate` | Add notes to a backup |
| POST | `/api/history/export-bulk` | Export multiple backups as ZIP |
| POST | `/api/history/clear` | Clear all backup history |
| POST | `/api/validate` | Validate a backup's integrity |
| GET  | `/api/export/<id>` | Download backup as .zip |
| POST | `/api/restore` | Restore files from a backup |
| POST | `/api/restore/file` | Restore a single file from a backup |
| GET  | `/api/settings` | Get settings |
| POST | `/api/settings` | Save settings |
| POST | `/api/settings/validate-dest` | Check if destination is writable |
| POST | `/api/settings/test-webhook` | Test webhook URL |
| POST | `/api/files/browse` | List files in a directory |
| POST | `/api/files/read` | Read a text file |
| POST | `/api/files/save` | Save a text file |
| POST | `/api/files/create` | Create a new empty file |
| POST | `/api/files/mkdir` | Create a new folder |
| POST | `/api/files/delete` | Delete a file |
| POST | `/api/files/rename` | Rename a file |
| GET  | `/api/system/info` | Runtime info (version, uptime, python) |
| GET  | `/api/health` | Health check |

---

## ✨ Features

- **Real incremental backups** — only changed files are copied after the first run
- **SHA-256 integrity hashing** — every backup gets a `BACKUP.sha256` for verification
- **Real-time file watcher** — detects changes instantly (uses watchdog, falls back to polling)
- **Line-diff viewer** — see exactly what changed in any file, before or after backup
- **File editor** — open, edit, and save text files directly in the browser
- **Find & Replace** — `Ctrl+F` in the editor opens a find/replace bar
- **File sidebar search** — filter files by name in the file browser
- **History filters** — filter by watch, status, search query, or date range
- **Bulk select** — export or delete multiple backups at once
- **Backup notes** — annotate any backup with your own notes
- **Dry run / Preview** — see what would be backed up before running
- **Pause/Resume** — temporarily stop watching a target without removing it
- **Tags & notes** — organize watches with tags and descriptions
- **Export as ZIP** — download any backup as a zip file
- **Bulk export** — download multiple backups as a single ZIP
- **Restore** — restore all files or a single file from any backup
- **Auto-backup daemon** — runs backups on a configurable schedule
- **Retention policy** — old backups are auto-deleted based on your retention setting
- **Compression** — optional gzip compression to save disk space
- **Webhook notifications** — POST alerts to Discord, Slack, ntfy.sh on failure
- **Cancel backups** — stop a running backup mid-way
- **Watch stats** — per-watch success rate, disk usage, and history
- **Duplicate watch** — clone a watch target with a new name/path
- **Context menus** — right-click watches or files for quick actions
- **Light/dark theme** — toggle between themes, preference is saved
- **Desktop notifications** — browser notifications when backups finish
- **Sound notifications** — audio beep on backup complete or failure
- **Rate limiting** — built-in API rate limiting via flask-limiter
- **Atomic config writes** — config.json is never left in a corrupt state

---

## ⌨️ Keyboard Shortcuts

| Shortcut | Action |
|---|---|
| `Alt+1` | Dashboard |
| `Alt+2` | Watches |
| `Alt+3` | File Editor |
| `Alt+4` | History |
| `Alt+5` | Settings |
| `Ctrl+S` | Save current file (editor) |
| `Ctrl+F` | Open Find/Replace bar (editor) |
| `Ctrl+Shift+S` | Save file + run backup (editor) |
| `Ctrl+Shift+B` | Backup all watches |
| `Enter` | Next match (find bar) |
| `Shift+Enter` | Previous match (find bar) |
| `Escape` | Close modal / close find bar |
| `?` | Show all shortcuts |

---

## 🎨 Customizing Colors

Edit the CSS variables in `static/style.css`:
```css
:root {
  --amber: #f5a623;   /* main accent */
  --green: #39d98a;   /* success */
  --red:   #ff5c5c;   /* error */
  --blue:  #4da6ff;   /* info */
  --bg0:   #060809;   /* darkest background */
}
```

---

## 💡 Notes

- First backup of any target is always a **full backup**
- Subsequent backups are **incremental** — only modified/added/deleted files
- Each backup creates a `MANIFEST.json` + `BACKUP.sha256` for integrity verification
- Compressed backups store files as `.gz` — restore and validate handle this automatically
- If `config.json` becomes corrupted, it is automatically backed up as `.bak` and reset
- `watchdog` is optional — if not installed, the watcher falls back to polling every 5 seconds
- Set `BACKUPSYS_PASSWORD` environment variable to enable basic auth password protection

⚠️ **Known Limitation:** Backups are not deduplicated across watches.
If you watch `C:\Projects\A` and `C:\Projects` separately,
files in `A` will be backed up twice.