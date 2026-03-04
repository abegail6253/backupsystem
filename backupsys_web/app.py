
import json
import os
import shutil
import sys
import tempfile
import threading
import time
import urllib.request
import logging

from logging.handlers import RotatingFileHandler
from datetime import datetime
from pathlib import Path
from typing import Optional
from flask import Flask, jsonify, request, render_template, send_file, after_this_request, Response

from backup_engine import (
    _safe_size, _human_size,
    run_backup, validate_backup, list_backups, get_watch_stats,
    build_snapshot, diff_snapshots,
    restore_backup, cleanup_old_backups,
    get_backup_by_id, read_file_safe, _fix_path, hash_file,
    browse_backup_contents, safe_path, hash_directory,
    export_backup_zip,
    MAX_EDIT_BYTES, _backup_index,
)

import config_manager as cfg_mod
from config_manager import CONFIG_PATH
from watcher import WatcherManager

# ── Setup Logging ──────────────────────────────────────────────────────────────

log_file = Path(__file__).parent / "backupsys.log"
handler = RotatingFileHandler(str(log_file), maxBytes=10*1024*1024, backupCount=5)
handler.setFormatter(logging.Formatter(
    '%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
))
logging.getLogger().addHandler(handler)
logging.getLogger().setLevel(logging.INFO)

logger = logging.getLogger(__name__)
logger.info("=" * 60)
logger.info("[BackupSys] Server started")
logger.info("=" * 60)

# ── Flask App ──────────────────────────────────────────────────────────────────

app     = Flask(__name__)
watcher = WatcherManager()

_APP_START_TIME = time.time()

# Per-watch lock to prevent duplicate backup race condition
_backup_locks: dict = {}
_backup_locks_lock = threading.Lock()

from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["200 per day", "50 per hour"],
    storage_uri="memory://"
)

import time

class BackupThrottler:
    """Limit backup I/O to prevent system overload."""
    def __init__(self, max_mbps: float = 100.0):
        self.max_bytes_per_sec = int(max_mbps * 1024 * 1024)
        self.last_time = time.time()
        self.bytes_sent = 0
    
    def throttle(self, bytes_copied: int):
        """Call after copying each file."""
        if self.max_bytes_per_sec <= 0:
            return  # unlimited — skip throttling entirely
        self.bytes_sent += bytes_copied
        elapsed = time.time() - self.last_time
        if elapsed < 1.0:
            expected_wait = (self.bytes_sent / self.max_bytes_per_sec) - elapsed
            if expected_wait > 0:
                time.sleep(expected_wait)
        else:
            self.bytes_sent = 0
            self.last_time = time.time()


def _send_email_notification(result: dict, email_config: dict):
    """Send email on backup failure."""
    if not email_config.get("enabled"):
        return
    
    try:
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        
        msg = MIMEMultipart()
        msg["From"] = email_config.get("from_addr")
        msg["To"] = email_config.get("to_addr")
        msg["Subject"] = f"❌ BackupSys Failed: {result['watch_name']}"
        
        body = f"""
BackupSys Backup Failed

Watch: {result['watch_name']}
Time: {result['timestamp']}
Error: {result['error']}

Destination: {result['destination']}
Source: {result['source']}
"""
        msg.attach(MIMEText(body, "plain"))
        
        with smtplib.SMTP(email_config.get("smtp_host"), email_config.get("smtp_port", 587)) as server:
            server.starttls()
            server.login(email_config.get("username"), email_config.get("password"))
            server.send_message(msg)
        
        logger.info(f"📧 Email sent for failed backup: {result['watch_name']}")
    except Exception as e:
        logger.warning(f"⚠️  Email notification failed: {e}")
# ── Optional Basic Auth ───────────────────────────────────────────────────────

@app.before_request
def _auth_check():
    password = os.environ.get("BACKUPSYS_PASSWORD", "").strip()
    if not password:
        return  # auth disabled — open access
    # Allow health check without auth
    if request.path == "/api/health":
        return
    auth = request.authorization
    if not (auth and auth.password == password):
        return Response(
            "BackupSys — authentication required.",
            401,
            {"WWW-Authenticate": 'Basic realm="BackupSys"'},
        )

def _fire_webhook(result: dict):
    """POST a JSON payload to webhook_url on backup failure (with retry)."""
    try:
        cfg = cfg_mod.load()
        url = cfg.get("webhook_url", "").strip()
        if not url:
            return
        
        logger.info(f"📤 Sending webhook for failed backup: {result.get('watch_name')}")
        
        payload = json.dumps({
            "event":      "backup_failed",
            "watch_name": result.get("watch_name"),
            "watch_id":   result.get("watch_id"),
            "error":      result.get("error"),
            "timestamp":  result.get("timestamp"),
            "backup_id":  result.get("backup_id"),
        }).encode()
        
        # Retry up to 3 times with backoff
        for attempt in range(3):
            try:
                req = urllib.request.Request(url, data=payload,
                                           headers={"Content-Type": "application/json"},
                                           method="POST")
                urllib.request.urlopen(req, timeout=8)
                logger.info(f"✅ Webhook sent successfully for {result.get('watch_name')}")
                return
            except Exception as e:
                if attempt < 2:
                    delay = 2 ** attempt
                    logger.warning(f"⚠️  Webhook attempt {attempt + 1} failed, retrying in {delay}s...")
                    time.sleep(delay)
                else:
                    raise
    except Exception as e:
        logger.warning(f"⚠️  Webhook failed for {result.get('watch_name')}: {e}")

def _get_backup_lock(watch_id: str) -> threading.Lock:
    with _backup_locks_lock:
        if watch_id not in _backup_locks:
            _backup_locks[watch_id] = threading.Lock()
        return _backup_locks[watch_id]

# watch_id → { running, progress, last_result, cancel_requested, started_at, finished_at }
_backup_status: dict = {}
_cancel_flags:  dict = {}
# Concurrent backup limiter
_MAX_CONCURRENT_BACKUPS = 3  # Limit simultaneous backups
_active_backup_count = 0
_backup_semaphore = threading.Semaphore(_MAX_CONCURRENT_BACKUPS)

# Single daemon — controlled by an event
_daemon_thread:   threading.Thread | None = None
_daemon_stop:     threading.Event         = threading.Event()
_daemon_lock      = threading.Lock()
_daemon_next_run: float = 0.0

def cleanup_incomplete_backups(destination: str, older_than_hours: int = 24):
    """Remove backup folders that have no MANIFEST.json (crashed mid-copy)."""
    dest = Path(destination)
    if not dest.exists():
        return
    cutoff = time.time() - (older_than_hours * 3600)
    for d in dest.iterdir():
        if not d.is_dir():
            continue
        manifest_p = d / "MANIFEST.json"
        if not manifest_p.exists():
            try:
                mtime = d.stat().st_mtime
                if mtime < cutoff:
                    shutil.rmtree(str(d), ignore_errors=True)
                    logger.info(f"🧹 Removed incomplete backup: {d.name}")
            except Exception:
                pass


@app.route("/favicon.ico")
def favicon():
    return "", 204

@app.route("/api/watches/<watch_id>/dry-run", methods=["GET"])
@limiter.limit("60 per minute")
def api_backup_dryrun(watch_id):
    """Simulate a backup without copying files."""
    cfg   = cfg_mod.load()
    watch = cfg_mod.get_watch(cfg, watch_id)
    if not watch:
        return jsonify({"error": "Watch not found"}), 404
    
    if watch.get("paused"):
        return jsonify({"error": "Watch is paused"}), 400
    
    try:
        _excl    = list(dict.fromkeys(cfg.get("default_exclude_patterns", []) + watch.get("exclude_patterns", [])))
        new_snap = build_snapshot(watch["path"], previous=watch.get("last_snapshot"), exclude_patterns=_excl)
        old_snap = watch.get("last_snapshot") or {}
        changes = diff_snapshots(old_snap, new_snap)
        
        files_to_copy = [c for c in changes if c["type"] in ("added", "modified")]
        total_size = sum(c.get("size", 0) for c in files_to_copy)
        
        # Count how many old backups would be removed by retention
        from datetime import datetime, timedelta
        cfg2 = cfg_mod.load()
        retention = cfg2.get("retention_days", 30)
        old_backups = list_backups(cfg2["destination"], watch_id)
        cutoff = datetime.now() - timedelta(days=retention)
        files_to_delete = sum(
            1 for b in old_backups
            if b.get("timestamp", "") < cutoff.isoformat()
        )

        return jsonify({
            "ok":               True,
            "watch_name":       watch["name"],
            "files_to_copy":    len(files_to_copy),
            "total_files":      len(new_snap),
            "total_size_human": _human_size(total_size),
            "files_to_delete":  files_to_delete,
            "changes":          changes[:20],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/backup/<watch_id>", methods=["POST"])
@limiter.limit("30 per minute")
def api_backup(watch_id):
    cfg   = cfg_mod.load()
    watch = cfg_mod.get_watch(cfg, watch_id)
    if not watch:
        return jsonify({"error": "Watch not found"}), 404
    
    # ✅ CHECK 1: Is this specific watch already running?
    if _backup_status.get(watch_id, {}).get("running"):
        return jsonify({
            "error": "Backup already running for this watch. Wait for it to complete."
        }), 429
    
    # ✅ CHECK 2: Are we at the concurrent backup limit?
    running_count = sum(1 for s in _backup_status.values() if s.get("running"))
    if running_count >= _MAX_CONCURRENT_BACKUPS:
        return jsonify({
            "error": f"Too many backups running ({running_count}/{_MAX_CONCURRENT_BACKUPS}). Try again later."
        }), 429
    
    if watch.get("paused"):
        return jsonify({"error": "Watch is paused — resume it before backing up"}), 400
    
    # CHECK 3: Fast free-space guard (no directory walk — avoids hanging the button)
    try:
        free_space = shutil.disk_usage(cfg["destination"]).free
        if free_space < 100 * 1024 * 1024:  # < 100 MB free
            return jsonify({
                "error": f"Critically low disk space — only {_human_size(free_space)} free at destination.",
                "free_space": free_space,
            }), 507
    except Exception:
        pass
    
    data        = request.json or {}
    incremental = data.get("incremental", True)
    
    # ✅ Try to start backup (this function has internal locking)
    started     = _trigger_backup(watch_id, incremental)
    if not started:
        return jsonify({"error": "Backup already running for this watch"}), 429
    
    return jsonify({"ok": True, "message": "Backup started"})


# ── Init ──────────────────────────────────────────────────────────────────────

def _init_watchers():
    cfg = cfg_mod.load()
    for w in cfg["watches"]:
        _p = Path(w["path"])
        if _p == _p.anchor or len(_p.parts) <= 1:
            print(f"[watcher] ⚠ Skipping drive/filesystem root: {w['path']} — change this watch path to a subfolder.", flush=True)
            continue
        if w.get("active") and not w.get("paused") and _p.exists():
            watcher.start(w["id"], w["path"], exclude_patterns=w.get("exclude_patterns", []))

threading.Thread(target=_init_watchers, daemon=True, name="init-watchers").start()


def _startup_cleanup():
    try:
        cfg = cfg_mod.load()
        cleanup_incomplete_backups(cfg["destination"], older_than_hours=24)
    except Exception as e:
        logger.warning(f"⚠️  Startup cleanup failed: {e}")

threading.Thread(target=_startup_cleanup, daemon=True, name="startup-cleanup").start()


def _start_daemon_if_enabled():
    """Resume the auto-backup daemon on server boot if it was previously enabled."""
    global _daemon_thread, _daemon_next_run
    cfg = cfg_mod.load()
    # Resume any backups that were queued before restart
    queued = cfg_mod.load_backup_queue()
    if queued:
        logger.info(f"📋 Resuming {len(queued)} queued backup(s) from last session")
        for item in queued:
            _trigger_backup(item["watch_id"], incremental=item.get("incremental", True))
        cfg_mod.clear_backup_queue()
    if not cfg.get("auto_backup"):
        return
    with _daemon_lock:
        if _daemon_thread is not None and _daemon_thread.is_alive():
            return
        _daemon_stop.clear()
        _daemon_thread = threading.Thread(
            target=_daemon_loop, daemon=True, name="auto-backup-daemon"
        )
        _daemon_thread.start()

threading.Thread(target=_start_daemon_if_enabled, daemon=True, name="init-daemon").start()



@app.route("/api/settings/export", methods=["GET"])
def api_export_config():
    """Download config.json for backup/migration."""
    import copy, tempfile
    cfg  = cfg_mod.load()
    safe = copy.deepcopy(cfg)
    if safe.get("email_config"):
        safe["email_config"]["password"] = "***REDACTED***"
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False,
        dir=Path(CONFIG_PATH).parent
    )
    json.dump(safe, tmp, indent=2)
    tmp.close()

    @after_this_request
    def _cleanup(response):
        try: os.unlink(tmp.name)
        except: pass
        return response

    return send_file(
        tmp.name,
        as_attachment=True,
        download_name=f"backupsys_config_{datetime.now().strftime('%Y%m%d')}.json",
        mimetype="application/json"
    )

@app.route("/api/settings/import", methods=["POST"])
def api_import_config():
    """Upload config.json to restore watches/settings."""
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400
    
    try:
        data = json.load(request.files["file"])
        cfg_mod.save(data)
        return jsonify({"ok": True, "message": "Config restored"})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route("/api/backup/<watch_id>/resume", methods=["POST"])
def api_resume_backup(watch_id):
    """Resume a failed or incomplete backup."""
    cfg   = cfg_mod.load()
    watch = cfg_mod.get_watch(cfg, watch_id)
    if not watch:
        return jsonify({"error": "Watch not found"}), 404

    # Check if there's an incomplete backup to clean up
    dest = Path(cfg["destination"])
    incomplete = []
    if dest.exists():
        for d in dest.iterdir():
            if d.is_dir() and not (d / "MANIFEST.json").exists():
                try:
                    created = d.stat().st_ctime
                    if time.time() - created > 3600:  # older than 1 hour
                        incomplete.append(d)
                except Exception:
                    pass

    # Remove old incomplete backups
    for d in incomplete:
        shutil.rmtree(str(d), ignore_errors=True)

    # Then start fresh backup
    started = _trigger_backup(watch_id, incremental=True)
    if not started:
        return jsonify({"error": "Backup already running for this watch"}), 429
    return jsonify({"ok": True, "message": "Backup resumed"})

# ── CORS + cache helpers ──────────────────────────────────────────────────────

@app.after_request
def _headers(response):
    response.headers["Access-Control-Allow-Origin"]  = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    response.headers["Access-Control-Allow-Methods"] = "GET,POST,PATCH,DELETE,OPTIONS"
    response.headers["Vary"]                  = "Origin"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"]        = "SAMEORIGIN"
    response.headers["X-XSS-Protection"]       = "1; mode=block"
    response.headers["Referrer-Policy"]        = "strict-origin-when-cross-origin"
    if request.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"]        = "no-cache"
    return response


# ── Helper: get allowed roots for file editor ─────────────────────────────────

def _get_allowed_roots() -> list:
    """Returns list of watch target paths that are allowed for file operations."""
    cfg = cfg_mod.load()
    return [w["path"] for w in cfg["watches"] if w.get("path")]


def _safe_editor_path(requested: str) -> Optional[str]:
    """Validate a path is within a watch root. Returns safe path or None."""
    roots = _get_allowed_roots()
    if not roots:
        return None
    return safe_path(requested, roots)


# Fix #7 — helper: verify that a path still falls within at least one watch root.
# Used by api_browse() to prevent the parent-dir link from escaping the root.
def _path_within_any_root(p: Path, roots: list) -> bool:
    p_str = str(p.resolve())
    for root in roots:
        try:
            root_str = str(Path(_fix_path(root)).resolve())
            if os.name == "nt":
                if p_str.lower() == root_str.lower() or \
                   p_str.lower().startswith(root_str.lower() + os.sep):
                    return True
            else:
                if p_str == root_str or p_str.startswith(root_str + os.sep):
                    return True
        except Exception:
            continue
    return False


@app.route("/api/system/resources")
def api_system_resources():
    """Monitor CPU, memory, and disk usage."""
    try:
        import psutil
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = shutil.disk_usage(cfg_mod.load()["destination"])
        
        return jsonify({
            "cpu_percent":    cpu_percent,
            "memory_percent": memory.percent,
            "memory_gb":      round(memory.used / (1024**3), 2),
            "disk_free_gb":   round(disk.free / (1024**3), 2),
            "disk_percent":   (disk.used / disk.total) * 100,
        })
    except ImportError:
        return jsonify({"error": "psutil not installed"}), 501
# ── Pages ─────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


# ── Health & system info ──────────────────────────────────────────────────────

@app.route("/api/health")
def api_health():
    # Don't log health checks (they spam)
    return jsonify({"ok": True, "version": "1.9", "ts": time.time()})


@app.route("/api/system/info")
def api_system_info():
    uptime_s = max(0, int(time.time() - _APP_START_TIME))
    h, rem   = divmod(uptime_s, 3600)
    m, s     = divmod(rem, 60)
    info = {
        "version":      "1.9",
        "uptime_s":     uptime_s,
        "uptime_human": f"{h:02d}:{m:02d}:{s:02d}",
        "python":       sys.version.split()[0],
        "platform":     sys.platform,
        "start_ts":     _APP_START_TIME,
    }
    try:
        cfg  = cfg_mod.load()
        disk = shutil.disk_usage(cfg["destination"])
        info["dest_free"]  = _human_size(disk.free)
        info["dest_total"] = _human_size(disk.total)
        info["dest_used_pct"] = round(disk.used / disk.total * 100, 1)
    except Exception:
        pass
    return jsonify(info)

@app.route("/api/logs")
def api_logs():
    n = min(int(request.args.get("lines", 200)), 1000)
    try:
        with open(log_file, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        return jsonify({"lines": [l.rstrip() for l in lines[-n:]], "total": len(lines)})
    except FileNotFoundError:
        return jsonify({"lines": [], "total": 0})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── API: Stats ────────────────────────────────────────────────────────────────

@app.route("/api/stats")
def api_stats():
    cfg     = cfg_mod.load()
    backups = list_backups(cfg["destination"])
    running = {wid: s for wid, s in _backup_status.items() if s.get("running")}
    return jsonify({
        "total_watches":     len(cfg["watches"]),
        "active_watches":    sum(1 for w in cfg["watches"] if w.get("active") and not w.get("paused")),
        "paused_watches":    sum(1 for w in cfg["watches"] if w.get("paused")),
        "total_backups":     len(backups),
        "success_count":     sum(1 for b in backups if b.get("status") == "success"),
        "failed_count":      sum(1 for b in backups if b.get("status") not in ("success", "cancelled")),
        "cancelled_count":   sum(1 for b in backups if b.get("status") == "cancelled"),
        "running_jobs":      len(running),
        "running_watch_ids": list(running.keys()),
        "daemon_next_run":   _daemon_next_run if _daemon_thread and _daemon_thread.is_alive() else None,
    })


# ── API: Dashboard ────────────────────────────────────────────────────────────

@app.route("/api/dashboard")
def api_dashboard():
    cfg     = cfg_mod.load()
    backups = list_backups(cfg["destination"])
    success_count   = sum(1 for b in backups if b.get("status") == "success")
    failed_count    = sum(1 for b in backups if b.get("status") not in ("success", "cancelled"))
    cancelled_count = sum(1 for b in backups if b.get("status") == "cancelled")

    # ✅ STEP 1: Compute watch_disk_usage FIRST
    watch_disk_usage: dict = {}
    for w in cfg["watches"]:
        watch_disk_usage[w["id"]] = _backup_index.get_watch_disk_usage(
            cfg["destination"], w["id"]
        )

    # ✅ STEP 2: Compute watch-based metrics
    watch_backup_counts: dict = {}
    watch_last_status:   dict = {}
    watch_last_ts:       dict = {}
    for b in backups:
        wid = b.get("watch_id", "")
        if wid:
            watch_backup_counts[wid] = watch_backup_counts.get(wid, 0) + 1
            if wid not in watch_last_status:
                watch_last_status[wid] = b.get("status", "unknown")
                watch_last_ts[wid]     = b.get("timestamp")

    # ✅ STEP 3: NOW build watches list (watch_disk_usage is ready)
    watches = []
    for w in cfg["watches"]:
        wd = dict(w)
        wd.setdefault("paused", False)
        wd.setdefault("tags", [])
        wd.setdefault("notes", "")
        _wd_bytes = watch_disk_usage.get(w["id"], 0)
        wd["disk_usage_human"] = _human_size(_wd_bytes) if _wd_bytes > 0 else ""
        
        # ── Path health indicators ──
        path_p = Path(w["path"])
        wd["path_exists"]   = path_p.exists()
        wd["path_readable"] = path_p.exists() and os.access(w["path"], os.R_OK)
        wd["path_writable"] = path_p.exists() and os.access(w["path"], os.W_OK)
        
        wd["pending_count"] = watcher.pending_count(w["id"])
        _last_st  = _backup_status.get(w["id"], {})
        _last_res = _last_st.get("last_result") or {}
        wd["last_error"] = _last_res.get("error") if _last_res.get("status") == "failed" else None
        watches.append(wd)

    # ✅ STEP 4: Destination checks + Largest Backup Indicator
    dest_size_bytes = sum(watch_disk_usage.values())
    dest_path     = Path(cfg["destination"])
    dest_writable = True
    dest_free_human  = "?"
    dest_low_disk    = False
    backup_pct = 0

    if dest_path.exists():
        try:
            dest_writable = os.access(str(dest_path), os.W_OK)
        except Exception:
            dest_writable = False
        try:
            _disk = shutil.disk_usage(str(dest_path))
            dest_free_human = _human_size(_disk.free)
            dest_low_disk   = _disk.free < 500 * 1024 * 1024
            backup_pct = (dest_size_bytes / _disk.total * 100) if _disk.total > 0 else 0
        except Exception:
            backup_pct = 0
    else:
        dest_writable = False
        backup_pct = 0

    # ── Largest watch by disk usage ──
    largest_watch_id, largest_watch_bytes = max(
        watch_disk_usage.items(),
        key=lambda x: x[1],
        default=("none", 0)
    )

    next_run = _daemon_next_run if (_daemon_thread and _daemon_thread.is_alive()) else None

    # ✅ STEP 5: Return (all variables defined)
    return jsonify({
        "watches":             watches,
        "settings":            cfg,
        "total_backups":       len(backups),
        "success_count":       success_count,
        "failed_count":        failed_count,
        "cancelled_count":     cancelled_count,
        "recent_backups":      backups[:8],
        "backup_statuses":     _backup_status,
        "watch_backup_counts": watch_backup_counts,
        "watch_last_status":   watch_last_status,
        "watch_last_ts":       watch_last_ts,
        "watch_disk_usage":    {k: _human_size(v) for k, v in watch_disk_usage.items()},
        "dest_size_bytes":     dest_size_bytes,
        "dest_size_human":     _human_size(dest_size_bytes),
        "dest_writable":       dest_writable,
        "dest_free_human":     dest_free_human,
        "dest_low_disk":       dest_low_disk,
        "dest_backup_pct":     backup_pct,
        "daemon_next_run":     next_run,
        "uptime_s":            int(time.time() - _APP_START_TIME),
        "largest_watch_id":    largest_watch_id,
        "largest_watch_size":  _human_size(largest_watch_bytes),
    })


# ── API: Watches ──────────────────────────────────────────────────────────────

@app.route("/api/watches", methods=["GET"])
def api_get_watches():
    cfg = cfg_mod.load()
    watches = []
    for w in cfg["watches"]:
        w_data = dict(w)
        w_data["is_watching"]     = watcher.is_watching(w["id"])
        w_data["pending_changes"] = watcher.get_pending(w["id"])
        w_data["pending_count"]   = watcher.pending_count(w["id"])
        w_data.setdefault("paused", False)
        w_data.setdefault("tags", [])
        w_data.setdefault("notes", "")
        w_data.setdefault("active", True)
        last_st = _backup_status.get(w["id"], {})
        last_result = last_st.get("last_result") or {}
        w_data["last_error"]      = last_result.get("error") if last_result.get("status") == "failed" else None
        # These were missing — watches page needs them too
        w_data["path_exists"]      = Path(w["path"]).exists()
        _disk_bytes = _backup_index.get_watch_disk_usage(cfg["destination"], w["id"])
        w_data["disk_usage_human"] = _human_size(_disk_bytes) if _disk_bytes > 0 else ""
            
        watches.append(w_data)
    return jsonify(watches)


@app.route("/api/watches", methods=["POST"])
def api_add_watch():
    data  = request.json or {}
    name  = data.get("name", "").strip()
    path  = data.get("path", "").strip()
    wtype = data.get("type", "local")
    tags  = data.get("tags", [])
    notes = data.get("notes", "")
    exclude_patterns = data.get("exclude_patterns", [])
    if not name or not path:
        return jsonify({"error": "Name and path required"}), 400

    # Warn against watching a drive root or filesystem root
    _p = Path(path)
    _parts = _p.parts
    _is_root = (
        len(_parts) == 1                          # Unix /
        or (len(_parts) == 1 and _p.drive)        # Windows C:\
        or (len(_parts) == 2 and _p.drive and _parts[1] == '\\')  # Windows C:\
        or path.rstrip('/\\') == _p.drive.rstrip(':') + ':\\'     # e.g. D:\
        or (len(_p.parts) <= 1 and (_p.drive or path in ('/', '\\')))
    )
    if _is_root:
        return jsonify({
            "error": "Watching a drive root (e.g. D:\\) is not supported — it would scan millions of files. "
                     "Choose a specific subfolder instead, and use exclude_patterns for large subdirs."
        }), 400

    cfg = cfg_mod.load()
    try:
        w = cfg_mod.add_watch(cfg, name, path, wtype, tags, notes, exclude_patterns)
    except ValueError as e:
        return jsonify({"error": str(e)}), 409
    if Path(path).exists() and not w.get("paused"):
        watcher.start(w["id"], path, exclude_patterns=w.get("exclude_patterns", []))
    return jsonify(w)


@app.route("/api/watches/<watch_id>", methods=["DELETE"])
def api_remove_watch(watch_id):
    cfg   = cfg_mod.load()
    watch = cfg_mod.get_watch(cfg, watch_id)
    if not watch:
        return jsonify({"error": "Watch not found"}), 404
    watcher.stop(watch_id)
    cfg_mod.remove_watch(cfg, watch_id)
    _backup_index.invalidate(cfg["destination"])
    with _backup_locks_lock:
        _backup_locks.pop(watch_id, None)
    _cancel_flags.pop(watch_id, None)
    _backup_status.pop(watch_id, None)
    return jsonify({"ok": True})


@app.route("/api/watches/<watch_id>", methods=["PATCH"])
def api_update_watch(watch_id):
    data  = request.json or {}
    cfg   = cfg_mod.load()
    watch = cfg_mod.get_watch(cfg, watch_id)
    if not watch:
        return jsonify({"error": "Watch not found"}), 404
    cfg_mod.update_watch_meta(
        cfg, watch_id,
        name=data.get("name"),
        tags=data.get("tags"),
        notes=data.get("notes"),
        exclude_patterns=data.get("exclude_patterns"),
        max_backups=data.get("max_backups"),
        skip_auto_backup=data.get("skip_auto_backup"),
        reset_snapshot=data.get("reset_snapshot"),
    )
    if data.get("reset_snapshot"):
        return jsonify({"ok": True, "reset": True, "message": "Snapshot cleared — next backup will be full"})
    return jsonify({"ok": True})


@app.route("/api/watches/<watch_id>/pause", methods=["POST"])
def api_pause_watch(watch_id):
    data   = request.json or {}
    paused = data.get("paused", True)
    cfg    = cfg_mod.load()
    watch  = cfg_mod.get_watch(cfg, watch_id)
    if not watch:
        return jsonify({"error": "Watch not found"}), 404
    cfg_mod.pause_watch(cfg, watch_id, paused)
    if paused:
        watcher.stop(watch_id)
    else:
        if Path(watch["path"]).exists():
            watcher.start(watch_id, watch["path"], exclude_patterns=watch.get("exclude_patterns", []))
    return jsonify({"ok": True, "paused": paused})


@app.route("/api/watches/<watch_id>/scan", methods=["POST"])
def api_scan(watch_id):
    cfg   = cfg_mod.load()
    watch = cfg_mod.get_watch(cfg, watch_id)
    if not watch:
        return jsonify({"error": "Watch not found"}), 404
    _excl    = list(dict.fromkeys(cfg.get("default_exclude_patterns", []) + watch.get("exclude_patterns", [])))
    new_snap = build_snapshot(watch["path"], exclude_patterns=_excl)
    old_snap = watch.get("last_snapshot") or {}
    changes  = diff_snapshots(old_snap, new_snap)
    return jsonify({"changes": changes, "total": len(changes)})


@app.route("/api/watches/<watch_id>/stats", methods=["GET"])
def api_watch_stats(watch_id):
    cfg   = cfg_mod.load()
    watch = cfg_mod.get_watch(cfg, watch_id)
    if not watch:
        return jsonify({"error": "Watch not found"}), 404
    stats = get_watch_stats(cfg["destination"], watch_id)
    stats["name"]   = watch.get("name", "")
    stats["path"]   = watch.get("path", "")
    stats["type"]   = watch.get("type", "local")
    stats["tags"]   = watch.get("tags", [])
    stats["paused"] = watch.get("paused", False)
    return jsonify(stats)


@app.route("/api/watches/<watch_id>/disk-usage", methods=["GET"])
def api_watch_disk_usage(watch_id):
    """Return total disk space used by all backups of a watch."""
    cfg   = cfg_mod.load()
    watch = cfg_mod.get_watch(cfg, watch_id)
    if not watch:
        return jsonify({"error": "Watch not found"}), 404
    from backup_engine import _human_size
    bytes_used = _backup_index.get_watch_disk_usage(cfg["destination"], watch_id)
    return jsonify({
        "watch_id":     watch_id,
        "bytes":        bytes_used,
        "human":        _human_size(bytes_used),
        "backup_count": len(list_backups(cfg["destination"], watch_id)),
    })


@app.route("/api/watches/<watch_id>/rename-path", methods=["POST"])
def api_rename_watch_path(watch_id):
    data     = request.json or {}
    new_path = data.get("path", "").strip()
    if not new_path:
        return jsonify({"error": "path required"}), 400
    cfg   = cfg_mod.load()
    watch = cfg_mod.get_watch(cfg, watch_id)
    if not watch:
        return jsonify({"error": "Watch not found"}), 404
    cfg_mod.update_watch_path(cfg, watch_id, new_path)
    watch = cfg_mod.get_watch(cfg_mod.load(), watch_id)
    watcher.restart(watch_id, new_path, exclude_patterns=(watch or {}).get("exclude_patterns", []))
    return jsonify({"ok": True, "new_path": new_path})


@app.route("/api/watches/<watch_id>/duplicate", methods=["POST"])
def api_duplicate_watch(watch_id):
    data     = request.json or {}
    new_name = data.get("name", "").strip()
    new_path = data.get("path", "").strip()
    cfg      = cfg_mod.load()
    watch    = cfg_mod.get_watch(cfg, watch_id)
    if not watch:
        return jsonify({"error": "Watch not found"}), 404
    if not new_name:
        new_name = watch["name"] + " (copy)"
    if not new_path:
        new_path = watch["path"]
    try:
        new_watch = cfg_mod.clone_watch(cfg, watch_id, new_name, new_path)
    except ValueError as e:
        return jsonify({"error": str(e)}), 409
    if not new_watch:
        return jsonify({"error": "Duplicate failed"}), 500
    if Path(new_path).exists():
        watcher.start(new_watch["id"], new_path)
    return jsonify(new_watch)


@app.route("/api/watches/<watch_id>/filediff", methods=["POST"])
@limiter.limit("30 per minute")
def api_file_diff(watch_id):
    import difflib as _dl
    cfg   = cfg_mod.load()
    watch = cfg_mod.get_watch(cfg, watch_id)
    if not watch:
        return jsonify({"error": "Watch not found"}), 404

    raw_path  = (request.json or {}).get("file_path", "")
    file_path = safe_path(raw_path, [watch["path"]])
    if not file_path:
        return jsonify({"error": "Path not allowed"}), 403

    p = Path(file_path)
    dest    = cfg["destination"]
    backups = list_backups(dest, watch_id)
    old_content = ""
    has_backup  = False
    if backups:
        last_backup_dir = backups[0].get("backup_dir", "")
        watch_root      = Path(_fix_path(watch["path"]))
        try:
            rel_path = str(p.relative_to(watch_root))
        except ValueError:
            rel_path = p.name
        backup_file = Path(last_backup_dir) / rel_path
        if backup_file.exists():
            try:
                old_content = backup_file.read_text(encoding="utf-8", errors="replace")
                has_backup  = True
            except Exception:
                old_content = ""

    try:
        new_content = p.read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    old_lines = old_content.splitlines(keepends=True)
    new_lines = new_content.splitlines(keepends=True)
    matcher   = _dl.SequenceMatcher(None, old_lines, new_lines)
    line_changes = []
    for op, i1, i2, j1, j2 in matcher.get_opcodes():
        if op == "equal":
            for k in range(i1, i2):
                line_changes.append({"type": "equal",   "old_ln": k+1,  "new_ln": j1+(k-i1)+1, "text": old_lines[k].rstrip("\n")})
        elif op == "replace":
            for k in range(i1, i2):
                line_changes.append({"type": "removed", "old_ln": k+1,  "new_ln": None,         "text": old_lines[k].rstrip("\n")})
            for k in range(j1, j2):
                line_changes.append({"type": "added",   "old_ln": None, "new_ln": k+1,          "text": new_lines[k].rstrip("\n")})
        elif op == "delete":
            for k in range(i1, i2):
                line_changes.append({"type": "removed", "old_ln": k+1,  "new_ln": None,         "text": old_lines[k].rstrip("\n")})
        elif op == "insert":
            for k in range(j1, j2):
                line_changes.append({"type": "added",   "old_ln": None, "new_ln": k+1,          "text": new_lines[k].rstrip("\n")})

    added   = sum(1 for c in line_changes if c["type"] == "added")
    removed = sum(1 for c in line_changes if c["type"] == "removed")
    return jsonify({
        "file":       p.name,
        "diff":       line_changes,
        "added":      added,
        "removed":    removed,
        "has_backup": has_backup,
    })


# ── API: Backup ───────────────────────────────────────────────────────────────

def _trigger_backup(watch_id: str, incremental: bool = True) -> bool:
    """
    Start a backup in a background thread.
    Uses a per-watch lock to prevent race conditions from simultaneous calls.
    Returns True if backup was started, False if already running.
    """
    global _active_backup_count
    lock = _get_backup_lock(watch_id)

    if not lock.acquire(blocking=False):
        logger.debug(f"⏳ Backup already running for watch: {watch_id}")
        return False

    if _active_backup_count >= _MAX_CONCURRENT_BACKUPS:
        lock.release()
        logger.warning(
            f"⚠️  Concurrent backup limit reached "
            f"({_active_backup_count}/{_MAX_CONCURRENT_BACKUPS}), queuing: {watch_id}"
        )
        return False

    if _backup_status.get(watch_id, {}).get("running"):
        lock.release()
        logger.debug(f"⏳ Backup already running for watch: {watch_id}")
        return False

    _cancel_flags.pop(watch_id, None)

    _backup_status[watch_id] = {
        "running": True,
        "progress": 0,
        "current_file": "",
        "last_result": None,
        "started_at": time.time(),
        "cancel_requested": False,
    }

    lock.release()

    def _run():
        global _active_backup_count

        _backup_semaphore.acquire()
        _active_backup_count += 1
        logger.info(f"🔄 Backup started for watch: {watch_id}")

        try:
            cfg = cfg_mod.load()
            watch = cfg_mod.get_watch(cfg, watch_id)

            if not watch:
                logger.error(f"❌ Watch not found: {watch_id}")
                _backup_status[watch_id] = {
                    "running": False,
                    "progress": 0,
                    "last_result": {"status": "failed", "error": "Watch not found"},
                    "cancel_requested": False,
                }
                return

            _p = Path(watch["path"])
            if len(_p.parts) <= 1 or str(_p) == _p.anchor:
                logger.error(f"❌ Refusing to backup drive root: {watch['path']}")
                _backup_status[watch_id] = {
                    "running": False,
                    "progress": 0,
                    "last_result": {
                        "status": "failed",
                        "error": (
                            f"Refusing to backup drive root '{watch['path']}'. "
                            "Change the path to a specific subfolder."
                        ),
                    },
                    "cancel_requested": False,
                }
                return

            if watch.get("paused"):
                logger.warning(f"⚠️  Watch is paused: {watch_id} ({watch['name']})")
                _backup_status[watch_id] = {
                    "running": False,
                    "progress": 0,
                    "last_result": {"status": "failed", "error": "Watch is paused"},
                    "cancel_requested": False,
                }
                return

            if not Path(watch["path"]).exists():
                logger.error(f"❌ Source path not found: {watch['path']}")
                _backup_status[watch_id] = {
                    "running": False,
                    "progress": 0,
                    "last_result": {"status": "failed", "error": f"Source path not found: {watch['path']}"},
                    "cancel_requested": False,
                }
                return

            Path(cfg["destination"]).mkdir(parents=True, exist_ok=True)

            _mbps = cfg.get("max_backup_mbps", 0)
            throttler = BackupThrottler(max_mbps=_mbps) if _mbps and _mbps > 0 else None

            def _progress_cb(copied: int, total: int, _current: str):
                if _cancel_flags.get(watch_id):
                    _backup_status[watch_id]["cancel_requested"] = True
                    raise InterruptedError("Backup cancelled by user")

                pct = int(copied / max(total, 1) * 100)
                _backup_status[watch_id]["progress"] = pct
                _backup_status[watch_id]["current_file"] = _current

            _global_excl = cfg.get("default_exclude_patterns", [])
            _watch_excl  = watch.get("exclude_patterns", [])
            _all_excl    = list(dict.fromkeys(_global_excl + _watch_excl))  # deduped, order preserved

            _compress = cfg.get("compression_enabled", False) or watch.get("compression", False)
            result = run_backup(
                source=watch["path"],
                destination=cfg["destination"],
                watch_id=watch_id,
                watch_name=watch["name"],
                storage_type=cfg["storage_type"],
                previous_snapshot=watch.get("last_snapshot"),
                incremental=incremental,
                progress_cb=_progress_cb,
                exclude_patterns=_all_excl,
                compress=_compress,
                throttler=throttler,
            )

            was_cancelled = bool(_cancel_flags.pop(watch_id, False))

            if result["status"] == "success" and not was_cancelled:
                logger.info(
                    f"✅ Backup successful: {watch_id} "
                    f"({watch['name']}) - {result['files_copied']} files "
                    f"- {result['total_size']}"
                )

                cfg_mod.update_watch_snapshot(
                    cfg,
                    watch_id,
                    result.get("snapshot", {}),
                    result["timestamp"],
                )

                watcher.clear_pending(watch_id)

                try:
                    cleanup_old_backups(
                        cfg["destination"],
                        cfg.get("retention_days", 30),
                    )
                except Exception as e:
                    logger.warning(f"⚠️  Cleanup failed: {e}")

                try:
                    fresh_watch = cfg_mod.get_watch(cfg_mod.load(), watch_id)
                    max_b = (fresh_watch or {}).get("max_backups", 0)
                    if max_b and max_b > 0:
                        bk_list = list_backups(cfg["destination"], watch_id)
                        to_delete = bk_list[max_b:]
                        for old in to_delete:
                            bd = old.get("backup_dir")
                            if bd:
                                shutil.rmtree(bd, ignore_errors=True)

                        if to_delete:
                            _backup_index.invalidate(cfg["destination"])
                            logger.info(
                                f"🗑 Pruned {len(to_delete)} backup(s) for "
                                f"'{watch.get('name', watch_id)}' (max_backups={max_b})"
                            )
                except Exception as e:
                    logger.warning(f"⚠️  Max-backup pruning failed: {e}")

                # ── Success webhook ────────────────────────────────────────────
                if cfg.get("webhook_on_success") and cfg.get("webhook_url", "").strip():
                    def _fire_success_hook(_r=result):
                        try:
                            import urllib.request as _ur, json as _js
                            payload = _js.dumps({
                                "event":      "backup_success",
                                "watch_name": _r.get("watch_name"),
                                "watch_id":   _r.get("watch_id"),
                                "files":      _r.get("files_copied"),
                                "size":       _r.get("total_size"),
                                "timestamp":  _r.get("timestamp"),
                            }).encode()
                            req = _ur.Request(cfg["webhook_url"], data=payload,
                                              headers={"Content-Type": "application/json"}, method="POST")
                            _ur.urlopen(req, timeout=6)
                        except Exception as _e:
                            logger.warning(f"⚠️ Success webhook failed: {_e}")
                    threading.Thread(target=_fire_success_hook, daemon=True, name=f"hook-ok-{watch_id}").start()

                # ✅ NEW FEATURE — SUCCESS EMAIL NOTIFICATION
                if cfg.get("email_config", {}).get("enabled"):

                    def _notify_success(_result=result):
                        try:
                            import smtplib
                            from email.mime.text import MIMEText

                            ec = cfg.get("email_config", {})

                            msg = MIMEText(
                                f"Backup successful: {_result['watch_name']}\n"
                                f"{_result['files_copied']} files · {_result['total_size']}"
                            )

                            msg["Subject"] = f"✅ BackupSys Success: {_result['watch_name']}"
                            msg["From"] = ec.get("from_addr")
                            msg["To"] = ec.get("to_addr")

                            with smtplib.SMTP(ec.get("smtp_host"), int(ec.get("smtp_port", 587))) as s:
                                s.starttls()
                                s.login(ec.get("username"), ec.get("password"))
                                s.send_message(msg)

                        except Exception as e:
                            logger.warning(f"⚠️ Success email failed: {e}")

                    threading.Thread(
                        target=_notify_success,
                        daemon=True,
                        name=f"notify-success-{watch_id}"
                    ).start()

            elif was_cancelled:
                logger.warning(
                    f"⚠️  Backup cancelled: {watch_id} ({watch['name']})"
                )

            else:
                logger.error(
                    f"❌ Backup failed: {watch_id} "
                    f"({watch['name']}) - {result['error']}"
                )

            _slim_result = {k: v for k, v in result.items() if k != "snapshot"}
            _backup_status[watch_id] = {
                "running": False,
                "progress": 100 if result["status"] == "success" else 0,
                "last_result": _slim_result,
                "finished_at": time.time(),
                "cancel_requested": was_cancelled,
            }

            if result["status"] == "failed":
                def _notify(_result=result):
                    _fire_webhook(_result)
                    _cfg = cfg_mod.load()
                    if _cfg.get("email_config", {}).get("enabled"):
                        _send_email_notification(_result, _cfg["email_config"])

                threading.Thread(
                    target=_notify,
                    daemon=True,
                    name=f"notify-{watch_id}"
                ).start()

                _fresh_cfg = cfg_mod.load()
                if _fresh_cfg.get("auto_retry") and not was_cancelled:
                    delay = _fresh_cfg.get("retry_delay_min", 5) * 60
                    _wname = watch.get("name", watch_id)

                    logger.info(f"🔄 Auto-retry scheduled for '{_wname}' in {delay//60}m")

                    def _schedule_retry(wid=watch_id, delay_s=delay):
                        time.sleep(delay_s)
                        logger.info(f"🔄 Auto-retrying backup: {wid}")
                        _trigger_backup(wid, incremental=True)

                    threading.Thread(
                        target=_schedule_retry,
                        daemon=True,
                        name=f"retry-{watch_id}"
                    ).start()

        finally:
            _active_backup_count -= 1
            _backup_semaphore.release()

            try:
                _backup_index.invalidate(cfg_mod.load()["destination"])
            except Exception:
                pass

            logger.debug(
                f"🧹 Backup cleanup for {watch_id} "
                f"- Active backups: {_active_backup_count}"
            )

    threading.Thread(
        target=_run,
        daemon=True,
        name=f"backup-{watch_id}"
    ).start()

    return True





@app.route("/api/backup/<watch_id>/status", methods=["GET"])
def api_backup_status(watch_id):
    status = _backup_status.get(watch_id, {
        "running": False, "progress": 0, "last_result": None, "cancel_requested": False
    })
    return jsonify(status)


@app.route("/api/backup/<watch_id>/cancel", methods=["POST"])
def api_cancel_backup(watch_id):
    if not _backup_status.get(watch_id, {}).get("running"):
        return jsonify({"error": "No backup running for this watch"}), 400
    _cancel_flags[watch_id] = True
    _backup_status[watch_id]["cancel_requested"] = True
    return jsonify({"ok": True, "message": "Cancel requested — backup will stop after current file"})


@app.route("/api/backup/all", methods=["POST"])
@limiter.limit("20 per minute")
def api_backup_all():
    cfg = cfg_mod.load()

    # Fast disk space guard before starting anything
    try:
        free_space = shutil.disk_usage(cfg["destination"]).free
        if free_space < 100 * 1024 * 1024:
            return jsonify({
                "error": f"Critically low disk space — only {_human_size(free_space)} free at destination. Cannot start bulk backup.",
                "ok": False
            }), 507
    except Exception:
        pass

    started = []
    skipped = []
    for w in cfg["watches"]:
        if not w.get("active") or w.get("paused"):
            continue
        if _trigger_backup(w["id"]):
            started.append(w["id"])
        else:
            skipped.append(w["id"])
    return jsonify({"ok": True, "started": started, "skipped": skipped})


@app.route("/api/backup/all/cancel", methods=["POST"])
def api_cancel_all():
    """Cancel all running backups."""
    cancelled = []
    for watch_id, status in _backup_status.items():
        if status.get("running"):
            _cancel_flags[watch_id] = True
            _backup_status[watch_id]["cancel_requested"] = True
            cancelled.append(watch_id)
    return jsonify({"ok": True, "cancelled": cancelled, "count": len(cancelled)})


@app.route("/api/backup/all/status", methods=["GET"])
def api_backup_all_status():
    """Return status of all watches in one request."""
    return jsonify({
        "statuses":      _backup_status,
        "running_ids":   [wid for wid, s in _backup_status.items() if s.get("running")],
        "running_count": sum(1 for s in _backup_status.values() if s.get("running")),
    })


# ── API: Backup Browse (preview before restore) ───────────────────────────────

@app.route("/api/backup/<backup_id>/browse", methods=["GET"])
def api_backup_browse(backup_id):
    """List all files inside a backup directory for preview before restoring."""
    cfg   = cfg_mod.load()
    found = get_backup_by_id(cfg["destination"], backup_id)
    if not found:
        return jsonify({"error": "Backup not found"}), 404
    backup_dir, _ = found
    result = browse_backup_contents(backup_dir)
    return jsonify(result)


# ── API: History ──────────────────────────────────────────────────────────────

@app.route("/api/history")
def api_history():
    cfg     = cfg_mod.load()
    backups = list_backups(cfg["destination"])

    # Filtering
    q         = request.args.get("q",        "").strip().lower()
    status    = request.args.get("status",   "").strip().lower()
    watch_id  = request.args.get("watch_id", "").strip()
    date_from = request.args.get("from",     "").strip()
    date_to   = request.args.get("to",       "").strip()

    if q:
        backups = [b for b in backups if
            q in (b.get("watch_name") or "").lower() or
            q in (b.get("timestamp")  or "").lower() or
            q in (b.get("backup_hash") or "").lower() or
            q in (b.get("backup_id")  or "").lower() or
            q in (b.get("status")     or "").lower() or
            q in (b.get("user_notes") or "").lower()
        ]
    if status in ("success", "failed", "cancelled"):
        if status == "failed":
            backups = [b for b in backups if b.get("status") not in ("success", "cancelled")]
        else:
            backups = [b for b in backups if b.get("status") == status]
    if watch_id:
        backups = [b for b in backups if b.get("watch_id") == watch_id]
    if date_from:
        backups = [b for b in backups if (b.get("timestamp") or "") >= date_from]
    if date_to:
        backups = [b for b in backups if (b.get("timestamp") or "") <= date_to + "T23:59:59"]

    # Pagination
    total    = len(backups)
    page     = max(1, int(request.args.get("page",     1)))
    per_page = min(200, max(10, int(request.args.get("per_page", 100))))
    start    = (page - 1) * per_page
    end      = start + per_page

    return jsonify({
        "backups":  backups[start:end],
        "total":    total,
        "page":     page,
        "per_page": per_page,
        "pages":    (total + per_page - 1) // per_page if total else 1,
    })


@app.route("/api/history/stats")
def api_history_stats():
    cfg     = cfg_mod.load()
    backups = list_backups(cfg["destination"])

    # Apply same filters as /api/history so stats match the visible list
    q         = request.args.get("q",        "").strip().lower()
    status    = request.args.get("status",   "").strip().lower()
    watch_id  = request.args.get("watch_id", "").strip()
    date_from = request.args.get("from",     "").strip()
    date_to   = request.args.get("to",       "").strip()

    if q:
        backups = [b for b in backups if
            q in (b.get("watch_name") or "").lower() or
            q in (b.get("timestamp")  or "").lower() or
            q in (b.get("backup_hash") or "").lower() or
            q in (b.get("user_notes") or "").lower()]
    if status in ("success", "failed", "cancelled"):
        if status == "failed":
            backups = [b for b in backups if b.get("status") not in ("success", "cancelled")]
        else:
            backups = [b for b in backups if b.get("status") == status]
    if watch_id:
        backups = [b for b in backups if b.get("watch_id") == watch_id]
    if date_from:
        backups = [b for b in backups if (b.get("timestamp") or "") >= date_from]
    if date_to:
        backups = [b for b in backups if (b.get("timestamp") or "") <= date_to + "T23:59:59"]

    total   = len(backups)
    success   = sum(1 for b in backups if b.get("status") == "success")
    failed    = sum(1 for b in backups if b.get("status") not in ("success", "cancelled"))
    cancelled = sum(1 for b in backups if b.get("status") == "cancelled")
    total_files = sum(b.get("files_copied", 0) for b in backups)

    dest_bytes = sum(
        b.get("total_size_bytes", 0)
        for b in backups
        if b.get("status") == "success"
    )

    return jsonify({
        "total":            total,
        "success":          success,
        "failed":           failed,
        "cancelled":        cancelled,
        "total_files":      total_files,
        "total_size_bytes": dest_bytes,
        "total_size_human": _human_size(dest_bytes),
    })


@app.route("/api/history/watches")
def api_history_watches():
    cfg = cfg_mod.load()
    # Start with all configured watches so newly added ones appear immediately
    seen = {w["id"]: w["name"] for w in cfg["watches"]}
    # Also include watches removed from config that still have backup history
    for b in _backup_index.get(cfg["destination"]):
        wid = b.get("watch_id")
        if wid and wid not in seen:
            seen[wid] = b.get("watch_name", wid)
    return jsonify([{"id": k, "name": v} for k, v in seen.items()])


@app.route("/api/history/<backup_id>", methods=["DELETE"])
def api_delete_backup(backup_id):
    cfg   = cfg_mod.load()
    found = get_backup_by_id(cfg["destination"], backup_id)
    if not found:
        return jsonify({"error": "Backup not found"}), 404
    backup_dir, _ = found
    shutil.rmtree(backup_dir, ignore_errors=True)
    _backup_index.invalidate(cfg["destination"])
    return jsonify({"ok": True, "deleted": Path(backup_dir).name})


@app.route("/api/history/<backup_id>/annotate", methods=["POST"])
def api_annotate_backup(backup_id):
    """Add or update user notes on a backup record."""
    data  = request.json or {}
    notes = data.get("notes", "").strip()
    cfg   = cfg_mod.load()
    found = get_backup_by_id(cfg["destination"], backup_id)
    if not found:
        return jsonify({"error": "Backup not found"}), 404
    backup_dir, manifest = found
    manifest["user_notes"] = notes
    manifest_path = Path(backup_dir) / "MANIFEST.json"
    try:
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)

        # Fix #3 — rewrite BACKUP.sha256 so that validate_backup() stays consistent
        # after the manifest is updated. hash_directory() excludes MANIFEST.json,
        # so the stored hash must be recomputed here to stay accurate.
        new_hash = hash_directory(backup_dir)
        with open(Path(backup_dir) / "BACKUP.sha256", "w") as f:
            f.write(f"{new_hash}  {Path(backup_dir).name}\n")

        _backup_index.invalidate(cfg["destination"])
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"ok": True, "notes": notes})


@app.route("/api/history/export-bulk", methods=["POST"])
def api_export_bulk():
    """Export multiple backups as a single zip archive."""
    data       = request.json or {}
    backup_ids = data.get("backup_ids", [])
    if not backup_ids:
        return jsonify({"error": "backup_ids required"}), 400
    if len(backup_ids) > 20:
        return jsonify({"error": "Too many backups selected (max 20)"}), 400

    cfg     = cfg_mod.load()
    tmp_dir = tempfile.mkdtemp(prefix="bsys_bulk_export_")

    try:
        included = 0
        for bid in backup_ids:
            found = get_backup_by_id(cfg["destination"], bid)
            if not found:
                continue
            backup_dir, manifest = found
            sub_name = Path(backup_dir).name
            sub_dir  = Path(tmp_dir) / sub_name
            shutil.copytree(backup_dir, str(sub_dir))
            included += 1

        if not included:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            return jsonify({"error": "None of the specified backups were found"}), 404

        zip_path = Path(tmp_dir) / "bulk_export.zip"
        shutil.make_archive(str(zip_path.with_suffix("")), "zip", tmp_dir, base_dir=None)

        @after_this_request
        def _cleanup(response):
            try:
                shutil.rmtree(tmp_dir, ignore_errors=True)
            except Exception:
                pass
            return response

        from datetime import datetime
        fname = f"backupsys_bulk_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
        return send_file(str(zip_path), as_attachment=True, download_name=fname,
                         mimetype="application/zip")

    except Exception as e:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/history/clear", methods=["POST"])
@limiter.limit("5 per minute")
def api_history_clear():
    cfg     = cfg_mod.load()
    dest    = Path(cfg["destination"])
    deleted = 0
    errors  = []

    if dest.exists():
        for d in list(dest.iterdir()):
            # ✅ ADD: Don't delete symlinks
            if d.is_symlink():
                errors.append(f"Skipped symlink: {d.name}")
                continue
                
            if d.is_dir() and (d / "MANIFEST.json").exists():
                try:
                    shutil.rmtree(str(d), ignore_errors=True)
                    deleted += 1
                except Exception as e:
                    errors.append(str(e))

    for w in cfg["watches"]:
        w["last_backup"]   = None
        w["last_snapshot"] = None
    cfg_mod.save(cfg)
    _backup_index.invalidate(cfg["destination"])

    for w in cfg["watches"]:
        watcher.clear_pending(w["id"])

    # Clear stale in-memory status for non-running backups
    for wid in list(_backup_status.keys()):
        if not _backup_status[wid].get("running"):
            _backup_status.pop(wid, None)

    return jsonify({"ok": True, "deleted": deleted, "errors": errors})


@app.route("/api/validate", methods=["POST"])
def api_validate():
    backup_dir = (request.json or {}).get("backup_dir", "")
    if not backup_dir:
        return jsonify({"error": "backup_dir required"}), 400

    cfg = cfg_mod.load()
    
    # ✅ USE THE NEW VALIDATION FUNCTION (replaces old try-except block)
    if not _validate_backup_dir_safe(backup_dir, cfg["destination"]):
        return jsonify({"error": "backup_dir must be within configured destination (symlink detected)"}), 403

    result = validate_backup(backup_dir)
    return jsonify(result)


# ── API: Export ───────────────────────────────────────────────────────────────

@app.route("/api/export/<backup_id>", methods=["GET"])
def api_export_backup(backup_id):
    cfg   = cfg_mod.load()
    found = get_backup_by_id(cfg["destination"], backup_id)
    if not found:
        return jsonify({"error": "Backup not found"}), 404

    backup_dir, _ = found
    
    # ✅ ADD THIS CHECK
    if not _validate_backup_dir_safe(backup_dir, cfg["destination"]):
        return jsonify({"error": "Backup directory is invalid (symlink detected)"}), 403
    
    tmp_dir       = tempfile.mkdtemp(prefix="backupsys_export_")
    result        = export_backup_zip(backup_dir, tmp_dir)

    if not result["ok"]:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return jsonify({"error": result["error"]}), 500

    @after_this_request
    def _cleanup(response):
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass
        return response

    return send_file(
        result["path"],
        as_attachment=True,
        download_name=result["filename"],
        mimetype="application/zip",
    )




def _validate_backup_dir_safe(backup_dir: str, destination: str) -> bool:
    """Ensure backup_dir is not a symlink and lives within destination."""
    try:
        bd_raw = Path(backup_dir)
        bd = bd_raw.resolve()
        dt = Path(destination).resolve()
        # Check it's actually inside destination
        bd.relative_to(dt)
        if bd_raw.is_symlink():          # ← check bd itself
            return False
        # Check no component is a symlink (symlink traversal protection)
        for parent in bd.parents:
            if parent.is_symlink():
                return False
        return True
    except (ValueError, OSError):
        return False


@app.route("/api/backup/<backup_id>/manifest-export")
def api_export_manifest(backup_id):
    """Export backup file manifest as JSON or CSV"""
    fmt = request.args.get("format", "json").lower()
    cfg = cfg_mod.load()
    found = get_backup_by_id(cfg["destination"], backup_id)
    if not found:
        return jsonify({"error": "Backup not found"}), 404
    
    backup_dir, manifest = found
    files = []
    for entry in manifest.get("changes", []):
        if entry["type"] in ("added", "modified"):
            files.append({
                "path": entry["path"],
                "size": entry.get("size", 0),
                "type": "file"
            })
    
    if fmt == "csv":
        import io
        import csv
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=["path", "size", "type"])
        writer.writeheader()
        writer.writerows(files)
        return output.getvalue(), 200, {"Content-Disposition": f"attachment; filename=manifest_{backup_id}.csv"}
    else:
        return jsonify({
            "backup_id": backup_id,
            "timestamp": manifest.get("timestamp"),
            "watch": manifest.get("watch_name"),
            "files": files,
            "total_files": len(files),
            "total_size": manifest.get("total_size_bytes", 0)
        })

# ── API: Restore ──────────────────────────────────────────────────────────────

@app.route("/api/restore", methods=["POST"])
def api_restore():
    data               = request.json or {}
    backup_dir         = data.get("backup_dir", "").strip()
    target             = data.get("target_path", "").strip()
    incremental_only   = data.get("incremental", False)
    custom_target      = data.get("custom_target", False)

    if not backup_dir:
        return jsonify({"error": "backup_dir required"}), 400

    cfg = cfg_mod.load()
    
    # Validate backup_dir is within configured destination (prevent symlink escape)
    if not _validate_backup_dir_safe(backup_dir, cfg["destination"]):
        return jsonify({"error": "backup_dir must be within configured destination (symlink detected)"}), 403

    # If target not specified, try to get source from manifest
    if not target:
        manifest_p = Path(backup_dir) / "MANIFEST.json"
        if manifest_p.exists():
            try:
                with open(manifest_p) as f:
                    m = json.load(f)
                target = m.get("source", "")
            except Exception:
                pass

    if not target:
        return jsonify({"error": "Could not determine restore target. Provide target_path."}), 400

    # Validate target is within a configured watch path (prevent path traversal on restore)
    # Only enforce for programmatic/manifest restores, not user-chosen custom paths
    allowed_roots = [w["path"] for w in cfg["watches"]]
    from backup_engine import safe_path as _sp
    is_custom = bool(data.get("custom_target", False))
    if allowed_roots and not is_custom and not _sp(target, allowed_roots):
        return jsonify({"error": "Restore target must be within a configured watch path"}), 403

    # Perform restore with incremental option
    result = restore_backup(backup_dir, target, incremental_only=incremental_only)

    # ── NEW: Verify restored files against MANIFEST snapshot ──
    if result.get("ok"):
        try:
            from backup_engine import hash_file
            manifest_p = Path(backup_dir) / "MANIFEST.json"
            if manifest_p.exists():
                with open(manifest_p) as f:
                    manifest = json.load(f)
                snap = manifest.get("snapshot", {})
                mismatches = []

                for rel_path, meta in snap.items():
                    restored_file = Path(target) / rel_path
                    if restored_file.exists():
                        actual   = hash_file(str(restored_file))
                        expected = meta.get("hash")
                        if actual and expected and actual != expected:
                            mismatches.append(rel_path)

                if mismatches:
                    result["verification_failed"] = mismatches
                    result["verified"] = False
                else:
                    result["verified"] = True

        except Exception as e:
            result["verification_warning"] = str(e)

    return jsonify(result)


@app.route("/api/restore/file", methods=["POST"])
def api_restore_single_file():
    """Restore a single file from a backup to its original location."""
    import gzip as _gz

    data      = request.json or {}
    backup_id = data.get("backup_id", "").strip()
    file_path = data.get("file_path", "").strip()

    if not backup_id or not file_path:
        return jsonify({"error": "backup_id and file_path required"}), 400

    cfg   = cfg_mod.load()
    found = get_backup_by_id(cfg["destination"], backup_id)
    if not found:
        return jsonify({"error": "Backup not found"}), 404

    backup_dir, manifest = found

    # ✅ VALIDATE backup_dir itself is safe
    if not _validate_backup_dir_safe(backup_dir, cfg["destination"]):
        return jsonify({"error": "Backup directory is invalid (symlink detected)"}), 403

    src    = Path(backup_dir) / file_path
    src_gz = Path(backup_dir) / (file_path + '.gz')

    if not src.exists() and not src_gz.exists():
        return jsonify({"error": "File not found in backup"}), 404

    original_source = manifest.get("source", "")
    if not original_source:
        return jsonify({"error": "Cannot determine original source path from manifest"}), 400

    target = Path(_fix_path(original_source)) / file_path

    try:
        target.resolve().relative_to(Path(_fix_path(original_source)).resolve())
    except ValueError:
        return jsonify({"error": "Refusing to restore outside original watch path"}), 403

    # Use gz version if that's what exists (compressed backup)
    if not src.exists() and src_gz.exists():
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            with _gz.open(str(src_gz), 'rb') as f_in, open(str(target), 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
            return jsonify({"ok": True, "restored_to": str(target), "file": file_path})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    # Normal uncompressed restore
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(src), str(target))
        return jsonify({"ok": True, "restored_to": str(target), "file": file_path})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/backup/<backup_id>/download-file")
def api_download_backup_file(backup_id):
    """Download a single file directly from a backup."""
    import gzip as _gz
    file_path = request.args.get("path", "").strip()
    if not file_path or ".." in file_path or file_path.startswith(("/", "\\")):
        return jsonify({"error": "Invalid file path"}), 400

    cfg   = cfg_mod.load()
    found = get_backup_by_id(cfg["destination"], backup_id)
    if not found:
        return jsonify({"error": "Backup not found"}), 404

    backup_dir, _ = found
    if not _validate_backup_dir_safe(backup_dir, cfg["destination"]):
        return jsonify({"error": "Invalid backup directory"}), 403

    src    = Path(backup_dir) / file_path
    src_gz = Path(backup_dir) / (file_path + ".gz")

    if not src.exists() and src_gz.exists():
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=Path(file_path).suffix)
        try:
            with _gz.open(str(src_gz), "rb") as f_in:
                shutil.copyfileobj(f_in, tmp)
            tmp.close()
            @after_this_request
            def _cleanup(response):
                try: os.unlink(tmp.name)
                except: pass
                return response
            return send_file(tmp.name, as_attachment=True, download_name=Path(file_path).name)
        except Exception as e:
            tmp.close()
            try: os.unlink(tmp.name)
            except: pass
            return jsonify({"error": str(e)}), 500
    elif src.exists():
        return send_file(str(src), as_attachment=True, download_name=Path(file_path).name)
    else:
        return jsonify({"error": "File not found in backup"}), 404

# ── API: Settings ─────────────────────────────────────────────────────────────

@app.route("/api/settings", methods=["GET"])
def api_get_settings():
    cfg = cfg_mod.load()
    ec = dict(cfg.get("email_config", {}))
    if ec.get("password"):
        ec["password"] = "••••••••"   # never send raw password to browser
    return jsonify({
        "destination":         cfg["destination"],
        "storage_type":        cfg["storage_type"],
        "auto_backup":         cfg["auto_backup"],
        "interval_min":        cfg["interval_min"],
        "retention_days":      cfg["retention_days"],
        "webhook_url":         cfg.get("webhook_url", ""),
        "webhook_on_success":  cfg.get("webhook_on_success", False),
        "compression_enabled": cfg.get("compression_enabled", False),
        "auto_retry":          cfg.get("auto_retry", False),
        "retry_delay_min":     cfg.get("retry_delay_min", 5),
        "max_backup_mbps":     cfg.get("max_backup_mbps", 0),
        "email_config":        ec,
    })

def _daemon_loop():
    global _daemon_next_run
    while not _daemon_stop.is_set():
        sleep_secs = 1800
        try:
            c          = cfg_mod.load()
            sleep_secs = max(60, c.get("interval_min", 30) * 60)
            if not c.get("auto_backup"):
                _daemon_next_run = 0.0
                return
            now = time.time()
            for _wi, w in enumerate(c["watches"]):
                _p = Path(w["path"])
                if str(_p) == _p.anchor or len(_p.parts) <= 1:
                    continue  # never auto-backup a drive root
                if w.get("active") and not w.get("paused") and not w.get("skip_auto_backup"):
                    if _backup_status.get(w["id"], {}).get("running"):
                        continue
                    # Skip if last backup was within the current interval (prevents restart storm)
                    last_ts = w.get("last_backup")
                    if last_ts:
                        try:
                            elapsed = now - datetime.fromisoformat(last_ts).timestamp()
                            if elapsed < sleep_secs:
                                logger.debug(
                                    f"⏭ Skipping '{w['name']}' — "
                                    f"last backup {int(elapsed//60)}m ago, "
                                    f"interval is {int(sleep_secs//60)}m"
                                )
                                continue
                        except Exception as e:
                            logger.warning(f"⚠ Could not parse last_backup timestamp for '{w['name']}': {e}")
                    else:
                        logger.info(f"🆕 '{w['name']}' has never been backed up — triggering first backup")
                    # Stagger startup bursts: add a small per-watch delay so all
                    # watches don't hammer disk simultaneously on server restart.
                    _stagger = min(_wi * 5, 90)  # spread each watch 5s apart, cap at 90s
                    if _stagger:
                        def _delayed(wid=w["id"], delay=_stagger):
                            time.sleep(delay)
                            _trigger_backup(wid, incremental=True)
                        threading.Thread(target=_delayed, daemon=True, name=f"stagger-{w['id']}").start()
                    else:
                        _trigger_backup(w["id"], incremental=True)
            _daemon_next_run = time.time() + sleep_secs
        except Exception as e:
            logger.error(f"[daemon] ⚠ Error in backup loop: {e}")
        _daemon_stop.wait(sleep_secs)
    _daemon_next_run = 0.0

@app.route("/api/settings/test-webhook", methods=["POST"])
def api_test_webhook():
    data = request.json or {}
    url  = data.get("url", "").strip() or cfg_mod.load().get("webhook_url", "").strip()
    if not url:
        return jsonify({"error": "No webhook URL provided"}), 400
    try:
        payload = json.dumps({
            "event":     "test",
            "message":   "BackupSys webhook test — if you see this, it works!",
            "timestamp": time.time(),
        }).encode()
        req = urllib.request.Request(
            url, data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            return jsonify({"ok": True, "http_status": resp.status})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/settings/test-email", methods=["POST"])
def api_test_email():
    data = request.json or {}
    ec   = data.get("email_config") or {}
    if ec:
        _MASK = "••••••••"
        _stored = cfg_mod.load().get("email_config", {})
        if ec.get("password", "") in ("", _MASK):
            ec = dict(ec)
            ec["password"] = _stored.get("password", "")
    else:
        ec = cfg_mod.load().get("email_config", {})
    if not ec.get("enabled"):
        return jsonify({"error": "Email notifications are not enabled"}), 400
    try:
        import smtplib
        from email.mime.text import MIMEText
        msg            = MIMEText("BackupSys email test — if you see this, it works!")
        msg["Subject"] = "BackupSys — Email Test"
        msg["From"]    = ec.get("from_addr", "")
        msg["To"]      = ec.get("to_addr", "")
        with smtplib.SMTP(ec.get("smtp_host", ""), int(ec.get("smtp_port", 587))) as s:
            s.starttls()
            s.login(ec.get("username", ""), ec.get("password", ""))
            s.send_message(msg)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/settings", methods=["POST"])
def api_save_settings():
    global _daemon_thread, _daemon_next_run

    data = request.json or {}
    cfg  = cfg_mod.load()
    cfg["compression_enabled"] = data.get("compression_enabled", cfg.get("compression_enabled", False))
    cfg["destination"]  = data.get("destination",  cfg["destination"])
    cfg["storage_type"] = data.get("storage_type", cfg["storage_type"])
    cfg["auto_backup"]  = data.get("auto_backup",  cfg["auto_backup"])

    try:
        interval = int(data.get("interval_min", cfg["interval_min"]))
        cfg["interval_min"] = max(1, interval)
    except (ValueError, TypeError):
        pass

    try:
        retention = int(data.get("retention_days", cfg["retention_days"]))
        cfg["retention_days"] = max(1, retention)
    except (ValueError, TypeError):
        pass

    cfg["webhook_url"]     = data.get("webhook_url", cfg.get("webhook_url", "")).strip()
    cfg["webhook_on_success"] = bool(data.get("webhook_on_success", cfg.get("webhook_on_success", False)))
    cfg["auto_retry"]      = bool(data.get("auto_retry", cfg.get("auto_retry", False)))
    cfg["retry_delay_min"] = max(1, int(data.get("retry_delay_min", cfg.get("retry_delay_min", 5))))

    # ── Bandwidth throttle ────────────────────────────────────────────────────
    mbps = data.get("max_backup_mbps", cfg.get("max_backup_mbps", 0))
    cfg["max_backup_mbps"] = max(0.0, float(mbps)) if mbps else 0.0

    ec = data.get("email_config")
    if ec is not None:
        _MASK         = "••••••••"
        _submitted_pw = ec.get("password", "")
        _existing_pw  = cfg.get("email_config", {}).get("password", "")
        cfg["email_config"] = {
            "enabled":   bool(ec.get("enabled", False)),
            "smtp_host": ec.get("smtp_host", "").strip(),
            "smtp_port": int(ec.get("smtp_port", 587)),
            "username":  ec.get("username", "").strip(),
            "password":  _existing_pw if _submitted_pw in ("", _MASK) else _submitted_pw,
            "from_addr": ec.get("from_addr", "").strip(),
            "to_addr":   ec.get("to_addr", "").strip(),
        }

    # Gate cleanup on destination/retention actually changing (read old values before save)
    old_cfg = cfg_mod.load()
    if (data.get("destination") != old_cfg.get("destination") or
            data.get("retention_days") != old_cfg.get("retention_days")):
        threading.Thread(
            target=cleanup_old_backups,
            args=(cfg["destination"], cfg["retention_days"]),
            daemon=True,
        ).start()

    cfg_mod.save(cfg)
    _backup_index.invalidate(cfg["destination"])

    # ✅ ADD: Clean up incomplete backups (older than 24 hours)
    threading.Thread(
        target=cleanup_incomplete_backups,
        args=(cfg["destination"], 24),
        daemon=True,
        name="cleanup-incomplete-backups",
    ).start()

    with _daemon_lock:
        if cfg["auto_backup"]:
            if _daemon_thread is not None and _daemon_thread.is_alive():
                _daemon_stop.set()
                _daemon_thread.join(timeout=2)

            _daemon_stop.clear()

            _daemon_thread = threading.Thread(target=_daemon_loop, daemon=True, name="auto-backup-daemon")
            _daemon_thread.start()
        else:
            _daemon_stop.set()
            _daemon_next_run = 0.0

    return jsonify({"ok": True})


@app.route("/api/settings/validate-dest", methods=["POST"])
def api_validate_dest():
    path = (request.json or {}).get("path", "")
    if not path:
        return jsonify({"ok": False, "error": "Path required"}), 400
    return jsonify(cfg_mod.validate_destination(path))


# ── API: File Editor ──────────────────────────────────────────────────────────

EDITABLE_EXTENSIONS = {
    ".txt", ".md", ".json", ".js", ".ts", ".jsx", ".tsx",
    ".css", ".html", ".htm", ".xml", ".csv", ".yaml", ".yml",
    ".py", ".env", ".ini", ".cfg", ".log", ".sh", ".bat",
    ".sql", ".php", ".rb", ".java", ".c", ".cpp", ".h",
    ".toml", ".lock", ".gitignore", ".dockerignore", ".editorconfig",
}


def _is_editable(path: str) -> bool:
    p = Path(path)
    return p.suffix.lower() in EDITABLE_EXTENSIONS or p.suffix == ""


@app.route("/api/files/browse", methods=["POST"])
def api_browse():
    raw_path = (request.json or {}).get("path", "")
    path = _fix_path(raw_path)
    p    = Path(path)
    if not p.exists():
        return jsonify({"error": f"Path not found: {path}"}), 404
    if p.is_file():
        return jsonify({"error": "Path is a file, not a folder"}), 400

    allowed_roots = _get_allowed_roots()

    # Security: must be within a watch root or BE a watch root
    is_allowed = False
    for root in allowed_roots:
        try:
            root_p = Path(_fix_path(root)).resolve()
            if str(p.resolve()) == str(root_p):
                is_allowed = True
                break
            p.resolve().relative_to(root_p)
            is_allowed = True
            break
        except ValueError:
            continue
    if not is_allowed and allowed_roots:
        return jsonify({"error": "Path not within any watch target"}), 403

    MAX_BROWSE_ITEMS = 1500
    items = []
    try:
        all_raw = list(p.iterdir())
        truncated = len(all_raw) > MAX_BROWSE_ITEMS
        for item in sorted(all_raw[:MAX_BROWSE_ITEMS], key=lambda x: (x.is_file(), x.name.lower())):
            try:
                stat = item.stat()
                size = stat.st_size if item.is_file() else 0
                items.append({
                    "name":      item.name,
                    "path":      str(item),
                    "is_dir":    item.is_dir(),
                    "is_file":   item.is_file(),
                    "size":      size,
                    "editable":  _is_editable(str(item)) if item.is_file() else False,
                    "too_large": size > MAX_EDIT_BYTES if item.is_file() else False,
                    "ext":       item.suffix.lower(),
                    "mtime":     stat.st_mtime if item.is_file() else 0,
                })
            except Exception:
                pass
    except PermissionError:
        return jsonify({"error": "Permission denied"}), 403


    raw_parent = p.parent
    if raw_parent != p and _path_within_any_root(raw_parent, allowed_roots):
        parent = str(raw_parent)
    else:
        parent = None

    return jsonify({"path": str(p), "parent": parent, "items": items,
                    "truncated": truncated if 'truncated' in dir() else False})


@app.route("/api/files/read", methods=["POST"])
def api_read_file():
    raw_path = (request.json or {}).get("path", "")
    if not raw_path:
        return jsonify({"error": "path required"}), 400
    path = safe_path(raw_path, _get_allowed_roots())
    if not path:
        return jsonify({"error": "Path not within any watch target"}), 403
    if not _is_editable(path):
        return jsonify({"error": "File type not editable in browser"}), 400
    try:
        data = read_file_safe(path)
        return jsonify(data)
    except FileNotFoundError as e:
        return jsonify({"error": str(e)}), 404
    except ValueError as e:
        return jsonify({"error": str(e), "too_large": True}), 413
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/files/save", methods=["POST"])
@limiter.limit("60 per minute")
def api_save_file():
    import difflib as _dl
    
    data        = request.json or {}
    raw_path    = data.get("path", "")
    new_content = data.get("content", "")

    # Validate content size before processing
    if len(new_content.encode('utf-8')) > MAX_EDIT_BYTES:
        return jsonify({"error": f"File content too large (max {MAX_EDIT_BYTES // (1024 * 1024)}MB)"}), 413

    if not raw_path:
        return jsonify({"error": "path required"}), 400

    path = safe_path(raw_path, _get_allowed_roots())
    if not path:
        return jsonify({"error": "Path not within any watch target"}), 403

    p = Path(path)
    if not p.exists():
        return jsonify({"error": "File not found"}), 404

    try:
        old_content = p.read_text(encoding="utf-8", errors="replace")
        old_hash    = hash_file(str(p))
        p.write_text(new_content, encoding="utf-8")
        new_hash = hash_file(str(p))

        old_lines = old_content.splitlines(keepends=True)
        new_lines = new_content.splitlines(keepends=True)
        matcher   = _dl.SequenceMatcher(None, old_lines, new_lines)
        line_changes = []
        for op, i1, i2, j1, j2 in matcher.get_opcodes():
            if op == "equal":
                for k in range(i1, i2):
                    line_changes.append({"type": "equal",   "old_ln": k+1,  "new_ln": j1+(k-i1)+1, "text": old_lines[k].rstrip("\n")})
            elif op == "replace":
                for k in range(i1, i2):
                    line_changes.append({"type": "removed", "old_ln": k+1,  "new_ln": None,         "text": old_lines[k].rstrip("\n")})
                for k in range(j1, j2):
                    line_changes.append({"type": "added",   "old_ln": None, "new_ln": k+1,          "text": new_lines[k].rstrip("\n")})
            elif op == "delete":
                for k in range(i1, i2):
                    line_changes.append({"type": "removed", "old_ln": k+1,  "new_ln": None,         "text": old_lines[k].rstrip("\n")})
            elif op == "insert":
                for k in range(j1, j2):
                    line_changes.append({"type": "added",   "old_ln": None, "new_ln": k+1,          "text": new_lines[k].rstrip("\n")})

        added   = sum(1 for c in line_changes if c["type"] == "added")
        removed = sum(1 for c in line_changes if c["type"] == "removed")

        return jsonify({
            "ok":            True,
            "old_hash":      old_hash,
            "new_hash":      new_hash,
            "changed":       old_hash != new_hash,
            "lines_added":   added,
            "lines_removed": removed,
            "diff":          line_changes,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/files/create", methods=["POST"])
def api_create_file():
    data = request.json or {}
    raw  = data.get("path", "")
    if not raw:
        return jsonify({"error": "path required"}), 400
    path = safe_path(raw, _get_allowed_roots())
    if not path:
        return jsonify({"error": "Path not within any watch target"}), 403
    p = Path(path)
    if p.exists():
        return jsonify({"error": "File already exists"}), 400
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("", encoding="utf-8")
        return jsonify({"ok": True, "path": str(p)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

        
@app.route("/api/files/upload", methods=["POST"])
@limiter.limit("20 per minute")
def api_upload_file():
    dest_dir = request.form.get("path", "")
    if not dest_dir:
        return jsonify({"error": "path required"}), 400
    safe = safe_path(dest_dir, _get_allowed_roots())
    if not safe:
        return jsonify({"error": "Path not within any watch target"}), 403
    files = request.files.getlist("files")
    if not files:
        return jsonify({"error": "No files provided"}), 400

    _MAX_UPLOAD_BYTES = 100 * 1024 * 1024  # 100 MB per file

    uploaded, errors = [], []
    for f in files:
        fname = Path(f.filename).name if f.filename else ""
        if not fname:
            continue
        if "/" in fname or "\\" in fname:
            errors.append(f"{fname}: invalid filename")
            continue
        # Size check before saving
        f.seek(0, 2)
        fsize = f.tell()
        f.seek(0)
        if fsize > _MAX_UPLOAD_BYTES:
            errors.append(f"{fname}: too large (max 100 MB)")
            continue
        dest = Path(safe) / fname
        if dest.exists():
            errors.append(f"{fname}: already exists")
            continue
        try:
            f.save(str(dest))
            uploaded.append(fname)
        except Exception as e:
            errors.append(f"{fname}: {e}")

    return jsonify({"ok": True, "uploaded": uploaded, "errors": errors})

@app.route("/api/files/mkdir", methods=["POST"])
def api_mkdir():
    data = request.json or {}
    raw  = data.get("path", "")
    if not raw:
        return jsonify({"error": "path required"}), 400
    path = safe_path(raw, _get_allowed_roots())
    if not path:
        return jsonify({"error": "Path not within any watch target"}), 403
    p = Path(path)
    if p.exists():
        return jsonify({"error": "Folder already exists"}), 400
    try:
        p.mkdir(parents=True, exist_ok=False)
        return jsonify({"ok": True, "path": str(p)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/files/delete", methods=["POST"])
def api_delete_file():
    data = request.json or {}
    raw  = data.get("path", "")
    if not raw:
        return jsonify({"error": "path required"}), 400
    path = safe_path(raw, _get_allowed_roots())
    if not path:
        return jsonify({"error": "Path not within any watch target"}), 403
    p = Path(path)
    if not p.exists():
        return jsonify({"error": "File not found"}), 404
    if p.is_dir():
        return jsonify({"error": "Cannot delete directories via this endpoint"}), 400
    try:
        p.unlink()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/files/rename", methods=["POST"])
def api_rename_file():
    data     = request.json or {}
    old_path = data.get("old_path", "")
    new_name = data.get("new_name", "").strip()
    if not old_path or not new_name:
        return jsonify({"error": "old_path and new_name required"}), 400
    old_safe = safe_path(old_path, _get_allowed_roots())
    if not old_safe:
        return jsonify({"error": "Path not within any watch target"}), 403
    if "/" in new_name or "\\" in new_name:
        return jsonify({"error": "new_name must be a filename, not a path"}), 400
    p = Path(old_safe)
    if not p.exists():
        return jsonify({"error": "File not found"}), 404
    new_path = p.parent / new_name
    if new_path.exists():
        return jsonify({"error": "A file with that name already exists"}), 409
    try:
        p.rename(new_path)
        return jsonify({"ok": True, "new_path": str(new_path)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/history/activity")
def api_history_activity():
    from collections import defaultdict
    from datetime import datetime, timedelta
    days = min(90, int(request.args.get("days", 14)))
    cfg  = cfg_mod.load()
    backups = list_backups(cfg["destination"])
    cutoff  = (datetime.now() - timedelta(days=days)).isoformat()
    counts  = defaultdict(lambda: {"success": 0, "failed": 0, "cancelled": 0})
    for b in backups:
        ts = b.get("timestamp", "")
        if ts < cutoff:
            continue
        day    = ts[:10]
        status = b.get("status", "failed")
        if status == "success":       counts[day]["success"]   += 1
        elif status == "cancelled":   counts[day]["cancelled"] += 1
        else:                         counts[day]["failed"]     += 1
    return jsonify({"days": days, "activity": dict(counts)})

# ── Run ───────────────────────────────────────────────────────────────────────

def _graceful_shutdown():
    """Clean up daemon and watchers on exit."""
    global _daemon_thread
    print("\n[app] Shutting down gracefully...", flush=True)
    running = [{"watch_id": wid, "incremental": True}
               for wid, s in _backup_status.items() if s.get("running")]
    if running:
        cfg_mod.save_backup_queue(running)
        print(f"[app] Queued {len(running)} in-progress backup(s) for next start.", flush=True)
    _daemon_stop.set()
    watcher.stop_all()
    if _daemon_thread and _daemon_thread.is_alive():
        _daemon_thread.join(timeout=3)
    print("[app] Shutdown complete.", flush=True)

import atexit
atexit.register(_graceful_shutdown)
def _periodic_maintenance():
    """Run background maintenance tasks every 6 hours."""
    while True:
        time.sleep(6 * 3600)
        try:
            cfg = cfg_mod.load()
            cleanup_incomplete_backups(cfg["destination"], older_than_hours=24)
            logger.info("🧹 Periodic maintenance complete")
        except Exception as e:
            logger.warning(f"⚠️  Periodic maintenance failed: {e}")

threading.Thread(target=_periodic_maintenance, daemon=True, name="maintenance").start()

if __name__ == "__main__":
    print("\n" + "=" * 52)
    print("  🛡  BackupSys Web v1.9 is running!")
    print("  👉  Open http://localhost:5000 in your browser")
    print("=" * 52 + "\n")
    try:
        app.run(debug=False, port=5000, threaded=True)
    except KeyboardInterrupt:
        _graceful_shutdown()