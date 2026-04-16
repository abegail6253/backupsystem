"""
BackupSys - Config Manager  v1.9
Loads, saves, and validates the configuration file (config.json).
Includes persistent backup queue for disaster recovery.

Changes vs v1.8:
- ADDED: Persistent backup queue (backup_queue.json)
- ADDED: save_backup_queue() and load_backup_queue() functions
- CONFIRMED: skip_auto_backup per-watch flag support
"""

import json
import os
import random
import string
import sys
import tempfile
import time
import threading
from pathlib import Path
from typing import List, Optional

import logging
logger = logging.getLogger(__name__)

CONFIG_PATH   = Path(__file__).parent / "config.json"
QUEUE_PATH    = Path(os.environ.get("BACKUPSYS_DATA_DIR", Path(__file__).parent)) / "backup_queue.json"
HISTORY_PATH  = Path(os.environ.get("BACKUPSYS_DATA_DIR", Path(__file__).parent)) / "history.json"
SNAPSHOTS_DIR = Path(os.environ.get("BACKUPSYS_DATA_DIR", Path(__file__).parent)) / "snapshots"  # ← now env-var aware like QUEUE_PATH / HISTORY_PATH
_save_lock    = threading.Lock()

DEFAULT_CONFIG = {
    "destination":         "./backups",
    "storage_type":        "local",
    "dest_type":           "local",
    "dest_sftp":           {},
    "dest_smb":            {},
    "dest_ftp":            {},
    "dest_https":          {},
    "auto_backup":         False,
    "interval_min":        30,
    "interval_unit":       "minutes",
    "retention_days":      30,
    "webhook_url":         "",
    "webhook_on_success":  False,
    "compression_enabled": False,
    "auto_retry":          False,
    "retry_delay_min":     5,
    "max_backup_mbps":     0.0,
    "backup_schedule_times": [],   # e.g. ["02:00", "14:00"] — run at specific times of day
    "email_config": {
        "enabled":           False,
        "notify_on_success": False,   # ← send email on successful backup
        "notify_on_failure": True,    # ← send email on failed backup (default on when email enabled)
        "smtp_use_ssl":      False,
        "smtp_host":         "",
        "smtp_port":         587,
        "username":          "",
        "password":          "",
        "from_addr":         "",
        "to_addr":           "",
    },
    "default_exclude_patterns": [
        ".git", ".gitignore", "__pycache__", "node_modules",
        "*.pyc", "*.tmp", ".DS_Store", "Thumbs.db",
        "*.lock", "*.db", "*.sqlite*", ".env",
    ],
    "watches":        [],
}

WATCH_TEMPLATE = {
    "id":               "",
    "name":             "",
    "path":             "",
    "type":             "local",
    "active":           True,
    "paused":           False,
    "tags":             [],
    "notes":            "",
    "last_backup":      None,
    "last_snapshot":    None,
    "exclude_patterns": [],
    "backup_count":     0,
    "last_backup_size": 0,
    "compression":      False,
    "max_backups":      0,        # 0 = unlimited; prunes oldest first after each backup
    "skip_auto_backup": False,    # exclude from daemon without pausing manual backups
    "color":            "",       # optional color label (hex or empty)
    "interval_min":     0,
    "retention_days":   0,        # 0 = use global interval; >0 = watch-specific interval
    "cloud_config":     {},       # S3/cloud credentials per-watch
    "encrypt_key":      "",       # Fernet key (44 chars, URL-safe base64); empty = no encryption
}


# ─── NEW: Robust Path Normalizer ─────────────────────────────────────────────

def _norm_path(path: str) -> str:
    """
    Normalize paths safely even if they don't exist.
    On Windows, comparison is case-insensitive.
    """
    try:
        p = Path(path)
        normalised = (
            str(p.resolve()) if p.exists()
            else os.path.normpath(os.path.abspath(str(p)))
        )
        return normalised.lower() if os.name == "nt" else normalised
    except Exception:
        return path


# ─── NEW: Persistent Backup Queue ─────────────────────────────────────────────

def save_backup_queue(queue: list):
    """
    Persist backup queue to disk.
    Called when a backup is queued to ensure it survives app restart.
    """
    try:
        with _save_lock:
            QUEUE_PATH.parent.mkdir(parents=True, exist_ok=True)
            with open(QUEUE_PATH, "w", encoding="utf-8") as f:
                json.dump(queue, f, indent=2)
            logger.info(f"💾 Backup queue saved: {len(queue)} item(s)")
    except Exception as e:
        logger.warning(f"⚠️  Failed to save backup queue: {e}")


def load_backup_queue() -> list:
    """
    Restore backup queue from disk.
    Called on app startup to resume queued backups.
    """
    if QUEUE_PATH.exists():
        try:
            with open(QUEUE_PATH, "r", encoding="utf-8") as f:
                queue = json.load(f)
            logger.info(f"📋 Backup queue restored: {len(queue)} item(s)")
            return queue
        except json.JSONDecodeError:
            logger.warning(f"⚠️  Backup queue corrupted, starting fresh")
            return []
        except Exception as e:
            logger.warning(f"⚠️  Failed to load backup queue: {e}")
            return []
    return []


def clear_backup_queue():
    """
    Clear the persisted backup queue.
    Called after all queued backups are processed or on manual reset.
    """
    try:
        if QUEUE_PATH.exists():
            QUEUE_PATH.unlink()
            logger.info(f"🗑 Backup queue cleared")
    except Exception as e:
        logger.warning(f"⚠️  Failed to clear backup queue: {e}")


# ─── Persistent Change History ─────────────────────────────────────────────────

def save_history(entries: list):
    """
    Persist change history log to disk (capped at 5000 entries).
    Called periodically from the desktop app when new entries arrive.
    """
    try:
        HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=HISTORY_PATH.parent, suffix=".tmp")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(entries[-5000:], f)
        os.replace(tmp, HISTORY_PATH)
    except Exception as e:
        logger.warning(f"⚠️ Failed to save history: {e}")


def load_history() -> list:
    """
    Load persisted change history from disk.
    Called once on app startup.
    """
    if HISTORY_PATH.exists():
        try:
            with open(HISTORY_PATH, encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"⚠️ Failed to load history: {e}")
    return []


# ─── Load / Save ──────────────────────────────────────────────────────────────

_config_cache: dict = {"cfg": None, "mtime": 0.0}
_cache_lock = threading.Lock()  # separate lock so reads don't block saves

def load() -> dict:
    global _config_cache
    try:
        mtime = CONFIG_PATH.stat().st_mtime if CONFIG_PATH.exists() else 0.0
        with _cache_lock:
            if _config_cache["cfg"] is not None and mtime == _config_cache["mtime"]:
                return _config_cache["cfg"]
    except Exception:
        pass
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH, encoding="utf-8") as f:
                cfg = json.load(f)

            for k, v in DEFAULT_CONFIG.items():
                if k not in cfg:
                    cfg[k] = v

            # Merge missing email_config sub-keys from defaults
            default_ec = DEFAULT_CONFIG.get("email_config", {})
            cfg_ec = cfg.setdefault("email_config", {})
            for k, v in default_ec.items():
                if k not in cfg_ec:
                    cfg_ec[k] = v

            # Resolve relative destination paths to absolute (relative to config.json location)
            _dest = cfg.get("destination", "./backups")
            if _dest and not os.path.isabs(_dest):
                cfg["destination"] = str((CONFIG_PATH.parent / _dest).resolve())

            # Guard against hand-edited bad values that would spin the daemon
            cfg["interval_min"]   = max(1, int(cfg.get("interval_min",   30)))
            cfg["retention_days"] = max(1, int(cfg.get("retention_days", 30)))

            # Ensure schedule list is always a list of "HH:MM" strings
            sched = cfg.get("backup_schedule_times", [])
            if not isinstance(sched, list):
                sched = []
            cfg["backup_schedule_times"] = [
                s for s in sched if isinstance(s, str) and len(s) == 5
            ]

            migrated = False
            for w in cfg.get("watches", []):
                for k, v in WATCH_TEMPLATE.items():
                    if k not in w:
                        w[k] = list(v) if isinstance(v, list) else v
                        migrated = True

            if migrated:
                try:
                    save(cfg)
                except Exception:
                    pass

            # Allow overriding email password via env var (safer than plaintext in config)
            env_pw = os.environ.get("BACKUPSYS_EMAIL_PASSWORD", "").strip()
            if env_pw and "email_config" in cfg:
                cfg["email_config"]["password"] = env_pw

            # Per-watch encryption key override via environment variable.
            # Set BACKUPSYS_ENCRYPT_KEY_<WATCH_ID>=<fernet_key> to avoid storing
            # keys in plain text in config.json.
            # Example: BACKUPSYS_ENCRYPT_KEY_w_kkxjf0=your44charbase64key
            for w in cfg.get("watches", []):
                wid     = w.get("id", "")
                env_key = os.environ.get(f"BACKUPSYS_ENCRYPT_KEY_{wid}", "").strip()
                if env_key:
                    w["encrypt_key"] = env_key

            # Global fallback key (applies only to watches that have no key set)
            global_env_key = os.environ.get("BACKUPSYS_ENCRYPT_KEY_DEFAULT", "").strip()
            if global_env_key:
                for w in cfg.get("watches", []):
                    if not w.get("encrypt_key"):
                        w["encrypt_key"] = global_env_key

            with _cache_lock:
                _config_cache["cfg"] = cfg
                _config_cache["mtime"] = CONFIG_PATH.stat().st_mtime if CONFIG_PATH.exists() else 0.0
            return cfg

        except (json.JSONDecodeError, IOError) as e:
            print(f"[config_manager] WARNING: config.json is corrupt ({e}), resetting.", file=sys.stderr)
            _backup_corrupt_config()

    cfg = dict(DEFAULT_CONFIG)
    save(cfg)
    return cfg


def _backup_corrupt_config():
    import shutil as _sh
    if CONFIG_PATH.exists():
        bak = CONFIG_PATH.with_suffix(f".{int(time.time())}.bak")
        try:
            _sh.copy2(str(CONFIG_PATH), str(bak))
        except Exception:
            pass

    # Clean up .bak files older than 7 days
    try:
        cutoff = time.time() - (7 * 86400)
        for f in CONFIG_PATH.parent.glob("*.bak"):
            try:
                if f.stat().st_mtime < cutoff:
                    f.unlink()
            except Exception:
                pass
    except Exception:
        pass


def save(cfg: dict):
    global _config_cache
    with _cache_lock:
        _config_cache["cfg"] = None  # invalidate cache on save
    import tempfile

    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)

    with _save_lock:
        # ATOMIC SAVE: Prevent config corruption during crashes
        fd, temp_path = tempfile.mkstemp(dir=CONFIG_PATH.parent, suffix=".tmp")

        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(cfg, f, indent=2)

            os.replace(temp_path, CONFIG_PATH)

        except Exception as e:
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception:
                pass
            raise e


def _snapshot_path(watch_id: str, dest_type: str = "") -> Path:
    """Return the snapshot file path for a given watch + destination type.

    Each destination gets its own snapshot so that SFTP and GDrive
    each independently track what they have backed up.  A watch that has never
    backed up to a particular destination will find no snapshot file and will
    therefore perform a full backup to that destination on the first run.

    Legacy single-snapshot files (no dest_type suffix) are still readable via
    load_snapshot() — they are migrated transparently on first write.
    """
    suffix = f"_{dest_type}" if dest_type else ""
    return SNAPSHOTS_DIR / f"{watch_id}{suffix}.json"


def save_snapshot(watch_id: str, snapshot: dict, dest_type: str = ""):
    """Save snapshot keyed by watch_id + dest_type (e.g. 'sftp', 'gdrive')."""
    try:
        SNAPSHOTS_DIR.mkdir(exist_ok=True)
        path = _snapshot_path(watch_id, dest_type)
        fd, tmp = tempfile.mkstemp(dir=SNAPSHOTS_DIR, suffix=".tmp")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(snapshot, f)
        os.replace(tmp, path)
    except Exception as e:
        logger.warning(f"⚠️ Failed to save snapshot for {watch_id} ({dest_type}): {e}")

def load_snapshot(watch_id: str, dest_type: str = "") -> dict:
    """Load snapshot for a specific destination.

    Falls back to the legacy unsuffixed file so existing installations keep
    working after the upgrade.  On the first successful backup the new
    per-destination file will be written and the legacy file is no longer used.
    """
    path = _snapshot_path(watch_id, dest_type)
    if path.exists():
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    # Fallback: legacy single-snapshot file (no dest_type suffix)
    if dest_type:
        legacy = SNAPSHOTS_DIR / f"{watch_id}.json"
        if legacy.exists():
            try:
                with open(legacy, encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
    return {}

def delete_snapshot(watch_id: str, dest_type: str = ""):
    """Delete snapshot for a watch (and destination), forcing a full backup next run.

    If dest_type is empty, deletes ALL snapshot files for the watch (legacy +
    all per-destination variants) so a full backup runs to every destination.
    """
    try:
        if dest_type:
            # Delete only the specific destination snapshot
            p = _snapshot_path(watch_id, dest_type)
            if p.exists():
                p.unlink()
        else:
            # Delete every snapshot file that belongs to this watch_id
            SNAPSHOTS_DIR.mkdir(exist_ok=True)
            for p in SNAPSHOTS_DIR.glob(f"{watch_id}*.json"):
                try:
                    p.unlink()
                except Exception:
                    pass
    except Exception as e:
        logger.warning(f"⚠️ Failed to delete snapshot for {watch_id} ({dest_type}): {e}")

# ─── Watch CRUD ───────────────────────────────────────────────────────────────

def add_watch(cfg: dict, name: str, path: str, watch_type: str = "local",
              tags: Optional[list] = None, notes: str = "",
              exclude_patterns: Optional[list] = None,
              skip_path_check: bool = False,
              cloud_config: Optional[dict] = None,
              interval_min: int = 0,
              encrypt_key: Optional[str] = None) -> dict:
    """Add a new watch target with validation."""

    norm_path = _norm_path(path)

    # ─── VALIDATION SECTION ───────────────────────────────────────────────

    # 1. Check path is not empty
    if not path or not path.strip():
        raise ValueError("Path cannot be empty")

    # 2. Check for dangerous/system paths
    dangerous_paths = {
        # Windows
        r"C:\$Recycle.Bin",
        r"C:\System Volume Information",
        r"C:\ProgramData",
        r"C:\Program Files",
        r"C:\Program Files (x86)",
        r"C:\Windows",
        r"C:\PerfLogs",
        # Unix/Linux
        "/sys",
        "/proc",
        "/dev",
        "/etc",
        "/root",
        "/boot",
        "/var/log",
    }

    norm_lower = norm_path.lower() if os.name == "nt" else norm_path
    for dangerous in dangerous_paths:
        dangerous_norm = _norm_path(dangerous).lower() if os.name == "nt" else _norm_path(dangerous)
        if norm_lower.startswith(dangerous_norm):
            raise ValueError(f"Cannot watch system/protected path: {path}")

    # 3. Check path exists
    try:
        p = Path(path)

        if not p.exists() and not skip_path_check:
            raise ValueError(f"Path does not exist: {path}")

        # Allow files and directories
        pass

        # 4. Try to access path with timeout (catch permission + network hang)
        try:
            import signal

            def _timeout_handler(signum, frame):
                raise TimeoutError("Path access timed out (possibly network share)")

            alarm_set = False
            if hasattr(signal, 'SIGALRM'):
                try:
                    signal.signal(signal.SIGALRM, _timeout_handler)
                    signal.alarm(3)
                    alarm_set = True
                except Exception:
                    pass  # signal only works on main thread; skip timeout in threaded Flask

            try:
                if p.is_dir():
                    list(p.iterdir())
                else:
                    p.stat()  # just verify file is accessible
            finally:
                if alarm_set and hasattr(signal, 'SIGALRM'):
                    signal.alarm(0)

        except TimeoutError as e:
            raise ValueError(str(e))
        except PermissionError:
            raise ValueError(f"Permission denied: cannot access {path}")
        except OSError as e:
            raise ValueError(f"Cannot access path: {e}")

    except ValueError:
        raise
    except Exception as e:
        raise ValueError(f"Invalid path: {e}")

    # ─── DUPLICATE CHECK ────────────────────────────────────────────────────

    for w in cfg["watches"]:
        w_norm = _norm_path(w["path"])
        if w_norm == norm_path:
            raise ValueError(f"Path already watched: {path}")

        # Warn about overlapping watches
        if (norm_path.lower().startswith(w_norm.lower() + os.sep) if os.name == "nt"
                else norm_path.startswith(w_norm + os.sep)):
            logger.warning(f"⚠️  Watch '{path}' overlaps with existing watch '{w['path']}' — files may be backed up twice")
        elif (w_norm.lower().startswith(norm_path.lower() + os.sep) if os.name == "nt"
              else w_norm.startswith(norm_path + os.sep)):
            logger.warning(f"⚠️  Existing watch '{w['path']}' overlaps with new watch '{path}' — files may be backed up twice")

    # ─── CREATE WATCH ───────────────────────────────────────────────────────

    wid = "w_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=6))

    watch: dict = {}
    for k, v in WATCH_TEMPLATE.items():
        watch[k] = list(v) if isinstance(v, list) else v

    watch.update({
        "id":               wid,
        "name":             name,
        "path":             path,
        "type":             watch_type,
        "tags":             list(tags) if tags else [],
        "notes":            notes or "",
        "exclude_patterns": list(exclude_patterns) if exclude_patterns else cfg.get("default_exclude_patterns", []),
        "cloud_config":     cloud_config or {},
        "interval_min":     max(0, int(interval_min)),
        "encrypt_key":      encrypt_key.strip() if encrypt_key else "",
    })

    cfg["watches"].append(watch)
    save(cfg)
    return watch


def remove_watch(cfg: dict, watch_id: str) -> bool:
    before = len(cfg["watches"])
    cfg["watches"] = [w for w in cfg["watches"] if w["id"] != watch_id]
    save(cfg)
    removed = len(cfg["watches"]) < before
    if removed:
        snap = SNAPSHOTS_DIR / f"{watch_id}.json"
        try:
            if snap.exists():
                snap.unlink()
        except Exception:
            pass
    return removed


def get_watch(cfg: dict, watch_id: str) -> Optional[dict]:
    return next((w for w in cfg["watches"] if w["id"] == watch_id), None)


def get_watch_by_path(cfg: dict, path: str) -> Optional[dict]:
    """Look up a watch by normalized path."""
    norm = _norm_path(path)
    for w in cfg["watches"]:
        if _norm_path(w["path"]) == norm:
            return w
    return None


def get_all_watch_ids(cfg: dict) -> List[str]:
    return [w["id"] for w in cfg.get("watches", [])]


def update_watch_snapshot(cfg: dict, watch_id: str, snapshot: dict, ts: str,
                          size_bytes: int = 0, dest_type: str = ""):
    for w in cfg["watches"]:
        if w["id"] == watch_id:
            w["last_snapshot"]    = None   # no longer stored in config
            w["last_backup"]      = ts
            w["backup_count"]     = w.get("backup_count", 0) + 1
            w["last_backup_size"] = size_bytes
    save_snapshot(watch_id, snapshot, dest_type)
    save(cfg)


def pause_watch(cfg: dict, watch_id: str, paused: bool):
    for w in cfg["watches"]:
        if w["id"] == watch_id:
            w["paused"] = paused
    save(cfg)


def update_watch_meta(
    cfg: dict,
    watch_id: str,
    name:              Optional[str]  = None,
    tags:              Optional[list] = None,
    notes:             Optional[str]  = None,
    exclude_patterns:  Optional[list] = None,
    max_backups:       Optional[int]  = None,
    skip_auto_backup:  Optional[bool] = None,
    reset_snapshot:    Optional[bool] = None,
    color:             Optional[str]  = None,
    interval_min:      Optional[int]  = None,
    active:            Optional[bool] = None,
    retention_days:    Optional[int]  = None,   # ← added
    compression:       Optional[bool] = None,   # ← added
    encrypt_key:       Optional[str]  = None,   # ← added: Fernet key or "" to disable
):
    """Update watch metadata and optionally reset snapshot for full re-backup."""
    for w in cfg["watches"]:
        if w["id"] == watch_id:
            if name             is not None: w["name"]             = name
            if tags             is not None: w["tags"]             = list(tags)
            if notes            is not None: w["notes"]            = notes
            if exclude_patterns is not None: w["exclude_patterns"] = list(exclude_patterns)
            if max_backups      is not None: w["max_backups"]      = max(0, int(max_backups))
            if skip_auto_backup is not None: w["skip_auto_backup"] = bool(skip_auto_backup)
            if color            is not None: w["color"]            = color[:7]  # max #rrggbb
            if interval_min     is not None: w["interval_min"]     = max(0, int(interval_min))
            if active           is not None: w["active"]           = bool(active)
            if retention_days   is not None: w["retention_days"]   = max(0, int(retention_days))  # ← added
            if compression      is not None: w["compression"]      = bool(compression)            # ← added
            if encrypt_key      is not None: w["encrypt_key"]      = encrypt_key.strip()          # ← added
            if reset_snapshot   is not None and reset_snapshot:
                w["last_snapshot"] = None
    save(cfg)


def update_watch_path(cfg: dict, watch_id: str, new_path: str):
    for w in cfg["watches"]:
        if w["id"] == watch_id:
            w["path"]          = new_path
            w["last_snapshot"] = None
    save(cfg)


def clone_watch(cfg: dict, watch_id: str, new_name: str, new_path: str) -> Optional[dict]:
    src = get_watch(cfg, watch_id)
    if not src:
        return None
    new_watch = add_watch(
        cfg, new_name, new_path, src["type"],
        list(src.get("tags", [])),
        src.get("notes", ""),
        list(src.get("exclude_patterns", [])),
        interval_min=src.get("interval_min", 0),
        encrypt_key=src.get("encrypt_key", ""),   # BUG FIX: carry over encryption key
    )
    # Preserve ALL per-watch settings from source
    if new_watch:
        update_watch_meta(
            cfg, new_watch["id"],
            max_backups=src.get("max_backups", 0),
            skip_auto_backup=src.get("skip_auto_backup", False),
            active=src.get("active", True),
            compression=src.get("compression", False),
            retention_days=src.get("retention_days", 0),
            color=src.get("color", ""),
            encrypt_key=src.get("encrypt_key", ""),  # BUG FIX: persist in config too
        )
        new_watch["max_backups"]      = src.get("max_backups", 0)
        new_watch["skip_auto_backup"] = src.get("skip_auto_backup", False)
        new_watch["compression"]      = src.get("compression", False)
        new_watch["retention_days"]   = src.get("retention_days", 0)
        new_watch["color"]            = src.get("color", "")
        new_watch["encrypt_key"]      = src.get("encrypt_key", "")  # BUG FIX: return dict too
    return new_watch


def reorder_watches(cfg: dict, ordered_ids: list):
    """Reorder watches to match the given list of IDs."""
    id_map = {w["id"]: w for w in cfg["watches"]}
    reordered = [id_map[wid] for wid in ordered_ids if wid in id_map]
    # Append any watches not in the list (safety net)
    included = set(ordered_ids)
    for w in cfg["watches"]:
        if w["id"] not in included:
            reordered.append(w)
    cfg["watches"] = reordered
    save(cfg)


# ─── Settings validation ──────────────────────────────────────────────────────

def validate_destination(path: str) -> dict:
    if not path or not path.strip():
        return {"ok": False, "error": "Path cannot be empty"}

    for ch in ("\x00",):
        if ch in path:
            return {"ok": False, "error": "Path contains illegal characters"}

    try:
        p = Path(path)
        p.mkdir(parents=True, exist_ok=True)
        test_file = p / ".backupsys_write_test"
        test_file.touch()
        test_file.unlink()
        return {"ok": True}

    except PermissionError:
        return {"ok": False, "error": f"Permission denied: cannot write to '{path}'"}
    except OSError as e:
        return {"ok": False, "error": f"OS error: {e}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}