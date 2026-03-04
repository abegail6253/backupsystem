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
import shutil
import random
import string
import sys
import time
import threading
from pathlib import Path
from typing import List, Optional

import logging
logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).parent / "config.json"
QUEUE_PATH = Path(__file__).parent / "backup_queue.json"
_save_lock = threading.Lock()

DEFAULT_CONFIG = {
    "destination":         "./backups",
    "storage_type":        "local",
    "auto_backup":         False,
    "interval_min":        30,
    "retention_days":      30,
    "webhook_url":         "",
    "compression_enabled": False,
    "auto_retry":          False,
    "retry_delay_min":     5,
    "max_backup_mbps":     0,
    "email_config": {
        "enabled":   False,
        "smtp_host": "",
        "smtp_port": 587,
        "username":  "",
        "password":  "",
        "from_addr": "",
        "to_addr":   "",
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


# ─── Load / Save ──────────────────────────────────────────────────────────────

def load() -> dict:
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH, encoding="utf-8") as f:
                cfg = json.load(f)

            for k, v in DEFAULT_CONFIG.items():
                if k not in cfg:
                    cfg[k] = v

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


# ─── Watch CRUD ───────────────────────────────────────────────────────────────

def add_watch(cfg: dict, name: str, path: str, watch_type: str = "local",
              tags: Optional[list] = None, notes: str = "",
              exclude_patterns: Optional[list] = None) -> dict:
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

    # 3. Check path exists and is a directory
    try:
        p = Path(path)

        if not p.exists():
            raise ValueError(f"Path does not exist: {path}")

        if not p.is_dir():
            raise ValueError(f"Path is not a directory: {path}")

        # 4. Check for symlinks (security)
        if p.is_symlink():
            raise ValueError(f"Path cannot be a symlink: {path}")

        # 5. Try to list contents with timeout (catch permission + network hang)
        try:
            import signal

            def _timeout_handler(signum, frame):
                raise TimeoutError("Path access timed out (possibly network share)")

            if hasattr(signal, 'SIGALRM'):
                signal.signal(signal.SIGALRM, _timeout_handler)
                signal.alarm(3)

            try:
                list(p.iterdir())
            finally:
                if hasattr(signal, 'SIGALRM'):
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
    })

    cfg["watches"].append(watch)
    save(cfg)
    return watch


def remove_watch(cfg: dict, watch_id: str) -> bool:
    before = len(cfg["watches"])
    cfg["watches"] = [w for w in cfg["watches"] if w["id"] != watch_id]
    save(cfg)
    return len(cfg["watches"]) < before


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


def update_watch_snapshot(cfg: dict, watch_id: str, snapshot: dict, ts: str):
    for w in cfg["watches"]:
        if w["id"] == watch_id:
            w["last_snapshot"] = snapshot
            w["last_backup"]   = ts
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
    )
    # Preserve per-watch limits from source
    if new_watch:
        update_watch_meta(
            cfg, new_watch["id"],
            max_backups=src.get("max_backups", 0),
            skip_auto_backup=src.get("skip_auto_backup", False),
        )
        new_watch["max_backups"]      = src.get("max_backups", 0)
        new_watch["skip_auto_backup"] = src.get("skip_auto_backup", False)
    return new_watch


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