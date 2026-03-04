import os
import uuid
import shutil
import hashlib
import json
import time
import threading
import difflib
import fnmatch
import logging
logger = logging.getLogger(__name__)
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

# Optional encryption support
try:
    from cryptography.fernet import Fernet
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

MAX_EDIT_BYTES = 5 * 1024 * 1024   # 5 MB – files larger than this refuse to open in editor

# ─── Path helpers ─────────────────────────────────────────────────────────────

def _fix_path(path: str) -> str:
    """
    Normalise paths so the engine works on both native Windows and
    Git-Bash / WSL style paths.
    """
    import re
    m = re.match(r'^/([a-zA-Z])(/.*)?$', path)
    if m:
        drive = m.group(1).upper()
        rest  = (m.group(2) or "").replace("/", "\\")
        return f"{drive}:{rest}" if rest else f"{drive}\\"
    m2 = re.match(r'^([a-zA-Z]):/(.*)$', path)
    if m2:
        drive = m2.group(1).upper()
        rest  = m2.group(2).replace("/", "\\")
        return f"{drive}:\\{rest}"
    return path


def _is_excluded(rel_path: str, abs_path: Path, patterns: List[str]) -> bool:
    """Return True if the file matches any exclude pattern."""
    parts = Path(rel_path).parts
    for pat in patterns:
        if fnmatch.fnmatch(rel_path, pat):              return True
        if fnmatch.fnmatch(abs_path.name, pat):         return True
        if any(fnmatch.fnmatch(p, pat) for p in parts): return True
    return False


def safe_path(requested: str, allowed_roots: List[str]) -> Optional[str]:
    """
    Resolve *requested* and verify it lives under one of the *allowed_roots*.
    Returns the resolved absolute path string, or None if path traversal detected.

    Fix #1: Case-insensitive comparison on Windows (os.name == "nt").
    Use this for every file editor API to prevent directory traversal attacks.
    """
    # Reject obviously malicious paths
    if not requested or any(c in requested for c in ('\x00', '\r', '\n')):
        return None
    
    try:
        resolved = Path(_fix_path(requested)).resolve()
        for root in allowed_roots:
            try:
                root_resolved = Path(_fix_path(root)).resolve()
                # Fix #1 — case-insensitive on Windows
                if os.name == "nt":
                    res_str  = str(resolved).lower()
                    root_str = str(root_resolved).lower()
                    if res_str == root_str or res_str.startswith(root_str + os.sep.lower()):
                        return str(resolved)
                else:
                    resolved.relative_to(root_resolved)
                    return str(resolved)
            except ValueError:
                continue
        return None
    except Exception:
        return None


# ─── Hashing ──────────────────────────────────────────────────────────────────

def hash_file(path: str) -> str:
    """SHA-256 hash of a single file.  Returns '' on any I/O error."""
    h = hashlib.sha256()
    try:
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()
    except (IOError, OSError):
        return ""


# Fix #3 — exclude MANIFEST.json so the directory hash stays stable after
# the manifest is written during run_backup().
EXCLUDE = {"BACKUP.sha256", "MANIFEST.json"}


def hash_directory(path: str) -> str:
    """Composite SHA-256 of all files in a directory (sorted for determinism).
    Excludes BACKUP.sha256 and MANIFEST.json (Fix #3).

    Safety: Depth is measured relative to the backup root, not the absolute
    path, so deep system paths like C:\\Users\\john\\... don't trigger the
    guard prematurely. Only genuine symlink loops (40+ levels deep inside the
    folder) are skipped.
    """
    h = hashlib.sha256()
    root = Path(path)
    if not root.exists():
        return ""

    # Count depth relative to the backup root, not absolute path
    _root_depth = path.count(os.sep)

    for fp in sorted(root.rglob("*")):
        if fp.is_file() and fp.name not in EXCLUDE:
            # Guard against deep symlink loops only
            if str(fp).count(os.sep) - _root_depth > 40:
                continue
            rel = str(fp.relative_to(root))
            h.update(rel.encode())
            h.update(hash_file(str(fp)).encode())
    return h.hexdigest()


# ─── Snapshot ─────────────────────────────────────────────────────────────────

def build_snapshot(
    path: str,
    previous: Optional[Dict] = None,
    exclude_patterns: Optional[List[str]] = None,
    timeout_sec: int = 300
) -> Dict[str, dict]:
    """
    Walk *path* and return a snapshot dict:
        { relative_path: { hash, size, mtime } }

    Improvements:
    - Reuses previous snapshot entries when mtime + size are unchanged
    - Timeout protection for network shares
    - Unix: uses signal.alarm()
    - Windows: uses thread timeout
    """

    import os
    import signal
    import threading
    from pathlib import Path

    path = _fix_path(path)
    root = Path(path)

    if not root.exists():
        return {}

    # -------------------------------------------------
    # Internal scanning logic
    # -------------------------------------------------
    def _do_scan() -> Dict[str, dict]:

        snapshot: Dict[str, dict] = {}

        # -------------------------
        # Single File Case
        # -------------------------
        if root.is_file():
            try:
                stat = root.stat()
                snapshot[root.name] = {
                    "hash": hash_file(str(root)),
                    "size": stat.st_size,
                    "mtime": stat.st_mtime,
                }
            except (IOError, OSError, PermissionError):
                pass

            return snapshot

        # -------------------------
        # Directory Walk
        # -------------------------
        for fp in root.rglob("*"):

            if not fp.is_file():
                continue

            rel = str(fp.relative_to(root))

            if exclude_patterns and _is_excluded(rel, fp, exclude_patterns):
                continue

            try:
                stat = fp.stat()

                # Reuse previous entry if unchanged
                if (
                    previous
                    and rel in previous
                    and previous[rel].get("mtime") == stat.st_mtime
                    and previous[rel].get("size") == stat.st_size
                ):
                    snapshot[rel] = previous[rel]
                    continue

                snapshot[rel] = {
                    "hash": hash_file(str(fp)),
                    "size": stat.st_size,
                    "mtime": stat.st_mtime,
                }

            except (IOError, OSError, PermissionError):
                continue

        return snapshot

    # -------------------------------------------------
    # WINDOWS TIMEOUT PROTECTION
    # -------------------------------------------------
    if os.name == "nt":

        result_container = {}
        exception_container = {}

        def _scan():
            try:
                result_container["data"] = _do_scan()
            except Exception as e:
                exception_container["error"] = e

        thread = threading.Thread(target=_scan, daemon=True)
        thread.start()

        thread.join(timeout=timeout_sec)

        if thread.is_alive():
            print(f"[WARNING] Snapshot timed out after {timeout_sec}s on {path}")
            return result_container.get("data", {})

        if exception_container.get("error"):
            raise exception_container["error"]

        return result_container.get("data", {})

    # -------------------------------------------------
    # UNIX TIMEOUT (SIGALRM)
    # -------------------------------------------------
    else:

        def _timeout_handler(signum, frame):
            raise TimeoutError(f"Snapshot scan exceeded {timeout_sec} seconds")

        old_handler = None
        alarm_set = False

        if hasattr(signal, "SIGALRM") and timeout_sec > 0:
            try:
                old_handler = signal.signal(signal.SIGALRM, _timeout_handler)
                signal.alarm(timeout_sec)
                alarm_set = True
            except Exception:
                alarm_set = False

        try:
            return _do_scan()

        except TimeoutError:
            print(f"[WARNING] Snapshot timed out after {timeout_sec} seconds.")
            return {}

        finally:
            if alarm_set:
                signal.alarm(0)
                if old_handler:
                    signal.signal(signal.SIGALRM, old_handler)


def diff_snapshots(old: Dict, new: Dict) -> List[dict]:
    """Compare two snapshots and return a list of change records."""

    changes = []
    all_keys = set(old) | set(new)

    for rel in sorted(all_keys):

        if rel not in old:
            changes.append({
                "type": "added",
                "path": rel,
                "old_hash": None,
                "new_hash": new[rel]["hash"],
                "size": new[rel]["size"],
            })

        elif rel not in new:
            changes.append({
                "type": "deleted",
                "path": rel,
                "old_hash": old[rel]["hash"],
                "new_hash": None,
                "size": old[rel]["size"],
            })

        elif old[rel]["hash"] != new[rel]["hash"]:
            changes.append({
                "type": "modified",
                "path": rel,
                "old_hash": old[rel]["hash"],
                "new_hash": new[rel]["hash"],
                "size": new[rel]["size"],
            })

    return changes


# ─── BackupIndex ──────────────────────────────────────────────────────────────

class BackupIndex:
    """
    In-memory cache of all backup manifests so list_backups() doesn't
    re-scan the disk on every API call (which gets slow with many backups).

    Invalidated whenever a backup is created or deleted.
    Thread-safe via a single lock.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._cache: Dict[str, list] = {}   # destination → sorted backups
            cls._instance._dirty: set = set()
        return cls._instance

    def invalidate(self, destination: str):
        with self._lock:
            self._dirty.add(destination)

    def get(self, destination: str, watch_id: Optional[str] = None) -> List[dict]:
        with self._lock:
            if destination not in self._dirty and destination in self._cache:
                backups = self._cache.get(destination, [])
                if watch_id:
                    return [b for b in backups if b.get("watch_id") == watch_id]
                return list(backups)
            self._dirty.discard(destination)

        # Rebuild outside the lock so other threads aren't blocked during disk I/O
        dest = Path(destination)
        backups = []
        if dest.exists():
            for d in sorted(dest.iterdir(), reverse=True):
                if not d.is_dir():
                    continue
                manifest_p = d / "MANIFEST.json"
                if not manifest_p.exists():
                    continue
                try:
                    with open(manifest_p) as f:
                        m = json.load(f)
                except Exception:
                    continue
                m["backup_dir"]  = str(d)
                hp = d / "BACKUP.sha256"
                m["backup_hash"] = hp.read_text().split()[0] if hp.exists() else "N/A"
                if "status" not in m:
                    m["status"] = "success" if m.get("files_copied", 0) > 0 else "failed"
                backups.append(m)

        with self._lock:
            self._cache[destination] = backups

        if watch_id:
            return [b for b in backups if b.get("watch_id") == watch_id]
        return list(backups)

    def _rebuild(self, destination: str):
        pass  # rebuild is now handled inline in get()

    def get_by_id(self, destination: str, backup_id: str) -> Optional[Tuple[str, dict]]:
        for b in self.get(destination):
            if b.get("backup_id") == backup_id:
                return b["backup_dir"], b
        return None

    def get_watch_ids(self, destination: str) -> List[dict]:
        """Return distinct watches that have at least one backup."""
        seen: Dict[str, str] = {}
        for b in self.get(destination):
            wid = b.get("watch_id")
            if wid and wid not in seen:
                seen[wid] = b.get("watch_name", wid)
        return [{"id": k, "name": v} for k, v in seen.items()]

    def get_watch_disk_usage(self, destination: str, watch_id: str) -> int:
        """Return total disk bytes used by all backups for a watch."""
        total = 0
        for b in self.get(destination, watch_id):
            bd = b.get("backup_dir")
            if bd:
                total += _safe_size(bd)
        return total


_backup_index = BackupIndex()


# ─── Backup ───────────────────────────────────────────────────────────────────

def _safe_size(path: str) -> int:
    p = Path(path)
    if not p.exists():
        return 0
    if p.is_file():
        try:
            return p.stat().st_size
        except Exception:
            return 0
    total = 0
    for f in p.rglob("*"):
        if f.is_file():
            try:
                total += f.stat().st_size
            except Exception:
                pass
    return total


def _human_size(n) -> str:
    if not n:
        return "0 B"
    n = int(n)
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"

# ─── Encryption helpers ──────────────────────────────────────────────────────

def generate_encryption_key() -> str:
    """Generate a new Fernet encryption key (44 characters, URL-safe base64)."""
    if not CRYPTO_AVAILABLE:
        raise RuntimeError("cryptography library not installed")
    return Fernet.generate_key().decode('utf-8')


def _encrypt_file(src_path: str, dest_path: str, key: str) -> None:
    """Encrypt a file using Fernet (AES-128)."""
    if not CRYPTO_AVAILABLE:
        raise RuntimeError("cryptography library not installed")
    
    try:
        # Validate key format
        if len(key) != 44:
            raise ValueError(
                f"Encryption key must be 44 characters (got {len(key)}). "
                "Generate one with: python -c \"from backup_engine import generate_encryption_key; print(generate_encryption_key())\""
            )
        
        cipher = Fernet(key.encode())
        with open(src_path, 'rb') as f:
            plaintext = f.read()
        ciphertext = cipher.encrypt(plaintext)
        with open(dest_path, 'wb') as f:
            f.write(ciphertext)
    except Exception as e:
        raise RuntimeError(f"Encryption failed for {Path(src_path).name}: {e}")


def _decrypt_file(src_path: str, dest_path: str, key: str) -> None:
    """Decrypt a file using Fernet (AES-128)."""
    if not CRYPTO_AVAILABLE:
        raise RuntimeError("cryptography library not installed")
    
    try:
        if len(key) != 44:
            raise ValueError(
                f"Encryption key must be 44 characters (got {len(key)})"
            )
        
        cipher = Fernet(key.encode())
        with open(src_path, 'rb') as f:
            ciphertext = f.read()
        plaintext = cipher.decrypt(ciphertext)
        with open(dest_path, 'wb') as f:
            f.write(plaintext)
    except Exception as e:
        raise RuntimeError(f"Decryption failed for {Path(src_path).name}: {e}")

def run_backup(
    source: str,
    destination: str,
    watch_id: str,
    watch_name: str,
    storage_type: str,
    previous_snapshot: Optional[Dict] = None,
    incremental: bool = True,
    progress_cb: Optional[Callable[[int, int, str], None]] = None,
    exclude_patterns: Optional[List[str]] = None,
    encrypt_key: Optional[str] = None,
    compress: bool = False,
    throttler=None,
) -> dict:
    """
    Execute a backup from source → destination.
    progress_cb(copied, total, current_file) is called after each file copy.
    Tracks failed files for reporting.
    """
    import gzip
    import shutil as _sh

    started = time.time()
    ts      = datetime.now().isoformat()

    source      = _fix_path(source)
    destination = _fix_path(destination)

    dest_root = Path(destination)
    src_path  = Path(source)

    _bid = _short_id()
    result = {
        "id":            _bid,
        "backup_id":     _bid,
        "watch_id":      watch_id,
        "watch_name":    watch_name,
        "source":        source,
        "destination":   destination,
        "storage":       storage_type,
        "timestamp":     ts,
        "status":        "failed",
        "error":         None,
        "changes":       [],
        "files_changed": 0,
        "files_copied":  0,
        "total_files":   0,
        "total_size":    "0 B",
        "backup_hash":   "",
        "duration_s":    0.0,
        "incremental":   incremental,
        "progress":      0,
        "compressed":    compress,
        "failed_files":  [],  # ← track failed files
    }

    backup_dir = None

    try:
        if not src_path.exists():
            raise FileNotFoundError(f"Source not found: {source}")

        safe_name  = "".join(c if c.isalnum() else "_" for c in watch_name)
        backup_dir = dest_root / f"{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}__{safe_name}"
        backup_dir.mkdir(parents=True, exist_ok=True)

        new_snapshot = build_snapshot(source, previous=previous_snapshot, exclude_patterns=exclude_patterns or [])

        if incremental and previous_snapshot:
            changes       = diff_snapshots(previous_snapshot, new_snapshot)
            files_to_copy = [c for c in changes if c["type"] in ("added", "modified")]
        else:
            changes = [
                {
                    "type":     "added",
                    "path":     k,
                    "new_hash": v["hash"],
                    "size":     v["size"],
                    "old_hash": None,
                }
                for k, v in new_snapshot.items()
            ]
            files_to_copy = changes

        result["changes"] = changes
        total  = len(files_to_copy)
        copied = 0

        # Disk space check
        try:
            needed = sum(c.get("size", 0) for c in files_to_copy)
            free   = shutil.disk_usage(destination).free
            if needed > 0 and free < needed * 1.5:
                raise OSError(
                    f"Insufficient disk space. Need {_human_size(needed * 1.5)}, "
                    f"only {_human_size(free)} available."
                )
        except OSError:
            raise
        except Exception as e:
            print(f"[backup] ⚠ Could not check disk space: {e}", flush=True)

        # ── Single-file source ──────────────────────────────
        if src_path.is_file():
            if encrypt_key and CRYPTO_AVAILABLE:
                _encrypt_file(source, str(backup_dir / src_path.name), encrypt_key)
            elif compress:
                dest_gz = backup_dir / (src_path.name + '.gz')
                with open(source, 'rb') as f_in, gzip.open(str(dest_gz), 'wb') as f_out:
                    _sh.copyfileobj(f_in, f_out)
            else:
                shutil.copy2(source, backup_dir / src_path.name)
                if hash_file(str(source)) != hash_file(str(backup_dir / src_path.name)):
                    raise IOError(f"Integrity check failed for {src_path.name}. Backup aborted.")

            copied = 1
            if progress_cb:
                try:
                    progress_cb(1, 1, src_path.name)
                except InterruptedError:
                    raise
                except Exception:
                    pass

        # ── Directory source ───────────────────────────────
        else:
            for entry in files_to_copy:
                src_file  = src_path / entry["path"]
                dest_file = backup_dir / entry["path"]
                dest_file.parent.mkdir(parents=True, exist_ok=True)

                if src_file.exists():
                    try:
                        # Periodic low-disk check every 5 files
                        if copied % 5 == 0:
                            try:
                                free = shutil.disk_usage(destination).free
                                if free < 100 * 1024 * 1024:
                                    raise OSError(f"Critical: Only {_human_size(free)} free at destination")
                            except OSError:
                                raise
                            except Exception:
                                pass

                        if encrypt_key and CRYPTO_AVAILABLE:
                            _encrypt_file(str(src_file), str(dest_file), encrypt_key)
                        elif compress:
                            dest_gz = Path(str(dest_file) + '.gz')
                            dest_gz.parent.mkdir(parents=True, exist_ok=True)
                            with open(str(src_file), 'rb') as f_in, gzip.open(str(dest_gz), 'wb') as f_out:
                                _sh.copyfileobj(f_in, f_out)
                            dest_file = dest_gz
                        else:
                            shutil.copy2(str(src_file), str(dest_file))
                            if hash_file(str(src_file)) != hash_file(str(dest_file)):
                                raise IOError(f"Integrity check failed for {entry['path']}. Backup aborted.")

                        # Throttle if requested
                        if throttler:
                            try:
                                throttler.throttle(entry.get("size", 0))
                            except Exception:
                                pass

                        copied += 1

                    except (PermissionError, OSError) as e:
                        logger.warning(f"⚠ Skipped {entry['path']}: {e}")
                        # Track failed files
                        result["failed_files"].append({
                            "path": entry["path"],
                            "reason": str(e)
                        })

                if progress_cb:
                    try:
                        pct = int(copied / max(total, 1) * 100) if total > 0 else 0
                        if pct % 10 == 0 and copied > 0:
                            logger.info(f"📦 {watch_name}: {pct}% ({copied}/{total} files)")
                        progress_cb(copied, total or 1, entry["path"])
                    except InterruptedError:
                        raise
                    except Exception:
                        pass

            # Write .DELETED markers
            for c in changes:
                if c["type"] == "deleted":
                    marker = backup_dir / (c["path"] + ".DELETED")
                    try:
                        marker.touch()
                    except Exception:
                        pass

        # ── Write MANIFEST ───────────────────────────────
        _manifest_size     = _safe_size(str(backup_dir))
        _manifest_duration = round(time.time() - started, 2)
        _manifest_speed    = (_manifest_size / (1024 * 1024)) / max(_manifest_duration, 0.1)

        # Calculate compression ratio
        compression_ratio = 0.0
        if compress:
            # Calculate uncompressed estimate from file sizes
            uncompressed_est = sum(c.get("size", 0) for c in changes)
            actual_size = _safe_size(str(backup_dir))
            if uncompressed_est > 0:
                compression_ratio = round((1 - actual_size / uncompressed_est) * 100, 1)

        manifest = {
            "backup_id":          result["id"],
            "watch_id":           watch_id,
            "watch_name":         watch_name,
            "source":             source,
            "timestamp":          ts,
            "status":             "success",
            "incremental":        incremental,
            "compressed":         compress,
            "compression_ratio":  compression_ratio,  # NEW
            "files_copied":       copied,
            "changes":            changes,
            "snapshot":           new_snapshot,
            "duration_s":         _manifest_duration,
            "throughput_mbs":     round(_manifest_speed, 2),
            "total_size_bytes":   _manifest_size,
            "failed_files":       result.get("failed_files", []),
        }

        manifest_path = backup_dir / "MANIFEST.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)

        # Verify MANIFEST
        try:
            with open(manifest_path) as f:
                json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            raise RuntimeError(f"MANIFEST.json was not written correctly — backup is corrupted: {e}")

        # ── Integrity hash ───────────────────────────────
        backup_hash    = hash_directory(str(backup_dir))
        hash_file_path = backup_dir / "BACKUP.sha256"

        try:
            with open(hash_file_path, "w") as f:
                f.write(f"{backup_hash}  {backup_dir.name}\n")
            with open(hash_file_path) as f:
                stored = f.read().split()[0]
                if stored != backup_hash:
                    raise RuntimeError("BACKUP.sha256 hash mismatch after write")
        except (IOError, RuntimeError) as e:
            raise RuntimeError(f"BACKUP.sha256 was not written correctly: {e}")

        total_size     = _safe_size(str(backup_dir))
        duration       = round(time.time() - started, 2)
        throughput_mbs = (total_size / (1024 * 1024)) / max(duration, 0.1) if duration > 0 else 0

        result.update({
            "status":           "success",
            "files_changed":    len([c for c in changes if c["type"] != "unchanged"]),
            "files_copied":     copied,
            "total_files":      len(new_snapshot),
            "total_size":       _human_size(total_size),
            "backup_hash":      backup_hash,
            "backup_dir":       str(backup_dir),
            "duration_s":       duration,
            "throughput_mbs":   round(throughput_mbs, 2),
            "compression_ratio": compression_ratio,  # NEW
            "snapshot":         new_snapshot,
            "progress":         100,
        })
        logger.info(
            f"✅ Backup complete: {watch_name} | {_human_size(total_size)} | "
            f"{throughput_mbs:.1f} MB/s | {duration}s"
            + (" | compressed" if compress else "")
        )

    except InterruptedError:
        result["error"]      = "Backup cancelled by user"
        result["status"]     = "cancelled"
        result["duration_s"] = round(time.time() - started, 2)
        if backup_dir is not None and Path(backup_dir).exists():
            shutil.rmtree(str(backup_dir), ignore_errors=True)

    except Exception as e:
        import traceback
        logger.error(f"❌ Backup failed for {watch_name}: {e}\n{traceback.format_exc()}")
        result["error"]      = str(e)
        result["duration_s"] = round(time.time() - started, 2)

    finally:
        _backup_index.invalidate(destination)

    return result

def export_backup_zip(backup_dir: str, tmp_dir: str) -> dict:
    """Package a backup directory into a zip file for download."""
    bd = Path(backup_dir)
    if not bd.exists():
        return {"ok": False, "error": "Backup directory not found"}
    try:
        ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
        name = f"backup_{bd.name}_{ts}"
        zip_base = Path(tmp_dir) / name
        shutil.make_archive(str(zip_base), "zip", str(bd.parent), bd.name)
        zip_path = zip_base.with_suffix(".zip")
        return {"ok": True, "path": str(zip_path), "filename": f"{name}.zip"}
    except Exception as e:
        return {"ok": False, "error": str(e)}
# ─── Restore ──────────────────────────────────────────────────────────────────

def restore_backup(backup_dir: str, target_path: str, incremental_only: bool = False) -> dict:
    """Restore files from a backup directory back to target_path.
    
    Args:
        backup_dir: Path to the backup directory
        target_path: Path where files should be restored
        incremental_only: If True, only restore files that don't already exist at target
    """
    import gzip as _gz

    backup_dir  = _fix_path(backup_dir)
    target_path = _fix_path(target_path)

    bd = Path(backup_dir)
    tp = Path(target_path)

    result: dict = {
        "ok":             False,
        "files_restored": 0,
        "skipped":        0,
        "errors":         [],
        "error":          None,
    }

    try:
        manifest_p = bd / "MANIFEST.json"
        if not manifest_p.exists():
            result["error"] = "MANIFEST.json not found — cannot restore"
            return result

        try:
            with open(manifest_p) as f:
                manifest = json.load(f)
        except json.JSONDecodeError:
            result["error"] = "MANIFEST.json is corrupted (invalid JSON) — cannot restore"
            return result
        except Exception as e:
            result["error"] = f"Could not read MANIFEST.json: {e}"
            return result

        tp.mkdir(parents=True, exist_ok=True)
        restored = 0
        skipped  = 0

        for entry in manifest.get("changes", []):
            etype = entry.get("type")
            rel   = entry.get("path", "")

            if etype in ("added", "modified"):
                src_file = bd / rel
                gz_file  = bd / (rel + '.gz')
                dst_file = tp / rel

                # ── NEW: Skip if incremental_only and file already exists ──
                if incremental_only and dst_file.exists():
                    skipped += 1
                    continue

                if not src_file.exists() and gz_file.exists():
                    # Compressed backup — decompress on restore
                    try:
                        dst_file.parent.mkdir(parents=True, exist_ok=True)
                        with _gz.open(str(gz_file), 'rb') as f_in, open(str(dst_file), 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                        restored += 1
                    except Exception as e:
                        result["errors"].append(f"{rel}: {e}")

                elif src_file.exists():
                    # Normal uncompressed backup
                    try:
                        dst_file.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(str(src_file), str(dst_file))
                        restored += 1
                    except Exception as e:
                        result["errors"].append(f"{rel}: {e}")

                else:
                    skipped += 1

            elif etype == "deleted":
                dst_file = tp / rel
                if dst_file.exists():
                    try:
                        dst_file.unlink()
                    except Exception:
                        pass

        result["ok"]             = True
        result["files_restored"] = restored
        result["skipped"]        = skipped

    except Exception as e:
        result["error"] = str(e)

    return result


# ─── Cleanup ──────────────────────────────────────────────────────────────────

def cleanup_old_backups(destination: str, retention_days: int) -> dict:
    """Delete backup folders older than retention_days and report cleanup results."""

    dest = Path(destination)

    result = {
        "deleted": 0,
        "freed_bytes": 0,
        "freed_human": "0 B",
        "errors": []
    }

    if not dest.exists() or retention_days <= 0:
        return result

    cutoff = datetime.now() - timedelta(days=retention_days)

    for d in list(dest.iterdir()):

        if not d.is_dir():
            continue

        manifest_p = d / "MANIFEST.json"

        if not manifest_p.exists():
            continue

        try:
            with open(manifest_p) as f:
                m = json.load(f)

            ts_str = m.get("timestamp", "")

            if not ts_str:
                continue

            ts = datetime.fromisoformat(ts_str)

            if ts < cutoff:

                size = _safe_size(str(d))

                shutil.rmtree(str(d), ignore_errors=True)

                result["deleted"] += 1
                result["freed_bytes"] += size

        except Exception as e:
            result["errors"].append(str(e))

    # Convert bytes to human readable
    result["freed_human"] = _human_size(result["freed_bytes"])

    # Invalidate backup index and log cleanup notification
    if result["deleted"] > 0:
        _backup_index.invalidate(destination)

        logger.info(
            f"🗑 Deleted {result['deleted']} old backup(s) "
            f"(older than {retention_days}d), freed {result['freed_human']}"
        )

    return result


# ─── Validation ───────────────────────────────────────────────────────────────

def validate_backup(backup_dir: str) -> dict:
    """
    Re-hash the backup directory and compare against stored BACKUP.sha256.
    Also verifies per-file hashes against the manifest snapshot.
    """
    backup_dir = _fix_path(backup_dir)
    bd         = Path(backup_dir)
    result: dict = {
        "valid":           False,
        "stored_hash":     None,
        "computed_hash":   None,
        "missing_files":   [],
        "corrupted_files": [],
        "manifest_ok":     False,
        "error":           None,
    }

    try:
        hash_file_p = bd / "BACKUP.sha256"
        manifest_p  = bd / "MANIFEST.json"

        if not hash_file_p.exists():
            result["error"] = "BACKUP.sha256 not found"
            return result

        stored_hash           = hash_file_p.read_text().split()[0]
        result["stored_hash"] = stored_hash

        if manifest_p.exists():
            try:
                with open(manifest_p) as f:
                    manifest = json.load(f)
            except json.JSONDecodeError:
                result["error"] = "MANIFEST.json is corrupted (invalid JSON)"
                result["manifest_ok"] = False
                return result
            except Exception as e:
                result["error"] = f"Could not read MANIFEST.json: {e}"
                result["manifest_ok"] = False
                return result

            snap = manifest.get("snapshot", {})
            for entry in manifest.get("changes", []):
                if entry["type"] in ("added", "modified"):
                    rel       = entry["path"]
                    fp        = bd / rel
                    fp_gz     = bd / (rel + '.gz')
                    actual_fp = fp if fp.exists() else (fp_gz if fp_gz.exists() else None)

                    if not actual_fp:
                        result["missing_files"].append(rel)
                    elif snap.get(rel) and not fp_gz.exists():
                        # Only hash-check uncompressed files (gz changes the hash)
                        actual = hash_file(str(actual_fp))
                        if actual and actual != snap[rel].get("hash", actual):
                            result["corrupted_files"].append(rel)

            result["manifest_ok"] = (
                len(result["missing_files"]) == 0
                and len(result["corrupted_files"]) == 0
            )

        h = hashlib.sha256()
        for fp in sorted(bd.rglob("*")):
            if fp.is_file() and fp.name not in EXCLUDE:
                rel = str(fp.relative_to(bd))
                h.update(rel.encode())
                h.update(hash_file(str(fp)).encode())
        computed                = h.hexdigest()
        result["computed_hash"] = computed
        result["valid"]         = (computed == stored_hash)

    except Exception as e:
        result["error"] = str(e)

    return result


# ─── Stats ────────────────────────────────────────────────────────────────────

def get_watch_stats(destination: str, watch_id: str) -> dict:
    """Return aggregate stats for a single watch target."""
    backups       = list_backups(destination, watch_id)
    total         = len(backups)
    success       = sum(1 for b in backups if b.get("status") == "success")
    cancelled     = sum(1 for b in backups if b.get("status") == "cancelled")
    total_copied  = sum(b.get("files_copied", 0) for b in backups)
    last_ts       = backups[0].get("timestamp") if backups else None
    disk_bytes    = _backup_index.get_watch_disk_usage(destination, watch_id)
    
    # FIX: Safely calculate fail count (don't assume all keys exist)
    fail_count    = 0
    for b in backups:
        status = b.get("status", "unknown")
        if status not in ("success", "cancelled"):
            fail_count += 1
    
    return {
        "total_backups":      total,
        "success_count":      success,
        "fail_count":         fail_count,
        "cancelled_count":    cancelled,
        "total_files_copied": total_copied,
        "last_backup":        last_ts,
        "disk_usage_bytes":   disk_bytes,
        "disk_usage_human":   _human_size(disk_bytes),
    }


def get_watch_size_human(destination: str, watch_id: str) -> str:
    """Quick human-readable disk usage for a watch."""
    return _human_size(_backup_index.get_watch_disk_usage(destination, watch_id))


# ─── Lookup helpers ───────────────────────────────────────────────────────────

def get_backup_by_id(destination: str, backup_id: str) -> Optional[Tuple[str, dict]]:
    """
    Find a backup by ID using the in-memory index (fast).
    Falls back to disk scan if cache miss.
    """
    result = _backup_index.get_by_id(destination, backup_id)
    if result:
        return result
    dest = Path(destination)
    if not dest.exists():
        return None
    for d in dest.iterdir():
        if not d.is_dir():
            continue
        mp = d / "MANIFEST.json"
        if not mp.exists():
            continue
        try:
            with open(mp) as f:
                m = json.load(f)
            if m.get("backup_id") == backup_id:
                return str(d), m
        except Exception:
            pass
    return None


def list_backups(destination: str, watch_id: Optional[str] = None) -> List[dict]:
    """
    Return sorted list of backup manifests.
    Uses in-memory index for O(1) repeated calls.
    """
    return _backup_index.get(destination, watch_id)

def browse_backup_contents(backup_dir: str) -> dict:
    """
    List all files inside a backup directory.
    Returns a dict: { files: [{path, size_human}], deleted: [path], total: N }
    """
    bd = Path(backup_dir)
    if not bd.exists():
        return {"error": "Backup directory not found", "files": [], "deleted": [], "total": 0}

    manifest_p = bd / "MANIFEST.json"
    files = []
    deleted = []

    if manifest_p.exists():
        try:
            with open(manifest_p, "r", encoding="utf-8") as f:
                manifest = json.load(f)
            for entry in manifest.get("changes", []):
                etype = entry.get("type", "")
                rel   = entry.get("path", "")
                if etype == "deleted":
                    deleted.append(rel)
                elif etype in ("added", "modified"):
                    fp    = bd / rel
                    fp_gz = bd / (rel + '.gz')
                    if fp.exists():
                        size = fp.stat().st_size
                    elif fp_gz.exists():
                        size = entry.get("size", 0)  # show original pre-compression size
                    else:
                        size = entry.get("size", 0)
                    files.append({"path": rel, "size_human": _human_size(size)})
        except Exception as e:
            return {"error": str(e), "files": [], "deleted": [], "total": 0}
    else:
        # No manifest — just list raw files
        for fp in sorted(bd.rglob("*")):
            if fp.is_file() and fp.name not in ("MANIFEST.json", "BACKUP.sha256"):
                rel = str(fp.relative_to(bd))
                files.append({"path": rel, "size_human": _human_size(fp.stat().st_size)})

    files.sort(key=lambda x: x["path"])
    return {"files": files, "deleted": deleted, "total": len(files)}

# ─── Read file safely ─────────────────────────────────────────────────────────

def read_file_safe(path: str) -> dict:
    """
    Read a text file with a size guard.
    Returns { content, name, path, hash, size, lines } or raises ValueError.
    """
    path = _fix_path(path)
    p    = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"File not found: {path}")
    size = p.stat().st_size
    if size > MAX_EDIT_BYTES:
        raise ValueError(
            f"File too large to edit in browser ({_human_size(size)}). "
            f"Limit is {_human_size(MAX_EDIT_BYTES)}."
        )
    content = p.read_text(encoding="utf-8", errors="replace")
    return {
        "path":    str(p),
        "name":    p.name,
        "content": content,
        "hash":    hash_file(str(p)),
        "size":    size,
        "lines":   content.count("\n") + 1,
    }


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _short_id() -> str:
    """Fix #10 — UUID-based IDs to eliminate collision risk."""
    return uuid.uuid4().hex[:16]