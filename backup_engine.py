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

# Optional transport/notification helpers
try:
    from transport_utils import (
        upload_to_sftp, upload_to_ftp, upload_to_smb, upload_to_https
    )
    TRANSPORT_AVAILABLE = True
except ImportError:
    TRANSPORT_AVAILABLE = False

try:
    from notification_utils import (
        send_email_notification, send_webhook_notification, build_backup_email
    )
    NOTIFICATIONS_AVAILABLE = True
except ImportError:
    NOTIFICATIONS_AVAILABLE = False

MAX_EDIT_BYTES   = 5 * 1024 * 1024   # 5 MB – files larger than this refuse to open in editor
FERNET_WARN_BYTES = 50  * 1024 * 1024  # 50 MB – warn about in-memory encryption
FERNET_MAX_BYTES  = 200 * 1024 * 1024  # 200 MB – hard limit; Fernet loads the full file into RAM


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
            self.bytes_sent = bytes_copied  # carry over what was just added
            self.last_time = time.time()


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
    # Normalize double-backslashes that may come from JS string escaping
    if '\\\\' in path:
        path = path.replace('\\\\', '\\')
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

            if fp.is_symlink():
                continue  # skip symlinks to prevent loops and traversal

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
            logger.warning(f"[snapshot] Timed out scanning '{path}' — returning empty snapshot, next backup will be full")
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
            logger.warning(f"[snapshot] Timed out scanning '{path}' — returning empty snapshot, next backup will be full")
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

    def _norm(self, destination: str) -> str:
        """
        Return a canonical cache key for *destination* so that paths that
        refer to the same directory but differ only in slash style
        (e.g. ``D:/backups`` vs ``D:\\backups``) always map to the same
        entry.  This prevents a stale-cache bug where run_backup() calls
        invalidate() with a backslash path (after _fix_path) while
        list_backups() is called from desktop_app with a forward-slash path
        from config.json, causing the cache to never be invalidated and
        always returning an empty list for the "backups" watch.
        """
        try:
            return str(Path(_fix_path(destination)).resolve())
        except Exception:
            return destination

    def invalidate(self, destination: str):
        with self._lock:
            self._dirty.add(self._norm(destination))

    def get(self, destination: str, watch_id: Optional[str] = None) -> List[dict]:
        destination = self._norm(destination)
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
        """Return total disk bytes used by all backups for a watch.
        Uses manifest's stored total_size_bytes when available (avoids re-scan).
        """
        total = 0
        for b in self.get(destination, watch_id):
            manifest_bytes = b.get("total_size_bytes", 0)
            if manifest_bytes:
                total += manifest_bytes
            else:
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
    if not n or n < 0:
        return "0 B"
    n = int(n)
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            # Bytes are whole numbers — no decimal needed
            return f"{n} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"

# ─── Encryption helpers ──────────────────────────────────────────────────────

def generate_encryption_key() -> str:
    """Generate a new Fernet encryption key (44 characters, URL-safe base64)."""
    if not CRYPTO_AVAILABLE:
        raise RuntimeError("cryptography library not installed")
    return Fernet.generate_key().decode('utf-8')


def _encrypt_file(src_path: str, dest_path: str, key: str) -> None:
    """Encrypt a file using Fernet (AES-128-CBC + HMAC-SHA256).

    Note: Fernet requires the full plaintext in memory to compute the HMAC
    before writing.  For files larger than ~500 MB consider using a streaming
    cipher (e.g. AES-GCM via cryptography.hazmat) instead.
    """
    if not CRYPTO_AVAILABLE:
        raise RuntimeError("cryptography library not installed")

    try:
        # Validate key format: must be 44-char URL-safe base64 that decodes to 32 bytes
        try:
            import base64 as _b64
            raw = _b64.urlsafe_b64decode(key + "==")
            if len(raw) != 32:
                raise ValueError("decoded key must be 32 bytes")
        except Exception:
            raise ValueError(
                f"Invalid Fernet encryption key (must be 44-char URL-safe base64, 32 bytes decoded). "
                "Generate one with: python -c \"from backup_engine import generate_encryption_key; print(generate_encryption_key())\""
            )

        file_size = Path(src_path).stat().st_size if Path(src_path).exists() else 0
        if file_size > FERNET_MAX_BYTES:
            raise ValueError(
                f"File too large to encrypt in-memory ({_human_size(file_size)}). "
                f"Fernet requires the entire file in RAM (hard limit: {_human_size(FERNET_MAX_BYTES)}). "
                "Options: (1) disable encryption for this watch, "
                "(2) enable compression which reduces file size before encrypting, or "
                "(3) exclude this file via exclude_patterns. "
                "To raise the limit, increase FERNET_MAX_BYTES in backup_engine.py."
            )
        if file_size > FERNET_WARN_BYTES:
            logger.warning(
                f"[encrypt] {Path(src_path).name} is {_human_size(file_size)} — "
                "Fernet loads the whole file into memory; performance may be impacted."
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
        try:
            import base64 as _b64
            raw = _b64.urlsafe_b64decode(key + "==")
            if len(raw) != 32:
                raise ValueError("decoded key must be 32 bytes")
        except Exception:
            raise ValueError(f"Invalid Fernet encryption key (must be 44-char URL-safe base64, 32 bytes decoded)")
        
        cipher = Fernet(key.encode())
        with open(src_path, 'rb') as f:
            ciphertext = f.read()
        plaintext = cipher.decrypt(ciphertext)
        with open(dest_path, 'wb') as f:
            f.write(plaintext)
    except Exception as e:
        raise RuntimeError(f"Decryption failed for {Path(src_path).name}: {e}")

        

def upload_to_dropbox(local_dir: str, cloud_config: dict) -> dict:
    """Upload backup folder to Dropbox, preserving subfolder structure."""
    try:
        import dropbox
        from dropbox.files import WriteMode, CommitInfo, UploadSessionCursor
    except ImportError:
        return {"ok": False, "error": "dropbox not installed — run: pip install dropbox"}

    CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB

    try:
        token       = cloud_config["access_token"]
        remote_path = cloud_config.get("remote_path", "/backupsys").rstrip("/")
        dbx         = dropbox.Dropbox(token)
        ld          = Path(local_dir)
        uploaded    = 0

        # Verify auth early
        dbx.users_get_current_account()

        # Internal backup metadata — never upload to cloud storage
        _SKIP = {"MANIFEST.json", "BACKUP.sha256"}

        for fp in ld.rglob("*"):
            if not fp.is_file():
                continue
            if fp.name in _SKIP:
                continue

            rel  = fp.relative_to(ld)
            dest = f"{remote_path}/{ld.name}/{rel}".replace("\\", "/")
            size = fp.stat().st_size

            with open(fp, "rb") as f:
                if size <= CHUNK_SIZE:
                    # Small file — single request
                    dbx.files_upload(f.read(), dest, mode=WriteMode.overwrite)
                else:
                    # Large file — chunked session upload
                    session = dbx.files_upload_session_start(f.read(CHUNK_SIZE))
                    cursor  = UploadSessionCursor(
                        session_id=session.session_id,
                        offset=f.tell(),
                    )
                    session_finished = False
                    while True:
                        chunk     = f.read(CHUNK_SIZE)
                        remaining = size - f.tell()
                        if not chunk or remaining <= 0:
                            dbx.files_upload_session_finish(
                                chunk or b"", cursor,
                                CommitInfo(path=dest, mode=WriteMode.overwrite),
                            )
                            session_finished = True
                            break
                        dbx.files_upload_session_append_v2(chunk, cursor)
                        cursor.offset = f.tell()
                    # Safety net — prevents a dangling session if loop exits unexpectedly
                    if not session_finished:
                        dbx.files_upload_session_finish(
                            b"", cursor,
                            CommitInfo(path=dest, mode=WriteMode.overwrite),
                        )

            uploaded += 1

        return {"ok": True, "uploaded": uploaded, "path": remote_path}

    except dropbox.exceptions.AuthError:
        return {"ok": False, "error": "Invalid Dropbox access token"}
    except KeyError:
        return {"ok": False, "error": "Missing 'access_token' in cloud_config"}
    except Exception as e:
        return {"ok": False, "error": str(e)}
def upload_to_gdrive(local_dir: str, cloud_config: dict) -> dict:
    """Upload backup folder to Google Drive using OAuth user credentials.

    Reuses existing folders/files rather than creating duplicates on every run.
    Internal backup metadata (MANIFEST.json, BACKUP.sha256) are never uploaded.
    """
    try:
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload
    except ImportError:
        return {"ok": False, "error": "google libs not installed — run: pip install google-api-python-client google-auth"}
    try:
        creds = Credentials(
            token=cloud_config.get("access_token"),
            refresh_token=cloud_config.get("refresh_token"),
            token_uri="https://oauth2.googleapis.com/token",
            client_id=cloud_config.get("client_id"),
            client_secret=cloud_config.get("client_secret"),
        )
        folder_id = cloud_config.get("folder_id", "")
        service   = build("drive", "v3", credentials=creds)
        ld        = Path(local_dir)
        uploaded  = 0

        # Internal backup metadata — never upload to cloud storage
        _SKIP = {"MANIFEST.json", "BACKUP.sha256"}

        # Find an existing folder by name under parent, or create it
        def _find_or_create_folder(name: str, parent_id: str) -> str:
            q = (
                f"name = {repr(name)} "
                f"and mimeType = 'application/vnd.google-apps.folder' "
                f"and \'{parent_id}\' in parents "
                f"and trashed = false"
            )
            res = service.files().list(q=q, fields="files(id)", pageSize=1).execute()
            files = res.get("files", [])
            if files:
                return files[0]["id"]
            meta = {
                "name": name,
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [parent_id],
            }
            return service.files().create(body=meta, fields="id").execute()["id"]

        # Upload a file — overwrite if it already exists, otherwise create
        def _upload_file(fp: Path, parent_id: str):
            media = MediaFileUpload(str(fp), resumable=True)
            q = (
                f"name = {repr(fp.name)} "
                f"and \'{parent_id}\' in parents "
                f"and trashed = false"
            )
            res = service.files().list(q=q, fields="files(id)", pageSize=1).execute()
            existing = res.get("files", [])
            if existing:
                service.files().update(
                    fileId=existing[0]["id"],
                    media_body=media,
                ).execute()
            else:
                meta = {"name": fp.name, "parents": [parent_id]}
                service.files().create(body=meta, media_body=media, fields="id").execute()

        # Resolve the top-level watch folder (reuse if exists)
        top_parent    = folder_id if folder_id else "root"
        run_folder_id = _find_or_create_folder(ld.name, top_parent)

        _folder_cache: dict = {"": run_folder_id}

        def _get_or_create_folder(rel_parts):
            path_key  = ""
            parent_id = run_folder_id
            for part in rel_parts:
                path_key = path_key + "/" + part if path_key else part
                if path_key not in _folder_cache:
                    _folder_cache[path_key] = _find_or_create_folder(part, parent_id)
                parent_id = _folder_cache[path_key]
            return parent_id

        for fp in ld.rglob("*"):
            if fp.is_file() and fp.name not in _SKIP:
                rel       = fp.relative_to(ld)
                parent_id = _get_or_create_folder(list(rel.parts[:-1]))
                _upload_file(fp, parent_id)
                uploaded += 1

        return {"ok": True, "uploaded": uploaded, "folder_id": run_folder_id}
    except Exception as e:
        return {"ok": False, "error": str(e)}

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
    cloud_config: Optional[Dict] = None,
    triggered_by: Optional[str] = None,
) -> dict:
    """
    Execute a backup from source → destination.
    progress_cb(copied, total, current_file) is called after each file copy.
    Tracks failed files for reporting.
    Cloud upload result is persisted in MANIFEST.json and returned in result dict.
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
        "failed_files":  [],
        "cloud_upload":  None,  # Will be populated if cloud upload is configured
        "triggered_by":  triggered_by or "auto",
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
            if needed > 0 and free < needed * 1.1:
                raise OSError(
                    f"Insufficient disk space. Need ~{_human_size(int(needed * 1.1))}, "
                    f"only {_human_size(free)} available."
                )
        except OSError:
            raise
        except Exception as e:
            print(f"[backup] ⚠ Could not check disk space: {e}", flush=True)

        # ── Single-file source ──────────────────────────────
        if src_path.is_file():
            try:
                if encrypt_key and CRYPTO_AVAILABLE:
                    _encrypt_file(source, str(backup_dir / src_path.name), encrypt_key)
                elif compress:
                    dest_gz = backup_dir / (src_path.name + '.gz')
                    with open(source, 'rb') as f_in, gzip.open(str(dest_gz), 'wb') as f_out:
                        _sh.copyfileobj(f_in, f_out)
                else:
                    shutil.copy2(source, backup_dir / src_path.name)
                    if hash_file(str(source)) != hash_file(str(backup_dir / src_path.name)):
                        logger.warning(f"⚠ Integrity mismatch for {src_path.name} — file may have changed during backup")
                        result["failed_files"].append({
                            "path": src_path.name,
                            "reason": "Hash mismatch — file modified during backup"
                        })
                copied = 1
            except (PermissionError, OSError) as e:
                logger.warning(f"⚠ Could not copy {src_path.name}: {e}")
                result["failed_files"].append({"path": src_path.name, "reason": str(e)})

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
                                logger.warning(f"⚠ Integrity mismatch for {entry['path']} — file may have changed during backup")
                                result["failed_files"].append({
                                    "path": entry["path"],
                                    "reason": "Hash mismatch — file modified during backup"
                                })
                                continue

                        # Throttle if requested
                        if throttler:
                            try:
                                throttler.throttle(entry.get("size", 0))
                            except Exception:
                                pass

                        copied += 1

                    except (PermissionError, OSError) as e:
                        logger.warning(f"⚠ Skipped {entry['path']}: {e}")
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

        # Calculate compression ratio
        compression_ratio = 0.0
        if compress:
            uncompressed_est = sum(c.get("size", 0) for c in changes)
            actual_size = _safe_size(str(backup_dir))
            if uncompressed_est > 0:
                compression_ratio = round((1 - actual_size / uncompressed_est) * 100, 1)

        # ── Remote destination upload ──────────────────────────────────────────
        # Handles: cloud (Dropbox/GDrive), sftp, ftp, smb, https.
        # _dest_type is forwarded from desktop_app via cloud_config["_dest_type"].
        # Falls back to storage_type for backward compatibility.
        cloud_upload_result = None
        _dest_type = (cloud_config or {}).get("_dest_type", "") or storage_type

        if _dest_type == "cloud" and cloud_config:
            provider = cloud_config.get("provider", "dropbox")
            logger.info(f"☁ Uploading backup to cloud ({provider}): {watch_name}")
            if provider == "dropbox" and cloud_config.get("access_token"):
                cloud_upload_result = upload_to_dropbox(str(backup_dir), cloud_config)
            elif provider == "gdrive" and cloud_config.get("access_token"):
                cloud_upload_result = upload_to_gdrive(str(backup_dir), cloud_config)
            else:
                cloud_upload_result = {"ok": False, "error": f"Not connected — use the Connect button for {provider}"}

        elif _dest_type == "sftp" and TRANSPORT_AVAILABLE:
            sftp_cfg = (cloud_config or {}).get("sftp_config") or cloud_config or {}
            logger.info(f"📡 Uploading backup via SFTP: {watch_name}")
            cloud_upload_result = upload_to_sftp(str(backup_dir), sftp_cfg)

        elif _dest_type == "ftp" and TRANSPORT_AVAILABLE:
            ftp_cfg = (cloud_config or {}).get("ftp_config") or cloud_config or {}
            logger.info(f"📡 Uploading backup via FTP: {watch_name}")
            cloud_upload_result = upload_to_ftp(str(backup_dir), ftp_cfg)

        elif _dest_type == "smb" and TRANSPORT_AVAILABLE:
            smb_cfg = (cloud_config or {}).get("smb_config") or cloud_config or {}
            logger.info(f"📡 Uploading backup via SMB: {watch_name}")
            cloud_upload_result = upload_to_smb(str(backup_dir), smb_cfg)

        elif _dest_type == "https" and TRANSPORT_AVAILABLE:
            https_cfg = (cloud_config or {}).get("https_config") or cloud_config or {}
            logger.info(f"📡 Uploading backup via HTTPS: {watch_name}")
            cloud_upload_result = upload_to_https(str(backup_dir), https_cfg)

        if cloud_upload_result and not cloud_upload_result["ok"]:
            logger.warning(f"⚠ Remote upload failed: {cloud_upload_result['error']}")
        elif cloud_upload_result:
            logger.info(f"☁ Remote upload complete: {cloud_upload_result.get('uploaded', '?')} files uploaded")

        # ── Write MANIFEST (now includes cloud_upload_result) ────────
        manifest = {
            "backup_id":          result["id"],
            "watch_id":           watch_id,
            "watch_name":         watch_name,
            "source":             source,
            "timestamp":          ts,
            "status":             "success",
            "incremental":        incremental,
            "compressed":         compress,
            "compression_ratio":  compression_ratio,
            "files_copied":       copied,
            "changes":            changes,
            "snapshot":           new_snapshot,
            "duration_s":         duration,
            "throughput_mbs":     round(throughput_mbs, 2),
            "total_size_bytes":   total_size,
            "failed_files":       result.get("failed_files", []),
            "cloud_upload":       cloud_upload_result,  # NEW: persisted in manifest
            "triggered_by":       triggered_by or "auto",
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

        # ── Update result dict with all final stats ──────────────────
        result.update({
            "status":            "success",
            "files_changed":     len([c for c in changes if c["type"] != "unchanged"]),
            "files_copied":      copied,
            "total_files":       len(new_snapshot),
            "total_size":        _human_size(total_size),
            "total_size_bytes":  total_size,
            "backup_hash":       backup_hash,
            "backup_dir":        str(backup_dir),
            "duration_s":        duration,
            "throughput_mbs":    round(throughput_mbs, 2),
            "compression_ratio": compression_ratio,
            "snapshot":          new_snapshot,
            "progress":          100,
            "cloud_upload":      cloud_upload_result,
        })

        logger.info(
            f"✅ Backup complete: {watch_name} | {_human_size(total_size)} | "
            f"{throughput_mbs:.1f} MB/s | {duration}s"
            + (" | compressed" if compress else "")
        )

        # ── Email notification ─────────────────────────────────────────────
        # NOTE: desktop_app.py manages its own email/webhook notifications and
        # does NOT pass email_config or dest_* through cloud_config here.
        # These blocks fire only when run_backup() is called directly from a
        # CLI script or test harness with a fully-populated cloud_config dict.
        _email_cfg   = (cloud_config or {}).get("email_config", {})
        _webhook_url = (cloud_config or {}).get("webhook_url", "")
        _webhook_ok  = (cloud_config or {}).get("webhook_on_success", False)

        if _email_cfg and _email_cfg.get("enabled") and NOTIFICATIONS_AVAILABLE:
            _success = result["status"] == "success"
            _should_email = (
                (_success     and _email_cfg.get("notify_on_success", False)) or
                (not _success and _email_cfg.get("notify_on_failure", True))
            )
            if _should_email:
                try:
                    _subj, _body = build_backup_email(result)
                    _er = send_email_notification(_email_cfg, _subj, _body)
                    if not _er["ok"]:
                        logger.warning(f"[email] Notification failed: {_er['error']}")
                except Exception as _ne:
                    logger.warning(f"[email] Unexpected error sending notification: {_ne}")

        # ── Webhook notification ───────────────────────────────────────────
        if _webhook_url and NOTIFICATIONS_AVAILABLE:
            _success = result["status"] == "success"
            if not _success or _webhook_ok:
                try:
                    send_webhook_notification(_webhook_url, {
                        "event":        "backup_complete",
                        "status":       result.get("status", "unknown"),
                        "watch_name":   watch_name,
                        "watch_id":     watch_id,
                        "backup_id":    result.get("id"),
                        "timestamp":    ts,
                        "files_copied": result.get("files_copied", 0),
                        "total_size":   result.get("total_size", "0 B"),
                        "duration_s":   result.get("duration_s", 0),
                        "error":        result.get("error"),
                    })
                except Exception as _we:
                    logger.warning(f"[webhook] Unexpected error sending notification: {_we}")

    except InterruptedError:
        result["error"]      = "Backup cancelled by user"
        result["status"]     = "cancelled"
        result["duration_s"] = round(time.time() - started, 2)
        if backup_dir is not None and Path(backup_dir).exists():
            shutil.rmtree(str(backup_dir), ignore_errors=True)
        logger.warning(f"⚠ Backup cancelled: {watch_name}")

    except Exception as e:
        import traceback
        logger.error(f"❌ Backup failed for {watch_name}: {e}\n{traceback.format_exc()}")
        result["error"]      = str(e)
        result["status"]     = "failed"
        result["duration_s"] = round(time.time() - started, 2)
        # Clean up the partial backup directory so it doesn't waste disk space.
        # (InterruptedError already does this above; we mirror it here for all
        # other failures — disk full, permission error, network drop, etc.)
        if backup_dir is not None and Path(backup_dir).exists():
            shutil.rmtree(str(backup_dir), ignore_errors=True)
            logger.info(f"🗑 Removed partial backup dir after failure: {backup_dir}")

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

def restore_backup(backup_dir: str, target_path: str, incremental_only: bool = False, encrypt_key: str = None) -> dict:
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
        snap     = manifest.get("snapshot", {})  # per-file hashes for integrity check

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

                elif src_file.exists():  # ← updated
                    try:
                        dst_file.parent.mkdir(parents=True, exist_ok=True)
                        if encrypt_key and CRYPTO_AVAILABLE:
                            _decrypt_file(str(src_file), str(dst_file), encrypt_key)
                        else:
                            shutil.copy2(str(src_file), str(dst_file))

                        # ── Post-restore integrity check ──────────────────
                        # Verify restored file hash matches the original
                        # snapshot entry (skipped for encrypted/compressed
                        # files since their hash would differ).
                        if not encrypt_key and not gz_file.exists():
                            expected_hash = snap.get(rel, {}).get("hash", "")
                            if expected_hash:
                                actual_hash = hash_file(str(dst_file))
                                if actual_hash and actual_hash != expected_hash:
                                    result["errors"].append(
                                        f"{rel}: hash mismatch after restore "
                                        f"(expected {expected_hash[:8]}…, "
                                        f"got {actual_hash[:8]}…)"
                                    )
                                    logger.warning(
                                        f"[restore] ⚠ Hash mismatch for {rel} — "
                                        "file may be corrupted in the backup"
                                    )
                                    continue  # don't count as successfully restored

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


def restore_full_chain(
    destination: str,
    watch_id: str,
    target_path: str,
    up_to_backup_id: Optional[str] = None,
    encrypt_key: Optional[str] = None,
    progress_cb: Optional[Callable[[int, int, str], None]] = None,
) -> dict:
    """
    Restore the FULL incremental chain for a watch to target_path.

    Unlike restore_backup() which only restores a single snapshot delta,
    this function replays every incremental backup in chronological order
    (oldest → newest) so the target directory ends up in the exact state
    it was in at the chosen point in time.

    Args:
        destination:      The backup root directory (cfg["destination"]).
        watch_id:         ID of the watch to restore.
        target_path:      Where to write the restored files.
        up_to_backup_id:  Stop after this backup ID (inclusive). If None,
                          restores up to the most recent backup.
        encrypt_key:      Fernet key to decrypt encrypted backups.
        progress_cb:      Optional progress(step, total_steps, label) callback.

    Returns a dict:
        ok, files_restored, steps_applied, skipped, errors, error
    """
    result: dict = {
        "ok": False,
        "files_restored": 0,
        "steps_applied": 0,
        "skipped": 0,
        "errors": [],
        "error": None,
    }

    try:
        # All backups for this watch, newest-first from the index
        all_backups = list_backups(destination, watch_id)
        if not all_backups:
            result["error"] = f"No backups found for watch {watch_id}"
            return result

        # Reverse to chronological order (oldest first)
        chain = list(reversed(all_backups))

        # Trim chain to up_to_backup_id if specified
        if up_to_backup_id:
            trimmed = []
            for b in chain:
                trimmed.append(b)
                if b.get("backup_id") == up_to_backup_id:
                    break
            else:
                result["error"] = f"backup_id '{up_to_backup_id}' not found in chain"
                return result
            chain = trimmed

        total_steps = len(chain)
        Path(target_path).mkdir(parents=True, exist_ok=True)

        for step_idx, backup_meta in enumerate(chain):
            backup_dir = backup_meta.get("backup_dir", "")
            bid        = backup_meta.get("backup_id", "?")
            ts         = backup_meta.get("timestamp", "")

            if not backup_dir or not Path(backup_dir).exists():
                result["errors"].append(f"Step {step_idx + 1}: backup dir missing ({backup_dir})")
                continue

            if progress_cb:
                try:
                    progress_cb(step_idx + 1, total_steps, f"Applying {ts[:19]} ({bid[:8]}…)")
                except Exception:
                    pass

            step_result = restore_backup(
                backup_dir=backup_dir,
                target_path=target_path,
                incremental_only=False,
                encrypt_key=encrypt_key,
            )

            result["files_restored"] += step_result.get("files_restored", 0)
            result["skipped"]        += step_result.get("skipped", 0)
            result["errors"].extend(step_result.get("errors", []))
            result["steps_applied"]  += 1

        result["ok"] = result["steps_applied"] > 0
        if not result["ok"] and not result["error"]:
            result["error"] = "No backup steps could be applied"

    except Exception as e:
        import traceback
        result["error"] = str(e)
        logger.error(f"[restore_chain] Failed: {e}\n{traceback.format_exc()}")

    return result


# ─── Cleanup ──────────────────────────────────────────────────────────────────

def cleanup_old_backups(destination: str, retention_days: int, watch_id: Optional[str] = None) -> dict:
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
            # Orphaned directory — left by a failed backup that was interrupted
            # before its MANIFEST could be written.  Clean it up if it's old
            # enough (>1 day grace period) so we don't remove in-progress dirs.
            try:
                age_hours = (datetime.now().timestamp() - d.stat().st_mtime) / 3600
                if age_hours > 24:
                    size = _safe_size(str(d))
                    shutil.rmtree(str(d), ignore_errors=True)
                    result["deleted"] += 1
                    result["freed_bytes"] += size
                    logger.info(f"🗑 Removed orphaned backup dir (no MANIFEST, {age_hours:.0f}h old): {d.name}")
            except Exception as e:
                result["errors"].append(f"orphan {d.name}: {e}")
            continue

        try:
            with open(manifest_p) as f:
                m = json.load(f)

            # Skip if scoped to a specific watch and this backup doesn't match
            if watch_id and m.get("watch_id") != watch_id:
                continue

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
    """Full UUID4 hex for zero collision risk."""
    return uuid.uuid4().hex  # 32 chars, no truncation


# ─── Max-Backups Pruning ──────────────────────────────────────────────────────

def prune_excess_backups(destination: str, watch_id: str, max_backups: int) -> dict:
    """
    Delete the oldest backups for a watch when the count exceeds max_backups.
    Called automatically after each successful backup if max_backups > 0.
    """
    result = {"pruned": 0, "freed_bytes": 0, "freed_human": "0 B", "errors": []}

    if max_backups <= 0:
        return result

    backups = list_backups(destination, watch_id)  # newest-first
    if len(backups) <= max_backups:
        return result

    to_delete = backups[max_backups:]  # everything beyond the keep limit

    for b in to_delete:
        bd = b.get("backup_dir", "")
        if not bd or not Path(bd).exists():
            continue
        try:
            size = _safe_size(bd)
            shutil.rmtree(bd, ignore_errors=True)
            result["pruned"]      += 1
            result["freed_bytes"] += size
        except Exception as e:
            result["errors"].append(str(e))

    result["freed_human"] = _human_size(result["freed_bytes"])

    if result["pruned"]:
        _backup_index.invalidate(destination)
        logger.info(
            f"✂  Pruned {result['pruned']} excess backup(s) for watch {watch_id} "
            f"(max={max_backups}), freed {result['freed_human']}"
        )

    return result