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


class BackupThrottler:
    """Limit backup I/O to prevent system overload.

    Designed to be called per-chunk (inside the read/write loop), not per-file.
    Uses a sliding 1-second window so each window resets cleanly — no debt
    carries over from previous windows, preventing the 'giant sleep' bug that
    occurred when a large file reset bytes_sent to a non-zero value.
    """
    def __init__(self, max_mbps: float = 100.0):
        self.max_bytes_per_sec = int(max_mbps * 1024 * 1024)
        self.window_start  = time.time()
        self.window_bytes  = 0

    def throttle(self, bytes_copied: int):
        """Call after writing each chunk.  Thread-safe for single-writer use."""
        if self.max_bytes_per_sec <= 0:
            return  # unlimited

        self.window_bytes += bytes_copied
        elapsed = time.time() - self.window_start

        # How long *should* it have taken to send window_bytes at our limit?
        expected = self.window_bytes / self.max_bytes_per_sec
        wait = expected - elapsed
        if wait > 0:
            time.sleep(wait)

        # Reset window every second so debt never accumulates across windows.
        # This is the critical fix: old code used bytes_sent = bytes_copied
        # (carrying the full window debt forward), which caused the next small
        # file to sleep for tens of minutes.
        if (time.time() - self.window_start) >= 1.0:
            self.window_start = time.time()
            self.window_bytes = 0


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
    # Preserve UNC/SMB network paths (\\server\\share or //server/share).
    if path.startswith('\\\\') or path.startswith('//'):
        return path.replace('/', '\\')
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

def hash_file(path: str, cancel_event=None) -> str:
    """SHA-256 hash of a single file.  Returns '' on any I/O error.

    cancel_event: optional threading.Event — if set, raises InterruptedError
    between chunks so large-file hashing on slow/network drives can be aborted
    immediately without waiting for the full file to be read.
    """
    h = hashlib.sha256()
    try:
        with open(path, "rb") as f:
            chunk_count = 0
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
                # Check cancel every 16 chunks (~1 MB) so we don't slow down
                # fast local hashing while still responding quickly on large
                # network files where each chunk takes real time.
                chunk_count += 1
                if chunk_count % 16 == 0 and cancel_event is not None and cancel_event.is_set():
                    raise InterruptedError("Backup cancelled by user")
        return h.hexdigest()
    except InterruptedError:
        raise  # propagate cancel — do not swallow
    except (IOError, OSError):
        return ""


def _dest_already_identical(src_file: Path, dest_file: Path) -> bool:
    """Return True if *dest_file* already exists and has the same size as
    *src_file*.

    Size-only comparison (no mtime) is intentional: NAS/SMB shares sometimes
    report timestamps with timezone offsets or precision differences that make
    mtime checks unreliable even when files are identical.  For the seeding
    use-case (user pre-populated the backup destination before adding the
    watch) a matching size is a strong enough signal to skip the re-copy.
    Files whose content changed but size stayed the same are an edge case that
    the next incremental run will catch via the snapshot hash diff.
    """
    try:
        if not dest_file.exists():
            return False
        return src_file.stat().st_size == dest_file.stat().st_size
    except OSError:
        return False


# Fix #3 — exclude MANIFEST.json so the directory hash stays stable after
# the manifest is written during run_backup().
EXCLUDE = {"BACKUP.sha256", "MANIFEST.json"}


class _HashingWriter:
    """Wraps a writable file-like object and computes SHA-256 over every byte
    that flows through it.  Used so gzip/other writers can stream bytes to disk
    while we hash them in a single pass — no second read of the output file.

    Usage::

        with open(dest, 'wb') as raw:
            hw = _HashingWriter(raw)
            with gzip.open(hw, 'wb') as gz:
                # ... write chunks ...
        digest = hw.hexdigest()
    """

    def __init__(self, f):
        self._f = f
        self._h = hashlib.sha256()

    def write(self, data: bytes) -> int:
        self._h.update(data)
        return self._f.write(data)

    def hexdigest(self) -> str:
        return self._h.hexdigest()

    # Forward every other attribute access to the underlying file object so
    # gzip.open() (and anything else) can call flush(), tell(), etc. normally.
    def __getattr__(self, name):
        return getattr(self._f, name)


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
    timeout_sec: int = 300,
    scan_cb: Optional[Callable] = None,
    cancel_event=None,
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

    # Network/UNC paths are slow to enumerate — give them much more time.
    # 300s (5 min) is too short for large SMB shares.
    _norm = path.replace("/", "\\")
    if _norm.startswith("\\\\") and timeout_sec <= 300:
        timeout_sec = 7200  # 2 hours for network paths

    # -------------------------------------------------
    # Internal scanning logic
    # -------------------------------------------------
    # result_container is declared here (outside _do_scan) so that both the
    # Windows thread-timeout path and the Unix SIGALRM path can access it,
    # and so _do_scan can write partial results for the timeout handler.
    result_container: dict = {}

    def _do_scan() -> Dict[str, dict]:

        snapshot: Dict[str, dict] = {}
        result_container["data"] = snapshot  # live reference — partial results visible on timeout

        # On first backup there is no previous snapshot, so every file would
        # need to be hashed here — but run_backup copies every file anyway
        # and computes hashes inline during copy (backfilling the snapshot).
        # Skipping hashing here avoids reading each file twice on large/network
        # sources and cuts first-backup scan time to a fast directory walk only.
        _first_backup = not previous

        # -------------------------
        # Single File Case
        # -------------------------
        if root.is_file():
            try:
                stat = root.stat()
                snapshot[root.name] = {
                    "hash": "" if _first_backup else hash_file(str(root), cancel_event=cancel_event),
                    "size": stat.st_size,
                    "mtime": stat.st_mtime,
                }
            except InterruptedError:
                raise
            except (IOError, OSError, PermissionError):
                pass

            return snapshot

        # -------------------------
        # Directory Walk
        # -------------------------
        for fp in root.rglob("*"):

            # Check cancel on every file entry, before any I/O.
            # This ensures cancellation is responsive even on slow/network
            # sources where each rglob step can stall for seconds.
            if cancel_event is not None and cancel_event.is_set():
                raise InterruptedError("Backup cancelled by user")

            if fp.is_symlink():
                continue  # skip symlinks to prevent loops and traversal

            if not fp.is_file():
                continue

            rel = str(fp.relative_to(root))

            if exclude_patterns and _is_excluded(rel, fp, exclude_patterns):
                continue

            try:
                stat = fp.stat()
                if scan_cb:
                    try:
                        scan_cb(rel)
                    except InterruptedError:
                        raise  # let cancel propagate — do NOT swallow
                    except Exception:
                        pass

                # Reuse previous entry if mtime+size unchanged (incremental fast path)
                if (
                    previous
                    and rel in previous
                    and previous[rel].get("mtime") == stat.st_mtime
                    and previous[rel].get("size") == stat.st_size
                ):
                    snapshot[rel] = previous[rel]
                    continue

                snapshot[rel] = {
                    # First backup: leave hash empty — run_backup fills it in
                    # while copying so each file is read only once.
                    # Subsequent backups: only new/changed files reach here.
                    "hash": "" if _first_backup else hash_file(str(fp), cancel_event=cancel_event),
                    "size": stat.st_size,
                    "mtime": stat.st_mtime,
                }

            except InterruptedError:
                raise  # cancel must not be swallowed by the I/O except below
            except (IOError, OSError, PermissionError):
                continue

        return snapshot

    # -------------------------------------------------
    # WINDOWS TIMEOUT PROTECTION
    # -------------------------------------------------
    if os.name == "nt":

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
            partial = result_container.get("data", {})
            n = len(partial) if isinstance(partial, dict) else 0
            logger.warning(
                f"[snapshot] Timed out scanning '{path}' after {timeout_sec}s "
                f"— using {n} partial file(s) found so far"
            )
            return partial if isinstance(partial, dict) else {}

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
    """Generate a new Fernet/AES encryption key (44 characters, URL-safe base64)."""
    if not CRYPTO_AVAILABLE:
        raise RuntimeError("cryptography library not installed")
    return Fernet.generate_key().decode('utf-8')


# ─── Streaming AES-256-GCM encryption (no file-size limit) ───────────────────
#
# File format  (v2, magic = b'BACKENC1'):
#   [8]  magic  b'BACKENC1'
#   [12] nonce_seed   random bytes; per-chunk nonce = seed XOR little-endian chunk index
#   Repeated until EOF:
#     [4]  uint32 LE  length of encrypted chunk (ciphertext + 16-byte GCM tag)
#     [N]  encrypted chunk  (AES-256-GCM, tag appended by hazmat)
#   [4]  uint32 LE  0x00000000  — EOF sentinel
#
# The Fernet key (32 raw bytes from URL-safe base64) is re-derived to an
# AES-256 key via HKDF-SHA256 so the same user-visible key works for both
# legacy Fernet files and new streaming files.
#
# Backward compat:
#   _decrypt_file() sniffs the first 8 bytes:
#     b'BACKENC1' → streaming AES-GCM (this format)
#     anything else → legacy Fernet

_AES_MAGIC   = b"BACKENC1"
_AES_CHUNK   = 1 * 1024 * 1024   # 1 MB plaintext per chunk


def _derive_aes_key(raw_key_bytes: bytes) -> bytes:
    """Derive a 32-byte AES-256 key from the Fernet key material via HKDF-SHA256."""
    from cryptography.hazmat.primitives.kdf.hkdf import HKDF
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.backends import default_backend
    return HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=None,
        info=b"BackupSys-AES-GCM-v1",
        backend=default_backend(),
    ).derive(raw_key_bytes)


def _validate_key(key: str) -> bytes:
    """Validate and return the 32 raw bytes of an encryption key string."""
    import base64 as _b64
    try:
        raw = _b64.urlsafe_b64decode(key + "==")
        if len(raw) != 32:
            raise ValueError("decoded key must be 32 bytes")
        return raw
    except Exception:
        raise ValueError(
            "Invalid encryption key (must be 44-char URL-safe base64, 32 bytes decoded). "
            "Generate one with: python -c \"from backup_engine import generate_encryption_key; "
            "print(generate_encryption_key())\""
        )


def _encrypt_file(src_path: str, dest_path: str, key: str) -> str:
    """Encrypt a file using streaming AES-256-GCM.

    Processes the source in 1 MB chunks so arbitrarily large files are
    supported with constant ~2 MB RAM overhead.  Returns the SHA-256 hex
    digest of the *ciphertext* written to dest_path.

    Legacy Fernet files written by earlier BackupSys versions are still
    decryptable via _decrypt_file() — only new writes use this format.
    """
    if not CRYPTO_AVAILABLE:
        raise RuntimeError("cryptography library not installed")

    try:
        import struct
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        raw_key  = _validate_key(key)
        aes_key  = _derive_aes_key(raw_key)
        cipher   = AESGCM(aes_key)

        # 12-byte random seed; per-chunk nonce = seed XOR little-endian chunk index
        import os as _os
        nonce_seed = _os.urandom(12)

        out_hash = hashlib.sha256()

        with open(src_path, "rb") as f_in, open(dest_path, "wb") as f_out:
            # Header
            header = _AES_MAGIC + nonce_seed
            f_out.write(header)
            out_hash.update(header)

            chunk_idx = 0
            while True:
                plaintext = f_in.read(_AES_CHUNK)
                if not plaintext:
                    break
                # Per-chunk nonce: XOR seed with little-endian index
                idx_bytes = struct.pack("<Q", chunk_idx)[:12].ljust(12, b"\x00")
                nonce = bytes(a ^ b for a, b in zip(nonce_seed, idx_bytes))
                ciphertext = cipher.encrypt(nonce, plaintext, None)  # 16-byte GCM tag appended
                length_field = struct.pack("<I", len(ciphertext))
                f_out.write(length_field)
                out_hash.update(length_field)
                f_out.write(ciphertext)
                out_hash.update(ciphertext)
                chunk_idx += 1

            # EOF sentinel
            eof = struct.pack("<I", 0)
            f_out.write(eof)
            out_hash.update(eof)

        return out_hash.hexdigest()

    except Exception as e:
        raise RuntimeError(f"Encryption failed for {Path(src_path).name}: {e}")


def _decrypt_file(src_path: str, dest_path: str, key: str) -> None:
    """Decrypt a file.  Auto-detects format: streaming AES-GCM (v2) or legacy Fernet (v1)."""
    if not CRYPTO_AVAILABLE:
        raise RuntimeError("cryptography library not installed")

    try:
        with open(src_path, "rb") as fh:
            magic = fh.read(8)

        if magic == _AES_MAGIC:
            # ── Streaming AES-256-GCM ─────────────────────────────────────────
            import struct
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM

            raw_key = _validate_key(key)
            aes_key = _derive_aes_key(raw_key)
            cipher  = AESGCM(aes_key)

            with open(src_path, "rb") as f_in, open(dest_path, "wb") as f_out:
                f_in.read(8)  # skip magic (already read above)
                nonce_seed = f_in.read(12)
                chunk_idx  = 0
                while True:
                    len_field = f_in.read(4)
                    if len(len_field) < 4:
                        break
                    clen = struct.unpack("<I", len_field)[0]
                    if clen == 0:
                        break   # EOF sentinel
                    ciphertext = f_in.read(clen)
                    idx_bytes  = struct.pack("<Q", chunk_idx)[:12].ljust(12, b"\x00")
                    nonce      = bytes(a ^ b for a, b in zip(nonce_seed, idx_bytes))
                    plaintext  = cipher.decrypt(nonce, ciphertext, None)
                    f_out.write(plaintext)
                    chunk_idx += 1
        else:
            # ── Legacy Fernet (v1) ────────────────────────────────────────────
            _validate_key(key)
            cipher = Fernet(key.encode())
            with open(src_path, "rb") as f:
                ciphertext = f.read()
            plaintext = cipher.decrypt(ciphertext)
            with open(dest_path, "wb") as f:
                f.write(plaintext)

    except Exception as e:
        raise RuntimeError(f"Decryption failed for {Path(src_path).name}: {e}")
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


def download_from_gdrive(cloud_config: dict, local_dest_dir: str) -> dict:
    """Download all backup folders from Google Drive to local directory.

    Downloads all subfolders from the configured folder_id to local_dest_dir.
    Recreates the directory structure locally.
    """
    try:
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaIoBaseDownload
        import io
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
        ld        = Path(local_dest_dir)
        ld.mkdir(parents=True, exist_ok=True)
        downloaded = 0

        # The root is the folder_id
        root_id = folder_id if folder_id else "root"

        # Recursively download folders and files
        def _download_folder(remote_id: str, local_path: Path):
            nonlocal downloaded
            # List files and folders in this remote folder
            page_token = None
            while True:
                res = service.files().list(
                    q=f"\'{remote_id}\' in parents and trashed = false",
                    fields="nextPageToken, files(id, name, mimeType)",
                    pageToken=page_token
                ).execute()
                for item in res.get("files", []):
                    item_path = local_path / item["name"]
                    if item["mimeType"] == "application/vnd.google-apps.folder":
                        item_path.mkdir(exist_ok=True)
                        _download_folder(item["id"], item_path)
                    else:
                        # Download file
                        request = service.files().get_media(fileId=item["id"])
                        with open(item_path, "wb") as f:
                            downloader = MediaIoBaseDownload(f, request)
                            done = False
                            while not done:
                                status, done = downloader.next_chunk()
                        downloaded += 1
                page_token = res.get("nextPageToken")
                if not page_token:
                    break

        _download_folder(root_id, ld)
        return {"ok": True, "downloaded": downloaded}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ─── VSS (Volume Shadow Copy Service) ────────────────────────────────────────
# Allows BackupSys to read open/locked files (Outlook PST, browser databases,
# running application databases) by taking a point-in-time snapshot via Windows
# VSS before the backup starts.  Requires Windows and admin privileges.
# Fails gracefully — if VSS is unavailable, backup continues on live files.

def _vss_create_snapshot(volume: str) -> tuple:
    """
    Create a VSS shadow copy of *volume* (e.g. 'C:\\').
    Returns (shadow_id: str, shadow_device_path: str) on success,
    or (None, None) on failure.

    shadow_device_path looks like:
        \\\\?\\GLOBALROOT\\Device\\HarddiskVolumeShadowCopy5\\
    Call _vss_delete_snapshot(shadow_id) after the backup completes.
    """
    if os.name != "nt":
        return None, None
    try:
        import subprocess
        # PowerShell one-liner: create shadow copy, return ID|DeviceObject
        ps_cmd = (
            f'$s=(Get-WmiObject -List Win32_ShadowCopy).Create("{volume}","ClientAccessible");'
            f'$sc=Get-WmiObject Win32_ShadowCopy -Filter "ID=\'$($s.ShadowID)\'";'
            f'Write-Output ($sc.ID + "|" + $sc.DeviceObject)'
        )
        r = subprocess.run(
            ["powershell", "-NonInteractive", "-NoProfile", "-Command", ps_cmd],
            capture_output=True, text=True, timeout=30,
        )
        out = r.stdout.strip()
        if r.returncode == 0 and "|" in out:
            shadow_id, device = out.split("|", 1)
            device = device.strip()
            if not device.endswith("\\"):
                device += "\\"
            logger.info(f"[vss] Shadow copy created: {device}")
            return shadow_id.strip(), device
    except Exception as e:
        logger.warning(
            f"[vss] Shadow copy unavailable: {e}. "
            "Install pywin32 (pip install pywin32) and run as Administrator to enable "
            "VSS support for locked/open files (Outlook PST, browser databases, etc.)."
        )
    return None, None


def _vss_delete_snapshot(shadow_id: str) -> None:
    """Delete a VSS shadow copy by ID (best-effort; never raises)."""
    if not shadow_id or os.name != "nt":
        return
    try:
        import subprocess
        ps_cmd = (
            f'$sc=Get-WmiObject Win32_ShadowCopy -Filter "ID=\'{shadow_id}\'";'
            f'if($sc){{$sc.Delete()}}'
        )
        subprocess.run(
            ["powershell", "-NonInteractive", "-NoProfile", "-Command", ps_cmd],
            capture_output=True, timeout=15,
        )
        logger.debug(f"[vss] Shadow copy deleted: {shadow_id}")
    except Exception as e:
        logger.debug(f"[vss] Failed to delete shadow copy {shadow_id}: {e}")


def _vss_remap_source(source: str, device_path: str) -> str:
    """
    Map a live source path to its equivalent inside the shadow copy.

    Example:
        source      = C:\\Users\\user\\Documents
        device_path = \\\\?\\GLOBALROOT\\Device\\HarddiskVolumeShadowCopy5\\
        result      = \\\\?\\GLOBALROOT\\Device\\HarddiskVolumeShadowCopy5\\Users\\user\\Documents
    """
    p = Path(source)
    # strip drive letter + backslash, e.g. "C:\Users\..." → "Users\..."
    rel = str(p)[len(p.anchor):]
    return device_path + rel


def run_backup(
    source: str,
    destination: str,
    watch_id: str,
    watch_name: str,
    storage_type: str,
    previous_snapshot: Optional[Dict] = None,
    incremental: bool = True,
    progress_cb: Optional[Callable[[int, int, str], None]] = None,
    scan_cb: Optional[Callable[[str], None]] = None,
    exclude_patterns: Optional[List[str]] = None,
    encrypt_key: Optional[str] = None,
    compress: bool = False,
    throttler=None,
    cloud_config: Optional[Dict] = None,
    destinations: Optional[List[Dict]] = None,
    triggered_by: Optional[str] = None,
    cancel_event=None,
    sync_mode: bool = False,
    dry_run: bool = False,
    max_file_size_mb: float = 0,
    pre_backup_cmd: Optional[str] = None,
    post_backup_cmd: Optional[str] = None,
) -> dict:
    """
    Execute a backup from source → destination.
    progress_cb(copied, total, current_file) is called after each file copy.
    Tracks failed files for reporting.
    Cloud upload result is persisted in MANIFEST.json and returned in result dict.

    dry_run=True: scans source and builds the change list but copies nothing.
                  Returns immediately with status='dry_run' and the full changes list.
    max_file_size_mb: if > 0, skip files larger than this many MB (0 = no limit).
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
        "encrypted":     bool(encrypt_key),
        "failed_files":  [],
        "destinations_upload":  [],  # Will be populated if destinations are configured
        "triggered_by":  triggered_by or "auto",
    }

    backup_dir = None

    # VSS and resume state — initialised before the try block so the
    # finally clause can always clean them up safely.
    _vss_shadow_id = None
    _resume_path   = dest_root / f"_resume_{watch_id}.json"

    # ── Pre-backup hook ────────────────────────────────────────────────────────
    if pre_backup_cmd and pre_backup_cmd.strip():
        logger.info(f"[hook] Running pre-backup command: {pre_backup_cmd}")
        try:
            import subprocess as _sp
            _pre = _sp.run(
                pre_backup_cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=300,  # 5-minute hard timeout
            )
            if _pre.stdout.strip():
                logger.info(f"[hook] pre-backup stdout: {_pre.stdout.strip()}")
            if _pre.stderr.strip():
                logger.warning(f"[hook] pre-backup stderr: {_pre.stderr.strip()}")
            if _pre.returncode != 0:
                raise RuntimeError(
                    f"Pre-backup command exited with code {_pre.returncode}: "
                    f"{_pre.stderr.strip() or _pre.stdout.strip()}"
                )
            logger.info(f"[hook] Pre-backup command succeeded (exit 0)")
        except Exception as _hook_err:
            result["error"] = f"Pre-backup hook failed: {_hook_err}"
            result["status"] = "failed"
            return result

    try:
        if not src_path.exists():
            raise FileNotFoundError(f"Source not found: {source}")

        # ── VSS: create a shadow copy so locked/open files can be read ──────
        # Only attempted on Windows, only for local or UNC sources (not SFTP paths),
        # and only for directory sources (not single-file watches).
        # Fails gracefully: if VSS is unavailable the backup continues on live files.
        _effective_source = source
        if (os.name == "nt"
                and src_path.is_dir()
                and not str(source).startswith("\\\\?\\")  # not already a shadow path
                and Path(source).drive):                    # has a drive letter (local disk)
            _volume = str(Path(source).drive) + "\\"
            _sid, _sdevice = _vss_create_snapshot(_volume)
            if _sid and _sdevice:
                _vss_shadow_id    = _sid
                _effective_source = _vss_remap_source(source, _sdevice)
                logger.info(f"[vss] Reading from shadow copy: {_effective_source}")
            else:
                logger.warning(
                    "[vss] Shadow copy unavailable — reading from live filesystem. "
                    "Open/locked files (Outlook PST, browser DBs) may be skipped. "
                    "Install pywin32 and run as Administrator to enable VSS."
                )

        safe_name  = "".join(c if c.isalnum() else "_" for c in watch_name)

        # ── Resume: reuse a partial backup dir from a previous interrupted run ──
        _resume_completed: set = set()
        if not sync_mode and _resume_path.exists():
            try:
                _rc      = json.loads(_resume_path.read_text(encoding="utf-8"))
                _rc_dir  = Path(_rc.get("backup_dir", ""))
                if (_rc_dir.exists()
                        and str(_rc_dir.parent).lower() == str(dest_root).lower()):
                    backup_dir          = _rc_dir
                    _resume_completed   = set(_rc.get("completed", []))
                    logger.info(
                        f"[resume] Resuming interrupted backup: {_rc_dir.name} "
                        f"({len(_resume_completed)} file(s) already done)"
                    )
            except Exception as _re:
                logger.debug(f"[resume] Ignoring bad checkpoint: {_re}")
                try:
                    _resume_path.unlink()
                except Exception:
                    pass

        if backup_dir is None:
            if sync_mode:
                backup_dir = dest_root
                backup_dir.mkdir(parents=True, exist_ok=True)
            else:
                backup_dir = dest_root / f"{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}__{safe_name}"
                backup_dir.mkdir(parents=True, exist_ok=True)

        new_snapshot = build_snapshot(
            _effective_source,
            previous=previous_snapshot,
            exclude_patterns=exclude_patterns or [],
            scan_cb=scan_cb,
            cancel_event=cancel_event,
        )

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
        total       = len(files_to_copy)

        # ── Size filter — skip files exceeding the per-watch size cap ─────────
        if max_file_size_mb and max_file_size_mb > 0:
            _size_limit = int(max_file_size_mb * 1024 * 1024)
            _skipped_large = [c for c in files_to_copy if c.get("size", 0) > _size_limit]
            files_to_copy  = [c for c in files_to_copy if c.get("size", 0) <= _size_limit]
            if _skipped_large:
                logger.info(
                    f"[backup] '{watch_name}': skipped {len(_skipped_large)} file(s) "
                    f"exceeding {max_file_size_mb:.0f} MB size limit"
                )
                for _sl in _skipped_large:
                    result["failed_files"].append({
                        "path": _sl["path"],
                        "reason": f"Exceeds max_file_size_mb={max_file_size_mb:.0f} MB "
                                  f"(file is {_human_size(_sl.get('size', 0))})",
                    })
            total = len(files_to_copy)

        # ── Dry run — return change list without copying anything ─────────────
        if dry_run:
            result.update({
                "status":        "dry_run",
                "total_files":   len(new_snapshot),
                "files_changed": len([c for c in changes if c["type"] != "unchanged"]),
                "files_to_copy": total,
                "total_size":    _human_size(sum(c.get("size", 0) for c in files_to_copy)),
                "total_size_bytes": sum(c.get("size", 0) for c in files_to_copy),
                "snapshot":      new_snapshot,
                "progress":      100,
            })
            logger.info(
                f"[dry-run] '{watch_name}': {total} file(s) would be copied "
                f"({result['total_size']})"
            )
            return result
        copied      = 0
        logger.info(
            f"[backup] '{watch_name}': {total} file(s) to process — "
            f"sync_mode={sync_mode}, incremental={incremental}, "
            f"encrypt={bool(encrypt_key)}, compress={compress}"
        )
        # Byte-level progress tracking for accurate ETA on large files.
        # total_bytes is pre-computed from the change list sizes; bytes_done
        # is incremented as each file finishes so the UI can show MB/s & ETA.
        total_bytes = sum(c.get("size", 0) for c in files_to_copy)
        bytes_done  = 0

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

        # Track hashes of every file actually written to backup_dir.
        # Keys are relative paths matching what validate_backup sees via rglob
        # (e.g. "subdir/file.txt" for plain/encrypt, "subdir/file.txt.gz" for
        # compress, "subdir/file.txt.DELETED" for deletion markers).
        # Using this instead of new_snapshot fixes the incremental case where
        # backup_dir only contains changed files but new_snapshot has all files.
        _files_written_hashes: dict = {}

        # ── Single-file source ──────────────────────────────
        if src_path.is_file():
            try:
                if encrypt_key and CRYPTO_AVAILABLE:
                    _files_written_hashes[src_path.name] = _encrypt_file(source, str(backup_dir / src_path.name), encrypt_key)
                elif compress:
                    dest_gz = backup_dir / (src_path.name + '.gz')
                    with open(str(dest_gz), 'wb') as _raw_gz:
                        _hw = _HashingWriter(_raw_gz)
                        with gzip.open(_hw, 'wb') as f_out:
                            with open(source, 'rb') as f_in:
                                _sh.copyfileobj(f_in, f_out)
                    _files_written_hashes[src_path.name + '.gz'] = _hw.hexdigest()
                else:
                    # Chunked copy — computes source hash inline so we never
                    # need a separate hash_file(src) or hash_file(dest) pass.
                    _h_sf = hashlib.sha256()
                    _dest_sf = backup_dir / src_path.name
                    with open(source, 'rb') as _f_in, open(str(_dest_sf), 'wb') as _f_out:
                        while True:
                            _sf_chunk = _f_in.read(4 * 1024 * 1024)
                            if not _sf_chunk:
                                break
                            _h_sf.update(_sf_chunk)
                            _f_out.write(_sf_chunk)
                    shutil.copystat(source, str(_dest_sf))
                    _sf_hash = _h_sf.hexdigest()
                    _files_written_hashes[src_path.name] = _sf_hash
                    # Backfill snapshot so MANIFEST stores the real hash —
                    # needed both for validate_backup's per-file check and for
                    # future incremental runs that reuse snapshot entries.
                    if src_path.name in new_snapshot:
                        new_snapshot[src_path.name]["hash"] = _sf_hash
                    # Lightweight integrity check: size only (no dest re-read)
                    try:
                        if _dest_sf.stat().st_size != src_path.stat().st_size:
                            logger.warning(f"⚠ Size mismatch for {src_path.name} — file may have changed during backup")
                            result["failed_files"].append({
                                "path": src_path.name,
                                "reason": "Size mismatch — file modified during backup"
                            })
                    except OSError:
                        pass
                copied = 1
            except (PermissionError, OSError) as e:
                logger.warning(f"⚠ Could not copy {src_path.name}: {e}")
                result["failed_files"].append({"path": src_path.name, "reason": str(e)})

            if progress_cb:
                try:
                    _fsz = src_path.stat().st_size if src_path.exists() else 0
                    progress_cb(1, 1, src_path.name, _fsz, _fsz)
                except InterruptedError:
                    raise
                except Exception:
                    pass

        # ── Directory source ───────────────────────────────
        else:
            # Choose chunk size based on source/dest path type.
            # SMB/UNC network paths benefit greatly from larger chunks:
            # fewer round-trips per file = much higher effective throughput.
            # Local paths use 4 MB (good balance of memory and speed).
            _src_str    = str(source).replace("/", "\\")
            _dest_str   = str(destination).replace("/", "\\")
            _is_network = _src_str.startswith("\\\\") or _dest_str.startswith("\\\\")
            _CHUNK      = 16 * 1024 * 1024 if _is_network else 4 * 1024 * 1024  # 16 MB for network, 4 MB local
            for entry in files_to_copy:
                src_file  = src_path / entry["path"]
                dest_file = backup_dir / entry["path"]
                dest_file.parent.mkdir(parents=True, exist_ok=True)

                # ── Resume: skip files already copied in a previous interrupted run ──
                if entry["path"] in _resume_completed and dest_file.exists():
                    copied     += 1
                    bytes_done += entry.get("size", 0)
                    _files_written_hashes[entry["path"]] = hash_file(str(dest_file))
                    continue

                # ── Seed-skip optimisation ──────────────────────────────────
                # In sync mode the dest_file IS the live destination, so if it
                # already matches the source (same size + mtime within 2 s) we
                # can skip the copy entirely.  This is critical when the user
                # pre-seeded the backup drive or re-adds a watch after a
                # reinstall — without this every file would be re-written even
                # though nothing changed.  The optimisation applies to ALL
                # path-based destination types (local, UNC/SMB, mounted SFTP,
                # etc.) because the check is purely filesystem-level.
                #
                # Not applied for encrypt/compress because the destination file
                # format differs from the source (we cannot compare sizes).
                # Not applied for versioned (non-sync) mode because backup_dir
                # is a fresh timestamped folder that must contain all files.
                if sync_mode and not encrypt_key and not compress:
                    if _dest_already_identical(src_file, dest_file):
                        copied     += 1
                        bytes_done += entry.get("size", 0)
                        if progress_cb:
                            try:
                                progress_cb(copied, total or 1, entry["path"],
                                            bytes_done, total_bytes)
                            except InterruptedError:
                                raise
                            except Exception:
                                pass
                        if copied == 1:
                            logger.info(
                                f"[seed-skip] '{watch_name}': destination already has "
                                f"identical files — skipping unchanged (size+mtime match). "
                                f"Only new/modified files will be copied."
                            )
                        continue

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
                            _files_written_hashes[entry["path"]] = _encrypt_file(str(src_file), str(dest_file), encrypt_key)
                        elif compress:
                            dest_gz = Path(str(dest_file) + '.gz')
                            dest_gz.parent.mkdir(parents=True, exist_ok=True)
                            # Chunked compress so cancel is checked periodically.
                            # _HashingWriter intercepts every byte gzip writes to disk
                            # so we get the output hash without a second read of dest_gz.
                            _cmp_chunk_bytes = 0
                            with open(str(dest_gz), 'wb') as _raw_gz:
                                _hw = _HashingWriter(_raw_gz)
                                with gzip.open(_hw, 'wb') as f_out:
                                    with open(str(src_file), 'rb') as f_in:
                                        while True:
                                            chunk = f_in.read(_CHUNK)
                                            if not chunk:
                                                break
                                            f_out.write(chunk)
                                            _cmp_chunk_bytes += len(chunk)
                                            if throttler:
                                                try:
                                                    throttler.throttle(len(chunk))
                                                except Exception:
                                                    pass
                                            if progress_cb:
                                                try:
                                                    # Clamp so bar never hits 100% mid-copy
                                                    # (file may be larger than snapshot size)
                                                    _cmp_reported = min(bytes_done + _cmp_chunk_bytes,
                                                                        total_bytes - 1) if total_bytes > 0 else bytes_done + _cmp_chunk_bytes
                                                    progress_cb(copied, total or 1, entry["path"],
                                                                _cmp_reported, total_bytes)
                                                except InterruptedError:
                                                    raise
                                                except Exception:
                                                    pass
                            dest_file = dest_gz
                            _files_written_hashes[entry["path"] + ".gz"] = _hw.hexdigest()
                            # Use actual bytes read (not snapshot size) so bytes_done
                            # stays accurate even if the file grew since the snapshot.
                            _cmp_actual_size = _cmp_chunk_bytes
                        else:
                            # Cancellable chunked copy — checks cancel every chunk.
                            # Also computes source SHA-256 inline so we never need
                            # a separate hash_file(src) pass (saves one full read).
                            # Throttling is done per-chunk (inside this loop) so
                            # large files are paced evenly and progress_cb fires
                            # regularly — no multi-minute gaps from a single
                            # post-file throttle call.
                            _h_src = hashlib.sha256()
                            _chunk_bytes = 0  # bytes written so far for this file
                            with open(str(src_file), 'rb') as f_in, open(str(dest_file), 'wb') as f_out:
                                while True:
                                    chunk = f_in.read(_CHUNK)
                                    if not chunk:
                                        break
                                    _h_src.update(chunk)
                                    f_out.write(chunk)
                                    _chunk_bytes += len(chunk)
                                    # Throttle per-chunk for smooth, even pacing
                                    if throttler:
                                        try:
                                            throttler.throttle(len(chunk))
                                        except Exception:
                                            pass
                                    # Honour cancel request between chunks
                                    if progress_cb:
                                        try:
                                            # Clamp reported bytes so bar never prematurely
                                            # hits 100% mid-copy (file may be larger than
                                            # its snapshot size used in total_bytes).
                                            _reported = min(bytes_done + _chunk_bytes,
                                                            total_bytes - 1) if total_bytes > 0 else bytes_done + _chunk_bytes
                                            progress_cb(copied, total or 1, entry["path"],
                                                        _reported, total_bytes)
                                        except InterruptedError:
                                            raise
                                        except Exception:
                                            pass
                            _src_hash = _h_src.hexdigest()
                            # Preserve original timestamps (shutil.copy2 behaviour)
                            shutil.copystat(str(src_file), str(dest_file))
                            # Lightweight integrity check: verify dest file size matches
                            # source size. We do NOT re-read the dest file to recompute
                            # its hash — that would double the network traffic on SMB/UNC
                            # sources (every byte read once to write, once to verify),
                            # cutting effective throughput in half. The src hash was already
                            # computed inline chunk-by-chunk during the write loop above,
                            # and any OS-level write error would have raised an exception
                            # before we reach this point.
                            try:
                                _dest_size = Path(str(dest_file)).stat().st_size
                                _src_size  = entry.get("size", 0)
                                if _src_size > 0 and _dest_size != _src_size:
                                    logger.warning(
                                        f"⚠ Size mismatch for {entry['path']} "
                                        f"(src={_src_size}, dest={_dest_size}) — file may have changed during backup"
                                    )
                                    result["failed_files"].append({
                                        "path":   entry["path"],
                                        "reason": f"Size mismatch (src={_src_size}, dest={_dest_size})",
                                    })
                                    continue
                            except OSError:
                                pass  # dest stat failed — not critical, proceed
                            # Backfill the snapshot with the hash computed during copy.
                            # This is especially important on first backup where build_snapshot
                            # skipped hashing — without this the next backup would re-hash
                            # every file because all hashes would be empty strings.
                            _rel_key = entry["path"]
                            if _rel_key in new_snapshot:
                                new_snapshot[_rel_key]["hash"] = _src_hash
                            _files_written_hashes[entry["path"]] = _src_hash

                        copied     += 1
                        bytes_done += entry.get("size", 0)

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
                        progress_cb(copied, total or 1, entry["path"], bytes_done, total_bytes)
                    except InterruptedError:
                        raise
                    except Exception:
                        pass

            # Write .DELETED markers — only in versioned (non-sync) mode.
            # In sync_mode backup_dir IS the live destination, so writing
            # .DELETED marker files there would pollute it with junk files.
            if not sync_mode:
                for c in changes:
                    if c["type"] == "deleted":
                        marker = backup_dir / (c["path"] + ".DELETED")
                        try:
                            marker.touch()
                            # Include in hash — validate_backup sees these via rglob
                            _files_written_hashes[c["path"] + ".DELETED"] = hash_file(str(marker))
                        except Exception:
                            pass

        # ── Integrity hash ───────────────────
        # Build the directory hash from _files_written_hashes — the set of files
        # actually written to backup_dir during this run.  This is correct for
        # both full backups (all files) and incremental backups (only changed
        # files + .DELETED markers), and matches exactly what validate_backup
        # computes via rglob.  Using new_snapshot was wrong for incrementals:
        # it contains ALL source files, but backup_dir only holds changed ones.
        _bh = hashlib.sha256()
        _any_hash = False
        for _rel in sorted(_files_written_hashes.keys()):
            _fhash = _files_written_hashes.get(_rel, "")
            if _fhash:
                _bh.update(_rel.encode())
                _bh.update(_fhash.encode())
                _any_hash = True
        # Fall back to full disk scan only when nothing was tracked at all
        # (e.g. every file failed to copy — extremely rare edge case).
        backup_hash    = _bh.hexdigest() if _any_hash else hash_directory(str(backup_dir))

        # In sync mode backup_dir IS the live destination — skip writing internal
        # metadata files (BACKUP.sha256, MANIFEST.json) there so they do not
        # pollute the user's backed-up files.
        if not sync_mode:
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

        # Use the pre-computed byte total so we never do a full rglob walk of the
        # backup destination (which can be an SMB/UNC network path and takes
        # minutes to re-scan).  For compressed backups the output size differs
        # from the source size, so fall back to a local disk scan only in that
        # case — compressed backup dirs are never on a remote path at this point.
        if compress:
            total_size = _safe_size(str(backup_dir))
        else:
            total_size = total_bytes if total_bytes > 0 else bytes_done
        duration       = round(time.time() - started, 2)
        throughput_mbs = (total_size / (1024 * 1024)) / max(duration, 0.1) if duration > 0 else 0

        # Calculate compression ratio
        compression_ratio = 0.0
        if compress:
            uncompressed_est = sum(c.get("size", 0) for c in changes)
            actual_size = total_size  # already computed above for compress case
            if uncompressed_est > 0:
                compression_ratio = round((1 - actual_size / uncompressed_est) * 100, 1)

        # ── Remote destination upload ──────────────────────────────────────────
        # Handles multiple destinations: cloud (GDrive), sftp, ftp, smb, https, webdav.
        def _upload_to_destination(backup_dir_path: str, dest_config: dict, progress_cb) -> dict:
            """Upload backup to a single destination."""
            dest_type = dest_config.get("_dest_type", "")
            if dest_type == "cloud":
                provider = dest_config.get("provider", "gdrive")
                if provider == "gdrive" and dest_config.get("access_token"):
                    return upload_to_gdrive(backup_dir_path, dest_config)
                else:
                    return {"ok": False, "error": f"Not connected — use the Connect button for {provider}"}
            elif dest_type == "sftp" and TRANSPORT_AVAILABLE:
                sftp_cfg = dest_config.get("sftp_config") or dest_config
                return upload_to_sftp(backup_dir_path, sftp_cfg, progress_cb=progress_cb)
            elif dest_type == "ftp" and TRANSPORT_AVAILABLE:
                ftp_cfg = dest_config.get("ftp_config") or dest_config
                return upload_to_ftp(backup_dir_path, ftp_cfg, progress_cb=progress_cb)
            elif dest_type == "ftps" and TRANSPORT_AVAILABLE:
                ftp_cfg = dict(dest_config.get("ftp_config") or dest_config)
                ftp_cfg["use_tls"] = True
                return upload_to_ftp(backup_dir_path, ftp_cfg, progress_cb=progress_cb)
            elif dest_type == "smb" and TRANSPORT_AVAILABLE:
                smb_cfg = dest_config.get("smb_config") or dest_config
                return upload_to_smb(backup_dir_path, smb_cfg, progress_cb=progress_cb)
            elif dest_type == "https" and TRANSPORT_AVAILABLE:
                https_cfg = dest_config.get("https_config") or dest_config
                return upload_to_https(backup_dir_path, https_cfg, progress_cb=progress_cb)
            elif dest_type == "webdav" and TRANSPORT_AVAILABLE:
                from transport_utils import upload_to_webdav
                webdav_cfg = dest_config.get("webdav_config") or dest_config
                return upload_to_webdav(backup_dir_path, webdav_cfg, progress_cb=progress_cb)
            else:
                return {"ok": False, "error": f"Unsupported destination type: {dest_type}"}

        cloud_upload_results = []
        if destinations:
            # Multi-destination mode
            for dest in destinations:
                dest_type = dest.get("dest_type")
                dest_config = dest.get("config", {})
                dest_config["_dest_type"] = dest_type
                logger.info(f"📡 Uploading to {dest_type}: {watch_name}")
                upload_result = _upload_to_destination(str(backup_dir), dest_config, _upload_progress)
                cloud_upload_results.append({"dest_type": dest_type, **upload_result})
        else:
            # Legacy single destination mode
            cloud_upload_result = None
            _dest_type = (cloud_config or {}).get("_dest_type", "") or storage_type

            if _dest_type == "cloud" and cloud_config:
                provider = cloud_config.get("provider", "gdrive")
                logger.info(f"☁ Uploading backup to cloud ({provider}): {watch_name}")
                if provider == "gdrive" and cloud_config.get("access_token"):
                    cloud_upload_result = upload_to_gdrive(str(backup_dir), cloud_config)
                else:
                    cloud_upload_result = {"ok": False, "error": f"Not connected — use the Connect button for {provider}"}

            elif _dest_type == "sftp" and TRANSPORT_AVAILABLE:
                sftp_cfg = (cloud_config or {}).get("sftp_config") or cloud_config or {}
                logger.info(f"📡 Uploading backup via SFTP: {watch_name}")
                cloud_upload_result = upload_to_sftp(str(backup_dir), sftp_cfg,
                                                     progress_cb=_upload_progress)

            elif _dest_type == "ftp" and TRANSPORT_AVAILABLE:
                ftp_cfg = (cloud_config or {}).get("ftp_config") or cloud_config or {}
                logger.info(f"📡 Uploading backup via FTP: {watch_name}")
                cloud_upload_result = upload_to_ftp(str(backup_dir), ftp_cfg,
                                                    progress_cb=_upload_progress)

            elif _dest_type == "ftps" and TRANSPORT_AVAILABLE:
                ftp_cfg = dict((cloud_config or {}).get("ftp_config") or cloud_config or {})
                ftp_cfg["use_tls"] = True  # enforce TLS for FTPS
                logger.info(f"📡 Uploading backup via FTPS: {watch_name}")
                cloud_upload_result = upload_to_ftp(str(backup_dir), ftp_cfg,
                                                    progress_cb=_upload_progress)

            elif _dest_type == "smb" and TRANSPORT_AVAILABLE:
                smb_cfg = (cloud_config or {}).get("smb_config") or cloud_config or {}
                logger.info(f"📡 Uploading backup via SMB: {watch_name}")
                cloud_upload_result = upload_to_smb(str(backup_dir), smb_cfg,
                                                    progress_cb=_upload_progress)

            elif _dest_type == "https" and TRANSPORT_AVAILABLE:
                https_cfg = (cloud_config or {}).get("https_config") or cloud_config or {}
                logger.info(f"📡 Uploading backup via HTTPS: {watch_name}")
                cloud_upload_result = upload_to_https(str(backup_dir), https_cfg,
                                                      progress_cb=_upload_progress)

            elif _dest_type == "webdav" and TRANSPORT_AVAILABLE:
                from transport_utils import upload_to_webdav
                webdav_cfg = (cloud_config or {}).get("webdav_config") or cloud_config or {}
                logger.info(f"📡 Uploading backup via WebDAV: {watch_name}")
                cloud_upload_result = upload_to_webdav(str(backup_dir), webdav_cfg,
                                                       progress_cb=_upload_progress)

            if cloud_upload_result:
                cloud_upload_results.append({"dest_type": _dest_type, **cloud_upload_result})

        # Report failures
        for res in cloud_upload_results:
            if not res.get("ok", False):
                logger.warning(f"⚠ Remote upload failed ({res.get('dest_type', '?')}): {res.get('error', 'Unknown error')}")
            else:
                logger.info(f"☁ Remote upload complete ({res.get('dest_type', '?')}): {res.get('uploaded', '?')} files uploaded")

        result["destinations_upload"] = cloud_upload_results

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
            "encrypted":          bool(encrypt_key),
            "compression_ratio":  compression_ratio,
            "files_copied":       copied,
            "changes":            changes,
            "snapshot":           new_snapshot,
            "duration_s":         duration,
            "throughput_mbs":     round(throughput_mbs, 2),
            "total_size_bytes":   total_size,
            "failed_files":       result.get("failed_files", []),
            "destinations_upload": cloud_upload_results,  # NEW: persisted in manifest
            "triggered_by":       triggered_by or "auto",
        }

        # In sync mode, skip writing MANIFEST.json into the destination folder
        # to avoid polluting the user's backed-up files.
        if not sync_mode:
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
        # Save resume checkpoint so the next run can continue where we left off.
        if backup_dir is not None and not sync_mode and Path(backup_dir).exists():
            try:
                _resume_path.write_text(
                    json.dumps({
                        "backup_dir": str(backup_dir),
                        "completed":  list(_resume_completed),
                    }),
                    encoding="utf-8",
                )
                logger.info(f"[resume] Checkpoint saved — next run will resume from {backup_dir.name}")
            except Exception as _ce:
                logger.debug(f"[resume] Could not save checkpoint: {_ce}")
        logger.warning(f"⚠ Backup cancelled: {watch_name}")

    except Exception as e:
        import traceback
        logger.error(f"❌ Backup failed for {watch_name}: {e}\n{traceback.format_exc()}")
        result["error"]      = str(e)
        result["status"]     = "failed"
        result["duration_s"] = round(time.time() - started, 2)
        # Save resume checkpoint for recoverable failures (disk full, network drop).
        if backup_dir is not None and not sync_mode and Path(backup_dir).exists():
            try:
                _resume_path.write_text(
                    json.dumps({
                        "backup_dir": str(backup_dir),
                        "completed":  list(_resume_completed),
                    }),
                    encoding="utf-8",
                )
                logger.info(f"[resume] Checkpoint saved after failure — next run will resume")
            except Exception:
                pass
        elif backup_dir is not None and Path(backup_dir).exists():
            shutil.rmtree(str(backup_dir), ignore_errors=True)
            logger.info(f"🗑 Removed partial backup dir after failure: {backup_dir}")

    finally:
        # Always clean up the VSS shadow copy, even on success or exception.
        if _vss_shadow_id:
            _vss_delete_snapshot(_vss_shadow_id)
        # On success: remove any stale resume checkpoint for this watch.
        if result.get("status") == "success":
            try:
                if _resume_path.exists():
                    _resume_path.unlink()
            except Exception:
                pass
        _backup_index.invalidate(destination)

    # ── Post-backup hook ───────────────────────────────────────────────────────
    # Runs regardless of success/failure so callers can always do cleanup.
    # A failing post-hook is logged but does NOT change result["status"].
    if post_backup_cmd and post_backup_cmd.strip():
        logger.info(f"[hook] Running post-backup command: {post_backup_cmd}")
        try:
            import subprocess as _sp
            _post = _sp.run(
                post_backup_cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=300,
                env={**os.environ,
                     "BACKUPSYS_STATUS":   result.get("status", ""),
                     "BACKUPSYS_WATCH":    watch_name,
                     "BACKUPSYS_WATCH_ID": watch_id},
            )
            if _post.stdout.strip():
                logger.info(f"[hook] post-backup stdout: {_post.stdout.strip()}")
            if _post.stderr.strip():
                logger.warning(f"[hook] post-backup stderr: {_post.stderr.strip()}")
            if _post.returncode != 0:
                logger.warning(
                    f"[hook] Post-backup command exited {_post.returncode} — "
                    f"backup result is unchanged."
                )
            else:
                logger.info(f"[hook] Post-backup command succeeded (exit 0)")
        except Exception as _ph:
            logger.warning(f"[hook] Post-backup hook error (non-fatal): {_ph}")

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

            # Apply deletion markers written by run_backup().
            # restore_backup() already handles etype=="deleted" entries in
            # the manifest changes list, but only removes files that *exist*
            # at target_path.  Walking the .DELETED markers on disk as well
            # ensures files restored by an earlier chain step (which weren't
            # present at the time of this step's backup) are correctly removed.
            _bd_path = Path(backup_dir)
            for marker in _bd_path.rglob("*.DELETED"):
                rel = str(marker.relative_to(_bd_path))
                original_rel = rel[: -len(".DELETED")]  # strip marker suffix
                target_file  = Path(target_path) / original_rel
                if target_file.exists():
                    try:
                        target_file.unlink()
                    except Exception as _de:
                        result["errors"].append(f"delete {original_rel}: {_de}")

        result["ok"] = result["steps_applied"] > 0
        if not result["ok"] and not result["error"]:
            result["error"] = "No backup steps could be applied"

    except Exception as e:
        import traceback
        result["error"] = str(e)
        logger.error(f"[restore_chain] Failed: {e}\n{traceback.format_exc()}")

    return result


# ─── Backup size estimator ─────────────────────────────────────────────────────

def estimate_backup_size(
    source: str,
    exclude_patterns: Optional[List[str]] = None,
    previous_snapshot: Optional[Dict] = None,
    max_file_size_mb: float = 0,
    cancel_event=None,
) -> dict:
    """
    Walk *source* and estimate how many bytes a backup would transfer.

    If *previous_snapshot* is supplied (incremental mode) only changed/new files
    are counted.  Returns::

        {
          "total_files":    int,   # files that would be backed up
          "skipped_files":  int,   # files skipped by size or exclude rules
          "total_bytes":    int,   # raw bytes of files to copy
          "total_human":    str,   # human-readable (e.g. "1.4 GB")
          "incremental":    bool,
          "new_files":      int,
          "changed_files":  int,
          "deleted_files":  int,   # only meaningful in incremental mode
          "error":          str | None,
        }
    """
    result: dict = {
        "total_files":   0,
        "skipped_files": 0,
        "total_bytes":   0,
        "total_human":   "0 B",
        "incremental":   previous_snapshot is not None,
        "new_files":     0,
        "changed_files": 0,
        "deleted_files": 0,
        "error":         None,
    }

    try:
        src = Path(_fix_path(source))
        if not src.exists():
            result["error"] = f"Source not found: {source}"
            return result

        excl = list(exclude_patterns or [])
        max_bytes = int(max_file_size_mb * 1024 * 1024) if max_file_size_mb > 0 else 0

        # Build current file → mtime/size map without full hashing (fast)
        current: dict[str, dict] = {}
        if src.is_file():
            st = src.stat()
            current[src.name] = {"mtime": st.st_mtime, "size": st.st_size}
        else:
            for fp in src.rglob("*"):
                if cancel_event and cancel_event.is_set():
                    result["error"] = "Cancelled"
                    return result
                if not fp.is_file():
                    continue
                rel = str(fp.relative_to(src))
                if _is_excluded(rel, fp, excl):
                    result["skipped_files"] += 1
                    continue
                try:
                    st = fp.stat()
                except OSError:
                    result["skipped_files"] += 1
                    continue
                if max_bytes and st.st_size > max_bytes:
                    result["skipped_files"] += 1
                    continue
                current[rel] = {"mtime": st.st_mtime, "size": st.st_size}

        if previous_snapshot:
            prev = previous_snapshot
            for rel, info in current.items():
                if rel not in prev:
                    result["new_files"]    += 1
                    result["total_files"]  += 1
                    result["total_bytes"]  += info["size"]
                elif (info["mtime"] != prev[rel].get("mtime")
                      or info["size"] != prev[rel].get("size")):
                    result["changed_files"] += 1
                    result["total_files"]   += 1
                    result["total_bytes"]   += info["size"]
            result["deleted_files"] = sum(
                1 for r in prev if r not in current
            )
        else:
            for rel, info in current.items():
                result["total_files"] += 1
                result["total_bytes"] += info["size"]

        result["total_human"] = _human_size(result["total_bytes"])

    except Exception as exc:
        result["error"] = str(exc)

    return result


# ─── Cleanup ──────────────────────────────────────────────────────────────────

def preview_cleanup(destination: str, retention_days: int, watch_id: Optional[str] = None) -> dict:
    """Preview which backup folders would be deleted by cleanup_old_backups — without deleting anything.
    Returns { to_delete: [folder_names], freed_bytes, freed_human }
    """
    dest   = Path(destination)
    result = {"to_delete": [], "freed_bytes": 0, "freed_human": "0 B"}

    if not dest.exists() or retention_days <= 0:
        return result

    cutoff = datetime.now() - timedelta(days=retention_days)

    for d in list(dest.iterdir()):
        if not d.is_dir():
            continue
        manifest_p = d / "MANIFEST.json"
        if not manifest_p.exists():
            try:
                age_hours = (datetime.now().timestamp() - d.stat().st_mtime) / 3600
                if age_hours > 24:
                    result["to_delete"].append(d.name)
                    result["freed_bytes"] += _safe_size(str(d))
            except Exception:
                pass
            continue
        try:
            with open(manifest_p) as f:
                m = json.load(f)
            if watch_id and m.get("watch_id") != watch_id:
                continue
            ts_str = m.get("timestamp", "")
            if not ts_str:
                continue
            if datetime.fromisoformat(ts_str) < cutoff:
                result["to_delete"].append(d.name)
                result["freed_bytes"] += _safe_size(str(d))
        except Exception:
            pass

    result["freed_human"] = _human_size(result["freed_bytes"])
    return result


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

            snap      = manifest.get("snapshot", {})
            is_enc    = manifest.get("encrypted", False)
            for entry in manifest.get("changes", []):
                if entry["type"] in ("added", "modified"):
                    rel       = entry["path"]
                    fp        = bd / rel
                    fp_gz     = bd / (rel + '.gz')
                    actual_fp = fp if fp.exists() else (fp_gz if fp_gz.exists() else None)

                    if not actual_fp:
                        result["missing_files"].append(rel)
                    elif snap.get(rel) and not fp_gz.exists() and not is_enc:
                        # Skip per-file hash check for:
                        #   compressed — gz bytes differ from plaintext hash
                        #   encrypted  — ciphertext bytes differ from plaintext hash
                        # Both are covered by the overall BACKUP.sha256 check below.
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


def restore_single_file(
    backup_dir: str,
    relative_path: str,
    target_path: str,
    encrypt_key: Optional[str] = None,
    overwrite: bool = True,
) -> dict:
    """
    Restore a single file from a backup directory.

    Args:
        backup_dir:     Path to the versioned backup directory.
        relative_path:  Relative path of the file inside the backup
                        (as returned by browse_backup_contents).
        target_path:    Full destination path where the file should be written.
                        If it is a directory, the original filename is appended.
        encrypt_key:    Fernet/AES key if the backup is encrypted.
        overwrite:      If False and target already exists, skip and report.

    Returns a dict::

        { "ok": bool, "restored_to": str, "size_bytes": int, "error": str|None }
    """
    import gzip as _gz

    result: dict = {"ok": False, "restored_to": "", "size_bytes": 0, "error": None}

    try:
        bd  = Path(_fix_path(backup_dir))
        rel = relative_path.lstrip("/\\")

        # Strip any .enc / .gz suffixes so we can find the actual disk file
        # even when the caller passes the logical (decrypted) name.
        candidates = [
            bd / rel,
            bd / (rel + ".enc"),
            bd / (rel + ".gz"),
            bd / (rel + ".enc.gz"),
            bd / (rel + ".gz.enc"),
        ]
        src_file: Optional[Path] = None
        for c in candidates:
            if c.exists() and c.is_file():
                src_file = c
                break

        if src_file is None:
            result["error"] = f"File not found in backup: {rel}"
            return result

        dest = Path(_fix_path(target_path))
        if dest.is_dir():
            # Keep original filename, not the .enc/.gz decorated name
            base_name = Path(rel).name
            for suffix in (".enc", ".gz"):
                if base_name.endswith(suffix):
                    base_name = base_name[: -len(suffix)]
            dest = dest / base_name

        if dest.exists() and not overwrite:
            result["error"] = f"Target already exists and overwrite=False: {dest}"
            return result

        dest.parent.mkdir(parents=True, exist_ok=True)

        # Decrypt and/or decompress into a temp file, then move atomically
        import tempfile
        tmp_fd, tmp_path = tempfile.mkstemp(dir=dest.parent)
        os.close(tmp_fd)
        tmp = Path(tmp_path)

        try:
            name = src_file.name
            if encrypt_key and (".enc" in name):
                # Decrypt to tmp (handles both AES-GCM and legacy Fernet)
                _decrypt_file(str(src_file), str(tmp), encrypt_key)
                # If also compressed (.gz.enc or .enc.gz) — decompress tmp in place
                if ".gz" in name:
                    gz_tmp = Path(str(tmp) + ".gz2")
                    tmp.rename(gz_tmp)
                    with _gz.open(str(gz_tmp), "rb") as fin, open(str(tmp), "wb") as fout:
                        shutil.copyfileobj(fin, fout)
                    gz_tmp.unlink(missing_ok=True)
            elif ".gz" in name:
                with _gz.open(str(src_file), "rb") as fin, open(str(tmp), "wb") as fout:
                    shutil.copyfileobj(fin, fout)
            else:
                shutil.copy2(str(src_file), str(tmp))

            size = tmp.stat().st_size
            os.replace(str(tmp), str(dest))
        except Exception:
            tmp.unlink(missing_ok=True)
            raise

        result.update({"ok": True, "restored_to": str(dest), "size_bytes": size})
        logger.info(f"[restore] Single file restored: {rel} → {dest} ({_human_size(size)})")

    except Exception as exc:
        import traceback
        logger.error(f"[restore] Single file restore failed: {exc}\n{traceback.format_exc()}")
        result["error"] = str(exc)

    return result


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