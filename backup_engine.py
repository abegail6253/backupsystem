def _is_gdrive_ready(config):
    """Return True if config is for Google Drive and has an access token."""
    return config and config.get("provider") == "gdrive" and bool(config.get("access_token"))
import os
import io
import uuid
import shutil
import hashlib
import json
import time
import threading
import difflib
import fnmatch
import subprocess
import sys
import logging
logger = logging.getLogger(__name__)

# Suppress console/PowerShell pop-up windows on Windows
_WIN_NO_WINDOW = subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple, Union

# Optional encryption support
try:
    from cryptography.fernet import Fernet
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

# Check whether the hazmat sub-package (AES-GCM) is functional.
# Some installations have a broken kdf/ciphers sub-package despite Fernet working.
try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM as _AESGCM_TEST
    _AESGCM_TEST(b"\x00" * 32)  # basic sanity check
    HAZMAT_AVAILABLE = True
    del _AESGCM_TEST
except Exception:
    HAZMAT_AVAILABLE = False

# Optional transport/notification helpers
try:
    from transport_utils import (
        upload_to_sftp, upload_to_ftp, upload_to_https, upload_to_rclone,
        download_from_sftp, download_from_ftp,
        check_remote_free_space,
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
# FERNET_MAX_BYTES: 200 MB was the old Fernet limit. AES-GCM streaming has no file size limit.


def _ensure_writable(path) -> None:
    """Remove read-only flag from *path* if it exists.

    On Windows, network sources often have the read-only attribute set, and
    shutil.copystat() faithfully mirrors it onto the destination.  A
    subsequent backup run then fails with [Errno 13] Permission denied
    when trying to open that file for writing.  Clearing the flag before
    opening for write prevents this without altering the source.
    """
    try:
        p = os.fspath(path)
        if os.path.exists(p):
            current = os.stat(p).st_mode
            import stat as _stat
            if not (current & _stat.S_IWRITE):
                os.chmod(p, current | _stat.S_IWRITE)
    except (OSError, AttributeError):
        pass  # Non-fatal — let the open() below surface the real error


def _unc_host(path: str) -> str:
    """Return the lowercase hostname from a UNC path, or '' if not UNC.

    Examples:
        '\\\\192.168.254.108\\share\\dir' → '192.168.254.108'
        'C:\\foo'                         → ''
    """
    p = path.replace("/", "\\")
    if not p.startswith("\\\\"):
        return ""
    parts = p.lstrip("\\").split("\\")
    return parts[0].lower() if parts else ""


def _server_side_copy(src: str, dst: str, progress_cb=None) -> bool:
    """Attempt a server-side (offloaded) copy using the Windows CopyFileEx API.

    When source and destination are on the same Windows host the OS can issue
    an FSCTL_SRV_COPYCHUNK request so the PC copies the data server-side —
    the bytes never travel over the network to the PC.  This can be 10-100×
    faster than the Python read→write loop for large files on a LAN.

    progress_cb, if given, is called as progress_cb(bytes_transferred, total_bytes)
    from the CopyFileEx native callback so the UI gets real byte-level progress
    even during server-side offload (no Python chunk loop → no per-chunk cb).

    Returns True if the offloaded copy succeeded, False if it is unavailable
    or failed (caller should fall back to the normal chunked copy).

    Tries ctypes first (no external deps), then pywin32 as fallback.
    Safe to call when neither is available — returns False immediately.
    """
    if os.name != "nt":
        return False

    # ── Attempt 1: ctypes (built into Python — no pywin32 required) ──────────
    try:
        import ctypes
        import ctypes.wintypes

        COPY_FILE_NO_BUFFERING = 0x00001000
        PROGRESS_CONTINUE = 0

        # Define the native callback type for CopyFileExW
        _PROGRESS_ROUTINE = ctypes.WINFUNCTYPE(
            ctypes.wintypes.DWORD,   # return DWORD (PROGRESS_CONTINUE=0)
            ctypes.c_int64,          # TotalFileSize      (LARGE_INTEGER)
            ctypes.c_int64,          # TotalBytesTransferred
            ctypes.c_int64,          # StreamSize
            ctypes.c_int64,          # StreamBytesTransferred
            ctypes.wintypes.DWORD,   # dwStreamNumber
            ctypes.wintypes.DWORD,   # dwCallbackReason
            ctypes.wintypes.HANDLE,  # hSourceFile
            ctypes.wintypes.HANDLE,  # hDestinationFile
            ctypes.c_void_p,         # lpData
        )

        kernel32 = ctypes.windll.kernel32
        kernel32.CopyFileExW.restype  = ctypes.wintypes.BOOL
        kernel32.CopyFileExW.argtypes = [
            ctypes.c_wchar_p,
            ctypes.c_wchar_p,
            ctypes.c_void_p,                       # progress routine (nullable)
            ctypes.c_void_p,                       # lpData
            ctypes.POINTER(ctypes.wintypes.BOOL),  # pbCancel
            ctypes.wintypes.DWORD,                 # dwCopyFlags
        ]

        _cb_func = None
        if progress_cb is not None:
            def _raw_cb(total_size, total_transferred, stream_size,
                        stream_transferred, stream_num, reason,
                        h_src, h_dst, data):
                try:
                    progress_cb(int(total_transferred), int(total_size))
                except Exception:
                    pass
                return PROGRESS_CONTINUE

            _cb_func = _PROGRESS_ROUTINE(_raw_cb)

        pb_cancel = ctypes.wintypes.BOOL(False)
        ret = kernel32.CopyFileExW(
            src, dst,
            _cb_func,
            None,
            ctypes.byref(pb_cancel),
            COPY_FILE_NO_BUFFERING,
        )
        if ret:
            return True
        # Non-zero means success; zero means failure — fall through to pywin32
        err = ctypes.get_last_error()
        logger.debug(f"[server-copy] CopyFileExW via ctypes failed (err={err}) — trying pywin32")
    except Exception as _ct_err:
        logger.debug(f"[server-copy] ctypes CopyFileExW unavailable ({_ct_err}) — trying pywin32")

    # ── Attempt 2: pywin32 (optional install) ─────────────────────────────────
    try:
        import win32file
        COPY_FILE_NO_BUFFERING = 0x00001000

        _routine = None
        if progress_cb is not None:
            def _routine(total_size, total_transferred, stream_size,
                         stream_transferred, stream_num, reason,
                         h_src, h_dst, data):
                try:
                    progress_cb(int(total_transferred), int(total_size))
                except Exception:
                    pass
                return 0  # PROGRESS_CONTINUE

        win32file.CopyFileEx(src, dst, _routine, None, False, COPY_FILE_NO_BUFFERING)
        return True
    except ImportError:
        logger.debug("[server-copy] pywin32 not installed — server-side copy unavailable")
        return False
    except Exception as _w32_err:
        logger.debug(f"[server-copy] win32file.CopyFileEx failed ({_w32_err})")
        return False


def _parallel_copy_files(
    entries: list,
    src_root,
    dst_root,
    cancel_event=None,
    progress_cb=None,
    total_files: int = 0,
    total_bytes: int = 0,
    chunk_size: int = 16 * 1024 * 1024,
    max_workers: int = 4,
) -> tuple:
    """Copy a list of file entries in parallel using a thread pool.

    Used as the fast fallback when robocopy is unavailable for UNC→UNC
    copies where the Python serial loop is the bottleneck.  4 concurrent
    streams typically 3-4× the throughput of a single-threaded loop on a
    Gigabit LAN because read latency is hidden by overlapping I/O.

    Skips files that already exist in the destination with a matching size
    (seed-skip) — avoids re-copying when the user pre-populated the dest
    or a previous backup already wrote the file.

    Returns (files_copied, bytes_skipped_seed, bytes_copied, failed_entries).
    Thread-safe progress reporting via a lock-protected counter.
    """
    import concurrent.futures as _cf
    import threading as _th

    _lock        = _th.Lock()
    _copied      = [0]
    _bytes_done  = [0]
    _skipped     = [0]
    _failed      = []

    def _copy_one(entry):
        if cancel_event is not None and cancel_event.is_set():
            return
        src_file  = src_root / entry["path"]
        dest_file = dst_root / entry["path"]
        src_size  = entry.get("size", 0)
        try:
            # ── Seed-skip: if dest already has same-size file, skip copy ──────
            if dest_file.exists():
                try:
                    _dst_st = dest_file.stat()
                    if _dst_st.st_size == src_size and src_size > 0:
                        logger.debug(
                            f"[parallel-copy] seed-skip '{entry['path']}' "
                            f"— dest already has matching size ({src_size:,} bytes)"
                        )
                        with _lock:
                            _skipped[0] += 1
                            _bytes_done[0] += src_size
                            if progress_cb:
                                try:
                                    progress_cb(
                                        _copied[0] + _skipped[0], total_files or 1,
                                        "\x00verify\x00" + entry["path"],
                                        _bytes_done[0], total_bytes,
                                    )
                                except Exception:
                                    pass
                        return
                except OSError:
                    pass  # can't stat dest → fall through to copy

            dest_file.parent.mkdir(parents=True, exist_ok=True)
            with open(str(src_file), "rb", buffering=chunk_size) as f_in, \
                 open(str(dest_file), "wb", buffering=chunk_size) as f_out:
                while True:
                    if cancel_event is not None and cancel_event.is_set():
                        return
                    chunk = f_in.read(chunk_size)
                    if not chunk:
                        break
                    f_out.write(chunk)
                    with _lock:
                        _bytes_done[0] += len(chunk)
                        if progress_cb:
                            try:
                                progress_cb(
                                    _copied[0] + _skipped[0], total_files or 1,
                                    entry["path"],
                                    _bytes_done[0], total_bytes,
                                )
                            except Exception:
                                pass
            with _lock:
                _copied[0] += 1
                if progress_cb:
                    try:
                        progress_cb(
                            _copied[0] + _skipped[0], total_files or 1,
                            entry["path"],
                            _bytes_done[0], total_bytes,
                        )
                    except Exception:
                        pass
        except Exception as _e:
            logger.warning(f"[parallel-copy] failed for {entry['path']}: {_e}")
            with _lock:
                _failed.append(entry)

    with _cf.ThreadPoolExecutor(max_workers=max_workers) as pool:
        list(pool.map(_copy_one, entries))

    if _skipped[0]:
        logger.info(
            f"[parallel-copy] {_skipped[0]} file(s) already in destination — skipped "
            f"({_skipped[0]} of {total_files} total)"
        )

    return _copied[0], _bytes_done[0], _failed


def _robocopy_file(
    src: str,
    dst: str,
    cancel_event=None,
    progress_cb=None,
    copied_files: int = 0,
    total_files: int = 0,
    bytes_done_offset: int = 0,
    total_bytes: int = 0,
) -> bool:
    """Copy a single file using robocopy (built into every modern Windows).

    robocopy uses native Windows I/O, unbuffered large-file mode (/J), and
    can trigger server-side copy on Windows PCs that support it — often
    significantly faster than Python's read/write loop for large files.

    Returns True on success, False if robocopy is unavailable or failed.
    Progress is polled by watching the destination file size grow.
    """
    if os.name != "nt":
        return False
    try:
        src_p = Path(src)
        dst_p = Path(dst)

        proc = subprocess.Popen(
            [
                "robocopy",
                str(src_p.parent),
                str(dst_p.parent),
                src_p.name,
                "/J",       # unbuffered I/O — faster for large files
                "/COPY:DAT",# copy Data, Attributes, Timestamps
                "/R:0",     # no retries on failure
                "/W:0",     # no wait between retries
                "/NP",      # no progress % in stdout
                "/NFL",     # no file list
                "/NDL",     # no dir list
                "/NJH",     # no job header
                "/NJS",     # no job summary
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=_WIN_NO_WINDOW,
        )

        # Poll dest size for progress while robocopy runs
        while proc.poll() is None:
            if cancel_event is not None and cancel_event.is_set():
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except Exception:
                    pass
                return False
            if progress_cb is not None:
                try:
                    written = dst_p.stat().st_size if dst_p.exists() else 0
                    progress_cb(
                        copied_files, total_files or 1, src_p.name,
                        bytes_done_offset + written, total_bytes,
                    )
                except Exception:
                    pass
            time.sleep(0.25)

        rc = proc.returncode if proc.returncode is not None else proc.wait()
        # robocopy exit codes: 0=nothing to copy (match), 1=copied OK,
        # 2=extra files in dest (not an error for us), 3=0+2 combo.
        # Codes >= 8 mean at least one file failed.
        return rc is not None and rc < 8
    except FileNotFoundError:
        # robocopy not found (very old Windows or stripped image)
        return False
    except Exception as _rb_err:
        logger.debug(f"[robocopy] failed ({_rb_err})")
        return False


def _robocopy_dir(
    src: str,
    dst: str,
    cancel_event=None,
    progress_cb=None,
    total_files: int = 0,
    total_bytes: int = 0,
    files_to_copy: list = None,
    size_hints: list = None,
) -> tuple:
    """Copy an entire directory tree using robocopy.

    When both *src* and *dst* are UNC paths on the same Windows host robocopy
    can negotiate server-side copy so the data stays on the same machine —
    order-of-magnitude faster than Python's read→write loop for UNC→UNC.

    If *files_to_copy* is provided (incremental mode), only those specific
    files are copied — robocopy is called once per unique subdirectory that
    contains changed files, passing explicit filenames so unchanged files
    are left untouched.

    If *files_to_copy* is None or empty (full backup), robocopy copies the
    entire tree in one shot with /E /MT:8.

    Progress is driven by parsing robocopy's stdout line-by-line so the UI
    updates in real time as each file is copied — no polling lag.

    Returns (success: bool, files_copied: int, bytes_copied: int).
    files_copied/bytes_copied reflect what robocopy actually processed,
    which may exceed the pre-computed total when the scan was incomplete.
    """
    if os.name != "nt":
        return False, 0, 0

    dst_path = Path(dst)

    # Build a size lookup from files_to_copy for byte-accurate progress.
    # Keys are normalised relative paths (backslash, no leading slash).
    # For full-backup mode files_to_copy is None but size_hints carries the
    # same data — populate the map from whichever is available so the UI
    # shows real byte progress instead of staying on "Preparing...".
    _size_map: dict = {}
    _basename_map: dict = {}   # filename-only lookup for full-backup robocopy output
    _hint_source = files_to_copy or size_hints or []
    for e in _hint_source:
        _k = e["path"].replace("/", "\\").lstrip("\\")
        _sz = e.get("size", 0)
        _size_map[_k] = _sz
        # Robocopy's /E full-tree run prints only the bare filename, not the
        # relative path.  Index by basename so _read_stdout can still look up
        # the size even for files nested inside subdirectories.
        _bn = _k.split("\\")[-1]
        _basename_map[_bn] = _basename_map.get(_bn, 0) + _sz

    # Shared counters — accumulated across all _run_robocopy calls so the
    # caller gets the true count even when total_files/total_bytes were 0
    # (e.g. scan timed out or returned an incomplete snapshot).
    _rc_files = [0]
    _rc_bytes = [0]

    def _run_robocopy(cmd: list, dir_prefix: str = "") -> bool:
        """Run a robocopy command, parse stdout for real-time progress.

        dir_prefix: relative subdir prefix for incremental multi-dir runs,
                    used to look up sizes in _size_map.
        """
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                creationflags=_WIN_NO_WINDOW,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,          # line-buffered
            )
        except FileNotFoundError:
            return False

        def _read_stdout():
            """Read robocopy stdout lines and update progress counters.

            Robocopy emits two kinds of output lines (when /NP is absent):

            1. Intra-file percentage lines:
                   "        0%"   "       16%"  ...  "      100%"
               These arrive BEFORE the filename line and give real-time
               byte-level progress inside a large file.

            2. Completion lines (one per copied file):
                   "\t    10485760\ttestfile"
               The middle tab-separated field is the file size in bytes
               as reported by robocopy itself — more reliable than our
               pre-scan _size_map lookup.

            Strategy: on a percentage line credit fractional bytes so the
            bar moves smoothly; on a completion line parse robocopy's own
            size and add the remaining bytes not yet credited.
            """
            import re as _re
            _pct_re = _re.compile(r'^(\d+(?:\.\d+)?)%$')

            # Per-file running state
            _cur_size      = [0]   # size of file currently being transferred
            _cur_credited  = [0]   # bytes already emitted via % lines this file
            _cur_fname     = [""]  # best-known filename (for progress label)
            _in_pct_block  = [False]

            # Size queue: ordered list of expected file sizes so percentage
            # lines can be attributed before the filename is printed.
            # For incremental runs we know the exact order; for full-backup
            # it is approximate but still far better than zero.
            _hint_source = files_to_copy or size_hints or []
            if dir_prefix and dir_prefix != ".":
                _q = [
                    e.get("size", 0)
                    for e in _hint_source
                    if str(Path(e["path"].replace("/", "\\")).parent) == dir_prefix
                ]
            else:
                _q = [e.get("size", 0) for e in _hint_source]
            _size_queue = list(_q)

            def _start_next_file():
                _cur_size[0]    = _size_queue.pop(0) if _size_queue else 0
                _cur_credited[0] = 0

            for raw_line in proc.stdout:
                line = raw_line.strip()
                if not line:
                    continue
                _skip_prefixes = (
                    "----------", "ROBOCOPY", "Started", "Source",
                    "Dest", "Files", "Dirs", "Options", "Waiting",
                    "ERROR", "WARNING", "New Dir", "*EXTRA",
                    "Newer", "Older", "same", "Tweaked",
                    "Existing Dir", "Lonely", "Changed", "Junction",
                )
                if any(line.startswith(p) for p in _skip_prefixes):
                    continue

                m = _pct_re.match(line)
                if m:
                    # ── Robocopy intra-file percentage line ─────────────
                    pct = float(m.group(1)) / 100.0
                    if not _in_pct_block[0]:
                        _in_pct_block[0] = True
                        _start_next_file()
                    if _cur_size[0] > 0:
                        target = int(_cur_size[0] * pct)
                        delta  = target - _cur_credited[0]
                        if delta > 0:
                            _rc_bytes[0]     += delta
                            _cur_credited[0] += delta
                            if progress_cb is not None:
                                try:
                                    progress_cb(
                                        _rc_files[0],
                                        total_files or 1,
                                        _cur_fname[0],
                                        _rc_bytes[0],
                                        total_bytes,
                                    )
                                except Exception:
                                    pass
                    continue

                # ── Completion / filename line ───────────────────────────
                _in_pct_block[0] = False

                parts = line.split("\t")
                fname = parts[-1].strip() if parts else line
                if not fname:
                    continue

                # Skip directory summary lines — robocopy prints these to
                # report how many files it processed per directory, e.g.:
                #   "\t                   2\tD:\test\"
                # After strip()+split("\t"), fname becomes "D:\test\" (ends
                # with backslash).  These are NOT copied files and must not
                # increment _rc_files.  Same applies to forward-slash paths.
                if fname.endswith("\\") or fname.endswith("/"):
                    logger.debug(f"[robocopy-stdout] SKIPPED dir-summary line: {line!r}")
                    continue

                _cur_fname[0] = fname

                # Parse robocopy's own reported byte count (middle field).
                # Format: "\t    SIZE\tFILENAME"
                _reported_size = 0
                if len(parts) >= 3:
                    try:
                        _reported_size = int(parts[-2].strip().replace(",", ""))
                    except (ValueError, IndexError):
                        pass

                # Fall back to pre-scan map only when robocopy didn't print size
                if _reported_size > 0:
                    _fsz = _reported_size
                else:
                    _rel_key = (dir_prefix + "\\" + fname).lstrip("\\") if dir_prefix and dir_prefix != "." else fname
                    _fsz = _size_map.get(_rel_key, _size_map.get(fname, _basename_map.get(fname, 0)))

                # Credit remaining bytes (those not yet emitted via % lines)
                remaining = _fsz - _cur_credited[0]
                if remaining > 0:
                    _rc_bytes[0] += remaining

                _cur_size[0]    = _fsz
                _cur_credited[0] = 0
                _rc_files[0]   += 1

                if progress_cb is not None:
                    try:
                        progress_cb(
                            _rc_files[0],
                            total_files or 1,
                            fname,
                            _rc_bytes[0],
                            total_bytes,
                        )
                    except Exception:
                        pass

                if cancel_event is not None and cancel_event.is_set():
                    proc.terminate()
                    raise InterruptedError("Backup cancelled by user")
        _reader = threading.Thread(target=_read_stdout, daemon=True)
        _reader.start()

        # Wait for process, checking cancel every 0.25s.
        # Also poll destination file sizes to emit real progress for the UI —
        # robocopy with /MT:8 suppresses per-file percentage lines entirely
        # (they only appear in single-threaded mode), so the _read_stdout thread
        # gets no data until each file finishes.  For a single 10 GB file that
        # means the UI shows "Copying…" with zero progress for 20+ minutes.
        #
        # Fix: every 0.25 s, walk the destination and sum file sizes.  The delta
        # from the last poll is the bytes robocopy wrote in that interval — emit
        # it as a progress callback so the bar, speed, and ETA stay live.
        _last_polled_bytes = [0]
        _last_poll_time    = [time.time()]
        _poll_fname        = [files_to_copy[0]["path"] if files_to_copy else ""]

        def _poll_dest_progress():
            """Sum sizes of all files currently in dst_path and emit progress.

            The poll gives approximate real-time byte progress while robocopy is
            running, but it MUST NOT push bytes_done to >= total_bytes.  Only
            _read_stdout (which parses robocopy's actual completion lines) should
            be allowed to declare a file "done".  If the poll reports full size
            prematurely (robocopy pre-allocates the destination file before
            writing data, or the scan under-counted total_bytes), the UI would
            flash "100% Finalizing…" and then jump backward when _read_stdout
            emits the real in-progress value — causing the visible oscillation.
            Fix: clamp the emitted bytes_done to at most (total_bytes - 1).
            """
            if progress_cb is None or not dst_path.exists():
                return
            try:
                total_written = 0
                for _fp in dst_path.rglob("*"):
                    if _fp.is_file():
                        try:
                            total_written += _fp.stat().st_size
                        except OSError:
                            pass
                now = time.time()
                _last_poll_time[0] = now
                # Only emit if bytes actually grew — skip spurious 0-delta calls.
                if total_written <= _last_polled_bytes[0]:
                    return
                _last_polled_bytes[0] = total_written
                # Cap at (total_bytes - 1) so the poll never triggers "Finalizing…".
                # _read_stdout is the authoritative source for the 100% transition.
                _capped = min(total_written, max(0, total_bytes - 1)) if total_bytes > 0 else total_written
                try:
                    progress_cb(
                        _rc_files[0],
                        total_files or 1,
                        _poll_fname[0],
                        _capped,
                        total_bytes,
                    )
                except Exception:
                    pass
            except Exception:
                pass

        while proc.poll() is None:
            if cancel_event is not None and cancel_event.is_set():
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except Exception:
                    pass
                _reader.join(timeout=2)
                return False
            time.sleep(0.25)
            # Emit dest-size progress so UI shows live MB/s + ETA even with /MT
            _poll_dest_progress()

        _reader.join(timeout=5)
        rc = proc.returncode if proc.returncode is not None else proc.wait()
        # robocopy exit codes: 0=no-op/match, 1=copied OK, 2=extra in dest,
        # 3=0+2 combo, 4=mismatched, 8+=failure.  <8 is success for our purposes.
        _rc_ok = rc is not None and rc < 8
        if not _rc_ok:
            logger.warning(
                f"[robocopy] exited with code {rc} "
                f"(>=8 means at least one file failed; check Windows Event Log "
                f"or re-run manually: robocopy \"{src}\" \"{dst}\" /E /LOG:rc_debug.txt). "
                f"Common SMB causes: code 8=permission/ACL error (try /COPY:D), "
                f"code 16=serious error (SMB session dropped), "
                f"code 32=share access violation."
            )
        return _rc_ok

    try:
        dst_path.mkdir(parents=True, exist_ok=True)

        # ── Incremental mode: only copy specific changed files ─────────────
        if files_to_copy:
            # Group changed files by their parent directory (relative to src root)
            # so we can call robocopy once per directory rather than once per file.
            from collections import defaultdict
            _dir_files: dict = defaultdict(list)
            for entry in files_to_copy:
                rel = entry["path"].replace("/", "\\")
                rel_dir  = str(Path(rel).parent)   # e.g. "subdir" or "."
                filename = Path(rel).name
                _dir_files[rel_dir].append(filename)

            for rel_dir, filenames in _dir_files.items():
                src_dir = src if rel_dir == "." else str(Path(src) / rel_dir)
                dst_dir = dst if rel_dir == "." else str(dst_path / rel_dir)
                cmd = [
                    "robocopy", src_dir, dst_dir,
                    *filenames,
                    "/J",
                    "/COPY:DAT",
                    "/R:2", "/W:3",
                    "/NJH", "/NJS",   # keep /NFL removed so filenames appear in stdout
                ]
                if not _run_robocopy(cmd, dir_prefix=rel_dir):
                    return False, _rc_files[0], _rc_bytes[0]
            return True, _rc_files[0], _rc_bytes[0]

        # ── Full backup mode: copy entire tree in one shot ──────────────────
        cmd = [
            "robocopy",
            src,
            dst,
            "/E",        # copy subdirectories including empty ones
            "/J",        # unbuffered I/O — faster for large files on SMB
            "/COPY:DAT", # copy Data, Attributes, Timestamps
            "/MT:4",     # 4 threads — /MT:8 suppresses per-file % progress lines;
                         # /MT:4 balances throughput with progress visibility.
                         # Dest-size polling in the wait loop provides live progress
                         # regardless, but fewer threads = more % lines on older Windows.
            "/R:2",      # 2 retries on failure
            "/W:3",      # 3-second wait between retries
            "/NJH",      # no job header
            "/NJS",      # no job summary
            # intentionally no /NP and no /NFL so filenames appear in stdout
        ]
        ok = _run_robocopy(cmd)
        # ── Cancel check: if user pressed Cancel during robocopy, stop immediately.
        # _run_robocopy returns False for both failure AND cancel — distinguish here
        # so we don't start the next retry after a deliberate user cancellation.
        if not ok and cancel_event is not None and cancel_event.is_set():
            return False, _rc_files[0], _rc_bytes[0]
        if not ok:
            # ── Retry 1: without /MT:4 and /J — some SMB server configurations
            # WD) rejects concurrent writes or unbuffered I/O via robocopy.
            # Single-threaded + buffered is slower but far more compatible.
            # NOTE: counters are NOT reset so the UI progress bar does not jump
            # back to 0% — it continues from where the previous attempt left off.
            logger.warning(
                "[robocopy_dir] full-tree copy failed with /MT:4 /J — retrying "
                "without multi-threading and unbuffered I/O (SMB compatibility mode)"
            )
            cmd_compat = [
                "robocopy",
                src,
                dst,
                "/E",
                "/COPY:DAT",
                "/R:2",
                "/W:3",
                "/NJH",
                "/NJS",
                # /J and /MT omitted — broadest SMB compatibility
            ]
            ok = _run_robocopy(cmd_compat)
            # Cancel check before Retry 2
            if not ok and cancel_event is not None and cancel_event.is_set():
                return False, _rc_files[0], _rc_bytes[0]
            if ok:
                logger.info("[robocopy_dir] retry (compat mode) succeeded")
            else:
                # ── Retry 2: data-only copy, no attributes/timestamps ────────────
                # Some SMB server configurations (especially SMB1/2) reject
                # NTFS attribute/timestamp propagation via robocopy — exit code 8+.
                # /COPY:D copies data only (no ACLs, no attributes, no timestamps).
                # /XO skips files that are the same age or older in the destination
                # so already-correct files aren't re-copied.
                # /IPG:1 adds a 1ms inter-packet gap to reduce SMB congestion.
                # NOTE: counters are NOT reset — progress bar continues from current
                # position instead of resetting to 0% for a third time.
                logger.warning(
                    "[robocopy_dir] compat-mode retry also failed — trying data-only "
                    "copy (/COPY:D /XO) for maximum SMB compatibility"
                )
                cmd_dataonly = [
                    "robocopy",
                    src,
                    dst,
                    "/E",
                    "/COPY:D",   # data only — no attrs/timestamps (widest SMB compat)
                    "/XO",       # exclude older files in destination (skip already-copied)
                    "/R:3",
                    "/W:5",
                    "/IPG:1",    # 1 ms inter-packet gap — reduces SMB2 congestion
                    "/NJH",
                    "/NJS",
                ]
                ok = _run_robocopy(cmd_dataonly)
                if ok:
                    logger.info("[robocopy_dir] data-only retry succeeded")
                else:
                    logger.warning(
                        "[robocopy_dir] all robocopy modes failed — "
                        "falling back to Python per-file copy loop. "
                        "Check Logs for robocopy exit codes. "
                        "Tip: run 'robocopy \"" + src + "\" \"" + dst + "\" /E /LOG:rc_debug.txt' "
                        "manually to see the full error."
                    )
        return ok, _rc_files[0], _rc_bytes[0]

    except FileNotFoundError:
        return False, 0, 0
    except Exception as _rb_err:
        logger.debug(f"[robocopy_dir] failed ({_rb_err})")
        return False, 0, 0


def _resolve_compress_level(compress) -> int:
    """Resolve compression parameter to gzip level (0-9).
    
    Args:
        compress: bool or int - False/0 = off, True = level 6, 1-9 = gzip level
        
    Returns:
        int: gzip compression level (0 = no compression, 1-9 = gzip levels)
    """
    if not compress:
        return 0
    if compress is True:
        return 6
    if isinstance(compress, int):
        return max(0, min(9, compress))  # clamp to valid range
    return 0  # fallback for unexpected types


class BackupThrottler:
    """Limit backup I/O to prevent system overload.

    Designed to be called per-chunk (inside the read/write loop), not per-file.
    Uses a sliding 1-second window so each window resets cleanly — no debt
    carries over from previous windows, preventing the 'giant sleep' bug that
    occurred when a large file reset bytes_sent to a non-zero value.
    
    Supports optional time-based schedule: each window can have different limits.
    """
    def __init__(self, max_mbps: float = 100.0, schedule: list = None):
        self.max_mbps = max_mbps
        self.schedule = schedule or []  # List of {"start": "HH:MM", "end": "HH:MM", "max_mbps": float}
        self.max_bytes_per_sec = int(max_mbps * 1024 * 1024)
        self.window_start  = time.time()
        self.window_bytes  = 0

    def _get_current_limit(self) -> int:
        """Get current bandwidth limit in bytes/sec, checking schedule windows."""
        if not self.schedule:
            return self.max_bytes_per_sec
        
        now = time.strftime("%H:%M")
        for window in self.schedule:
            start = window.get("start", "")
            end = window.get("end", "")
            max_mbps = window.get("max_mbps", 0.0)
            if start and end and self._time_in_range(now, start, end):
                return int(max_mbps * 1024 * 1024)
        return self.max_bytes_per_sec
    
    def _time_in_range(self, current: str, start: str, end: str) -> bool:
        """Check if current time (HH:MM) is within start-end range."""
        def to_minutes(t):
            h, m = map(int, t.split(":"))
            return h * 60 + m
        current_min, start_min, end_min = to_minutes(current), to_minutes(start), to_minutes(end)
        if start_min <= end_min:
            return start_min <= current_min <= end_min
        else:
            return current_min >= start_min or current_min <= end_min

    def throttle(self, bytes_copied: int):
        """Call after writing each chunk.  Thread-safe for single-writer use."""
        current_limit = self._get_current_limit()
        if current_limit <= 0:
            return  # unlimited

        self.window_bytes += bytes_copied
        elapsed = time.time() - self.window_start

        # How long *should* it have taken to send window_bytes at our limit?
        expected = self.window_bytes / current_limit
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
    # Preserve UNC network paths (\\server\\share or //server/share).
    if path.startswith('\\\\') or path.startswith('//'):
        return path.replace('/', '\\')
    # Normalize double-backslashes that may come from JS string escaping
    if '\\\\' in path:
        path = path.replace('\\\\', '\\')
    return path


def _is_excluded(rel_path: str, abs_path: Path, patterns: List[str]) -> bool:
    """Return True if the file should be excluded from the backup.

    Patterns prefixed with '!' are include-only (whitelist) rules.
    When at least one '!'-prefixed pattern is present the function operates in
    *whitelist mode*: a file is excluded unless its name or relative path
    matches at least one include pattern (fnmatch rules apply).

    In the absence of any '!' patterns the function operates in the normal
    *blacklist mode*: a file is excluded when its name, relative path, or any
    path component matches any of the given patterns.

    This mirrors the logic in watcher.py so that the file-system watcher and
    the backup engine always agree on which files are in scope.
    """
    include_only = [p[1:] for p in patterns if p.startswith("!")]
    exclude_only = [p      for p in patterns if not p.startswith("!")]

    if include_only:
        # Whitelist mode — exclude the file unless it matches an include rule.
        return not any(
            fnmatch.fnmatch(abs_path.name, pat) or fnmatch.fnmatch(rel_path, pat)
            for pat in include_only
        )

    # Normal blacklist mode — unchanged behaviour.
    parts = Path(rel_path).parts
    for pat in exclude_only:
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

    Size-only comparison (no mtime) is intentional: SMB shares sometimes
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

    def fileno(self):
        # Explicitly raise UnsupportedOperation so callers (e.g. gzip.open)
        # that probe for a real file descriptor fall back to the write() path
        # instead of trying to use the underlying fd directly, which causes
        # [Errno 22] Invalid argument on Windows.
        raise io.UnsupportedOperation("fileno")

    # Forward every other attribute access to the underlying file object so
    # gzip.GzipFile (and anything else) can call flush(), tell(), etc. normally.
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

def _flush_smb_dir_cache(path: str) -> None:
    """Force Windows SMB client to flush its directory-listing cache for *path*.

    Background: the Windows SMB2 client caches FIND_FIRST2 (directory listing)
    responses for several seconds.  When a remote machine deletes a file, the
    local SMB client may continue to serve the stale cached listing for up to
    ~10-30 s, depending on the SMB negotiation parameters.
    os.scandir() reads from this cache, so build_snapshot() sees the file as
    still present and diff_snapshots() reports no deletion.

    Strategy (three-layer approach, most aggressive first):

    Layer 1 — NtQueryDirectoryFile with RestartScan=True:
        Opens a directory handle and calls NtQueryDirectoryFile with the
        RestartScan flag set. This forces the SMB2 client to send a fresh
        QUERY_DIRECTORY request to the SMB server, bypassing any client-side
        directory cache. The server must respond with live data.
        This is the most reliable method for defeating both client-side AND
        some server-side caches on Windows/SMB hosts.

    Layer 2 — FindFirstFileExW with FIND_FIRST_EX_NO_BUFFERING:
        Uses the no-buffering flag which tells Windows to bypass its internal
        directory cache and go directly to the filesystem/network.

    Layer 3 — FindFirstFileW (original fallback):
        The original approach. Forces a FIND_FIRST2 SMB request but may still
        hit Windows client-side cache on some configurations.

    This is a no-op on non-Windows platforms and on local (non-UNC) paths.
    """
    if os.name != "nt":
        return
    norm = path.replace("/", "\\")
    if not norm.startswith("\\\\"):
        return  # local path — SMB cache not involved

    _flushed_via = "none"
    try:
        import ctypes
        import ctypes.wintypes as wt
        kernel32 = ctypes.windll.kernel32
        ntdll    = ctypes.windll.ntdll

        # ── Layer 1: NtQueryDirectoryFile with RestartScan ────────────────
        # Open directory handle, then call NtQueryDirectoryFile with
        # RestartScan=True to force server to re-enumerate from scratch.
        try:
            FILE_LIST_DIRECTORY        = 0x0001
            FILE_SHARE_READ            = 0x00000001
            FILE_SHARE_WRITE           = 0x00000002
            FILE_SHARE_DELETE          = 0x00000004
            OPEN_EXISTING              = 3
            FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
            INVALID_HANDLE_VALUE       = ctypes.c_void_p(-1).value

            hDir = kernel32.CreateFileW(
                norm.rstrip("\\"),
                FILE_LIST_DIRECTORY,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                None,
                OPEN_EXISTING,
                FILE_FLAG_BACKUP_SEMANTICS,
                None,
            )
            if hDir != INVALID_HANDLE_VALUE:
                try:
                    # FILE_DIRECTORY_INFORMATION class = 1
                    buf_size = 65536
                    buf = ctypes.create_string_buffer(buf_size)

                    class IO_STATUS_BLOCK(ctypes.Structure):
                        _fields_ = [("Status", ctypes.c_long),
                                    ("Information", ctypes.c_size_t)]

                    io_status = IO_STATUS_BLOCK()

                    # Call with RestartScan=True — forces SMB QUERY_DIRECTORY
                    # with SL_RESTART_SCAN flag, bypassing all caches.
                    status = ntdll.NtQueryDirectoryFile(
                        hDir,           # FileHandle
                        None,           # Event
                        None,           # ApcRoutine
                        None,           # ApcContext
                        ctypes.byref(io_status),  # IoStatusBlock
                        buf,            # FileInformation
                        buf_size,       # Length
                        1,              # FileInformationClass = FileDirectoryInformation
                        False,          # ReturnSingleEntry
                        None,           # FileName filter (None = all)
                        True,           # RestartScan — KEY FLAG
                    )
                    if status == 0 or status == 0x80000006:  # STATUS_NO_MORE_FILES is also ok
                        _flushed_via = "NtQueryDirectoryFile_RestartScan"
                finally:
                    kernel32.CloseHandle(hDir)
        except Exception:
            pass

        # ── Layer 2: FindFirstFileExW with FIND_FIRST_EX_NO_BUFFERING ────
        if _flushed_via == "none":
            try:
                class _WIN32_FIND_DATAW(ctypes.Structure):
                    _fields_ = [
                        ("dwFileAttributes",    wt.DWORD),
                        ("ftCreationTime",      ctypes.c_ulonglong),
                        ("ftLastAccessTime",    ctypes.c_ulonglong),
                        ("ftLastWriteTime",     ctypes.c_ulonglong),
                        ("nFileSizeHigh",       wt.DWORD),
                        ("nFileSizeLow",        wt.DWORD),
                        ("dwReserved0",         wt.DWORD),
                        ("dwReserved1",         wt.DWORD),
                        ("cFileName",           ctypes.c_wchar * 260),
                        ("cAlternateFileName",  ctypes.c_wchar * 14),
                    ]

                find_data = _WIN32_FIND_DATAW()
                pattern   = norm.rstrip("\\") + "\\*"
                # FIND_FIRST_EX_NO_BUFFERING = 0x2 — bypass client-side cache
                FIND_FIRST_EX_NO_BUFFERING = 0x2
                FindExInfoBasic = 1
                FindExSearchNameMatch = 0
                handle = kernel32.FindFirstFileExW(
                    pattern,
                    FindExInfoBasic,
                    ctypes.byref(find_data),
                    FindExSearchNameMatch,
                    None,
                    FIND_FIRST_EX_NO_BUFFERING,
                )
                INVALID = ctypes.c_void_p(-1).value
                if handle != INVALID:
                    kernel32.FindClose(handle)
                    _flushed_via = "FindFirstFileExW_NO_BUFFERING"
            except Exception:
                pass

        # ── Layer 3: FindFirstFileW (original fallback) ───────────────────
        if _flushed_via == "none":
            try:
                class _WIN32_FIND_DATAW2(ctypes.Structure):
                    _fields_ = [
                        ("dwFileAttributes",    wt.DWORD),
                        ("ftCreationTime",      ctypes.c_ulonglong),
                        ("ftLastAccessTime",    ctypes.c_ulonglong),
                        ("ftLastWriteTime",     ctypes.c_ulonglong),
                        ("nFileSizeHigh",       wt.DWORD),
                        ("nFileSizeLow",        wt.DWORD),
                        ("dwReserved0",         wt.DWORD),
                        ("dwReserved1",         wt.DWORD),
                        ("cFileName",           ctypes.c_wchar * 260),
                        ("cAlternateFileName",  ctypes.c_wchar * 14),
                    ]
                find_data2 = _WIN32_FIND_DATAW2()
                pattern2   = norm.rstrip("\\") + "\\*"
                handle2    = kernel32.FindFirstFileW(pattern2, ctypes.byref(find_data2))
                INVALID2   = ctypes.c_void_p(-1).value
                if handle2 != INVALID2:
                    kernel32.FindClose(handle2)
                    _flushed_via = "FindFirstFileW"
            except Exception:
                pass

        logger.info(
            f"[flush_smb_dir_cache] path={path!r} method_used={_flushed_via!r} "
            f"— NtQueryDirectoryFile RestartScan forces server re-enumeration"
        )

    except Exception:
        pass  # never let a cache-flush failure break the poll loop

# Public alias — watcher.py references this without the leading underscore.
# The private _flush_smb_dir_cache name is kept for internal use; this alias
# ensures the AttributeError (and silent swallow) bug is fixed without
# renaming the private function throughout the codebase.
flush_smb_dir_cache = _flush_smb_dir_cache


def build_snapshot(
    path: str,
    previous: Optional[Dict] = None,
    exclude_patterns: Optional[List[str]] = None,
    timeout_sec: int = 300,
    scan_cb: Optional[Callable] = None,
    cancel_event=None,
    changed_paths: Optional[List[str]] = None,
    skipped_symlinks: Optional[List[str]] = None,
) -> Dict[str, dict]:
    """
    Walk *path* and return a snapshot dict:
        { relative_path: { hash, size, mtime } }

    Improvements:
    - Reuses previous snapshot entries when mtime + size are unchanged
    - Timeout protection for network shares
    - Unix: uses signal.alarm()
    - Windows: uses thread timeout
    - changed_paths fast-path: when the watcher provides the exact set of
      changed/added/deleted relative paths, skip the full rglob and instead
      start from the previous snapshot, re-stat only changed files, and
      remove any deleted ones. Falls back to full scan if previous is absent.
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
    # 300s (5 min) is too short for large network shares.
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

        # For UNC/SMB sources, flush the Windows SMB client directory cache
        # before scanning so we pick up files added from other machines that
        # the local SMB client may still be caching as absent.  The flush is
        # a lightweight NtQueryDirectoryFile RestartScan call on the root; each
        # os.scandir() in _walk below opens its own enumeration handle which
        # also bypasses the per-directory cache at the OS level.
        if not changed_paths and str(path).startswith("\\\\"):
            try:
                _flush_smb_dir_cache(str(path))
            except Exception:
                pass

        # ── Watcher fast-path ─────────────────────────────────────────────
        # When the watcher has tracked exactly which files changed, skip the
        # expensive rglob and instead patch the previous snapshot in-place:
        #   1. Start from a copy of the previous snapshot (all unchanged files
        #      are inherited for free — no I/O needed).
        #   2. Re-stat every path the watcher flagged as changed/added/deleted.
        #   3. Remove entries whose file no longer exists on disk (deletions).
        # This turns an O(all files) scan into an O(changed files) scan.
        # We fall back to the full rglob if no previous snapshot exists (first
        # backup) or if changed_paths is None/empty (scheduled full-scan).
        if changed_paths and previous and not root.is_file():
            snapshot.update(previous)  # inherit all unchanged entries
            # Normalise changed_paths: the watcher emits absolute paths but
            # build_snapshot works in relative paths (relative to root).
            # Convert any absolute path that starts with root to a relative path;
            # leave already-relative paths unchanged so callers that pass relative
            # paths continue to work correctly.
            _root_str = str(root).rstrip("/\\")
            _norm_changed = []
            for _cp in changed_paths:
                _cp_norm = _cp.replace("/", os.sep)
                _root_norm = _root_str.replace("/", os.sep)
                if _cp_norm.lower().startswith(_root_norm.lower() + os.sep):
                    _cp_norm = _cp_norm[len(_root_norm) + 1:]
                _norm_changed.append(_cp_norm)
            changed_set = set(_norm_changed)
            for rel in changed_set:
                if cancel_event is not None and cancel_event.is_set():
                    raise InterruptedError("Backup cancelled by user")
                fp = root / rel
                if scan_cb:
                    try:
                        scan_cb(rel)
                    except InterruptedError:
                        raise
                    except Exception:
                        logger.debug("[suppressed] Exception ignored near: try: |                         scan_cb(rel) |                     except Interru", exc_info=True)
                        pass
                if not fp.exists() or fp.is_symlink():
                    # File was deleted — remove from snapshot
                    snapshot.pop(rel, None)
                    continue
                if exclude_patterns and _is_excluded(rel, fp, exclude_patterns):
                    snapshot.pop(rel, None)
                    continue
                try:
                    st = fp.stat()
                    snapshot[rel] = {"hash": "", "size": st.st_size, "mtime": st.st_mtime}
                except (IOError, OSError, PermissionError):
                    snapshot.pop(rel, None)
            return snapshot

        # -------------------------
        # Single File Case
        # -------------------------
        if root.is_file():
            try:
                stat = root.stat()
                snapshot[root.name] = {
                    # Always skip hashing during scan — copy phase backfills it.
                    "hash": "",
                    "size": stat.st_size,
                    "mtime": stat.st_mtime,
                }
            except InterruptedError:
                raise
            except (IOError, OSError, PermissionError):
                pass

            return snapshot

        # -------------------------
        # Directory Walk (os.scandir-based for speed)
        # -------------------------
        # os.scandir returns DirEntry objects whose .stat() is cached from the
        # directory listing — on Windows/UNC this means ONE network request per
        # directory instead of one per file (as rglob + fp.stat() would do).
        # This dramatically speeds up the scan phase on network shares.
        def _walk(dir_path: Path, rel_prefix: str):
            try:
                entries = list(os.scandir(str(dir_path)))
            except (OSError, PermissionError):
                return
            logger.info(
                f"[snapshot] os.scandir({str(dir_path)!r}) returned "
                f"{len(entries)} raw entries: "
                f"{[e.name for e in entries]}"
            )
            for entry in entries:
                if cancel_event is not None and cancel_event.is_set():
                    raise InterruptedError("Backup cancelled by user")
                rel = rel_prefix + entry.name if rel_prefix else entry.name
                try:
                    if entry.is_symlink():
                        try:
                            target = os.readlink(entry.path)
                            sym_detail = f" → {target}"
                        except OSError:
                            sym_detail = " (broken symlink)"
                        logger.warning(
                            f"[snapshot] Symlink skipped (not backed up): {rel}{sym_detail}"
                        )
                        if skipped_symlinks is not None:
                            skipped_symlinks.append(rel)
                        continue
                    if entry.is_dir(follow_symlinks=False):
                        _walk(Path(entry.path), rel + os.sep)
                        continue
                    if not entry.is_file(follow_symlinks=False):
                        continue
                    if exclude_patterns and _is_excluded(rel, Path(entry.path), exclude_patterns):
                        continue
                    # Use cached stat from scandir — no extra network round-trip
                    st = entry.stat(follow_symlinks=False)
                    if scan_cb:
                        try:
                            scan_cb(rel)
                        except InterruptedError:
                            raise
                        except Exception:
                            pass
                    # Reuse previous entry if mtime+size unchanged (incremental fast path)
                    if (
                        previous
                        and rel in previous
                        and previous[rel].get("mtime") == st.st_mtime
                        and previous[rel].get("size") == st.st_size
                    ):
                        snapshot[rel] = previous[rel]
                        continue
                    snapshot[rel] = {"hash": "", "size": st.st_size, "mtime": st.st_mtime}
                except InterruptedError:
                    raise
                except (IOError, OSError, PermissionError):
                    continue

        _walk(root, "")
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


def diff_snapshots(old: Dict, new: Dict, max_deletes: int = 500, size_only: bool = False) -> List[dict]:
    """Compare two snapshots and return a list of change records.

    max_deletes: cap the number of "deleted" entries recorded. When a stale
    snapshot has thousands of old paths (e.g. user emptied the watched folder),
    iterating all of them is slow and produces a huge changeset that is never
    actually used for copying. We record up to max_deletes deletions and stop.

    size_only: when True and both hashes are empty, treat a file as modified
    only when its *size* differs — mtime is ignored entirely.  Use this for
    destination-folder poll watches where robocopy may preserve the source
    file's original mtime, causing spurious "modified" events on every poll
    cycle even though the file has not actually changed.
    """

    changes = []

    # Added / modified — always check every new file (usually tiny set)
    for rel, meta in new.items():
        if rel not in old:
            changes.append({
                "type": "added",
                "path": rel,
                "old_hash": None,
                "new_hash": meta["hash"],
                "size": meta["size"],
            })
        elif old[rel]["hash"] != meta["hash"] or (
            # Fix: when both hashes are "" (file was seed-skipped on a previous
            # first backup and its hash was never backfilled), fall back to
            # mtime+size comparison so changed files are not silently missed.
            # Without this, diff_snapshots would compare "" == "" → not modified,
            # and incremental backups would never copy changed files again.
            #
            # Note: FAT-based SMB shares have ~2-second mtime precision
            # and Python's stat().st_mtime can return slightly different float
            # values for the same unchanged file across network calls.  Use a 2-second
            # tolerance so a file whose mtime shifts by <2 s (but size stays the
            # same) is NOT treated as modified.  This prevents the 10 GB re-copy
            # loop seen with robocopy-backed sync-mode watches on SMB sources.
            #
            # size_only=True: destination-folder poll watches use robocopy which
            # preserves the source mtime; each os.stat() over SMB can return a
            # slightly different float for the same file, making mtime unreliable.
            # When size_only is set we ignore mtime entirely and only flag genuine
            # size differences, eliminating the perpetual "modified" spam on dest
            # watches when the actual file content has not changed.
            old[rel]["hash"] == "" and meta["hash"] == "" and (
                old[rel].get("size") != meta.get("size") or
                (not size_only and abs((old[rel].get("mtime") or 0.0) - (meta.get("mtime") or 0.0)) > 2.0)
            )
        ):
            changes.append({
                "type": "modified",
                "path": rel,
                "old_hash": old[rel]["hash"],
                "new_hash": meta["hash"],
                "size": meta["size"],
            })

    # Deleted — cap at max_deletes to avoid blocking on stale snapshots
    deleted_changes = []
    delete_count = 0
    for rel, meta in old.items():
        if rel not in new:
            if delete_count >= max_deletes:
                break
            deleted_changes.append({
                "type": "deleted",
                "path": rel,
                "old_hash": meta["hash"],
                "new_hash": None,
                "size": meta["size"],
            })
            delete_count += 1

    # Return deletions FIRST so the history timeline is correct when both a
    # deletion and an addition are detected in the same poll cycle.
    # (e.g. a coworker deletes testfile_10gb.dat and adds IMG_0019.pdf — the
    # deletion happened first and should appear first in the history log.)
    return deleted_changes + changes


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
            # Collect all directories to scan: top-level versioned dirs +
            # sync-mode metadata dirs inside .backupsys_meta subfolders.
            _scan_dirs = []
            for d in dest.iterdir():
                if not d.is_dir():
                    continue
                if d.name == ".backupsys_meta":
                    # Sync-mode metadata: each sub-subfolder is one backup run
                    try:
                        for _sd in d.iterdir():
                            if _sd.is_dir():
                                _scan_dirs.append(_sd)
                    except Exception:
                        pass
                else:
                    _scan_dirs.append(d)
            for d in sorted(_scan_dirs, key=lambda p: p.name, reverse=True):
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
                logger.debug('[suppressed] Exception ignored.', exc_info=True)
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
    """Generate a new encryption key (44 characters, URL-safe base64)."""
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
    """Derive a 32-byte AES-256 key via HKDF-SHA256 (RFC 5869).

    Uses cryptography.hazmat HKDF when available; falls back to a pure-stdlib
    HKDF implementation so the function works even if the cryptography package's
    kdf sub-package is broken or missing.
    """
    try:
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
    except (ImportError, Exception):
        # Pure-stdlib HKDF-SHA256 (RFC 5869) — no external dependencies.
        import hashlib, hmac
        hash_len = 32  # SHA-256 output length
        salt = b"\x00" * hash_len          # HKDF spec: salt defaults to HashLen zeros
        info = b"BackupSys-AES-GCM-v1"
        length = 32
        # Extract
        prk = hmac.new(salt, raw_key_bytes, hashlib.sha256).digest()
        # Expand
        t = b""
        okm = b""
        for i in range(1, -(-length // hash_len) + 1):  # ceil(length / hash_len)
            t = hmac.new(prk, t + info + bytes([i]), hashlib.sha256).digest()
            okm += t
        return okm[:length]


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
    """Encrypt a file using streaming AES-256-GCM (or Fernet fallback).

    Processes the source in 1 MB chunks so arbitrarily large files are
    supported with constant ~2 MB RAM overhead.  Returns the SHA-256 hex
    digest of the *ciphertext* written to dest_path.

    Legacy Fernet files written by earlier BackupSys versions are still
    decryptable via _decrypt_file() — only new writes use this format.
    When the hazmat sub-package (AESGCM) is unavailable, falls back to
    Fernet so encryption still works on broken cryptography installs.
    """
    if not CRYPTO_AVAILABLE:
        raise RuntimeError("cryptography library not installed")

    try:
        if not HAZMAT_AVAILABLE:
            raise ImportError("hazmat unavailable, use Fernet fallback")

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
                if not isinstance(ciphertext, bytes):
                    raise RuntimeError("AESGCM.encrypt did not return bytes — hazmat may be broken")
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

    except (ImportError, RuntimeError) as _hazmat_err:
        # Fallback: use Fernet (confirmed working even on broken hazmat installs)
        logger.debug("AES-GCM unavailable (%s); falling back to Fernet encryption", _hazmat_err)
        try:
            _validate_key(key)
            cipher_f = Fernet(key.encode())
            with open(src_path, "rb") as f:
                plaintext = f.read()
            ciphertext_f = cipher_f.encrypt(plaintext)
            h = hashlib.sha256(ciphertext_f).hexdigest()
            with open(dest_path, "wb") as f:
                f.write(ciphertext_f)
            return h
        except Exception as e_fernet:
            raise RuntimeError(f"Encryption failed for {Path(src_path).name}: {e_fernet}")

    except Exception as e:
        raise RuntimeError(f"Encryption failed for {Path(src_path).name}: {e}")


def _encrypt_bytes(data: bytes, key: str) -> bytes:
    """Encrypt raw bytes with AES-256-GCM (same streaming format as _encrypt_file).

    Used for small in-memory payloads like MANIFEST.json content.
    Returns the complete ciphertext blob (header + chunks + EOF sentinel).
    Falls back to Fernet when hazmat is unavailable.
    """
    if HAZMAT_AVAILABLE:
        import struct
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        import io, os as _os

        raw_key  = _validate_key(key)
        aes_key  = _derive_aes_key(raw_key)
        cipher   = AESGCM(aes_key)
        nonce_seed = _os.urandom(12)

        out = io.BytesIO()
        out.write(_AES_MAGIC + nonce_seed)

        for chunk_idx, offset in enumerate(range(0, len(data), _AES_CHUNK)):
            plaintext = data[offset:offset + _AES_CHUNK]
            idx_bytes = struct.pack("<Q", chunk_idx)[:12].ljust(12, b"\x00")
            nonce     = bytes(a ^ b for a, b in zip(nonce_seed, idx_bytes))
            ciphertext = cipher.encrypt(nonce, plaintext, None)
            out.write(struct.pack("<I", len(ciphertext)))
            out.write(ciphertext)

        out.write(struct.pack("<I", 0))  # EOF sentinel
        return out.getvalue()
    else:
        # Fernet fallback
        _validate_key(key)
        return Fernet(key.encode()).encrypt(data)


def _decrypt_bytes(data: bytes, key: str) -> bytes:
    """Decrypt bytes produced by _encrypt_bytes.

    Supports both streaming AES-256-GCM format and Fernet fallback format.
    Raises RuntimeError if decryption fails.
    """
    _validate_key(key)

    if len(data) >= 8 and data[:8] == _AES_MAGIC:
        # Streaming AES-256-GCM format
        import struct
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        import io

        raw_key = _validate_key(key)
        aes_key = _derive_aes_key(raw_key)
        cipher  = AESGCM(aes_key)

        buf = io.BytesIO(data)
        buf.read(8)           # skip magic
        nonce_seed = buf.read(12)

        out = io.BytesIO()
        chunk_idx = 0
        while True:
            len_field = buf.read(4)
            if len(len_field) < 4:
                break
            clen = struct.unpack("<I", len_field)[0]
            if clen == 0:
                break
            ciphertext = buf.read(clen)
            idx_bytes  = struct.pack("<Q", chunk_idx)[:12].ljust(12, b"\x00")
            nonce      = bytes(a ^ b for a, b in zip(nonce_seed, idx_bytes))
            out.write(cipher.decrypt(nonce, ciphertext, None))
            chunk_idx += 1

        return out.getvalue()
    else:
        # Fernet format (fallback path or legacy)
        return Fernet(key.encode()).decrypt(data)


def _decrypt_file(src_path: str, dest_path: str, key: str) -> None:
    """Decrypt a file.  Auto-detects format: streaming AES-GCM (v2) or legacy/fallback Fernet (v1)."""
    if not CRYPTO_AVAILABLE:
        raise RuntimeError("cryptography library not installed")

    try:
        with open(src_path, "rb") as fh:
            magic = fh.read(8)

        if magic == _AES_MAGIC:
            # ── Streaming AES-256-GCM ─────────────────────────────────────────
            if not HAZMAT_AVAILABLE:
                raise RuntimeError(
                    "This backup was encrypted with AES-GCM but the hazmat "
                    "sub-package is unavailable on this install. "
                    "Please reinstall the cryptography package."
                )
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
            # ── Fernet (v1 legacy or hazmat-fallback) ─────────────────────────
            _validate_key(key)
            cipher = Fernet(key.encode())
            with open(src_path, "rb") as f:
                ciphertext = f.read()
            plaintext = cipher.decrypt(ciphertext)
            with open(dest_path, "wb") as f:
                f.write(plaintext)

    except Exception as e:
        raise RuntimeError(f"Decryption failed for {Path(src_path).name}: {e}")
def upload_to_gdrive(local_dir: str, cloud_config: dict, allowed_rel_paths: Optional[set] = None) -> dict:
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

        # Upload a file — overwrite if it already exists, otherwise create.
        # upload_name lets the caller specify a display name different from fp.name
        # (used when decompressing .gz files so the original filename is preserved).
        def _upload_file(fp: Path, parent_id: str, upload_name: str = ""):
            name  = upload_name or fp.name
            media = MediaFileUpload(str(fp), resumable=True)
            q = (
                f"name = {repr(name)} "
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
                meta = {"name": name, "parents": [parent_id]}
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

        import gzip as _gzip
        import tempfile as _tempfile

        # ── Upfront cleanup: delete every .gz file from the GDrive folder ──
        # Files backed up with compression are stored as .xlsx.gz, .docx.gz etc.
        # We always upload the decompressed version, so any .gz already on
        # GDrive (from previous runs before this fix, or re-runs) must be
        # removed first. Running the sweep unconditionally at the start is
        # more reliable than trying to delete after each individual upload.
        try:
            page_token = None
            while True:
                list_res = service.files().list(
                    q=f"'{run_folder_id}' in parents and trashed = false",
                    fields="nextPageToken, files(id, name)",
                    pageToken=page_token,
                    pageSize=100,
                ).execute()
                for gf in list_res.get("files", []):
                    if gf["name"].endswith(".gz"):
                        try:
                            service.files().delete(fileId=gf["id"]).execute()
                        except Exception:
                            logger.debug('[suppressed] Exception ignored.', exc_info=True)
                            pass
                page_token = list_res.get("nextPageToken")
                if not page_token:
                    break
        except Exception:
            pass  # cleanup sweep is best-effort; never block the upload

        # Build the iterable of files to upload.
        # If allowed_rel_paths is provided (populated from the backup snapshot),
        # only upload those specific files — this prevents stray folders that happen
        # to exist in the destination (e.g. Monthly/Weekly leftover dirs) from being
        # mirrored to GDrive.  Without the filter, rglob("*") would blindly upload
        # everything, including directories that were never part of this backup.
        if allowed_rel_paths is not None:
            _candidates = []
            for rel_str in allowed_rel_paths:
                fp_candidate = ld / rel_str
                if fp_candidate.is_file():
                    _candidates.append(fp_candidate)
        else:
            _candidates = [fp for fp in ld.rglob("*") if fp.is_file()]

        for fp in _candidates:
            # Skip internal metadata and deletion markers
            if fp.name in _SKIP or fp.name.endswith(".DELETED"):
                continue

            rel       = fp.relative_to(ld)
            parent_id = _get_or_create_folder(list(rel.parts[:-1]))

            # Decompress .gz files before uploading so Google Drive/Sheets
            # can open them natively (e.g. "test sheet.xlsx.gz" → "test sheet.xlsx").
            if fp.name.endswith(".gz"):
                original_name = fp.name[:-3]   # strip .gz suffix
                tmp_fd, tmp_path = _tempfile.mkstemp(suffix="_" + original_name)
                os.close(tmp_fd)
                try:
                    with _gzip.open(str(fp), "rb") as f_in, open(tmp_path, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)
                    _upload_file(Path(tmp_path), parent_id, upload_name=original_name)
                finally:
                    try:
                        os.remove(tmp_path)
                    except OSError:
                        pass
            else:
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
            creationflags=_WIN_NO_WINDOW,
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
            creationflags=_WIN_NO_WINDOW,
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


# ─── Remote source download helper ───────────────────────────────────────────

def _download_remote_source(
    source_type: str,
    remote_path: str,
    sftp_cfg:   Optional[dict] = None,
    ftp_cfg:    Optional[dict] = None,
    webdav_cfg: Optional[dict] = None,
    progress_cb=None,
) -> dict:
    """
    Download a remote source directory to a local temp directory so the backup
    engine can treat it as a plain local path.

    source_type: "sftp" | "ftp" | "ftps" | "webdav"
    remote_path: path on the remote server (e.g. "/home/user/docs")
    sftp_cfg / ftp_cfg / webdav_cfg: connection credentials dict.

    Returns:
        {
          "ok":         bool,
          "temp_dir":   str,      # caller must rmtree when done; None on failure
          "error":      str|None,
          "downloaded": int,
        }
    """
    import tempfile
    import shutil

    if not TRANSPORT_AVAILABLE:
        return {"ok": False, "temp_dir": None,
                "error": "transport_utils not available (install paramiko / ftplib / webdavclient3 dependencies)"}

    temp_dir = tempfile.mkdtemp(prefix="backupsys_src_")
    try:
        if source_type == "sftp":
            result = download_from_sftp(remote_path, temp_dir, sftp_cfg or {}, progress_cb=progress_cb)
        elif source_type in ("ftp", "ftps"):
            result = download_from_ftp(remote_path, temp_dir, ftp_cfg or {}, progress_cb=progress_cb)
        elif source_type == "webdav":
            from transport_utils import download_from_webdav
            result = download_from_webdav(remote_path, temp_dir, webdav_cfg or {}, progress_cb=progress_cb)
        else:
            shutil.rmtree(temp_dir, ignore_errors=True)
            return {"ok": False, "temp_dir": None,
                    "error": f"Unknown remote source type: {source_type!r}"}

        if result.get("status") != "ok":
            shutil.rmtree(temp_dir, ignore_errors=True)
            return {"ok": False, "temp_dir": None,
                    "error": result.get("error", "Download failed"),
                    "downloaded": result.get("downloaded", 0)}

        return {"ok": True, "temp_dir": temp_dir,
                "error": None, "downloaded": result.get("downloaded", 0)}

    except Exception as exc:
        shutil.rmtree(temp_dir, ignore_errors=True)
        return {"ok": False, "temp_dir": None, "error": str(exc), "downloaded": 0}


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
    compress: Union[bool, int] = False,
    throttler=None,
    cloud_config: Optional[Dict] = None,
    destinations: Optional[List[Dict]] = None,
    triggered_by: Optional[str] = None,
    cancel_event=None,
    pause_event=None,
    sync_mode: bool = False,
    dry_run: bool = False,
    max_file_size_mb: float = 0,
    pre_backup_cmd: str = "",
    post_backup_cmd: str = "",
    verify_after: bool = False,
    changed_paths: Optional[List[str]] = None,
    verify_remote_upload: bool = False,
    source_type: str = "local",             # "local" | "sftp" | "ftp" | "webdav"
    source_sftp_cfg:   Optional[Dict] = None,  # SFTP credentials when source_type="sftp"
    source_ftp_cfg:    Optional[Dict] = None,  # FTP  credentials when source_type="ftp"
    source_webdav_cfg: Optional[Dict] = None,  # WebDAV credentials when source_type="webdav"
    force_robocopy: bool = False,  # override same-host UNC detection and always use robocopy
) -> dict:
    """
    Execute a backup from source → destination.
    progress_cb(copied, total, current_file) is called after each file copy.
    Tracks failed files for reporting.
    GDrive upload result is persisted in MANIFEST.json and returned in result dict.

    dry_run=True: scans source and builds the change list but copies nothing.
                  Returns immediately with status='dry_run' and the full changes list.
    max_file_size_mb: if > 0, skip files larger than this many MB (0 = no limit).
    """
    import gzip
    import shutil as _sh

    started = time.time()
    ts      = datetime.now().isoformat()

    # Resolve compression level for backward compatibility
    compress_level = _resolve_compress_level(compress)

    # ── Sync mode: never compress ──────────────────────────────────────────
    # In sync mode the destination is a direct mirror of the source — files
    # must keep their original names and extensions (e.g. .xlsx, .docx) so
    # they can be opened directly without any extra steps.  Compression
    # would rename files to .xlsx.gz which breaks direct access.
    # GDrive already decompresses on upload, so keeping sync uncompressed
    # gives consistent behaviour both locally and in GDrive.
    if sync_mode and compress_level > 0:
        compress_level = 0
        logger.info(
            f"[backup] '{watch_name}': compression disabled for sync mode "
            f"— files are stored with original extensions for direct access."
        )

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
        "compressed":    compress_level > 0,
        "encrypted":     bool(encrypt_key),
        "failed_files":  [],
        "skipped_symlinks": [],      # symlinks found in source but not backed up
        "destinations_upload":  [],  # Will be populated if destinations are configured
        "triggered_by":  triggered_by or "auto",
    }

    backup_dir = None

    # VSS and resume state — initialised before the try block so the
    # finally clause can always clean them up safely.
    _vss_shadow_id = None
    _resume_path   = dest_root / f"_resume_{watch_id}.json"

    # ── Remote source: download to a temp dir before processing ───────────────
    # When source_type is "sftp", "ftp", or "webdav" we fetch the remote
    # directory into a
    # local temp dir and then treat that temp dir as the backup source.  This
    # allows the rest of the engine (snapshot diffing, compression, encryption,
    # VSS, incremental logic) to work without any changes.
    _remote_source_temp: Optional[str] = None
    if source_type in ("sftp", "ftp", "ftps", "webdav"):
        logger.info(f"[remote-src] Downloading {source_type.upper()} source: {source}")
        _dl = _download_remote_source(
            source_type=source_type,
            remote_path=source,
            sftp_cfg=source_sftp_cfg,
            ftp_cfg=source_ftp_cfg,
            webdav_cfg=source_webdav_cfg,
            progress_cb=scan_cb,
        )
        if not _dl["ok"]:
            result["error"] = f"Failed to download {source_type.upper()} source: {_dl['error']}"
            return result
        _remote_source_temp = _dl["temp_dir"]
        logger.info(f"[remote-src] Downloaded {_dl['downloaded']} file(s) to {_remote_source_temp}")
        source   = _remote_source_temp
        src_path = Path(source)

    # ── Pre-backup hook ────────────────────────────────────────────────────────
    if pre_backup_cmd:
        _pre_cmd = subprocess.run(pre_backup_cmd, shell=True, capture_output=True, text=True, creationflags=_WIN_NO_WINDOW)
        if _pre_cmd.returncode != 0:
            return {"ok": False, "error": f"Pre-backup command failed (exit {_pre_cmd.returncode}): {_pre_cmd.stderr}"}

    try:
        if not src_path.exists():
            raise FileNotFoundError(f"Source not found: {source}")

        # ── VSS: create a shadow copy so locked/open files can be read ──────
        # Only attempted on Windows, only for local or UNC sources (not SFTP paths),
        # and only for directory sources (not single-file watches).
        # Fails gracefully: if VSS is unavailable the backup continues on live files.
        _effective_source = source
        # VSS shadow copy: local drives only (C:\, D:\ etc.)
        # Path.drive returns the UNC prefix for network paths (e.g. \\server\share),
        # so we must explicitly exclude network sources — VSS only applies to local
        # NTFS volumes and attempting it on a network path wastes time and emits a
        # misleading "shadow copy unavailable" warning.
        _is_unc_source = str(source).startswith("\\\\") or str(source).startswith("//")
        if (os.name == "nt"
                and src_path.is_dir()
                and not str(source).startswith("\\\\?\\")  # not already a shadow path
                and not _is_unc_source                          # skip UNC / SMB sources
                and Path(source).drive):                        # has a local drive letter
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
                    logger.debug("[suppressed] Exception ignored while deleting bad checkpoint.", exc_info=True)
                    pass

        if backup_dir is None:
            if sync_mode:
                backup_dir = dest_root
                if not dry_run:
                    backup_dir.mkdir(parents=True, exist_ok=True)
            else:
                backup_dir = dest_root / f"{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}__{safe_name}"
                if not dry_run:
                    backup_dir.mkdir(parents=True, exist_ok=True)

        # ── Auto-exclude backup output folders inside the source ────────────
        # Prevents scanning backup output dirs (e.g. 20240101_123456__name)
        # that were previously written into the watched source folder, and
        # also excludes the destination folder itself if it is inside the source.
        _excl = list(exclude_patterns or [])
        try:
            _src_resolved  = Path(_effective_source).resolve()
            _dest_resolved = dest_root.resolve()
            # Case 1: destination is inside source — exclude the top-level subdir
            try:
                _dest_rel = _dest_resolved.relative_to(_src_resolved)
                _excl_top = _dest_rel.parts[0] if _dest_rel.parts else ""
                if _excl_top and _excl_top not in _excl:
                    _excl.append(_excl_top)
                    logger.info(
                        f"[snapshot] Auto-excluding dest subfolder '{_excl_top}' "
                        f"from source scan"
                    )
            except ValueError:
                pass  # dest not inside source

            # Case 2: exclude backup output dirs (YYYYMMDD_HHMMSS*__<name>) that
            # were previously created directly inside the source folder.
            # Pattern: starts with 8 digits (date), underscore, 6 digits (time).
            import re as _re
            _backup_dir_pat = _re.compile(r"^\d{8}_\d{6}")
            try:
                for _child in _src_resolved.iterdir():
                    if _child.is_dir() and _backup_dir_pat.match(_child.name):
                        if _child.name not in _excl:
                            _excl.append(_child.name)
                            logger.info(
                                f"[snapshot] Auto-excluding stale backup folder "
                                f"'{_child.name}' from source scan"
                            )
            except Exception:
                logger.debug('[suppressed] Exception ignored.', exc_info=True)
                pass
        except Exception:
            logger.debug('[suppressed] Exception ignored.', exc_info=True)
            pass

        new_snapshot = build_snapshot(
            _effective_source,
            previous=previous_snapshot,
            exclude_patterns=_excl,
            scan_cb=scan_cb,
            cancel_event=cancel_event,
            changed_paths=changed_paths,
            skipped_symlinks=result["skipped_symlinks"],
        )

        if incremental and previous_snapshot:
            # Use size_only=True for UNC/SMB sources: robocopy fast-path stores
            # hash="" for all files, so diff falls back to mtime comparison.
            # SMB mtime can drift by more than the 2 s tolerance on every stat()
            # call — this causes every file to appear "modified" on each
            # incremental run, triggering a full re-copy of the entire source.
            # size_only=True ignores mtime entirely and only flags genuine size
            # differences (actual content changes), which is both correct and fast.
            _unc_src = str(source).startswith("\\\\") or str(source).startswith("//")
            changes       = diff_snapshots(previous_snapshot, new_snapshot, size_only=_unc_src)
            files_to_copy = [c for c in changes if c["type"] in ("added", "modified")]

            # ── Sync-mode destination check ───────────────────────────────────
            # diff_snapshots only compares source→source snapshots, so it cannot
            # detect files that were deleted from the *destination* while the
            # source remained unchanged.  In sync mode we want the destination to
            # mirror the source exactly, so we build a {rel: size} index of the
            # destination and find any source files absent from it.
            #
            # Optimisation: when source and destination are on the same Windows host,
            # skip the expensive rglob and instead stat() only the files we know
            # about (from new_snapshot).  One stat() per file vs. a full directory
            # traversal — much faster on large SMB shares.
            #
            # IMPORTANT — watcher fast-path runs (changed_paths is set):
            # build_snapshot()'s changed_paths fast-path returns new_snapshot
            # pre-populated with EVERY previously-known file (inherited unchanged)
            # plus the handful that actually changed — it is NOT limited to just
            # the changed files. Looping over new_snapshot here would therefore
            # stat() every file in the whole share (one network round-trip each)
            # on every small incremental run, turning a 2-file change into a scan
            # of the entire destination — slower than the original full directory
            # walk, and with no UI feedback during it (scan_cb is intentionally
            # not called in this loop), which makes the app look stuck.
            # On these fast runs we only need to check the few files that are
            # actually about to be copied; the comprehensive missing-file
            # self-heal below is skipped here and instead relies on scheduled/
            # full/manual scans (where changed_paths is None) to catch files
            # that were deleted directly from the destination.
            if sync_mode:
                _fast_incremental = bool(changed_paths)
                _dest_index_inc: Dict[str, int] = {}
                _dest_host_inc = _unc_host(str(dest_root))
                _src_host_inc  = _unc_host(str(src_path))
                _same_host_inc = bool(_dest_host_inc and _dest_host_inc == _src_host_inc)
                try:
                    if _fast_incremental:
                        # Only stat() the small set of files this run actually
                        # intends to copy — never the whole destination.
                        for _c in files_to_copy:
                            if cancel_event is not None and cancel_event.is_set():
                                raise InterruptedError("Backup cancelled by user")
                            _dp = dest_root / _c["path"]
                            try:
                                _dest_index_inc[_c["path"]] = _dp.stat().st_size
                            except OSError:
                                pass  # file absent from dest — will be queued for copy
                    elif _same_host_inc:
                        # Fast path: stat only files we care about, no full rglob
                        for _rel_key in new_snapshot:
                            if cancel_event is not None and cancel_event.is_set():
                                raise InterruptedError("Backup cancelled by user")
                            _dp = dest_root / _rel_key
                            try:
                                _dest_index_inc[_rel_key] = _dp.stat().st_size
                            except OSError:
                                pass  # file absent from dest — will be queued for copy
                    else:
                        for _dp in dest_root.rglob("*"):
                            if cancel_event is not None and cancel_event.is_set():
                                raise InterruptedError("Backup cancelled by user")
                            if not _dp.is_file() or _dp.is_symlink():
                                continue
                            try:
                                _rel = str(_dp.relative_to(dest_root))
                                _dest_index_inc[_rel] = _dp.stat().st_size
                                # NOTE: scan_cb is intentionally NOT called here.
                                # This is an internal destination scan (sync-mode
                                # missing-file detection), not a source file discovery
                                # scan.  Calling scan_cb here would make the UI show
                                # "Scanning: <backup file>" during a preparation step,
                                # hiding the real copy-phase progress from the user.
                            except (OSError, ValueError):
                                pass
                except InterruptedError:
                    raise
                except Exception as _de:
                    logger.warning(
                        f"[sync] '{watch_name}': destination scan failed ({_de}) "
                        f"— skipping missing-file detection"
                    )
                    _dest_index_inc = None

                if _dest_index_inc is not None:
                    # ── Filter: remove files already correct in destination ──────
                    # diff_snapshots can produce false "modified" entries when the
                    # SMB mtime fluctuates slightly (2-second precision) and the
                    # saved snapshot has hash="" (robocopy fast-path skips hashing).
                    # In sync mode the goal is destination == source; if the dest
                    # already has the file at the right size it is already correct —
                    # no need to re-copy 10 GB just because the mtime drifted 1 s.
                    _before_filter = len(files_to_copy)
                    files_to_copy = [
                        c for c in files_to_copy
                        if c["path"] not in _dest_index_inc
                        or _dest_index_inc.get(c["path"], -1) != new_snapshot.get(c["path"], {}).get("size", -2)
                    ]
                    _filtered = _before_filter - len(files_to_copy)
                    if _filtered > 0:
                        logger.info(
                            f"[sync] '{watch_name}': {_filtered} file(s) skipped — "
                            f"destination already has the correct size (SMB mtime drift suppressed)"
                        )

                    # The comprehensive "anything else missing from the
                    # destination" self-heal requires knowing the destination
                    # state for EVERY file, which _dest_index_inc only has on
                    # full scans (not on the fast-path above) — running it here
                    # on a fast incremental run would wrongly treat every
                    # uninspected file as missing and re-queue the entire share.
                    if not _fast_incremental:
                        _already_queued = {c["path"] for c in files_to_copy}
                        _dest_missing = [
                            {
                                "type":     "added",
                                "path":     _rel,
                                "new_hash": _meta["hash"],
                                "size":     _meta["size"],
                                "old_hash": None,
                            }
                            for _rel, _meta in new_snapshot.items()
                            if _rel not in _already_queued
                            and (
                                _rel not in _dest_index_inc
                                or _dest_index_inc[_rel] != _meta["size"]
                            )
                        ]
                        if _dest_missing:
                            logger.info(
                                f"[sync] '{watch_name}': {len(_dest_missing)} file(s) missing "
                                f"from destination — queuing for re-copy"
                            )
                            files_to_copy = files_to_copy + _dest_missing
                            changes       = changes + _dest_missing
        else:
            # ── Sync-mode first-backup fast path ─────────────────────────────
            # In sync mode the destination may already be pre-populated (e.g.
            # user seeded the drive or re-added the watch after a reinstall).
            # Instead of queuing all source files and then seed-skipping them
            # one by one in the copy loop (O(n) individual stat calls over the
            # network), we build a {rel: size} index of the destination and diff
            # it against new_snapshot.  Optimisation: when source and destination
            # are on the same Windows host, skip the rglob and stat only the files
            # we know about — order-of-magnitude faster on large shares.
            if sync_mode and not encrypt_key and compress_level == 0:
                _dest_index: Dict[str, tuple] = {}   # rel_path → (size, mtime)
                _dest_host_fb = _unc_host(str(dest_root))
                _src_host_fb  = _unc_host(str(src_path))
                _same_host_fb = bool(_dest_host_fb and _dest_host_fb == _src_host_fb)
                try:
                    if _same_host_fb:
                        # Fast path: stat only the files in the snapshot, no rglob
                        for _rel_key in new_snapshot:
                            if cancel_event is not None and cancel_event.is_set():
                                raise InterruptedError("Backup cancelled by user")
                            _dp = dest_root / _rel_key
                            try:
                                _st = _dp.stat()
                                _dest_index[_rel_key] = (_st.st_size, _st.st_mtime)
                                logger.debug(
                                    f"[sync] '{watch_name}': dest pre-scan found '{_rel_key}' "
                                    f"({_st.st_size:,} bytes)"
                                )
                            except OSError as _oserr:
                                logger.debug(
                                    f"[sync] '{watch_name}': dest pre-scan: '{_rel_key}' "
                                    f"not found or not accessible ({_oserr}) — will copy"
                                )
                    else:
                        for _dp in dest_root.rglob("*"):
                            if cancel_event is not None and cancel_event.is_set():
                                raise InterruptedError("Backup cancelled by user")
                            if not _dp.is_file() or _dp.is_symlink():
                                continue
                            try:
                                _rel = str(_dp.relative_to(dest_root))
                                _st = _dp.stat()
                                _dest_index[_rel] = (_st.st_size, _st.st_mtime)
                                # NOTE: scan_cb intentionally omitted here — this is
                                # an internal destination pre-scan for sync-mode
                                # first-backup, not source file discovery.
                            except (OSError, ValueError):
                                pass
                except InterruptedError:
                    raise
                except Exception as _de:
                    logger.warning(f"[sync] '{watch_name}': destination pre-scan failed ({_de}) — falling back to full copy")
                    _dest_index = {}   # safe fallback: treat dest as empty → copy everything

                if _dest_index:
                    logger.info(
                        f"[sync] '{watch_name}': destination pre-scan found "
                        f"{len(_dest_index)} file(s) — checking which need copying..."
                    )
                elif new_snapshot:
                    logger.warning(
                        f"[sync] '{watch_name}': destination pre-scan found 0 files "
                        f"(dest may be empty, inaccessible, or all stats failed) — "
                        f"all {len(new_snapshot)} source file(s) will be queued for copy. "
                        f"Check Logs (debug) for per-file stat errors."
                    )

                changes = []
                _skipped_seed = []
                for k, v in new_snapshot.items():
                    _src_size = v["size"]
                    _dest_entry = _dest_index.get(k)
                    if _dest_entry is None:
                        # Not found in destination at all
                        logger.debug(f"[sync] '{watch_name}': queuing '{k}' — not found in destination")
                        changes.append({
                            "type":     "added",
                            "path":     k,
                            "new_hash": v["hash"],
                            "size":     _src_size,
                            "old_hash": None,
                        })
                    else:
                        _dest_size, _dest_mtime = _dest_entry
                        # Primary check: size must match exactly.
                        # Secondary check: if sizes differ but mtime matches within
                        # 5 seconds (SMB timestamp precision — bumped from 2s for
                        # UNC-to-UNC where clocks may drift slightly), treat as
                        # identical — catches SMB rounding/timezone quirks.
                        # Edge case: if snapshot returned size=0 for this file
                        # (partial SMB scan / timeout), don't force a re-copy of
                        # a large file that is clearly already present at the
                        # destination — fall back to mtime-only for size=0 entries.
                        _size_ok  = (_src_size > 0 and _dest_size == _src_size)
                        _src_meta = new_snapshot[k]
                        _src_mtime = _src_meta.get("mtime", 0)
                        _mtime_ok = _src_mtime and abs(_dest_mtime - _src_mtime) <= 5
                        _size_unknown = (_src_size == 0 and _dest_size > 0)
                        if _size_ok or _mtime_ok or _size_unknown:
                            _reason = (
                                "size matches" if _size_ok
                                else f"mtime matches within 5s (src={_src_mtime:.0f} dest={_dest_mtime:.0f}, size src={_src_size} dest={_dest_size})"
                                if _mtime_ok
                                else f"snapshot size=0 (partial SMB scan) — dest has {_dest_size:,} bytes, trusting mtime"
                            )
                            logger.debug(f"[sync] '{watch_name}': skipping '{k}' — already in destination ({_reason})")
                            _skipped_seed.append(k)
                            # Fix: stamp the snapshot with the *destination* mtime so
                            # diff_snapshots on the next incremental run compares a stable
                            # value.  Robocopy preserves the source mtime on the dest file,
                            # but os.stat() over a local/UNC path can return a slightly
                            # different float on each call (FAT/SMB 2-second precision).
                            # Without this, both old and new snapshot entries have hash=""
                            # and the mtime fallback in diff_snapshots may detect a spurious
                            # change, re-queuing these already-present files as "modified"
                            # on the very next backup even though nothing actually changed.
                            if k in new_snapshot and _dest_mtime:
                                new_snapshot[k]["mtime"] = _dest_mtime
                        else:
                            logger.info(
                                f"[sync] '{watch_name}': queuing '{k}' — "
                                f"dest size={_dest_size:,} bytes vs src size={_src_size:,} bytes "
                                f"(dest mtime={_dest_mtime:.0f}, src mtime={_src_mtime:.0f})"
                            )
                            changes.append({
                                "type":     "added",
                                "path":     k,
                                "new_hash": v["hash"],
                                "size":     _src_size,
                                "old_hash": None,
                            })

                if _skipped_seed:
                    logger.info(
                        f"[sync] '{watch_name}': {len(_skipped_seed)} file(s) already in "
                        f"destination — skipped: {', '.join(_skipped_seed[:5])}"
                        + (" ..." if len(_skipped_seed) > 5 else "")
                    )
                if changes:
                    logger.info(
                        f"[sync] '{watch_name}': {len(changes)} file(s) need copying "
                        f"({_human_size(sum(c['size'] for c in changes))})"
                    )
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

        # ── Record only real changes in result ───────────────────────────────
        # `changes` is the raw diff_snapshots() output which can include false
        # "modified" entries when both hashes are "" (robocopy fast-path) and
        # the SMB mtime drifts slightly between snapshots.  The dest-size filter
        # above already removed those from files_to_copy (nothing to re-copy),
        # but they were still in `changes` — causing spurious "modified" rows in
        # History and a phantom pending-change count of 1 after backup.
        #
        # Fix: rebuild the reported change list as:
        #   • all entries that are actually queued for copy (files_to_copy), PLUS
        #   • all "deleted" entries from the diff (never filtered by dest-size check).
        # This means History only shows what was genuinely added/modified/deleted.
        _deleted_changes = [c for c in changes if c.get("type") == "deleted"]
        _copy_paths      = {c["path"] for c in files_to_copy}
        # Avoid double-counting: deletions already in files_to_copy are impossible
        # (deleted files are never queued for copy), but guard anyway.
        _extra_deletes   = [c for c in _deleted_changes if c["path"] not in _copy_paths]
        result["changes"] = files_to_copy + _extra_deletes
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
            f"encrypt={bool(encrypt_key)}, compress={compress_level}"
        )
        # ── Nothing to copy — destination already up to date ─────────────────
        # Signal the UI immediately so it can show "Up to date" instead of
        # staying frozen on "Preparing… X GB to copy" while we write the manifest.
        if total == 0 and progress_cb:
            try:
                progress_cb(0, 0, "\x00uptodate\x00", 0, 0)
            except Exception:
                pass
        # Byte-level progress tracking for accurate ETA on large files.
        # total_bytes is pre-computed from the change list sizes; bytes_done
        # is incremented as each file finishes so the UI can show MB/s & ETA.
        # NOTE: We intentionally do NOT re-stat size=0 entries here.
        # Doing so fires one network round-trip per file before any copying starts,
        # causing a multi-minute stall on large SMB shares before the first byte
        # is copied.  The rglob scan already captured real sizes via stat();
        # any residual size=0 entries are genuinely empty files or edge cases
        # — the UI handles total_bytes gracefully by showing bytes-copied count.
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

        # Remote free-space check — runs before any files are copied so we
        # abort early rather than failing halfway through a large upload.
        if TRANSPORT_AVAILABLE and storage_type not in ("local", ""):
            try:
                _remote_space = check_remote_free_space(storage_type, cfg, needed)
                if not _remote_space.get("ok") and not _remote_space.get("skipped"):
                    result["status"] = "failed"
                    result["error"]  = _remote_space["error"]
                    return result
            except Exception as _rsp_err:
                # Non-fatal — log and continue; the upload itself will surface the real error
                print(f"[backup] ⚠ Remote space check error (non-fatal): {_rsp_err}", flush=True)

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
                elif compress_level > 0:
                    dest_gz = backup_dir / (src_path.name + '.gz')
                    _ensure_writable(dest_gz)
                    with open(str(dest_gz), 'wb') as _raw_gz:
                        _hw = _HashingWriter(_raw_gz)
                        # Use GzipFile instead of gzip.open() — gzip.open() calls
                        # fileno() on the wrapper which fails on Windows (Errno 22).
                        # GzipFile only needs write(), which _HashingWriter provides.
                        with gzip.GzipFile(fileobj=_hw, mode='wb', compresslevel=compress_level) as f_out:
                            with open(source, 'rb') as f_in:
                                _sh.copyfileobj(f_in, f_out)
                    _files_written_hashes[src_path.name + '.gz'] = _hw.hexdigest()
                else:
                    # Chunked copy — computes source hash inline so we never
                    # need a separate hash_file(src) or hash_file(dest) pass.
                    _h_sf = hashlib.sha256()
                    _dest_sf = backup_dir / src_path.name
                    _ensure_writable(_dest_sf)
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
                    logger.debug("[suppressed] Exception ignored near: _fsz = src_path.stat().st_size if src_path.exists() else 0 |                    ", exc_info=True)
                    pass

        # ── Directory source ───────────────────────────────
        else:
            # Choose chunk size based on source/dest path type.
            # UNC network paths benefit greatly from larger chunks:
            # fewer round-trips per file = much higher effective throughput.
            # Local paths use 4 MB (good balance of memory and speed).
            _src_str    = str(source).replace("/", "\\")
            _dest_str   = str(destination).replace("/", "\\")
            _is_network = _src_str.startswith("\\\\") or _dest_str.startswith("\\\\")
            _CHUNK      = 16 * 1024 * 1024 if _is_network else 4 * 1024 * 1024  # 16 MB for network, 4 MB local

            # Same-host optimisation: when both UNC paths share the same server
            # (e.g. \\PC\share1 → \\PC\share2) we can ask the PC to copy the
            # data internally via the Windows CopyFileEx offload API.  The bytes
            # never traverse the network — order-of-magnitude speed improvement.
            _src_host  = _unc_host(_src_str)
            _dest_host = _unc_host(_dest_str)
            _is_same_host = bool(_src_host and _src_host == _dest_host)

            # ── Whole-directory robocopy fast-path ─────────────────────────────
            # robocopy is faster than Python's copy loop for ALL path combinations:
            #
            #   UNC → UNC (same host): robocopy /MT:8 /J — multi-threaded fast path
            #   UNC → local (D:\):     one network hop, unbuffered /J, 8 threads
            #   local → local:         unbuffered /J + /MT:8 vs Python's serial loop
            #   local → UNC:           same as UNC→local in reverse
            #
            # Requires: Windows, no encryption, no compression, no throttle.
            # Falls back to Python per-file loop if robocopy fails or isn't available.
            #
            # Previously same-host UNC skipped robocopy to try CopyFileEx ODX
            # (FSCTL_SRV_COPYCHUNK server-side copy). However ODX only works when
            # the Windows PC explicitly supports it — most configurations
            # don't. When ODX is absent, CopyFileEx falls back to a single-threaded
            # PC-mediated copy (read UNC → write UNC through PC RAM), which is
            # 10-20× slower than robocopy's /MT:8 /J multi-threaded path.
            # Robocopy is now the default for ALL path types including same-host UNC.
            # CopyFileEx ODX is still attempted per-file in the fallback loop below
            # (when robocopy is unavailable or fails for specific files).
            _use_robocopy = (
                os.name == "nt"
                and not encrypt_key
                and compress_level == 0
                and not throttler
                # Same-host UNC now uses robocopy too — identical treatment to
                # local→UNC or UNC→local paths.  Much faster than single-threaded
                # CopyFileEx when the PC does not support ODX/FSCTL_SRV_COPYCHUNK.
            )
            if _is_same_host and os.name == "nt":
                logger.info(
                    f"[same-host] '{watch_name}': source and destination are on the same "
                    f"Windows host ({_src_host}) — using robocopy /MT:8 /J fast-path "
                    f"(same treatment as all other path types)."
                )
            if _use_robocopy:
                # ── Fast exit: nothing to copy on this incremental run ──────────
                # When the incremental diff + sync-dest filter produced an empty
                # files_to_copy list there is literally nothing to write.  Calling
                # _robocopy_dir with an empty list makes it fall through to the
                # full-tree /E scan, which on UNC→UNC can (a) take minutes just to
                # scan and (b) return a non-zero exit code that triggers the slow
                # Python per-file fallback.  Skip both entirely and go straight to
                # finalisation so the UI shows "Up to date" and the backup
                # completes in milliseconds.
                if not files_to_copy:
                    logger.info(
                        f"[robocopy-dir] '{watch_name}': nothing to copy "
                        f"(files_to_copy empty — incremental diff or sync-mode dest already seeded) — skipping robocopy"
                    )
                    if progress_cb:
                        try:
                            progress_cb(0, 1, "\x00uptodate\x00", 1, 1)
                        except Exception:
                            pass
                    # files_to_copy is already [] — per-file loop will be a no-op
                else:
                    _rc_label = (
                        f"same-host Windows server-side copy ({_src_host})" if _is_same_host
                        else f"{_src_str} → {str(backup_dir)}"
                    )
                    logger.info(
                        f"[robocopy-dir] '{watch_name}': using robocopy fast-path "
                        f"(/MT:8 /J) — {_rc_label}"
                    )
                    backup_dir.mkdir(parents=True, exist_ok=True)
                    # Signal the UI immediately so it shows "Copying…" instead of
                    # staying frozen on "Preparing…" for the entire robocopy run.
                    # Emitted via ui_status (BlockingQueuedConnection) so the worker
                    # blocks until the UI has actually painted the update before
                    # robocopy starts — no sleep needed.
                    if progress_cb:
                        try:
                            progress_cb(0, total or 1, "\x00robocopy\x00", 1, total_bytes or 1)
                        except Exception:
                            pass
                    time.sleep(0.15)
                    # For the size-hint queue: pass the full files_to_copy list so
                    # robocopy's per-file percentage lines can be attributed to the
                    # right file sizes.
                    # Pass an explicit files_to_copy list to robocopy when we
                    # have pre-filtered changes (excludes, max size, changed_paths)
                    # so robocopy doesn't fall back to a full-tree /E scan that
                    # would ignore those filters. Keep the previous behaviour of
                    # using the files list for incremental runs too.
                    # FIX: Always pass the explicit file list to robocopy when we
                    # know exactly which files need copying.  Previously this was only
                    # done for incremental runs, so a first/sync backup called robocopy
                    # with /E (copy entire tree) even when only 2 out of 7 files actually
                    # needed copying — robocopy had no file list and re-copied Thumbs.db
                    # and old MANIFEST files that were already present in the destination.
                    # Now we always pass files_to_copy when it is non-empty, so robocopy
                    # copies only the files that the sync-mode diff identified as missing.
                    _rc_files = files_to_copy if files_to_copy else None

                    _rc_dir_ok, _rc_dir_files, _rc_dir_bytes = _robocopy_dir(
                        src=_src_str,
                        dst=str(backup_dir),
                        cancel_event=cancel_event,
                        progress_cb=progress_cb,
                        total_files=total,
                        total_bytes=total_bytes,
                        files_to_copy=_rc_files,
                        size_hints=files_to_copy,  # always pass for byte-accurate progress tracking
                    )
                    if _rc_dir_ok:
                        # Use actual counts reported by robocopy stdout parser.
                        # When total==0 (scan returned empty / incomplete snapshot),
                        # _rc_dir_files captures the real number of files robocopy
                        # processed so the log shows the correct count instead of "0".
                        _effective_copied = _rc_dir_files if _rc_dir_files > 0 else total
                        _effective_bytes  = _rc_dir_bytes  if _rc_dir_bytes  > 0 else total_bytes
                        logger.info(
                            f"[robocopy-dir] '{watch_name}': robocopy completed successfully "
                            f"({_effective_copied} file(s), {_human_size(_effective_bytes)}) — skipping post-copy "
                            f"hash (hashes backfilled on next incremental run)"
                        )
                        # Post-copy hashing: compute hashes for files just written by robocopy
                        # so the returned snapshot contains real SHA-256 values even when
                        # the fast-path was used. This keeps incremental runs deterministic
                        # and avoids relying on a later backfill pass.
                        for entry in files_to_copy:
                            rel = entry["path"]
                            dest_fp = backup_dir / rel
                            try:
                                h = hash_file(str(dest_fp))
                            except Exception:
                                h = ""
                            _files_written_hashes[rel] = h
                            if rel in new_snapshot and h:
                                new_snapshot[rel]["hash"] = h
                        copied     = _effective_copied
                        bytes_done = _effective_bytes
                        if progress_cb:
                            try:
                                # FIX: emit bytes_done == total_bytes so the UI
                                # formula produces pct=100 → "Finalizing…".
                                # The dest-size poll caps at (total_bytes - 1) to
                                # avoid a premature 100% flash mid-copy, but
                                # robocopy has now fully returned — safe to emit
                                # the true 100% here so the bar completes.
                                _final_total = max(total_bytes, bytes_done) or 1
                                progress_cb(copied, max(total, copied) or 1, "", _final_total, _final_total)
                            except Exception:
                                pass
                        # Skip the per-file loop entirely
                        files_to_copy = []
                    else:
                        # If the user cancelled during robocopy, honour it immediately —
                        # do NOT fall through to the Python/parallel copy loop.
                        if cancel_event is not None and cancel_event.is_set():
                            raise InterruptedError("Backup cancelled by user")
                        logger.warning(
                            f"[robocopy-dir] '{watch_name}': robocopy fast-path failed "
                            f"— falling back to Python per-file copy loop"
                            + (" (parallel mode: 4 threads)" if _is_same_host and not encrypt_key and compress_level == 0 and not throttler else "")
                        )
                        # ── Parallel copy fast-path (UNC→UNC, no encrypt/compress/throttle) ──
                        # When robocopy fails for a same-host UNC copy, run 4 concurrent
                        # copy threads instead of the serial loop below.  On a Gigabit LAN
                        # this typically 3-4× throughput vs single-threaded Python I/O.
                        if _is_same_host and not encrypt_key and compress_level == 0 and not throttler and files_to_copy:
                            logger.info(
                                f"[parallel-copy] '{watch_name}': using 4-thread parallel "
                                f"copy for UNC→UNC fallback ({len(files_to_copy)} file(s))"
                            )
                            _pc_copied, _pc_bytes, _pc_failed = _parallel_copy_files(
                                entries=files_to_copy,
                                src_root=src_path,
                                dst_root=backup_dir,
                                cancel_event=cancel_event,
                                progress_cb=progress_cb,
                                total_files=total,
                                total_bytes=total_bytes,
                                chunk_size=_CHUNK,
                                max_workers=4,
                            )
                            for entry in files_to_copy:
                                if entry not in _pc_failed:
                                    _files_written_hashes[entry["path"]] = ""
                            for entry in _pc_failed:
                                result["failed_files"].append({
                                    "path":   entry["path"],
                                    "reason": "parallel copy failed — see log for details",
                                })
                            copied     += _pc_copied
                            bytes_done += _pc_bytes
                            if progress_cb:
                                try:
                                    # FIX: same as robocopy path — emit true 100%
                                    _final_total = max(total_bytes, bytes_done) or 1
                                    progress_cb(copied, max(total, copied) or 1, "", _final_total, _final_total)
                                except Exception:
                                    pass
                            # Skip the serial per-file loop
                            files_to_copy = []

            # ── Emit initial "Copying…" signal before the per-file loop ──────
            # The robocopy fast-path emits a "\x00robocopy\x00" sentinel above so
            # the UI transitions from "Preparing…" to "Copying…" immediately.
            # The per-file CopyFileEx path (same-host UNC) has no equivalent
            # signal, so the UI stays frozen on "Preparing…" until the first
            # file's progress callback fires — which for server-side UNC copies
            # (FSCTL_SRV_COPYCHUNK) may never happen until the file is done.
            # Emitting bytes_done=1 here guarantees the UI moves to "Copying…"
            # the moment the copy loop begins, regardless of callback frequency.
            if files_to_copy and progress_cb:
                try:
                    progress_cb(0, total or 1, files_to_copy[0]["path"], 1, total_bytes or 1)
                except Exception:
                    pass

            for entry in files_to_copy:
                # ── Pause: wait if pause_event is not set, but keep polling
                # cancel_event so a cancel issued while paused takes effect
                # immediately instead of hanging forever behind the pause. ──
                if pause_event is not None and not pause_event.is_set():
                    while not pause_event.is_set():
                        if cancel_event is not None and cancel_event.is_set():
                            raise InterruptedError("Backup cancelled by user")
                        pause_event.wait(timeout=0.25)

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
                # path-based destination types (local, UNC, mounted SFTP,
                # etc.) because the check is purely filesystem-level.
                #
                # Not applied for encrypt/compress because the destination file
                # format differs from the source (we cannot compare sizes).
                if sync_mode and not encrypt_key and compress_level == 0:
                    if _dest_already_identical(src_file, dest_file):
                        # Seed-skip: dest already has same-size file — skip re-copy.
                        # Do NOT call hash_file(src_file) here: for network sources
                        # that reads the entire file over the wire just to store a hash,
                        # turning a 0-work skip into a full network read (2 MB/s for 10 GB).
                        # Store "" and let diff_snapshots use mtime+size for change detection;
                        # the next real copy will backfill the hash inline at no extra cost.
                        try:
                            _seed_hash = ""  # skip expensive hash_file read over network
                            if entry["path"] in new_snapshot:
                                new_snapshot[entry["path"]]["hash"] = _seed_hash
                            _files_written_hashes[entry["path"]] = _seed_hash
                        except Exception:
                            pass
                        copied     += 1
                        bytes_done += entry.get("size", 0)
                        if progress_cb:
                            try:
                                # Prefix with sentinel so the UI can show
                                # "Verifying" instead of "Copying" — the file
                                # already exists at the destination unchanged.
                                progress_cb(copied, total or 1,
                                            "\x00verify\x00" + entry["path"],
                                            bytes_done, total_bytes)
                            except InterruptedError:
                                raise
                            except Exception:
                                logger.debug('[suppressed] Exception ignored.', exc_info=True)
                                pass
                        if copied == 1:
                            logger.info(
                                f"[seed-skip] '{watch_name}': destination already has "
                                f"identical files — skipping unchanged (size match). "
                                f"Only new/modified files will be copied."
                            )
                        continue

                # ── Versioned-mode pre-seed optimisation ───────────────────
                # If dest_root already has this file with identical content (same
                # size AND same SHA-256 hash), copy it locally from dest_root to
                # backup_dir instead of re-reading from the source.  The versioned
                # backup still gets a complete copy in backup_dir (correct), but
                # we avoid a slow re-read from a potentially remote/network source.
                # Hash comparison (not size-only) ensures same-size but different-
                # content files are correctly detected and read from source.
                # Only applied on first backup and when no encryption/compression.
                if (not sync_mode and not encrypt_key and compress_level == 0
                        and not previous_snapshot):  # first backup only
                    _preseed_file = dest_root / entry["path"]
                    if _dest_already_identical(src_file, _preseed_file):
                        try:
                            # Hash both files to confirm content truly identical.
                            # Size matched above; this catches same-size/different-content.
                            _preseed_hash = hash_file(str(_preseed_file), cancel_event=cancel_event)
                            _src_hash_ps  = hash_file(str(src_file),      cancel_event=cancel_event)
                            if _preseed_hash and _src_hash_ps and _preseed_hash == _src_hash_ps:
                                # Content confirmed identical — copy locally (fast)
                                dest_file.parent.mkdir(parents=True, exist_ok=True)
                                shutil.copy2(str(_preseed_file), str(dest_file))
                                if entry["path"] in new_snapshot:
                                    new_snapshot[entry["path"]]["hash"] = _preseed_hash
                                _files_written_hashes[entry["path"]] = _preseed_hash
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
                                        f"[preseed] '{watch_name}': destination already has "
                                        f"identical files (size+hash match) — copying locally "
                                        f"instead of re-reading from source."
                                    )
                                continue
                            # Hashes differ — same size, different content.
                            # Fall through to normal source copy so correct file is backed up.
                        except (OSError, PermissionError) as _pe:
                            logger.debug(f"[preseed] Check failed for {entry['path']}: {_pe} — falling back to source read")
                            # Fall through to normal source copy below

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
                                logger.debug("[suppressed] Exception ignored near: if free < 100 * 1024 * 1024: |                                     raise OSError", exc_info=True)
                                pass

                        if encrypt_key and CRYPTO_AVAILABLE:
                            _files_written_hashes[entry["path"]] = _encrypt_file(str(src_file), str(dest_file), encrypt_key)
                        elif compress_level > 0:
                            dest_gz = Path(str(dest_file) + '.gz')
                            dest_gz.parent.mkdir(parents=True, exist_ok=True)
                            # Chunked compress so cancel is checked periodically.
                            # _HashingWriter intercepts every byte gzip writes to disk
                            # so we get the output hash without a second read of dest_gz.
                            _cmp_chunk_bytes = 0
                            _ensure_writable(dest_gz)
                            with open(str(dest_gz), 'wb') as _raw_gz:
                                _hw = _HashingWriter(_raw_gz)
                                # Use GzipFile instead of gzip.open() — gzip.open() calls
                                # fileno() on the wrapper which fails on Windows (Errno 22).
                                # GzipFile only needs write(), which _HashingWriter provides.
                                with gzip.GzipFile(fileobj=_hw, mode='wb', compresslevel=compress_level) as f_out:
                                    with open(str(src_file), 'rb') as f_in:
                                        while True:
                                            # ── Pause: wait if pause_event is not set, but keep polling
                                            # cancel_event so a cancel issued while paused takes effect
                                            # immediately instead of hanging forever behind the pause. ──
                                            if pause_event is not None and not pause_event.is_set():
                                                while not pause_event.is_set():
                                                    if cancel_event is not None and cancel_event.is_set():
                                                        raise InterruptedError("Backup cancelled by user")
                                                    pause_event.wait(timeout=0.25)

                                            chunk = f_in.read(_CHUNK)
                                            if not chunk:
                                                break
                                            f_out.write(chunk)
                                            _cmp_chunk_bytes += len(chunk)
                                            if throttler:
                                                try:
                                                    throttler.throttle(len(chunk))
                                                except Exception:
                                                    logger.debug("[suppressed] Exception ignored near: _cmp_chunk_bytes += len(chunk) |                                             if ", exc_info=True)
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
                                                    logger.debug('[suppressed] Exception ignored.', exc_info=True)
                                                    pass
                            dest_file = dest_gz
                            _files_written_hashes[entry["path"] + ".gz"] = _hw.hexdigest()
                            # Use actual bytes read (not snapshot size) so bytes_done
                            # stays accurate even if the file grew since the snapshot.
                            _cmp_actual_size = _cmp_chunk_bytes
                        else:
                            # ── Fast-path hierarchy for same-host UNC copies ─────
                            # Priority: CopyFileEx (ctypes/pywin32) → robocopy → chunked Python
                            #
                            # CopyFileEx: asks the OS to issue FSCTL_SRV_COPYCHUNK so the
                            # Windows server-side copy — bytes stay on the same host.
                            # Now uses ctypes (no pywin32 required), with pywin32 as backup.
                            #
                            # robocopy: Windows built-in tool, unbuffered I/O (/J), can also
                            # trigger server-side copy and has better buffer management than
                            # Python's read→write loop.  Used when CopyFileEx is not available
                            # or the PC doesn't support FSCTL_SRV_COPYCHUNK.
                            #
                            # Chunked Python loop: always available fallback; slowest for
                            # UNC→UNC because every byte travels PC RAM → remote over the network.
                            _used_server_copy  = False
                            _used_robocopy     = False
                            if _is_same_host and not throttler:  # UNC→UNC same-host: use CopyFileEx offload or robocopy (avoids slow Python read→write loop)
                                # ── Step 1: CopyFileEx (ctypes-first, no pywin32 needed) ──
                                # Run in a background thread so cancel_event is honoured
                                # while the native copy proceeds.
                                _ensure_writable(dest_file)
                                import threading as _threading
                                _copy_result = [False]
                                _server_bytes_written = [0]

                                def _native_progress_cb(written, total):
                                    _server_bytes_written[0] = written

                                def _do_server_copy():
                                    _copy_result[0] = _server_side_copy(
                                        str(src_file), str(dest_file),
                                        progress_cb=_native_progress_cb,
                                    )
                                _ct = _threading.Thread(target=_do_server_copy, daemon=True)
                                _ct.start()
                                while _ct.is_alive():
                                    _ct.join(timeout=0.25)
                                    if progress_cb:
                                        try:
                                            _written = _server_bytes_written[0]
                                            if _written == 0:
                                                # CopyFileEx server-side (FSCTL_SRV_COPYCHUNK): the PC
                                                # copies data internally so the progress callback may
                                                # never fire until completion.  Emit the server-copy
                                                # sentinel so the UI shows a pulsing indeterminate bar
                                                # + "Copying… (server-side)" instead of a
                                                # frozen "0.00%" bar.
                                                progress_cb(
                                                    copied, total or 1,
                                                    "\x00smb_copy\x00" + entry["path"],
                                                    0, total_bytes or 1,
                                                )
                                            else:
                                                # CopyFileEx is reporting real byte progress —
                                                # switch to the normal percentage bar.
                                                progress_cb(
                                                    copied, total or 1, entry["path"],
                                                    bytes_done + _written, total_bytes,
                                                )
                                        except InterruptedError:
                                            raise
                                        except Exception:
                                            pass
                                    if cancel_event is not None and cancel_event.is_set():
                                        raise InterruptedError("Backup cancelled by user")
                                _used_server_copy = _copy_result[0]
                                if _used_server_copy:
                                    if copied == 0:
                                        logger.info(
                                            f"[same-host] '{watch_name}': source and destination "
                                            f"are on the same host ({_src_host}) — using "
                                            f"CopyFileEx offload (no pywin32 needed)"
                                        )
                                    _src_hash    = hash_file(str(src_file), cancel_event=cancel_event) or ""
                                    _chunk_bytes = entry.get("size", 0)
                                    try:
                                        shutil.copystat(str(src_file), str(dest_file))
                                    except (PermissionError, OSError):
                                        pass
                                else:
                                    # ── Step 2: robocopy (Windows built-in) ───────────
                                    if copied == 0:
                                        logger.info(
                                            f"[same-host] '{watch_name}': CopyFileEx unavailable "
                                            f"or failed — trying robocopy for UNC-to-UNC copy "
                                            f"(host: {_src_host})"
                                        )
                                    _ensure_writable(dest_file)
                                    _used_robocopy = _robocopy_file(
                                        str(src_file), str(dest_file),
                                        cancel_event=cancel_event,
                                        progress_cb=progress_cb,
                                        copied_files=copied,
                                        total_files=total,
                                        bytes_done_offset=bytes_done,
                                        total_bytes=total_bytes,
                                    )
                                    if _used_robocopy:
                                        if copied == 0:
                                            logger.info(
                                                f"[same-host] '{watch_name}': using robocopy "
                                                f"(unbuffered /J mode) for UNC-to-UNC copy"
                                            )
                                        _src_hash    = hash_file(str(src_file), cancel_event=cancel_event) or ""
                                        _chunk_bytes = entry.get("size", 0)
                                        try:
                                            shutil.copystat(str(src_file), str(dest_file))
                                        except (PermissionError, OSError):
                                            pass
                                    else:
                                        if copied == 0:
                                            logger.warning(
                                                f"[same-host] '{watch_name}': CopyFileEx and robocopy "
                                                f"both unavailable — falling back to Python chunked "
                                                f"copy (slower: data travels PC↔PC over network). "
                                                f"Install pywin32 for server-side copy offload."
                                            )

                            if not _used_server_copy and not _used_robocopy:
                            # Cancellable chunked copy — checks cancel every chunk.
                            # Also computes source SHA-256 inline so we never need
                            # a separate hash_file(src) pass (saves one full read).
                            # Throttling is done per-chunk (inside this loop) so
                            # large files are paced evenly and progress_cb fires
                            # regularly — no multi-minute gaps from a single
                            # post-file throttle call.
                                _h_src = hashlib.sha256()
                                _chunk_bytes = 0  # bytes written so far for this file
                                _ensure_writable(dest_file)
                                # Use explicit large buffer for UNC/network destinations.
                                # Python's default 8 KB buffer causes excessive syscalls
                                # over network — batching at _CHUNK size matches the read size
                                # and maximises throughput on both local and network paths.
                                _write_buf = _CHUNK
                                with open(str(src_file), 'rb', buffering=_CHUNK) as f_in, open(str(dest_file), 'wb', buffering=_write_buf) as f_out:
                                    while True:
                                        # ── Pause: wait if pause_event is not set, but keep polling
                                        # cancel_event so a cancel issued while paused takes effect
                                        # immediately instead of hanging forever behind the pause. ──
                                        if pause_event is not None and not pause_event.is_set():
                                            while not pause_event.is_set():
                                                if cancel_event is not None and cancel_event.is_set():
                                                    raise InterruptedError("Backup cancelled by user")
                                                pause_event.wait(timeout=0.25)

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
                                                logger.debug("[suppressed] Exception ignored near: # Throttle per-chunk for smooth, even pacing |                                  ", exc_info=True)
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
                                                logger.debug('[suppressed] Exception ignored.', exc_info=True)
                                                pass
                                _src_hash = _h_src.hexdigest()
                                # Preserve original timestamps (shutil.copy2 behaviour).
                                # Wrapped in its own try/except: SMB sources can raise
                                # PermissionError or OSError when reading metadata (mtime,
                                # permissions) even after the file bytes were written
                                # successfully.  A metadata failure must NOT discard the
                                # successfully written file — only log a warning and continue.
                                # Skip copystat for UNC/network destinations —
                                # the extra metadata round-trip adds latency per file.
                                if not str(dest_file).startswith("\\\\") and not str(dest_file).startswith("//"):
                                    try:
                                        shutil.copystat(str(src_file), str(dest_file))
                                    except (PermissionError, OSError) as _cs_err:
                                        logger.warning(
                                            f"[copystat] '{watch_name}': could not copy metadata "
                                            f"for {entry['path']} ({_cs_err}) — file data was "
                                            f"written successfully; timestamps may differ."
                                        )
                            # Lightweight integrity check: verify dest file size matches
                            # source size. We do NOT re-read the dest file to recompute
                            # its hash — that would double the network traffic on UNC
                            # sources. The src hash was already computed inline during copy.
                            try:
                                _dest_size = Path(str(dest_file)).stat().st_size
                                _src_size  = entry.get("size", 0)
                                if _src_size > 0 and _dest_size != _src_size:
                                    logger.warning(
                                        f"⚠ Size mismatch for {entry['path']} "
                                        f"(src={_src_size}, dest={_dest_size}) — "
                                        f"file may have changed during backup or copy was truncated"
                                    )
                                    # Remove the partial/mismatched destination file so it
                                    # is not mistaken for a valid backup on the next run.
                                    try:
                                        Path(str(dest_file)).unlink(missing_ok=True)
                                    except OSError as _ul_err:
                                        logger.debug(f"[cleanup] Could not remove partial dest file: {_ul_err}")
                                    result["failed_files"].append({
                                        "path":   entry["path"],
                                        "reason": f"Size mismatch after copy (expected {_src_size}, got {_dest_size}) — partial file removed",
                                    })
                                    continue
                            except OSError as _st_err:
                                # dest stat failed — warn but do not discard the file;
                                # the write loop completed without error so the data is likely intact.
                                logger.warning(
                                    f"[integrity] Could not stat dest file {entry['path']} "
                                    f"after copy ({_st_err}) — assuming write was successful"
                                )
                            # Backfill the snapshot with the hash computed during copy.
                            # This is especially important on first backup where build_snapshot
                            # skipped hashing — without this the next backup would re-hash
                            # every file because all hashes would be empty strings.
                            _rel_key = entry["path"]
                            if _rel_key in new_snapshot:
                                new_snapshot[_rel_key]["hash"] = _src_hash
                            _files_written_hashes[entry["path"]] = _src_hash

                        copied     += 1
                        # Use actual bytes written (_chunk_bytes for plain copies) not
                        # the snapshot size estimate — they differ when the source file
                        # changed size on the remote PC during the copy (grew or was truncated).
                        # encrypt/compress paths track bytes differently so fall back to
                        # snapshot size there (same as before).
                        if not encrypt_key and compress_level == 0:
                            bytes_done += _chunk_bytes
                        else:
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
                        logger.debug('[suppressed] Exception ignored.', exc_info=True)
                        pass

            # ── Emit true 100% now that all files are copied ─────────────────
            # The dest-size poll caps at (total_bytes - 1) throughout the copy
            # to avoid a premature 100% flash.  Now that the per-file loop is
            # done, emit bytes_done == total_bytes so the UI transitions from
            # "99% Copying…" → "100% Finalizing…" immediately.
            if progress_cb and total_bytes > 0:
                try:
                    _loop_final = max(total_bytes, bytes_done) or 1
                    progress_cb(copied, max(total, copied) or 1, "", _loop_final, _loop_final)
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
                            logger.debug("[suppressed] Exception ignored near: try: |                             marker.touch() |                             ", exc_info=True)
                            pass

        # ── Integrity hash ───────────────────
        # Build the directory hash from _files_written_hashes — the set of files
        # actually written to backup_dir during this run.  This is correct for
        # both full backups (all files) and incremental backups (only changed
        # files + .DELETED markers), and matches exactly what validate_backup
        # computes via rglob.  Using new_snapshot was wrong for incrementals:
        # it contains ALL source files, but backup_dir only holds changed ones.
        #
        # In sync mode the hash is never written to disk (BACKUP.sha256 is
        # skipped to avoid polluting the live destination), so we skip the
        # expensive hash_directory() fallback entirely — it would re-scan the
        # whole destination over the network for no benefit.
        if sync_mode:
            backup_hash = ""
        else:
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
            backup_hash = _bh.hexdigest() if _any_hash else hash_directory(str(backup_dir))

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

        # Use actual bytes written (bytes_done) for the reported size so the
        # manifest and UI always show what was truly copied — not the pre-scan
        # snapshot estimate (total_bytes).  Previously total_bytes was used,
        # which meant if the source file changed size mid-copy (e.g. was
        # truncated or replaced), the app would still report the old snapshot
        # size (e.g. "10.0 GB") even though only 64 MB actually landed in the
        # destination.  bytes_done is the authoritative count of bytes written.
        # For compressed backups we still do the disk scan (output size differs
        # from source size and bytes_done tracks pre-compression source bytes).
        if compress_level > 0:
            total_size = _safe_size(str(backup_dir))
        else:
            # Always use actual bytes written — never fall back to the pre-scan
            # estimate (total_bytes).  Previously the fallback meant that when
            # 0 files were copied (e.g. all failed with OSError) the UI still
            # reported the full source size (e.g. "10.0 GB") even though nothing
            # landed in the destination.  bytes_done is the authoritative count.
            total_size = bytes_done
        duration       = round(time.time() - started, 2)
        throughput_mbs = (total_size / (1024 * 1024)) / max(duration, 0.1) if duration > 0 else 0

        # Calculate compression ratio
        compression_ratio = 0.0
        if compress_level > 0:
            uncompressed_est = sum(c.get("size", 0) for c in changes)
            actual_size = total_size  # already computed above for compress case
            if uncompressed_est > 0:
                compression_ratio = round((1 - actual_size / uncompressed_est) * 100, 1)

        # ── Remote destination upload ──────────────────────────────────────────
        # Handles multiple destinations: cloud (GDrive), sftp, ftp, https, webdav.
        def _upload_to_destination(backup_dir_path: str, dest_config: dict, progress_cb, allowed_rel_paths=None) -> dict:
            """Upload backup to a single destination."""
            dest_type = dest_config.get("_dest_type", "")
            if dest_type == "gdrive":
                provider = dest_config.get("provider", "gdrive")
                if _is_gdrive_ready(dest_config):
                    return upload_to_gdrive(backup_dir_path, dest_config, allowed_rel_paths=allowed_rel_paths)
                else:
                    return {"ok": False, "error": f"Not connected  use the Connect button for {provider}"}
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
            elif dest_type == "https" and TRANSPORT_AVAILABLE:
                https_cfg = dest_config.get("https_config") or dest_config
                return upload_to_https(backup_dir_path, https_cfg, progress_cb=progress_cb)
            elif dest_type == "webdav" and TRANSPORT_AVAILABLE:
                from transport_utils import upload_to_webdav
                webdav_cfg = dest_config.get("webdav_config") or dest_config
                return upload_to_webdav(backup_dir_path, webdav_cfg, progress_cb=progress_cb)
            elif dest_type == "rclone" and TRANSPORT_AVAILABLE:
                rclone_cfg = dest_config.get("rclone_config") or dest_config
                return upload_to_rclone(backup_dir_path, rclone_cfg, progress_cb=progress_cb)
            else:
                return {"ok": False, "error": f"Unsupported destination type: {dest_type}"}

        def _upload_progress(bytes_done, total_bytes, filename):
            if progress_cb:
                try:
                    # Tag with \x00upload\x00 prefix so the UI can distinguish
                    # a real cloud upload from a single-file local/SMB copy.
                    # (The old heuristic current==1 and total==1 was a false
                    # positive for any backup that has exactly one file.)
                    progress_cb(0, total or 1, "\x00upload\x00" + (filename or ""), bytes_done, total_bytes)
                except Exception:
                    logger.debug("[suppressed] Exception ignored near: def _upload_progress(bytes_done, total_bytes, filename): |             if progre", exc_info=True)
                    pass

        # Build the set of relative paths that legitimately belong to this backup.
        # Derived from new_snapshot (the full source file list), adjusted for
        # compression (dest files get a .gz suffix when compress_level > 0).
        # Passing this to upload_to_gdrive prevents stray folders/files that happen
        # to exist in the backup destination (e.g. Monthly/Weekly dirs left over
        # from a different tool) from being mirrored to GDrive.
        _backup_rel_paths: Optional[set] = None
        if new_snapshot:
            if compress_level > 0:
                _backup_rel_paths = {k + ".gz" for k in new_snapshot.keys()}
            else:
                _backup_rel_paths = set(new_snapshot.keys())

        cloud_upload_results = []
        cloud_upload_result = None

        # Skip cloud upload entirely when nothing was copied — avoids unnecessary
        # network round-trips (token refresh + folder listing) on every scheduled
        # backup that finds no changes, which was making each run feel slow.
        _skip_upload = (copied == 0 and not sync_mode)
        if _skip_upload:
            logger.info(f"☁ Skipping cloud upload for {watch_name} — no files copied")

        if not _skip_upload and destinations:
            # Multi-destination mode
            for dest in destinations:
                dest_type = dest.get("dest_type")
                dest_config = dest.get("config", {})
                dest_config["_dest_type"] = dest_type
                logger.info(f" Uploading to {dest_type}: {watch_name}")
                upload_result = _upload_to_destination(str(backup_dir), dest_config, _upload_progress, allowed_rel_paths=_backup_rel_paths)
                cloud_upload_results.append({"dest_type": dest_type if dest_type != "cloud" else "gdrive", **upload_result})
        elif not _skip_upload:
            # Legacy single destination mode
            cloud_upload_result = None
            _dest_type = (cloud_config or {}).get("_dest_type", "") or storage_type

            if _dest_type == "gdrive" and cloud_config:
                provider = cloud_config.get("provider", "gdrive")
                logger.info(f" Uploading backup to gdrive ({provider}): {watch_name}")
                if _is_gdrive_ready(cloud_config):
                    cloud_upload_result = upload_to_gdrive(str(backup_dir), cloud_config, allowed_rel_paths=_backup_rel_paths)
                else:
                    cloud_upload_result = {"ok": False, "error": f"Not connected  use the Connect button for {provider}"}

            elif _dest_type == "sftp" and TRANSPORT_AVAILABLE:
                sftp_cfg = (cloud_config or {}).get("sftp_config") or cloud_config or {}
                logger.info(f"📡 Uploading backup via SFTP: {watch_name}")
                cloud_upload_result = upload_to_sftp(str(backup_dir), sftp_cfg,
                                                     progress_cb=_upload_progress,
                                                     verify=verify_remote_upload)

            elif _dest_type == "ftp" and TRANSPORT_AVAILABLE:
                ftp_cfg = (cloud_config or {}).get("ftp_config") or cloud_config or {}
                logger.info(f"📡 Uploading backup via FTP: {watch_name}")
                cloud_upload_result = upload_to_ftp(str(backup_dir), ftp_cfg,
                                                    progress_cb=_upload_progress,
                                                    verify=verify_remote_upload)

            elif _dest_type == "ftps" and TRANSPORT_AVAILABLE:
                ftp_cfg = dict((cloud_config or {}).get("ftp_config") or cloud_config or {})
                ftp_cfg["use_tls"] = True  # enforce TLS for FTPS
                logger.info(f"📡 Uploading backup via FTPS: {watch_name}")
                cloud_upload_result = upload_to_ftp(str(backup_dir), ftp_cfg,
                                                    progress_cb=_upload_progress,
                                                    verify=verify_remote_upload)


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
                                                       progress_cb=_upload_progress,
                                                       verify=verify_remote_upload)

            elif _dest_type == "rclone" and TRANSPORT_AVAILABLE:
                rclone_cfg = (cloud_config or {}).get("rclone_config") or cloud_config or {}
                logger.info(f"📡 Uploading backup via rclone: {watch_name}")
                cloud_upload_result = upload_to_rclone(str(backup_dir), rclone_cfg,
                                                       progress_cb=_upload_progress)

            if cloud_upload_result:
                cloud_upload_results.append({"dest_type": _dest_type, **cloud_upload_result})

        # Report failures
        for res in cloud_upload_results:
            if not res.get("ok", False):
                logger.warning(f"⚠ Remote upload failed ({res.get('dest_type', '?')}): {res.get('error', 'Unknown error')}")
            else:
                logger.info(f"☁ Remote upload complete ({res.get('dest_type', '?')}): {res.get('uploaded', '?')} files uploaded")
                # Surface post-upload checksum warnings
                warn_list = res.get("warnings") or res.get("verify_warnings", [])
                if warn_list:
                    for w_msg in warn_list:
                        logger.warning(f"⚠ Remote verify ({res.get('dest_type', '?')}): {w_msg}")
                    # Bubble warnings up so the UI can display them
                    res["verify_warnings"] = warn_list

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
            "skipped_symlinks":   result.get("skipped_symlinks", []),
            "destinations_upload": cloud_upload_results,  # NEW: persisted in manifest
            "triggered_by":       triggered_by or "auto",
        }

        # In sync mode, backup_dir IS the live destination — we cannot write
        # MANIFEST.json there as it would pollute the user's backed-up files.
        # Instead, write a lightweight manifest stub into a hidden metadata
        # subfolder (.backupsys_meta) so the app can track backup history,
        # show the last-backup time, and count backups correctly.
        if sync_mode:
            try:
                _meta_dir = dest_root / ".backupsys_meta"
                _meta_dir.mkdir(parents=True, exist_ok=True)
                # Hide the metadata folder so it doesn't clutter the user's view
                # in Windows Explorer.  Works on Windows (SMB share or local); is a
                # no-op on Linux/Mac targets where FILE_ATTRIBUTE_HIDDEN has no meaning.
                try:
                    import ctypes as _ct
                    _ct.windll.kernel32.SetFileAttributesW(
                        str(_meta_dir),
                        0x02 | 0x04,  # FILE_ATTRIBUTE_HIDDEN | FILE_ATTRIBUTE_SYSTEM
                    )
                except Exception:
                    pass  # Non-Windows host or permission denied — silently ignore
                # Use timestamp as subfolder name (same convention as versioned mode)
                _sync_manifest_dir = _meta_dir / datetime.now().strftime("%Y%m%d_%H%M%S_%f")
                _sync_manifest_dir.mkdir(parents=True, exist_ok=True)
                _SYNC_KEYS = {
                    "backup_id", "watch_id", "watch_name", "source",
                    "timestamp", "status", "incremental", "compressed",
                    "encrypted", "files_copied", "duration_s",
                    "throughput_mbs", "total_size_bytes", "triggered_by",
                }
                _sync_stub = {k: v for k, v in manifest.items() if k in _SYNC_KEYS}
                _sync_stub["sync_mode"] = True
                _sync_stub["backup_dir"] = str(_sync_manifest_dir)
                _sync_manifest_path = _sync_manifest_dir / "MANIFEST.json"
                with open(_sync_manifest_path, "w", encoding="utf-8") as _f:
                    json.dump(_sync_stub, _f, indent=2)
                logger.debug(f"[sync] Wrote metadata stub to {_sync_manifest_path}")
            except Exception as _sm_err:
                logger.warning(f"[sync] Could not write metadata stub: {_sm_err}")

        if not sync_mode:
            manifest_path = backup_dir / "MANIFEST.json"

            if encrypt_key and CRYPTO_AVAILABLE:
                # ── Encrypted manifest ────────────────────────────────────────
                # Write the full manifest (including file paths and per-file
                # hashes in "snapshot" and "changes") as an encrypted blob so
                # that anyone with access to the backup destination cannot read
                # the directory tree of a supposedly encrypted backup.
                try:
                    manifest_json = json.dumps(manifest, indent=2).encode()
                    enc_blob = _encrypt_bytes(manifest_json, encrypt_key)
                    enc_path = backup_dir / "MANIFEST.json.enc"
                    enc_path.write_bytes(enc_blob)
                except Exception as _enc_err:
                    logger.warning("Could not encrypt MANIFEST — falling back to plaintext: %s", _enc_err)
                    with open(manifest_path, "w") as f:
                        json.dump(manifest, f, indent=2)
                else:
                    # Write a lightweight stub so BackupIndex (and old clients)
                    # can still enumerate backups without the encryption key.
                    # Sensitive fields (snapshot, changes) are intentionally omitted.
                    _STUB_KEYS = {
                        "backup_id", "watch_id", "watch_name", "source",
                        "timestamp", "status", "incremental", "compressed",
                        "encrypted", "compression_ratio", "files_copied",
                        "duration_s", "throughput_mbs", "total_size_bytes",
                        "triggered_by",
                    }
                    stub = {k: v for k, v in manifest.items() if k in _STUB_KEYS}
                    stub["__note"] = (
                        "Sensitive fields (file paths, per-file hashes) are stored "
                        "in MANIFEST.json.enc — decrypt with the watch encryption key."
                    )
                    with open(manifest_path, "w") as f:
                        json.dump(stub, f, indent=2)
            else:
                with open(manifest_path, "w") as f:
                    json.dump(manifest, f, indent=2)

            # Verify MANIFEST (always check the plaintext file — stub or full)
            try:
                with open(manifest_path) as f:
                    json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                raise RuntimeError(f"MANIFEST.json was not written correctly — backup is corrupted: {e}")

        # ── Update result dict with all final stats ──────────────────
        # Determine final status: if there were files to copy but none succeeded,
        # report "partial_failure" instead of "success" so the caller can surface
        # this clearly to the user (e.g. all files hit OSError/PermissionError).
        _final_status = "success"
        if total > 0 and copied == 0 and result.get("failed_files"):
            _final_status = "partial_failure"
        elif total > 0 and 0 < copied < total and result.get("failed_files"):
            _final_status = "partial_failure"

        result.update({
            "status":            _final_status,
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

        _sym_count = len(result.get("skipped_symlinks", []))
        if _sym_count:
            logger.warning(
                f"⚠ '{watch_name}': {_sym_count} symlink(s) were skipped and are NOT included "
                f"in this backup. Check the log for the full list or review the backup result's "
                f"'skipped_symlinks' field."
            )

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
                logger.debug('[suppressed] Exception ignored.', exc_info=True)
                pass
        elif backup_dir is not None and Path(backup_dir).exists():
            shutil.rmtree(str(backup_dir), ignore_errors=True)
            logger.info(f"🗑 Removed partial backup dir after failure: {backup_dir}")

    finally:
        # Always clean up the VSS shadow copy, even on success or exception.
        if _vss_shadow_id:
            _vss_delete_snapshot(_vss_shadow_id)
        # Clean up the remote-source temp dir downloaded earlier.
        if _remote_source_temp:
            try:
                import shutil as _sh
                _sh.rmtree(_remote_source_temp, ignore_errors=True)
            except Exception:
                logger.debug("[suppressed] Exception ignored near: if _remote_source_temp: |             try: |                 import shutil as _s", exc_info=True)
                pass
        # On success: remove any stale resume checkpoint for this watch.
        if result.get("status") == "success":
            try:
                if _resume_path.exists():
                    _resume_path.unlink()
            except Exception:
                logger.debug('[suppressed] Exception ignored.', exc_info=True)
                pass
        _backup_index.invalidate(destination)

    # ── Post-backup hook ───────────────────────────────────────────────────────
    if post_backup_cmd:
        result_cmd = subprocess.run(post_backup_cmd, shell=True, capture_output=True, text=True, creationflags=_WIN_NO_WINDOW)
        if result_cmd.returncode != 0:
            logger.warning(f"Post-backup command failed (exit {result_cmd.returncode}): {result_cmd.stderr}")

    # ── Post-backup verification (test-restore to temp) ─────────────────────
    result["verify_errors"] = []
    result["verify_ok"]     = None
    result["verify_report"] = None
    if verify_after and result.get("status") == "success":
        try:
            logger.info(f"[verify] Starting test-restore verification for '{watch_name}'…")
            vr = verify_restore_integrity(
                backup_dir   = result.get("backup_dir", str(backup_dir)),
                encrypt_key  = encrypt_key,
                progress_cb  = progress_cb,
            )
            result["verify_ok"]     = vr["ok"]
            result["verify_errors"] = vr["corrupted_files"] + vr["missing_files"]
            result["verify_report"] = vr
            if vr["ok"]:
                logger.info(
                    f"[verify] ✅ '{watch_name}': {vr['files_verified']} file(s) verified, "
                    f"{vr['files_skipped']} existence-checked in {vr['duration_s']}s"
                )
            else:
                logger.warning(
                    f"[verify] ⚠ '{watch_name}': verification failed — "
                    f"{vr['files_failed']} file(s) corrupt/missing "
                    f"({len(vr['restore_errors'])} restore errors); "
                    f"error={vr.get('error')}"
                )
        except Exception as _ve:
            result["verify_errors"] = [f"Verification exception: {_ve}"]
            result["verify_ok"]     = False

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

def restore_backup(backup_dir: str, target_path: str, incremental_only: bool = False,
                   encrypt_key: str = None, progress_cb=None, overwrite: bool = True,
                   remote_type: str = None, remote_cfg: dict = None) -> dict:
    """Restore files from a backup directory back to target_path.

    When backup_dir resides on a remote destination (WebDAV, rclone, SFTP,
    FTP, or Google Drive) pass remote_type + remote_cfg and restore_backup() will
    download the store to a local temp directory before restoring, then clean up.

    Args:
        backup_dir: Path to the backup directory (local, or remote path when
                    remote_type is set — e.g. the share sub-path for WebDAV).
        target_path: Path where files should be restored.
        incremental_only: If True, only restore files that don't already exist at target.
        encrypt_key: Fernet key for decryption.
        progress_cb: Optional callback(restored_count, total_files, filename).
        overwrite: If True, overwrite existing files; if False, skip them.
        remote_type: One of "sftp"|"ftp"|"ftps"|"webdav"|"rclone"|"gdrive".
                     When set, backup_dir is treated as the remote path.
        remote_cfg: Credentials / config dict matching remote_type.
    """
    import gzip as _gz

    # ── Optional: download from a remote destination before restoring ──────────
    _restore_temp_dir: Optional[str] = None
    if remote_type:
        import tempfile, shutil as _sh2
        _restore_temp_dir = tempfile.mkdtemp(prefix="backupsys_restore_")
        cfg = remote_cfg or {}
        try:
            if remote_type in ("sftp", "ftps"):
                _dl = download_from_sftp(backup_dir, _restore_temp_dir, cfg)
            elif remote_type in ("ftp",):
                _dl = download_from_ftp(backup_dir, _restore_temp_dir, cfg)
            elif remote_type == "webdav":
                from transport_utils import download_from_webdav
                _dl = download_from_webdav(backup_dir, _restore_temp_dir, cfg)
            elif remote_type == "rclone":
                from transport_utils import download_from_rclone
                _dl = download_from_rclone(backup_dir, _restore_temp_dir, cfg)
            elif remote_type == "https":
                from transport_utils import download_from_https
                _dl = download_from_https(backup_dir, _restore_temp_dir, cfg)
            elif remote_type == "gdrive":
                _dl = download_from_gdrive(cfg, _restore_temp_dir)
                if _dl.get("ok"):
                    _dl["status"] = "ok"
            else:
                _sh2.rmtree(_restore_temp_dir, ignore_errors=True)
                return {"ok": False, "files_restored": 0, "skipped": 0, "errors": [],
                        "error": f"Unsupported remote_type for restore: {remote_type!r}"}
            if _dl.get("status") != "ok":
                _sh2.rmtree(_restore_temp_dir, ignore_errors=True)
                return {"ok": False, "files_restored": 0, "skipped": 0, "errors": [],
                        "error": f"Failed to download backup from {remote_type}: {_dl.get('error')}"}
            backup_dir = _restore_temp_dir  # restore from the local copy
        except Exception as _dl_err:
            _sh2.rmtree(_restore_temp_dir, ignore_errors=True)
            return {"ok": False, "files_restored": 0, "skipped": 0, "errors": [],
                    "error": f"Remote download error: {_dl_err}"}

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
        enc_p      = bd / "MANIFEST.json.enc"

        # Prefer the encrypted manifest when we have a key — it contains the
        # full snapshot and changes lists needed for a faithful restore.
        if encrypt_key and CRYPTO_AVAILABLE and enc_p.exists():
            try:
                manifest = json.loads(_decrypt_bytes(enc_p.read_bytes(), encrypt_key))
            except Exception as _dec_err:
                # Encrypted manifest decryption failed — this almost certainly means the
                # wrong key was supplied.  Do NOT fall back to the plaintext stub: the
                # stub intentionally omits 'changes' (for security), so a silent fallback
                # would appear to succeed while restoring nothing.  Fail loudly instead.
                logger.warning(
                    "Could not decrypt MANIFEST.json.enc (%s); wrong key or corrupted manifest",
                    _dec_err,
                )
                result["error"] = (
                    "MANIFEST.json.enc decryption failed — "
                    "wrong encryption key or corrupted manifest"
                )
                result["errors"].append(result["error"])
                return result
        elif manifest_p.exists():
            try:
                with open(manifest_p) as f:
                    manifest = json.load(f)
            except json.JSONDecodeError:
                result["error"] = "MANIFEST.json is corrupted (invalid JSON) — cannot restore"
                return result
            except Exception as e:
                result["error"] = f"Could not read MANIFEST.json: {e}"
                return result
        else:
            result["error"] = "MANIFEST.json not found — cannot restore"
            return result

        tp.mkdir(parents=True, exist_ok=True)
        restored = 0
        skipped  = 0
        snap     = manifest.get("snapshot", {})  # per-file hashes for integrity check
        total_files = len([e for e in manifest.get("changes", []) if e.get("type") in ("added", "modified")])

        for entry in manifest.get("changes", []):
            etype = entry.get("type")
            rel   = entry.get("path", "")

            if etype in ("added", "modified"):
                src_file = bd / rel
                gz_file  = bd / (rel + '.gz')
                dst_file = tp / rel

                # ── NEW: Skip if not overwrite and file already exists ──
                if not overwrite and dst_file.exists():
                    skipped += 1
                    continue

                if not src_file.exists() and gz_file.exists():
                    # Compressed backup — decompress on restore
                    try:
                        dst_file.parent.mkdir(parents=True, exist_ok=True)
                        with _gz.open(str(gz_file), 'rb') as f_in, open(str(dst_file), 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                        restored += 1
                        if progress_cb:
                            try:
                                progress_cb(restored, total_files, rel)
                            except Exception:
                                logger.debug("[suppressed] Exception ignored near: restored += 1 |                         if progress_cb: |                       ", exc_info=True)
                                pass
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
                        if progress_cb:
                            try:
                                progress_cb(restored, total_files, rel)
                            except Exception:
                                logger.debug("[suppressed] Exception ignored near: restored += 1 |                         if progress_cb: |                       ", exc_info=True)
                                pass
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
                        logger.debug("[suppressed] Exception ignored near: dst_file = tp / rel |                 if dst_file.exists(): |                   ", exc_info=True)
                        pass

        result["ok"]             = True
        result["files_restored"] = restored
        result["skipped"]        = skipped

    except Exception as e:
        result["error"] = str(e)

    # Clean up the temp dir we created for a remote download (if any)
    if _restore_temp_dir:
        import shutil as _sh3
        _sh3.rmtree(_restore_temp_dir, ignore_errors=True)

    return result


def restore_full_chain(
    destination: str,
    watch_id: str,
    target_path: str,
    up_to_backup_id: Optional[str] = None,
    encrypt_key: Optional[str] = None,
    progress_cb: Optional[Callable[[int, int, str], None]] = None,
    overwrite: bool = True,
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
        overwrite:        If True, overwrite existing files; if False, skip them.

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
                    logger.debug("[suppressed] Exception ignored near: if progress_cb: |                 try: |                     progress_cb(step_id", exc_info=True)
                    pass

            step_result = restore_backup(
                backup_dir=backup_dir,
                target_path=target_path,
                encrypt_key=encrypt_key,
                overwrite=overwrite,
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


# ─── Restore Verification ─────────────────────────────────────────────────────

def verify_restore_integrity(
    backup_dir: str,
    encrypt_key: Optional[str] = None,
    progress_cb: Optional[Callable] = None,
    keep_temp: bool = False,
) -> dict:
    """
    Verify a backup by performing a full test-restore to a temporary directory
    and comparing every restored file against the MANIFEST snapshot hashes.

    Workflow:
      1.  Load MANIFEST.json — fail fast if absent / corrupt.
      2.  Create an OS temp directory (under the system temp root, never inside
          the watched source or the backup destination).
      3.  Call restore_backup() to restore all files into the temp dir.
          This path exercises the full restore stack: decompression, decryption,
          MANIFEST validation, and file-copy logic.
      4.  For every entry in the manifest snapshot that carries a non-empty
          stored hash, re-hash the restored file and compare byte-for-byte.
      5.  Encrypted / compressed files whose snapshot hash is empty (normal —
          the snapshot stores plaintext hashes but encrypted ciphertext is in
          the backup dir) are *existence-checked* instead: the restore must
          produce a non-empty file, but no hash comparison is performed.
      6.  Delete the temp directory unless *keep_temp=True*.
      7.  Return a detailed report dict.

    Returns::

        {
          ok:              bool,   # True iff restore succeeded AND 0 errors
          files_restored:  int,    # files restore_backup() copied
          files_verified:  int,    # files hash-checked (has stored hash)
          files_skipped:   int,    # files existence-checked (no stored hash)
          files_failed:    int,    # verification failures
          missing_files:   [str],  # expected in snapshot but absent after restore
          corrupted_files: [str],  # restored but hash mismatch
          restore_errors:  [str],  # errors from restore_backup()
          error:           str|None,
          temp_dir:        str,    # path used (deleted unless keep_temp=True)
          duration_s:      float,
        }
    """
    import tempfile as _tempfile

    started  = time.time()
    result: dict = {
        "ok":              False,
        "files_restored":  0,
        "files_verified":  0,
        "files_skipped":   0,
        "files_failed":    0,
        "missing_files":   [],
        "corrupted_files": [],
        "restore_errors":  [],
        "error":           None,
        "temp_dir":        "",
        "duration_s":      0.0,
    }

    backup_dir = _fix_path(backup_dir)
    bd         = Path(backup_dir)
    tmp_dir    = None

    try:
        # ── 1. Load manifest ────────────────────────────────────────────────
        manifest_p = bd / "MANIFEST.json"
        if not manifest_p.exists():
            result["error"] = "MANIFEST.json not found — cannot verify"
            return result
        try:
            with open(manifest_p) as f:
                manifest = json.load(f)
        except json.JSONDecodeError as e:
            result["error"] = f"MANIFEST.json is corrupted: {e}"
            return result

        snap        = manifest.get("snapshot", {})
        is_enc      = manifest.get("encrypted", False)
        is_cmp      = bool(manifest.get("compressed", False))
        changes     = manifest.get("changes", [])
        to_restore  = [c for c in changes if c.get("type") in ("added", "modified")]

        if not to_restore and not snap:
            # Empty backup (no files) — trivially OK
            result.update({"ok": True, "duration_s": round(time.time() - started, 2)})
            return result

        # ── 2. Create temp dir ──────────────────────────────────────────────
        tmp_dir = _tempfile.mkdtemp(prefix="backupsys_verify_")
        result["temp_dir"] = tmp_dir

        # ── 3. Restore to temp ──────────────────────────────────────────────
        total_steps = len(to_restore)

        def _restore_progress(restored, total, fname):
            if progress_cb:
                try:
                    progress_cb(restored, total, f"Restoring: {fname}")
                except Exception:
                    logger.debug("[suppressed] Exception ignored near: def _restore_progress(restored, total, fname): |             if progress_cb: |  ", exc_info=True)
                    pass

        restore_result = restore_backup(
            backup_dir=backup_dir,
            target_path=tmp_dir,
            encrypt_key=encrypt_key,
            progress_cb=_restore_progress,
            overwrite=True,
        )

        result["files_restored"] = restore_result.get("files_restored", 0)
        result["restore_errors"] = list(restore_result.get("errors", []))

        if not restore_result.get("ok", False):
            result["error"] = restore_result.get("error") or "Restore step failed"
            return result

        # ── 4 & 5. Verify restored files ────────────────────────────────────
        verified = 0
        skipped  = 0
        failed   = 0
        missing  = []
        corrupt  = []

        for entry in to_restore:
            rel = entry.get("path", "")
            if not rel:
                continue

            restored_file = Path(tmp_dir) / rel
            stored_hash   = snap.get(rel, {}).get("hash", "") if isinstance(snap.get(rel), dict) else ""

            if progress_cb:
                try:
                    progress_cb(verified + skipped + failed, total_steps, f"Verifying: {rel}")
                except Exception:
                    logger.debug("[suppressed] Exception ignored near: if progress_cb: |                 try: |                     progress_cb(verifie", exc_info=True)
                    pass

            if not restored_file.exists():
                # File should have been restored but isn't present
                missing.append(rel)
                failed += 1
                continue

            if stored_hash:
                # Full hash comparison
                actual_hash = hash_file(str(restored_file))
                if actual_hash and actual_hash != stored_hash:
                    corrupt.append(rel)
                    failed += 1
                    logger.warning(
                        f"[verify-restore] Hash mismatch: {rel} "
                        f"(expected {stored_hash[:12]}…, got {actual_hash[:12]}…)"
                    )
                else:
                    verified += 1
            else:
                # Encrypted or compressed — existence + non-empty size check
                if restored_file.stat().st_size == 0:
                    corrupt.append(rel)
                    failed += 1
                    logger.warning(f"[verify-restore] Zero-byte file after restore: {rel}")
                else:
                    skipped += 1

        result.update({
            "ok":              (failed == 0 and not result["restore_errors"]),
            "files_verified":  verified,
            "files_skipped":   skipped,
            "files_failed":    failed,
            "missing_files":   missing,
            "corrupted_files": corrupt,
        })

        logger.info(
            f"[verify-restore] {bd.name}: "
            f"{verified} verified, {skipped} existence-checked, {failed} failed "
            f"({result['files_restored']} files restored to temp)"
        )

    except Exception as exc:
        import traceback
        result["error"] = str(exc)
        logger.error(f"[verify-restore] Unexpected error: {exc}\n{traceback.format_exc()}")

    finally:
        result["duration_s"] = round(time.time() - started, 2)
        if tmp_dir and not keep_temp:
            try:
                shutil.rmtree(tmp_dir, ignore_errors=True)
            except Exception:
                logger.debug('[suppressed] Exception ignored.', exc_info=True)
                pass

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
            logger.debug('[suppressed] Exception ignored.', exc_info=True)
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


# ─── Encryption Key Rotation ──────────────────────────────────────────────────

def rotate_encryption_key(backup_dir, old_key, new_key, progress_cb=None):
    """
    Rotate encryption key for all .enc files in backup_dir.
    Decrypts each .enc file with old_key, re-encrypts with new_key, replaces in place,
    and updates the manifest.json hash entry for that file.
    progress_cb(rel_path, idx, total) is called for progress updates if provided.
    Returns: {ok: bool, files_rotated: int, errors: list}
    """
    import tempfile
    backup_dir = _fix_path(backup_dir)
    bd = Path(backup_dir)
    errors = []
    files_rotated = 0
    manifest_path = bd / "MANIFEST.json"
    # Load manifest
    try:
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)
    except Exception as e:
        return {"ok": False, "files_rotated": 0, "errors": [f"Failed to load manifest: {e}"]}

    # Find all .enc files
    enc_files = [f for f in bd.rglob("*.enc") if f.is_file()]
    total = len(enc_files)
    for idx, enc_file in enumerate(enc_files):
        rel_path = str(enc_file.relative_to(bd))
        try:
            # Decrypt to temp file
            with tempfile.NamedTemporaryFile(delete=False) as tmp_dec:
                tmp_dec_path = tmp_dec.name
            _decrypt_file(str(enc_file), tmp_dec_path, old_key)
            # Encrypt to temp file
            with tempfile.NamedTemporaryFile(delete=False) as tmp_enc:
                tmp_enc_path = tmp_enc.name
            new_hash = _encrypt_file(tmp_dec_path, tmp_enc_path, new_key)
            # Replace original file with new encrypted file
            os.replace(tmp_enc_path, str(enc_file))
            os.remove(tmp_dec_path)
            files_rotated += 1
            # Update manifest hash if present
            snap = manifest.get("snapshot", {})
            if rel_path in snap:
                snap[rel_path]["hash"] = new_hash
        except Exception as e:
            errors.append(f"{rel_path}: {e}")
            # Clean up temp files if they exist
            for p in [locals().get('tmp_dec_path'), locals().get('tmp_enc_path')]:
                if p and os.path.exists(p):
                    try: os.remove(p)
                    except Exception:
                        logger.debug("[suppressed] Exception ignored near: # Clean up temp files if they exist |             for p in [locals().get('tmp_de", exc_info=True)
        if progress_cb:
            try:
                progress_cb(rel_path, idx + 1, total)
            except Exception:
                logger.debug("[suppressed] Exception ignored near: except Exception: pass |         if progress_cb: |             try: |           ", exc_info=True)
                pass
    # Save manifest
    try:
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)
    except Exception as e:
        errors.append(f"Failed to save manifest: {e}")
        return {"ok": False, "files_rotated": files_rotated, "errors": errors}
    return {"ok": len(errors) == 0, "files_rotated": files_rotated, "errors": errors}


# ─── Cleanup helpers (preview + delete old backups) ──────────────────────────
def preview_cleanup(backups_dir: str, retention_days: int, watch_id: str = None) -> dict:
    """
    Return a preview of backup folders that would be deleted under the
    given retention policy. Result contains a `to_delete` list of folder paths.
    """
    p = Path(backups_dir)
    now = datetime.utcnow()
    to_delete = []
    if not p.exists() or not p.is_dir():
        return {"to_delete": []}

    for child in p.iterdir():
        if not child.is_dir():
            continue
        manifest = child / "MANIFEST.json"
        if not manifest.exists():
            continue
        try:
            data = json.loads(manifest.read_text(encoding="utf-8"))
            mid = data.get("watch_id")
            ts = data.get("timestamp")
            if watch_id and mid != watch_id:
                continue
            if not ts:
                continue
            try:
                dt = datetime.fromisoformat(ts)
            except Exception:
                continue
            age_days = (now - dt).days
            if age_days > int(retention_days):
                to_delete.append(str(child))
        except Exception:
            continue
    return {"to_delete": to_delete}


def cleanup_old_backups(backups_dir: str, retention_days: int, watch_id: str = None) -> dict:
    """
    Delete old backup folders under `backups_dir` older than `retention_days`.
    Returns keys: ok, deleted (count), freed_bytes (int), freed_human (str).
    """
    preview = preview_cleanup(backups_dir, retention_days, watch_id=watch_id)
    deleted = 0
    freed = 0
    for pstr in preview.get("to_delete", []):
        try:
            p = Path(pstr)
            size = 0
            for fp in p.rglob("*"):
                try:
                    if fp.is_file():
                        size += fp.stat().st_size
                except Exception:
                    continue
            shutil.rmtree(p, ignore_errors=True)
            deleted += 1
            freed += size
        except Exception:
            continue

    def _human_size(n: int) -> str:
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if n < 1024:
                return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
            n /= 1024
        return f"{n:.1f} PB"

    return {"ok": True, "deleted": deleted, "freed_bytes": freed, "freed_human": _human_size(freed)}