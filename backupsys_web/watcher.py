import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional

# Pending change buffer per watch_id
_pending: Dict[str, List[dict]] = {}
_lock = threading.Lock()

try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler, FileSystemEvent
    WATCHDOG_AVAILABLE = True
except ImportError:
    WATCHDOG_AVAILABLE = False


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _safe_size(path: str) -> int:
    """Return file size in bytes, 0 on any error."""
    try:
        p = Path(path)
        return p.stat().st_size if p.is_file() else 0
    except Exception:
        return 0


# ─── Watchdog handler ────────────────────────────────────────────────────────

if WATCHDOG_AVAILABLE:
    class _Handler(FileSystemEventHandler):
        def __init__(self, watch_id: str, on_change: Optional[Callable] = None, exclude_patterns: Optional[List[str]] = None):
            super().__init__()
            self.watch_id        = watch_id
            self.on_change       = on_change
            self.exclude_patterns = exclude_patterns or []

        def _is_excluded(self, path: str) -> bool:
            if not self.exclude_patterns:
                return False
            import fnmatch as _fn
            p = Path(path)
            for pat in self.exclude_patterns:
                if _fn.fnmatch(p.name, pat):
                    return True
                for part in p.parts:
                    if _fn.fnmatch(part, pat):
                        return True
            return False

        def _record(self, event_type: str, src: str, dest: Optional[str] = None):
            if self._is_excluded(src):
                return
            entry = {
                "type":      event_type,
                "path":      src,
                "dest":      dest,
                "timestamp": datetime.now().isoformat(),
                "size":      _safe_size(src),
            }

            with _lock:
                bucket = _pending.setdefault(self.watch_id, [])
                # Remove existing entry for this path if it exists
                _pending[self.watch_id] = [e for e in bucket if e["path"] != src]
                _pending[self.watch_id].append(entry)

            if self.on_change:
                try:
                    self.on_change(self.watch_id, entry)
                except Exception:
                    pass

        def on_modified(self, event: FileSystemEvent):
            if not event.is_directory:
                self._record("modified", event.src_path)

        def on_created(self, event: FileSystemEvent):
            if not event.is_directory:
                self._record("added", event.src_path)

        def on_deleted(self, event: FileSystemEvent):
            if not event.is_directory:
                self._record("deleted", event.src_path)

        def on_moved(self, event: FileSystemEvent):
            if not event.is_directory:
                dest = getattr(event, "dest_path", None)
                # If renamed INTO an excluded pattern (e.g. file.tmp), record as deleted
                if dest and self._is_excluded(dest):
                    self._record("deleted", event.src_path)
                else:
                    self._record("renamed", event.src_path, dest)


# ─── Watcher manager ─────────────────────────────────────────────────────────

class WatcherManager:
    """Manages watchdog observers (or polling threads) for multiple watch targets."""

    def __init__(self):
        self._observers:        Dict[str, object]            = {}
        self._poll_threads:     Dict[str, threading.Thread]  = {}
        self._poll_stop_events: Dict[str, threading.Event]   = {}
        self._running = True

    # ── public API ────────────────────────────────────────────────────────────

    def start(self, watch_id: str, path: str, on_change: Optional[Callable] = None, exclude_patterns: Optional[List[str]] = None) -> bool:
        if watch_id in self._observers or watch_id in self._poll_threads:
            return True

        if not Path(path).exists():
            return False

        if WATCHDOG_AVAILABLE:
            try:
                handler  = _Handler(watch_id, on_change, exclude_patterns=exclude_patterns)
                observer = Observer()
                observer.schedule(handler, path, recursive=True)
                observer.start()
                self._observers[watch_id] = observer
                return True
            except Exception:
                pass

        import logging as _logging
        _watcher_logger = _logging.getLogger(__name__)
        _watcher_logger.warning(
            f"[watcher] watchdog unavailable — polling every 5s for: {path}"
        )
        self._start_polling(watch_id, path, on_change, exclude_patterns)
        return True

    def stop(self, watch_id: str):
        if watch_id in self._observers:
            try:
                self._observers[watch_id].stop()
                self._observers[watch_id].join(timeout=2)
            except Exception:
                pass
            del self._observers[watch_id]

        if watch_id in self._poll_stop_events:
            self._poll_stop_events[watch_id].set()
            del self._poll_stop_events[watch_id]
        self._poll_threads.pop(watch_id, None)

        with _lock:
            _pending.pop(watch_id, None)

    def restart(self, watch_id: str, path: str, on_change: Optional[Callable] = None, exclude_patterns: Optional[List[str]] = None) -> bool:
        self.stop(watch_id)
        return self.start(watch_id, path, on_change, exclude_patterns)

    def stop_all(self):
        self._running = False
        for wid in list(self._observers.keys()) + list(self._poll_threads.keys()):
            self.stop(wid)

    def get_pending(self, watch_id: str) -> List[dict]:
        with _lock:
            return list(_pending.get(watch_id, []))

    def get_all_pending(self) -> Dict[str, List[dict]]:
        with _lock:
            return {wid: list(changes) for wid, changes in _pending.items()}

    def clear_pending(self, watch_id: str):
        with _lock:
            _pending.pop(watch_id, None)

    def is_watching(self, watch_id: str) -> bool:
        return watch_id in self._observers or watch_id in self._poll_threads

    def pending_count(self, watch_id: str) -> int:
        with _lock:
            return len(_pending.get(watch_id, []))

    # ── internal ──────────────────────────────────────────────────────────────

    def _start_polling(self, watch_id: str, path: str, on_change: Optional[Callable], exclude_patterns: Optional[List[str]] = None):
        """Simple polling fallback — checks mtimes every 5 seconds."""
        from backup_engine import build_snapshot, diff_snapshots

        stop_event = threading.Event()
        self._poll_stop_events[watch_id] = stop_event
        _excl = exclude_patterns or []

        def _poll():
            snap = build_snapshot(path, exclude_patterns=_excl)

            while not stop_event.is_set():
                stop_event.wait(5)
                if stop_event.is_set():
                    break
                try:
                    new_snap = build_snapshot(path, previous=snap, exclude_patterns=_excl)
                    changes  = diff_snapshots(snap, new_snap)

                    for c in changes:
                        entry = {**c, "timestamp": datetime.now().isoformat()}
                        with _lock:
                            bucket = _pending.setdefault(watch_id, [])
                            # Remove any existing entry for this path
                            _pending[watch_id] = [e for e in bucket if e["path"] != c["path"]]
                            _pending[watch_id].append(entry)

                        if on_change:
                            try:
                                on_change(watch_id, entry)
                            except Exception:
                                pass

                    snap = new_snap

                except Exception:
                    pass

        t = threading.Thread(target=_poll, daemon=True, name=f"poll-{watch_id}")
        t.start()
        self._poll_threads[watch_id] = t