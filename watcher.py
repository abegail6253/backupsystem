import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional
import logging
logger = logging.getLogger(__name__)

# Pending change buffer per watch_id
_pending: Dict[str, List[dict]] = {}
_lock = threading.Lock()

try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler, FileSystemEvent
    WATCHDOG_AVAILABLE = True
except ImportError:
    WATCHDOG_AVAILABLE = False

try:
    from backup_engine import _safe_size
except ImportError:
    def _safe_size(path: str) -> int:
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
            # Single-file watch: only allow the target file
            include_only = [p[1:] for p in self.exclude_patterns if p.startswith("!")]
            if include_only:
                return Path(path).name not in include_only
            if not self.exclude_patterns:
                return False
            import fnmatch as _fn
            p = Path(path)
            for pat in self.exclude_patterns:
                if pat.startswith("!"):
                    continue  # skip include markers
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
                # Cap memory usage — prune to half when over limit to avoid repeated single-trim under high churn
                if len(_pending[self.watch_id]) > 5000:
                    _pending[self.watch_id] = _pending[self.watch_id][-2500:]

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

    # Seconds to wait after the last file-change event before firing the callback.
    # Prevents flooding the UI when an editor auto-saves many files in quick succession.
    DEBOUNCE_DELAY: float = 2.0

    def __init__(self):
        self._observers:        Dict[str, object]            = {}
        self._poll_threads:     Dict[str, threading.Thread]  = {}
        self._poll_stop_events: Dict[str, threading.Event]   = {}
        self._debounce_timers:  Dict[str, threading.Timer]   = {}
        self._debounce_lock     = threading.Lock()
        self._running = True

    # ── Debounce helper ───────────────────────────────────────────────────────

    def _make_debounced_callback(
        self,
        watch_id: str,
        on_change: Optional[Callable],
    ) -> Optional[Callable]:
        """
        Wrap *on_change* so that rapid bursts of events are collapsed into a
        single call fired DEBOUNCE_DELAY seconds after the **last** event.

        The raw event is still stored in the pending buffer immediately (so
        pending_count() stays accurate); only the UI/application callback is
        debounced.
        """
        if on_change is None:
            return None

        def _debounced(wid: str, entry: dict):
            with self._debounce_lock:
                existing = self._debounce_timers.pop(wid, None)
                if existing is not None:
                    existing.cancel()

                def _fire():
                    with self._debounce_lock:
                        self._debounce_timers.pop(wid, None)
                    try:
                        on_change(wid, entry)
                    except Exception:
                        pass

                t = threading.Timer(self.DEBOUNCE_DELAY, _fire)
                t.daemon = True
                self._debounce_timers[wid] = t
                t.start()

        return _debounced

    # ── public API ────────────────────────────────────────────────────────────

    def start(self, watch_id: str, path: str, on_change: Optional[Callable] = None, exclude_patterns: Optional[List[str]] = None, interval_min: int = 0) -> bool:
        if watch_id in self._observers or watch_id in self._poll_threads:
            return True

        p = Path(path)
        if not p.exists():
            return False

        # For single files, watch the parent directory but filter to only that file
        if p.is_file():
            _target_filename = p.name
            path = str(p.parent)
            if exclude_patterns is None:
                exclude_patterns = []
            # Exclude everything that isn't the target file
            exclude_patterns = [pat for pat in exclude_patterns]  # copy
            if f"!{_target_filename}" not in exclude_patterns:
                exclude_patterns.append(f"!{_target_filename}")

        if WATCHDOG_AVAILABLE:
            try:
                debounced = self._make_debounced_callback(watch_id, on_change)
                handler  = _Handler(watch_id, debounced, exclude_patterns=exclude_patterns)
                observer = Observer()
                observer.schedule(handler, path, recursive=True)
                observer.start()
                self._observers[watch_id] = observer
                return True
            except Exception:
                pass

        logger.warning(
            f"[watcher] watchdog unavailable — polling every 60s for: {path}"
        )
        self._start_polling(watch_id, path, on_change, exclude_patterns, interval_min=interval_min)
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

        # Cancel any pending debounce timer for this watch
        with self._debounce_lock:
            t = self._debounce_timers.pop(watch_id, None)
            if t is not None:
                t.cancel()

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

    def check_and_restart_dead(
        self,
        watch_id: str,
        path: str,
        on_change=None,
        exclude_patterns=None,
        interval_min: int = 0,
    ) -> bool:
        """
        Check if the observer for *watch_id* has died (e.g. network share went
        offline) and restart it if so.  Safe to call on every timer tick.
        Returns True if a restart was performed.
        """
        obs = self._observers.get(watch_id)
        if obs is not None and not obs.is_alive():
            logger.warning(
                f"[watcher] Observer for '{watch_id}' died — restarting (path: {path})"
            )
            # Clean up the dead observer without joining (it's already dead)
            try:
                obs.stop()
            except Exception:
                pass
            del self._observers[watch_id]
            # Cancel any stale debounce timer
            with self._debounce_lock:
                t = self._debounce_timers.pop(watch_id, None)
                if t is not None:
                    t.cancel()
            # Restart
            self.start(watch_id, path, on_change, exclude_patterns, interval_min)
            return True
        return False

    # ── internal ──────────────────────────────────────────────────────────────

    def _start_polling(self, watch_id: str, path: str, on_change: Optional[Callable], exclude_patterns: Optional[List[str]] = None, interval_min: int = 0):
        """Simple polling fallback — checks mtimes every 60 seconds."""
        try:
            from backup_engine import build_snapshot, diff_snapshots
        except ImportError as e:
            import logging as _log
            _log.getLogger(__name__).error(f"[watcher] polling disabled — backup_engine import failed: {e}")
            return

        stop_event = threading.Event()
        self._poll_stop_events[watch_id] = stop_event
        _excl = exclude_patterns or []
        # Apply the same debounce used by the watchdog path so rapid poll-detected
        # changes don't flood the UI callback on network shares.
        _debounced_cb = self._make_debounced_callback(watch_id, on_change)

        def _poll():
            snap = build_snapshot(path, exclude_patterns=_excl)

            while not stop_event.is_set():
                # ✅ FIX: Use interval_min to calculate poll interval
                poll_secs = max(30, interval_min * 60) if interval_min > 0 else 60
                stop_event.wait(poll_secs)  # poll every poll_secs — watchdog handles real-time, this is just a fallback
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

                        if _debounced_cb:
                            try:
                                _debounced_cb(watch_id, entry)
                            except Exception:
                                pass

                except Exception:
                    pass

        t = threading.Thread(target=_poll, daemon=True, name=f"poll-{watch_id}")
        t.start()
        self._poll_threads[watch_id] = t