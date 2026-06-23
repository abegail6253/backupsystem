import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional
import logging
logger = logging.getLogger(__name__)

# Import config_manager for persistent history
try:
    import config_manager
except ImportError:
    config_manager = None

# Pending change buffer per watch_id
_pending: Dict[str, List[dict]] = {}
_lock = threading.Lock()

# Per-path SMB session snapshot cache.
# Keyed by normalised lowercase path.  Written whenever watchdog captures a live
# NetSessionEnum snapshot; read by the unc_poll snapshot-borrow in desktop_app.py
# when the watchdog entry has not yet arrived in _pending (race where unc_poll
# dispatches attribution before watchdog's on_change fires).
# Format: { path_lower: (capture_timestamp_float, smb_sessions_snapshot_list) }
# Entries expire after _PATH_SNAP_TTL_S seconds.
_path_smb_snapshot: Dict[str, tuple] = {}
_path_smb_snapshot_lock = threading.Lock()
_PATH_SNAP_TTL_S = 30   # discard snapshots older than this

# Optional suppression hook registered by desktop_app.
# Signature: (watch_id: str, event_type: str, path: str) -> bool
# When it returns True the event is suppressed: NOT persisted to history.json
# and NOT forwarded to on_change.  This lets desktop_app block destination-
# watcher noise (backup writes, unc_poll duplicates) at the lowest level so
# nothing leaks into the on-disk history file.
_history_persist_suppressor: Optional[Callable] = None

# ── SMB Session Cache ────────────────────────────────────────────────────────
# Problem: when a coworker opens a share at 8am and deletes a file at 7pm,
# their Event 4624 (Network Logon) is 11 hours old — outside any practical
# time-window query. NetSessionEnum at deletion time also returns nothing
# because the session may have closed instantly after the delete.
#
# Solution: poll NetSessionEnum every 60 seconds for each watched UNC host and
# keep a rolling 24-hour history of who had sessions open, timestamped.  When
# a deletion fires, _get_session_at_time() scans the cache for the entry whose
# timestamp is closest to (and before) the deletion time, ignoring own-machine
# entries.  This gives correct attribution even for sessions opened hours ago.
#
# Structure: { host: [(timestamp_utc, [{"username":..,"machine":..,"ip":..}])] }
# The list is kept in reverse-chronological order (newest first).
# Max age: 24 hours.  Poll interval: 60 seconds.
_smb_session_cache: Dict[str, list] = {}       # host -> [(ts, [sessions])]
_smb_session_cache_lock = threading.Lock()
_smb_session_poll_threads: Dict[str, threading.Thread] = {}
_smb_session_poll_stops:   Dict[str, threading.Event]  = {}
_SMB_SESSION_CACHE_MAX_AGE_S  = 86400   # 24 hours
_SMB_SESSION_POLL_INTERVAL_S  = 60      # poll every 60 seconds


def _smb_session_cache_push(host: str, sessions: list) -> None:
    """Append a timestamped snapshot to the cache for *host*, pruning entries older than 24h."""
    import time as _t_sc
    now_ts = _t_sc.time()  # use time.time() — datetime.utcnow().timestamp() mis-applies TZ offset
    cutoff = now_ts - _SMB_SESSION_CACHE_MAX_AGE_S
    with _smb_session_cache_lock:
        bucket = _smb_session_cache.setdefault(host, [])
        bucket.insert(0, (now_ts, sessions))
        # Prune old entries (list is newest-first so we can break early)
        while bucket and bucket[-1][0] < cutoff:
            bucket.pop()


def get_recent_idle_history(host: str, ip: str, event_ts: float,
                             own_host: str = "", own_ip: str = "",
                             max_snapshots: int = 5) -> list:
    """
    Return a list of (timestamp, idle_time) tuples for the given client IP,
    taken from the most recent *max_snapshots* cache entries before event_ts.
    Useful for detecting persistent-monitor behaviour (idle oscillates near 0)
    vs genuine write (first drop to 0 after a long idle stretch).
    Returns newest-first.
    """
    with _smb_session_cache_lock:
        bucket = list(_smb_session_cache.get(host, []))

    if not bucket:
        return []

    own_host_lower = own_host.lower()
    result = []
    for ts, sessions in bucket:
        if ts > event_ts + 5:
            continue
        if event_ts - ts > _SMB_SESSION_CACHE_MAX_AGE_S:
            break
        for s in sessions:
            m = (s.get("machine") or "").lower()
            sip = s.get("ip") or ""
            if own_host_lower and m == own_host_lower:
                continue
            if own_ip and sip == own_ip:
                continue
            if sip == ip:
                result.append((ts, s.get("idle_time")))
                break
        if len(result) >= max_snapshots:
            break
    return result  # newest-first



def get_cached_session_at_time(host: str, event_ts: float,
                                own_host: str = "", own_ip: str = "") -> list:
    """
    Return the session list from the cache entry whose timestamp is closest
    to *event_ts* (unix UTC float) and at most 24h before it.
    Own-machine sessions are excluded using *own_host* / *own_ip*.
    Returns [] if nothing is found.
    """
    with _smb_session_cache_lock:
        bucket = list(_smb_session_cache.get(host, []))  # shallow copy under lock

    if not bucket:
        return []

    # Find the snapshot taken closest to (and at most 24h before) event_ts.
    # bucket is newest-first.
    best_ts, best_sessions = None, []
    for ts, sessions in bucket:
        if ts > event_ts + 5:        # snapshot taken after the event — skip
            continue
        if event_ts - ts > _SMB_SESSION_CACHE_MAX_AGE_S:
            break                    # too old
        best_ts, best_sessions = ts, sessions
        break                        # newest qualifying entry wins

    if best_ts is None:
        return []

    own_host_lower = own_host.lower()
    result = []
    for s in best_sessions:
        m = (s.get("machine") or "").lower()
        ip = s.get("ip") or ""
        if own_host_lower and m == own_host_lower:
            continue
        if own_ip and ip == own_ip:
            continue
        result.append(s)
    return result


def start_smb_session_poller(host: str, smb_audit_cfg: dict,
                              own_host: str = "", own_ip: str = "") -> None:
    """
    Start a background thread that polls NetSessionEnum for *host* every 60s
    and stores results in _smb_session_cache.  Safe to call multiple times for
    the same host — only one thread per host is started.
    """
    with _smb_session_cache_lock:
        if host in _smb_session_poll_threads and _smb_session_poll_threads[host].is_alive():
            return   # already running

    stop_evt = threading.Event()
    _smb_session_poll_stops[host] = stop_evt

    def _poll_loop():
        import socket as _sp_sock
        _nse_fn = _get_net_session_enum_fn("[smb_session_poller] ")
        _nas_user = (smb_audit_cfg or {}).get("username", "")
        _nas_pass = (smb_audit_cfg or {}).get("password", "")
        _ipc_connected = False
        logger.info(f"[smb_session_poller] Started for host={host!r} poll_interval={_SMB_SESSION_POLL_INTERVAL_S}s")
        # FIX: Run an immediate poll before the first 60s sleep so the session
        # cache is populated right away.  Previously the poller slept 60s first,
        # leaving the cache empty for the first minute.  If a coworker added
        # files within that window, Step0b found raw=0 and fell back to the
        # local machine owner — misattributing the event to this PC.
        _first_iteration = True
        while True:
            if not _first_iteration:
                if stop_evt.wait(timeout=_SMB_SESSION_POLL_INTERVAL_S):
                    break   # stop requested
            _first_iteration = False
            if stop_evt.is_set():
                break
            try:
                # Ensure IPC$ auth so NetSessionEnum returns remote sessions
                if not _ipc_connected and _nas_user and _nas_pass:
                    try:
                        import win32net as _wn_p, win32netcon as _wnc_p
                        _wn_p.NetUseAdd(None, 1, {
                            "remote": f"\\\\{host}\\IPC$",
                            "username": _nas_user, "password": _nas_pass,
                            "domainname": "", "asg_type": getattr(_wnc_p, "USE_IPC", 3),
                        })
                        _ipc_connected = True
                    except Exception:
                        pass   # may already be connected; continue anyway

                if _nse_fn is None:
                    _nse_fn = _get_net_session_enum_fn("[smb_session_poller] ")
                if _nse_fn is None:
                    stop_evt.wait(timeout=300)  # no pywin32 — back off
                    continue

                raw = []
                try:
                    raw = _nse_fn(host)
                except Exception as _e_poll:
                    logger.debug(f"[smb_session_poller] NetSessionEnum failed: {_e_poll!r}")
                    continue

                own_h = own_host.lower()
                sessions = []
                for _s in raw:
                    _client = (_s.get("client_name") or "").lstrip("\\").lower()
                    _uname  = _s.get("user_name") or _s.get("username") or ""
                    if not _client or not _uname:
                        continue
                    try:
                        _cip = _sp_sock.gethostbyname(_client)
                    except Exception:
                        _cip = _client
                    if own_h and _client == own_h:
                        continue
                    if own_ip and _cip == own_ip:
                        continue
                    sessions.append({
                        "username": _uname,
                        "machine":  _client,
                        "ip":       _cip,
                        "idle_time":  _s.get("idle_time"),   # preserve for monitor-detection
                        "active_time": _s.get("active_time"),
                    })

                _smb_session_cache_push(host, sessions)
                if sessions:
                    logger.debug(
                        f"[smb_session_poller] {host!r}: cached {len(sessions)} session(s): "
                        f"{[s['machine'] for s in sessions]}"
                    )
            except Exception as _outer:
                logger.debug(f"[smb_session_poller] Unexpected error: {_outer!r}")

        logger.info(f"[smb_session_poller] Stopped for host={host!r}")

    t = threading.Thread(target=_poll_loop, name=f"smb-sess-poll-{host}", daemon=True)
    _smb_session_poll_threads[host] = t
    t.start()


def stop_smb_session_poller(host: str) -> None:
    """Stop the background session poller for *host* if running."""
    evt = _smb_session_poll_stops.get(host)
    if evt:
        evt.set()

# ── NetSessionEnum calling-convention probe cache ────────────────────────────
# win32net.NetSessionEnum signature varies across pywin32 builds.  Known forms:
#
#   String-server variants (server = UNC host string):
#     (server, client, level, resume)  — 4-arg with client ""
#     (server, client, level)          — 3-arg with client ""
#     (server, client, level, resume)  — 4-arg with client None
#     (server, client, level)          — 3-arg with client None
#     (server, level, resume)          — 3-arg, no client arg
#     (server, level)                  — 2-arg, no client arg
#
#   None-server variants (server = None → queries via the already-established
#   IPC$ connection; some pywin32 builds require this form):
#     (None, client, level, resume)
#     (None, client, level)
#     (None, None, level, resume)
#     (None, None, level)
#     (None, level, resume)
#     (None, level)
#
# The error "'str' object cannot be interpreted as an integer" means the build
# expects an integer where it's receiving a string — typically because the
# client arg ("" / None) is being interpreted as the level slot, or because the
# server slot must be None rather than a host string.
#
# We probe once per process, cache the working lambda, and reuse it everywhere.
# Probing with None-server variants is safe: no network I/O is attempted when
# server=None (queries the local machine's session table via IPC$).
_net_session_enum_fn = None          # cached callable or False if unavailable
_net_session_enum_lock = threading.Lock()

def _get_net_session_enum_fn(log_prefix: str = ""):
    """
    Return a callable f(host) -> list[dict] that calls NetSessionEnum for the
    given host, trying every known signature variant until one doesn't raise
    TypeError.  The working variant is cached for the process lifetime.

    Returns None if win32net is unavailable or all variants fail.

    Probe strategy
    ──────────────
    String-server variants probe with "localhost" as the host; any exception
    other than TypeError means the signature is accepted (host unreachable /
    access denied is fine — we only need the calling convention).

    None-server variants (server=None) probe with a direct call; they query the
    local machine's IPC$ session table, which is safe and produces no TypeError
    if the variant is accepted.

    For None-server variants the returned callable still accepts a host argument
    (for API compatibility) but always passes None as the server — the real
    session routing is determined by which IPC$ connection is already
    authenticated (via NetUseAdd) at call time.
    """
    global _net_session_enum_fn
    if _net_session_enum_fn is not None:
        return _net_session_enum_fn if _net_session_enum_fn is not False else None
    with _net_session_enum_lock:
        if _net_session_enum_fn is not None:
            return _net_session_enum_fn if _net_session_enum_fn is not False else None
        try:
            import win32net as _wn
        except ImportError:
            logger.info(f"{log_prefix}win32net not available — NetSessionEnum disabled")
            _net_session_enum_fn = False
            return None

        def _make_str_caller(fn):
            """Caller for string-server variants: passes host as-is."""
            def _call(host):
                raw = fn(host)
                return (raw[0] if isinstance(raw, tuple) else raw) or []
            _call._is_host_aware = True   # actually queries the remote host
            return _call

        def _make_none_caller(fn):
            """Caller for None-server variants: ignores host, always uses None."""
            def _call(_host):
                raw = fn()
                return (raw[0] if isinstance(raw, tuple) else raw) or []
            _call._is_host_aware = True   # routes via IPC$ NetUseAdd to the host
            return _call

        # ── String-server candidates (probe with "localhost") ──────────────
        _probe_host = "localhost"
        _str_candidates = [
            ("(server, \"\", level, resume)", lambda h: _wn.NetSessionEnum(h, "", 10, 0)),
            ("(server, \"\", level)",          lambda h: _wn.NetSessionEnum(h, "", 10)),
            ("(server, None, level, resume)", lambda h: _wn.NetSessionEnum(h, None, 10, 0)),
            ("(server, None, level)",         lambda h: _wn.NetSessionEnum(h, None, 10)),
            ("(server, level, resume)",       lambda h: _wn.NetSessionEnum(h, 10, 0)),
            ("(server, level)",               lambda h: _wn.NetSessionEnum(h, 10)),
        ]
        for _label, _fn in _str_candidates:
            try:
                _fn(_probe_host)
                logger.info(
                    f"{log_prefix}NetSessionEnum probe: string-server variant {_label!r} accepted "
                    f"(cached for process lifetime)"
                )
                _net_session_enum_fn = _make_str_caller(_fn)
                return _net_session_enum_fn
            except TypeError:
                logger.debug(
                    f"{log_prefix}NetSessionEnum probe: string-server variant {_label!r} "
                    f"→ TypeError (wrong signature)"
                )
            except Exception as _e:
                logger.info(
                    f"{log_prefix}NetSessionEnum probe: string-server variant {_label!r} accepted "
                    f"(non-TypeError on probe: {_e!r}) — cached for process lifetime"
                )
                _net_session_enum_fn = _make_str_caller(_fn)
                return _net_session_enum_fn

        # ── None-server candidates (probe by calling directly) ─────────────
        # These variants pass None as the server, which instructs win32net to
        # query via the already-established IPC$ connection (NetUseAdd target).
        _none_candidates = [
            ("(None, \"\", level, resume)", lambda: _wn.NetSessionEnum(None, "", 10, 0)),
            ("(None, \"\", level)",          lambda: _wn.NetSessionEnum(None, "", 10)),
            ("(None, None, level, resume)", lambda: _wn.NetSessionEnum(None, None, 10, 0)),
            ("(None, None, level)",         lambda: _wn.NetSessionEnum(None, None, 10)),
            ("(None, level, resume)",       lambda: _wn.NetSessionEnum(None, 10, 0)),
            ("(None, level)",               lambda: _wn.NetSessionEnum(None, 10)),
        ]
        for _label, _fn in _none_candidates:
            try:
                _fn()
                logger.info(
                    f"{log_prefix}NetSessionEnum probe: None-server variant {_label!r} accepted "
                    f"(cached for process lifetime — SMB sessions routed via IPC$ NetUseAdd)"
                )
                _net_session_enum_fn = _make_none_caller(_fn)
                return _net_session_enum_fn
            except TypeError:
                logger.debug(
                    f"{log_prefix}NetSessionEnum probe: None-server variant {_label!r} "
                    f"→ TypeError (wrong signature)"
                )
            except Exception as _e:
                logger.info(
                    f"{log_prefix}NetSessionEnum probe: None-server variant {_label!r} accepted "
                    f"(non-TypeError on probe: {_e!r}) — cached for process lifetime"
                )
                _net_session_enum_fn = _make_none_caller(_fn)
                return _net_session_enum_fn

        # ── No-server candidates: first arg IS the level ───────────────────
        # Some pywin32 builds expose no server slot at all — passing any
        # non-integer (str or None) as the first arg raises
        # "'X' object cannot be interpreted as an integer".
        # In this form the function queries the local machine's session table.
        _noserver_candidates = [
            ("(\"\" , level, resume)", lambda: _wn.NetSessionEnum("", 10, 0)),
            ("(\"\" , level)",         lambda: _wn.NetSessionEnum("", 10)),
            ("(level, resume)",        lambda: _wn.NetSessionEnum(10, 0)),
            ("(level,)",               lambda: _wn.NetSessionEnum(10)),
        ]

        def _make_noserver_caller(fn):
            """Caller for no-server variants: always calls with fixed args, ignores host."""
            def _call(_host):
                raw = fn()
                return (raw[0] if isinstance(raw, tuple) else raw) or []
            _call._is_host_aware = False  # queries local session table only — NOT remote
            return _call

        for _label, _fn in _noserver_candidates:
            try:
                _fn()
                logger.info(
                    f"{log_prefix}NetSessionEnum probe: no-server variant {_label!r} accepted "
                    f"(cached for process lifetime — queries local IPC$ session table)"
                )
                _net_session_enum_fn = _make_noserver_caller(_fn)
                return _net_session_enum_fn
            except TypeError:
                logger.debug(
                    f"{log_prefix}NetSessionEnum probe: no-server variant {_label!r} "
                    f"→ TypeError (wrong signature)"
                )
            except Exception as _e:
                logger.info(
                    f"{log_prefix}NetSessionEnum probe: no-server variant {_label!r} accepted "
                    f"(non-TypeError on probe: {_e!r}) — cached for process lifetime"
                )
                _net_session_enum_fn = _make_noserver_caller(_fn)
                return _net_session_enum_fn

        logger.warning(
            f"{log_prefix}NetSessionEnum probe: ALL 16 variants "
            f"(6 string-server + 6 None-server + 4 no-server) "
            f"raised TypeError — this pywin32 build may not support NetSessionEnum. "
            f"Who-did-it tracking will use Windows Security Event Log and SMB session enumeration."
        )
        _net_session_enum_fn = False
        return None


def register_history_persist_suppressor(fn: Optional[Callable]) -> None:
    """Register (or clear) the suppression hook.  Called by desktop_app."""
    global _history_persist_suppressor
    _history_persist_suppressor = fn


# ── History-defer flag ───────────────────────────────────────────────────────
# When desktop_app is running, it handles history persistence AFTER attribution
# (user/machine/IP enrichment).  Setting this flag tells the watcher to skip
# its own save_history() calls so the on-disk file isn't polluted with
# attribution-less entries that appear as duplicates in the History window.
_defer_history_to_app: bool = False

def set_defer_history_to_app(enabled: bool) -> None:
    """Called by desktop_app at startup to disable watcher-level history saves."""
    global _defer_history_to_app
    _defer_history_to_app = enabled


# ── Module-level watcher-manager ref (set by desktop_app) ─────────────────
_registered_watcher_manager = None

def register_watcher_manager(mgr) -> None:
    """Called by desktop_app to register its WatcherManager instance so
    module-level helpers (e.g. reset_watch_snapshot) can reach it."""
    global _registered_watcher_manager
    _registered_watcher_manager = mgr


def reset_watch_snapshot(watch_id: str) -> None:
    """Signal the poll thread for *watch_id* to discard its current snapshot
    and rebuild on the next cycle.  Used by the ~$ lock-file deletion guard
    in desktop_app to force re-detection of Office saves that were missed due
    to SMB CHANGE_NOTIFY buffer overflow.
    Silently does nothing if the manager is not registered or watch_id unknown.
    """
    mgr = _registered_watcher_manager
    if mgr is None:
        return
    try:
        # Wake the __unc_poll thread so the snap-reset is processed promptly.
        _unc_poll_wakeup = mgr._snap_reset_events.get(watch_id + "__wakeup")
        if _unc_poll_wakeup is not None:
            _unc_poll_wakeup.set()
        # Also signal the main snap-reset event.
        mgr.reset_snapshot(watch_id)
    except Exception:
        pass

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
        def __init__(self, watch_id: str, on_change: Optional[Callable] = None, exclude_patterns: Optional[List[str]] = None, smb_audit_cfg: Optional[dict] = None):
            super().__init__()
            self.watch_id        = watch_id
            self.on_change       = on_change
            self.exclude_patterns = exclude_patterns or []
            self.smb_audit_cfg   = smb_audit_cfg or {}

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
            logger.debug(f"[watchdog._record] event_type={event_type!r} src={src!r} dest={dest!r} watch_id={self.watch_id!r}")
            if self._is_excluded(src):
                logger.debug(f"[watchdog._record] EXCLUDED: {src!r} — skipping")
                return
            entry = {
                "type":             event_type,
                "path":             src,
                "dest":             dest,
                "timestamp":        datetime.now().isoformat(),
                "size":             _safe_size(src),
                "detection_source": "watchdog",
            }
            logger.info(f"[watchdog] Detected {event_type.upper()}: {src!r} (watch_id={self.watch_id!r})")

            # ── Snapshot active SMB sessions RIGHT NOW before the session closes ─
            # For UNC paths, NetSessionEnum must run at the instant of detection.
            # By the time _get_editor_info runs (after 2s debounce), the SMB
            # session for a deletion is already closed and NetSessionEnum finds
            # nothing. Capture it here and attach it to the entry.
            def _unc_host_quick(p: str) -> str:
                # Replace ALL backslashes in one pass so \\host\share → //host/share
                # (double-replace "\\\\→/" then "\\→/" turns \\host into /host,
                # missing the leading slash — use a single replace instead)
                n = p.replace("\\", "/")
                if n.startswith("//"):
                    parts = n.lstrip("/").split("/")
                    return parts[0] if parts else ""
                logger.debug(
                    f"[watchdog._record] _unc_host_quick: path={p!r} → n={n!r} "
                    f"(not a UNC path — no host extracted)"
                )
                return ""

            _smb_host = _unc_host_quick(src)
            logger.info(
                f"[watchdog._record] UNC host extraction: src={src!r} → "
                f"_smb_host={_smb_host!r} event_type={event_type!r} "
                f"watch_id={self.watch_id!r}"
            )
            if _smb_host:
                try:
                    import win32net as _w32net, socket as _sock
                    import time as _time_snap
                    # Authenticate to IPC$ with SMB credentials before NetSessionEnum.
                    # This improves session query reliability on Windows/Mac hosts.
                    # with access denied, returning an empty list even when sessions exist.
                    _smb_user = self.smb_audit_cfg.get("username", "")
                    _smb_pass = self.smb_audit_cfg.get("password", "")
                    _ipc_auth_ok = False
                    if _smb_user and _smb_pass:
                        try:
                            import win32netcon as _w32nc
                            _USE_IPC = getattr(_w32nc, "USE_IPC", 3)
                            _use_info = {
                                "remote":     f"\\\\{_smb_host}\\IPC$",
                                "username":   _smb_user,
                                "password":   _smb_pass,
                                "domainname": "",
                                "asg_type":   _USE_IPC,
                            }
                            _w32net.NetUseAdd(None, 1, _use_info)
                            _ipc_auth_ok = True
                            logger.debug(
                                f"[watchdog._record] IPC$ auth OK for {_smb_host!r} "
                                f"user={_smb_user!r} (watch_id={self.watch_id!r})"
                            )
                        except Exception as _ipc_err:
                            logger.debug(
                                f"[watchdog._record] IPC$ auth failed for {_smb_host!r}: {_ipc_err!r} "
                                f"(may already be connected — proceeding anyway)"
                            )
                    else:
                        logger.info(
                            f"[watchdog._record] No SMB credentials configured — "
                            f"NetSessionEnum on {_smb_host!r} may fail without admin credentials. "
                            f"Add SMB admin credentials in watch settings for reliable attribution. "
                            f"(watch_id={self.watch_id!r} type={event_type!r})"
                        )

                    _own_host = ""
                    _own_ip   = ""
                    try:
                        _own_host = _sock.gethostname().lower()
                        _own_ip   = _sock.gethostbyname(_own_host)
                    except Exception:
                        pass
                    # BUG-FIX (attribution, same root cause as desktop_app._get_editor_info):
                    # the NetFileEnum filter below used to compare the handle's USERNAME
                    # against the local HOSTNAME — those never match for a real account
                    # name (e.g. 'user' vs 'desktop-0edubap'), so a local handle was never
                    # excluded as "own", and the resulting "remote" snapshot could get
                    # matched to a coworker's session purely because both PCs happen to
                    # log in under the same generic Windows username. Resolve and use the
                    # actual local username for this comparison instead.
                    _own_user = ""
                    try:
                        import win32api as _w32api_own
                        _own_user = (_w32api_own.GetUserName() or "").lower()
                    except Exception:
                        try:
                            import os as _os_own
                            _own_user = (_os_own.environ.get("USERNAME", "") or "").lower()
                        except Exception:
                            pass
                    logger.info(
                        f"[watchdog._record] own identity for local/remote disambiguation: "
                        f"own_host={_own_host!r} own_ip={_own_ip!r} own_user={_own_user!r} "
                        f"(watch_id={self.watch_id!r} type={event_type!r})"
                    )

                    def _run_net_session_enum() -> list:
                        """Run NetSessionEnum level 10 using the probed calling convention."""
                        _nse_fn = _get_net_session_enum_fn("[watchdog._record] ")
                        if _nse_fn is None:
                            logger.info(
                                f"[watchdog._record] NetSessionEnum UNAVAILABLE — "
                                f"_get_net_session_enum_fn returned None (all 16 pywin32 calling "
                                f"conventions raised TypeError at probe time). "
                                f"NetFileEnum fallback will be attempted. "
                                f"watch_id={self.watch_id!r} type={event_type!r} host={_smb_host!r}"
                            )
                            return []
                        try:
                            return _nse_fn(_smb_host)
                        except Exception as _e:
                            logger.info(
                                f"[watchdog._record] NetSessionEnum call failed at runtime: {_e!r} "
                                f"watch_id={self.watch_id!r}"
                            )
                            return []

                    def _build_snapshot(sessions: list) -> list:
                        """Filter sessions to usable (non-own, has username) entries.

                        Side-effect: populates _own_session_min_active_time with the
                        minimum active_time seen across all own-machine sessions.  A
                        short active_time (e.g. < 30s) on an own session means the local
                        machine just opened a new loopback SMB connection to its own
                        share — the hallmark of a local Explorer file-copy operation.
                        This value is stored alongside smb_sessions_snapshot so
                        _get_editor_info can use it as a local-write tiebreaker.
                        """
                        _out = []
                        _own_min_at = None   # track shortest own-session active_time
                        for _s in sessions:
                            _client = (_s.get("client_name") or "").lstrip("\\").lower()
                            _uname  = _s.get("user_name") or _s.get("username") or ""
                            try:
                                _cip = _sock.gethostbyname(_client) if _client else ""
                            except Exception:
                                _cip = _client
                            _is_own = (
                                _client == _own_host or
                                (_own_ip and _cip == _own_ip)
                            )
                            # Determine filter verdict for clear logging
                            if not _client:
                                _verdict = "SKIP_no_client"
                            elif _is_own:
                                _verdict = f"SKIP_own_machine(own_host={_own_host!r} own_ip={_own_ip!r})"
                            elif not _uname:
                                _verdict = "SKIP_no_username"
                            else:
                                _verdict = "ACCEPT"
                            logger.info(
                                f"[watchdog._record] NetSessionEnum raw session: "
                                f"client={_client!r} user={_uname!r} ip={_cip!r} "
                                f"is_own={_is_own} own_host={_own_host!r} own_ip={_own_ip!r} "
                                f"verdict={_verdict!r} "
                                f"all_keys={list(_s.keys())!r} all_vals={dict(_s)!r} "
                                f"(watch_id={self.watch_id!r} type={event_type!r})"
                            )
                            if _verdict == "ACCEPT":
                                _out.append({
                                    "username":    _uname,
                                    "machine":     _client,
                                    "ip":          _cip,
                                    "idle_time":   _s.get("idle_time"),
                                    "active_time": _s.get("active_time"),
                                })
                            elif _is_own:
                                # Track the minimum active_time across own-machine
                                # sessions.  A freshly-opened loopback SMB session
                                # (active_time ~ a few seconds) is strong evidence
                                # the local user just performed a file operation.
                                _at = _s.get("active_time")
                                if _at is not None:
                                    _own_min_at = _at if _own_min_at is None else min(_own_min_at, _at)
                        if _own_min_at is not None:
                            # Attach min own-session active_time to every accepted
                            # entry so _get_editor_info can inspect it for local-write
                            # detection without needing a separate data channel.
                            for _e in _out:
                                _e["own_min_active_time"] = _own_min_at
                            logger.info(
                                f"[watchdog._record] own-session min active_time={_own_min_at}s "
                                f"(attached to {len(_out)} snapshot entry(s) as "
                                f"'own_min_active_time') "
                                f"(watch_id={self.watch_id!r} type={event_type!r})"
                            )
                        if not _out and sessions:
                            logger.info(
                                f"[watchdog._record] NetSessionEnum: all {len(sessions)} raw session(s) "
                                f"were filtered out. "
                                f"own_host={_own_host!r} own_ip={_own_ip!r} "
                                f"(watch_id={self.watch_id!r} type={event_type!r}). "
                                f"HINT: if the Windows PC host ({_smb_host!r}) is both source and "
                                f"the coworker's machine, and they deleted locally (not via SMB), "
                                f"NetSessionEnum will show 0 sessions from them — "
                                f"only the Security Event Log with SACL auditing can identify local actors."
                            )
                        return _out

                    # For deletions: retry NetSessionEnum up to 5 times over ~2.5s.
                    # The SMB session may close within ~100–300ms of the delete
                    # completing, but the watchdog event fires asynchronously so we
                    # may catch the session if we start immediately and keep retrying.
                    # 5 × 500ms gives us a 2.5s window without blocking the watcher.
                    _is_deletion = event_type == "deleted"
                    _max_attempts = 5 if _is_deletion else 1
                    _retry_delay  = 0.5  # seconds between retries

                    _sessions_raw = []
                    _snapped      = []
                    for _attempt in range(1, _max_attempts + 1):
                        _sessions_raw = _run_net_session_enum()
                        logger.info(
                            f"[watchdog._record] NetSessionEnum attempt {_attempt}/{_max_attempts}: "
                            f"{len(_sessions_raw)} raw session(s) on {_smb_host!r} "
                            f"ipc_auth_ok={_ipc_auth_ok} "
                            f"(watch_id={self.watch_id!r} type={event_type!r})"
                        )
                        _snapped = _build_snapshot(_sessions_raw)
                        if _snapped:
                            logger.info(
                                f"[watchdog._record] NetSessionEnum attempt {_attempt}: "
                                f"found {len(_snapped)} usable session(s) — stopping retries"
                            )
                            break
                        if _attempt < _max_attempts:
                            logger.info(
                                f"[watchdog._record] NetSessionEnum attempt {_attempt}: "
                                f"no usable session yet — retrying in {_retry_delay}s "
                                f"(deletion retry: session may still be closing)"
                            )
                            _time_snap.sleep(_retry_delay)

                    if _snapped:
                        entry["smb_sessions_snapshot"] = _snapped
                        # Also store in the per-path cache so unc_poll attribution
                        # can find it even when watchdog's on_change fires after unc_poll
                        # has already tried to borrow from _pending and failed.
                        import time as _snap_ts
                        _pkey = (entry.get("path") or "").lower()
                        if _pkey:
                            with _path_smb_snapshot_lock:
                                _path_smb_snapshot[_pkey] = (_snap_ts.time(), list(_snapped))
                        logger.info(
                            f"[watchdog._record] NetSessionEnum snapshot captured: {_snapped} "
                            f"(watch_id={self.watch_id!r} type={event_type!r})"
                        )
                    else:
                        _raw_count = len(_sessions_raw)
                        logger.info(
                            f"[watchdog._record] NetSessionEnum: no usable session captured after "
                            f"{_max_attempts} attempt(s) — {_raw_count} raw session(s) on last attempt, "
                            f"all filtered (own-machine or no username). "
                            f"own_host={_own_host!r} own_ip={_own_ip!r} "
                            f"ipc_auth_ok={_ipc_auth_ok} "
                            f"watch_id={self.watch_id!r} type={event_type!r}"
                        )
                        if not _ipc_auth_ok:
                            logger.info(
                                f"[watchdog._record] Attribution hint: IPC$ auth was NOT established "
                                f"for {_smb_host!r}. Add SMB credentials in watch settings → "
                                f"NetSessionEnum/NetFileEnum will be able to query the session table."
                            )
                        # ── Fallback: NetFileEnum — lists open files with per-user info ──
                        # NetFileEnum (level 3) returns FILE_INFO_3: id, permissions,
                        # num_locks, pathname, username. Known pywin32 signatures:
                        #
                        #   String-server (server = PC IP string):
                        #     (server, basepath, level, resumeHandle)  — 4-arg
                        #     (server, None, level, resumeHandle)      — 4-arg, None basepath
                        #     (server, level, resumeHandle)            — 3-arg, no basepath
                        #     (server, level)                          — 2-arg
                        #     (server, basepath, username, level, resumeHandle) — 5-arg legacy
                        #
                        #   None-server (server = None → uses IPC$ connection established
                        #   above via NetUseAdd; required on some pywin32 builds where
                        #   passing a string raises "str cannot be interpreted as integer"):
                        #     (None, basepath, level, resumeHandle)
                        #     (None, None, level, resumeHandle)
                        #     (None, level, resumeHandle)
                        #     (None, level)
                        #
                        # We probe all variants, string-server first, then None-server.
                        try:
                            import win32net as _w32nfe
                            _file_entries_raw: list = []
                            _nfe_sig_used = ""
                            logger.info(
                                f"[watchdog._record] NetFileEnum fallback: probing signatures "
                                f"on {_smb_host!r} ipc_auth_ok={_ipc_auth_ok} "
                                f"(watch_id={self.watch_id!r} type={event_type!r})"
                            )
                            _nfe_variants = [
                                # ── String-server variants ──────────────────────────────
                                ("(host,'',3,0)",    lambda: _w32nfe.NetFileEnum(_smb_host, "",   3, 0)),
                                ("(host,None,3,0)",  lambda: _w32nfe.NetFileEnum(_smb_host, None, 3, 0)),
                                ("(host,3,0)",       lambda: _w32nfe.NetFileEnum(_smb_host, 3, 0)),
                                ("(host,3)",         lambda: _w32nfe.NetFileEnum(_smb_host, 3)),
                                ("(host,'','',3,0)", lambda: _w32nfe.NetFileEnum(_smb_host, "", "", 3, 0)),
                                # ── None-server variants (IPC$ connection already open) ─
                                ("(None,'',3,0)",    lambda: _w32nfe.NetFileEnum(None, "",   3, 0)),
                                ("(None,None,3,0)",  lambda: _w32nfe.NetFileEnum(None, None, 3, 0)),
                                ("(None,3,0)",       lambda: _w32nfe.NetFileEnum(None, 3, 0)),
                                ("(None,3)",         lambda: _w32nfe.NetFileEnum(None, 3)),
                                # ── No-server variants: first arg IS the level ──────────
                                # Some pywin32 builds expose no server slot at all;
                                # passing any object (str or None) as the first arg raises
                                # "X cannot be interpreted as an integer".
                                ("(3,0)",            lambda: _w32nfe.NetFileEnum(3, 0)),
                                ("(3,)",             lambda: _w32nfe.NetFileEnum(3)),
                            ]
                            for _nfe_sig, _nfe_call in _nfe_variants:
                                try:
                                    _nfe_result = _nfe_call()
                                    # NetFileEnum returns (data, total, resumeHandle)
                                    if isinstance(_nfe_result, tuple) and len(_nfe_result) >= 1:
                                        _file_entries_raw = list(_nfe_result[0])
                                    else:
                                        _file_entries_raw = list(_nfe_result)
                                    _nfe_sig_used = _nfe_sig
                                    logger.info(
                                        f"[watchdog._record] NetFileEnum signature {_nfe_sig!r} "
                                        f"ACCEPTED — {len(_file_entries_raw)} open file(s) "
                                        f"on {_smb_host!r} (watch_id={self.watch_id!r})"
                                    )
                                    break
                                except TypeError as _nfe_te:
                                    logger.debug(
                                        f"[watchdog._record] NetFileEnum signature {_nfe_sig!r} "
                                        f"→ TypeError: {_nfe_te!r}"
                                    )
                                except Exception as _nfe_e:
                                    _nfe_e_str = str(_nfe_e)
                                    _is_access_denied = (
                                        "Access is denied" in _nfe_e_str or
                                        "access denied" in _nfe_e_str.lower() or
                                        getattr(_nfe_e, "winerror", None) == 5
                                    )
                                    if _is_access_denied:
                                        logger.debug(
                                            f"[watchdog._record] NetFileEnum signature {_nfe_sig!r} "
                                            f"→ ACCESS DENIED on {_smb_host!r}. "
                                            f"FIX: the SMB credentials in watch settings must have "
                                            f"admin rights on the Windows PC. "
                                            f"account is in the 'administrators' group. "
                                            f""
                                            f"(watch_id={self.watch_id!r} ipc_auth_ok={_ipc_auth_ok})"
                                        )
                                        # Don't break — try remaining variants in case another
                                        # signature form (e.g. None-server) is accepted by this host.
                                        continue
                                    else:
                                        logger.debug(
                                            f"[watchdog._record] NetFileEnum signature {_nfe_sig!r} "
                                            f"→ {type(_nfe_e).__name__}: {_nfe_e!r} — stopping probe"
                                        )
                                    break  # non-TypeError, non-AccessDenied = pywin32 sig mismatch, stop

                            if not _nfe_sig_used:
                                _is_dest_watch = self.watch_id.endswith("__dest")
                                logger.info(
                                    f"[watchdog._record] NetFileEnum: all 11 signatures failed "
                                    f"(5 string-server + 4 None-server + 2 no-server) — "
                                    f"this pywin32 build may not support NetFileEnum, or the PC "
                                    f"rejected the request. (watch_id={self.watch_id!r})"
                                )
                                if _is_dest_watch:
                                    # Destination watcher on a SMB path — the actor could be a
                                    # coworker or a backup job. We cannot tell
                                    # without SMB session data. Emit a prominent hint.
                                    logger.info(
                                        f"[watchdog._record] ATTRIBUTION HINT (dest watch, same-SMB path): "
                                        f"This is a destination-folder deletion on {_smb_host!r}. "
                                        f"All Win32 session-enum strategies failed. "
                                        f"To identify the actor, enable object auditing (SACL) on the Windows PC shared folder. "
                                        f""
                                        f""
                                        f"Ensure SMB admin credentials are set in watch settings. "
                                        f"(watch_id={self.watch_id!r} smb_host={_smb_host!r} "
                                        f"ipc_auth_ok={_ipc_auth_ok} event_type={event_type!r})"
                                    )

                            # Dump every raw entry at INFO so they always appear in logs
                            for _fe_idx, _fe in enumerate(_file_entries_raw):
                                _fe_user = (
                                    _fe.get("fi3_username") or _fe.get("username") or
                                    _fe.get("fi2_username") or ""
                                )
                                _fe_path = (
                                    _fe.get("fi3_pathname") or _fe.get("pathname") or
                                    _fe.get("fi2_pathname") or _fe.get("path") or ""
                                )
                                logger.info(
                                    f"[watchdog._record] NetFileEnum entry [{_fe_idx}]: "
                                    f"user={_fe_user!r} path={_fe_path!r} "
                                    f"all_keys={list(_fe.keys())!r} all_vals={dict(_fe)!r}"
                                )

                            _snap_from_files: list = []
                            for _fe in _file_entries_raw:
                                _fe_user = (
                                    _fe.get("fi3_username") or _fe.get("username") or
                                    _fe.get("fi2_username") or ""
                                )
                                _fe_path = (
                                    _fe.get("fi3_pathname") or _fe.get("pathname") or
                                    _fe.get("fi2_pathname") or _fe.get("path") or ""
                                )
                                if not _fe_user:
                                    continue
                                _fe_user_lower = _fe_user.lower()
                                _is_own_user = (
                                    _fe_user_lower == _own_user or
                                    _fe_user_lower.endswith("\\" + _own_user) or
                                    _fe_user_lower == _own_host or
                                    _fe_user_lower.endswith("\\" + _own_host)
                                )
                                logger.info(
                                    f"[watchdog._record] NetFileEnum filter: "
                                    f"user={_fe_user!r} is_own={_is_own_user} "
                                    f"compared_against: own_user={_own_user!r} own_host={_own_host!r}"
                                )
                                if not _is_own_user:
                                    _snap_from_files.append({
                                        "username": _fe_user,
                                        "machine":  "",   # NetFileEnum gives no client IP
                                        "ip":       "",
                                        "source":   "NetFileEnum",
                                        "filepath": _fe_path,
                                    })
                            if _snap_from_files:
                                entry["smb_sessions_snapshot"] = _snap_from_files
                                logger.info(
                                    f"[watchdog._record] NetFileEnum SUCCESS: "
                                    f"captured {len(_snap_from_files)} session(s): "
                                    f"{_snap_from_files} "
                                    f"(watch_id={self.watch_id!r} type={event_type!r})"
                                )
                            else:
                                logger.info(
                                    f"[watchdog._record] NetFileEnum: no usable entries after "
                                    f"filtering {len(_file_entries_raw)} raw entries "
                                    f"(sig={_nfe_sig_used!r} own_host={_own_host!r}). "
                                    f"Attribution will rely on SMB session enumeration. "
                                    f"(watch_id={self.watch_id!r} type={event_type!r})"
                                )
                        except ImportError:
                            logger.info(
                                f"[watchdog._record] NetFileEnum skipped — "
                                f"win32net not available (watch_id={self.watch_id!r})"
                            )
                        except Exception as _nfe_outer:
                            logger.info(
                                f"[watchdog._record] NetFileEnum outer error: {_nfe_outer!r} "
                                f"(watch_id={self.watch_id!r})"
                            )
                except ImportError:
                    logger.info(
                        f"[watchdog._record] win32net not available (pywin32 not installed) — "
                        f"no smb_sessions_snapshot. watch_id={self.watch_id!r} type={event_type!r}"
                    )
                except Exception as _e:
                    logger.info(
                        f"[watchdog._record] NetSessionEnum snapshot failed: {_e!r} "
                        f"(watch_id={self.watch_id!r} type={event_type!r})"
                    )

            # Check suppression hook before touching history or calling on_change
            if _history_persist_suppressor is not None:
                try:
                    if _history_persist_suppressor(self.watch_id, event_type, src):
                        logger.debug(
                            f"[watchdog._record] SUPPRESSED by hook: "
                            f"watch_id={self.watch_id!r} type={event_type!r} path={src!r}"
                        )
                        return
                except Exception:
                    pass

            with _lock:
                bucket = _pending.setdefault(self.watch_id, [])
                # Remove existing entry for this path if it exists
                _pending[self.watch_id] = [e for e in bucket if e["path"] != src]
                _pending[self.watch_id].append(entry)
                # Cap memory usage — prune to 2500 when over limit
                if len(_pending[self.watch_id]) > 2500:
                    _pending[self.watch_id] = _pending[self.watch_id][-2500:]

                # Persist to history.json immediately (capped at 5000 entries)
                # Skip when desktop_app is running — it saves history AFTER
                # attribution (user/machine/IP enrichment) to avoid blank rows.
                if config_manager is not None and not _defer_history_to_app:
                    try:
                        # Load current history, append, and save
                        history = config_manager.load_history()
                        history.append({
                            "type": entry["type"],
                            "path": entry["path"],
                            "dest": entry["dest"],
                            "timestamp": entry["timestamp"],
                            "size": entry["size"],
                            "detection_source": entry["detection_source"],
                            "watch_id": self.watch_id,
                        })
                        if len(history) > 5000:
                            history = history[-5000:]
                        config_manager.save_history(history)
                    except Exception as e:
                        logger.warning(f"[watcher] Failed to persist history: {e}")

            if self.on_change:
                try:
                    self.on_change(self.watch_id, entry)
                except Exception:
                    pass

        def on_modified(self, event: FileSystemEvent):
            logger.debug(f"[watchdog.on_modified] is_directory={event.is_directory} src={event.src_path!r}")
            if not event.is_directory:
                self._record("modified", event.src_path)

        def on_created(self, event: FileSystemEvent):
            logger.debug(f"[watchdog.on_created] is_directory={event.is_directory} src={event.src_path!r}")
            if not event.is_directory:
                self._record("added", event.src_path)

        def on_deleted(self, event: FileSystemEvent):
            logger.debug(f"[watchdog.on_deleted] is_directory={event.is_directory} src={event.src_path!r}")
            if not event.is_directory:
                logger.warning(f"[watchdog] DELETE event fired for: {event.src_path!r} (is_directory={event.is_directory})")
                self._record("deleted", event.src_path)
            else:
                logger.debug(f"[watchdog.on_deleted] IGNORED (is_directory=True): {event.src_path!r}")

        def on_moved(self, event: FileSystemEvent):
            logger.info(
                f"[watchdog] on_moved: is_directory={event.is_directory} "
                f"src={event.src_path!r} dest={getattr(event, 'dest_path', None)!r} "
                f"(watch_id={self.watch_id!r})"
            )
            if not event.is_directory:
                dest = getattr(event, "dest_path", None)
                # If renamed INTO an excluded pattern (e.g. file.tmp), record as deleted
                if dest and self._is_excluded(dest):
                    logger.info(f"[watchdog] RENAME→EXCLUDED: recording as deleted: {event.src_path!r}")
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
        self._watches:          Dict[str, str]               = {}  # watch_id -> path
        # Per-poll-thread event: set by reset_snapshot() to tell the thread
        # to rebuild its snapshot baseline on the next iteration instead of
        # diffing against a stale pre-backup snapshot.
        self._snap_reset_events: Dict[str, threading.Event] = {}
        # Real-time UNC change-notify threads (Windows ReadDirectoryChangesW).
        # These replace the 15s poll for UNC paths — they fire within ~1 second
        # of any change, regardless of which client made it.
        self._unc_notify_threads:    Dict[str, threading.Thread] = {}
        self._unc_notify_stop_events: Dict[str, threading.Event] = {}

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

        IMPORTANT: debounce is keyed on (watch_id, path, event_type) NOT just
        watch_id.  Keying on watch_id alone caused a critical bug: when Windows
        fires ADDED + MODIFIED almost simultaneously for the same newly-written
        file, the MODIFIED event would cancel the ADDED timer, so only MODIFIED
        was ever delivered.  This broke spurious-modified suppression because
        the ADDED pre-stamp was never set, and MODIFIED was recorded as a
        genuine edit instead of being suppressed.
        """
        if on_change is None:
            return None

        def _debounced(wid: str, entry: dict):
            # Key on (watch_id, path, type) so different event types for the
            # same file are debounced independently and both fire.
            _path_key = (entry.get("path") or "").lower()
            _type_key = entry.get("type") or ""
            _db_key   = (wid, _path_key, _type_key)
            with self._debounce_lock:
                existing = self._debounce_timers.pop(_db_key, None)
                if existing is not None:
                    existing.cancel()

                def _fire():
                    with self._debounce_lock:
                        self._debounce_timers.pop(_db_key, None)
                    try:
                        on_change(wid, entry)
                    except Exception:
                        pass

                t = threading.Timer(self.DEBOUNCE_DELAY, _fire)
                t.daemon = True
                self._debounce_timers[_db_key] = t
                t.start()

        return _debounced

    # ── public API ────────────────────────────────────────────────────────────

    @staticmethod
    def _is_unc_path(path: str) -> bool:
        """Return True if *path* is a UNC network path (\\\\host\\share or //host/share)."""
        norm = path.replace("\\", "/")
        return norm.startswith("//")

    @staticmethod
    def _unc_host(path: str) -> str:
        """Extract the host portion from a UNC path, e.g. '\\\\192.168.1.1\\share' → '192.168.1.1'."""
        norm = path.replace("\\", "/").lstrip("/")
        return norm.split("/")[0] if norm else ""

    def start(self, watch_id: str, path: str, on_change: Optional[Callable] = None, exclude_patterns: Optional[List[str]] = None, interval_min: int = 0, source_type: str = "local", smb_audit_cfg: Optional[dict] = None) -> bool:
        if watch_id in self._observers or watch_id in self._poll_threads:
            return True

        p = Path(path)

        logger.info(
            f"[watcher.start] Starting watch: watch_id={watch_id!r} path={path!r} "
            f"source_type={source_type!r} interval_min={interval_min}"
        )

        # Remote sources (sftp/ftp) can't be watched by watchdog — fall back
        # to interval polling immediately instead of probing path existence.
        if source_type in ("sftp", "ftp", "ftps"):
            logger.info(
                f"[watcher] Remote source ({source_type}) — using interval polling for: {path}"
            )
            self._start_polling(watch_id, path, on_change, exclude_patterns,
                                interval_min=interval_min, source_type=source_type,
                                smb_audit_cfg=smb_audit_cfg)
            return True

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

        is_unc = self._is_unc_path(path)

        if WATCHDOG_AVAILABLE:
            try:
                debounced = self._make_debounced_callback(watch_id, on_change)
                handler  = _Handler(watch_id, debounced, exclude_patterns=exclude_patterns, smb_audit_cfg=smb_audit_cfg)
                observer = Observer()
                observer.schedule(handler, path, recursive=True)
                observer.start()
                self._observers[watch_id] = observer
                logger.info(f"[watcher.start] watchdog Observer started for watch_id={watch_id!r} path={path!r}")

                # FIX: For UNC/network paths, watchdog's ReadDirectoryChangesW only
                # fires for changes made through THIS machine's SMB connection.
                # Deletions (and other changes) performed directly on the remote host
                # are invisible to watchdog.  Run a supplemental snapshot-diff poll
                # alongside the observer so we catch remote-originated deletions.
                if is_unc:
                    logger.info(
                        f"[watcher] UNC path detected — starting real-time SMB2 "
                        f"CHANGE_NOTIFY watcher for: {path}"
                    )
                    # Try real-time ReadDirectoryChangesW first — fires within
                    # ~100-500 ms for changes from ANY SMB client (including
                    # coworkers on other machines).
                    # FIX: pass the DEBOUNCED callback into unc_notify (not the
                    # raw on_change).  Without debounce, unc_notify calls on_change
                    # directly for both ADDED and MODIFIED (which Windows fires
                    # simultaneously when a coworker creates a file).  The two events
                    # race; MODIFIED can win before the ADDED pre-stamp is set →
                    # MODIFIED passes spurious-suppression and is recorded as a real
                    # edit instead of being dropped.  The 2 s debounce gives the
                    # ADDED thread time to set the pre-stamp so the MODIFIED thread
                    # sees it and suppresses itself correctly.
                    notify_ok = self._start_unc_notify(
                        watch_id + "__unc_notify", path,
                        self._make_debounced_callback(watch_id, on_change),
                        exclude_patterns
                    )
                    if not notify_ok:
                        logger.warning(
                            f"[watcher] Real-time UNC notify unavailable for {path} "
                            f"— falling back to 15s supplemental poll"
                        )
                    # FIX: ALWAYS run a 15s snapshot-diff poll alongside unc_notify,
                    # even when the notify thread starts successfully.
                    # ReadDirectoryChangesW silently drops notifications when the SMB
                    # buffer overflows (ERROR_NOTIFY_ENUM_DIR) — which happens
                    # whenever robocopy floods the queue during a backup copy.
                    # Any change (e.g. a coworker deleting a file) that occurs
                    # during the overflow storm is lost with no recovery mechanism.
                    # The supplemental poll catches those missed events by diffing
                    # snapshots every 15 s, independent of notify health.
                    self._start_polling(
                        watch_id + "__unc_poll", path, on_change,
                        exclude_patterns,
                        interval_min=0,
                        source_type="local",
                        smb_audit_cfg=smb_audit_cfg,
                    )
                    if notify_ok:
                        logger.info(
                            f"[watcher] UNC path: real-time notify active + 15s "
                            f"snapshot-diff poll running as overflow safety net for: {path}"
                        )

                # ── SMB Session Cache poller ───────────────────────────────────
                # For UNC paths, start a background thread that polls
                # NetSessionEnum every 60s and caches the results with timestamps.
                # This lets _get_editor_info attribute deletions even when the
                # actor's 4624 Network Logon event is hours old and outside any
                # practical time-window query (e.g. session opened at 8am,
                # deletion at 7pm).
                if is_unc:
                    import socket as _wstart_sock
                    _unc_smb_host = ""
                    try:
                        _unc_parts = path.lstrip("\\").split("\\")
                        _unc_smb_host = _unc_parts[0] if _unc_parts else ""
                    except Exception:
                        pass
                    if _unc_smb_host and smb_audit_cfg:
                        try:
                            _wstart_own_host = _wstart_sock.gethostname().lower()
                            _wstart_own_ip   = _wstart_sock.gethostbyname(_wstart_own_host)
                        except Exception:
                            _wstart_own_host = ""
                            _wstart_own_ip   = ""
                        start_smb_session_poller(
                            _unc_smb_host, smb_audit_cfg,
                            own_host=_wstart_own_host,
                            own_ip=_wstart_own_ip,
                        )
                        logger.info(
                            f"[watcher] SMB session cache poller started for host={_unc_smb_host!r} "
                            f"(poll every {_SMB_SESSION_POLL_INTERVAL_S}s, 24h history)"
                        )
                else:
                    # ── BUG-FIX: local-path-as-SMB-share also needs a session poller ──
                    # A watch on a local drive path (e.g. D:\testshare) can still be
                    # reached by coworkers over the network if the folder is shared.
                    # _get_editor_info() redirects such local paths through the UNC
                    # SMB-audit pipeline using THIS machine's own IP as the "host",
                    # and relies on get_cached_session_at_time(own_ip, ...) to find
                    # the real remote actor (Step0b). But that cache is ONLY ever
                    # populated by start_smb_session_poller() — which, before this
                    # fix, was only ever called for genuine UNC watches (is_unc=True).
                    # For local watches it was never started at all, so the cache
                    # stayed permanently empty (raw=0), Step0b always fell through,
                    # and attribution fell back to the local win32security owner
                    # lookup — which always reports the share OWNER (this PC), never
                    # the coworker who actually wrote the file over SMB.
                    # Fix: start a poller keyed on our OWN ip/hostname so genuine
                    # remote NetSessionEnum entries get cached for local shares too.
                    import socket as _wstart_lsock
                    try:
                        _wstart_local_host = _wstart_lsock.gethostname().lower()
                        _wstart_local_ip   = _wstart_lsock.gethostbyname(_wstart_local_host)
                    except Exception:
                        _wstart_local_host = ""
                        _wstart_local_ip   = ""
                    if _wstart_local_ip:
                        # No SMB credentials are needed to enumerate sessions on our
                        # OWN machine — NetSessionEnum(own_host) works locally without
                        # IPC$ auth, unlike querying a *remote* host's session table.
                        start_smb_session_poller(
                            _wstart_local_ip, smb_audit_cfg or {},
                            own_host=_wstart_local_host,
                            own_ip=_wstart_local_ip,
                        )
                        logger.info(
                            f"[watcher] LOCAL-SHARE SMB session cache poller started for "
                            f"own_host={_wstart_local_host!r} own_ip={_wstart_local_ip!r} "
                            f"path={path!r} (poll every {_SMB_SESSION_POLL_INTERVAL_S}s) — "
                            f"needed so coworkers writing to this locally-shared folder "
                            f"can be attributed instead of falling back to the folder owner."
                        )
                    else:
                        logger.warning(
                            f"[watcher] LOCAL-SHARE SMB session poller NOT started for "
                            f"path={path!r} — could not resolve own hostname/IP. "
                            f"Coworker writes to this folder may be misattributed to the "
                            f"local owner until this is resolved."
                        )

                return True
            except Exception:
                pass

        logger.warning(
            f"[watcher] watchdog unavailable — polling every 60s for: {path}"
        )
        self._start_polling(watch_id, path, on_change, exclude_patterns, interval_min=interval_min, source_type=source_type, smb_audit_cfg=smb_audit_cfg)
        return True

    def add_watch(self, watch_id: str, path: str) -> bool:
        """Add a watch for the given path. Returns True if successfully started."""
        if watch_id in self._watches:
            return True  # already watching
        if self.start(watch_id, path):
            self._watches[watch_id] = path
            return True
        return False

    def remove_watch(self, watch_id: str):
        """Remove a watch."""
        self.stop(watch_id)
        self._watches.pop(watch_id, None)

    def flush(self, watch_id: str) -> List[dict]:
        """Return pending events for the watch and clear the buffer."""
        pending = self.get_pending(watch_id)
        self.clear_pending(watch_id)
        return pending

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
        self._snap_reset_events.pop(watch_id, None)

        # Also stop the supplemental UNC poll thread if one was started
        _unc_poll_id = watch_id + "__unc_poll"
        if _unc_poll_id in self._poll_stop_events:
            self._poll_stop_events[_unc_poll_id].set()
            del self._poll_stop_events[_unc_poll_id]
        self._poll_threads.pop(_unc_poll_id, None)
        self._snap_reset_events.pop(_unc_poll_id, None)

        # Also stop the real-time UNC notify thread if one was started
        self._stop_unc_notify(watch_id)

        # Cancel any pending debounce timer for this watch
        with self._debounce_lock:
            t = self._debounce_timers.pop(watch_id, None)
            if t is not None:
                t.cancel()

        with _lock:
            _pending.pop(watch_id, None)

    def restart(self, watch_id: str, path: str, on_change: Optional[Callable] = None, exclude_patterns: Optional[List[str]] = None, source_type: str = "local") -> bool:
        self.stop(watch_id)
        return self.start(watch_id, path, on_change, exclude_patterns, source_type=source_type)

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
        # Always reset the snapshot baseline for all poll threads (main, __unc_notify, __unc_poll)
        for poll_id in (watch_id, watch_id + "__unc_notify", watch_id + "__unc_poll"):
            evt = self._snap_reset_events.get(poll_id)
            if evt is not None:
                evt.set()
                logger.debug(
                    f"[watcher] clear_pending: snap_reset_event SET for poll_id={poll_id!r} "
                    f"(triggered by backup completion for watch_id={watch_id!r}) — "
                    f"NOTE: __unc_poll thread will receive but intentionally ignore this signal"
                )

    def reset_snapshot(self, watch_id: str):
        """Signal the poll thread for *watch_id* (and its __unc_notify companion,
        if any) to discard its current snapshot and rebuild from the live
        filesystem on the next iteration.
        NOTE: the __unc_poll thread is intentionally excluded — its baseline
        must survive backup completion so it can catch deletions that occurred
        during the notify overflow window."""
        for poll_id in (watch_id, watch_id + "__unc_notify"):
            evt = self._snap_reset_events.get(poll_id)
            if evt is not None:
                evt.set()

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

    # ── Real-time UNC change notification (Windows only) ─────────────────────

    def _start_unc_notify(
        self,
        watch_id: str,
        path: str,
        on_change: Optional[Callable],
        exclude_patterns: Optional[List[str]] = None,
    ) -> bool:
        """Start a real-time ReadDirectoryChangesW watcher for a UNC path.

        WHY THIS IS NEEDED
        ──────────────────
        Watchdog's Observer uses ReadDirectoryChangesW internally, but it opens
        the directory handle with GENERIC_READ access tied to THIS machine's SMB
        session.  The SMB server only sends CHANGE_NOTIFY responses for changes that
        flow through that specific session — so deletions by a different SMB
        client (your coworker's machine) are never signalled.

        FIX: Open the directory ourselves with FILE_SHARE_DELETE in the share
        flags and FILE_LIST_DIRECTORY access.  This tells the SMB server to include
        this handle in its "notify all sessions" list.  Combined with
        ReadDirectoryChangesW(watchSubtree=True), we receive instant SMB2
        CHANGE_NOTIFY messages for ANY client's changes — additions, deletions,
        renames — within ~100-500 ms of when they happen.

        Returns True if the thread started successfully (Windows + path OK),
        False if unavailable (non-Windows, path gone, etc.).  The caller falls
        back to the 15s poll when this returns False.
        """
        import os as _os
        if _os.name != "nt":
            return False

        unc_key = watch_id + "__unc_notify"
        if unc_key in self._unc_notify_threads:
            return True  # already running

        stop_evt = threading.Event()
        self._unc_notify_stop_events[unc_key] = stop_evt

        _excl = exclude_patterns or []
        # Use the same _callback_id logic as the poll thread so events are
        # recorded under the real watch_id, not the "__unc_notify" variant.
        _UNC_SUFFIX = "__unc_notify"
        _callback_id = watch_id[: -len(_UNC_SUFFIX)] if watch_id.endswith(_UNC_SUFFIX) else watch_id

        # FIX: Pass a reference to _snap_reset_events into the thread closure
        # so that on buffer overflow the notify thread can wake the supplemental
        # poll immediately and recover any missed deletions/changes.
        _snap_reset_events_ref = self._snap_reset_events

        def _notify_thread():
            try:
                import ctypes
                import ctypes.wintypes as wt

                kernel32 = ctypes.windll.kernel32

                # ── Constants ────────────────────────────────────────────────
                FILE_LIST_DIRECTORY        = 0x0001
                FILE_SHARE_READ            = 0x00000001
                FILE_SHARE_WRITE           = 0x00000002
                FILE_SHARE_DELETE          = 0x00000004   # ← key: tells SMB server to notify all sessions
                OPEN_EXISTING              = 3
                FILE_FLAG_BACKUP_SEMANTICS = 0x02000000   # required to open a directory
                FILE_FLAG_OVERLAPPED       = 0x40000000
                INVALID_HANDLE_VALUE       = ctypes.c_void_p(-1).value

                FILE_NOTIFY_CHANGE_FILE_NAME  = 0x00000001
                FILE_NOTIFY_CHANGE_DIR_NAME   = 0x00000002
                FILE_NOTIFY_CHANGE_LAST_WRITE = 0x00000010
                FILE_NOTIFY_CHANGE_SIZE       = 0x00000008
                NOTIFY_FILTER = (
                    FILE_NOTIFY_CHANGE_FILE_NAME
                    | FILE_NOTIFY_CHANGE_DIR_NAME
                    | FILE_NOTIFY_CHANGE_LAST_WRITE
                    | FILE_NOTIFY_CHANGE_SIZE
                )

                # FILE_NOTIFY_INFORMATION action codes
                ACTION_ADDED    = 1
                ACTION_REMOVED  = 2
                ACTION_MODIFIED = 3
                ACTION_RENAMED_OLD = 4
                ACTION_RENAMED_NEW = 5

                ACTION_MAP = {
                    ACTION_ADDED:       "added",
                    ACTION_REMOVED:     "deleted",
                    ACTION_MODIFIED:    "modified",
                    ACTION_RENAMED_OLD: "renamed",
                    ACTION_RENAMED_NEW: None,   # skip — paired with RENAMED_OLD
                }

                # ── Open a handle to the UNC directory ───────────────────────
                hDir = kernel32.CreateFileW(
                    path,
                    FILE_LIST_DIRECTORY,
                    FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                    None,
                    OPEN_EXISTING,
                    FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OVERLAPPED,
                    None,
                )
                if hDir == INVALID_HANDLE_VALUE:
                    err = ctypes.GetLastError()
                    logger.warning(
                        f"[unc_notify] CreateFileW failed for {path} "
                        f"(error {err}) — falling back to poll"
                    )
                    return

                logger.info(f"[unc_notify] Real-time SMB2 CHANGE_NOTIFY active for: {path}")

                # 64 KB per call — but robocopy during a backup generates a huge
                # flood of notifications that can overflow the buffer.  Use 256 KB
                # to reduce overflow risk.  We handle overflow explicitly below.
                BUF_SIZE   = 262144
                buf        = ctypes.create_string_buffer(BUF_SIZE)
                bytes_ret  = wt.DWORD(0)

                ERROR_NOTIFY_ENUM_DIR = 1022  # buffer overflow — must reopen handle

                def _open_handle():
                    h = kernel32.CreateFileW(
                        path,
                        FILE_LIST_DIRECTORY,
                        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                        None,
                        OPEN_EXISTING,
                        FILE_FLAG_BACKUP_SEMANTICS,   # synchronous — simpler than OVERLAPPED
                        None,
                    )
                    return h

                hDir = _open_handle()
                if hDir == INVALID_HANDLE_VALUE:
                    err = ctypes.GetLastError()
                    logger.warning(
                        f"[unc_notify] CreateFileW failed for {path} "
                        f"(error {err}) — falling back to poll"
                    )
                    return

                logger.info(f"[unc_notify] Real-time SMB2 CHANGE_NOTIFY active for: {path}")

                # Exponential backoff state for ERROR_NOTIFY_ENUM_DIR reopen loop.
                # When a same-host robocopy backup is running it floods the SMB
                # CHANGE_NOTIFY buffer continuously; reopening the handle immediately
                # causes hundreds of reopen/overflow cycles per minute (visible in
                # logs as endless "Buffer overflow … reopening" pairs).  Backing off
                # exponentially reduces server hammering and log noise while the backup
                # is in progress, then resets once the handle stays stable.
                _overflow_backoff   = 0.5   # current wait before next reopen (seconds)
                _OVERFLOW_BACKOFF_MAX = 30.0  # cap — max 30s between reopen attempts
                _OVERFLOW_BACKOFF_RESET_AFTER = 60.0  # reset backoff after 60s of stability
                _OVERFLOW_COUNT      = 0      # consecutive overflow counter
                _OVERFLOW_GIVE_UP_AT = 20     # give up unc_notify after this many consecutive overflows
                import time as _time_mod
                _last_overflow_ts = 0.0  # monotonic time of last overflow

                while not stop_evt.is_set():
                    # Reset backoff + overflow counter if the handle has been stable long enough.
                    if _last_overflow_ts and (_time_mod.monotonic() - _last_overflow_ts) > _OVERFLOW_BACKOFF_RESET_AFTER:
                        if _OVERFLOW_COUNT > 0:
                            logger.info(f"[unc_notify] Handle stable for {_OVERFLOW_BACKOFF_RESET_AFTER}s — resetting overflow counter (was {_OVERFLOW_COUNT})")
                        _overflow_backoff = 0.5
                        _last_overflow_ts = 0.0
                        _OVERFLOW_COUNT   = 0

                    bytes_ret.value = 0
                    ok = kernel32.ReadDirectoryChangesW(
                        hDir,
                        buf,
                        BUF_SIZE,
                        True,          # watchSubtree
                        NOTIFY_FILTER,
                        ctypes.byref(bytes_ret),
                        None,          # lpOverlapped — synchronous call
                        None,          # lpCompletionRoutine
                    )
                    if stop_evt.is_set():
                        break

                    if not ok or bytes_ret.value == 0:
                        err = ctypes.GetLastError()

                        if err == ERROR_NOTIFY_ENUM_DIR or bytes_ret.value == 0:
                            # Buffer overflow — the SMB server dropped our CHANGE_NOTIFY
                            # subscription.  We must close and reopen the handle
                            # to re-register.  This happens when a same-host robocopy
                            # backup floods the notification queue for the source share.
                            #
                            # Use exponential backoff so we don't hammer the SMB server with
                            # hundreds of reopen attempts per minute while the backup
                            # is running.  The backoff doubles on each consecutive
                            # overflow and resets after _OVERFLOW_BACKOFF_RESET_AFTER
                            # seconds of handle stability.
                            logger.warning(
                                f"[unc_notify] Buffer overflow (ERROR_NOTIFY_ENUM_DIR) "
                                f"for {path} — reopening handle in {_overflow_backoff:.1f}s"
                            )
                            kernel32.CloseHandle(hDir)

                            # Wake the poll thread for a normal diff via the dedicated
                            # __wakeup event (NOT snap_reset_event, which would destroy
                            # the pre-deletion baseline).
                            # Rate-limit: only signal once per 12s to avoid an infinite
                            # overflow→wakeup→flush→overflow tight loop during robocopy.
                            import time as _t
                            _now_overflow = _t.monotonic()
                            _last_wakeup_sent = getattr(_notify_thread, '_last_wakeup_sent', 0.0)
                            if (_now_overflow - _last_wakeup_sent) >= 12.0:
                                _notify_thread._last_wakeup_sent = _now_overflow
                                try:
                                    _unc_poll_wakeup = _snap_reset_events_ref.get(
                                        _callback_id + "__unc_poll__wakeup"
                                    )
                                    if _unc_poll_wakeup is not None:
                                        _unc_poll_wakeup.set()
                                        logger.debug("[unc_notify] Sent wakeup signal to poll thread (overflow recovery)")
                                except Exception:
                                    pass
                            else:
                                logger.debug(
                                    f"[unc_notify] Overflow wakeup suppressed — "
                                    f"last sent {_now_overflow - _last_wakeup_sent:.1f}s ago (rate-limit 12s)"
                                )

                            stop_evt.wait(_overflow_backoff)
                            if stop_evt.is_set():
                                return

                            # Advance backoff (doubles each time, capped).
                            _last_overflow_ts  = _time_mod.monotonic()
                            _overflow_backoff  = min(_overflow_backoff * 2, _OVERFLOW_BACKOFF_MAX)
                            _OVERFLOW_COUNT   += 1

                            # If this SMB server cannot sustain a CHANGE_NOTIFY subscription
                            # (constant overflows even when idle), stop trying.
                            # The 15s snapshot-diff poll is the safety net and will
                            # catch all deletions on its own.
                            if _OVERFLOW_COUNT >= _OVERFLOW_GIVE_UP_AT:
                                logger.warning(
                                    f"[unc_notify] {_OVERFLOW_COUNT} consecutive buffer overflows "
                                    f"for {path} — this SMB server cannot maintain a stable CHANGE_NOTIFY "
                                    f"subscription. Disabling unc_notify; the 15s poll will "
                                    f"handle all change detection."
                                )
                                return

                            hDir = _open_handle()
                            if hDir == INVALID_HANDLE_VALUE:
                                logger.warning(f"[unc_notify] Reopen failed for {path} — giving up")
                                return
                            logger.info(
                                f"[unc_notify] Handle reopened — real-time detection restored for: {path} "
                                f"(overflow #{_OVERFLOW_COUNT})"
                            )
                            continue

                        if err in (995, 87, 6):
                            # 995 = ERROR_OPERATION_ABORTED (handle closed by stop)
                            # 87  = ERROR_INVALID_PARAMETER
                            # 6   = ERROR_INVALID_HANDLE
                            break
                        logger.warning(f"[unc_notify] ReadDirectoryChangesW error {err} — retrying")
                        stop_evt.wait(1)
                        continue

                    # ── Parse FILE_NOTIFY_INFORMATION records ────────────────
                    offset = 0
                    prev_old_name = ""
                    while offset < bytes_ret.value:
                        try:
                            next_offset  = ctypes.c_ulong.from_buffer_copy(buf, offset).value
                            action       = ctypes.c_ulong.from_buffer_copy(buf, offset + 4).value
                            fname_len    = ctypes.c_ulong.from_buffer_copy(buf, offset + 8).value
                            fname_bytes  = buf.raw[offset + 12: offset + 12 + fname_len]
                            fname        = fname_bytes.decode("utf-16-le", errors="replace")
                        except Exception:
                            break

                        etype = ACTION_MAP.get(action)
                        if etype is not None:
                            full_path = str(Path(path) / fname)
                            logger.debug(f"[unc_notify] Raw event: action={action} etype={etype!r} fname={fname!r} full_path={full_path!r}")
                            _excl_result = exclude_patterns and _is_excluded(fname, Path(full_path), _excl)
                            if _excl_result:
                                logger.debug(f"[unc_notify] EXCLUDED: {full_path!r} — dropping event")
                            if not _excl_result:
                                entry = {
                                    "type":             etype,
                                    "path":             full_path,
                                    "dest":             None,
                                    "timestamp":        datetime.now().isoformat(),
                                    "size":             0,
                                    "detection_source": "unc_notify",
                                }
                                if etype == "deleted":
                                    logger.warning(
                                        f"[unc_notify] Detected DELETION: {full_path!r} "
                                        f"(watch_id={_callback_id!r} overflow_count={_OVERFLOW_COUNT})"
                                    )
                                else:
                                    logger.info(f"[unc_notify] Detected {etype.upper()}: {full_path!r} (watch_id={_callback_id!r})")
                                if action == ACTION_RENAMED_OLD:
                                    prev_old_name = full_path
                                elif action == ACTION_RENAMED_NEW and prev_old_name:
                                    entry["type"] = "renamed"
                                    entry["dest"] = full_path
                                    entry["path"] = prev_old_name
                                    prev_old_name = ""

                                with _lock:
                                    bucket = _pending.setdefault(_callback_id, [])
                                    _pending[_callback_id] = [
                                        e for e in bucket if e["path"] != entry["path"]
                                    ]
                                    _pending[_callback_id].append(entry)

                                if on_change:
                                    try:
                                        on_change(_callback_id, entry)
                                    except Exception as cb_err:
                                        logger.warning(f"[unc_notify] callback error: {cb_err}")

                        if next_offset == 0:
                            break
                        offset += next_offset

                kernel32.CloseHandle(hDir)
                logger.info(f"[unc_notify] Stopped for: {path}")

            except Exception as exc:
                logger.warning(f"[unc_notify] Fatal error for {path}: {exc} — falling back to poll")

        t = threading.Thread(target=_notify_thread, daemon=True, name=f"unc-notify-{watch_id}")
        t.start()
        self._unc_notify_threads[unc_key] = t

        # Give the thread 0.5 s to open the handle.  If it exits immediately
        # (handle open failed), fall back to poll.
        t.join(timeout=0.5)
        if not t.is_alive():
            self._unc_notify_stop_events.pop(unc_key, None)
            self._unc_notify_threads.pop(unc_key, None)
            logger.warning(f"[unc_notify] Thread exited immediately for {path} — will use poll fallback")
            return False

        return True

    def _stop_unc_notify(self, watch_id: str):
        """Stop the real-time UNC notify thread for watch_id (if running)."""
        unc_key = watch_id + "__unc_notify"
        evt = self._unc_notify_stop_events.pop(unc_key, None)
        if evt:
            evt.set()
        self._unc_notify_threads.pop(unc_key, None)

    def _start_polling(self, watch_id: str, path: str, on_change: Optional[Callable], exclude_patterns: Optional[List[str]] = None, interval_min: int = 0, source_type: str = "local", smb_audit_cfg: Optional[dict] = None):
        """Simple polling fallback — checks mtimes every 60 seconds.
        For remote source types (sftp/ftp) build_snapshot is skipped and the
        poll just fires on_change every interval so the daemon re-downloads and
        re-diffs on each tick.
        """
        try:
            from backup_engine import build_snapshot, diff_snapshots
            import backup_engine as _backup_engine_mod
        except ImportError as e:
            import logging as _log
            _log.getLogger(__name__).error(f"[watcher] polling disabled — backup_engine import failed: {e}")
            return

        stop_event = threading.Event()
        self._poll_stop_events[watch_id] = stop_event
        # Per-thread event: set externally (via reset_snapshot / clear_pending)
        # to tell this thread to rebuild its snapshot baseline on the next wake-up.
        # NOTE: for __unc_poll threads this is intentionally NOT set by backup
        # completion — see clear_pending() / reset_snapshot().
        snap_reset_event = threading.Event()
        self._snap_reset_events[watch_id] = snap_reset_event
        # Separate wakeup event: set ONLY by the unc_notify thread on overflow to
        # wake the poll early for a normal diff — does NOT trigger baseline reset.
        # Stored under watch_id + "__wakeup" so the notify thread can reach it.
        snap_wakeup_event = threading.Event()
        self._snap_reset_events[watch_id + "__wakeup"] = snap_wakeup_event
        _excl = exclude_patterns or []

        # FIX: When this poll thread is a supplemental UNC poll (watch_id ends with
        # "__unc_poll"), events must be delivered under the *real* watch_id so the
        # UI and history log associate them with the correct watch entry.
        _UNC_SUFFIX = "__unc_poll"
        _callback_id = watch_id[: -len(_UNC_SUFFIX)] if watch_id.endswith(_UNC_SUFFIX) else watch_id
        # True when this poll thread is watching a backup destination folder
        # (watch_id ends with "__dest" or "__dest__unc_poll").
        _is_dest_poll = "__dest" in watch_id

        # FIX: Do NOT debounce the UNC supplemental poll callback.
        # The watchdog handler already debounces real-time events.  Adding a
        # second debounce here means any other activity on the share keeps
        # resetting the timer and can delay (or permanently suppress) deletion
        # notifications.  The poll fires at most every 60 s on its own, so
        # extra debouncing only adds latency with no benefit.
        _is_unc_poll = watch_id.endswith("__unc_poll")
        _debounced_cb = on_change if _is_unc_poll else self._make_debounced_callback(_callback_id, on_change)

        def _poll():
            # ── Remote source: no local snapshot to diff — just signal backup ──
            if source_type in ("sftp", "ftp", "ftps"):
                while not stop_event.is_set():
                    poll_secs = max(60, interval_min * 60) if interval_min > 0 else 300
                    stop_event.wait(poll_secs)
                    if stop_event.is_set():
                        break
                    entry = {
                        "type":      "remote_tick",
                        "path":      path,
                        "timestamp": datetime.now().isoformat(),
                        "size":      0,
                    }
                    with _lock:
                        _pending.setdefault(_callback_id, [])
                        _pending[_callback_id].append(entry)
                    if _debounced_cb:
                        try:
                            _debounced_cb(_callback_id, entry)
                        except Exception:
                            pass
                return

            # ── One-time startup flush + initial snapshot ─────────────────────
            # Flush SMB dir cache once to ensure the baseline is fresh.
            # We do NOT flush on every poll cycle — FindFirstFileW triggers
            # additional SMB CHANGE_NOTIFY buffer overflows during robocopy,
            # which would wake the poll every ~1s and advance the snapshot
            # baseline before any deletion can be detected.
            try:
                _backup_engine_mod.flush_smb_dir_cache(path)
                logger.info(f"[watcher] Flushed SMB dir cache before initial poll snapshot for: {path}")
            except Exception as exc:
                logger.warning(f"[watcher] Failed to flush SMB dir cache before initial poll: {exc}")

            snap = build_snapshot(path, exclude_patterns=_excl)
            logger.info(
                f"[watcher] UNC poll initial snapshot: {len(snap)} file(s) in {path} "
                f"(watch_id={_callback_id!r})"
            )
            logger.debug(f"[watcher] UNC poll initial snapshot files: {list(snap.keys())[:20]}")

            # Drain any snap_reset_event signals that arrived during startup
            # (unc_notify may set it before we even enter the loop).
            snap_reset_event.clear()
            snap_wakeup_event.clear()

            # Monotonic timestamp of the last completed diff cycle.
            # Used to rate-limit early wakeups from the unc_notify overflow signal.
            _last_diff_mono: float = 0.0
            # Monotonic deadline until which we use a faster 5-second poll
            # interval.  Set whenever an Office file (xlsx/docx/etc.) is
            # detected as "added" so that quick edits (user adds a file and
            # immediately opens + saves it) are caught within 5s rather than
            # waiting the full 15s cycle.
            _office_fast_until: float = 0.0

            # ── Main poll loop ────────────────────────────────────────────────
            _poll_cycle = 0
            while not stop_event.is_set():
                _poll_cycle += 1
                is_unc_poll = watch_id.endswith("__unc_poll")

                # Determine base sleep interval
                if is_unc_poll:
                    _in_office_fast_mode = time.monotonic() < _office_fast_until
                    poll_secs = 5 if _in_office_fast_mode else 15
                    if _in_office_fast_mode:
                        logger.info(
                            f"[watcher] UNC poll OFFICE FAST-POLL active: using 5s interval "
                            f"(watch_id={_callback_id!r} fast_mode_expires_in="
                            f"{_office_fast_until - time.monotonic():.1f}s)"
                        )
                else:
                    poll_secs = max(30, interval_min * 60) if interval_min > 0 else 60

                # ── Sleep phase ───────────────────────────────────────────────
                # We sleep for poll_secs but can wake early via two signals:
                #
                #   snap_reset_event  — set by backup completion on non-__unc_poll
                #                       threads.  Triggers a diff + baseline advance.
                #   snap_wakeup_event — set by unc_notify overflow recovery.
                #                       Only honoured if ≥ (poll_secs-5)s have elapsed
                #                       since the last diff, to prevent an infinite
                #                       overflow→wakeup→diff→overflow tight loop.
                #
                # CRITICAL: do NOT honour snap_wakeup_event on every overflow.
                # During robocopy, unc_notify overflows every 10s continuously.
                # If we wake and diff on each one, build_snapshot blocks on the
                # large file being copied, the poll thread hangs, and deletions
                # that happen after the backup are never detected.
                _min_wakeup_gap = max(poll_secs - 5, 10) if is_unc_poll else 0
                _deadline = time.monotonic() + poll_secs
                logger.info(
                    f"[watcher] UNC poll cycle #{_poll_cycle} sleeping {poll_secs}s "
                    f"(watch_id={_callback_id!r} baseline={len(snap)} file(s) "
                    f"min_wakeup_gap={_min_wakeup_gap}s "
                    f"snap_reset_set={snap_reset_event.is_set()} "
                    f"wakeup_set={snap_wakeup_event.is_set()})"
                )

                while not stop_event.is_set():
                    _remaining = _deadline - time.monotonic()
                    if _remaining <= 0:
                        break
                    # Wait at most 1s so we can check stop_event responsively
                    snap_wakeup_event.wait(min(_remaining, 1.0))
                    if snap_reset_event.is_set():
                        logger.debug(
                            f"[watcher] UNC poll cycle #{_poll_cycle} woken EARLY by snap_reset_event "
                            f"(watch_id={_callback_id!r} is_unc_poll={is_unc_poll} "
                            f"elapsed={time.monotonic() - (_deadline - poll_secs):.1f}s of {poll_secs}s)"
                        )
                        break
                    if snap_wakeup_event.is_set():
                        _elapsed = time.monotonic() - _last_diff_mono
                        if _elapsed >= _min_wakeup_gap:
                            logger.info(
                                f"[watcher] UNC poll cycle #{_poll_cycle} woken EARLY by overflow wakeup "
                                f"({_elapsed:.1f}s since last diff >= min={_min_wakeup_gap}s "
                                f"watch_id={_callback_id!r})"
                            )
                            snap_wakeup_event.clear()
                            break
                        else:
                            # Too soon — suppress and keep sleeping
                            logger.warning(
                                f"[watcher] UNC poll cycle #{_poll_cycle} overflow wakeup SUPPRESSED "
                                f"({_elapsed:.1f}s since last diff < min {_min_wakeup_gap}s — "
                                f"poll_secs={poll_secs}s watch_id={_callback_id!r})"
                            )
                            snap_wakeup_event.clear()

                if stop_event.is_set():
                    break

                # ── Diff phase ────────────────────────────────────────────────
                logger.info(
                    f"[watcher] UNC poll cycle #{_poll_cycle} entering diff phase "
                    f"(watch_id={_callback_id!r} is_unc_poll={is_unc_poll} "
                    f"baseline={len(snap)} file(s) "
                    f"snap_reset_set={snap_reset_event.is_set()} "
                    f"wakeup_set={snap_wakeup_event.is_set()})"
                )
                try:
                    # Handle snap_reset_event (non-__unc_poll only).
                    # For __unc_poll the baseline MUST survive backup completion —
                    # reset_snapshot() and clear_pending() intentionally skip it.
                    if snap_reset_event.is_set():
                        snap_reset_event.clear()
                        if is_unc_poll:
                            logger.info(
                                f"[watcher] UNC poll cycle #{_poll_cycle}: snap_reset_event received "
                                f"but INTENTIONALLY SKIPPED for __unc_poll — baseline preserved at "
                                f"{len(snap)} file(s) so post-backup deletions can still be detected "
                                f"(watch_id={_callback_id!r})"
                            )
                        if not is_unc_poll:
                            try:
                                _backup_engine_mod.flush_smb_dir_cache(path)
                            except Exception:
                                pass
                            new_snap = build_snapshot(path, previous=snap, exclude_patterns=_excl)
                            # After a backup reset, ALWAYS use size_only=True:
                            # robocopy preserves source mtime so the first post-backup
                            # diff will see mtime drift on every file — even on local
                            # source watchers.  Only flag genuine size changes.
                            overflow_changes = diff_snapshots(snap, new_snap, size_only=True)
                            if overflow_changes:
                                logger.info(
                                    f"[watcher] poll caught {len(overflow_changes)} "
                                    f"change(s) after baseline reset for: {path}"
                                )
                            for c in overflow_changes:
                                rel_path  = c["path"]
                                full_path = str(Path(path) / rel_path)
                                entry = {
                                    **c,
                                    "path": full_path,
                                    "timestamp": datetime.now().isoformat(),
                                    "detection_source": "poll_reset_recovery",
                                }
                                # Check suppression hook — same check used by the normal
                                # poll diff path.  Without this, false "modified" entries
                                # caused by mtime drift right after a backup bypass the
                                # 60-second post-backup grace window and fire on_change,
                                # showing a spurious "1 change (modified)" badge and
                                # history entry even though nothing actually changed.
                                if _history_persist_suppressor is not None:
                                    try:
                                        if _history_persist_suppressor(_callback_id, entry["type"], full_path):
                                            logger.debug(
                                                f"[watcher] poll_reset_recovery SUPPRESSED by hook: "
                                                f"watch_id={_callback_id!r} type={entry['type']!r} "
                                                f"path={full_path!r}"
                                            )
                                            continue
                                    except Exception:
                                        pass
                                with _lock:
                                    bucket = _pending.setdefault(_callback_id, [])
                                    _pending[_callback_id] = [e for e in bucket if e["path"] != full_path]
                                    _pending[_callback_id].append(entry)
                                if _debounced_cb:
                                    try:
                                        _debounced_cb(_callback_id, entry)
                                    except Exception:
                                        pass
                            snap = new_snap
                            _last_diff_mono = time.monotonic()
                            logger.info(f"[watcher] UNC poll snapshot reset after backup for: {path}")
                            continue  # restart sleep phase with fresh baseline

                    # Normal diff cycle — flush SMB dir cache before scanning.
                    # The Windows SMB2 client caches directory listings for up to
                    # ~10-30s. When a remote machine (coworker's PC) deletes a file,
                    # os.scandir() will still return the deleted file from cache,
                    # making diff_snapshots() see old=1 → new=1 and miss the deletion.
                    # We must flush before EVERY poll scan, not just at startup.
                    # NOTE: The original "do NOT flush every cycle" comment was to
                    # avoid triggering extra SMB CHANGE_NOTIFY overflows DURING
                    # robocopy. We guard against that by checking if a backup is
                    # actively running (_backup_engine_mod) — but the simplest and
                    # correct fix is: always flush. FindFirstFileW on a 1-file dir
                    # is near-instant and the overflow risk only applies during
                    # active large-file copies, not during idle polling.
                    if is_unc_poll:
                        try:
                            _backup_engine_mod.flush_smb_dir_cache(path)
                            logger.info(
                                f"[watcher] UNC poll cycle #{_poll_cycle} flushed SMB dir cache "
                                f"before scan (watch_id={_callback_id!r})"
                            )
                        except Exception as _flush_exc:
                            logger.warning(
                                f"[watcher] UNC poll cycle #{_poll_cycle} SMB cache flush failed: "
                                f"{_flush_exc} — scan may see stale directory listing "
                                f"(watch_id={_callback_id!r})"
                            )

                    logger.info(
                        f"[watcher] UNC poll cycle #{_poll_cycle} starting diff: baseline={len(snap)} file(s) "
                        f"(watch_id={_callback_id!r})"
                    )

                    # Use a short-timeout snapshot scan so that a large file being
                    # actively copied by robocopy does NOT block this thread for
                    # minutes.  If the scan times out we skip this cycle and retry
                    # on the next tick — the baseline is preserved so we will still
                    # catch any deletion once the copy finishes and the scan succeeds.
                    _scan_cancel = threading.Event()
                    _scan_result: dict = {}

                    def _do_scan_thread():
                        try:
                            result = build_snapshot(
                                path, previous=snap, exclude_patterns=_excl
                            )
                            _scan_result["snap"] = result
                            # Log every file seen in the scan so we can confirm
                            # whether the SMB server is returning stale/deleted entries
                            logger.info(
                                f"[watcher] UNC poll cycle #{_poll_cycle} scan returned "
                                f"{len(result)} file(s): {sorted(result.keys())} "
                                f"(watch_id={_callback_id!r})"
                            )
                        except Exception as exc:
                            _scan_result["error"] = exc

                    _scan_thread = threading.Thread(target=_do_scan_thread, daemon=True,
                                                    name=f"poll-scan-{watch_id}")
                    _scan_thread.start()
                    logger.info(
                        f"[watcher] UNC poll cycle #{_poll_cycle} scan thread started for {path} "
                        f"(watch_id={_callback_id!r})"
                    )
                    # Give the scan up to 60s.  For 1-file shares this is generous;
                    # for large shares it may occasionally time out under heavy SMB
                    # load, but we will retry on the next 15s cycle.
                    _scan_thread.join(timeout=60)

                    if _scan_thread.is_alive():
                        logger.warning(
                            f"[watcher] UNC poll scan timed out after 60s for {path} "
                            f"— skipping this cycle, baseline preserved for next tick"
                        )
                        continue  # do NOT advance snap; retry next cycle

                    if "error" in _scan_result:
                        logger.warning(
                            f"[watcher] UNC poll scan error for {path}: {_scan_result['error']} "
                            f"— skipping this cycle"
                        )
                        continue

                    new_snap = _scan_result["snap"]
                    # Use size_only=True for ALL poll watches — both destination-folder
                    # watches (where robocopy mtime drift is well-known) and regular
                    # source watches (where the first post-backup diff sees the same
                    # mtime drift on local files).  Watchdog handles real mtime-based
                    # modifications on local paths in real-time; the poll is only a
                    # safety net for UNC/network paths where watchdog can miss events.
                    # size_only=True ignores mtime and only flags genuine size changes,
                    # preventing spurious "modified" entries after every backup.
                    changes  = diff_snapshots(snap, new_snap, size_only=True)

                    logger.info(
                        f"[watcher] UNC poll cycle #{_poll_cycle} diff result: "
                        f"old={len(snap)} → new={len(new_snap)} file(s), "
                        f"{len(changes)} change(s) (watch_id={_callback_id!r})"
                    )
                    if changes:
                        logger.info(
                            f"[watcher] UNC poll found {len(changes)} change(s) for "
                            f"watch_id={_callback_id!r}: {[(c['type'], c['path']) for c in changes]}"
                        )
                    # Debug: show files present in BOTH snapshots that were NOT
                    # flagged as changed — helps diagnose missed modifications.
                    if not _is_dest_poll:
                        _unchanged_common = [
                            (rel,
                             snap[rel].get("size"), snap[rel].get("mtime"),
                             new_snap[rel].get("size"), new_snap[rel].get("mtime"))
                            for rel in snap
                            if rel in new_snap
                            and not any(c["path"] == rel for c in changes)
                        ]
                        if _unchanged_common:
                            logger.info(
                                f"[watcher] UNC poll cycle #{_poll_cycle} UNCHANGED "
                                f"(in both snapshots, not flagged as modified) "
                                f"watch_id={_callback_id!r}: "
                                + "; ".join(
                                    f"{r}(old_size={os_},{om_} → new_size={ns_},{nm_})"
                                    for r, os_, om_, ns_, nm_ in _unchanged_common
                                )
                            )

                    # WARNING-level per-cycle status for Office files so this
                    # survives log truncation.  Shows mtime/size comparison and
                    # whether the file was detected as changed, unchanged, added,
                    # or absent — critical for diagnosing missed xlsx modifications.
                    import os as _os_office
                    _OFFICE_EXTS = {'.xlsx', '.xls', '.docx', '.doc', '.pptx',
                                    '.ppt', '.xlsm', '.xlsb', '.docm', '.pptm'}
                    for _o_rel in sorted(set(list(snap.keys()) + list(new_snap.keys()))):
                        if _os_office.path.splitext(_o_rel)[1].lower() not in _OFFICE_EXTS:
                            continue
                        _o_old = snap.get(_o_rel)
                        _o_new = new_snap.get(_o_rel)
                        _o_change_type = next(
                            (c["type"] for c in changes if c["path"] == _o_rel), None
                        )
                        if _o_old is None and _o_new is None:
                            continue
                        # Build old/new strings without backslashes inside f-expr
                        # (Python <3.12 does not allow backslash in f-string exprs)
                        if _o_old is None:
                            _o_old_str = "ABSENT"
                        else:
                            _o_old_str = "size=%s,mtime=%s" % (
                                _o_old.get("size"), _o_old.get("mtime"))
                        if _o_new is None:
                            _o_new_str = "ABSENT"
                        else:
                            _o_new_str = "size=%s,mtime=%s" % (
                                _o_new.get("size"), _o_new.get("mtime"))
                        _o_status = (
                            _o_change_type.upper() if _o_change_type else "UNCHANGED"
                        )
                        logger.warning(
                            f"[watcher] UNC poll cycle #{_poll_cycle} OFFICE FILE "
                            f"watch_id={_callback_id!r} file={_o_rel!r}: "
                            f"old={_o_old_str} → new={_o_new_str} status={_o_status}"
                        )

                    for c in changes:
                        rel_path  = c["path"]
                        full_path = str(Path(path) / rel_path)
                        entry = {
                            **c,
                            "path":             full_path,
                            "timestamp":        datetime.now().isoformat(),
                            "detection_source": "unc_poll" if is_unc_poll else "poll",
                        }

                        # Check suppression hook before touching history or calling on_change
                        if _history_persist_suppressor is not None:
                            try:
                                if _history_persist_suppressor(_callback_id, entry["type"], full_path):
                                    logger.debug(
                                        f"[watcher] UNC poll SUPPRESSED by hook: "
                                        f"watch_id={_callback_id!r} type={entry['type']!r} "
                                        f"path={full_path!r}"
                                    )
                                    continue
                            except Exception:
                                pass

                        with _lock:
                            bucket = _pending.setdefault(_callback_id, [])
                            _pending[_callback_id] = [e for e in bucket if e["path"] != full_path]
                            _pending[_callback_id].append(entry)

                            if entry["type"] == "deleted":
                                logger.warning(
                                    f"[watcher] POLL detected DELETION: {entry['path']} "
                                    f"(watch_id={_callback_id!r})"
                                )
                                # unc_poll fires up to 15s after the actual delete, so the
                                # SMB session is almost certainly already closed.  But we
                                # still attempt NetSessionEnum here as a best-effort:
                                # occasionally a user has many files open on the same share
                                # and their session persists past the delete.  If we get a
                                # hit we attach smb_sessions_snapshot so _get_editor_info
                                # can resolve the user identity via SMB session enumeration.
                                _poll_smb_host = ""
                                _fp_norm = full_path.replace("\\\\", "\\")
                                if _fp_norm.startswith("\\"):
                                    _parts = _fp_norm.lstrip("\\").split("\\")
                                    _poll_smb_host = _parts[0] if _parts else ""
                                if _poll_smb_host:
                                    try:
                                        import socket as _s_poll
                                        # Retrieve SMB credentials from the closure (captured
                                        # when the poll thread was started via smb_audit_cfg).
                                        _poll_nas_cfg  = smb_audit_cfg if smb_audit_cfg else {}
                                        _nas_user_poll = _poll_nas_cfg.get("username", "") if isinstance(_poll_nas_cfg, dict) else ""
                                        _nas_pass_poll = _poll_nas_cfg.get("password", "") if isinstance(_poll_nas_cfg, dict) else ""
                                        if _nas_user_poll and _nas_pass_poll:
                                            try:
                                                import win32net as _wn_poll, win32netcon as _wnc_poll
                                                _USE_IPC_P = getattr(_wnc_poll, "USE_IPC", 3)
                                                _wn_poll.NetUseAdd(None, 1, {
                                                    "remote":     f"\\\\{_poll_smb_host}\\IPC$",
                                                    "username":   _nas_user_poll,
                                                    "password":   _nas_pass_poll,
                                                    "domainname": "",
                                                    "asg_type":   _USE_IPC_P,
                                                })
                                                logger.debug(
                                                    f"[watcher] POLL deletion IPC$ auth OK for {_poll_smb_host!r}"
                                                )
                                            except Exception as _poll_ipc_err:
                                                logger.debug(
                                                    f"[watcher] POLL deletion IPC$ auth failed: {_poll_ipc_err!r} — proceeding"
                                                )
                                        else:
                                            logger.info(
                                                f"[watcher] POLL deletion — no SMB credentials; "
                                                f"NetSessionEnum on {_poll_smb_host!r} may be empty. "
                                                f"Add credentials in watch settings for reliable attribution."
                                            )
                                        _own_h_poll  = ""
                                        _own_ip_poll = ""
                                        try:
                                            _own_h_poll  = _s_poll.gethostname().lower()
                                            _own_ip_poll = _s_poll.gethostbyname(_own_h_poll)
                                        except Exception:
                                            pass
                                        # Use the process-level probe cache so we always use the
                                        # correct calling convention for this pywin32 build.
                                        _nse_fn = _get_net_session_enum_fn("[watcher POLL deletion] ")
                                        _raw_sess = []
                                        if _nse_fn is not None:
                                            try:
                                                _raw_sess = _nse_fn(_poll_smb_host)
                                            except Exception as _poll_nse_err:
                                                logger.debug(
                                                    f"[watcher] POLL deletion NetSessionEnum call failed: {_poll_nse_err!r}"
                                                )
                                        _poll_snap = []
                                        for _ps in _raw_sess:
                                            _pc = (_ps.get("client_name") or "").lstrip("\\").lower()
                                            _pu = _ps.get("user_name") or _ps.get("username") or ""
                                            try:
                                                _pip = _s_poll.gethostbyname(_pc) if _pc else ""
                                            except Exception:
                                                _pip = _pc
                                            _is_own = (_pc == _own_h_poll or (_own_ip_poll and _pip == _own_ip_poll))
                                            logger.debug(
                                                f"[watcher] POLL deletion NetSessionEnum session: "
                                                f"client={_pc!r} user={_pu!r} ip={_pip!r} is_own={_is_own}"
                                            )
                                            if _pc and not _is_own and _pu:
                                                _poll_snap.append({"username": _pu, "machine": _pc, "ip": _pip})
                                        if _poll_snap:
                                            entry["smb_sessions_snapshot"] = _poll_snap
                                            logger.info(
                                                f"[watcher] POLL deletion SMB snapshot captured: {_poll_snap} "
                                                f"path={entry['path']!r} (watch_id={_callback_id!r})"
                                            )
                                        else:
                                            logger.info(
                                                f"[watcher] POLL deletion — no SMB session found on {_poll_smb_host!r} "
                                                f"(session likely already closed ~15s after delete; "
                                                f"attribution will rely on SMB session enumeration). "
                                                f"path={entry['path']!r} detection_source={entry['detection_source']!r} "
                                                f"(watch_id={_callback_id!r})"
                                            )
                                    except Exception as _poll_snap_err:
                                        logger.info(
                                            f"[watcher] POLL deletion — SMB snapshot attempt failed: {_poll_snap_err!r} "
                                            f"(watch_id={_callback_id!r})"
                                        )
                                else:
                                    logger.info(
                                        f"[watcher] POLL deletion — could not parse SMB host from path={full_path!r}; "
                                        f"no smb_sessions_snapshot attempted. (watch_id={_callback_id!r})"
                                    )

                            # Persist to history immediately
                            # Skip when desktop_app is running — it saves history
                            # AFTER attribution to avoid blank user/machine rows.
                            if config_manager is not None and not _defer_history_to_app:
                                try:
                                    history = config_manager.load_history()
                                    history_entry = {
                                        "type":             entry["type"],
                                        "path":             entry["path"],
                                        "dest":             entry.get("dest"),
                                        "timestamp":        entry["timestamp"],
                                        "size":             entry.get("size"),
                                        "detection_source": entry["detection_source"],
                                        "watch_id":         _callback_id,
                                    }
                                    history.append(history_entry)
                                    logger.info(f"[watcher] UNC poll cycle #{_poll_cycle} persisting to history: {history_entry}")
                                    if len(history) > 5000:
                                        history = history[-5000:]
                                    config_manager.save_history(history)
                                    if entry["type"] == "deleted":
                                        logger.warning(
                                            f"[watcher] POLL wrote deletion to history: "
                                            f"{entry['path']} (watch_id={_callback_id!r})"
                                        )
                                except Exception as exc:
                                    logger.warning(f"[watcher] Poll failed to persist history: {exc}")

                        if _debounced_cb:
                            try:
                                _debounced_cb(_callback_id, entry)
                            except Exception as cb_err:
                                logger.warning(f"[watcher] poll callback error: {cb_err}")

                    # If any Office file was just ADDED, activate fast-poll mode
                    # so a quick follow-up edit (user adds xlsx and immediately
                    # opens + saves it) is caught within 5s instead of 15s.
                    import os as _os_fp
                    _OFFICE_EXTS_FP = {'.xlsx', '.xls', '.docx', '.doc', '.pptx',
                                       '.ppt', '.xlsm', '.xlsb', '.docm', '.pptm'}
                    for _c_fp in changes:
                        if (
                            _c_fp.get("type") == "added"
                            and _os_fp.path.splitext(_c_fp.get("path", ""))[1].lower()
                                in _OFFICE_EXTS_FP
                        ):
                            _office_fast_until = time.monotonic() + 30.0
                            logger.warning(
                                f"[watcher] UNC poll OFFICE ADDED — fast-poll mode ON "
                                f"(5s for 30s): file={_c_fp['path']!r} "
                                f"watch_id={_callback_id!r}"
                            )
                            break

                    # Advance baseline only after a successful diff
                    snap = new_snap
                    _last_diff_mono = time.monotonic()
                    logger.info(
                        f"[watcher] UNC poll cycle #{_poll_cycle} complete: "
                        f"baseline advanced to {len(snap)} file(s) "
                        f"changes_found={len(changes)} "
                        f"(watch_id={_callback_id!r})"
                    )
                    if changes:
                        logger.info(
                            f"[watcher] UNC poll cycle #{_poll_cycle} CHANGES DISPATCHED: "
                            f"{[(c['type'], c['path']) for c in changes]} "
                            f"(watch_id={_callback_id!r})"
                        )
                    else:
                        logger.info(
                            f"[watcher] UNC poll cycle #{_poll_cycle} no changes detected "
                            f"(watch_id={_callback_id!r})"
                        )

                except Exception as poll_err:
                    logger.warning(f"[watcher] UNC poll error for {path}: {poll_err}", exc_info=True)

        t = threading.Thread(target=_poll, daemon=True, name=f"poll-{watch_id}")
        t.start()
        self._poll_threads[watch_id] = t