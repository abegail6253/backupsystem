"""Shared event-handler base class for watchdog file-system events.

All watcher module state (_pending, _lock, _history_persist_suppressor,
_defer_history_to_app, config_manager, etc.) is accessed via a deferred
``import watcher as _watcher`` inside each method.  This avoids the
circular import that would occur if watcher.py imported this module at
the top level while this module also imported watcher at the top level.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)


class BaseEventHandler:
    """Carries the shared _is_excluded / _record pipeline.

    Subclasses implement ``handle(event)`` which maps a watchdog
    FileSystemEvent to a ``_record()`` call with the correct event_type.
    """

    def __init__(
        self,
        watch_id: str,
        on_change: Optional[Callable],
        exclude_patterns: Optional[List[str]],
        smb_audit_cfg: Optional[dict],
    ):
        self.watch_id        = watch_id
        self.on_change       = on_change
        self.exclude_patterns = exclude_patterns or []
        self.smb_audit_cfg   = smb_audit_cfg or {}

    # ── exclusion filter ──────────────────────────────────────────────────────

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

    # ── core recording pipeline ───────────────────────────────────────────────

    def _record(self, event_type: str, src: str, dest: Optional[str] = None):
        import watcher as _watcher
        logger.debug(f"[watchdog._record] event_type={event_type!r} src={src!r} dest={dest!r} watch_id={self.watch_id!r}")
        if self._is_excluded(src):
            logger.debug(f"[watchdog._record] EXCLUDED: {src!r} — skipping")
            return
        entry = {
            "type":             event_type,
            "path":             src,
            "dest":             dest,
            "timestamp":        datetime.now().isoformat(),
            "size":             _watcher._safe_size(src),
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
                    _nse_fn = _watcher._get_net_session_enum_fn("[watchdog._record] ")
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
                    _own_min_it = None   # track shortest own-session idle_time
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
                            _at = _s.get("active_time")
                            _it = _s.get("idle_time")
                            if _at is not None:
                                if _own_min_at is None or _at < _own_min_at:
                                    _own_min_at = _at
                            if _it is not None:
                                if _own_min_it is None or _it < _own_min_it:
                                    _own_min_it = _it
                    if _own_min_at is not None:
                        logger.info(
                            f"[watchdog._record] own-session min active_time={_own_min_at}s "
                            f"(will be stored as 'own_min_active_time') "
                            f"(watch_id={self.watch_id!r} type={event_type!r})"
                        )
                    if _own_min_it is not None:
                        logger.info(
                            f"[watchdog._record] own-session min idle_time={_own_min_it}s "
                            f"(will be stored as 'own_min_idle_time') "
                            f"(watch_id={self.watch_id!r} type={event_type!r})"
                        )
                    if _out:
                        if _own_min_at is not None:
                            _out[0]["own_min_active_time"] = _own_min_at
                        if _own_min_it is not None:
                            _out[0]["own_min_idle_time"] = _own_min_it
                    if not _out:
                        logger.info(
                            f"[watchdog._record] _build_snapshot: no usable remote sessions. "
                            f"own_host={_own_host!r} own_ip={_own_ip!r} "
                            f"(watch_id={self.watch_id!r} type={event_type!r}). "
                            f"HINT: if the Windows PC host ({_smb_host!r}) is both source and "
                            f"watcher, make sure SMB credentials in settings match the PC's "
                            f"admin account — without valid credentials NetSessionEnum returns "
                            f"an empty list even when sessions exist."
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
                        with _watcher._path_smb_snapshot_lock:
                            _watcher._path_smb_snapshot[_pkey] = (_snap_ts.time(), list(_snapped))
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
        if _watcher._history_persist_suppressor is not None:
            try:
                if _watcher._history_persist_suppressor(self.watch_id, event_type, src):
                    logger.info(
                        f"[watchdog._record] SUPPRESSED by hook: "
                        f"watch_id={self.watch_id!r} type={event_type!r} path={src!r} "
                        f"— event dropped before _pending queue (see "
                        f"[_dest_event_suppressed] log above for reason)"
                    )
                    return
            except Exception:
                pass

        with _watcher._lock:
            bucket = _watcher._pending.setdefault(self.watch_id, [])
            # Remove existing entry for this path if it exists
            _watcher._pending[self.watch_id] = [e for e in bucket if e["path"] != src]
            _watcher._pending[self.watch_id].append(entry)
            # Cap memory usage — prune to 2500 when over limit
            if len(_watcher._pending[self.watch_id]) > 2500:
                _watcher._pending[self.watch_id] = _watcher._pending[self.watch_id][-2500:]

            # Persist to history.json immediately (capped at 5000 entries)
            # Skip when desktop_app is running — it saves history AFTER
            # attribution (user/machine/IP enrichment) to avoid blank rows.
            if _watcher.config_manager is not None and not _watcher._defer_history_to_app:
                try:
                    # Load current history, append, and save
                    history = _watcher.config_manager.load_history()
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
                    _watcher.config_manager.save_history(history)
                except Exception as e:
                    logger.warning(f"[watcher] Failed to persist history: {e}")

        if self.on_change:
            try:
                self.on_change(self.watch_id, entry)
            except Exception:
                pass
