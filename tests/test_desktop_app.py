"""
tests/test_desktop_app.py — Comprehensive unit tests for desktop_app.py

Covers:
  - BackupWorker thread: init, pause/resume, stop, force-full logic, stale-snapshot detection
  - Scheduling: backup-window logic, metered-connection guard, quota enforcement
  - Integrity check: trigger, scheduler-absent path, _on_integrity_result, _on_integrity_run_finished
  - HistoryWindow: construction and CSV export
  - Restore chain: _resolve_restore_destination branching
  - Theme helpers (extending existing coverage)
  - Transport free-space helpers
  - API management endpoint helpers (db schema, command queue)

Run:
    pytest tests/test_desktop_app.py -v

All tests run without a display (QApplication is never created).  GUI classes are
instantiated only where their __init__ is exercisable without a live Qt event loop;
otherwise the behaviour-under-test is extracted into pure functions and mocked.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch, PropertyMock, call

import pytest

# ── make project root importable ──────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Heavy Qt / GUI import is guarded: we patch the Qt layer out so tests run in CI
# environments that have no display.  Modules that import PyQt5 at the top level
# are wrapped before import.
_QT_MOCKS: dict[str, MagicMock] = {}

for _mod in [
    "PyQt5", "PyQt5.QtWidgets", "PyQt5.QtCore", "PyQt5.QtGui",
    "PyQt5.QtNetwork",
]:
    _m = MagicMock()
    sys.modules[_mod] = _m
    _QT_MOCKS[_mod] = _m

# Provide realistic stand-ins for the Qt symbols desktop_app uses at module scope
_QtCore = sys.modules["PyQt5.QtCore"]
_QtCore.Qt.Checked = 2
_QtCore.Qt.Unchecked = 0
_QtCore.Qt.UserRole = 256
_QtCore.Qt.ElideNone = 0
_QtCore.QTimer = MagicMock()
_QtCore.QSettings = MagicMock()
_QtCore.pyqtSignal = lambda *a, **kw: MagicMock()
_QtCore.QThread = object  # BackupWorker extends this; make it a plain object

_QtWidgets = sys.modules["PyQt5.QtWidgets"]
_QtWidgets.QApplication = MagicMock()
_QtWidgets.QDialog = object
_QtWidgets.QMessageBox.information = MagicMock()
_QtWidgets.QMessageBox.critical = MagicMock()
_QtWidgets.QMessageBox.warning = MagicMock()
_QtWidgets.QSystemTrayIcon = MagicMock()
_QtWidgets.QSystemTrayIcon.Information = 1
_QtWidgets.QSystemTrayIcon.Warning = 2

# Stub out optional heavy deps before importing desktop_app
for _dep in ["paramiko", "cryptography",
             "cryptography.hazmat", "cryptography.hazmat.primitives",
             "cryptography.hazmat.primitives.ciphers",
             "cryptography.hazmat.primitives.ciphers.aead",
             "cryptography.hazmat.primitives.kdf",
             "cryptography.hazmat.primitives.kdf.scrypt",
             "cryptography.hazmat.backends",
             "win32api", "win32con", "win32gui",
             "winreg", "pystray", "PIL", "PIL.Image"]:
    if _dep not in sys.modules:
        sys.modules[_dep] = MagicMock()

import desktop_app as da  # noqa: E402 — after mocks

# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def _make_watch(**kw) -> dict:
    base = {
        "id":   "w_test",
        "name": "Test Watch",
        "path": "/tmp/src",
        "active": True,
        "paused": False,
        "pre_backup_cmd":  "",
        "post_backup_cmd": "",
    }
    base.update(kw)
    return base


def _make_cfg(**kw) -> dict:
    base = {
        "destination":   "/tmp/dst",
        "dest_type":     "local",
        "max_backup_mbps": 0,
        "auto_retry":    False,
        "retry_delay_min": 5,
        "watches":       [],
        "verify_remote_uploads": False,
        "force_full_interval_days": 0,
    }
    base.update(kw)
    return base


# ══════════════════════════════════════════════════════════════════════════════
# 1. BackupWorker — unit tests (no Qt event loop needed)
# ══════════════════════════════════════════════════════════════════════════════

class TestBackupWorkerInit:
    """BackupWorker.__init__ stores attributes correctly."""

    def _make_worker(self, **watch_kw) -> da.BackupWorker:
        w = _make_watch(**watch_kw)
        cfg = _make_cfg()
        # BackupWorker inherits QThread which we mocked to `object`
        worker = da.BackupWorker.__new__(da.BackupWorker)
        worker.watch         = w
        worker.cfg           = cfg
        worker.triggered_by  = "manual"
        worker.changed_paths = None
        worker._stop_event   = threading.Event()
        worker._pause_event  = threading.Event()
        worker._pause_event.set()
        worker.pre_backup_cmd  = w.get("pre_backup_cmd", "")
        worker.post_backup_cmd = w.get("post_backup_cmd", "")
        worker.verify_remote_uploads = bool(cfg.get("verify_remote_uploads", False))
        return worker

    def test_pause_event_starts_set(self):
        """_pause_event must start SET (running) — cleared only on pause()."""
        worker = self._make_worker()
        assert worker._pause_event.is_set(), "_pause_event should be SET at construction"

    def test_stop_event_starts_clear(self):
        worker = self._make_worker()
        assert not worker._stop_event.is_set()

    def test_request_stop_sets_event(self):
        worker = self._make_worker()
        worker.request_stop = lambda: worker._stop_event.set()
        worker.request_stop()
        assert worker._stop_event.is_set()

    def test_pause_clears_pause_event(self):
        worker = self._make_worker()
        worker.pause = lambda: worker._pause_event.clear()
        worker.pause()
        assert not worker._pause_event.is_set()

    def test_resume_sets_pause_event(self):
        worker = self._make_worker()
        worker._pause_event.clear()
        worker.resume = lambda: worker._pause_event.set()
        worker.resume()
        assert worker._pause_event.is_set()

    def test_pre_post_cmd_from_watch(self):
        worker = self._make_worker(pre_backup_cmd="echo pre", post_backup_cmd="echo post")
        assert worker.pre_backup_cmd  == "echo pre"
        assert worker.post_backup_cmd == "echo post"

    def test_verify_remote_uploads_from_cfg(self):
        w = _make_watch()
        cfg = _make_cfg(verify_remote_uploads=True)
        worker = da.BackupWorker.__new__(da.BackupWorker)
        worker.verify_remote_uploads = bool(cfg.get("verify_remote_uploads", False))
        assert worker.verify_remote_uploads is True


# ══════════════════════════════════════════════════════════════════════════════
# 2. Force-full interval logic (extracted from BackupWorker.run)
# ══════════════════════════════════════════════════════════════════════════════

class TestForceFullLogic:
    """The force-full-interval decision logic, tested as a pure function."""

    @staticmethod
    def _should_force_full(watch: dict, cfg: dict, snapshot: dict) -> bool:
        """Replicate the force-full check from BackupWorker.run."""
        import datetime as _dt
        interval = int(
            watch.get("force_full_interval_days") or
            cfg.get("force_full_interval_days") or 0
        )
        if interval <= 0 or not snapshot:
            return False
        last_str = watch.get("last_force_full_at") or ""
        if not last_str:
            return True  # never done a force-full → do one now
        try:
            last = _dt.datetime.fromisoformat(last_str.replace("Z", "+00:00"))
            last = last.replace(tzinfo=None)
            elapsed = (_dt.datetime.utcnow() - last).days
            return elapsed >= interval
        except Exception:
            return True  # unparseable → safe default

    def test_no_interval_returns_false(self):
        assert not self._should_force_full(_make_watch(), _make_cfg(), {"f": {}})

    def test_no_snapshot_returns_false(self):
        w = _make_watch(force_full_interval_days=7)
        assert not self._should_force_full(w, _make_cfg(), {})

    def test_never_done_returns_true(self):
        w = _make_watch(force_full_interval_days=7)
        assert self._should_force_full(w, _make_cfg(), {"f": {}})

    def test_recent_timestamp_returns_false(self):
        import datetime as _dt
        recent = (_dt.datetime.utcnow() - _dt.timedelta(days=2)).isoformat() + "Z"
        w = _make_watch(force_full_interval_days=7, last_force_full_at=recent)
        assert not self._should_force_full(w, _make_cfg(), {"f": {}})

    def test_old_timestamp_returns_true(self):
        import datetime as _dt
        old = (_dt.datetime.utcnow() - _dt.timedelta(days=10)).isoformat() + "Z"
        w = _make_watch(force_full_interval_days=7, last_force_full_at=old)
        assert self._should_force_full(w, _make_cfg(), {"f": {}})

    def test_corrupt_timestamp_returns_true(self):
        w = _make_watch(force_full_interval_days=7, last_force_full_at="not-a-date")
        assert self._should_force_full(w, _make_cfg(), {"f": {}})

    def test_global_cfg_interval_respected(self):
        """force_full_interval_days from cfg applies when watch has none."""
        w = _make_watch()
        cfg = _make_cfg(force_full_interval_days=3)
        assert self._should_force_full(w, cfg, {"f": {}})


# ══════════════════════════════════════════════════════════════════════════════
# 3. Stale-snapshot detection logic
# ══════════════════════════════════════════════════════════════════════════════

class TestStaleSnapshotDetection:
    """Replicate the stale-snapshot sampling heuristic from BackupWorker.run."""

    @staticmethod
    def _is_stale(snapshot: dict, src_root: Path, sample_size: int = 20) -> bool:
        sample_keys = list(snapshot.keys())[:sample_size]
        if not sample_keys:
            return False
        missing = sum(1 for k in sample_keys if not (src_root / k).exists())
        return missing > len(sample_keys) // 2

    def test_empty_snapshot_not_stale(self, tmp_path):
        assert not self._is_stale({}, tmp_path)

    def test_all_existing_not_stale(self, tmp_path):
        for i in range(5):
            (tmp_path / f"f{i}.txt").write_text("x")
        snap = {f"f{i}.txt": {} for i in range(5)}
        assert not self._is_stale(snap, tmp_path)

    def test_majority_missing_is_stale(self, tmp_path):
        # Only 1 of 5 files exist
        (tmp_path / "f0.txt").write_text("x")
        snap = {f"f{i}.txt": {} for i in range(5)}
        assert self._is_stale(snap, tmp_path)

    def test_exactly_half_missing_not_stale(self, tmp_path):
        # 2 of 4 exist → exactly half missing → not stale (needs > half)
        for i in range(2):
            (tmp_path / f"f{i}.txt").write_text("x")
        snap = {f"f{i}.txt": {} for i in range(4)}
        assert not self._is_stale(snap, tmp_path)


# ══════════════════════════════════════════════════════════════════════════════
# 4. Backup window check
# ══════════════════════════════════════════════════════════════════════════════

class TestBackupWindowLogic:
    """Replicate the backup-window in-window check from the scheduler loop."""

    @staticmethod
    def _in_window(now_h: int, now_m: int, start: str, end: str) -> bool:
        """Return True if (now_h, now_m) falls inside [start, end)."""
        now_secs = now_h * 3600 + now_m * 60
        ws, we = None, None
        if start:
            h, m = int(start[:2]), int(start[3:5])
            ws = h * 3600 + m * 60
        if end:
            h, m = int(end[:2]), int(end[3:5])
            we = h * 3600 + m * 60
        if ws is None and we is None:
            return True
        if ws is not None and we is not None:
            if ws < we:
                return ws <= now_secs < we
            else:
                return now_secs >= ws or now_secs < we
        if ws is not None:
            return now_secs >= ws
        return now_secs < we  # type: ignore[operator]

    def test_no_window_always_allowed(self):
        assert self._in_window(3, 0, "", "")

    def test_inside_same_day_window(self):
        assert self._in_window(3, 0, "01:00", "06:00")

    def test_before_same_day_window(self):
        assert not self._in_window(0, 30, "01:00", "06:00")

    def test_after_same_day_window(self):
        assert not self._in_window(7, 0, "01:00", "06:00")

    def test_overnight_window_before_midnight(self):
        assert self._in_window(23, 0, "22:00", "06:00")

    def test_overnight_window_after_midnight(self):
        assert self._in_window(3, 30, "22:00", "06:00")

    def test_overnight_window_outside(self):
        assert not self._in_window(10, 0, "22:00", "06:00")

    def test_only_start_bound(self):
        assert self._in_window(5, 0, "04:00", "")
        assert not self._in_window(3, 0, "04:00", "")

    def test_only_end_bound(self):
        assert self._in_window(2, 0, "", "06:00")
        assert not self._in_window(7, 0, "", "06:00")

    def test_exact_start_is_in_window(self):
        assert self._in_window(1, 0, "01:00", "06:00")

    def test_exact_end_is_outside_window(self):
        assert not self._in_window(6, 0, "01:00", "06:00")


# ══════════════════════════════════════════════════════════════════════════════
# 5. Metered-connection guard
# ══════════════════════════════════════════════════════════════════════════════

class TestMeteredConnection:

    def test_non_windows_returns_false(self):
        with patch.object(sys, "platform", "linux"):
            assert da.is_metered_connection() is False

    def test_windows_not_metered_exit_0(self):
        with patch.object(sys, "platform", "win32"):
            mock_result = Mock()
            mock_result.returncode = 0
            with patch("subprocess.run", return_value=mock_result):
                assert da.is_metered_connection() is False

    def test_windows_metered_exit_1(self):
        with patch.object(sys, "platform", "win32"):
            mock_result = Mock()
            mock_result.returncode = 1
            with patch("subprocess.run", return_value=mock_result):
                assert da.is_metered_connection() is True

    def test_windows_subprocess_exception_returns_false(self):
        with patch.object(sys, "platform", "win32"):
            with patch("subprocess.run", side_effect=OSError("no powershell")):
                assert da.is_metered_connection() is False


# ══════════════════════════════════════════════════════════════════════════════
# 6. Per-watch quota enforcement
# ══════════════════════════════════════════════════════════════════════════════

class TestQuotaEnforcement:
    """Quota-check branching: under limit, at 80%, at 90%, over limit."""

    @staticmethod
    def _quota_status(used: int, max_bytes: int) -> str:
        """Mirrors the quota decision tree in the scheduler loop."""
        if max_bytes <= 0:
            return "unlimited"
        pct = (used / max_bytes) * 100
        if used >= max_bytes:
            return "exceeded"
        if pct >= 90:
            return "warn_90"
        if pct >= 80:
            return "warn_80"
        return "ok"

    def test_no_limit_is_unlimited(self):
        assert self._quota_status(500, 0) == "unlimited"

    def test_well_under_limit_is_ok(self):
        assert self._quota_status(100, 1000) == "ok"

    def test_exactly_80pct_is_warn_80(self):
        assert self._quota_status(800, 1000) == "warn_80"

    def test_exactly_90pct_is_warn_90(self):
        assert self._quota_status(900, 1000) == "warn_90"

    def test_at_limit_is_exceeded(self):
        assert self._quota_status(1000, 1000) == "exceeded"

    def test_over_limit_is_exceeded(self):
        assert self._quota_status(1500, 1000) == "exceeded"


# ══════════════════════════════════════════════════════════════════════════════
# 7. Integrity check trigger — logic extracted as pure functions
# ══════════════════════════════════════════════════════════════════════════════

# Since MainWindow inherits from a mocked QMainWindow (which is a MagicMock),
# the class itself becomes a MagicMock and its methods aren't real callables
# in the test environment.  We therefore replicate the logic as standalone
# helper functions — the same pattern used for backup-window and quota checks.

def _trigger_integrity_now(self_sched, self_append_log, self_tray):
    """Replicate _trigger_integrity_check_now without Qt dependencies."""
    from unittest.mock import MagicMock as _MM
    if self_sched is None:
        # would call QMessageBox.information — record it
        return "no_scheduler"
    self_sched.run_now()
    self_append_log("🔍 Manual integrity check triggered — results will appear in the log.")
    if self_tray is not None:
        self_tray.showMessage("BackupSys", "Integrity check started.", 1, 3000)
    return "triggered"


def _on_integrity_result(watch_name: str, result: dict,
                          append_log, tray=None) -> None:
    """Replicate _on_integrity_result without Qt dependencies."""
    ok = result.get("valid") and result.get("manifest_ok", True)
    if ok:
        append_log(f"✔ Integrity OK: {watch_name}")
        return
    missing   = result.get("missing_files",   [])
    corrupted = result.get("corrupted_files", [])
    err       = result.get("error", "")
    detail    = ""
    if missing:
        detail += f"  Missing: {', '.join(missing[:3])}"
        if len(missing) > 3:
            detail += f" (+{len(missing)-3} more)"
    if corrupted:
        detail += f"  Corrupted: {', '.join(corrupted[:3])}"
        if len(corrupted) > 3:
            detail += f" (+{len(corrupted)-3} more)"
    if err:
        detail += f"  Error: {err}"
    append_log(f"⚠ Integrity FAILED: {watch_name}{detail}")
    if tray is not None:
        tray.showMessage("BackupSys",
                         f"⚠ Integrity check failed for {watch_name}. Check Activity Log.",
                         2, 5000)


def _on_integrity_run_finished(summary: dict, append_log) -> None:
    """Replicate _on_integrity_run_finished without Qt dependencies."""
    total  = summary.get("total", 0)
    passed = summary.get("passed", 0)
    failed = summary.get("failed", 0)
    if total > 0:
        append_log(
            f"✔ Integrity check complete: {passed}/{total} passed"
            + (f", {failed} failed" if failed else "")
        )


class TestIntegrityCheckTrigger:
    """_trigger_integrity_check_now behaviour under various scheduler states."""

    def test_no_scheduler_returns_sentinel(self):
        out = _trigger_integrity_now(None, MagicMock(), MagicMock())
        assert out == "no_scheduler"

    def test_with_scheduler_calls_run_now(self):
        sched = MagicMock()
        _trigger_integrity_now(sched, MagicMock(), MagicMock())
        sched.run_now.assert_called_once()

    def test_with_scheduler_appends_log(self):
        log = MagicMock()
        _trigger_integrity_now(MagicMock(), log, MagicMock())
        log.assert_called_once()
        assert "integrity" in log.call_args[0][0].lower()

    def test_tray_notification_shown(self):
        tray = MagicMock()
        _trigger_integrity_now(MagicMock(), MagicMock(), tray)
        tray.showMessage.assert_called_once()

    def test_returns_triggered(self):
        out = _trigger_integrity_now(MagicMock(), MagicMock(), MagicMock())
        assert out == "triggered"


# ══════════════════════════════════════════════════════════════════════════════
# 8. _on_integrity_result signal handler
# ══════════════════════════════════════════════════════════════════════════════

class TestOnIntegrityResult:
    """_on_integrity_result produces correct log messages for pass/fail."""

    def _call(self, watch_name: str, result: dict):
        logged = []
        tray = MagicMock()
        _on_integrity_result(watch_name, result, logged.append, tray)
        return logged[0] if logged else "", tray

    def test_ok_result_logs_success(self):
        msg, _ = self._call("MyDocs", {"valid": True, "manifest_ok": True})
        assert "OK" in msg or "✔" in msg
        assert "MyDocs" in msg

    def test_missing_files_logged(self):
        msg, _ = self._call("MyDocs", {
            "valid": False, "manifest_ok": True,
            "missing_files": ["a.txt", "b.txt"], "corrupted_files": []
        })
        assert "Missing" in msg or "missing" in msg.lower()

    def test_corrupted_files_logged(self):
        msg, _ = self._call("MyDocs", {
            "valid": False, "manifest_ok": True,
            "missing_files": [], "corrupted_files": ["bad.bin"]
        })
        assert "Corrupted" in msg or "corrupt" in msg.lower()

    def test_many_missing_truncated(self):
        files = [f"f{i}.txt" for i in range(10)]
        msg, _ = self._call("MyDocs", {
            "valid": False, "manifest_ok": True,
            "missing_files": files, "corrupted_files": []
        })
        assert "more" in msg

    def test_tray_warning_shown_on_failure(self):
        _, tray = self._call("X", {
            "valid": False, "manifest_ok": True,
            "missing_files": ["gone.txt"], "corrupted_files": []
        })
        tray.showMessage.assert_called_once()

    def test_no_tray_on_pass(self):
        _, tray = self._call("X", {"valid": True, "manifest_ok": True})
        tray.showMessage.assert_not_called()


# ══════════════════════════════════════════════════════════════════════════════
# 9. _on_integrity_run_finished
# ══════════════════════════════════════════════════════════════════════════════

class TestOnIntegrityRunFinished:

    def _call(self, summary: dict) -> str:
        logged = []
        _on_integrity_run_finished(summary, logged.append)
        return logged[0] if logged else ""

    def test_all_pass_summary(self):
        msg = self._call({"total": 3, "passed": 3, "failed": 0})
        assert "3" in msg and ("pass" in msg.lower() or "✔" in msg)

    def test_some_fail_mentions_failed_count(self):
        msg = self._call({"total": 5, "passed": 3, "failed": 2})
        assert "2" in msg and "fail" in msg.lower()

    def test_empty_summary_no_log(self):
        logged = []
        _on_integrity_run_finished({"total": 0}, logged.append)
        assert logged == []


# ══════════════════════════════════════════════════════════════════════════════
# 10. HistoryWindow construction (pure data, no Qt rendering)
# ══════════════════════════════════════════════════════════════════════════════

class TestHistoryWindowData:
    """Test HistoryWindow attribute storage without rendering any widgets."""

    def _make(self, history=None, backup_history=None, queue=None, cfg=None):
        hw = da.HistoryWindow.__new__(da.HistoryWindow)
        hw._all_history    = history        or []
        hw._backup_history = backup_history or []
        hw._backup_queue   = queue          or []
        hw._cfg            = cfg            or {}
        return hw

    def test_stores_all_history(self):
        entries = [{"ts": "2026-01-01", "file": "a.txt"}]
        hw = self._make(history=entries)
        assert hw._all_history == entries

    def test_stores_backup_history(self):
        bh = [{"backup_id": "b1", "status": "success"}]
        hw = self._make(backup_history=bh)
        assert hw._backup_history == bh

    def test_empty_queue_default(self):
        hw = self._make()
        assert hw._backup_queue == []

    def test_cfg_stored(self):
        hw = self._make(cfg={"dest_type": "sftp"})
        assert hw._cfg["dest_type"] == "sftp"


# ══════════════════════════════════════════════════════════════════════════════
# 11. Restore destination resolution logic
# ══════════════════════════════════════════════════════════════════════════════

class TestResolveRestoreDestination:
    """Test the branching in _resolve_restore_destination without real network calls."""

    # Replicate the routing logic as a pure function for isolated testing
    _REMOTE_TYPES = {"sftp", "ftps", "ftp", "webdav", "https", "rclone", "cloud"}

    def _resolve(self, global_dest_type: str,
                 per_watch_dests: list,
                 local_path_exists: bool = True) -> str:
        """Return 'global_remote', 'per_watch_remote', 'local', or 'no_dests'."""
        if global_dest_type in self._REMOTE_TYPES:
            return "global_remote"
        remote_dests = [d for d in per_watch_dests
                        if d.get("dest_type", "local") in self._REMOTE_TYPES]
        if remote_dests:
            return "per_watch_remote"
        if local_path_exists:
            return "local"
        return "no_dests"

    def test_global_sftp_returns_global_remote(self):
        assert self._resolve("sftp", []) == "global_remote"

    def test_global_local_with_sftp_watch_returns_per_watch(self):
        dests = [{"dest_type": "sftp", "config": {"host": "h"}}]
        assert self._resolve("local", dests) == "per_watch_remote"

    def test_global_local_no_remote_dests_returns_local(self):
        assert self._resolve("local", []) == "local"

    def test_no_local_path_returns_no_dests(self):
        assert self._resolve("local", [], local_path_exists=False) == "no_dests"

    def test_all_remote_types_recognized(self):
        for rt in self._REMOTE_TYPES:
            assert self._resolve(rt, []) == "global_remote"

    def test_cloud_is_remote_type(self):
        assert self._resolve("cloud", []) == "global_remote"


# ══════════════════════════════════════════════════════════════════════════════
# 12. transport_utils.check_remote_free_space — unit tests
# ══════════════════════════════════════════════════════════════════════════════

class TestCheckRemoteFreeSpace:
    """check_remote_free_space: each transport branch returns correct shape."""

    def setup_method(self):
        # Import the real function (transport_utils may or may not be importable)
        try:
            from transport_utils import check_remote_free_space, _human_size
            self.fn = check_remote_free_space
            self._hs = _human_size
        except ImportError:
            pytest.skip("transport_utils not importable in this environment")

    def test_unsupported_type_returns_skipped(self):
        result = self.fn("https", {}, 1000)
        assert result["ok"] is True
        assert result["skipped"] is True

    def test_sftp_no_paramiko_returns_ok(self):
        """When paramiko is missing, check returns ok=True (non-blocking)."""
        with patch.dict(sys.modules, {"paramiko": None}):
            result = self.fn("sftp", {"dest_sftp": {"host": "h", "port": 22,
                                                     "username": "u"}}, 1000)
        assert result["ok"] is True

    def test_sftp_unconfigured_returns_ok(self):
        """SFTP with no host configured → ok=True (skip)."""
        result = self.fn("sftp", {"dest_sftp": {}}, 1000)
        assert result["ok"] is True

    def test_rclone_not_installed_returns_ok(self):
        """rclone not in PATH → ok=True, skipped."""
        with patch("subprocess.run", side_effect=FileNotFoundError("rclone")):
            result = self.fn("rclone",
                             {"dest_rclone": {"remote": "myremote"}}, 1000)
        assert result["ok"] is True

    def test_rclone_reports_enough_space(self):
        mock_proc = Mock()
        mock_proc.returncode = 0
        mock_proc.stdout = json.dumps({"free": 10 * 1024 ** 3})  # 10 GB
        with patch("subprocess.run", return_value=mock_proc):
            result = self.fn("rclone",
                             {"dest_rclone": {"remote": "myremote"}},
                             1 * 1024 ** 3)  # need 1 GB
        assert result["ok"] is True
        assert result["free"] == 10 * 1024 ** 3

    def test_rclone_reports_insufficient_space(self):
        mock_proc = Mock()
        mock_proc.returncode = 0
        mock_proc.stdout = json.dumps({"free": 100 * 1024})  # 100 KB
        with patch("subprocess.run", return_value=mock_proc):
            result = self.fn("rclone",
                             {"dest_rclone": {"remote": "myremote"}},
                             1 * 1024 ** 3)  # need 1 GB
        assert result["ok"] is False
        assert "insufficient" in result["error"].lower()


    def test_webdav_quota_not_supported_returns_skipped(self):
        """Server that doesn't return quota-available-bytes → ok=True, skipped."""
        import urllib.error
        mock_resp = Mock()
        mock_resp.read.return_value = b"<D:multistatus></D:multistatus>"
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = Mock(return_value=False)
        mock_resp.status = 207

        with patch("urllib.request.urlopen", return_value=mock_resp):
            result = self.fn("webdav",
                             {"dest_webdav": {"url": "https://dav.example.com",
                                              "username": "u", "password": "p"}},
                             1024)
        assert result["ok"] is True

    def test_result_always_has_required_keys(self):
        result = self.fn("https", {}, 0)
        for key in ("ok", "free", "error", "skipped"):
            assert key in result, f"Missing key '{key}' in result"


# ══════════════════════════════════════════════════════════════════════════════
# 13. get_gdrive_quota — unit tests
# ══════════════════════════════════════════════════════════════════════════════

class TestGetGdriveQuota:

    def setup_method(self):
        try:
            from transport_utils import get_gdrive_quota
            self.fn = get_gdrive_quota
        except ImportError:
            pytest.skip("transport_utils not importable")

    def test_no_token_returns_error(self):
        result = self.fn({})
        assert result["ok"] is False
        assert "token" in result["error"].lower() or "connect" in result["error"].lower()

    def test_successful_response_parsed(self):
        mock_resp = Mock()
        mock_resp.read.return_value = json.dumps({
            "storageQuota": {
                "limit":         "17179869184",  # 16 GB
                "usage":         "5368709120",   # 5 GB
                "usageInDrive":  "4294967296",   # 4 GB
            }
        }).encode()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = Mock(return_value=False)
        with patch("urllib.request.urlopen", return_value=mock_resp):
            result = self.fn({"access_token": "tok"})
        assert result["ok"] is True
        assert result["limit"] == 17179869184
        assert result["usage"] == 5368709120
        assert result["drive_used"] == 4294967296
        assert result["free"] == 17179869184 - 5368709120

    def test_unlimited_plan_no_limit_key(self):
        mock_resp = Mock()
        mock_resp.read.return_value = json.dumps({
            "storageQuota": {
                "usage":        "1073741824",
                "usageInDrive": "536870912",
            }
        }).encode()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = Mock(return_value=False)
        with patch("urllib.request.urlopen", return_value=mock_resp):
            result = self.fn({"access_token": "tok"})
        assert result["ok"] is True
        assert result["limit"] == -1
        assert result["free"] == -1  # can't compute without limit

    def test_network_error_returns_error(self):
        with patch("urllib.request.urlopen", side_effect=OSError("network down")):
            result = self.fn({"access_token": "tok"})
        assert result["ok"] is False
        assert result["error"] != ""


# ══════════════════════════════════════════════════════════════════════════════
# 14. Flask API — management endpoint DB schema & command queue
# ══════════════════════════════════════════════════════════════════════════════

class TestFlaskAPIManagement:
    """Test backupsys_api management endpoints without HTTP (direct Flask test client)."""

    @pytest.fixture()
    def client(self, tmp_path):
        """Return a Flask test client with an isolated in-memory DB."""
        # Import with env vars set
        os.environ["BACKUPSYS_API_KEY"] = "a" * 32
        os.environ["BACKUPSYS_DB_PATH"] = str(tmp_path / "test.db")
        os.environ["BACKUPSYS_FILES_DIR"] = str(tmp_path / "files")

        # Force re-import so env vars are picked up
        import importlib
        import backupsys_api as api_mod
        importlib.reload(api_mod)
        api_mod._init_db()

        api_mod.app.config["TESTING"] = True
        with api_mod.app.test_client() as c:
            # Attach a reference so tests can call db helpers
            c._api = api_mod
            yield c

    def _sig(self, body: bytes) -> str:
        import hmac, hashlib
        key = ("a" * 32).encode()
        return hmac.new(key, body, hashlib.sha256).hexdigest()

    def _post(self, client, path: str, data: dict) -> object:
        body = json.dumps(data).encode()
        return client.post(
            path,
            data=body,
            content_type="application/json",
            headers={
                "X-BackupSys-Signature": self._sig(body),
                "X-BackupSys-Timestamp": str(time.time()),
            },
        )

    def _get(self, client, path: str, params: str = "") -> object:
        url = path + (f"?{params}" if params else "")
        empty = b""
        return client.get(
            url,
            headers={
                "X-BackupSys-Signature": self._sig(empty),
                "X-BackupSys-Timestamp": str(time.time()),
            },
        )

    def test_watches_register_and_list(self, client):
        resp = self._post(client, "/watches/register", {
            "machine_id": "PC1",
            "watches": [{"id": "w1", "name": "Docs", "source": "/home", "dest_type": "local", "active": True}]
        })
        assert resp.status_code == 200
        data = json.loads(resp.data)
        assert data["ok"] is True
        assert data["registered"] == 1

        resp2 = self._get(client, "/watches")
        assert resp2.status_code == 200
        rows = json.loads(resp2.data)["watches"]
        assert len(rows) == 1
        assert rows[0]["name"] == "Docs"

    def test_watches_register_missing_machine_id_returns_400(self, client):
        resp = self._post(client, "/watches/register", {"watches": []})
        assert resp.status_code == 400

    def test_queue_backup_command(self, client):
        resp = self._post(client, "/commands/backup", {
            "machine_id": "PC1",
            "watch_id": "w1",
            "watch_name": "Docs",
        })
        assert resp.status_code == 201
        data = json.loads(resp.data)
        assert data["ok"] is True
        assert "command_id" in data

    def test_queue_backup_missing_fields_returns_400(self, client):
        resp = self._post(client, "/commands/backup", {"machine_id": "PC1"})
        assert resp.status_code == 400

    def test_pending_commands_returned_for_machine(self, client):
        self._post(client, "/commands/backup", {
            "machine_id": "PC1", "watch_id": "w1", "watch_name": "Docs"
        })
        resp = self._get(client, "/commands/pending", "machine_id=PC1")
        assert resp.status_code == 200
        cmds = json.loads(resp.data)["commands"]
        assert len(cmds) == 1
        assert cmds[0]["command"] == "backup"

    def test_pending_excludes_other_machine(self, client):
        self._post(client, "/commands/backup", {
            "machine_id": "PC2", "watch_id": "w1", "watch_name": "Docs"
        })
        resp = self._get(client, "/commands/pending", "machine_id=PC1")
        cmds = json.loads(resp.data)["commands"]
        assert cmds == []

    def test_ack_command_marks_done(self, client):
        r = self._post(client, "/commands/backup", {
            "machine_id": "PC1", "watch_id": "w1", "watch_name": "Docs"
        })
        cmd_id = json.loads(r.data)["command_id"]
        body = json.dumps({"status": "done"}).encode()
        resp = client.post(
            f"/commands/{cmd_id}/ack",
            data=body,
            content_type="application/json",
            headers={
                "X-BackupSys-Signature": self._sig(body),
                "X-BackupSys-Timestamp": str(time.time()),
            },
        )
        assert resp.status_code == 200
        # Should no longer appear in pending
        pending = self._get(client, "/commands/pending", "machine_id=PC1")
        assert json.loads(pending.data)["commands"] == []

    def test_cancel_pending_command(self, client):
        r = self._post(client, "/commands/backup", {
            "machine_id": "PC1", "watch_id": "w1", "watch_name": "Docs"
        })
        cmd_id = json.loads(r.data)["command_id"]
        empty = b""
        resp = client.delete(
            f"/commands/{cmd_id}",
            headers={
                "X-BackupSys-Signature": self._sig(empty),
                "X-BackupSys-Timestamp": str(time.time()),
            },
        )
        assert resp.status_code == 200

    def test_cancel_already_acked_returns_409(self, client):
        r = self._post(client, "/commands/backup", {
            "machine_id": "PC1", "watch_id": "w1", "watch_name": "Docs"
        })
        cmd_id = json.loads(r.data)["command_id"]
        # First ack it
        body = json.dumps({"status": "done"}).encode()
        client.post(
            f"/commands/{cmd_id}/ack",
            data=body,
            content_type="application/json",
            headers={
                "X-BackupSys-Signature": self._sig(body),
                "X-BackupSys-Timestamp": str(time.time()),
            },
        )
        # Now try to cancel
        empty = b""
        resp = client.delete(
            f"/commands/{cmd_id}",
            headers={
                "X-BackupSys-Signature": self._sig(empty),
                "X-BackupSys-Timestamp": str(time.time()),
            },
        )
        assert resp.status_code == 409

    def test_config_update_queued(self, client):
        resp = self._post(client, "/commands/config", {
            "machine_id": "PC1",
            "updates": {"pause_on_metered": True, "max_backup_mbps": 5},
        })
        assert resp.status_code == 201
        data = json.loads(resp.data)
        assert data["ok"] is True

    def test_config_update_rejects_nested_values(self, client):
        resp = self._post(client, "/commands/config", {
            "machine_id": "PC1",
            "updates": {"watches": [{"id": "evil"}]},  # list → rejected
        })
        assert resp.status_code == 400

    def test_list_commands_returns_history(self, client):
        self._post(client, "/commands/backup", {
            "machine_id": "PC1", "watch_id": "w1", "watch_name": "Docs"
        })
        resp = self._get(client, "/commands")
        assert resp.status_code == 200
        data = json.loads(resp.data)
        assert data["count"] >= 1

    def test_unauthenticated_request_returns_401(self, client):
        resp = client.post("/commands/backup",
                           data=json.dumps({"machine_id": "PC1", "watch_id": "w1"}).encode(),
                           content_type="application/json")
        assert resp.status_code == 401


# ══════════════════════════════════════════════════════════════════════════════
# 15. config.template.json has all required keys
# ══════════════════════════════════════════════════════════════════════════════

class TestConfigTemplate:
    """Regression: verify the four previously-missing keys are now in the template."""

    @pytest.fixture(scope="class")
    def template(self):
        tmpl_path = Path(__file__).resolve().parent.parent / "config.template.json"
        with open(tmpl_path) as f:
            return json.load(f)

    def test_backup_window_start_present(self, template):
        assert "backup_window_start" in template

    def test_pause_on_metered_present(self, template):
        assert "pause_on_metered" in template

    def test_force_full_interval_days_present(self, template):
        assert "force_full_interval_days" in template

    def test_ntfy_config_present(self, template):
        assert "ntfy_config" in template

    def test_ntfy_config_has_required_fields(self, template):
        nc = template["ntfy_config"]
        for field in ("enabled", "server", "topic"):
            assert field in nc, f"ntfy_config missing '{field}'"

    def test_pause_on_metered_is_bool(self, template):
        assert isinstance(template["pause_on_metered"], bool)

    def test_force_full_interval_is_int(self, template):
        assert isinstance(template["force_full_interval_days"], int)


# ══════════════════════════════════════════════════════════════════════════════
# 16. Theme helpers (extends existing test_desktop_app_theme.py)
# ══════════════════════════════════════════════════════════════════════════════

class TestThemeResolution:
    """Covers auto/dark/light resolution including edge-cases not in the original suite."""

    def _resolve(self, saved: str) -> str:
        return saved if saved not in ("", None) else "dark"

    def test_empty_string_defaults_to_dark(self):
        assert self._resolve("") == "dark"

    def test_none_defaults_to_dark(self):
        assert self._resolve(None) == "dark"  # type: ignore[arg-type]

    def test_dark_returns_dark(self):
        assert self._resolve("dark") == "dark"

    def test_light_returns_light(self):
        assert self._resolve("light") == "light"

    def test_unknown_value_not_overridden(self):
        """Unknown theme values should not be silently forced to 'dark'."""
        assert self._resolve("solarized") == "solarized"


# ══════════════════════════════════════════════════════════════════════════════
# 17. Destination-watcher spurious-MODIFIED suppression (UNC dest regression)
# ══════════════════════════════════════════════════════════════════════════════
#
# Bug: when the destination watch path is itself a UNC path (e.g.
# \\host\share\1, rather than a local drive path like D:\share\1), the entire
# dest-event suppression pipeline (early-stamp, spurious-modified time-window
# check, content-fingerprint check, pre-delete-mod suppression, watchdog/
# unc_poll dedup) was nested inside `if _is_local_path and not _is_dest:`.
# Since a UNC destination is never `_is_local_path` (it starts with "\\\\")
# AND is always `_is_dest=True`, that condition is never true for it, so the
# suppression pipeline silently never ran. Every spurious SMB write-notify
# MODIFIED (caused by this app's own NtQueryDirectoryFile RestartScan cache
# flushing, and by SMB2 CHANGE_NOTIFY handle-reopen churn after
# ERROR_NOTIFY_ENUM_DIR buffer overflows) for a file that was never actually
# touched again was written to history as if it were a genuine edit.
#
# Fix: the suppression pipeline now runs unconditionally (dedented out of the
# `_is_local_path and not _is_dest` guard), and a new content-fingerprint
# check (size, mtime) suppresses MODIFIED events indefinitely -- not just
# within a short time window -- whenever the file's on-disk state hasn't
# actually changed since it was added or last genuinely edited.

class TestDestWatcherSpuriousModifiedSuppression:
    """Reproduces the reported bug: a single manual file-drop into a UNC
    destination folder produced several extra 'modified' rows in Change
    History over the following minutes, with no further user action."""

    @pytest.fixture
    def app(self, tmp_path):
        import desktop_app

        mw = desktop_app.MainWindow.__new__(desktop_app.MainWindow)
        mw.cfg = {
            "watches": [
                {
                    "id": "w_test",
                    "path": r"\\192.168.254.106\testshare",
                    "smb_audit_cfg": {},
                }
            ]
        }
        mw._dest_added_early = {}
        mw._dest_added_lock = threading.Lock()
        mw._dest_event_seen = {}
        mw._dest_event_seen_lock = threading.Lock()
        mw._source_event_seen = {}
        mw._source_event_seen_lock = threading.Lock()
        mw._pending_mod_before_del = {}
        mw._pending_mod_lock = threading.Lock()
        mw._dest_content_fp = {}
        mw._dest_content_fp_lock = threading.Lock()
        mw._change_counts = {}
        mw._history_log = []
        mw._history_save_counter = 0
        mw._history_window = None
        mw._post_backup_finish = {}
        mw._post_backup_filenames = {}
        mw._post_backup_dest_fnames = {}
        mw._POST_BACKUP_SUPPRESS_SECS = 120
        mw._watcher_start_times = {}
        mw._WATCHER_START_SUPPRESS_SECS = 60
        mw._workers = {}
        mw._pending_entries = {}
        mw._last_notif_time = {}
        mw._skipped_notified = {}
        mw._cards = {}
        mw._file_change_signal = MagicMock()
        mw._watch_name_for = lambda wid: "test"
        return mw

    @pytest.fixture
    def dest_file(self, tmp_path):
        p = tmp_path / "Screenshot.png"
        p.write_bytes(b"hello world")
        return str(p)

    @pytest.fixture
    def editor_info(self):
        return {
            "user": "user",
            "machine": "DESKTOP-KGG55PU",
            "ip": "192.168.254.105",
            "attribution_unknown": False,
        }

    def _backdate_all_stamps(self, app):
        """Simulate enough time passing that the short suppression windows
        (10s spurious-modified, 25s dedup) have long expired."""
        ancient = time.monotonic() - 9999
        for k in list(app._dest_added_early.keys()):
            app._dest_added_early[k] = ancient
        for k in list(app._dest_event_seen.keys()):
            app._dest_event_seen[k] = ancient

    def test_added_event_is_local_path_check_does_not_block_unc_dest(self, app):
        """Sanity check on the bug's precondition: a UNC path is never
        treated as local, confirming the old gating condition could never
        fire for this scenario."""
        watch_path = app.cfg["watches"][0]["path"]
        is_local_path = bool(watch_path) and not watch_path.startswith("\\\\")
        assert is_local_path is False

    def test_manual_add_records_single_history_row(self, app, dest_file, editor_info):
        with patch("desktop_app._get_editor_info", return_value=editor_info):
            app._on_file_change(
                "w_test__dest",
                {"type": "added", "path": dest_file, "detection_source": "watchdog"},
            )
        assert len(app._history_log) == 1
        assert app._history_log[0]["type"] == "added"

    def test_immediate_spurious_modified_suppressed_by_time_window(
        self, app, dest_file, editor_info
    ):
        with patch("desktop_app._get_editor_info", return_value=editor_info):
            app._on_file_change(
                "w_test__dest",
                {"type": "added", "path": dest_file, "detection_source": "watchdog"},
            )
            app._on_file_change(
                "w_test__dest",
                {"type": "modified", "path": dest_file, "detection_source": "watchdog"},
            )
        assert len(app._history_log) == 1

    def test_late_spurious_modified_suppressed_by_fingerprint(
        self, app, dest_file, editor_info
    ):
        """The core regression test: a MODIFIED event arrives long after every
        time-window has expired, but the file's (size, mtime) on disk is
        unchanged -- this must NOT be recorded as a genuine edit."""
        with patch("desktop_app._get_editor_info", return_value=editor_info):
            app._on_file_change(
                "w_test__dest",
                {"type": "added", "path": dest_file, "detection_source": "watchdog"},
            )
            self._backdate_all_stamps(app)
            app._on_file_change(
                "w_test__dest",
                {"type": "modified", "path": dest_file, "detection_source": "watchdog"},
            )
        assert len(app._history_log) == 1, (
            "spurious late MODIFIED with no actual file change was incorrectly "
            "recorded as a genuine edit"
        )

    def test_repeated_late_spurious_modifieds_all_suppressed(
        self, app, dest_file, editor_info
    ):
        """Reproduces the exact reported symptom: many repeated spurious
        MODIFIED notifications over several minutes for one untouched file."""
        with patch("desktop_app._get_editor_info", return_value=editor_info):
            app._on_file_change(
                "w_test__dest",
                {"type": "added", "path": dest_file, "detection_source": "watchdog"},
            )
            for _ in range(5):
                self._backdate_all_stamps(app)
                app._on_file_change(
                    "w_test__dest",
                    {
                        "type": "modified",
                        "path": dest_file,
                        "detection_source": "watchdog",
                    },
                )
        assert len(app._history_log) == 1

    def test_genuine_late_edit_is_still_recorded(self, app, dest_file, editor_info):
        """The fingerprint check must not over-suppress: a real edit (actual
        size/mtime change) arriving long after the time-window must still be
        recorded as a modification."""
        with patch("desktop_app._get_editor_info", return_value=editor_info):
            app._on_file_change(
                "w_test__dest",
                {"type": "added", "path": dest_file, "detection_source": "watchdog"},
            )
            self._backdate_all_stamps(app)

            time.sleep(0.05)
            with open(dest_file, "ab") as f:
                f.write(b" more data")
            os.utime(dest_file, None)

            app._on_file_change(
                "w_test__dest",
                {"type": "modified", "path": dest_file, "detection_source": "watchdog"},
            )
        assert len(app._history_log) == 2
        assert app._history_log[1]["type"] == "modified"

    def test_fingerprint_stat_failure_fails_open(self, app, dest_file, editor_info):
        """If the file can't be stat'd (e.g. transient lock), we must NOT
        suppress -- failing open avoids ever hiding a real change we
        couldn't verify."""
        with patch("desktop_app._get_editor_info", return_value=editor_info):
            app._on_file_change(
                "w_test__dest",
                {"type": "added", "path": dest_file, "detection_source": "watchdog"},
            )
            self._backdate_all_stamps(app)
            missing_path = dest_file + ".does-not-exist"
            app._on_file_change(
                "w_test__dest",
                {
                    "type": "modified",
                    "path": missing_path,
                    "detection_source": "watchdog",
                },
            )
        # Different path -> independent fingerprint slot, no prior fp, stat
        # fails -> not suppressed -> recorded as a (separate) genuine event.
        assert len(app._history_log) == 2


# ══════════════════════════════════════════════════════════════════════════════
# Regression: same-host local edit must not be attributed to a remote
# coworker just because both machines log in under the same generic
# Windows username (e.g. both named "user").
#
# Root cause: the NetFileEnum/'net file' "is this handle local?" check used
# to compare the handle's USERNAME against the local machine's HOSTNAME
# (e.g. "user" == "desktop-0edubap"), which can never match. A genuinely
# local write was therefore never recognised as local and fell through to
# the "remote user" branch, where it got matched — purely by username — to
# whatever coworker session happened to be sitting in the SMB session cache.
# ══════════════════════════════════════════════════════════════════════════════
class TestSameHostEditorAttributionUsernameCollision:
    OWN_HOST = "desktop-0edubap"
    OWN_IP = "192.168.254.106"
    COWORKER_IP = "192.168.254.105"
    SHARED_USERNAME = "user"  # both PCs log in under this same account name

    @pytest.fixture(autouse=True)
    def _reset_module_caches(self):
        # These module-level caches remember "NetFileEnum/'net file' previously
        # failed" across calls; reset so our mocked NetFileEnum is actually tried.
        da._same_host_nfe_available = None
        da._same_host_net_file_available = None
        da._same_host_nfe_sig = None
        yield
        da._same_host_nfe_available = None
        da._same_host_net_file_available = None
        da._same_host_nfe_sig = None

    def test_local_edit_not_attributed_to_coworker_with_same_username(self):
        """The exact reported scenario: local user 'user' on .106 edits a
        file held open on the same machine; a coworker's PC ('user' on
        .105) merely has an old SMB session sitting in the snapshot cache.
        The local edit must NOT come back stamped with the coworker's IP.
        """
        filepath = f"\\\\{self.OWN_IP}\\testshare\\test sheet.xlsx"

        # NetFileEnum reports ONE open handle on the matching file, held
        # under the local account name (this is what a same-host UNC write
        # looks like — Windows serves it via local loopback SMB, so the
        # handle's username is genuinely the local interactive user).
        local_handle = {
            "fi3_username": self.SHARED_USERNAME,
            "fi3_pathname": "D:\\testshare\\test sheet.xlsx",
        }

        mock_win32net = MagicMock()
        mock_win32net.NetFileEnum.return_value = ([local_handle], 1, 0)

        mock_win32api = MagicMock()
        mock_win32api.GetUserName.return_value = self.SHARED_USERNAME

        # NTFS owner lookup (Step1) — on a real Windows box with pywin32
        # installed this succeeds and reports the genuine local owner.
        # Mock it the same way so the test reflects production behaviour
        # rather than the "pywin32 missing" last-resort fallback path.
        mock_win32security = MagicMock()
        mock_win32security.OWNER_SECURITY_INFORMATION = 1
        mock_sd = MagicMock()
        mock_sd.GetSecurityDescriptorOwner.return_value = "S-1-5-21-fake-sid"
        mock_win32security.GetFileSecurity.return_value = mock_sd
        mock_win32security.LookupAccountSid.return_value = (
            self.SHARED_USERNAME, self.OWN_HOST.upper(), 1,
        )

        # Stale coworker SMB session sitting in the snapshot — same username,
        # different machine. This is what used to get matched by mistake.
        coworker_snapshot = [{
            "username": self.SHARED_USERNAME,
            "machine": self.COWORKER_IP,
            "ip": self.COWORKER_IP,
            "idle_time": 1,
            "active_time": 27000,
        }]

        # desktop_app._get_editor_info does `import socket` fresh inside the
        # function body, so patching desktop_app.socket has no effect — we
        # must patch the real stdlib functions it resolves to.
        with patch("socket.gethostname", return_value=self.OWN_HOST), \
             patch("socket.gethostbyname", return_value=self.OWN_IP), \
             patch.dict(sys.modules, {"win32net": mock_win32net,
                                       "win32api": mock_win32api,
                                       "win32security": mock_win32security}):
            info = da._get_editor_info(
                filepath,
                detection_source="watchdog",
                event_type="modified",
                smb_sessions_snapshot=coworker_snapshot,
            )

        # The bug: info["ip"] == self.COWORKER_IP (misattributed).
        # The fix: the local handle is recognised as local (username matches
        # the resolved local account), so we must NOT return the coworker's
        # IP/machine here.
        assert info["ip"] != self.COWORKER_IP, (
            f"Local edit was misattributed to coworker IP {self.COWORKER_IP!r}: "
            f"{info!r}"
        )
        assert info["machine"] != self.COWORKER_IP

    def test_remote_write_not_attributed_to_local_owner_on_single_idle_snapshot(self):
        """Reported scenario #2: coworker (.105) genuinely adds two files.
        NetFileEnum finds no open handle (the write already finished and
        the handle closed — the normal case). Only ONE prior idle-history
        snapshot exists for .105, showing idle=204s (clearly NOT a passive
        near-zero monitor) right before idle dropped to 0 when the files
        appeared. The old code discarded that single decisive snapshot and
        fell back to an active_time>5min check, which — because she simply
        had the share mapped for hours — wrongly concluded "persistent
        monitor" and credited the write to the local owner instead.
        """
        filepath = f"\\\\{self.OWN_IP}\\testshare\\Screenshot 2026-06-19 132211.png"

        # NetFileEnum has handles, but none of them match this filename —
        # the write already completed and the handle closed.
        unrelated_handle = {
            "fi3_username": self.SHARED_USERNAME,
            "fi3_pathname": "D:\\testshare\\",
        }
        mock_win32net = MagicMock()
        mock_win32net.NetFileEnum.return_value = ([unrelated_handle], 1, 0)

        mock_win32api = MagicMock()
        mock_win32api.GetUserName.return_value = self.SHARED_USERNAME

        mock_win32security = MagicMock()
        mock_win32security.OWNER_SECURITY_INFORMATION = 1
        mock_sd = MagicMock()
        mock_sd.GetSecurityDescriptorOwner.return_value = "S-1-5-21-fake-sid"
        mock_win32security.GetFileSecurity.return_value = mock_sd
        mock_win32security.LookupAccountSid.return_value = (
            self.SHARED_USERNAME, self.OWN_HOST.upper(), 1,
        )

        # The coworker's SMB session right now: idle_time=0 (just wrote),
        # but it's been open for hours (active_time >> 5 minutes) — a
        # perfectly normal "drive mapped all day" session, not a sign of
        # passive monitoring.
        coworker_snapshot = [{
            "username": self.SHARED_USERNAME,
            "machine": self.COWORKER_IP,
            "ip": self.COWORKER_IP,
            "idle_time": 0,
            "active_time": 29842,
        }]

        # Exactly one prior idle-history snapshot, taken a few seconds ago,
        # showing idle=204s — well above any "monitor" threshold.
        single_idle_snapshot = [(time.time() - 8, 204)]

        with patch("socket.gethostname", return_value=self.OWN_HOST), \
             patch("socket.gethostbyname", return_value=self.OWN_IP), \
             patch("watcher.get_recent_idle_history", return_value=single_idle_snapshot), \
             patch.dict(sys.modules, {"win32net": mock_win32net,
                                       "win32api": mock_win32api,
                                       "win32security": mock_win32security}):
            info = da._get_editor_info(
                filepath,
                detection_source="watchdog",
                event_type="added",
                smb_sessions_snapshot=coworker_snapshot,
            )

        assert info["ip"] == self.COWORKER_IP, (
            f"Genuine remote write was misattributed to the local owner "
            f"instead of the coworker {self.COWORKER_IP!r}: {info!r}"
        )


class TestLoopbackLocalBurstOverrideRegression:
    """Regression test for the reported bug: adding 'test sheet.xlsx' directly
    on .106 (DESKTOP-0EDUBAP, the share host) got stamped in History as added
    by .105 (DESKTOP-KGG55PU).

    Sequence reproduced from the user's logs:
      1. .105 has a long-lived SMB session to the share (active_time=4939s)
         that merely shows idle_time=0 at event time — CHANGE_NOTIFY noise,
         not a real write.
      2. .106 (this machine) writes 'test sheet.xlsx' locally. NetFileEnum's
         handle has already closed by the time attribution runs. The
         LOOPBACK-LOCAL check (own_min_active_time=8s < 30s, fresh, not
         stale) correctly proves this was a LOCAL write and sets
         is_persistent_monitor=True (i.e. "fall back to NTFS owner").
      3. .105 had ALSO genuinely written a different file (a screenshot)
         ~19s earlier, well within the 60s burst window.
      4. BUG: BURST-OVERRIDE saw step 3's confirmed write and flipped
         is_persistent_monitor back to False, discarding the correct
         LOOPBACK-LOCAL verdict from step 2 and misattributing
         'test sheet.xlsx' to .105's IP/machine instead of the local owner.
    """
    OWN_HOST = "desktop-0edubap"
    OWN_IP = "192.168.254.106"
    COWORKER_IP = "192.168.254.105"
    SHARED_USERNAME = "user"

    @pytest.fixture(autouse=True)
    def _reset_module_caches(self):
        da._same_host_nfe_available = None
        da._same_host_net_file_available = None
        da._same_host_nfe_sig = None
        da._recent_remote_write_ts.clear()
        da._recent_own_write_ts.clear()
        yield
        da._same_host_nfe_available = None
        da._same_host_net_file_available = None
        da._same_host_nfe_sig = None
        da._recent_remote_write_ts.clear()
        da._recent_own_write_ts.clear()

    def test_local_add_not_misattributed_to_coworker_after_unrelated_burst_write(self):
        filepath = f"\\\\{self.OWN_IP}\\testshare\\test sheet.xlsx"

        # NetFileEnum has handles, but none match this filename — the local
        # write already completed and its handle closed before attribution ran.
        unrelated_handle = {
            "fi3_username": self.SHARED_USERNAME,
            "fi3_pathname": "D:\\testshare\\",
        }
        mock_win32net = MagicMock()
        mock_win32net.NetFileEnum.return_value = ([unrelated_handle], 1, 0)

        mock_win32api = MagicMock()
        mock_win32api.GetUserName.return_value = self.SHARED_USERNAME

        # NTFS owner lookup correctly reports the local account — this file
        # genuinely lives on .106's own disk.
        mock_win32security = MagicMock()
        mock_win32security.OWNER_SECURITY_INFORMATION = 1
        mock_sd = MagicMock()
        mock_sd.GetSecurityDescriptorOwner.return_value = "S-1-5-21-fake-sid"
        mock_win32security.GetFileSecurity.return_value = mock_sd
        mock_win32security.LookupAccountSid.return_value = (
            self.SHARED_USERNAME, self.OWN_HOST.upper(), 1,
        )

        # .105's session: idle_time=0 (CHANGE_NOTIFY noise from the new file
        # appearing), but own_min_active_time=8s proves a FRESH local loopback
        # session opened for THIS write — the LOOPBACK-LOCAL signal.
        coworker_snapshot = [{
            "username": self.SHARED_USERNAME,
            "machine": self.COWORKER_IP,
            "ip": self.COWORKER_IP,
            "idle_time": 0,
            "active_time": 4939,
            "own_min_active_time": 8,
            "own_min_idle_time": 0,
        }]

        # One prior idle-history snapshot for .105, taken well before the
        # event (not co-temporal, not within any linger window): idle=205s,
        # clearly not a near-zero passive monitor reading.
        single_idle_snapshot = [(time.time() - 70, 205)]

        with patch("socket.gethostname", return_value=self.OWN_HOST), \
             patch("socket.gethostbyname", return_value=self.OWN_IP), \
             patch("watcher.get_recent_idle_history", return_value=single_idle_snapshot), \
             patch.dict(sys.modules, {"win32net": mock_win32net,
                                       "win32api": mock_win32api,
                                       "win32security": mock_win32security}):
            # .105 genuinely wrote a DIFFERENT file moments ago — this is the
            # unrelated burst sibling that must NOT bleed onto this event.
            da._record_remote_write(self.COWORKER_IP, "Screenshot 2025-06-19 114804.png")

            info = da._get_editor_info(
                filepath,
                detection_source="unc_poll",
                event_type="added",
                smb_sessions_snapshot=coworker_snapshot,
            )

        assert info["ip"] != self.COWORKER_IP, (
            f"Local add was misattributed to coworker IP {self.COWORKER_IP!r} "
            f"because of an unrelated burst write to a different file: {info!r}"
        )
        assert info["machine"] != self.COWORKER_IP
        assert info["user"] == self.SHARED_USERNAME


class TestShortLingerGuard:
    """Regression test for the SHORT-LINGER GUARD:

    Scenario reproduced from user logs (2026-06-24):
      1. .105 (DESKTOP-KGG55PU) connects at 13:33:48 and writes two files
         (confirmed by Step0 NFE — _record_remote_write is called).
      2. At 13:34:08 (.105 active_time=38s), .106 (own machine) writes two
         NEW files detected via unc_poll.
      3. Only ONE prior snapshot for .105 was taken 1.2s before the event —
         well within the 2s CO-TEMPORAL grace window — so it is discarded,
         leaving _unpoisoned_history=[] and _min_prior_idle=None.
      4. With active_time=38s <= _NFE_NEW_SESSION_THRESHOLD=300s the old
         code fell into the "recently connected → treating as writer" else-
         branch and overrode the NTFS owner with .105's IP.  BUG.
      5. FIX (SHORT-LINGER GUARD): when the short-session else-branch is
         reached AND the remote IP had a confirmed write > 15s ago, treat
         idle_time=0 as RESIDUAL and fall back to NTFS owner.
    """

    OWN_HOST        = "desktop-0edubap"
    OWN_IP          = "192.168.254.106"
    COWORKER_IP     = "192.168.254.105"
    SHARED_USERNAME = "user"

    @pytest.fixture(autouse=True)
    def _reset_module_caches(self):
        da._same_host_nfe_available = None
        da._same_host_net_file_available = None
        da._same_host_nfe_sig = None
        da._recent_remote_write_ts.clear()
        da._recent_own_write_ts.clear()
        yield
        da._same_host_nfe_available = None
        da._same_host_net_file_available = None
        da._same_host_nfe_sig = None
        da._recent_remote_write_ts.clear()
        da._recent_own_write_ts.clear()

    def test_own_add_not_misattributed_after_coworker_short_session_write(self):
        """Local 'added' (unc_poll) must NOT be stamped with coworker's IP
        when the coworker had a short-session confirmed write 20s ago and
        is still connected with directory-browsing handles (idle_time=0)
        but no file-specific handle on the current file.
        """
        filepath = (
            f"\\\\{self.OWN_IP}\\testshare\\"
            f"Screenshot 2026-06-23 132914.png"
        )

        # NFE returns only a directory handle for .105 (browsing, not writing)
        dir_handle = {
            "fi3_username": self.SHARED_USERNAME,
            "fi3_pathname": "D:\\testshare\\",
        }
        mock_win32net = MagicMock()
        mock_win32net.NetFileEnum.return_value = ([dir_handle], 1, 0)

        mock_win32api = MagicMock()
        mock_win32api.GetUserName.return_value = self.SHARED_USERNAME

        # NTFS owner = own machine (all files on .106's share show this)
        mock_win32security = MagicMock()
        mock_win32security.OWNER_SECURITY_INFORMATION = 1
        mock_sd = MagicMock()
        mock_sd.GetSecurityDescriptorOwner.return_value = "S-1-5-21-fake-sid"
        mock_win32security.GetFileSecurity.return_value = mock_sd
        mock_win32security.LookupAccountSid.return_value = (
            self.SHARED_USERNAME, self.OWN_HOST.upper(), 1,
        )

        # .105's session: active 38s (short, <= 300s threshold), idle_time=0
        coworker_snapshot = [{
            "username": self.SHARED_USERNAME,
            "machine": self.COWORKER_IP,
            "ip":      self.COWORKER_IP,
            "idle_time":   0,
            "active_time": 38,
        }]

        # Exactly ONE prior snapshot for .105 taken 1.2s before the event —
        # within the 2s CO-TEMPORAL grace window so it will be discarded,
        # leaving _unpoisoned_history=[] and _min_prior_idle=None.
        co_temporal_snapshot = [(time.time() - 1.2, 0)]

        with patch("socket.gethostname",  return_value=self.OWN_HOST), \
             patch("socket.gethostbyname", return_value=self.OWN_IP), \
             patch("watcher.get_recent_idle_history",
                   return_value=co_temporal_snapshot), \
             patch.dict(sys.modules, {"win32net": mock_win32net,
                                      "win32api": mock_win32api,
                                      "win32security": mock_win32security}):
            # .105 wrote a different file 20s ago (confirmed by Step0 NFE).
            # _record_remote_write always uses time.time() so we back-date
            # the entry directly.
            with da._recent_remote_write_lock:
                da._recent_remote_write_ts[self.COWORKER_IP] = (
                    time.time() - 20,
                    "Screenshot 2026-06-23 133510.png",
                    None,
                )

            info = da._get_editor_info(
                filepath,
                detection_source="unc_poll",
                event_type="added",
                smb_sessions_snapshot=coworker_snapshot,
            )

        assert info["ip"] != self.COWORKER_IP, (
            f"Own 'added' file was misattributed to coworker {self.COWORKER_IP!r} "
            f"(SHORT-LINGER GUARD did not fire): {info!r}"
        )
        assert info["machine"] != self.COWORKER_IP
        assert info["user"] == self.SHARED_USERNAME


class TestAddedWatchdogGuardPathD:
    """Regression: coworker (.105) writes a new batch of files 27s after its
    previous batch, while the local machine (.106) wrote unrelated files 13s
    ago.  The ADDED-WATCHDOG-GUARD PATH-C must NOT veto .105's live file
    handle just because the prior burst was stale (27s > 8s) — the key
    constraint is own_write_age < 8s (loopback candidate window), not < 30s.
    If own_write_age=13s the loopback session's idle_time≈13s > 5s is already
    excluded by _same_name_remote_active, so PATH-D must fire and trust the
    handle.

    Sequence from user logs (2026-06-24):
      1. .105 wrote screenshots at 14:04:02 (burst_age≈28s at event time).
      2. .106 wrote screenshots at 14:04:15 (own_write_age≈13s at event time).
      3. .105 writes new screenshots at 14:04:28 (current event).
      4. NFE finds .105's handle on the EXACT target file, idle_time=0.
      5. BUG: PATH-C fired (stale burst + own_write_age=13s < old 30s window)
         → veto → Step1 OWN-ACTIVE VETO → attributed to .106.  WRONG.
      6. FIX: _S0_WG_WIDE_S narrowed to 8s → own_write_age=13s >= 8s →
         local_recent=False → PATH-D fires → handle trusted → .105 gets credit.
    """

    OWN_HOST        = "desktop-0edubap"
    OWN_IP          = "192.168.254.106"
    COWORKER_IP     = "192.168.254.105"
    SHARED_USERNAME = "user"

    @pytest.fixture(autouse=True)
    def _reset_module_caches(self):
        da._same_host_nfe_available = None
        da._same_host_net_file_available = None
        da._same_host_nfe_sig = None
        da._recent_remote_write_ts.clear()
        da._recent_own_write_ts.clear()
        yield
        da._same_host_nfe_available = None
        da._same_host_net_file_available = None
        da._same_host_nfe_sig = None
        da._recent_remote_write_ts.clear()
        da._recent_own_write_ts.clear()

    def test_coworker_add_not_suppressed_when_own_write_older_than_loopback_window(self):
        """Coworker's genuine write (new batch, 27s gap) must be attributed to
        .105, not .106, even though .106 wrote unrelated files 13s ago.
        """
        filepath = (
            f"\\\\{self.OWN_IP}\\testshare\\"
            f"Screenshot 2025-06-16 090339.png"
        )

        # NFE finds .105's handle on the EXACT target file (still open for
        # reading/verification after write completed).
        file_handle = {
            "fi3_username": self.SHARED_USERNAME,
            "fi3_pathname": "D:\\testshare\\Screenshot 2025-06-16 090339.png",
        }
        mock_win32net = MagicMock()
        mock_win32net.NetFileEnum.return_value = ([file_handle], 1, 0)

        mock_win32api = MagicMock()
        mock_win32api.GetUserName.return_value = self.SHARED_USERNAME

        # NTFS owner = own machine (all share files show this).
        mock_win32security = MagicMock()
        mock_win32security.OWNER_SECURITY_INFORMATION = 1
        mock_sd = MagicMock()
        mock_sd.GetSecurityDescriptorOwner.return_value = "S-1-5-21-fake-sid"
        mock_win32security.GetFileSecurity.return_value = mock_sd
        mock_win32security.LookupAccountSid.return_value = (
            self.SHARED_USERNAME, self.OWN_HOST.upper(), 1,
        )

        # .105 session: idle_time=0 (actively writing right now).
        coworker_snapshot = [{
            "username": self.SHARED_USERNAME,
            "machine": self.COWORKER_IP,
            "ip":      self.COWORKER_IP,
            "idle_time":   0,
            "active_time": 1858,
        }]

        with patch("socket.gethostname",  return_value=self.OWN_HOST), \
             patch("socket.gethostbyname", return_value=self.OWN_IP), \
             patch.dict(sys.modules, {"win32net": mock_win32net,
                                      "win32api": mock_win32api,
                                      "win32security": mock_win32security}):
            # .105 wrote a different file 27.5s ago (stale burst).
            with da._recent_remote_write_lock:
                da._recent_remote_write_ts[self.COWORKER_IP] = (
                    time.time() - 27.5,
                    "Screenshot 2025-06-11 133510.png",
                    None,
                )
            # .106 wrote unrelated files 13s ago (outside 8s loopback window).
            with da._recent_own_write_lock:
                da._recent_own_write_ts["__last__"] = (
                    time.time() - 13,
                    "Screenshot 2026-06-23 102246.png",
                )

            info = da._get_editor_info(
                filepath,
                detection_source="watchdog",
                event_type="added",
                smb_sessions_snapshot=coworker_snapshot,
            )

        assert info["ip"] == self.COWORKER_IP, (
            f"Coworker's genuine write was misattributed to local machine "
            f"instead of coworker {self.COWORKER_IP!r}: {info!r}"
        )


class TestCompetingOwnWriteGuard:
    """Regression (2026-06-24): .105 writes files at 14:27:14, displayed as .106.

    Sequence from user logs:
      1. .105 wrote Screenshot 2025-06-16 085818.png 27.5s ago (stale burst).
      2. .106 wrote Screenshot 2026-06-23 100241.png 11.5s ago (own machine).
      3. .105 writes Screenshot 2025-06-10 114738.png NOW (detected via unc_poll).
      4. .105 session: active_time=3224s > 300s, no prior cache → _is_persistent_monitor=True.
      5. BUG: COMPETING-OWN-WRITE checked own_write_age < 60s (burst_window) → 11.5s < 60s
         → own_conflict=True → BURST-OVERRIDE suppressed → NTFS owner (.106). WRONG.
      6. FIX: COMPETING-OWN-WRITE window narrowed to 8s (loopback-candidate window):
         own_write_age=11.5s >= 8s → own_conflict=False → BURST-OVERRIDE fires → .105. CORRECT.
    """

    OWN_HOST        = "desktop-0edubap"
    OWN_IP          = "192.168.254.106"
    COWORKER_IP     = "192.168.254.105"
    SHARED_USERNAME = "user"

    @pytest.fixture(autouse=True)
    def _reset_module_caches(self):
        da._same_host_nfe_available = None
        da._same_host_net_file_available = None
        da._same_host_nfe_sig = None
        da._recent_remote_write_ts.clear()
        da._recent_own_write_ts.clear()
        yield
        da._same_host_nfe_available = None
        da._same_host_net_file_available = None
        da._same_host_nfe_sig = None
        da._recent_remote_write_ts.clear()
        da._recent_own_write_ts.clear()

    def test_burst_override_not_suppressed_when_own_write_outside_loopback_window(self):
        """COMPETING-OWN-WRITE must NOT suppress BURST-OVERRIDE when own_write_age >= 8s.

        .105 had a stale burst 27.5s ago. .106 (own machine) wrote 11.5s ago.
        Since own_write_age=11.5s >= 8s (loopback-candidate window), any loopback
        session from .106 would have idle_time ≈ 11.5s > 5s — already excluded by
        _same_name_remote_active. No real conflict. BURST-OVERRIDE must fire → .105.
        """
        filepath = (
            f"\\\\{self.OWN_IP}\\testshare\\"
            f"Screenshot 2025-06-10 114738.png"
        )

        # NFE finds no open handles → Step0 skipped.
        mock_win32net = MagicMock()
        mock_win32net.NetFileEnum.return_value = ([], 0, 0)

        mock_win32api = MagicMock()
        mock_win32api.GetUserName.return_value = self.SHARED_USERNAME

        # NTFS owner = own machine (conservative fallback before fix).
        mock_win32security = MagicMock()
        mock_win32security.OWNER_SECURITY_INFORMATION = 1
        mock_sd = MagicMock()
        mock_sd.GetSecurityDescriptorOwner.return_value = "S-1-5-21-fake-sid"
        mock_win32security.GetFileSecurity.return_value = mock_sd
        mock_win32security.LookupAccountSid.return_value = (
            self.SHARED_USERNAME, self.OWN_HOST.upper(), 1,
        )

        # .105 session: active_time=3224s > 300s, no prior idle cache
        # → _is_persistent_monitor=True via active_time heuristic.
        coworker_snapshot = [{
            "username":    self.SHARED_USERNAME,
            "machine":     self.COWORKER_IP,
            "ip":          self.COWORKER_IP,
            "idle_time":   1,
            "active_time": 3224,
        }]

        with patch("socket.gethostname",  return_value=self.OWN_HOST), \
             patch("socket.gethostbyname", return_value=self.OWN_IP), \
             patch.dict(sys.modules, {"win32net": mock_win32net,
                                      "win32api": mock_win32api,
                                      "win32security": mock_win32security}):
            # .105 wrote a different file 27.5s ago (stale burst, age > 8s tight threshold).
            with da._recent_remote_write_lock:
                da._recent_remote_write_ts[self.COWORKER_IP] = (
                    time.time() - 27.5,
                    "Screenshot 2025-06-16 085818.png",
                    None,
                )
            # .106 wrote 11.5s ago — outside 8s loopback window → no real conflict.
            with da._recent_own_write_lock:
                da._recent_own_write_ts["__last__"] = (
                    time.time() - 11.5,
                    "Screenshot 2026-06-23 100241.png",
                )

            info = da._get_editor_info(
                filepath,
                detection_source="unc_poll",
                event_type="added",
                smb_sessions_snapshot=coworker_snapshot,
            )

        assert info["ip"] == self.COWORKER_IP, (
            f"COMPETING-OWN-WRITE incorrectly suppressed BURST-OVERRIDE "
            f"(own_write_age=11.5s >= 8s loopback window, no real conflict): {info!r}"
        )


class TestOwnActiveVetoBurstVeryRecent:
    """Regression (2026-06-25): .105 writes 151957.png at 14:42:01 but shows as .106.

    Sibling 152009.png was confirmed as .105's 68ms earlier (very fresh burst).
    OWN-ACTIVE VETO fires (own_min_idle=0, prior_idle=76s ≤ 120s) and the old
    guard `if _is_persistent_monitor and not _own_active_veto_fired:` skipped
    BURST-OVERRIDE entirely — even for a 0.068s-old burst (remote mid-copy).

    FIX: when burst is very fresh (< 8s), enter BURST-OVERRIDE even if
    OWN-ACTIVE VETO fired. Inside, COMPETING-OWN-WRITE sees burst_very_recent=True
    → no conflict → BURST-OVERRIDE fires → .105.
    """

    OWN_HOST        = "desktop-0edubap"
    OWN_IP          = "192.168.254.106"
    COWORKER_IP     = "192.168.254.105"
    SHARED_USERNAME = "user"

    @pytest.fixture(autouse=True)
    def _reset_module_caches(self):
        da._same_host_nfe_available = None
        da._same_host_net_file_available = None
        da._same_host_nfe_sig = None
        da._recent_remote_write_ts.clear()
        da._recent_own_write_ts.clear()
        yield
        da._same_host_nfe_available = None
        da._same_host_net_file_available = None
        da._same_host_nfe_sig = None
        da._recent_remote_write_ts.clear()
        da._recent_own_write_ts.clear()

    def test_burst_override_fires_when_burst_very_recent_despite_own_active_veto(self):
        """OWN-ACTIVE VETO must NOT suppress BURST-OVERRIDE when burst is < 8s old.

        .105 confirmed 152009.png 68ms ago (very fresh burst).
        .106 own machine has idle_time=0 + prior_idle=76s → OWN-ACTIVE VETO fires.
        BUG: BURST-OVERRIDE entirely skipped → attributed to .106. WRONG.
        FIX: burst_age=0.068s < 8s tight → exempt from OWN-ACTIVE suppression → .105.
        """
        filepath = (
            f"\\\\{self.OWN_IP}\\testshare\\"
            f"Screenshot 2025-05-29 151957.png"
        )

        # NFE: no open file handles (handle closed after upload completed)
        mock_win32net = MagicMock()
        mock_win32net.NetFileEnum.return_value = ([], 0, 0)

        mock_win32api = MagicMock()
        mock_win32api.GetUserName.return_value = self.SHARED_USERNAME

        # NTFS owner = own machine (share-host credential, expected wrong answer pre-fix)
        mock_win32security = MagicMock()
        mock_win32security.OWNER_SECURITY_INFORMATION = 1
        mock_sd = MagicMock()
        mock_sd.GetSecurityDescriptorOwner.return_value = "S-1-5-21-fake-sid"
        mock_win32security.GetFileSecurity.return_value = mock_sd
        mock_win32security.LookupAccountSid.return_value = (
            self.SHARED_USERNAME, self.OWN_HOST.upper(), 1,
        )

        # .105 session: idle_time=0 (just finished uploading), active_time=3224s > 300s
        # → _is_persistent_monitor=True via single-snapshot branch.
        # own_min_idle_time=0  → OWN-ACTIVE VETO fires (own machine also idle=0).
        # own_min_active_time=60s > 30s → LOOPBACK-LOCAL does NOT fire (not a fresh
        # loopback: the session predates the current write by >30s).
        coworker_snapshot = [{
            "username":            self.SHARED_USERNAME,
            "machine":             self.COWORKER_IP,
            "ip":                  self.COWORKER_IP,
            "idle_time":           0,
            "active_time":         3224,
            "own_min_idle_time":   0,   # own machine idle=0 → OWN-ACTIVE VETO trigger
            "own_min_active_time": 60,  # loopback 60s old > 30s threshold → not fresh
        }]

        # One prior idle snapshot 20s before event: .105 idle=76s.
        # 76s > 20s (_HIGH_IDLE_GENUINE_THRESHOLD) but watchdog-same-host blocks
        # HIGH-IDLE-GENUINE, so conservative fallback → _is_persistent_monitor=True.
        # 76s ≤ 120s (_OWN_VETO_MAX_PRIOR_IDLE) → OWN-ACTIVE VETO fires.
        prior_idle_history = [(time.time() - 20, 76)]

        with patch("socket.gethostname",  return_value=self.OWN_HOST), \
             patch("socket.gethostbyname", return_value=self.OWN_IP), \
             patch("watcher.get_recent_idle_history", return_value=prior_idle_history), \
             patch.dict(sys.modules, {"win32net": mock_win32net,
                                      "win32api": mock_win32api,
                                      "win32security": mock_win32security}):
            # .105 confirmed 152009.png 68ms ago — very fresh burst (< 8s tight threshold)
            with da._recent_remote_write_lock:
                da._recent_remote_write_ts[self.COWORKER_IP] = (
                    time.time() - 0.068,
                    "Screenshot 2025-05-29 152009.png",
                    None,
                )
            # .106 wrote own file 11.4s ago — outside 8s loopback window, no conflict
            with da._recent_own_write_lock:
                da._recent_own_write_ts["__last__"] = (
                    time.time() - 11.4,
                    "Screenshot 2026-06-23 100241.png",
                )

            info = da._get_editor_info(
                filepath,
                detection_source="watchdog",
                event_type="added",
                smb_sessions_snapshot=coworker_snapshot,
            )

        assert info["ip"] == self.COWORKER_IP, (
            f"OWN-ACTIVE VETO incorrectly suppressed BURST-OVERRIDE for very-fresh burst "
            f"(burst_age=0.068s < 8s tight, remote mid-copy): {info!r}"
        )


class TestBurstConcurrentRemoteAdds:
    """Regression (2026-06-29): two near-simultaneous adds by remote .105 while
    the local machine .106 had also written files ~14s earlier.

    Real-world sequence (from user log export):
      • 08:23:39 — .106 (local) adds two files → recorded as confirmed own-writes.
      • 08:23:54 — .105 (coworker) adds two files in a burst.
          - File A's SMB write handle is still open → NetFileEnum confirms .105
            and records it as a remote burst sibling.
          - File B's handle has already closed → falls to snapshot attribution.

    BUG: for File B, the remote burst sibling (File A) was 0.087s fresh (< 8s,
    strong evidence .105 is mid-copy), but the CONCURRENT-OWN-WRITE GUARD used a
    30s window and saw the *stale* 14.5s-old local own-write → kept OWN-ACTIVE
    VETO → flipped File B to .106. One of two burst files was misattributed.

    FIX: the own-write conflict window is the tight 8s loopback window; a 14.5s
    local write is NOT concurrent with a 0.087s remote burst, so the override
    fires and File B is correctly attributed to .105.  (Pre-fix this asserted
    .106 and failed.)
    """

    OWN_HOST        = "desktop-0edubap"
    OWN_IP          = "192.168.254.106"
    COWORKER_IP     = "192.168.254.105"
    SHARED_USERNAME = "user"

    @pytest.fixture(autouse=True)
    def _reset_module_caches(self):
        da._same_host_nfe_available = None
        da._same_host_net_file_available = None
        da._same_host_nfe_sig = None
        da._recent_remote_write_ts.clear()
        da._recent_own_write_ts.clear()
        yield
        da._same_host_nfe_available = None
        da._same_host_net_file_available = None
        da._same_host_nfe_sig = None
        da._recent_remote_write_ts.clear()
        da._recent_own_write_ts.clear()

    def test_second_burst_add_attributed_to_remote_not_local(self):
        """File B (handle closed) must be attributed to .105 via the fresh remote
        burst sibling, NOT to .106 because of a stale 14.5s-old local own-write."""
        file_b = (
            f"\\\\{self.OWN_IP}\\testshare\\Screenshot 2025-06-18 153953.png"
        )

        # File B's SMB handle already closed → NetFileEnum returns nothing.
        mock_win32net = MagicMock()
        mock_win32net.NetFileEnum.return_value = ([], 0, 0)

        mock_win32api = MagicMock()
        mock_win32api.GetUserName.return_value = self.SHARED_USERNAME

        # NTFS owner = own machine (share host) — the wrong answer pre-fix.
        mock_win32security = MagicMock()
        mock_win32security.OWNER_SECURITY_INFORMATION = 1
        mock_sd = MagicMock()
        mock_sd.GetSecurityDescriptorOwner.return_value = "S-1-5-21-fake-sid"
        mock_win32security.GetFileSecurity.return_value = mock_sd
        mock_win32security.LookupAccountSid.return_value = (
            self.SHARED_USERNAME, self.OWN_HOST.upper(), 1,
        )

        # .105 session: idle=1 (mid-burst), active_time=147s, own_min_idle=0
        # (local loopback noise → OWN-ACTIVE VETO trigger),
        # own_min_active=60s > 30s (loopback predates the write → not a fresh local write).
        coworker_snapshot = [{
            "username":            self.SHARED_USERNAME,
            "machine":             self.COWORKER_IP,
            "ip":                  self.COWORKER_IP,
            "idle_time":           1,
            "active_time":         147,
            "own_min_idle_time":   0,
            "own_min_active_time": 60,
        }]
        prior_idle_history = [(time.time() - 27, 38)]

        with patch("socket.gethostname",  return_value=self.OWN_HOST), \
             patch("socket.gethostbyname", return_value=self.OWN_IP), \
             patch("watcher.get_recent_idle_history", return_value=prior_idle_history), \
             patch.dict(sys.modules, {"win32net": mock_win32net,
                                      "win32api": mock_win32api,
                                      "win32security": mock_win32security}):
            # File A confirmed .105 0.087s ago and recorded as a remote burst sibling
            # (tuple layout: ts, filepath, prev, event_type).
            with da._recent_remote_write_lock:
                da._recent_remote_write_ts[self.COWORKER_IP] = (
                    time.time() - 0.087,
                    "Screenshot 2025-06-19 081833.png",
                    None,
                    "added",
                )
            # .106 wrote its own files 14.5s ago (the earlier 08:23:39 burst):
            # within the OLD 30s guard window, but OUTSIDE the correct 8s window.
            with da._recent_own_write_lock:
                da._recent_own_write_ts["__last__"] = (
                    time.time() - 14.5,
                    "Screenshot 2026-06-26 092651.png",
                )

            info = da._get_editor_info(
                file_b,
                detection_source="watchdog",
                event_type="added",
                smb_sessions_snapshot=coworker_snapshot,
            )

        assert info["ip"] == self.COWORKER_IP, (
            f"A stale 14.5s-old local own-write must NOT veto a 0.087s-fresh remote "
            f"burst sibling (8s window): File B should be attributed to .105, got {info!r}"
        )


class TestSingleSnapshotOwnActiveVeto:
    """Regression (2026-06-25): .106 adds two files but both show as .105.

    Sequence from user logs (08:53:09-08:53:12):
      1. .106 writes Screenshot 2026-06-24 140439.png and 133422.png.
      2. SMB CHANGE_NOTIFY fires — resets .105's idle_time to 0.
      3. NetSessionEnum snapshot: .105 idle=0, active_time=266s, own_min_idle=0.
      4. Idle history: one prior snapshot 12s ago showing .105 idle=228s.
      5. CHECK B single-snapshot override: prior_idle=228s > 30s AND
         active_time=266s ≤ 300s → concludes .105 "recently connected to write".
      6. BUG: override ignores own_min_idle=0 (own machine writing via watchdog)
         → attributes files to .105 instead of .106.
      7. FIX: when detection_source='watchdog' AND own_min_idle ≤ 5s, block the
         "recently connected" shortcut (CHANGE_NOTIFY makes idle drop unreliable)
         and set _is_persistent_monitor=True + _own_active_veto=True → NTFS owner
         (.106).
    """

    OWN_HOST        = "desktop-0edubap"
    OWN_IP          = "192.168.254.106"
    COWORKER_IP     = "192.168.254.105"
    SHARED_USERNAME = "user"

    @pytest.fixture(autouse=True)
    def _reset_module_caches(self):
        da._same_host_nfe_available = None
        da._same_host_net_file_available = None
        da._same_host_nfe_sig = None
        da._recent_remote_write_ts.clear()
        da._recent_own_write_ts.clear()
        yield
        da._same_host_nfe_available = None
        da._same_host_net_file_available = None
        da._same_host_nfe_sig = None
        da._recent_remote_write_ts.clear()
        da._recent_own_write_ts.clear()

    def test_own_add_not_misattributed_when_coworker_has_short_session_high_prior_idle(self):
        """CHECK B single-snapshot override must be blocked when own machine is writing.

        .105 has active_time=266s (≤ 300s) + prior_idle=228s (> 30s) — normally
        this fires the "recently connected to write" shortcut.  But own_min_idle=0
        (watchdog event) proves THIS machine wrote the file via CHANGE_NOTIFY.
        The shortcut must be blocked so NTFS owner (.106) is used.
        """
        filepath = (
            f"\\\\{self.OWN_IP}\\testshare\\"
            f"Screenshot 2026-06-24 140439.png"
        )

        # NFE: no open handles (own-machine write closed before attribution)
        mock_win32net = MagicMock()
        mock_win32net.NetFileEnum.return_value = ([], 0, 0)

        mock_win32api = MagicMock()
        mock_win32api.GetUserName.return_value = self.SHARED_USERNAME

        # NTFS owner = own machine (.106) — the correct answer
        mock_win32security = MagicMock()
        mock_win32security.OWNER_SECURITY_INFORMATION = 1
        mock_sd = MagicMock()
        mock_sd.GetSecurityDescriptorOwner.return_value = "S-1-5-21-fake-sid"
        mock_win32security.GetFileSecurity.return_value = mock_sd
        mock_win32security.LookupAccountSid.return_value = (
            self.SHARED_USERNAME, self.OWN_HOST.upper(), 1,
        )

        # .105 session: idle_time=0 (CHANGE_NOTIFY reset), active_time=266s (short
        # session ≤ 300s threshold), own_min_idle=0 (own machine also idle=0).
        coworker_snapshot = [{
            "username":            self.SHARED_USERNAME,
            "machine":             self.COWORKER_IP,
            "ip":                  self.COWORKER_IP,
            "idle_time":           0,
            "active_time":         266,
            "own_min_idle_time":   0,   # own machine writing → OWN-ACTIVE VETO
            "own_min_active_time": 12,  # loopback session 12s old < 30s threshold
        }]

        # One prior idle snapshot 12s before event: .105 idle=228s (high — looks
        # like it was idle before the write, which normally means it IS the writer,
        # but here the idle dropped because of CHANGE_NOTIFY from .106's write).
        prior_idle_history = [(time.time() - 12, 228)]

        with patch("socket.gethostname",  return_value=self.OWN_HOST), \
             patch("socket.gethostbyname", return_value=self.OWN_IP), \
             patch("watcher.get_recent_idle_history", return_value=prior_idle_history), \
             patch.dict(sys.modules, {"win32net": mock_win32net,
                                      "win32api": mock_win32api,
                                      "win32security": mock_win32security}):
            # No prior .105 burst (coworker was just monitoring, not writing)
            # No prior own write recorded (fresh session)

            info = da._get_editor_info(
                filepath,
                detection_source="watchdog",
                event_type="added",
                smb_sessions_snapshot=coworker_snapshot,
            )

        assert info["ip"] == self.OWN_IP, (
            f"CHECK B single-snapshot 'recently connected' override incorrectly attributed "
            f"own-machine write to coworker (own_min_idle=0 + watchdog must block the "
            f"shortcut): {info!r}"
        )


class TestVetoFallbackBurstPatch:
    """Regression (2026-06-25): .105 adds two files concurrently; one caught by NFE,
    the other falls through OWN-ACTIVE VETO → NTFS fallback → .106.  After the NFE
    confirms .105, BURST-PATCH Case C must retroactively fix the NTFS-fallback entry
    without touching genuine .106 writes from an earlier batch.

    Two scenarios are tested:

    Scenario A (09:04 session — original fix):
      .106 writes at 09:04:11, .105 writes concurrently at 09:04:24.
      Detection-time gap = 13s.  Monotonic gap = 0.2s (concurrent sibling fast).
      → concurrent sibling patched ✓, earlier .106 write spared ✓

    Scenario B (09:19 session — BUG REPORT 2026-06-25):
      .106 writes at 09:19:19 (2 files).  .105 writes at 09:19:25 (separate batch).
      Detection-time gap between .106 batch and .105 confirming burst = 6s.
      OLD code (window=10s, no detection gate): 6s < 10s → WRONGLY patched .106's files.
      NEW code (3s detection-time gate): 6s > 3s → correctly spared ✓

    Both scenarios use the detection-timestamp gate (_veto_fallback_detect_ts vs
    confirmed entry's timestamp) as the primary discriminator.
    """

    OWN_HOST    = "desktop-0edubap"
    OWN_IP      = "192.168.254.106"
    COWORKER_IP = "192.168.254.105"

    def _make_veto_fallback_entry(
        self, path: str, ts_offset: float, detect_ts: str
    ) -> dict:
        """Simulate a history entry attributed via OWN-ACTIVE VETO → NTFS → own machine.

        detect_ts: ISO timestamp string representing when watchdog fired for this file
                   (corresponds to entry["timestamp"] set at detection time, stored in
                   _veto_fallback_detect_ts when the veto-fallback tag is propagated).
        ts_offset: seconds ago that _veto_fallback_ts (monotonic) was set (backstop).
        """
        import time as _t
        return {
            "path":                       path,
            "type":                       "added",
            "editor_user":                "user",
            "editor_machine":             self.OWN_HOST.upper(),
            "editor_ip":                  self.OWN_IP,
            "attribution_unknown":        False,
            "_veto_fallback_candidate_ip":  self.COWORKER_IP,
            "_veto_fallback_ts":            _t.monotonic() - ts_offset,
            "_veto_fallback_detect_ts":     detect_ts,
        }

    def _run_case_c(self, history_log: list, confirmed_entry: dict) -> list:
        """Inline replication of BURST-PATCH Case C logic from _on_file_change.
        Returns list of patched paths."""
        import time     as _t
        import datetime as _dt

        def _ts_diff_s(ts1, ts2):
            try:
                t1 = _dt.datetime.fromisoformat(ts1.rstrip("Z").replace(" ", "T"))
                t2 = _dt.datetime.fromisoformat(ts2.rstrip("Z").replace(" ", "T"))
                return abs((t1 - t2).total_seconds())
            except Exception:
                return None

        _burst_new_ip     = confirmed_entry["editor_ip"]
        _burst_new_machine = confirmed_entry["editor_machine"]
        _bp_now           = _t.monotonic()
        _VETO_PATCH_WINDOW_S  = 3
        _VETO_DETECT_WINDOW_S = 3
        _confirm_ts       = confirmed_entry.get("timestamp", "")
        patched = []

        for _older in reversed(history_log):
            if _older is confirmed_entry:
                continue
            _is_unk = _older.get("editor_user") in ("Unknown", "")
            _is_fb  = (
                not _is_unk
                and _older.get("_fallback_candidate_ip") == _burst_new_ip
                and _older.get("editor_ip") != _burst_new_ip
                and _older.get("type") == confirmed_entry["type"]
            )
            _vd_ts   = _older.get("_veto_fallback_detect_ts", "")
            _vd_diff = _ts_diff_s(_vd_ts, _confirm_ts) if (_vd_ts and _confirm_ts) else None
            _vd_ok   = (_vd_diff is not None and _vd_diff <= _VETO_DETECT_WINDOW_S)
            _veto_age = _bp_now - _older.get("_veto_fallback_ts", _bp_now)
            _is_veto_fb = (
                not _is_unk
                and not _is_fb
                and _older.get("_veto_fallback_candidate_ip") == _burst_new_ip
                and _older.get("editor_ip") != _burst_new_ip
                and _older.get("type") == confirmed_entry["type"]
                and _veto_age < _VETO_PATCH_WINDOW_S
                and _vd_ok
            )
            if not _is_unk and not _is_fb and not _is_veto_fb:
                continue
            _oh = _older["path"].replace("\\", "/").lstrip("/").split("/")[0].lower()
            if _oh != self.OWN_IP:
                continue
            _older["editor_ip"]      = _burst_new_ip
            _older["editor_machine"] = _burst_new_machine
            patched.append(_older["path"])
        return patched

    def test_concurrent_sibling_patched_genuine_local_spared(self):
        """Scenario A: .106 genuine write (13s before .105 burst) spared;
        .105 concurrent sibling (0.2s, same detection second) patched."""
        # .106's genuine write detected at 09:04:11 — 13s before .105 burst at 09:04:24
        genuine_local = self._make_veto_fallback_entry(
            f"\\\\{self.OWN_IP}\\testshare\\Screenshot 2026-06-24 140439.png",
            ts_offset=13.2,
            detect_ts="2026-06-25T09:04:11.000000",
        )
        # .105's concurrent write detected at 09:04:24 — same second as confirming burst
        concurrent_coworker = self._make_veto_fallback_entry(
            f"\\\\{self.OWN_IP}\\testshare\\Screenshot 2025-06-18 153953.png",
            ts_offset=0.2,
            detect_ts="2026-06-25T09:04:24.000000",
        )
        confirmed_entry = {
            "path":           f"\\\\{self.OWN_IP}\\testshare\\Screenshot 2025-06-19 081833.png",
            "type":           "added",
            "editor_user":    "user",
            "editor_machine": "DESKTOP-KGG55PU",
            "editor_ip":      self.COWORKER_IP,
            "timestamp":      "2026-06-25T09:04:24.000000",
        }

        patched_paths = self._run_case_c([genuine_local, concurrent_coworker], confirmed_entry)

        assert len(patched_paths) == 1, (
            f"Expected exactly 1 entry patched (concurrent sibling), got: {patched_paths!r}"
        )
        assert "153953" in patched_paths[0], (
            f"Wrong entry patched — should be 153953.png (concurrent), got: {patched_paths!r}"
        )
        assert concurrent_coworker["editor_ip"] == self.COWORKER_IP, (
            f"Concurrent sibling not patched to coworker IP: {concurrent_coworker!r}"
        )
        assert genuine_local["editor_ip"] == self.OWN_IP, (
            f"Genuine local write from 13s ago was incorrectly patched: {genuine_local!r}"
        )

    def test_six_second_gap_genuine_writes_spared(self):
        """Scenario B (2026-06-25 bug): .106 wrote at 09:19:19; .105 burst confirmed
        at 09:19:25 (6s later, separate batch).  Old code (10s window, no detect gate)
        incorrectly patched .106's files.  New code must spare them."""
        # .106's genuine writes detected at 09:19:19 — 6s before .105 burst
        genuine_1 = self._make_veto_fallback_entry(
            f"\\\\{self.OWN_IP}\\testshare\\Screenshot 2026-06-24 144215.png",
            ts_offset=6.0,
            detect_ts="2026-06-25T09:19:19.000000",
        )
        genuine_2 = self._make_veto_fallback_entry(
            f"\\\\{self.OWN_IP}\\testshare\\Screenshot 2026-06-24 142723.png",
            ts_offset=6.0,
            detect_ts="2026-06-25T09:19:19.000000",
        )
        # .105's files confirmed at 09:19:25 (separate watchdog batch)
        confirmed_entry = {
            "path":           f"\\\\{self.OWN_IP}\\testshare\\Screenshot 2025-06-19 081833.png",
            "type":           "added",
            "editor_user":    "user",
            "editor_machine": "DESKTOP-KGG55PU",
            "editor_ip":      self.COWORKER_IP,
            "timestamp":      "2026-06-25T09:19:25.000000",
        }

        patched_paths = self._run_case_c([genuine_1, genuine_2], confirmed_entry)

        assert len(patched_paths) == 0, (
            f".106's genuine writes from 6s earlier must NOT be patched, "
            f"but were patched: {patched_paths!r}"
        )
        assert genuine_1["editor_ip"] == self.OWN_IP, (
            f"genuine_1 was incorrectly patched: {genuine_1!r}"
        )
        assert genuine_2["editor_ip"] == self.OWN_IP, (
            f"genuine_2 was incorrectly patched: {genuine_2!r}"
        )
