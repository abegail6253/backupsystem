"""
tests/test_integrity_scheduler.py — Unit tests for integrity_scheduler.py
Run:  pytest tests/

PyQt5 and the backend modules are mocked so these tests run in any environment,
including headless CI, without a running Qt application.

We focus on the pure-Python scheduling logic:
  - _resolve_dest
  - IntegrityWorker.run  (summary counting, skipping watches with no backup_dir)
  - IntegrityScheduler._tick  (filtering, due-date logic, force flag)
"""
import sys
import types
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch, call
import pytest

# ─── Stub out PyQt5 before importing the module ──────────────────────────────
# This lets the tests run on CI machines that don't have Qt installed.

def _make_qt_stubs():
    """Return a minimal mock of the PyQt5 objects used by integrity_scheduler."""

    class _Signal:
        """Minimal pyqtSignal stand-in that records emits and allows connect()."""
        def __init__(self, *types):
            self._slots = []
        def connect(self, slot):
            self._slots.append(slot)
        def emit(self, *args):
            for s in self._slots:
                s(*args)

    class _QThread:
        def __init__(self, parent=None):
            pass
        def start(self):
            pass
        def isRunning(self):
            return False
        def deleteLater(self):
            pass

    class _QTimer:
        def __init__(self, parent=None):
            self._interval = 0
        def setInterval(self, ms):
            self._interval = ms
        def start(self):
            pass
        def stop(self):
            pass
        def timeout(self):
            pass
        # Make timeout a signal-like object
        timeout = _Signal()

    @staticmethod
    def _singleShot(ms, fn):
        pass  # don't actually delay in tests

    class _QObject:
        def __init__(self, parent=None):
            pass

    # Assemble mock PyQt5 module tree
    pyqt5       = types.ModuleType("PyQt5")
    qtcore      = types.ModuleType("PyQt5.QtCore")
    qtwidgets   = types.ModuleType("PyQt5.QtWidgets")

    qtcore.QThread      = _QThread
    qtcore.QTimer       = _QTimer
    qtcore.QTimer.singleShot = _singleShot
    qtcore.pyqtSignal   = lambda *a: _Signal(*a)
    qtcore.QObject      = _QObject

    qtwidgets.QGroupBox  = MagicMock()
    qtwidgets.QVBoxLayout = MagicMock()
    qtwidgets.QHBoxLayout = MagicMock()
    qtwidgets.QCheckBox  = MagicMock()
    qtwidgets.QSpinBox   = MagicMock()
    qtwidgets.QLabel     = MagicMock()
    qtwidgets.QWidget    = MagicMock()

    pyqt5.QtCore    = qtcore
    pyqt5.QtWidgets = qtwidgets

    return pyqt5, qtcore, qtwidgets

_pyqt5, _qtcore, _qtwidgets = _make_qt_stubs()

sys.modules.setdefault("PyQt5",           _pyqt5)
sys.modules.setdefault("PyQt5.QtCore",    _qtcore)
sys.modules.setdefault("PyQt5.QtWidgets", _qtwidgets)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import integrity_scheduler as isch


# ─── _resolve_dest ────────────────────────────────────────────────────────────

class TestResolveDest:
    def test_uses_watch_override_first(self):
        watch = {"dest_override": "/custom/dest"}
        cfg   = {"destination": "/global/dest"}
        assert isch._resolve_dest(watch, cfg) == "/custom/dest"

    def test_falls_back_to_global_destination(self):
        watch = {}
        cfg   = {"destination": "/global/dest"}
        assert isch._resolve_dest(watch, cfg) == "/global/dest"

    def test_default_destination_when_cfg_empty(self):
        assert isch._resolve_dest({}, {}) == "./backups"

    def test_empty_override_falls_through(self):
        watch = {"dest_override": ""}
        cfg   = {"destination": "/global"}
        # Empty string is falsy — should use global
        assert isch._resolve_dest(watch, cfg) == "/global"


# ─── IntegrityWorker.run ──────────────────────────────────────────────────────

class TestIntegrityWorkerRun:
    def _make_worker(self, jobs, validate_result):
        """Construct a worker with mocked backend."""
        mock_be = MagicMock()
        mock_be.validate_backup.return_value = validate_result

        mock_cm = MagicMock()
        mock_cm.load.return_value = {"watches": [
            {"id": j["watch_id"], "name": j["watch_name"]} for j in jobs
        ]}

        worker = isch.IntegrityWorker.__new__(isch.IntegrityWorker)
        worker._jobs   = jobs
        worker._cfg    = {}
        worker.watch_done = MagicMock()
        worker.finished   = MagicMock()

        with patch.object(isch, "backup_engine", mock_be), \
             patch.object(isch, "config_manager", mock_cm), \
             patch.object(isch, "_BACKEND_AVAILABLE", True):
            worker.run()

        return worker, mock_be

    def test_valid_backup_increments_ok(self):
        jobs = [{"watch_name": "Docs", "watch_id": "w1", "backup_dir": "/b/1"}]
        w, be = self._make_worker(jobs, {"valid": True, "manifest_ok": True})
        # finished should be emitted with ok=1
        w.finished.emit.assert_called_once()
        summary = w.finished.emit.call_args[0][0]
        assert summary["ok"] == 1
        assert summary["failed"] == 0

    def test_invalid_backup_increments_failed(self):
        jobs = [{"watch_name": "Docs", "watch_id": "w1", "backup_dir": "/b/1"}]
        w, be = self._make_worker(jobs, {"valid": False, "manifest_ok": False,
                                          "missing_files": ["x.txt"], "corrupted_files": []})
        summary = w.finished.emit.call_args[0][0]
        assert summary["failed"] == 1
        assert summary["ok"] == 0

    def test_missing_backup_dir_skipped(self):
        jobs = [{"watch_name": "Docs", "watch_id": "w1", "backup_dir": ""}]
        w, be = self._make_worker(jobs, {"valid": True, "manifest_ok": True})
        summary = w.finished.emit.call_args[0][0]
        assert summary["skipped"] == 1
        be.validate_backup.assert_not_called()

    def test_exception_in_validate_counts_as_failed(self):
        jobs = [{"watch_name": "Docs", "watch_id": "w1", "backup_dir": "/b/1"}]
        mock_be = MagicMock()
        mock_be.validate_backup.side_effect = RuntimeError("disk error")
        mock_cm = MagicMock()
        mock_cm.load.return_value = {"watches": [{"id": "w1", "name": "Docs"}]}

        worker = isch.IntegrityWorker.__new__(isch.IntegrityWorker)
        worker._jobs   = jobs
        worker._cfg    = {}
        worker.watch_done = MagicMock()
        worker.finished   = MagicMock()

        with patch.object(isch, "backup_engine", mock_be), \
             patch.object(isch, "config_manager", mock_cm), \
             patch.object(isch, "_BACKEND_AVAILABLE", True):
            worker.run()

        summary = worker.finished.emit.call_args[0][0]
        assert summary["failed"] == 1

    def test_watch_done_emitted_per_watch(self):
        jobs = [
            {"watch_name": "A", "watch_id": "w1", "backup_dir": "/b/1"},
            {"watch_name": "B", "watch_id": "w2", "backup_dir": "/b/2"},
        ]
        w, _ = self._make_worker(jobs, {"valid": True, "manifest_ok": True})
        assert w.watch_done.emit.call_count == 2

    def test_multiple_watches_summary(self):
        jobs = [
            {"watch_name": "A", "watch_id": "w1", "backup_dir": "/b/1"},
            {"watch_name": "B", "watch_id": "w2", "backup_dir": ""},     # skipped
            {"watch_name": "C", "watch_id": "w3", "backup_dir": "/b/3"},
        ]
        mock_be = MagicMock()
        # A passes, C fails
        mock_be.validate_backup.side_effect = [
            {"valid": True,  "manifest_ok": True},
            {"valid": False, "manifest_ok": False, "missing_files": [], "corrupted_files": []},
        ]
        mock_cm = MagicMock()
        mock_cm.load.return_value = {"watches": [
            {"id": j["watch_id"], "name": j["watch_name"]} for j in jobs
        ]}

        worker = isch.IntegrityWorker.__new__(isch.IntegrityWorker)
        worker._jobs   = jobs
        worker._cfg    = {}
        worker.watch_done = MagicMock()
        worker.finished   = MagicMock()

        with patch.object(isch, "backup_engine", mock_be), \
             patch.object(isch, "config_manager", mock_cm), \
             patch.object(isch, "_BACKEND_AVAILABLE", True):
            worker.run()

        summary = worker.finished.emit.call_args[0][0]
        assert summary == {"ok": 1, "failed": 1, "skipped": 1}


# ─── IntegrityScheduler._tick ─────────────────────────────────────────────────

class TestIntegritySchedulerTick:
    def _make_sched(self, cfg):
        """Build an IntegrityScheduler with mocked Qt and backend."""
        parent = MagicMock()
        sched  = object.__new__(isch.IntegrityScheduler)
        sched._parent  = parent
        sched._worker  = None
        sched._timer   = MagicMock()
        sched._bridge  = MagicMock()
        sched._bridge.watch_result = MagicMock()
        sched._bridge.run_finished = MagicMock()
        sched.watch_result = sched._bridge.watch_result
        sched.run_finished = sched._bridge.run_finished
        return sched

    def test_tick_skips_when_disabled(self):
        sched = self._make_sched({})
        cfg = {"integrity_check_enabled": False, "watches": [
            {"id": "w1", "name": "X", "active": True, "last_backup": "2026-01-01T00:00:00"},
        ]}
        mock_be = MagicMock()
        mock_cm = MagicMock()
        mock_cm.load.return_value = cfg

        with patch.object(isch, "backup_engine", mock_be), \
             patch.object(isch, "config_manager", mock_cm), \
             patch.object(isch, "_BACKEND_AVAILABLE", True):
            sched._tick(force=False)

        mock_be.list_backups.assert_not_called()

    def test_tick_force_overrides_disabled(self):
        sched = self._make_sched({})
        cfg = {
            "integrity_check_enabled": False,
            "integrity_check_interval_days": 7,
            "watches": [
                {
                    "id": "w1", "name": "X", "active": True,
                    "last_backup": "2026-01-01T00:00:00",
                    "last_integrity_check": None,
                }
            ],
        }
        mock_be = MagicMock()
        mock_be.list_backups.return_value = [{"backup_dir": "/b/1"}]
        mock_cm = MagicMock()
        mock_cm.load.return_value = cfg

        launched_workers = []

        def _fake_worker_init(jobs, cfg_arg, parent=None):
            m = MagicMock()
            m.isRunning.return_value = False
            launched_workers.append(jobs)
            return m

        with patch.object(isch, "backup_engine", mock_be), \
             patch.object(isch, "config_manager", mock_cm), \
             patch.object(isch, "_BACKEND_AVAILABLE", True), \
             patch.object(isch, "IntegrityWorker", side_effect=_fake_worker_init):
            sched._tick(force=True)

        assert len(launched_workers) == 1

    def test_tick_skips_watch_without_last_backup(self):
        sched = self._make_sched({})
        cfg = {
            "integrity_check_enabled": True,
            "integrity_check_interval_days": 7,
            "watches": [
                {"id": "w1", "name": "X", "active": True, "last_backup": None},
            ],
        }
        mock_be = MagicMock()
        mock_cm = MagicMock()
        mock_cm.load.return_value = cfg

        with patch.object(isch, "backup_engine", mock_be), \
             patch.object(isch, "config_manager", mock_cm), \
             patch.object(isch, "_BACKEND_AVAILABLE", True):
            sched._tick(force=False)

        mock_be.list_backups.assert_not_called()

    def test_tick_skips_watch_checked_recently(self):
        sched = self._make_sched({})
        recent = (datetime.now() - timedelta(days=1)).isoformat()
        cfg = {
            "integrity_check_enabled": True,
            "integrity_check_interval_days": 7,
            "watches": [
                {
                    "id": "w1", "name": "X", "active": True,
                    "last_backup": "2026-01-01T00:00:00",
                    "last_integrity_check": recent,
                }
            ],
        }
        mock_be = MagicMock()
        mock_cm = MagicMock()
        mock_cm.load.return_value = cfg

        with patch.object(isch, "backup_engine", mock_be), \
             patch.object(isch, "config_manager", mock_cm), \
             patch.object(isch, "_BACKEND_AVAILABLE", True):
            sched._tick(force=False)

        mock_be.list_backups.assert_not_called()

    def test_tick_runs_overdue_watch(self):
        sched = self._make_sched({})
        old = (datetime.now() - timedelta(days=10)).isoformat()
        cfg = {
            "integrity_check_enabled": True,
            "integrity_check_interval_days": 7,
            "watches": [
                {
                    "id": "w1", "name": "X", "active": True,
                    "last_backup": "2026-01-01T00:00:00",
                    "last_integrity_check": old,
                }
            ],
            "destination": "/backups",
        }
        mock_be = MagicMock()
        mock_be.list_backups.return_value = [{"backup_dir": "/b/1"}]
        mock_cm = MagicMock()
        mock_cm.load.return_value = cfg

        launched = []

        def _fake_worker(jobs, cfg_arg, parent=None):
            m = MagicMock()
            m.isRunning.return_value = False
            launched.append(jobs)
            return m

        with patch.object(isch, "backup_engine", mock_be), \
             patch.object(isch, "config_manager", mock_cm), \
             patch.object(isch, "_BACKEND_AVAILABLE", True), \
             patch.object(isch, "IntegrityWorker", side_effect=_fake_worker):
            sched._tick(force=False)

        assert len(launched) == 1
        assert launched[0][0]["watch_id"] == "w1"

    def test_tick_does_not_restart_running_worker(self):
        sched = self._make_sched({})
        running_worker = MagicMock()
        running_worker.isRunning.return_value = True
        sched._worker = running_worker

        cfg = {
            "integrity_check_enabled": True,
            "integrity_check_interval_days": 7,
            "watches": [],
        }
        mock_be = MagicMock()
        mock_cm = MagicMock()
        mock_cm.load.return_value = cfg

        with patch.object(isch, "backup_engine", mock_be), \
             patch.object(isch, "config_manager", mock_cm), \
             patch.object(isch, "_BACKEND_AVAILABLE", True):
            sched._tick(force=False)

        # load() should not even have been called
        mock_cm.load.assert_not_called()
