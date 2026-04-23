"""
integrity_scheduler.py — Scheduled backup integrity checker
============================================================

Runs validate_backup() for every watch on a configurable cadence
(default: weekly) without blocking the UI thread.

Architecture
------------
IntegrityWorker   QThread that calls backup_engine.validate_backup()
                  for a list of (watch, backup_dir) pairs.  Emits one
                  watch_done signal per watch, then a finished signal
                  when all are complete.

IntegrityScheduler  Thin coordinator that owns a QTimer, decides when
                  a check is due, and fires IntegrityWorker.  Designed
                  to live inside MainWindow.

Integration (minimal changes to desktop_app.py)
-----------------------------------------------
1.  Import at the top of desktop_app.py:
        from integrity_scheduler import IntegrityScheduler

2.  In MainWindow.__init__, after _start_auto_timer():
        self._integrity_scheduler = IntegrityScheduler(self)
        self._integrity_scheduler.watch_result.connect(self._on_integrity_result)
        self._integrity_scheduler.run_finished.connect(self._on_integrity_run_finished)
        self._integrity_scheduler.start()

3.  Add these two handlers to MainWindow:

        def _on_integrity_result(self, watch_name: str, result: dict):
            \"\"\"Called once per watch after its integrity check completes.\"\"\"
            ok = result.get("valid") and result.get("manifest_ok", True)
            if ok:
                self._append_log(f"✔ Integrity OK: {watch_name}")
            else:
                missing   = result.get("missing_files",   [])
                corrupted = result.get("corrupted_files", [])
                err       = result.get("error", "")
                detail    = ""
                if missing:
                    detail += f"  Missing:   {', '.join(missing[:3])}"
                    if len(missing) > 3:
                        detail += f" (+{len(missing)-3} more)"
                if corrupted:
                    detail += f"  Corrupted: {', '.join(corrupted[:3])}"
                    if len(corrupted) > 3:
                        detail += f" (+{len(corrupted)-3} more)"
                if err:
                    detail += f"  Error: {err}"
                self._append_log(f"⚠ Integrity FAILED: {watch_name}\\n{detail}")
                # Re-use the existing notification helpers (email + webhook)
                _send_email_notification(self.cfg,
                    subject=f"[BackupSys] Integrity check FAILED: {watch_name}",
                    body=(
                        f"Scheduled integrity check failed for watch '{watch_name}'.\\n"
                        f"\\n{detail}\\n"
                        f"\\nOpen BackupSys and run a new backup to repair."
                    ),
                )
                _send_webhook(self.cfg, {
                    "status":     "integrity_failed",
                    "watch_name": watch_name,
                    "missing":    missing,
                    "corrupted":  corrupted,
                    "error":      err,
                })

        def _on_integrity_run_finished(self, summary: dict):
            \"\"\"Called once when the full integrity run is complete.\"\"\"
            n_ok   = summary.get("ok",     0)
            n_fail = summary.get("failed", 0)
            n_skip = summary.get("skipped",0)
            self._append_log(
                f"Integrity run complete — "
                f"{n_ok} passed, {n_fail} failed, {n_skip} skipped."
            )

4.  In MainWindow.closeEvent (or wherever the app shuts down):
        self._integrity_scheduler.stop()

Config keys (add defaults in config_manager.load())
----------------------------------------------------
    "integrity_check_enabled":       False,
    "integrity_check_interval_days": 7,

Per-watch key (stored inside the watch dict):
    "last_integrity_check":          None   (ISO timestamp or None)

Admin Panel UI (Settings → General)
------------------------------------
Add a checkbox "Enable scheduled integrity checks" bound to
integrity_check_enabled, and a spinbox "Check every N days" bound to
integrity_check_interval_days.  See end of this file for a ready-made
_build_integrity_section() helper.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from PyQt5.QtCore import QThread, QTimer, pyqtSignal

logger = logging.getLogger(__name__)

# Lazy imports so this module can be unit-tested without a running Qt app.
try:
    import backup_engine
    import config_manager
    _BACKEND_AVAILABLE = True
except ImportError:
    _BACKEND_AVAILABLE = False

if TYPE_CHECKING:
    from PyQt5.QtWidgets import QWidget


# ── Worker ────────────────────────────────────────────────────────────────────

class IntegrityWorker(QThread):
    """
    Runs validate_backup() for a list of watches in a background thread.

    Parameters
    ----------
    jobs : list of dicts, each with keys:
        watch_name  str   — display name (for signals / logging)
        watch_id    str   — watch ID (used to update last_integrity_check)
        backup_dir  str   — path to the backup directory to validate
        sync_mode   bool  — True → validate dest folder directly (sync watches)
    cfg : dict — the current global config (needed for destination path)
    """

    # Emitted once per watch as soon as its check completes.
    # Args: watch_name (str), result dict from validate_backup()
    watch_done = pyqtSignal(str, dict)

    # Emitted when every watch in the job list has been checked.
    # Args: summary dict { "ok": int, "failed": int, "skipped": int }
    finished = pyqtSignal(dict)

    def __init__(self, jobs: list[dict], cfg: dict, parent=None):
        super().__init__(parent)
        self._jobs = jobs
        self._cfg  = cfg

    def run(self):
        summary = {"ok": 0, "failed": 0, "skipped": 0}

        for job in self._jobs:
            name       = job.get("watch_name", "?")
            backup_dir = job.get("backup_dir", "")

            if not backup_dir:
                logger.warning("IntegrityWorker: no backup_dir for watch '%s' — skipping", name)
                summary["skipped"] += 1
                continue

            logger.info("Integrity check: %s → %s", name, backup_dir)
            try:
                result = backup_engine.validate_backup(backup_dir)
            except Exception as exc:
                result = {
                    "valid":           False,
                    "manifest_ok":     False,
                    "missing_files":   [],
                    "corrupted_files": [],
                    "error":           str(exc),
                }
                logger.exception("validate_backup raised for '%s': %s", name, exc)

            ok = result.get("valid") and result.get("manifest_ok", True)
            if ok:
                summary["ok"] += 1
            else:
                summary["failed"] += 1
                logger.warning(
                    "Integrity FAILED for '%s': missing=%s corrupted=%s err=%s",
                    name,
                    result.get("missing_files", []),
                    result.get("corrupted_files", []),
                    result.get("error", ""),
                )

            # Stamp the watch with the check time regardless of pass/fail
            # so the scheduler knows it ran and doesn't immediately re-fire.
            try:
                cfg_live = config_manager.load()
                for w in cfg_live.get("watches", []):
                    if w["id"] == job["watch_id"]:
                        w["last_integrity_check"] = datetime.now().isoformat()
                        break
                config_manager.save(cfg_live)
            except Exception as exc:
                logger.warning("Could not stamp last_integrity_check: %s", exc)

            self.watch_done.emit(name, result)

        self.finished.emit(summary)


# ── Scheduler ─────────────────────────────────────────────────────────────────

class IntegrityScheduler:
    """
    Owns a QTimer that fires every 30 minutes to check whether any watch
    is overdue for an integrity check.  Builds the job list and launches
    IntegrityWorker when needed.

    Signals are re-exported from the worker so callers only need to connect
    to the scheduler, not to individual workers.

    Usage
    -----
    sched = IntegrityScheduler(parent_widget)
    sched.watch_result.connect(my_slot)
    sched.run_finished.connect(my_other_slot)
    sched.start()
    ...
    sched.stop()
    """

    # Forwarded from IntegrityWorker (see class docstring above)
    watch_result  = pyqtSignal(str, dict)
    run_finished  = pyqtSignal(dict)

    # How often the scheduler timer ticks (milliseconds).
    _TICK_MS = 30 * 60 * 1000  # 30 minutes

    def __init__(self, parent: "QWidget"):
        # Store parent — we instantiate the worker with it later so Qt's
        # object ownership is correct and the worker is cleaned up properly.
        self._parent = parent
        self._worker: IntegrityWorker | None = None
        self._timer  = QTimer(parent)
        self._timer.setInterval(self._TICK_MS)
        self._timer.timeout.connect(self._tick)

        # Attach our signals to the parent so external code can connect
        # to IntegrityScheduler directly.
        #
        # PyQt signals must live on a QObject, so we piggy-back on parent.
        # We use a bridge pattern: create a tiny private QObject to host them.
        from PyQt5.QtCore import QObject

        class _SignalBridge(QObject):
            watch_result = pyqtSignal(str, dict)
            run_finished = pyqtSignal(dict)

        self._bridge = _SignalBridge(parent)
        self.watch_result = self._bridge.watch_result
        self.run_finished = self._bridge.run_finished

    # ── Public API ────────────────────────────────────────────────────────────

    def start(self):
        """Start the scheduling timer.  Also fires an initial check after
        a 60-second delay so the app has time to fully initialise first."""
        self._timer.start()
        QTimer.singleShot(60_000, self._tick)
        logger.info("IntegrityScheduler started (tick every %d min)", self._TICK_MS // 60000)

    def stop(self):
        """Stop the timer.  Any in-progress worker runs to completion."""
        self._timer.stop()
        logger.info("IntegrityScheduler stopped")

    def run_now(self):
        """Trigger an immediate check for all watches, ignoring the schedule."""
        self._tick(force=True)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _tick(self, force: bool = False):
        """Called every 30 min (and once at startup).  Decides whether to run."""
        if not _BACKEND_AVAILABLE:
            return

        # Don't start a new run if one is already in progress.
        if self._worker and self._worker.isRunning():
            logger.debug("IntegrityScheduler: worker still running — skipping tick")
            return

        try:
            cfg = config_manager.load()
        except Exception as exc:
            logger.warning("IntegrityScheduler: could not load config: %s", exc)
            return

        if not force and not cfg.get("integrity_check_enabled", False):
            return

        interval_days = int(cfg.get("integrity_check_interval_days", 7))
        now = datetime.now()
        jobs: list[dict] = []

        for watch in cfg.get("watches", []):
            if not watch.get("active", True) or watch.get("paused", False):
                continue

            # Has this watch ever been backed up?
            if not watch.get("last_backup"):
                continue

            # Is it due?
            last_check_str = watch.get("last_integrity_check")
            if not force and last_check_str:
                try:
                    last_check = datetime.fromisoformat(last_check_str)
                    if now - last_check < timedelta(days=interval_days):
                        continue  # Not due yet
                except ValueError:
                    pass  # Malformed timestamp → treat as never checked

            # Find the most recent backup directory for this watch.
            try:
                dest    = _resolve_dest(watch, cfg)
                backups = backup_engine.list_backups(dest, watch["id"])
                if not backups:
                    logger.debug("IntegrityScheduler: no backups for '%s'", watch.get("name"))
                    continue
                backup_dir = backups[0].get("backup_dir", "")
            except Exception as exc:
                logger.warning(
                    "IntegrityScheduler: could not list backups for '%s': %s",
                    watch.get("name", watch["id"]), exc,
                )
                continue

            jobs.append({
                "watch_name": watch.get("name", watch["id"]),
                "watch_id":   watch["id"],
                "backup_dir": backup_dir,
                "sync_mode":  watch.get("sync_mode", False),
            })

        if not jobs:
            logger.debug("IntegrityScheduler: no watches due for integrity check")
            return

        logger.info(
            "IntegrityScheduler: starting integrity run for %d watch(es): %s",
            len(jobs),
            [j["watch_name"] for j in jobs],
        )

        self._worker = IntegrityWorker(jobs, cfg, parent=self._parent)
        self._worker.watch_done.connect(self._bridge.watch_result)
        self._worker.finished.connect(self._bridge.run_finished)
        self._worker.finished.connect(self._worker.deleteLater)
        self._worker.start()


def _resolve_dest(watch: dict, cfg: dict) -> str:
    """Return the effective destination path for a watch."""
    # Per-watch override takes priority
    if watch.get("dest_override"):
        return watch["dest_override"]
    return cfg.get("destination", "./backups")


# ── Admin Panel UI helper ──────────────────────────────────────────────────────

def build_integrity_settings_section(parent_widget, cfg: dict):
    """
    Build a ready-made QGroupBox for the Admin Panel / Settings UI.

    Returns (group_box, get_values_fn) where get_values_fn() → dict of
    the two config keys so _save_general() can write them back.

    Usage in AdminPanel._build_ui():

        from integrity_scheduler import build_integrity_settings_section
        group, get_integrity_vals = build_integrity_settings_section(self, self.cfg)
        some_layout.addWidget(group)
        # In _save_general():
        cfg.update(get_integrity_vals())
    """
    from PyQt5.QtWidgets import (
        QGroupBox, QVBoxLayout, QHBoxLayout,
        QCheckBox, QSpinBox, QLabel,
    )

    group  = QGroupBox("Integrity Checks")
    layout = QVBoxLayout(group)
    layout.setSpacing(8)

    enabled_cb = QCheckBox("Enable scheduled backup integrity checks")
    enabled_cb.setChecked(bool(cfg.get("integrity_check_enabled", False)))
    layout.addWidget(enabled_cb)

    row = QHBoxLayout()
    row.setSpacing(6)
    row.addWidget(QLabel("Check every"))

    interval_spin = QSpinBox()
    interval_spin.setRange(1, 365)
    interval_spin.setValue(int(cfg.get("integrity_check_interval_days", 7)))
    interval_spin.setSuffix(" day(s)")
    interval_spin.setFixedWidth(110)
    row.addWidget(interval_spin)
    row.addStretch()
    layout.addLayout(row)

    note = QLabel(
        "Each watch's most recent backup is re-hashed and verified against\n"
        "its stored SHA-256 and manifest.  Failures trigger email and webhook\n"
        "notifications using your existing notification settings."
    )
    note.setStyleSheet("color: #888; font-size: 11px;")
    layout.addWidget(note)

    def get_values() -> dict:
        return {
            "integrity_check_enabled":       enabled_cb.isChecked(),
            "integrity_check_interval_days": interval_spin.value(),
        }

    return group, get_values
