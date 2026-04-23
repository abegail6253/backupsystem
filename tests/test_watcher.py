"""
tests/test_watcher.py — Unit tests for watcher.py

Tests the internal event-buffering logic, exclude pattern matching,
and WatcherManager lifecycle.  No real filesystem watching is started;
watchdog events are injected directly into the handler.
"""

import sys
import time
import threading
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Guard: if watchdog is not installed the watcher module still imports but
# WATCHDOG_AVAILABLE will be False.  We test internal logic regardless.
import watcher


# ─── _pending buffer helpers ──────────────────────────────────────────────────

class TestPendingBuffer(unittest.TestCase):
    """Test the in-memory pending-change buffer directly."""

    def setUp(self):
        # Clear the module-level pending buffer before each test
        with watcher._lock:
            watcher._pending.clear()

    def _inject(self, handler, event_type: str, path: str, dest: str = None):
        """Simulate a filesystem event hitting the handler."""
        ev = MagicMock()
        ev.is_directory = False
        ev.src_path    = path
        ev.dest_path   = dest or ""
        if event_type == "modified":
            handler.on_modified(ev)
        elif event_type == "created":
            handler.on_created(ev)
        elif event_type == "deleted":
            handler.on_deleted(ev)
        elif event_type == "moved":
            handler.on_moved(ev)

    @unittest.skipUnless(watcher.WATCHDOG_AVAILABLE, "watchdog not installed")
    def test_event_recorded_in_pending(self):
        h = watcher._Handler("w_test1")
        self._inject(h, "created", "/watch/file.txt")
        with watcher._lock:
            entries = watcher._pending.get("w_test1", [])
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["type"], "added")
        self.assertEqual(entries[0]["path"], "/watch/file.txt")

    @unittest.skipUnless(watcher.WATCHDOG_AVAILABLE, "watchdog not installed")
    def test_duplicate_path_replaced(self):
        """A second event for the same path replaces the first."""
        h = watcher._Handler("w_test2")
        self._inject(h, "created",  "/watch/file.txt")
        self._inject(h, "modified", "/watch/file.txt")
        with watcher._lock:
            entries = watcher._pending.get("w_test2", [])
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["type"], "modified")

    @unittest.skipUnless(watcher.WATCHDOG_AVAILABLE, "watchdog not installed")
    def test_directory_events_ignored(self):
        h = watcher._Handler("w_test3")
        ev = MagicMock(); ev.is_directory = True; ev.src_path = "/watch/subdir"
        h.on_created(ev)
        with watcher._lock:
            entries = watcher._pending.get("w_test3", [])
        self.assertEqual(len(entries), 0)

    @unittest.skipUnless(watcher.WATCHDOG_AVAILABLE, "watchdog not installed")
    def test_buffer_cap_at_5000(self):
        """Buffer is pruned to 2500 entries once it exceeds 5000."""
        h = watcher._Handler("w_cap")
        for i in range(5005):
            ev = MagicMock(); ev.is_directory = False; ev.src_path = f"/watch/file{i}.txt"
            h.on_created(ev)
        with watcher._lock:
            entries = watcher._pending.get("w_cap", [])
        self.assertLessEqual(len(entries), 2500)

    @unittest.skipUnless(watcher.WATCHDOG_AVAILABLE, "watchdog not installed")
    def test_deleted_event_recorded(self):
        h = watcher._Handler("w_del")
        self._inject(h, "deleted", "/watch/gone.txt")
        with watcher._lock:
            entries = watcher._pending.get("w_del", [])
        self.assertEqual(entries[0]["type"], "deleted")

    @unittest.skipUnless(watcher.WATCHDOG_AVAILABLE, "watchdog not installed")
    def test_moved_event_recorded_with_dest(self):
        h = watcher._Handler("w_mv")
        ev = MagicMock()
        ev.is_directory = False
        ev.src_path     = "/watch/old.txt"
        ev.dest_path    = "/watch/new.txt"
        h.on_moved(ev)
        with watcher._lock:
            entries = watcher._pending.get("w_mv", [])
        self.assertEqual(len(entries), 1)
        self.assertIn(entries[0]["type"], ("moved", "renamed", "modified", "added"))


# ─── Exclude pattern tests ────────────────────────────────────────────────────

class TestExcludePatterns(unittest.TestCase):

    @unittest.skipUnless(watcher.WATCHDOG_AVAILABLE, "watchdog not installed")
    def setUp(self):
        with watcher._lock:
            watcher._pending.clear()

    @unittest.skipUnless(watcher.WATCHDOG_AVAILABLE, "watchdog not installed")
    def test_tmp_files_excluded(self):
        h = watcher._Handler("w_excl", exclude_patterns=["*.tmp"])
        ev = MagicMock(); ev.is_directory = False; ev.src_path = "/watch/work.tmp"
        h.on_created(ev)
        with watcher._lock:
            entries = watcher._pending.get("w_excl", [])
        self.assertEqual(len(entries), 0, "*.tmp should be excluded")

    @unittest.skipUnless(watcher.WATCHDOG_AVAILABLE, "watchdog not installed")
    def test_non_excluded_file_recorded(self):
        h = watcher._Handler("w_excl2", exclude_patterns=["*.tmp"])
        ev = MagicMock(); ev.is_directory = False; ev.src_path = "/watch/doc.docx"
        h.on_created(ev)
        with watcher._lock:
            entries = watcher._pending.get("w_excl2", [])
        self.assertEqual(len(entries), 1)

    @unittest.skipUnless(watcher.WATCHDOG_AVAILABLE, "watchdog not installed")
    def test_node_modules_directory_excluded(self):
        h = watcher._Handler("w_nm", exclude_patterns=["node_modules"])
        ev = MagicMock(); ev.is_directory = False
        ev.src_path = "/project/node_modules/lodash/index.js"
        h.on_created(ev)
        with watcher._lock:
            entries = watcher._pending.get("w_nm", [])
        self.assertEqual(len(entries), 0)

    @unittest.skipUnless(watcher.WATCHDOG_AVAILABLE, "watchdog not installed")
    def test_office_lock_files_excluded(self):
        h = watcher._Handler("w_office", exclude_patterns=["~$*"])
        ev = MagicMock(); ev.is_directory = False
        ev.src_path = "/docs/~$report.docx"
        h.on_created(ev)
        with watcher._lock:
            entries = watcher._pending.get("w_office", [])
        self.assertEqual(len(entries), 0)

    @unittest.skipUnless(watcher.WATCHDOG_AVAILABLE, "watchdog not installed")
    def test_git_dir_excluded(self):
        h = watcher._Handler("w_git", exclude_patterns=[".git"])
        ev = MagicMock(); ev.is_directory = False
        ev.src_path = "/project/.git/COMMIT_EDITMSG"
        h.on_created(ev)
        with watcher._lock:
            entries = watcher._pending.get("w_git", [])
        self.assertEqual(len(entries), 0)


# ─── on_change callback tests ─────────────────────────────────────────────────

class TestOnChangeCallback(unittest.TestCase):

    @unittest.skipUnless(watcher.WATCHDOG_AVAILABLE, "watchdog not installed")
    def setUp(self):
        with watcher._lock:
            watcher._pending.clear()

    @unittest.skipUnless(watcher.WATCHDOG_AVAILABLE, "watchdog not installed")
    def test_callback_invoked_on_event(self):
        received = []
        def _cb(watch_id, entry):
            received.append((watch_id, entry))

        h = watcher._Handler("w_cb", on_change=_cb)
        ev = MagicMock(); ev.is_directory = False; ev.src_path = "/watch/new.txt"
        h.on_created(ev)
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0][0], "w_cb")
        self.assertEqual(received[0][1]["type"], "added")

    @unittest.skipUnless(watcher.WATCHDOG_AVAILABLE, "watchdog not installed")
    def test_callback_exception_does_not_propagate(self):
        def _bad_cb(watch_id, entry):
            raise RuntimeError("callback error")

        h = watcher._Handler("w_cbfail", on_change=_bad_cb)
        ev = MagicMock(); ev.is_directory = False; ev.src_path = "/watch/f.txt"
        # Must not raise
        try:
            h.on_created(ev)
        except Exception as e:
            self.fail(f"Callback exception propagated: {e}")


# ─── WatcherManager tests ─────────────────────────────────────────────────────

class TestWatcherManager(unittest.TestCase):
    """Smoke tests for WatcherManager API (watchdog mocked)."""

    @unittest.skipUnless(watcher.WATCHDOG_AVAILABLE, "watchdog not installed")
    def setUp(self):
        with watcher._lock:
            watcher._pending.clear()

    @unittest.skipUnless(watcher.WATCHDOG_AVAILABLE, "watchdog not installed")
    def test_add_and_remove_watch(self):
        import tempfile, shutil
        tmp = Path(tempfile.mkdtemp())
        try:
            mgr = watcher.WatcherManager()
            with patch("watcher.Observer") as mock_obs_cls:
                mock_obs = MagicMock()
                mock_obs_cls.return_value = mock_obs
                mgr.add_watch("w_mgr1", str(tmp))
                self.assertIn("w_mgr1", mgr._watches)
                mgr.remove_watch("w_mgr1")
                self.assertNotIn("w_mgr1", mgr._watches)
        finally:
            shutil.rmtree(str(tmp), ignore_errors=True)

    @unittest.skipUnless(watcher.WATCHDOG_AVAILABLE, "watchdog not installed")
    def test_flush_returns_pending_and_clears(self):
        with watcher._lock:
            watcher._pending["w_flush"] = [{"type": "added", "path": "/f.txt", "timestamp": "", "size": 0}]
        mgr = watcher.WatcherManager()
        flushed = mgr.flush("w_flush")
        self.assertEqual(len(flushed), 1)
        with watcher._lock:
            remaining = watcher._pending.get("w_flush", [])
        self.assertEqual(len(remaining), 0)

    @unittest.skipUnless(watcher.WATCHDOG_AVAILABLE, "watchdog not installed")
    def test_flush_unknown_watch_returns_empty(self):
        mgr = watcher.WatcherManager()
        result = mgr.flush("w_nonexistent_xyz")
        self.assertEqual(result, [])

    @unittest.skipUnless(watcher.WATCHDOG_AVAILABLE, "watchdog not installed")
    def test_stop_all_does_not_raise(self):
        mgr = watcher.WatcherManager()
        with patch("watcher.Observer") as mock_obs_cls:
            mock_obs_cls.return_value = MagicMock()
            import tempfile, shutil
            tmp = Path(tempfile.mkdtemp())
            try:
                mgr.add_watch("w_stop", str(tmp))
                mgr.stop_all()
            finally:
                shutil.rmtree(str(tmp), ignore_errors=True)


# ─── watchdog not installed path ──────────────────────────────────────────────

class TestWatchdogUnavailable(unittest.TestCase):

    def test_watchdog_unavailable_flag(self):
        """Module should import cleanly even without watchdog."""
        # watcher already imported; WATCHDOG_AVAILABLE reflects reality
        self.assertIsInstance(watcher.WATCHDOG_AVAILABLE, bool)


if __name__ == "__main__":
    unittest.main()
