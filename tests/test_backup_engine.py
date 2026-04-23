"""
tests/test_backup_engine.py — Unit tests for backup_engine core logic.
Run:  pytest tests/
"""
import hashlib, json, os, sys, time
from datetime import datetime, timedelta
from pathlib import Path
import pytest
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import backup_engine as be

def _write(path, content=b"hello"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path

def _fake_snap(files):
    return {k: {"hash": hashlib.sha256(v).hexdigest(), "size": len(v)} for k, v in files.items()}

# ─── hash_file ────────────────────────────────────────────────────────────────
class TestHashFile:
    def test_known_hash(self, tmp_path):
        f = _write(tmp_path / "a.txt", b"hello world")
        assert be.hash_file(str(f)) == hashlib.sha256(b"hello world").hexdigest()

    def test_empty_file(self, tmp_path):
        f = _write(tmp_path / "empty.txt", b"")
        assert be.hash_file(str(f)) == hashlib.sha256(b"").hexdigest()

    def test_missing_file_returns_empty_string(self, tmp_path):
        # backup_engine returns "" for missing files (does not raise)
        assert be.hash_file(str(tmp_path / "ghost.txt")) == ""

    def test_cancel_event_returns_string(self, tmp_path):
        import threading
        ev = threading.Event(); ev.set()
        result = be.hash_file(str(_write(tmp_path / "x.txt", b"data")), cancel_event=ev)
        assert isinstance(result, str)

# ─── build_snapshot ───────────────────────────────────────────────────────────
class TestBuildSnapshot:
    def test_single_file(self, tmp_path):
        _write(tmp_path / "doc.txt", b"content")
        snap = be.build_snapshot(str(tmp_path))
        assert any("doc.txt" in k for k in snap)

    def test_excludes_pattern(self, tmp_path):
        _write(tmp_path / "keep.txt", b"k"); _write(tmp_path / "skip.tmp", b"s")
        snap = be.build_snapshot(str(tmp_path), exclude_patterns=["*.tmp"])
        assert not any("skip.tmp" in k for k in snap)
        assert any("keep.txt" in k for k in snap)

    def test_nested_files(self, tmp_path):
        _write(tmp_path / "sub" / "nested.txt", b"n")
        assert any("nested.txt" in k for k in be.build_snapshot(str(tmp_path)))

    def test_entry_has_hash_and_size(self, tmp_path):
        _write(tmp_path / "f.txt", b"abc")
        entry = next(v for k, v in be.build_snapshot(str(tmp_path)).items() if "f.txt" in k)
        assert "hash" in entry and "size" in entry

    def test_empty_dir(self, tmp_path):
        assert be.build_snapshot(str(tmp_path)) == {}

# ─── diff_snapshots ───────────────────────────────────────────────────────────
# diff uses "type" key (not "status") with values "added" / "deleted" / "modified"
class TestDiffSnapshots:
    def test_added(self):
        diff = be.diff_snapshots(_fake_snap({}), _fake_snap({"a.txt": b"new"}))
        added = [d for d in diff if d["type"] == "added"]
        assert len(added) == 1 and "a.txt" in added[0]["path"]

    def test_deleted(self):
        diff = be.diff_snapshots(_fake_snap({"gone.txt": b"bye"}), _fake_snap({}))
        assert any(d["type"] == "deleted" for d in diff)

    def test_modified(self):
        diff = be.diff_snapshots(_fake_snap({"f.txt": b"v1"}), _fake_snap({"f.txt": b"v2"}))
        assert any(d["type"] == "modified" for d in diff)

    def test_unchanged_produces_empty_diff(self):
        snap = _fake_snap({"same.txt": b"same"})
        assert be.diff_snapshots(snap, snap) == []

    def test_multiple_changes(self):
        old = _fake_snap({"keep.txt": b"k", "del.txt": b"d"})
        new = _fake_snap({"keep.txt": b"k", "add.txt": b"a"})
        types = {d["type"] for d in be.diff_snapshots(old, new)}
        assert "added" in types and "deleted" in types

    def test_entry_has_path_key(self):
        diff = be.diff_snapshots(_fake_snap({"x.txt": b"o"}), _fake_snap({}))
        assert diff and "path" in diff[0]

# ─── Encryption ───────────────────────────────────────────────────────────────
class TestEncryption:
    @pytest.fixture(autouse=True)
    def _skip(self):
        if not be.CRYPTO_AVAILABLE:
            pytest.skip("cryptography not installed")

    def test_roundtrip(self, tmp_path):
        key = be.generate_encryption_key()
        src = _write(tmp_path / "plain.txt", b"secret")
        enc = tmp_path / "plain.enc"; dec = tmp_path / "plain.dec"
        be._encrypt_file(str(src), str(enc), key)
        assert enc.read_bytes() != b"secret"
        be._decrypt_file(str(enc), str(dec), key)
        assert dec.read_bytes() == b"secret"

    def test_wrong_key_raises(self, tmp_path):
        k1, k2 = be.generate_encryption_key(), be.generate_encryption_key()
        src = _write(tmp_path / "f.txt", b"data")
        be._encrypt_file(str(src), str(tmp_path / "f.enc"), k1)
        with pytest.raises(Exception):
            be._decrypt_file(str(tmp_path / "f.enc"), str(tmp_path / "out.txt"), k2)

    def test_invalid_key_raises(self, tmp_path):
        with pytest.raises(Exception):
            be._encrypt_file(str(_write(tmp_path / "f.txt", b"d")), str(tmp_path / "out.enc"), "bad-key")

    def test_key_is_44_chars(self):
        assert len(be.generate_encryption_key()) == 44

    def test_fernet_size_error_mentions_settings(self, tmp_path):
        import unittest.mock as mock
        key = be.generate_encryption_key()
        # Write the file BEFORE patching so _write/mkdir don't use the fake stat
        src = _write(tmp_path / "big.bin", b"x")
        # MagicMock gives st_mode etc. for free so Path.is_dir() still works
        big_stat = mock.MagicMock()
        big_stat.st_size = be.FERNET_MAX_BYTES + 1
        with mock.patch.object(Path, "stat", return_value=big_stat):
            with pytest.raises((ValueError, RuntimeError)) as exc:
                be._encrypt_file(str(src), str(tmp_path / "out.enc"), key)
        assert "Settings" in str(exc.value) or "too large" in str(exc.value).lower()

# ─── cleanup_old_backups ──────────────────────────────────────────────────────
class TestCleanup:
    def _backup(self, dest, watch_id, days_ago):
        dt = datetime.now() - timedelta(days=days_ago)
        folder = dest / dt.strftime("backup_%Y%m%d_%H%M%S")
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "MANIFEST.json").write_text(json.dumps({
            "watch_id": watch_id,
            "timestamp": dt.isoformat(timespec="seconds"),
            "files": {}, "backup_id": folder.name,
        }))
        return folder

    def test_old_backup_deleted(self, tmp_path):
        wid = "w1"
        old = self._backup(tmp_path, wid, 40)
        new = self._backup(tmp_path, wid, 5)
        result = be.cleanup_old_backups(str(tmp_path), retention_days=30, watch_id=wid)
        assert not old.exists() and new.exists() and result["deleted"] >= 1

    def test_recent_backup_kept(self, tmp_path):
        wid = "w2"
        bk = self._backup(tmp_path, wid, 10)
        be.cleanup_old_backups(str(tmp_path), retention_days=30, watch_id=wid)
        assert bk.exists()

    def test_preview_returns_list(self, tmp_path):
        wid = "w3"
        self._backup(tmp_path, wid, 60)
        prev = be.preview_cleanup(str(tmp_path), retention_days=30, watch_id=wid)
        assert isinstance(prev["to_delete"], list) and len(prev["to_delete"]) >= 1

    def test_cleanup_result_has_required_keys(self, tmp_path):
        wid = "w4"
        self._backup(tmp_path, wid, 40)
        r = be.cleanup_old_backups(str(tmp_path), retention_days=30, watch_id=wid)
        assert "deleted" in r and "freed_bytes" in r and "freed_human" in r

# ─── _fix_path ────────────────────────────────────────────────────────────────
class TestFixPath:
    def test_windows_path(self):        assert be._fix_path("C:\\Users\\test").startswith("C:")
    def test_unix_drive(self):          assert be._fix_path("/C/Users/test").startswith("C:")
    def test_unc_path(self):
        r = be._fix_path("\\\\server\\share")
        assert r.startswith("\\\\") or r.startswith("//")
    def test_unix_path_unchanged(self): assert "home" in be._fix_path("/home/user")

# ─── BackupThrottler ──────────────────────────────────────────────────────────
class TestThrottler:
    def test_unlimited_no_sleep(self):
        t = be.BackupThrottler(max_mbps=0)
        start = time.time()
        for _ in range(1000): t.throttle(1024 * 1024)
        assert time.time() - start < 0.5

    def test_throttle_slows_transfer(self):
        t = be.BackupThrottler(max_mbps=10.0)
        start = time.time()
        for _ in range(5): t.throttle(1024 * 1024)
        assert time.time() - start >= 0.4

# ─── safe_path ────────────────────────────────────────────────────────────────
class TestSafePath:
    def test_allowed(self, tmp_path):
        assert be.safe_path(str(tmp_path / "sub" / "f.txt"), [str(tmp_path)]) is not None

    def test_disallowed(self, tmp_path):
        assert be.safe_path(str(tmp_path / "other" / "f.txt"), [str(tmp_path / "allowed")]) is None

    def test_traversal_blocked(self, tmp_path):
        target = str(tmp_path / "safe" / ".." / ".." / "etc" / "passwd")
        assert be.safe_path(target, [str(tmp_path / "safe")]) is None
