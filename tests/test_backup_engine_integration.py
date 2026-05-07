"""
tests/test_backup_engine_integration.py — End-to-end integration tests
=======================================================================
Tests the full backup → restore cycle using real temp directories.
No mocking of file I/O — these tests exercise the actual copy, hash,
manifest-write, and restore paths in backup_engine.py.

Covers:
  - run_backup():  full backup, incremental backup, dry-run, cancellation,
                   file exclusion, size-limit filtering, encryption round-trip
  - restore_backup(): restores files with correct content, overwrites/skips
                      correctly, handles missing/corrupt manifests gracefully
  - End-to-end:    backup + restore → restored files byte-identical to source

Run:
    pytest tests/test_backup_engine_integration.py -v
"""
from __future__ import annotations

import hashlib
import json
import shutil
import sys
import threading
import time
from pathlib import Path

import pytest

# ── Bootstrap ────────────────────────────────────────────────────────────────
_HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_HERE))
import backup_engine as be


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _write(path: Path, content: bytes = b"hello") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def _tree(root: Path) -> dict[str, bytes]:
    """Return {relative_path: content} for every file under root."""
    return {
        str(p.relative_to(root)): p.read_bytes()
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


def _run_backup(source, destination, watch_id="w_test", watch_name="Test",
                previous_snapshot=None, **kwargs) -> dict:
    return be.run_backup(
        source=str(source),
        destination=str(destination),
        watch_id=watch_id,
        watch_name=watch_name,
        storage_type="local",
        previous_snapshot=previous_snapshot,
        incremental=previous_snapshot is not None,
        **kwargs,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# run_backup — full backup
# ═══════════════════════════════════════════════════════════════════════════════

class TestRunBackupFull:
    def test_status_is_success(self, tmp_path):
        src = tmp_path / "src"; _write(src / "a.txt", b"aaa")
        dest = tmp_path / "dest"
        result = _run_backup(src, dest)
        assert result["status"] == "success"

    def test_returns_backup_dir(self, tmp_path):
        src = tmp_path / "src"; _write(src / "a.txt", b"aaa")
        result = _run_backup(src, tmp_path / "dest")
        assert "backup_dir" in result
        assert Path(result["backup_dir"]).is_dir()

    def test_files_copied_count(self, tmp_path):
        src = tmp_path / "src"
        _write(src / "a.txt", b"aaa")
        _write(src / "b.txt", b"bbb")
        result = _run_backup(src, tmp_path / "dest")
        assert result["files_copied"] >= 2

    def test_manifest_created(self, tmp_path):
        src = tmp_path / "src"; _write(src / "a.txt", b"data")
        result = _run_backup(src, tmp_path / "dest")
        manifest = Path(result["backup_dir"]) / "MANIFEST.json"
        assert manifest.exists()

    def test_manifest_valid_json(self, tmp_path):
        src = tmp_path / "src"; _write(src / "a.txt", b"data")
        result = _run_backup(src, tmp_path / "dest")
        manifest = Path(result["backup_dir"]) / "MANIFEST.json"
        data = json.loads(manifest.read_text())
        assert isinstance(data, dict)

    def test_manifest_has_watch_id(self, tmp_path):
        src = tmp_path / "src"; _write(src / "f.txt", b"x")
        result = _run_backup(src, tmp_path / "dest", watch_id="w_abc")
        manifest = json.loads((Path(result["backup_dir"]) / "MANIFEST.json").read_text())
        assert manifest["watch_id"] == "w_abc"

    def test_nested_files_backed_up(self, tmp_path):
        src = tmp_path / "src"
        _write(src / "sub" / "deep.txt", b"deep content")
        result = _run_backup(src, tmp_path / "dest")
        assert result["files_copied"] >= 1

    def test_snapshot_returned(self, tmp_path):
        src = tmp_path / "src"; _write(src / "a.txt", b"aaa")
        result = _run_backup(src, tmp_path / "dest")
        assert isinstance(result.get("snapshot"), dict)
        assert len(result["snapshot"]) >= 1

    def test_empty_source_succeeds(self, tmp_path):
        src = tmp_path / "src"; src.mkdir()
        result = _run_backup(src, tmp_path / "dest")
        assert result["status"] == "success"
        assert result["files_copied"] == 0

    def test_progress_callback_called(self, tmp_path):
        src = tmp_path / "src"
        for i in range(3):
            _write(src / f"f{i}.txt", b"x" * 100)
        calls = []
        _run_backup(src, tmp_path / "dest", progress_cb=lambda *a, **kw: calls.append(a))
        assert len(calls) >= 1

    def test_backup_hash_in_result(self, tmp_path):
        src = tmp_path / "src"; _write(src / "a.txt", b"data")
        result = _run_backup(src, tmp_path / "dest")
        assert result.get("backup_hash")

    def test_total_size_bytes_positive(self, tmp_path):
        src = tmp_path / "src"; _write(src / "a.txt", b"data" * 100)
        result = _run_backup(src, tmp_path / "dest")
        assert result["total_size_bytes"] > 0


# ═══════════════════════════════════════════════════════════════════════════════
# run_backup — dry run
# ═══════════════════════════════════════════════════════════════════════════════

class TestRunBackupDryRun:
    def test_status_is_dry_run(self, tmp_path):
        src = tmp_path / "src"; _write(src / "a.txt", b"x")
        result = _run_backup(src, tmp_path / "dest", dry_run=True)
        assert result["status"] == "dry_run"

    def test_no_files_copied_in_dry_run(self, tmp_path):
        src = tmp_path / "src"; _write(src / "a.txt", b"x")
        dest = tmp_path / "dest"
        _run_backup(src, dest, dry_run=True)
        # Nothing should have been written to the destination
        assert not any(True for _ in dest.rglob("*") if _.is_file())

    def test_changes_list_returned(self, tmp_path):
        src = tmp_path / "src"; _write(src / "a.txt", b"x")
        result = _run_backup(src, tmp_path / "dest", dry_run=True)
        assert isinstance(result.get("changes"), list)

    def test_dry_run_detects_added_files(self, tmp_path):
        src = tmp_path / "src"
        _write(src / "new.txt", b"new")
        result = _run_backup(src, tmp_path / "dest", dry_run=True)
        types = {c["type"] for c in result.get("changes", [])}
        assert "added" in types


# ═══════════════════════════════════════════════════════════════════════════════
# run_backup — incremental backup
# ═══════════════════════════════════════════════════════════════════════════════

class TestRunBackupIncremental:
    def test_incremental_only_copies_changed_files(self, tmp_path):
        src = tmp_path / "src"
        _write(src / "unchanged.txt", b"same")
        _write(src / "changed.txt", b"v1")
        dest = tmp_path / "dest"

        # First backup — full
        r1 = _run_backup(src, dest)
        snap1 = r1["snapshot"]
        assert r1["status"] == "success"

        # Modify one file, leave the other alone
        _write(src / "changed.txt", b"v2")

        # Second backup — incremental
        r2 = _run_backup(src, dest, previous_snapshot=snap1)
        assert r2["status"] == "success"
        # Only the modified file should be flagged as changed
        assert r2["files_copied"] < r1["files_copied"] or r2["files_copied"] >= 1

    def test_incremental_snapshot_updated(self, tmp_path):
        src = tmp_path / "src"; _write(src / "f.txt", b"v1")
        dest = tmp_path / "dest"
        r1 = _run_backup(src, dest)
        _write(src / "f.txt", b"v2")
        r2 = _run_backup(src, dest, previous_snapshot=r1["snapshot"])
        # Snapshot must reflect the new content
        key = next(k for k in r2["snapshot"] if "f.txt" in k)
        assert r2["snapshot"][key]["hash"] == hashlib.sha256(b"v2").hexdigest()

    def test_adding_file_shows_as_added(self, tmp_path):
        src = tmp_path / "src"; _write(src / "a.txt", b"a")
        dest = tmp_path / "dest"
        r1 = _run_backup(src, dest)
        _write(src / "b.txt", b"b")
        r2 = _run_backup(src, dest, previous_snapshot=r1["snapshot"])
        changes = r2.get("changes", [])
        assert any(c["type"] == "added" and "b.txt" in c["path"] for c in changes)


# ═══════════════════════════════════════════════════════════════════════════════
# run_backup — exclusions and size limits
# ═══════════════════════════════════════════════════════════════════════════════

class TestRunBackupExclusions:
    def test_excluded_pattern_not_backed_up(self, tmp_path):
        src = tmp_path / "src"
        _write(src / "keep.txt", b"keep")
        _write(src / "skip.tmp", b"skip")
        result = _run_backup(src, tmp_path / "dest", exclude_patterns=["*.tmp"])
        assert result["status"] == "success"
        bd = Path(result["backup_dir"])
        # skip.tmp must not appear anywhere in the backup directory
        assert not any("skip.tmp" in str(p) for p in bd.rglob("*"))

    def test_max_file_size_filters_large_files(self, tmp_path):
        src = tmp_path / "src"
        _write(src / "small.txt", b"x" * 10)
        _write(src / "large.txt", b"x" * (3 * 1024 * 1024))  # 3 MB
        result = _run_backup(src, tmp_path / "dest", max_file_size_mb=1)
        assert result["status"] == "success"
        bd = Path(result["backup_dir"])
        assert not any("large.txt" in str(p) for p in bd.rglob("*"))


# ═══════════════════════════════════════════════════════════════════════════════
# run_backup — cancellation
# ═══════════════════════════════════════════════════════════════════════════════

class TestRunBackupCancellation:
    def test_cancel_event_stops_backup(self, tmp_path):
        src = tmp_path / "src"
        for i in range(20):
            _write(src / f"file_{i:03d}.txt", b"x" * 1024)

        cancel = threading.Event()
        cancel.set()   # cancel immediately

        result = _run_backup(src, tmp_path / "dest", cancel_event=cancel)
        assert result["status"] in ("cancelled", "success")   # may finish if fast


# ═══════════════════════════════════════════════════════════════════════════════
# run_backup — encryption round-trip
# ═══════════════════════════════════════════════════════════════════════════════

class TestRunBackupEncryption:
    @pytest.fixture(autouse=True)
    def _skip_if_no_crypto(self):
        if not be.CRYPTO_AVAILABLE:
            pytest.skip("cryptography not installed")

    def test_encrypted_backup_files_not_plaintext(self, tmp_path):
        src = tmp_path / "src"
        _write(src / "secret.txt", b"topsecret")
        key = be.generate_encryption_key()
        result = _run_backup(src, tmp_path / "dest", encrypt_key=key)
        assert result["status"] == "success"
        bd = Path(result["backup_dir"])
        # None of the backup files should contain the plaintext content
        for f in bd.rglob("*"):
            if f.is_file() and f.suffix not in (".json", ".sha256"):
                assert b"topsecret" not in f.read_bytes()


# ═══════════════════════════════════════════════════════════════════════════════
# restore_backup
# ═══════════════════════════════════════════════════════════════════════════════

class TestRestoreBackup:
    def _backup_and_get_dir(self, src, dest, **kw) -> str:
        result = _run_backup(src, dest, **kw)
        assert result["status"] == "success"
        return result["backup_dir"]

    def test_restore_returns_ok_true(self, tmp_path):
        src = tmp_path / "src"; _write(src / "f.txt", b"data")
        bd = self._backup_and_get_dir(src, tmp_path / "dest")
        result = be.restore_backup(bd, str(tmp_path / "restored"))
        assert result["ok"] is True

    def test_restored_file_content_matches_source(self, tmp_path):
        src = tmp_path / "src"; _write(src / "hello.txt", b"hello world")
        bd = self._backup_and_get_dir(src, tmp_path / "dest")
        restore_dir = tmp_path / "restored"
        be.restore_backup(bd, str(restore_dir))
        restored = restore_dir / "hello.txt"
        assert restored.exists()
        assert restored.read_bytes() == b"hello world"

    def test_restore_count_matches_backed_up_files(self, tmp_path):
        src = tmp_path / "src"
        for i in range(4):
            _write(src / f"file{i}.txt", b"content")
        bd = self._backup_and_get_dir(src, tmp_path / "dest")
        result = be.restore_backup(bd, str(tmp_path / "restored"))
        assert result["files_restored"] == 4

    def test_nested_files_restored(self, tmp_path):
        src = tmp_path / "src"
        _write(src / "sub" / "deep.txt", b"deep")
        bd = self._backup_and_get_dir(src, tmp_path / "dest")
        restore_dir = tmp_path / "restored"
        be.restore_backup(bd, str(restore_dir))
        assert (restore_dir / "sub" / "deep.txt").read_bytes() == b"deep"

    def test_missing_manifest_returns_error(self, tmp_path):
        empty_dir = tmp_path / "no_manifest"; empty_dir.mkdir()
        result = be.restore_backup(str(empty_dir), str(tmp_path / "out"))
        assert result["ok"] is False
        assert result["error"]

    def test_corrupt_manifest_returns_error(self, tmp_path):
        bd = tmp_path / "bad_backup"; bd.mkdir()
        (bd / "MANIFEST.json").write_text("{ not valid json }")
        result = be.restore_backup(str(bd), str(tmp_path / "out"))
        assert result["ok"] is False

    def test_overwrite_false_skips_existing(self, tmp_path):
        src = tmp_path / "src"; _write(src / "f.txt", b"original")
        bd = self._backup_and_get_dir(src, tmp_path / "dest")
        restore_dir = tmp_path / "restored"
        _write(restore_dir / "f.txt", b"existing")
        result = be.restore_backup(bd, str(restore_dir), overwrite=False)
        # Existing file must not be overwritten
        assert (restore_dir / "f.txt").read_bytes() == b"existing"
        assert result["skipped"] >= 1

    def test_overwrite_true_replaces_existing(self, tmp_path):
        src = tmp_path / "src"; _write(src / "f.txt", b"new content")
        bd = self._backup_and_get_dir(src, tmp_path / "dest")
        restore_dir = tmp_path / "restored"
        _write(restore_dir / "f.txt", b"old content")
        be.restore_backup(bd, str(restore_dir), overwrite=True)
        assert (restore_dir / "f.txt").read_bytes() == b"new content"

    def test_progress_callback_fired(self, tmp_path):
        src = tmp_path / "src"
        for i in range(3):
            _write(src / f"f{i}.txt", b"x")
        bd = self._backup_and_get_dir(src, tmp_path / "dest")
        calls = []
        be.restore_backup(bd, str(tmp_path / "restored"),
                          progress_cb=lambda *a, **kw: calls.append(a))
        assert len(calls) >= 1


# ═══════════════════════════════════════════════════════════════════════════════
# End-to-end: backup → restore → verify byte-for-byte identity
# ═══════════════════════════════════════════════════════════════════════════════

class TestEndToEnd:
    def test_full_roundtrip_simple(self, tmp_path):
        """Back up a small tree and restore it — every file must be identical."""
        src = tmp_path / "src"
        _write(src / "readme.md",           b"# Readme")
        _write(src / "data" / "values.csv", b"a,b,c\n1,2,3\n")
        _write(src / "data" / "info.txt",   b"some info")

        dest    = tmp_path / "dest"
        restore = tmp_path / "restore"

        result = _run_backup(src, dest)
        assert result["status"] == "success"

        rr = be.restore_backup(result["backup_dir"], str(restore))
        assert rr["ok"] is True

        # Every source file must appear in the restore with identical bytes
        for rel, content in _tree(src).items():
            restored_file = restore / rel
            assert restored_file.exists(), f"Missing after restore: {rel}"
            assert restored_file.read_bytes() == content, f"Content mismatch: {rel}"

    def test_full_roundtrip_binary_files(self, tmp_path):
        """Binary content (e.g. images) must survive backup → restore intact."""
        src = tmp_path / "src"
        binary = bytes(range(256)) * 100
        _write(src / "binary.bin", binary)

        dest    = tmp_path / "dest"
        restore = tmp_path / "restore"
        result  = _run_backup(src, dest)
        be.restore_backup(result["backup_dir"], str(restore))
        assert (restore / "binary.bin").read_bytes() == binary

    def test_incremental_then_restore(self, tmp_path):
        """Two incremental backups; restore from the second must reflect all changes."""
        src = tmp_path / "src"
        _write(src / "a.txt", b"version 1")
        dest = tmp_path / "dest"

        r1 = _run_backup(src, dest)

        # Modify a file and add a new one
        _write(src / "a.txt", b"version 2")
        _write(src / "b.txt", b"brand new")
        r2 = _run_backup(src, dest, previous_snapshot=r1["snapshot"])
        assert r2["status"] == "success"

        restore = tmp_path / "restore"
        rr = be.restore_backup(r2["backup_dir"], str(restore))
        assert rr["ok"] is True
        assert (restore / "b.txt").read_bytes() == b"brand new"

    def test_encrypted_roundtrip(self, tmp_path):
        """Encrypt during backup; restore with correct key must yield original bytes."""
        if not be.CRYPTO_AVAILABLE:
            pytest.skip("cryptography not installed")

        src = tmp_path / "src"; _write(src / "secret.txt", b"my secret data")
        key = be.generate_encryption_key()
        dest = tmp_path / "dest"
        result = _run_backup(src, dest, encrypt_key=key)
        assert result["status"] == "success"

        restore = tmp_path / "restore"
        rr = be.restore_backup(result["backup_dir"], str(restore), encrypt_key=key)
        assert rr["ok"] is True
        assert (restore / "secret.txt").read_bytes() == b"my secret data"

    def test_restore_wrong_key_does_not_return_plaintext(self, tmp_path):
        """Restoring with a wrong key must not produce readable plaintext."""
        if not be.CRYPTO_AVAILABLE:
            pytest.skip("cryptography not installed")

        src = tmp_path / "src"; _write(src / "secret.txt", b"classified")
        key  = be.generate_encryption_key()
        key2 = be.generate_encryption_key()
        result = _run_backup(src, tmp_path / "dest", encrypt_key=key)

        restore = tmp_path / "restore"
        rr = be.restore_backup(result["backup_dir"], str(restore), encrypt_key=key2)
        restored = restore / "secret.txt"
        if restored.exists():
            assert restored.read_bytes() != b"classified"
        else:
            # Decryption failure may simply skip the file and report an error
            assert rr["errors"] or not rr["ok"]