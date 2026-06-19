"""
tests/test_backupsys_cli.py — Unit tests for backupsys_cli helpers and sub-commands.
Run:  pytest tests/test_backupsys_cli.py -v
"""
import json
import sys
from argparse import Namespace
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Stub heavy dependencies before importing the CLI module so we don't need a
# real config file or backup store on disk during tests.
# Save originals so other test modules are not affected by our stubs.
_orig_cfg_module = sys.modules.get("config_manager")
_orig_be_module  = sys.modules.get("backup_engine")

_cfg_mock = MagicMock()
_be_mock  = MagicMock()
sys.modules["config_manager"] = _cfg_mock
sys.modules["backup_engine"]  = _be_mock

import backupsys_cli as cli  # noqa: E402 — must come after stubs are in place

# Restore the real modules (if they existed) so other test files see them.
if _orig_cfg_module is not None:
    sys.modules["config_manager"] = _orig_cfg_module
else:
    sys.modules.pop("config_manager", None)
if _orig_be_module is not None:
    sys.modules["backup_engine"] = _orig_be_module
else:
    sys.modules.pop("backup_engine", None)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _inject_mocks():
    """Re-inject the mocks for each test so local imports inside commands work."""
    _be_mock.reset_mock()
    _cfg_mock.reset_mock()
    sys.modules["backup_engine"]  = _be_mock
    sys.modules["config_manager"] = _cfg_mock
    yield
    # Restore after each test to keep cross-module isolation
    sys.modules.pop("backup_engine",  None)
    sys.modules.pop("config_manager", None)

@pytest.fixture
def watch_a():
    return {
        "id": "w_aaa",
        "name": "My Documents",
        "path": "/home/user/docs",
        "active": True,
        "paused": False,
        "last_backup": "2026-05-01T10:00:00",
    }

@pytest.fixture
def watch_b():
    return {
        "id": "w_bbb",
        "name": "Photos",
        "path": "/home/user/photos",
        "active": True,
        "paused": False,
        "last_backup": None,
    }

@pytest.fixture
def simple_cfg(watch_a, watch_b):
    return {
        "destination": "/backups",
        "dest_type": "local",
        "watches": [watch_a, watch_b],
    }

@pytest.fixture(autouse=True)
def _reset_mocks():
    """Reset shared module mocks between tests."""
    _cfg_mock.reset_mock()
    _be_mock.reset_mock()
    yield


# ── _human ────────────────────────────────────────────────────────────────────

class TestHuman:
    def test_bytes(self):
        assert cli._human(512) == "512.0 B"

    def test_kilobytes(self):
        assert cli._human(2048) == "2.0 KB"

    def test_megabytes(self):
        assert cli._human(5 * 1024 ** 2) == "5.0 MB"

    def test_gigabytes(self):
        assert cli._human(3 * 1024 ** 3) == "3.0 GB"

    def test_zero(self):
        assert cli._human(0) == "0.0 B"


# ── _resolve_watch ────────────────────────────────────────────────────────────

class TestResolveWatch:
    def test_match_by_id(self, simple_cfg, watch_a):
        assert cli._resolve_watch(simple_cfg, "w_aaa") == watch_a

    def test_match_by_name(self, simple_cfg, watch_a):
        assert cli._resolve_watch(simple_cfg, "My Documents") == watch_a

    def test_match_case_insensitive_name(self, simple_cfg, watch_a):
        assert cli._resolve_watch(simple_cfg, "my documents") == watch_a

    def test_match_case_insensitive_id(self, simple_cfg, watch_a):
        assert cli._resolve_watch(simple_cfg, "W_AAA") == watch_a

    def test_no_match_returns_none(self, simple_cfg):
        assert cli._resolve_watch(simple_cfg, "nonexistent") is None

    def test_empty_watches(self):
        assert cli._resolve_watch({"watches": []}, "anything") is None

    def test_second_watch_resolved(self, simple_cfg, watch_b):
        assert cli._resolve_watch(simple_cfg, "Photos") == watch_b


# ── _latest_backup_dir ────────────────────────────────────────────────────────

class TestLatestBackupDir:
    def test_returns_most_recent_named_dir(self, tmp_path):
        (tmp_path / "20260501_120000__My_Documents").mkdir()
        (tmp_path / "20260502_120000__My_Documents").mkdir()
        result = cli._latest_backup_dir(str(tmp_path), "My Documents")
        assert "20260502" in result

    def test_fallback_to_date_pattern(self, tmp_path):
        (tmp_path / "20260503_090000_extra").mkdir()
        result = cli._latest_backup_dir(str(tmp_path), "NonExistent")
        assert result is not None and "20260503" in result

    def test_returns_none_when_no_candidates(self, tmp_path):
        assert cli._latest_backup_dir(str(tmp_path), "Watch") is None

    def test_picks_latest_when_multiple_exist(self, tmp_path):
        for d in ["20260401_000000__Docs", "20260501_000000__Docs", "20260301_000000__Docs"]:
            (tmp_path / d).mkdir()
        result = cli._latest_backup_dir(str(tmp_path), "Docs")
        assert "20260501" in result


# ── cmd_list ──────────────────────────────────────────────────────────────────

class TestCmdList:
    def test_no_watches_returns_zero_and_warns(self, capsys):
        rc = cli.cmd_list(Namespace(), {"watches": []})
        assert rc == 0
        assert "No watches" in capsys.readouterr().out

    def test_shows_all_watch_names(self, capsys, simple_cfg):
        cli.cmd_list(Namespace(), simple_cfg)
        out = capsys.readouterr().out
        assert "My Documents" in out
        assert "Photos" in out

    def test_shows_watch_id(self, capsys, simple_cfg, watch_a):
        cli.cmd_list(Namespace(), simple_cfg)
        assert watch_a["id"] in capsys.readouterr().out

    def test_returns_zero(self, simple_cfg):
        assert cli.cmd_list(Namespace(), simple_cfg) == 0

    def test_paused_watch_shows_paused_status(self, capsys):
        cfg = {"watches": [{"id": "w_p", "name": "Paused", "path": "/p",
                             "paused": True, "active": True, "last_backup": None}]}
        cli.cmd_list(Namespace(), cfg)
        assert "paused" in capsys.readouterr().out


# ── cmd_config ────────────────────────────────────────────────────────────────

class TestCmdConfig:
    def test_returns_zero(self, capsys, simple_cfg):
        assert cli.cmd_config(Namespace(), simple_cfg) == 0

    def test_prints_valid_json(self, capsys, simple_cfg):
        cli.cmd_config(Namespace(), simple_cfg)
        out = capsys.readouterr().out
        # cmd_config prints a decorative header before the JSON blob; grab from '{'
        json_start = out.index("{")
        parsed = json.loads(out[json_start:])
        assert "watches" in parsed

    def test_redacts_sftp_password(self, capsys):
        cfg = {"dest_sftp": {"host": "h", "password": "topsecret"}, "watches": []}
        cli.cmd_config(Namespace(), cfg)
        out = capsys.readouterr().out
        assert "topsecret" not in out
        assert "***" in out

    def test_redacts_email_password(self, capsys):
        cfg = {"email_config": {"host": "smtp", "password": "mypass"}, "watches": []}
        cli.cmd_config(Namespace(), cfg)
        assert "mypass" not in capsys.readouterr().out

    def test_redacts_watch_encrypt_key(self, capsys):
        cfg = {"watches": [{"id": "w1", "name": "X", "path": "/x",
                             "encrypt_key": "supersecretkey"}]}
        cli.cmd_config(Namespace(), cfg)
        assert "supersecretkey" not in capsys.readouterr().out

    def test_non_secret_host_preserved(self, capsys):
        cfg = {"dest_sftp": {"host": "myhost.example.com", "password": "x"}, "watches": []}
        cli.cmd_config(Namespace(), cfg)
        assert "myhost.example.com" in capsys.readouterr().out

    def test_redacts_token_fields(self, capsys):
        cfg = {"dest_webdav": {"url": "https://dav.example.com", "token": "tok123"}, "watches": []}
        cli.cmd_config(Namespace(), cfg)
        assert "tok123" not in capsys.readouterr().out


# ── cmd_keygen ────────────────────────────────────────────────────────────────

class TestCmdKeygen:
    def test_prints_generated_key(self, capsys):
        _be_mock.generate_encryption_key.return_value = "FAKEKEY=="
        cli.cmd_keygen(Namespace(), {})
        assert "FAKEKEY==" in capsys.readouterr().out

    def test_returns_zero_on_success(self):
        _be_mock.generate_encryption_key.return_value = "KEY"
        assert cli.cmd_keygen(Namespace(), {}) == 0

    def test_runtime_error_returns_one(self, capsys):
        _be_mock.generate_encryption_key.side_effect = RuntimeError("no crypto lib")
        rc = cli.cmd_keygen(Namespace(), {})
        assert rc == 1
        assert "no crypto lib" in capsys.readouterr().err

    def test_calls_generate_encryption_key_once(self):
        _be_mock.generate_encryption_key.return_value = "KEY"
        cli.cmd_keygen(Namespace(), {})
        _be_mock.generate_encryption_key.assert_called_once()


# ── cmd_history ───────────────────────────────────────────────────────────────

class TestCmdHistory:
    def test_empty_history_returns_zero_and_warns(self, capsys):
        _cfg_mock.load_history.return_value = []
        rc = cli.cmd_history(Namespace(limit=20), {})
        assert rc == 0
        assert "No history" in capsys.readouterr().out

    def test_shows_watch_name_and_status(self, capsys):
        _cfg_mock.load_history.return_value = [
            {"timestamp": "2026-05-01T10:00:00", "watch_name": "Docs",
             "status": "success", "files_copied": 5, "total_size": "1 MB"},
        ]
        cli.cmd_history(Namespace(limit=20), {})
        out = capsys.readouterr().out
        assert "Docs" in out
        assert "success" in out

    def test_limit_respected(self, capsys):
        entries = [
            {"timestamp": f"2026-05-0{i}T10:00:00", "watch_name": "W",
             "status": "success", "files_copied": i, "total_size": "0"}
            for i in range(1, 6)
        ]
        _cfg_mock.load_history.return_value = entries
        cli.cmd_history(Namespace(limit=2), {})
        out = capsys.readouterr().out
        # Footer should say "Showing 2 of 5 entries"
        assert "2" in out and "5" in out

    def test_failed_status_included(self, capsys):
        _cfg_mock.load_history.return_value = [
            {"timestamp": "2026-05-01T10:00:00", "watch_name": "W",
             "status": "failed", "files_copied": 0, "total_size": "0"},
        ]
        cli.cmd_history(Namespace(limit=20), {})
        assert "failed" in capsys.readouterr().out


# ── cmd_validate ──────────────────────────────────────────────────────────────

class TestCmdValidate:
    def _cfg(self, watches):
        return {"destination": "/b", "dest_type": "local", "watches": watches}

    def test_no_selector_returns_one(self, capsys):
        rc = cli.cmd_validate(Namespace(all=False, watch=None), self._cfg([]))
        assert rc == 1

    def test_unknown_watch_returns_one(self, capsys, watch_a):
        rc = cli.cmd_validate(Namespace(all=False, watch="Ghost"), self._cfg([watch_a]))
        assert rc == 1

    def test_valid_backup_returns_zero(self, capsys, watch_a):
        _be_mock.validate_backup.return_value = {"valid": True, "manifest_ok": True,
                                                  "files_checked": 10}
        args = Namespace(all=False, watch="My Documents")
        with patch.object(cli, "_latest_backup_dir", return_value="/b/snapshot"):
            rc = cli.cmd_validate(args, self._cfg([watch_a]))
        assert rc == 0
        assert "OK" in capsys.readouterr().out

    def test_invalid_backup_returns_one(self, capsys, watch_a):
        _be_mock.validate_backup.return_value = {
            "valid": False, "manifest_ok": False,
            "missing_files": ["lost.txt"], "corrupted_files": [],
        }
        args = Namespace(all=False, watch="My Documents")
        with patch.object(cli, "_latest_backup_dir", return_value="/b/snapshot"):
            rc = cli.cmd_validate(args, self._cfg([watch_a]))
        assert rc == 1

    def test_missing_file_name_printed(self, capsys, watch_a):
        _be_mock.validate_backup.return_value = {
            "valid": False, "manifest_ok": False,
            "missing_files": ["reports/q1.xlsx"], "corrupted_files": [],
        }
        args = Namespace(all=False, watch="My Documents")
        with patch.object(cli, "_latest_backup_dir", return_value="/b/snapshot"):
            cli.cmd_validate(args, self._cfg([watch_a]))
        assert "reports/q1.xlsx" in capsys.readouterr().err

    def test_all_flag_validates_each_active_watch(self, watch_a, watch_b):
        _be_mock.validate_backup.return_value = {"valid": True, "manifest_ok": True,
                                                  "files_checked": 1}
        args = Namespace(all=True, watch=None)
        with patch.object(cli, "_latest_backup_dir", return_value="/b/snapshot"):
            cli.cmd_validate(args, self._cfg([watch_a, watch_b]))
        assert _be_mock.validate_backup.call_count == 2

    def test_no_backup_dir_found_counts_as_failure(self, capsys, watch_a):
        args = Namespace(all=False, watch="My Documents")
        with patch.object(cli, "_latest_backup_dir", return_value=None):
            rc = cli.cmd_validate(args, self._cfg([watch_a]))
        assert rc == 1


# ── cmd_restore ───────────────────────────────────────────────────────────────

class TestCmdRestore:
    """Tests for cmd_restore — local, remote (HTTPS), and edge-case paths."""

    def _args(self, **kw):
        defaults = dict(watch="My Documents", target="/tmp/out",
                        backup_id=None, full_chain=False, no_overwrite=False)
        defaults.update(kw)
        return Namespace(**defaults)

    def _cfg(self, watch, dest_type="local", extra=None):
        cfg = {"watches": [watch], "destination": "/b", "dest_type": dest_type}
        if extra:
            cfg.update(extra)
        return cfg

    # ── guard clauses ─────────────────────────────────────────────────────────

    def test_watch_not_found_returns_one(self, capsys):
        rc = cli.cmd_restore(self._args(watch="Ghost"),
                             {"watches": [], "destination": "/b", "dest_type": "local"})
        assert rc == 1

    def test_missing_target_returns_one(self, capsys, watch_a):
        rc = cli.cmd_restore(self._args(target=None), self._cfg(watch_a))
        assert rc == 1
        assert "--target" in capsys.readouterr().err

    def test_no_backups_found_returns_one(self, capsys, watch_a):
        _be_mock.list_backups.return_value = []
        rc = cli.cmd_restore(self._args(), self._cfg(watch_a))
        assert rc == 1

    # ── local restore ─────────────────────────────────────────────────────────

    def test_successful_local_restore_returns_zero(self, capsys, watch_a):
        _be_mock.list_backups.return_value = [
            {"id": "bk_1", "timestamp": "2026-05-01T10:00:00", "size_human": "5 MB",
             "status": "success", "dir": "/b/snap"}
        ]
        _be_mock.restore_backup.return_value = {"ok": True, "files_restored": 7, "skipped": 0}
        rc = cli.cmd_restore(self._args(), self._cfg(watch_a))
        assert rc == 0
        assert "7" in capsys.readouterr().out

    def test_restore_engine_failure_returns_one(self, capsys, watch_a):
        _be_mock.list_backups.return_value = [
            {"id": "bk_1", "timestamp": "2026-05-01T10:00:00", "size_human": "1 MB",
             "status": "success", "dir": "/b/snap"}
        ]
        _be_mock.restore_backup.return_value = {"ok": False, "error": "disk full"}
        rc = cli.cmd_restore(self._args(), self._cfg(watch_a))
        assert rc == 1

    def test_specific_backup_id_is_used(self, watch_a):
        backups = [
            {"id": "bk_old", "timestamp": "2026-04-01T00:00:00", "size_human": "1 MB",
             "status": "success", "dir": "/b/old"},
            {"id": "bk_new", "timestamp": "2026-05-01T00:00:00", "size_human": "2 MB",
             "status": "success", "dir": "/b/new"},
        ]
        _be_mock.list_backups.return_value = backups
        _be_mock.restore_backup.return_value = {"ok": True, "files_restored": 3, "skipped": 0}
        cli.cmd_restore(self._args(backup_id="bk_old"), self._cfg(watch_a))
        assert _be_mock.restore_backup.call_args[1]["backup_dir"] == "/b/old"

    def test_unknown_backup_id_returns_one(self, watch_a):
        _be_mock.list_backups.return_value = [
            {"id": "bk_1", "status": "success", "dir": "/b/d",
             "timestamp": "2026-05-01T00:00:00", "size_human": "1 MB"}
        ]
        rc = cli.cmd_restore(self._args(backup_id="bk_ghost"), self._cfg(watch_a))
        assert rc == 1

    def test_full_chain_calls_restore_full_chain(self, watch_a):
        _be_mock.list_backups.return_value = [
            {"id": "bk_1", "timestamp": "2026-05-01T10:00:00", "size_human": "5 MB",
             "status": "success", "dir": "/b/snap"}
        ]
        _be_mock.restore_full_chain.return_value = {"ok": True, "files_restored": 4, "skipped": 0}
        cli.cmd_restore(self._args(full_chain=True), self._cfg(watch_a))
        _be_mock.restore_full_chain.assert_called_once()
        _be_mock.restore_backup.assert_not_called()

    def test_no_overwrite_flag_forwarded(self, watch_a):
        _be_mock.list_backups.return_value = [
            {"id": "bk_1", "timestamp": "2026-05-01T10:00:00", "size_human": "1 MB",
             "status": "success", "dir": "/b/snap"}
        ]
        _be_mock.restore_backup.return_value = {"ok": True, "files_restored": 0, "skipped": 5}
        cli.cmd_restore(self._args(no_overwrite=True), self._cfg(watch_a))
        assert _be_mock.restore_backup.call_args[1]["overwrite"] is False

    # ── HTTPS remote restore ──────────────────────────────────────────────────

    def test_https_calls_download_from_https(self, watch_a):
        """download_from_https must be invoked when dest_type is 'https'."""
        cfg = self._cfg(watch_a, dest_type="https",
                        extra={"dest_https": {"url": "https://bk.example.com", "token": "tok"}})
        _be_mock.list_backups.return_value = [
            {"id": "bk_1", "timestamp": "2026-05-01T10:00:00", "size_human": "5 MB",
             "status": "success", "dir": "/tmp/fake_restore/snap"}
        ]
        _be_mock.restore_backup.return_value = {"ok": True, "files_restored": 2, "skipped": 0}

        tu_mock = MagicMock()
        tu_mock.download_from_https.return_value = {"status": "ok", "downloaded": 3}

        with patch.dict(sys.modules, {"transport_utils": tu_mock}):
            with patch("tempfile.mkdtemp", return_value="/tmp/fake_restore"):
                with patch("shutil.rmtree"):
                    cli.cmd_restore(self._args(), cfg)

        tu_mock.download_from_https.assert_called_once()

    def test_https_download_failure_returns_one(self, capsys, watch_a):
        cfg = self._cfg(watch_a, dest_type="https",
                        extra={"dest_https": {"url": "https://x.example.com", "token": "t"}})
        tu_mock = MagicMock()
        tu_mock.download_from_https.return_value = {"status": "error",
                                                    "error": "connection refused"}
        with patch.dict(sys.modules, {"transport_utils": tu_mock}):
            with patch("tempfile.mkdtemp", return_value="/tmp/fake2"):
                with patch("shutil.rmtree"):
                    rc = cli.cmd_restore(self._args(), cfg)
        assert rc == 1
        assert "connection refused" in capsys.readouterr().err

    def test_https_passes_dest_https_config(self, watch_a):
        """The https_config dict from cfg['dest_https'] must be forwarded to download_from_https."""
        https_conf = {"url": "https://bk.example.com", "token": "mytoken", "verify_ssl": False}
        cfg = self._cfg(watch_a, dest_type="https", extra={"dest_https": https_conf})
        _be_mock.list_backups.return_value = [
            {"id": "bk_1", "timestamp": "2026-05-01T10:00:00", "size_human": "1 MB",
             "status": "success", "dir": "/tmp/fake_restore/snap"}
        ]
        _be_mock.restore_backup.return_value = {"ok": True, "files_restored": 1, "skipped": 0}

        tu_mock = MagicMock()
        tu_mock.download_from_https.return_value = {"status": "ok", "downloaded": 1}

        with patch.dict(sys.modules, {"transport_utils": tu_mock}):
            with patch("tempfile.mkdtemp", return_value="/tmp/fake_restore"):
                with patch("shutil.rmtree"):
                    cli.cmd_restore(self._args(), cfg)

        _call_args = tu_mock.download_from_https.call_args
        # Second positional arg (or kwarg) should be the https_config dict
        passed_cfg = _call_args[0][2] if len(_call_args[0]) >= 3 else _call_args[1].get("https_config")
        assert passed_cfg is not None
        assert passed_cfg.get("token") == "mytoken"


# ── cmd_config_backups ────────────────────────────────────────────────────────

class TestCmdConfigBackups:
    def test_no_backups_returns_zero_and_warns(self, capsys):
        _cfg_mock.list_config_backups.return_value = []
        rc = cli.cmd_config_backups(Namespace(restore_n=None), {})
        assert rc == 0
        assert "No config backups" in capsys.readouterr().out

    def test_lists_backups(self, capsys):
        _cfg_mock.list_config_backups.return_value = [
            {"n": 1, "mtime_iso": "2026-05-01T09:00:00", "size": 2048,
             "path": "/data/config.json.bak.1"},
        ]
        rc = cli.cmd_config_backups(Namespace(restore_n=None), {})
        assert rc == 0
        out = capsys.readouterr().out
        assert "/data/config.json.bak.1" in out

    def test_restore_success_returns_zero(self, capsys):
        _cfg_mock.restore_config_backup.return_value = {"ok": True}
        rc = cli.cmd_config_backups(Namespace(restore_n=1), {})
        assert rc == 0
        assert "restored" in capsys.readouterr().out.lower()

    def test_restore_failure_returns_one(self, capsys):
        _cfg_mock.restore_config_backup.return_value = {"ok": False, "error": "not found"}
        rc = cli.cmd_config_backups(Namespace(restore_n=2), {})
        assert rc == 1
        assert "not found" in capsys.readouterr().err

    def test_restore_calls_correct_n(self):
        _cfg_mock.restore_config_backup.return_value = {"ok": True}
        cli.cmd_config_backups(Namespace(restore_n=3), {})
        _cfg_mock.restore_config_backup.assert_called_once_with(3)


# ── cmd_search ────────────────────────────────────────────────────────────────

class TestCmdSearch:
    def _args(self, **kw):
        defaults = dict(file=None, watch=None, limit=200, verbose=False)
        defaults.update(kw)
        return Namespace(**defaults)

    def test_no_pattern_returns_one(self, capsys):
        rc = cli.cmd_search(self._args(file=""), {"destination": "/b", "watches": []})
        assert rc == 1
        assert "--file" in capsys.readouterr().err

    def test_no_destination_configured_returns_one(self, capsys):
        rc = cli.cmd_search(self._args(file="*.docx"),
                            {"destination": "", "watches": []})
        assert rc == 1

    def test_unknown_watch_filter_returns_one(self, capsys, watch_a):
        cfg = {"destination": "/b", "watches": [watch_a]}
        rc = cli.cmd_search(self._args(file="*.txt", watch="Ghost"), cfg)
        assert rc == 1

    def test_matches_files_in_manifests(self, capsys, tmp_path, watch_a):
        snap = tmp_path / "20260501_120000__My_Documents"
        snap.mkdir()
        manifest = {
            "watch_id": "w_aaa",
            "timestamp": "2026-05-01T12:00:00",
            "changes": [
                {"type": "added", "path": "reports/q1.docx", "size": 4096},
                {"type": "added", "path": "notes.txt", "size": 512},
            ],
        }
        (snap / "MANIFEST.json").write_text(json.dumps(manifest))
        cfg = {"destination": str(tmp_path), "watches": [watch_a]}
        rc = cli.cmd_search(self._args(file="*.docx"), cfg)
        assert rc == 0
        assert "q1.docx" in capsys.readouterr().out

    def test_no_matches_returns_zero(self, capsys, tmp_path, watch_a):
        snap = tmp_path / "20260501_120000__My_Documents"
        snap.mkdir()
        manifest = {
            "watch_id": "w_aaa",
            "timestamp": "2026-05-01T12:00:00",
            "changes": [{"type": "added", "path": "notes.txt", "size": 100}],
        }
        (snap / "MANIFEST.json").write_text(json.dumps(manifest))
        cfg = {"destination": str(tmp_path), "watches": [watch_a]}
        rc = cli.cmd_search(self._args(file="*.xlsx"), cfg)
        assert rc == 0
        out = capsys.readouterr().out
        assert "q1.docx" not in out
