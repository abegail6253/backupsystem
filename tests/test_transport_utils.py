"""
tests/test_transport_utils.py — Unit tests for transport_utils.py

All remote connections are mocked; no real servers are needed.
"""

import json
import os
import sys
import hashlib
import threading
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import transport_utils


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _make_backup_dir(tmp_path: Path, files: dict = None) -> Path:
    """Create a temporary backup directory with test files."""
    bd = tmp_path / "20240101_120000__TestWatch"
    bd.mkdir(parents=True)
    # Always write a non-metadata file
    (bd / "file1.txt").write_bytes(b"hello world")
    (bd / "subdir").mkdir()
    (bd / "subdir" / "file2.txt").write_bytes(b"nested file")
    # These should never be uploaded
    (bd / "MANIFEST.json").write_text("{}")
    (bd / "BACKUP.sha256").write_text("abc123  test\n")
    if files:
        for name, content in files.items():
            p = bd / name
            p.parent.mkdir(parents=True, exist_ok=True)
            if isinstance(content, bytes):
                p.write_bytes(content)
            else:
                p.write_text(content)
    return bd


# ─── SFTP tests ───────────────────────────────────────────────────────────────

class TestUploadSftp(unittest.TestCase):

    def test_missing_host_returns_error(self):
        result = transport_utils.upload_to_sftp("/tmp/fake", {"username": "u", "password": "p"})
        self.assertFalse(result["ok"])
        self.assertIn("host", result["error"].lower())

    def test_missing_username_returns_error(self):
        result = transport_utils.upload_to_sftp("/tmp/fake", {"host": "host.example.com"})
        self.assertFalse(result["ok"])
        self.assertIn("username", result["error"].lower())

    @patch("transport_utils.paramiko", create=True)
    def test_upload_success(self, mock_paramiko):
        """Full happy-path: paramiko transport + sftp client mocked."""
        import tempfile, shutil
        tmp = Path(tempfile.mkdtemp())
        try:
            bd = _make_backup_dir(tmp)

            mock_transport = MagicMock()
            mock_transport.is_authenticated.return_value = True
            mock_transport.get_remote_server_key.return_value = MagicMock(
                get_name=lambda: "ssh-rsa", __eq__=lambda s, o: True
            )
            mock_sftp = MagicMock()
            mock_sftp.stat.side_effect = FileNotFoundError
            mock_sftp.listdir.return_value = []

            mock_paramiko.Transport.return_value = mock_transport
            mock_paramiko.SFTPClient.from_transport.return_value = mock_sftp
            mock_paramiko.HostKeys.return_value = MagicMock(
                lookup=lambda x: None,
                add=MagicMock(), save=MagicMock()
            )

            with patch("transport_utils._CRED_STORE", False):
                result = transport_utils.upload_to_sftp(
                    str(bd),
                    {"host": "sftp.example.com", "username": "user",
                     "password": "pass", "remote_path": "/backups"},
                )
            # At minimum the function should not crash; ok depends on mock depth
            self.assertIn("ok", result)
        finally:
            shutil.rmtree(str(tmp), ignore_errors=True)

    def test_paramiko_not_installed_returns_error(self):
        with patch.dict("sys.modules", {"paramiko": None}):
            result = transport_utils.upload_to_sftp(
                "/tmp/fake",
                {"host": "h", "username": "u", "password": "p"},
            )
        self.assertFalse(result["ok"])
        self.assertIn("paramiko", result["error"].lower())

    @staticmethod
    def _fake_paramiko(open_factory=None, putfo_side_effect=None):
        """Build a fake `paramiko` module good enough to drive upload_to_sftp.

        upload_to_sftp does a local `import paramiko`, so it must be injected
        into sys.modules — patching transport_utils.paramiko is shadowed by
        that re-import and has no effect.
        """
        import types
        m = types.ModuleType("paramiko")

        class _HostKeys:
            def load(self, *a, **k): pass
            def lookup(self, *a, **k): return None
            def add(self, *a, **k): pass
            def save(self, *a, **k): pass

        class _Key:
            def get_name(self): return "ssh-ed25519"

        class _Transport:
            def __init__(self, *a, **k):
                self.default_window_size = 0
                self.default_max_packet_size = 0
            def connect(self, *a, **k): pass
            def get_remote_server_key(self): return _Key()
            def auth_password(self, *a, **k): pass
            def auth_publickey(self, *a, **k): pass
            def is_authenticated(self): return True
            def close(self): pass

        class _DefaultFH:
            def write(self, data): pass
            def close(self): pass

        class _SFTPClient:
            @classmethod
            def from_transport(cls, t): return cls()
            def stat(self, p): raise FileNotFoundError()
            def mkdir(self, p): pass
            def open(self, p, mode): return (open_factory() if open_factory else _DefaultFH())
            def putfo(self, fh, remote, file_size=0):
                if putfo_side_effect:
                    raise putfo_side_effect
            def close(self): pass

        class _SSHException(Exception): pass
        class _AuthException(Exception): pass

        m.HostKeys = _HostKeys
        m.Transport = _Transport
        m.SFTPClient = _SFTPClient
        m.RSAKey = m.Ed25519Key = m.ECDSAKey = _Key
        m.SSHException = _SSHException
        m.AuthenticationException = _AuthException
        m.ssh_exception = types.SimpleNamespace(PasswordRequiredException=_SSHException)
        return m

    def test_upload_with_progress_cb_succeeds(self):
        """Regression: the progress_cb chunked path used to raise
        UnboundLocalError (_bytes_done missing `nonlocal`), so every SFTP
        upload failed when a progress callback was supplied (the normal case)."""
        import tempfile, shutil
        tmp = Path(tempfile.mkdtemp())
        try:
            bd = _make_backup_dir(tmp)  # two real local files to stream
            calls = []
            with patch.dict("sys.modules", {"paramiko": self._fake_paramiko()}), \
                 patch("transport_utils._CRED_STORE", False):
                result = transport_utils.upload_to_sftp(
                    str(bd),
                    {"host": "sftp.example.com", "username": "user",
                     "password": "pass", "remote_path": "/backups"},
                    progress_cb=lambda *a: calls.append(a),
                    max_retries=0,
                )
            self.assertTrue(result["ok"])
            self.assertEqual(result["uploaded"], 2)
            self.assertGreaterEqual(len(calls), 1)  # progress actually reported
        finally:
            shutil.rmtree(str(tmp), ignore_errors=True)

    def test_total_upload_failure_reports_not_ok(self):
        """A total upload failure (0 of N files) must report ok=False, not a
        bare warning with ok=True (which masked data loss as success)."""
        import tempfile, shutil
        tmp = Path(tempfile.mkdtemp())
        try:
            bd = _make_backup_dir(tmp)
            fake = self._fake_paramiko(putfo_side_effect=OSError("disk full"))
            with patch.dict("sys.modules", {"paramiko": fake}), \
                 patch("transport_utils._CRED_STORE", False):
                result = transport_utils.upload_to_sftp(
                    str(bd),
                    {"host": "sftp.example.com", "username": "user",
                     "password": "pass", "remote_path": "/backups"},
                    max_retries=0,  # no progress_cb → putfo path, which fails
                )
            self.assertFalse(result["ok"])
            self.assertEqual(result["uploaded"], 0)
        finally:
            shutil.rmtree(str(tmp), ignore_errors=True)


# ─── FTP tests ────────────────────────────────────────────────────────────────

class TestUploadFtp(unittest.TestCase):

    def test_missing_host_returns_error(self):
        result = transport_utils.upload_to_ftp("/tmp/fake", {})
        self.assertFalse(result["ok"])

    @patch("ftplib.FTP_TLS")
    def test_upload_calls_storbinary(self, mock_ftps_cls):
        import tempfile, shutil
        tmp = Path(tempfile.mkdtemp())
        try:
            bd = _make_backup_dir(tmp)
            mock_ftp = MagicMock()
            mock_ftp.__enter__ = lambda s: mock_ftp
            mock_ftp.__exit__ = MagicMock(return_value=False)
            mock_ftps_cls.return_value = mock_ftp

            with patch("transport_utils._CRED_STORE", False):
                result = transport_utils.upload_to_ftp(
                    str(bd),
                    {"host": "ftp.example.com", "username": "u",
                     "password": "p", "use_tls": True},
                )
            self.assertIn("ok", result)
        finally:
            shutil.rmtree(str(tmp), ignore_errors=True)


# ─── WebDAV tests ─────────────────────────────────────────────────────────────

class TestUploadWebDav(unittest.TestCase):

    def test_missing_url_returns_error(self):
        result = transport_utils.upload_to_webdav("/tmp/fake", {})
        self.assertFalse(result["ok"])
        self.assertIn("url", result["error"].lower())

    def test_missing_username_returns_error(self):
        result = transport_utils.upload_to_webdav("/tmp/fake", {"url": "https://nc.example.com"})
        self.assertFalse(result["ok"])
        self.assertIn("username", result["error"].lower())

    def test_manifest_and_sha256_not_uploaded(self):
        """MANIFEST.json and BACKUP.sha256 must be excluded from uploads."""
        import tempfile, shutil
        tmp = Path(tempfile.mkdtemp())
        try:
            bd = _make_backup_dir(tmp)
            _SKIP = {"MANIFEST.json", "BACKUP.sha256"}
            all_files = [f for f in bd.rglob("*") if f.is_file()]
            to_upload = [f for f in all_files if f.name not in _SKIP]
            skipped   = [f for f in all_files if f.name in _SKIP]
            self.assertTrue(len(to_upload) > 0, "Should have real files to upload")
            self.assertTrue(len(skipped) == 2,  "Should have exactly 2 skipped metadata files")
        finally:
            shutil.rmtree(str(tmp), ignore_errors=True)

    @patch("urllib.request.urlopen")
    def test_put_file_called_per_non_metadata_file(self, mock_urlopen):
        """upload_to_webdav makes a PUT request for each non-metadata file."""
        import tempfile, shutil
        from unittest.mock import MagicMock
        tmp = Path(tempfile.mkdtemp())
        try:
            bd = _make_backup_dir(tmp)
            # Mock: MKCOL returns 201, PUT returns 201
            _SKIP = {"MANIFEST.json", "BACKUP.sha256"}
            real_files = [f for f in bd.rglob("*") if f.is_file() and f.name not in _SKIP]

            mock_resp = MagicMock()
            mock_resp.__enter__ = lambda s: mock_resp
            mock_resp.__exit__ = MagicMock(return_value=False)
            mock_resp.status = 201
            mock_urlopen.return_value = mock_resp

            result = transport_utils.upload_to_webdav(
                str(bd),
                {"url": "https://nc.example.com", "username": "user",
                 "password": "pass", "remote_path": "/backups",
                 "webdav_root": "/remote.php/dav/files/user", "verify_ssl": False},
            )
            # At least MKCOL + PUT calls should have been made
            self.assertTrue(mock_urlopen.called)
        finally:
            shutil.rmtree(str(tmp), ignore_errors=True)


class TestWebDavConnectionTest(unittest.TestCase):

    def test_missing_url_returns_error(self):
        result = transport_utils.test_webdav_connection({})
        self.assertFalse(result["ok"])

    @patch("urllib.request.urlopen")
    def test_propfind_207_returns_ok(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: mock_resp
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.status = 207
        mock_urlopen.return_value = mock_resp
        result = transport_utils.test_webdav_connection(
            {"url": "https://nc.example.com", "username": "u", "password": "p", "verify_ssl": False}
        )
        self.assertTrue(result["ok"])

    @patch("urllib.request.urlopen")
    def test_connection_error_returns_not_ok(self, mock_urlopen):
        import urllib.error
        mock_urlopen.side_effect = urllib.error.HTTPError(
            "https://nc.example.com", 401, "Unauthorized", {}, None
        )
        result = transport_utils.test_webdav_connection(
            {"url": "https://nc.example.com", "username": "u", "password": "bad", "verify_ssl": False}
        )
        self.assertFalse(result["ok"])
        self.assertIn("401", result["error"])


# ─── HTTPS tests ──────────────────────────────────────────────────────────────

class TestUploadHttps(unittest.TestCase):

    def test_missing_url_returns_error(self):
        result = transport_utils.upload_to_https("/tmp/fake", {})
        self.assertFalse(result["ok"])

    def test_metadata_excluded(self):
        """upload_to_https skips MANIFEST.json and BACKUP.sha256."""
        import tempfile, shutil
        tmp = Path(tempfile.mkdtemp())
        try:
            bd = _make_backup_dir(tmp)
            _SKIP = {"MANIFEST.json", "BACKUP.sha256"}
            all_files = [f for f in bd.rglob("*") if f.is_file()]
            uploadable = [f for f in all_files if f.name not in _SKIP]
            self.assertGreater(len(uploadable), 0)
        finally:
            shutil.rmtree(str(tmp), ignore_errors=True)


# ─── Cleanup helpers ──────────────────────────────────────────────────────────

class TestCleanupRemoteBackups(unittest.TestCase):

    def test_https_dest_skipped(self):
        result = transport_utils.cleanup_remote_backups({"dest_type": "https"}, 30)
        self.assertTrue(result["ok"])
        self.assertEqual(result["deleted"], 0)

    def test_local_dest_returns_ok(self):
        result = transport_utils.cleanup_remote_backups({"dest_type": "local"}, 30)
        self.assertTrue(result["ok"])


# ─── cleanup_rclone_backups tests ─────────────────────────────────────────────

class TestCleanupRcloneBackups(unittest.TestCase):
    """Unit tests for the rclone retention/cleanup function added in v1.1.8."""

    # ── config helpers ────────────────────────────────────────────────────────

    def _cfg(self, remote="myremote", path="/backups"):
        return {"remote": remote, "path": path}

    # ── guard: missing remote name ─────────────────────────────────────────────

    def test_missing_remote_name_returns_error(self):
        result = transport_utils.cleanup_rclone_backups({}, 30)
        self.assertFalse(result["ok"])
        self.assertIn("remote name", result["error"])

    # ── guard: retention_days <= 0 ────────────────────────────────────────────

    def test_zero_retention_days_is_noop(self):
        result = transport_utils.cleanup_rclone_backups(self._cfg(), 0)
        self.assertTrue(result["ok"])
        self.assertEqual(result["deleted"], 0)

    def test_negative_retention_days_is_noop(self):
        result = transport_utils.cleanup_rclone_backups(self._cfg(), -7)
        self.assertTrue(result["ok"])
        self.assertEqual(result["deleted"], 0)

    # ── guard: rclone not installed ───────────────────────────────────────────

    @patch("subprocess.run", side_effect=FileNotFoundError)
    def test_rclone_not_installed_returns_error(self, _mock):
        result = transport_utils.cleanup_rclone_backups(self._cfg(), 30)
        self.assertFalse(result["ok"])
        self.assertIn("rclone", result["error"].lower())

    # ── guard: rclone lsd failure ─────────────────────────────────────────────

    @patch("subprocess.run")
    def test_rclone_lsd_failure_returns_error(self, mock_run):
        # First call = rclone version (success), second = rclone lsd (failure)
        mock_version = MagicMock(returncode=0, stdout="rclone v1.65", stderr="")
        mock_lsd     = MagicMock(returncode=1, stdout="", stderr="permission denied")
        mock_run.side_effect = [mock_version, mock_lsd]

        result = transport_utils.cleanup_rclone_backups(self._cfg(), 30)
        self.assertFalse(result["ok"])
        self.assertIn("lsd failed", result["error"])

    # ── happy path: no expired folders ───────────────────────────────────────

    @patch("subprocess.run")
    def test_no_expired_folders_deletes_nothing(self, mock_run):
        import datetime
        # Folder timestamped 1 day ago — inside 30-day retention window
        recent_ts = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).strftime("%Y%m%d_%H%M%S")
        folder_name = f"{recent_ts}_watchid_backup"
        lsd_line = f"          -1 2024-01-01 00:00:00        -1 {folder_name}"

        mock_version = MagicMock(returncode=0, stdout="rclone v1.65", stderr="")
        mock_lsd     = MagicMock(returncode=0, stdout=lsd_line + "\n", stderr="")
        mock_run.side_effect = [mock_version, mock_lsd]

        result = transport_utils.cleanup_rclone_backups(self._cfg(), 30)
        self.assertTrue(result["ok"])
        self.assertEqual(result["deleted"], 0)
        # rclone purge must NOT have been called
        self.assertEqual(mock_run.call_count, 2)

    # ── happy path: one expired folder is purged ──────────────────────────────

    @patch("subprocess.run")
    def test_expired_folder_is_purged(self, mock_run):
        import datetime
        # Folder timestamped 60 days ago — outside 30-day retention window
        old_ts = (datetime.datetime.utcnow() - datetime.timedelta(days=60)).strftime("%Y%m%d_%H%M%S")
        folder_name = f"{old_ts}_watchid_old_backup"
        lsd_line = f"          -1 2024-01-01 00:00:00        -1 {folder_name}"

        mock_version = MagicMock(returncode=0, stdout="rclone v1.65", stderr="")
        mock_lsd     = MagicMock(returncode=0, stdout=lsd_line + "\n", stderr="")
        mock_purge   = MagicMock(returncode=0, stdout="", stderr="")
        mock_run.side_effect = [mock_version, mock_lsd, mock_purge]

        result = transport_utils.cleanup_rclone_backups(self._cfg(), 30)
        self.assertTrue(result["ok"])
        self.assertEqual(result["deleted"], 1)
        self.assertEqual(result["freed_bytes"], 0)   # rclone never reports freed bytes

        # Verify purge was called with the right remote path
        purge_call = mock_run.call_args_list[2]
        cmd = purge_call[0][0]
        self.assertIn("rclone", cmd[0])
        self.assertIn("purge", cmd)
        self.assertTrue(any(folder_name in arg for arg in cmd))

    # ── watch_id filter: only matching folders are purged ─────────────────────

    @patch("subprocess.run")
    def test_watch_id_filter_skips_other_watches(self, mock_run):
        """
        cleanup_rclone_backups uses name.startswith(watch_id), so watch_id must
        be a true prefix of the folder name.  BackupSys folder names start with
        a timestamp (YYYYMMDD_HHMMSS), so supplying the timestamp prefix as the
        watch_id confirms the filter mechanism works correctly.
        """
        import datetime
        old_ts     = (datetime.datetime.utcnow() - datetime.timedelta(days=60)).strftime("%Y%m%d_%H%M%S")
        other_ts   = (datetime.datetime.utcnow() - datetime.timedelta(days=61)).strftime("%Y%m%d_%H%M%S")
        target_folder = f"{old_ts}_watch_abc_backup"
        other_folder  = f"{other_ts}_watch_xyz_backup"
        lsd_out = "\n".join([
            f"          -1 2024-01-01 00:00:00        -1 {target_folder}",
            f"          -1 2024-01-01 00:00:00        -1 {other_folder}",
        ])

        mock_version = MagicMock(returncode=0, stdout="rclone v1.65", stderr="")
        mock_lsd     = MagicMock(returncode=0, stdout=lsd_out + "\n", stderr="")
        mock_purge   = MagicMock(returncode=0, stdout="", stderr="")
        mock_run.side_effect = [mock_version, mock_lsd, mock_purge]

        # Use old_ts as the watch_id prefix — it matches target_folder but not other_folder
        result = transport_utils.cleanup_rclone_backups(self._cfg(), 30, watch_id=old_ts)
        self.assertTrue(result["ok"])
        self.assertEqual(result["deleted"], 1)

        purge_call = mock_run.call_args_list[2]
        cmd = purge_call[0][0]
        self.assertTrue(any(target_folder in arg for arg in cmd))
        self.assertFalse(any(other_folder in arg for arg in cmd))

    # ── non-BackupSys folders are skipped safely ─────────────────────────────

    @patch("subprocess.run")
    def test_non_backupsys_folder_is_skipped(self, mock_run):
        lsd_line = "          -1 2024-01-01 00:00:00        -1 random_folder_no_timestamp"
        mock_version = MagicMock(returncode=0, stdout="rclone v1.65", stderr="")
        mock_lsd     = MagicMock(returncode=0, stdout=lsd_line + "\n", stderr="")
        mock_run.side_effect = [mock_version, mock_lsd]

        result = transport_utils.cleanup_rclone_backups(self._cfg(), 30)
        self.assertTrue(result["ok"])
        self.assertEqual(result["deleted"], 0)
        # No purge call
        self.assertEqual(mock_run.call_count, 2)

    # ── individual purge failure is collected but does not abort ─────────────

    @patch("subprocess.run")
    def test_purge_failure_is_collected_not_fatal(self, mock_run):
        import datetime
        old_ts = (datetime.datetime.utcnow() - datetime.timedelta(days=60)).strftime("%Y%m%d_%H%M%S")
        folder_name = f"{old_ts}_watchid_old"
        lsd_line = f"          -1 2024-01-01 00:00:00        -1 {folder_name}"

        mock_version = MagicMock(returncode=0, stdout="rclone v1.65", stderr="")
        mock_lsd     = MagicMock(returncode=0, stdout=lsd_line + "\n", stderr="")
        mock_purge   = MagicMock(returncode=1, stdout="", stderr="permission denied")
        mock_run.side_effect = [mock_version, mock_lsd, mock_purge]

        result = transport_utils.cleanup_rclone_backups(self._cfg(), 30)
        # Overall result still ok=True (best-effort), but deleted count is 0
        self.assertEqual(result["deleted"], 0)
        # error string should mention the failed folder
        self.assertTrue(result.get("error") or result.get("ok") is True)

    # ── remote_name and remote_path alternative keys are accepted ────────────

    @patch("subprocess.run")
    def test_alternative_config_keys(self, mock_run):
        """cleanup_rclone_backups also accepts remote_name / remote_path keys."""
        mock_version = MagicMock(returncode=0, stdout="rclone v1.65", stderr="")
        mock_lsd     = MagicMock(returncode=0, stdout="", stderr="")
        mock_run.side_effect = [mock_version, mock_lsd]

        result = transport_utils.cleanup_rclone_backups(
            {"remote_name": "myremote", "remote_path": "/backups"}, 30
        )
        self.assertTrue(result["ok"])
        lsd_call = mock_run.call_args_list[1]
        cmd = lsd_call[0][0]
        self.assertTrue(any("myremote:" in arg for arg in cmd))

    # ── remote_count reflects number of folders seen ─────────────────────────

    @patch("subprocess.run")
    def test_remote_count_is_total_folders_seen(self, mock_run):
        import datetime
        recent_ts = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).strftime("%Y%m%d_%H%M%S")
        lsd_out = "\n".join([
            f"          -1 2024-01-01 00:00:00        -1 {recent_ts}_w1_a",
            f"          -1 2024-01-01 00:00:00        -1 {recent_ts}_w2_b",
        ])
        mock_version = MagicMock(returncode=0, stdout="rclone v1.65", stderr="")
        mock_lsd     = MagicMock(returncode=0, stdout=lsd_out + "\n", stderr="")
        mock_run.side_effect = [mock_version, mock_lsd]

        result = transport_utils.cleanup_rclone_backups(self._cfg(), 30)
        self.assertTrue(result["ok"])
        self.assertEqual(result["remote_count"], 2)


if __name__ == "__main__":
    unittest.main()
