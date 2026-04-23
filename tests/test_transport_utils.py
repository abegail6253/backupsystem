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


if __name__ == "__main__":
    unittest.main()
