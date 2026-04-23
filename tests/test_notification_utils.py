"""
tests/test_notification_utils.py — Unit tests for notification_utils.py

SMTP and urllib are fully mocked; no real network calls are made.
"""

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import notification_utils


# ─── build_backup_email ───────────────────────────────────────────────────────

class TestBuildBackupEmail(unittest.TestCase):

    def _result(self, **kw):
        base = {
            "status": "success",
            "watch_name": "My Docs",
            "timestamp": "2024-06-01T14:30:00",
            "files_copied": 42,
            "total_size": "123.4 MB",
            "duration_s": 17.3,
            "backup_id": "abc123",
        }
        base.update(kw)
        return base

    def test_success_subject_contains_watch_name(self):
        subj, body = notification_utils.build_backup_email(self._result())
        self.assertIn("My Docs", subj)
        self.assertIn("SUCCESS", subj)

    def test_success_body_contains_stats(self):
        subj, body = notification_utils.build_backup_email(self._result())
        self.assertIn("42", body)
        self.assertIn("123.4 MB", body)
        self.assertIn("17.3s", body)

    def test_failure_subject_shows_failed(self):
        subj, body = notification_utils.build_backup_email(
            self._result(status="failed", error="Disk full")
        )
        self.assertIn("FAILED", subj)
        self.assertIn("Disk full", body)

    def test_cancelled_subject_shows_cancelled(self):
        subj, body = notification_utils.build_backup_email(
            self._result(status="cancelled")
        )
        self.assertIn("CANCELLED", subj)

    def test_failed_files_listed_in_body(self):
        _, body = notification_utils.build_backup_email(self._result(
            status="success",
            failed_files=[
                {"path": "locked.db", "reason": "Permission denied"},
                {"path": "open.pst",  "reason": "Sharing violation"},
            ]
        ))
        self.assertIn("locked.db", body)
        self.assertIn("open.pst", body)

    def test_large_failed_files_truncated(self):
        """More than 10 failed files should be capped with a summary."""
        _, body = notification_utils.build_backup_email(self._result(
            status="success",
            failed_files=[{"path": f"file{i}.txt", "reason": "err"} for i in range(15)],
        ))
        self.assertIn("more", body.lower())

    def test_compression_ratio_included_when_nonzero(self):
        _, body = notification_utils.build_backup_email(
            self._result(compression_ratio=35.2)
        )
        self.assertIn("35.2", body)

    def test_returns_two_strings(self):
        result = notification_utils.build_backup_email(self._result())
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], str)
        self.assertIsInstance(result[1], str)


# ─── send_email_notification ─────────────────────────────────────────────────

class TestSendEmailNotification(unittest.TestCase):

    BASE_CFG = {
        "smtp_host": "smtp.example.com",
        "smtp_port": 587,
        "smtp_use_ssl": False,
        "username": "user@example.com",
        "password": "apppass",
        "from_addr": "user@example.com",
        "to_addr": "alerts@example.com",
    }

    def test_missing_host_returns_error(self):
        result = notification_utils.send_email_notification({}, "subj", "body")
        self.assertFalse(result["ok"])
        self.assertIn("host", result["error"].lower())

    def test_missing_to_addr_returns_error(self):
        cfg = dict(self.BASE_CFG); cfg.pop("to_addr")
        result = notification_utils.send_email_notification(cfg, "subj", "body")
        self.assertFalse(result["ok"])

    @patch("smtplib.SMTP")
    def test_starttls_success(self, mock_smtp_cls):
        mock_smtp = MagicMock()
        mock_smtp.__enter__ = lambda s: mock_smtp
        mock_smtp.__exit__ = MagicMock(return_value=False)
        mock_smtp.has_extn.return_value = True
        mock_smtp_cls.return_value = mock_smtp

        result = notification_utils.send_email_notification(
            self.BASE_CFG, "Test subject", "Test body"
        )
        self.assertTrue(result["ok"])
        mock_smtp.sendmail.assert_called_once()

    @patch("smtplib.SMTP_SSL")
    def test_ssl_success(self, mock_ssl_cls):
        mock_smtp = MagicMock()
        mock_smtp.__enter__ = lambda s: mock_smtp
        mock_smtp.__exit__ = MagicMock(return_value=False)
        mock_ssl_cls.return_value = mock_smtp

        cfg = dict(self.BASE_CFG); cfg["smtp_use_ssl"] = True; cfg["smtp_port"] = 465
        result = notification_utils.send_email_notification(cfg, "subj", "body")
        self.assertTrue(result["ok"])

    @patch("smtplib.SMTP")
    def test_auth_error_returns_structured_error(self, mock_smtp_cls):
        import smtplib
        mock_smtp = MagicMock()
        mock_smtp.__enter__ = lambda s: mock_smtp
        mock_smtp.__exit__ = MagicMock(return_value=False)
        mock_smtp.has_extn.return_value = False
        mock_smtp.login.side_effect = smtplib.SMTPAuthenticationError(535, b"Bad credentials")
        mock_smtp_cls.return_value = mock_smtp

        result = notification_utils.send_email_notification(
            self.BASE_CFG, "subj", "body"
        )
        self.assertFalse(result["ok"])
        self.assertIn("auth", result["error"].lower())

    @patch("smtplib.SMTP")
    def test_html_body_sends_multipart(self, mock_smtp_cls):
        mock_smtp = MagicMock()
        mock_smtp.__enter__ = lambda s: mock_smtp
        mock_smtp.__exit__ = MagicMock(return_value=False)
        mock_smtp.has_extn.return_value = False
        mock_smtp_cls.return_value = mock_smtp

        result = notification_utils.send_email_notification(
            self.BASE_CFG, "subj", "plain body",
            body_html="<b>HTML body</b>"
        )
        self.assertTrue(result["ok"])


# ─── send_webhook_notification ────────────────────────────────────────────────

class TestSendWebhookNotification(unittest.TestCase):

    def test_empty_url_returns_error(self):
        result = notification_utils.send_webhook_notification("", {"event": "test"})
        self.assertFalse(result["ok"])

    @patch("urllib.request.urlopen")
    def test_success_response_returns_ok(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: mock_resp
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.status = 200
        mock_urlopen.return_value = mock_resp

        result = notification_utils.send_webhook_notification(
            "https://hooks.example.com/abc",
            {"event": "backup_complete", "status": "success"},
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], 200)

    @patch("urllib.request.urlopen")
    def test_http_404_returns_error(self, mock_urlopen):
        import urllib.error
        mock_urlopen.side_effect = urllib.error.HTTPError(
            "https://hooks.example.com/bad", 404, "Not Found", {}, None
        )
        result = notification_utils.send_webhook_notification(
            "https://hooks.example.com/bad", {"event": "test"}
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], 404)

    @patch("urllib.request.urlopen")
    def test_network_error_returns_error(self, mock_urlopen):
        import urllib.error
        mock_urlopen.side_effect = urllib.error.URLError("Connection refused")
        result = notification_utils.send_webhook_notification(
            "https://hooks.example.com/abc", {"event": "test"}
        )
        self.assertFalse(result["ok"])
        self.assertIsNone(result["status"])

    @patch("urllib.request.urlopen")
    def test_payload_serialised_as_json(self, mock_urlopen):
        """Verify the request body is valid JSON matching the payload."""
        import json
        captured = {}
        real_open = __builtins__

        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: mock_resp
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.status = 200

        def _capture(req, **kwargs):
            captured["body"] = req.data
            captured["ct"] = req.get_header("Content-type")
            return mock_resp

        mock_urlopen.side_effect = _capture

        payload = {"event": "backup_complete", "watch": "Docs", "files": 42}
        notification_utils.send_webhook_notification("https://hooks.example.com/abc", payload)

        self.assertIn("body", captured)
        parsed = json.loads(captured["body"].decode())
        self.assertEqual(parsed["event"], "backup_complete")
        self.assertEqual(parsed["files"], 42)
        self.assertEqual(captured["ct"], "application/json")


# ─── test_webhook ─────────────────────────────────────────────────────────────

class TestTestWebhook(unittest.TestCase):

    @patch("urllib.request.urlopen")
    def test_sends_test_event(self, mock_urlopen):
        import json
        captured = {}

        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: mock_resp
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.status = 200

        def _capture(req, **kw):
            captured["body"] = json.loads(req.data.decode())
            return mock_resp

        mock_urlopen.side_effect = _capture
        notification_utils.test_webhook("https://hooks.example.com/test")
        self.assertEqual(captured["body"]["event"], "test")

    @patch("urllib.request.urlopen")
    def test_test_email_sends_to_correct_recipient(self, _):
        """test_email() should include to_addr in the call."""
        with patch("smtplib.SMTP") as mock_smtp_cls:
            mock_smtp = MagicMock()
            mock_smtp.__enter__ = lambda s: mock_smtp
            mock_smtp.__exit__ = MagicMock(return_value=False)
            mock_smtp.has_extn.return_value = False
            mock_smtp_cls.return_value = mock_smtp

            cfg = {
                "smtp_host": "smtp.example.com", "smtp_port": 587,
                "smtp_use_ssl": False, "username": "u@e.com",
                "password": "pw", "from_addr": "u@e.com",
                "to_addr": "admin@e.com", "enabled": True,
            }
            result = notification_utils.test_email(cfg)
            self.assertTrue(result["ok"])
            # Verify sendmail was called with the right recipient
            args = mock_smtp.sendmail.call_args[0]
            self.assertIn("admin@e.com", args[1])


if __name__ == "__main__":
    unittest.main()
