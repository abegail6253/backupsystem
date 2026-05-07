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


# ─── send_telegram_notification ──────────────────────────────────────────────

class TestSendTelegramNotification(unittest.TestCase):

    BASE_CFG = {
        "bot_token": "123456:ABCDEF",
        "chat_id": "987654321",
    }

    def test_missing_token_returns_error(self):
        result = notification_utils.send_telegram_notification({}, "hello")
        self.assertFalse(result["ok"])
        self.assertIn("bot_token", result["error"].lower())

    def test_missing_chat_id_returns_error(self):
        cfg = {"bot_token": "tok"}
        result = notification_utils.send_telegram_notification(cfg, "hello")
        self.assertFalse(result["ok"])
        self.assertIn("chat_id", result["error"].lower())

    @patch("urllib.request.urlopen")
    def test_success_response_returns_ok(self, mock_urlopen):
        import json as _json
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: mock_resp
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.status = 200
        mock_resp.read.return_value = _json.dumps({"ok": True}).encode()
        mock_urlopen.return_value = mock_resp

        result = notification_utils.send_telegram_notification(self.BASE_CFG, "test")
        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], 200)
        self.assertIsNone(result["error"])

    @patch("urllib.request.urlopen")
    def test_telegram_api_error_in_body_returns_error(self, mock_urlopen):
        import json as _json
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: mock_resp
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.status = 200
        mock_resp.read.return_value = _json.dumps(
            {"ok": False, "description": "Bad Request: chat not found"}
        ).encode()
        mock_urlopen.return_value = mock_resp

        result = notification_utils.send_telegram_notification(self.BASE_CFG, "test")
        self.assertFalse(result["ok"])
        self.assertIn("chat not found", result["error"])

    @patch("urllib.request.urlopen")
    def test_http_error_returns_structured_error(self, mock_urlopen):
        import urllib.error
        mock_urlopen.side_effect = urllib.error.HTTPError(
            "https://api.telegram.org/...", 401, "Unauthorized", {}, None
        )
        result = notification_utils.send_telegram_notification(self.BASE_CFG, "test")
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], 401)

    @patch("urllib.request.urlopen")
    def test_network_error_returns_error_with_no_status(self, mock_urlopen):
        import urllib.error
        mock_urlopen.side_effect = urllib.error.URLError("Connection timed out")
        result = notification_utils.send_telegram_notification(self.BASE_CFG, "test")
        self.assertFalse(result["ok"])
        self.assertIsNone(result["status"])

    @patch("urllib.request.urlopen")
    def test_payload_sent_as_json_with_correct_fields(self, mock_urlopen):
        import json as _json
        captured = {}
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: mock_resp
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.status = 200
        mock_resp.read.return_value = _json.dumps({"ok": True}).encode()

        def _capture(req, **kw):
            captured["body"] = _json.loads(req.data.decode())
            captured["ct"] = req.get_header("Content-type")
            return mock_resp

        mock_urlopen.side_effect = _capture
        notification_utils.send_telegram_notification(self.BASE_CFG, "Hello!")
        self.assertEqual(captured["body"]["chat_id"], "987654321")
        self.assertEqual(captured["body"]["text"], "Hello!")
        self.assertEqual(captured["ct"], "application/json")


# ─── build_telegram_message ───────────────────────────────────────────────────

class TestBuildTelegramMessage(unittest.TestCase):

    def _result(self, **kw):
        base = {
            "status": "success",
            "watch_name": "My Docs",
            "timestamp": "2024-06-01T14:30:00",
            "files_copied": 10,
            "total_size": "5.0 MB",
            "duration_s": 3.2,
        }
        base.update(kw)
        return base

    def test_success_contains_ok_emoji_and_watch_name(self):
        msg = notification_utils.build_telegram_message(self._result())
        self.assertIn("✅", msg)
        self.assertIn("My Docs", msg)

    def test_failure_contains_failed_and_error(self):
        msg = notification_utils.build_telegram_message(
            self._result(status="failed", error="Disk full")
        )
        self.assertIn("❌", msg)
        self.assertIn("Disk full", msg)

    def test_cancelled_contains_cancelled_text(self):
        msg = notification_utils.build_telegram_message(
            self._result(status="cancelled")
        )
        self.assertIn("Cancelled", msg)

    def test_html_special_chars_are_escaped(self):
        """Watch names with <>&  must be HTML-escaped so Telegram doesn't choke."""
        msg = notification_utils.build_telegram_message(
            self._result(watch_name="A & B <test>")
        )
        self.assertNotIn("<test>", msg)
        self.assertIn("&lt;test&gt;", msg)

    def test_failed_files_mention_in_success_message(self):
        msg = notification_utils.build_telegram_message(self._result(
            failed_files=[{"path": "locked.db", "reason": "Permission denied"}]
        ))
        self.assertIn("1", msg)  # count of failed files


# ─── send_pushover_notification ───────────────────────────────────────────────

class TestSendPushoverNotification(unittest.TestCase):

    BASE_CFG = {
        "user_key": "uKEY1234567890",
        "api_token": "aTOKEN123456",
    }

    def test_missing_user_key_returns_error(self):
        result = notification_utils.send_pushover_notification(
            {"api_token": "tok"}, "title", "body"
        )
        self.assertFalse(result["ok"])
        self.assertIn("user_key", result["error"].lower())

    def test_missing_api_token_returns_error(self):
        result = notification_utils.send_pushover_notification(
            {"user_key": "key"}, "title", "body"
        )
        self.assertFalse(result["ok"])
        self.assertIn("api_token", result["error"].lower())

    @patch("urllib.request.urlopen")
    def test_success_response_returns_ok(self, mock_urlopen):
        import json as _json
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: mock_resp
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.status = 200
        mock_resp.read.return_value = _json.dumps({"status": 1}).encode()
        mock_urlopen.return_value = mock_resp

        result = notification_utils.send_pushover_notification(
            self.BASE_CFG, "Test title", "Test body"
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], 200)

    @patch("urllib.request.urlopen")
    def test_pushover_api_error_in_body_returns_error(self, mock_urlopen):
        import json as _json
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: mock_resp
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.status = 200
        mock_resp.read.return_value = _json.dumps(
            {"status": 0, "errors": ["user key is invalid"]}
        ).encode()
        mock_urlopen.return_value = mock_resp

        result = notification_utils.send_pushover_notification(
            self.BASE_CFG, "t", "b"
        )
        self.assertFalse(result["ok"])
        self.assertIn("user key is invalid", result["error"])

    @patch("urllib.request.urlopen")
    def test_http_error_returns_structured_error(self, mock_urlopen):
        import urllib.error
        mock_urlopen.side_effect = urllib.error.HTTPError(
            "https://api.pushover.net/...", 429, "Too Many Requests", {}, None
        )
        result = notification_utils.send_pushover_notification(
            self.BASE_CFG, "t", "b"
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], 429)

    @patch("urllib.request.urlopen")
    def test_network_error_returns_error_with_no_status(self, mock_urlopen):
        import urllib.error
        mock_urlopen.side_effect = urllib.error.URLError("Name resolution failed")
        result = notification_utils.send_pushover_notification(
            self.BASE_CFG, "t", "b"
        )
        self.assertFalse(result["ok"])
        self.assertIsNone(result["status"])

    @patch("urllib.request.urlopen")
    def test_priority_is_clamped_to_valid_range(self, mock_urlopen):
        """Priority must be clamped to [-2, 1] — never emergency (2)."""
        import json as _json, urllib.parse
        captured = {}
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: mock_resp
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.status = 200
        mock_resp.read.return_value = _json.dumps({"status": 1}).encode()

        def _cap(req, **kw):
            captured["fields"] = dict(urllib.parse.parse_qsl(req.data.decode()))
            return mock_resp

        mock_urlopen.side_effect = _cap
        # pass an out-of-range priority (3) — should be clamped to 1
        notification_utils.send_pushover_notification(self.BASE_CFG, "t", "b", priority=3)
        self.assertIn(captured.get("fields", {}).get("priority", ""), ["1", "-1", "0"])

    @patch("urllib.request.urlopen")
    def test_optional_device_and_sound_included_when_set(self, mock_urlopen):
        import json as _json, urllib.parse
        captured = {}
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: mock_resp
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.status = 200
        mock_resp.read.return_value = _json.dumps({"status": 1}).encode()

        def _cap(req, **kw):
            captured["fields"] = dict(urllib.parse.parse_qsl(req.data.decode()))
            return mock_resp

        mock_urlopen.side_effect = _cap
        cfg = dict(self.BASE_CFG, device="iphone", sound="magic")
        notification_utils.send_pushover_notification(cfg, "t", "b")
        self.assertEqual(captured["fields"].get("device"), "iphone")
        self.assertEqual(captured["fields"].get("sound"), "magic")


# ─── build_pushover_notification ─────────────────────────────────────────────

class TestBuildPushoverNotification(unittest.TestCase):

    def _result(self, **kw):
        base = {
            "status": "success",
            "watch_name": "My Docs",
            "timestamp": "2024-06-01T14:30:00",
            "files_copied": 5,
            "total_size": "2 MB",
            "duration_s": 1.5,
        }
        base.update(kw)
        return base

    def test_success_returns_low_priority(self):
        title, msg, priority = notification_utils.build_pushover_notification(self._result())
        self.assertIn("OK", title)
        self.assertIn("My Docs", title)
        self.assertEqual(priority, 0)

    def test_failure_returns_high_priority(self):
        _, _, priority = notification_utils.build_pushover_notification(
            self._result(status="failed", error="oops")
        )
        self.assertEqual(priority, 1)

    def test_cancelled_returns_negative_priority(self):
        _, _, priority = notification_utils.build_pushover_notification(
            self._result(status="cancelled")
        )
        self.assertLess(priority, 0)

    def test_returns_three_element_tuple(self):
        result = notification_utils.build_pushover_notification(self._result())
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 3)
        title, msg, priority = result
        self.assertIsInstance(title, str)
        self.assertIsInstance(msg, str)
        self.assertIsInstance(priority, int)


# ─── test_telegram / test_pushover convenience wrappers ──────────────────────

class TestConvenienceTestFunctions(unittest.TestCase):

    @patch("urllib.request.urlopen")
    def test_test_telegram_sends_a_message(self, mock_urlopen):
        import json as _json
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: mock_resp
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.status = 200
        mock_resp.read.return_value = _json.dumps({"ok": True}).encode()
        mock_urlopen.return_value = mock_resp

        cfg = {"bot_token": "tok:abc", "chat_id": "123"}
        result = notification_utils.test_telegram(cfg)
        self.assertTrue(result["ok"])
        # Verify something was actually POSTed
        mock_urlopen.assert_called_once()

    @patch("urllib.request.urlopen")
    def test_test_pushover_sends_a_notification(self, mock_urlopen):
        import json as _json
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: mock_resp
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.status = 200
        mock_resp.read.return_value = _json.dumps({"status": 1}).encode()
        mock_urlopen.return_value = mock_resp

        cfg = {"user_key": "uKEY", "api_token": "aTOK"}
        result = notification_utils.test_pushover(cfg)
        self.assertTrue(result["ok"])
        mock_urlopen.assert_called_once()


if __name__ == "__main__":
    unittest.main()

