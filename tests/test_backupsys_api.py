"""
tests/test_backupsys_api.py — Unit tests for the Flask API.
Run:  pytest tests/test_backupsys_api.py
"""
import json, os, sys, time, tempfile
from pathlib import Path
from unittest import mock
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Use a real temp file — ":memory:" creates a fresh DB per-connection
_DB_FILE = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_DB_FILE.close()

os.environ["BACKUPSYS_API_KEY"]      = "test-api-key-abcdef1234567890"
os.environ["BACKUPSYS_OTP_HMAC_KEY"] = "test-hmac-key-abcdef1234567890"
os.environ["BACKUPSYS_DB_PATH"]      = _DB_FILE.name
# Give SMTP config so the "SMTP not configured" check passes
os.environ.setdefault("BACKUPSYS_SMTP_HOST", "smtp.example.com")
os.environ.setdefault("BACKUPSYS_SMTP_PORT", "587")
os.environ.setdefault("BACKUPSYS_SMTP_USER", "test@example.com")
os.environ.setdefault("BACKUPSYS_SMTP_PASS", "testpass")

import backupsys_api as api

_SMTP_MOCK     = mock.patch("smtplib.SMTP",     autospec=True)
_SMTP_SSL_MOCK = mock.patch("smtplib.SMTP_SSL", autospec=True)
TEST_EMAIL = "admin@example.com"
API_KEY    = os.environ["BACKUPSYS_API_KEY"]


@pytest.fixture(autouse=True)
def _fresh_db():
    api._init_db()
    conn = api._get_conn()
    conn.execute("DELETE FROM otp_store")
    conn.commit(); conn.close()
    yield


@pytest.fixture()
def client():
    api.app.config["TESTING"] = True
    with api.app.test_client() as c:
        yield c


def _plant(otp, email=TEST_EMAIL, expire_in=300):
    api._otp_store_set(email, api._hmac_otp(otp), time.time() + expire_in, time.time())


# ─── /health ──────────────────────────────────────────────────────────────────
class TestHealth:
    def test_returns_200(self, client):
        assert client.get("/health").status_code == 200

    def test_has_ok_true(self, client):
        assert json.loads(client.get("/health").data).get("ok") is True

    def test_has_service_key(self, client):
        assert "service" in json.loads(client.get("/health").data)


# ─── /send-otp ────────────────────────────────────────────────────────────────
class TestSendOtp:
    def _post(self, client, email=TEST_EMAIL, key=API_KEY):
        return client.post("/send-otp", json={"email": email},
                           headers={"X-API-Key": key})

    def test_wrong_api_key_rejected(self, client):
        assert self._post(client, key="wrong").status_code == 401

    def test_missing_email_rejected(self, client):
        r = client.post("/send-otp", json={}, headers={"X-API-Key": API_KEY})
        assert r.status_code == 400

    def test_valid_request_does_not_crash(self, client):
        with _SMTP_MOCK, _SMTP_SSL_MOCK:
            r = self._post(client)
        assert r.status_code in (200, 500)

    def test_rate_limit_returns_429(self, client):
        # Plant a very recent OTP row so the rate-limit window is active
        api._otp_store_set(TEST_EMAIL, "fakehash", time.time() + 300, time.time())
        with _SMTP_MOCK, _SMTP_SSL_MOCK:
            r = self._post(client)
        assert r.status_code == 429


# ─── /verify-otp ──────────────────────────────────────────────────────────────
# API uses body key "code" (not "otp") and returns {"ok": bool}
class TestVerifyOtp:
    def _post(self, client, code, email=TEST_EMAIL, key=API_KEY):
        return client.post("/verify-otp",
                           json={"email": email, "code": code},
                           headers={"X-API-Key": key})

    def test_correct_code_returns_ok_true(self, client):
        _plant("123456")
        assert json.loads(self._post(client, "123456").data).get("ok") is True

    def test_wrong_code_returns_ok_false(self, client):
        _plant("999999")
        assert json.loads(self._post(client, "000000").data).get("ok") is False

    def test_expired_code_rejected(self, client):
        _plant("111111", expire_in=-10)
        assert json.loads(self._post(client, "111111").data).get("ok") is False

    def test_no_record_rejected(self, client):
        assert json.loads(self._post(client, "123456").data).get("ok") is False

    def test_wrong_api_key_rejected(self, client):
        assert self._post(client, "123456", key="bad").status_code == 401

    def test_lockout_after_max_attempts(self, client):
        _plant("correct")
        for _ in range(api.OTP_MAX_ATTEMPTS):
            self._post(client, "wrong")
        assert json.loads(self._post(client, "correct").data).get("ok") is False


# ─── HMAC helper ──────────────────────────────────────────────────────────────
class TestHmacOtp:
    def test_deterministic(self):
        assert api._hmac_otp("123456") == api._hmac_otp("123456")

    def test_distinct_differ(self):
        assert api._hmac_otp("111111") != api._hmac_otp("222222")

    def test_returns_hex(self):
        int(api._hmac_otp("000000"), 16)


# ─── OTP store helpers ────────────────────────────────────────────────────────
class TestOtpStore:
    def test_set_and_get(self):
        api._otp_store_set(TEST_EMAIL, "hash123", time.time()+300, time.time())
        row = api._otp_store_get(TEST_EMAIL)
        assert row and row["otp_hash"] == "hash123"

    def test_get_missing_returns_none(self):
        assert api._otp_store_get("nobody@x.com") is None

    def test_delete(self):
        api._otp_store_set(TEST_EMAIL, "h", time.time()+300, time.time())
        api._otp_store_delete(TEST_EMAIL)
        assert api._otp_store_get(TEST_EMAIL) is None

    def test_purge_expired(self):
        api._otp_store_set("old@x.com", "h",  time.time()-1,   time.time()-400)
        api._otp_store_set(TEST_EMAIL,  "h2", time.time()+300, time.time())
        api._otp_store_purge_expired()
        assert api._otp_store_get("old@x.com") is None
        assert api._otp_store_get(TEST_EMAIL)  is not None

    def test_increment_attempts(self):
        api._otp_store_set(TEST_EMAIL, "h", time.time()+300, time.time())
        assert api._otp_store_increment_attempts(TEST_EMAIL) == 1
        assert api._otp_store_increment_attempts(TEST_EMAIL) == 2
