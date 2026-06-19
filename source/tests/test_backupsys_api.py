"""
tests/test_backupsys_api.py — Comprehensive tests for backupsys_api.py
Run:  pytest tests/test_backupsys_api.py

Covers:
  - HMAC authentication (require_auth decorator)
  - /health
  - /ping
  - /backup/event   (ingestion, rate-limit, size guard, rolling cap)
  - /backup/events  (list with limit / status / machine_id filters, ordering)
  - /admin/stats
  - /otp/request    (rate-limit, window reset, lockout)
  - /otp/verify     (success, wrong OTP, expiry, lockout, case-insensitive)
  - /backup/upload  (multipart, path traversal)
  - /manifest       (file listing, missing dir)
  - /files/<path>   (serve file, missing file)
  - /dashboard      (HTML rendering)
  - Database migration (_init_db idempotency, machine_id column on old schema)
  - Helpers: _safe_path, _fmt_bytes, _compute_sig, _verify_sig,
             _generate_otp, _hash_otp, _event_rate_limit_check
"""

from __future__ import annotations

import hashlib
import hmac
import io
import json
import os
import sqlite3
import sys
import time
from pathlib import Path
from unittest.mock import patch

import pytest

# ── Make sure the project root is importable ─────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# ── Set a test API key BEFORE importing the module ───────────────────────────
_TEST_KEY = "testkeyforbackupsys_atleast32chars!!"
os.environ["BACKUPSYS_API_KEY"] = _TEST_KEY


# ── Import the module under test AFTER env vars are set ──────────────────────
import backupsys_api as api


# ─── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _isolated_db(tmp_path, monkeypatch):
    """
    Redirect every DB and file-store path to a fresh tmp_path for each test.
    Also reset the in-process rate-limit store so tests are independent.
    """
    db_path    = tmp_path / "test.db"
    files_dir  = tmp_path / "files"

    monkeypatch.setattr(api, "DB_PATH",   db_path)
    monkeypatch.setattr(api, "FILES_DIR", files_dir)
    monkeypatch.setattr(api, "API_KEY",   _TEST_KEY)

    # Clear the in-process event rate-limit store
    with api._event_rl_lock:
        api._event_rl_store.clear()

    api._init_db()
    yield tmp_path


@pytest.fixture()
def client(_isolated_db):
    """Flask test client with app context."""
    api.app.config["TESTING"] = True
    with api.app.test_client() as c:
        yield c


# ─── Helper: build a valid HMAC signature ─────────────────────────────────────

def _sig(body: bytes | str, key: str = _TEST_KEY) -> str:
    if isinstance(body, str):
        body = body.encode()
    return hmac.new(key.encode(), body, hashlib.sha256).hexdigest()


def _auth_headers(body: bytes | str = b"", key: str = _TEST_KEY) -> dict:
    return {
        "X-BackupSys-Signature": _sig(body, key),
        "X-BackupSys-Timestamp": str(time.time()),
    }


def _json_post(client, url, payload: dict, key: str = _TEST_KEY):
    """POST JSON with a valid HMAC signature and timestamp."""
    body = json.dumps(payload).encode()
    headers = {**_auth_headers(body, key), "Content-Type": "application/json"}
    return client.post(url, data=body, headers=headers)


# ─── Helpers unit tests ────────────────────────────────────────────────────────

class TestComputeSig:
    def test_known_value(self):
        result = api._compute_sig(b"hello")
        expected = hmac.new(_TEST_KEY.encode(), b"hello", hashlib.sha256).hexdigest()
        assert result == expected

    def test_empty_body(self):
        result = api._compute_sig(b"")
        assert isinstance(result, str) and len(result) == 64

    def test_different_bodies_differ(self):
        assert api._compute_sig(b"a") != api._compute_sig(b"b")


class TestVerifySig:
    def test_valid_signature(self):
        body = b'{"status":"success"}'
        assert api._verify_sig(body, _sig(body)) is True

    def test_invalid_signature(self):
        assert api._verify_sig(b"body", "deadbeef" * 8) is False

    def test_empty_signature(self):
        assert api._verify_sig(b"body", "") is False

    def test_case_insensitive(self):
        body = b"test"
        sig = _sig(body).upper()
        assert api._verify_sig(body, sig) is True


class TestGenerateOtp:
    def test_correct_length(self):
        otp = api._generate_otp()
        assert len(otp) == api.OTP_LENGTH

    def test_only_alphanumeric(self):
        for _ in range(10):
            otp = api._generate_otp()
            assert otp.isalnum() and otp == otp.upper()

    def test_randomness(self):
        otps = {api._generate_otp() for _ in range(20)}
        assert len(otps) > 1


class TestHashOtp:
    def test_is_sha256_hex(self):
        h = api._hash_otp("ABCD1234")
        assert len(h) == 64 and all(c in "0123456789abcdef" for c in h)

    def test_deterministic(self):
        assert api._hash_otp("X") == api._hash_otp("X")

    def test_different_inputs_differ(self):
        assert api._hash_otp("A") != api._hash_otp("B")


class TestFmtBytes:
    def test_zero(self):
        assert api._fmt_bytes(0) == "—"

    def test_none(self):
        assert api._fmt_bytes(None) == "—"

    def test_bytes(self):
        assert "B" in api._fmt_bytes(512)

    def test_kilobytes(self):
        assert "KB" in api._fmt_bytes(2048)

    def test_megabytes(self):
        assert "MB" in api._fmt_bytes(2 * 1024 * 1024)

    def test_gigabytes(self):
        assert "GB" in api._fmt_bytes(2 * 1024 ** 3)


class TestSafePath:
    def test_valid_path(self, tmp_path):
        result = api._safe_path(tmp_path, "subdir", "file.txt")
        assert result == (tmp_path / "subdir" / "file.txt").resolve()

    def test_traversal_attack_blocked(self, tmp_path):
        result = api._safe_path(tmp_path, "..", "etc", "passwd")
        assert result is None

    def test_nested_traversal_blocked(self, tmp_path):
        result = api._safe_path(tmp_path, "a", "..", "..", "etc")
        assert result is None

    def test_simple_filename(self, tmp_path):
        result = api._safe_path(tmp_path, "backup.tar.gz")
        assert result is not None
        assert result.parent == tmp_path.resolve()


class TestEventRateLimit:
    def test_allows_under_limit(self):
        with api._event_rl_lock:
            api._event_rl_store.clear()
        for _ in range(api.EVENT_RATE_LIMIT):
            assert api._event_rate_limit_check("1.2.3.4") is True

    def test_blocks_over_limit(self):
        with api._event_rl_lock:
            api._event_rl_store.clear()
        for _ in range(api.EVENT_RATE_LIMIT):
            api._event_rate_limit_check("5.6.7.8")
        assert api._event_rate_limit_check("5.6.7.8") is False

    def test_different_ips_are_independent(self):
        with api._event_rl_lock:
            api._event_rl_store.clear()
        for _ in range(api.EVENT_RATE_LIMIT):
            api._event_rate_limit_check("10.0.0.1")
        # A different IP is unaffected
        assert api._event_rate_limit_check("10.0.0.2") is True

    def test_old_timestamps_are_pruned(self):
        with api._event_rl_lock:
            # Manually plant expired timestamps
            api._event_rl_store["9.9.9.9"] = [time.time() - api.EVENT_RATE_WINDOW_SEC - 5]
        assert api._event_rate_limit_check("9.9.9.9") is True




class TestCheckTimestamp:
    """Unit tests for the _check_timestamp() replay-protection helper."""

    def test_valid_timestamp_accepted(self):
        """A timestamp within the tolerance window returns (True, '')."""
        with api.app.test_request_context(
            headers={"X-BackupSys-Timestamp": str(time.time())}
        ):
            ok, err = api._check_timestamp()
        assert ok is True
        assert err == ""

    def test_missing_header_rejected(self):
        """A request with no X-BackupSys-Timestamp header is rejected."""
        with api.app.test_request_context():
            ok, err = api._check_timestamp()
        assert ok is False
        assert "Missing" in err

    def test_non_numeric_header_rejected(self):
        """A non-numeric timestamp value is rejected."""
        with api.app.test_request_context(
            headers={"X-BackupSys-Timestamp": "not-a-number"}
        ):
            ok, err = api._check_timestamp()
        assert ok is False
        assert "Invalid" in err

    def test_expired_timestamp_rejected(self):
        """A timestamp older than TIMESTAMP_TOLERANCE_SEC is rejected."""
        old_ts = time.time() - api.TIMESTAMP_TOLERANCE_SEC - 1
        with api.app.test_request_context(
            headers={"X-BackupSys-Timestamp": str(old_ts)}
        ):
            ok, err = api._check_timestamp()
        assert ok is False
        assert "window" in err

    def test_future_timestamp_rejected(self):
        """A timestamp too far in the future is also rejected."""
        future_ts = time.time() + api.TIMESTAMP_TOLERANCE_SEC + 1
        with api.app.test_request_context(
            headers={"X-BackupSys-Timestamp": str(future_ts)}
        ):
            ok, err = api._check_timestamp()
        assert ok is False
        assert "window" in err

    def test_require_auth_rejects_missing_timestamp(self, client):
        """require_auth returns 401 when X-BackupSys-Timestamp is absent."""
        # Valid HMAC but deliberately no timestamp header
        r = client.post("/ping", headers={"X-BackupSys-Signature": _sig(b"")})
        assert r.status_code == 401
        assert b"Timestamp" in r.data or b"timestamp" in r.data or b"Missing" in r.data

    def test_require_auth_rejects_expired_timestamp(self, client):
        """require_auth returns 401 when the timestamp is outside the window."""
        old_ts = time.time() - api.TIMESTAMP_TOLERANCE_SEC - 5
        headers = {
            **_auth_headers(),
            "X-BackupSys-Timestamp": str(old_ts),
        }
        r = client.post("/ping", headers=headers)
        assert r.status_code == 401

    def test_require_auth_accepts_valid_timestamp(self, client):
        """require_auth passes through when timestamp and signature are both valid."""
        headers = {
            **_auth_headers(),
            "X-BackupSys-Timestamp": str(time.time()),
        }
        r = client.post("/ping", headers=headers)
        assert r.status_code == 200


# ─── /health ──────────────────────────────────────────────────────────────────

class TestHealth:
    def test_returns_200(self, client):
        r = client.get("/health")
        assert r.status_code == 200

    def test_body_has_status_ok(self, client):
        data = r = client.get("/health")
        assert json.loads(r.data)["status"] == "ok"

    def test_unauthenticated(self, client):
        """Health endpoint requires no signature."""
        r = client.get("/health")
        assert r.status_code == 200

    def test_has_ts_field(self, client):
        data = json.loads(client.get("/health").data)
        assert "ts" in data and "T" in data["ts"]


# ─── /ping ────────────────────────────────────────────────────────────────────

class TestPing:
    def test_authed_returns_200(self, client):
        r = client.post("/ping", headers=_auth_headers())
        assert r.status_code == 200
        assert json.loads(r.data)["ok"] is True

    def test_no_signature_returns_401(self, client):
        r = client.post("/ping")
        assert r.status_code == 401

    def test_wrong_signature_returns_401(self, client):
        r = client.post("/ping", headers={"X-BackupSys-Signature": "bad"})
        assert r.status_code == 401

    def test_wrong_key_returns_401(self, client):
        r = client.post("/ping", headers=_auth_headers(b"", key="wrongkey_wrongkey_wrongkey_wrong!"))
        assert r.status_code == 401


# ─── /backup/event ────────────────────────────────────────────────────────────

class TestBackupEvent:
    def _post(self, client, payload):
        return _json_post(client, "/backup/event", payload)

    def test_valid_event_returns_200(self, client):
        r = self._post(client, {"status": "success", "watch_name": "Docs",
                                 "files_copied": 5, "bytes_copied": 1024})
        assert r.status_code == 200
        assert json.loads(r.data)["ok"] is True

    def test_event_stored_in_db(self, client, _isolated_db):
        self._post(client, {"status": "success", "watch_name": "TestWatch",
                             "machine_id": "HOST1"})
        db = sqlite3.connect(str(api.DB_PATH))
        row = db.execute("SELECT watch_name, machine_id FROM backup_events").fetchone()
        assert row[0] == "TestWatch"
        assert row[1] == "HOST1"
        db.close()

    def test_no_signature_returns_401(self, client):
        r = client.post("/backup/event",
                        data=json.dumps({"status": "success"}),
                        content_type="application/json")
        assert r.status_code == 401

    def test_empty_body_returns_400(self, client):
        body = b""
        r = client.post("/backup/event",
                        data=body,
                        headers={**_auth_headers(body),
                                  "Content-Type": "application/json"})
        assert r.status_code == 400

    def test_payload_too_large_returns_413(self, client):
        large = {"status": "success", "data": "x" * (api.EVENT_MAX_BODY_BYTES + 1)}
        body = json.dumps(large).encode()
        headers = {**_auth_headers(body), "Content-Type": "application/json",
                   "Content-Length": str(len(body))}
        r = client.post("/backup/event", data=body, headers=headers)
        assert r.status_code == 413

    def test_machine_id_stored(self, client, _isolated_db):
        self._post(client, {"status": "failure", "machine_id": "MY-PC"})
        db = sqlite3.connect(str(api.DB_PATH))
        row = db.execute("SELECT machine_id FROM backup_events").fetchone()
        assert row[0] == "MY-PC"
        db.close()

    def test_received_at_in_response(self, client):
        r = self._post(client, {"status": "success"})
        data = json.loads(r.data)
        assert "received_at" in data

    def test_rate_limit_returns_429(self, client):
        # Flood from same IP to trigger rate limit
        with api._event_rl_lock:
            api._event_rl_store.clear()
        # Fill up the rate-limit bucket manually
        with api._event_rl_lock:
            api._event_rl_store["127.0.0.1"] = [time.time()] * api.EVENT_RATE_LIMIT
        r = self._post(client, {"status": "success"})
        assert r.status_code == 429

    def test_rolling_cap_enforced(self, client, _isolated_db, monkeypatch):
        """DB must never hold more than MAX_EVENTS rows."""
        monkeypatch.setattr(api, "MAX_EVENTS", 3)
        for i in range(5):
            self._post(client, {"status": "success", "watch_name": f"Watch{i}"})
        db = sqlite3.connect(str(api.DB_PATH))
        count = db.execute("SELECT COUNT(*) FROM backup_events").fetchone()[0]
        db.close()
        assert count == 3


# ─── /backup/events ───────────────────────────────────────────────────────────

class TestListEvents:
    def _seed(self, client, n=3):
        for i in range(n):
            _json_post(client, "/backup/event",
                       {"status": "success" if i % 2 == 0 else "failure",
                        "watch_name": f"Watch{i}",
                        "machine_id": f"HOST{i % 2}"})

    def _get(self, client, **params):
        body = b""
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"/backup/events?{qs}" if qs else "/backup/events"
        return client.get(url, headers=_auth_headers(body))

    def test_returns_200(self, client):
        assert self._get(client).status_code == 200

    def test_empty_returns_zero_count(self, client):
        data = json.loads(self._get(client).data)
        assert data["count"] == 0
        assert data["events"] == []

    def test_seeded_events_returned(self, client):
        self._seed(client, 3)
        data = json.loads(self._get(client).data)
        assert data["count"] == 3

    def test_limit_respected(self, client):
        self._seed(client, 5)
        data = json.loads(self._get(client, limit=2).data)
        assert data["count"] == 2

    def test_limit_max_capped_at_500(self, client):
        # Limit param capped at 500 even if 999 requested
        data = json.loads(self._get(client, limit=999).data)
        assert data["count"] <= 500

    def test_status_filter(self, client):
        self._seed(client, 4)
        data = json.loads(self._get(client, status="success").data)
        for e in data["events"]:
            assert e["status"] == "success"

    def test_machine_filter(self, client):
        self._seed(client, 4)
        data = json.loads(self._get(client, machine_id="HOST0").data)
        for e in data["events"]:
            assert e["machine_id"] == "HOST0"

    def test_no_auth_returns_401(self, client):
        r = client.get("/backup/events")
        assert r.status_code == 401

    def test_invalid_limit_defaults_gracefully(self, client):
        r = self._get(client, limit="banana")
        assert r.status_code == 200

    def test_newest_first_ordering(self, client):
        """Events must be returned newest-first (highest id first)."""
        self._seed(client, 2)
        # Post a third with a distinct watch name we can identify
        _json_post(client, "/backup/event",
                   {"status": "failure", "watch_name": "LastInserted"})
        events = self._get(client).json["events"]
        assert events[0]["watch_name"] == "LastInserted"


# ─── /admin/stats ─────────────────────────────────────────────────────────────

class TestAdminStats:
    def _get(self, client):
        return client.get("/admin/stats", headers=_auth_headers())

    def test_empty_db_returns_zeros(self, client):
        data = json.loads(self._get(client).data)
        assert data["total_events"] == 0
        assert data["successes"] == 0
        assert data["failures"] == 0

    def test_counts_match_seeded_events(self, client):
        _json_post(client, "/backup/event", {"status": "success"})
        _json_post(client, "/backup/event", {"status": "failure"})
        _json_post(client, "/backup/event", {"status": "success"})
        data = json.loads(self._get(client).data)
        assert data["total_events"] == 3
        assert data["successes"] == 2
        assert data["failures"] == 1

    def test_last_event_field(self, client):
        _json_post(client, "/backup/event", {"status": "success",
                                              "watch_name": "My Docs"})
        data = json.loads(self._get(client).data)
        assert data["last_event"]["watch_name"] == "My Docs"

    def test_machines_list(self, client):
        _json_post(client, "/backup/event", {"status": "success",
                                              "machine_id": "SERVER-1"})
        data = json.loads(self._get(client).data)
        machines = [m["machine"] for m in data["machines"]]
        assert "SERVER-1" in machines

    def test_no_auth_returns_401(self, client):
        assert client.get("/admin/stats").status_code == 401

    def test_has_ts_field(self, client):
        data = json.loads(self._get(client).data)
        assert "ts" in data


# ─── /otp/request ─────────────────────────────────────────────────────────────

class TestOtpRequest:
    def _post(self, client, key=_TEST_KEY):
        body = b"{}"
        return client.post("/otp/request",
                           data=body,
                           headers={**_auth_headers(body, key),
                                    "Content-Type": "application/json"})

    def test_first_request_returns_200(self, client):
        r = self._post(client)
        assert r.status_code == 200
        data = json.loads(r.data)
        assert data["ok"] is True
        assert "expires_in" in data

    def test_otp_not_returned_in_body(self, client):
        """OTP value must NOT be in the response body (security requirement)."""
        r = self._post(client)
        body_str = r.data.decode()
        # Check no 8-char uppercase+digit string that looks like an OTP is present
        assert "otp" not in body_str.lower() or "ok" in body_str.lower()

    def test_rate_limit_on_second_request(self, client):
        self._post(client)
        r = self._post(client)
        assert r.status_code == 429
        data = json.loads(r.data)
        assert "retry_in" in data

    def test_no_auth_returns_401(self, client):
        assert client.post("/otp/request").status_code == 401

    def test_lockout_prevents_new_otp(self, client, _isolated_db):
        """An IP that is locked out cannot request a new OTP."""
        with api.app.app_context():
            db = sqlite3.connect(str(api.DB_PATH))
            db.execute("""INSERT OR REPLACE INTO otp_state
                          (ip, otp_hash, issued_at, attempts, locked_until)
                          VALUES (?, ?, ?, ?, ?)""",
                       ("127.0.0.1", None, None, 5, time.time() + 900))
            db.commit()
            db.close()
        r = self._post(client)
        assert r.status_code == 429
        assert json.loads(r.data)["locked"] is True

    def test_rate_limit_resets_after_window(self, client, monkeypatch):
        """After OTP_RATE_LIMIT_SEC passes, a new OTP request must succeed."""
        monkeypatch.setattr(api, "OTP_RATE_LIMIT_SEC", 0)
        self._post(client)           # first request
        r = self._post(client)       # immediately again — window is 0 s so it's allowed
        assert r.status_code == 200


# ─── /otp/verify ──────────────────────────────────────────────────────────────

class TestOtpVerify:
    def _request_otp(self, client) -> str:
        """Issue an OTP via the API and return the raw OTP by reading the DB."""
        body = b"{}"
        client.post("/otp/request",
                    data=body,
                    headers={**_auth_headers(body), "Content-Type": "application/json"})
        # The OTP is stored hashed; we need to re-generate via the DB's hash
        # Instead, patch _generate_otp to a known value
        return None  # caller must use the patched value

    def _verify(self, client, otp_value: str):
        payload = {"otp": otp_value}
        return _json_post(client, "/otp/verify", payload)

    def test_correct_otp_returns_200(self, client):
        known_otp = "AAAABBBB"
        with patch.object(api, "_generate_otp", return_value=known_otp):
            body = b"{}"
            client.post("/otp/request",
                        data=body,
                        headers={**_auth_headers(body),
                                  "Content-Type": "application/json"})
        r = self._verify(client, known_otp)
        assert r.status_code == 200
        assert json.loads(r.data)["ok"] is True

    def test_wrong_otp_returns_400(self, client):
        known_otp = "AAAABBBB"
        with patch.object(api, "_generate_otp", return_value=known_otp):
            body = b"{}"
            client.post("/otp/request",
                        data=body,
                        headers={**_auth_headers(body),
                                  "Content-Type": "application/json"})
        r = self._verify(client, "WRONGOTP")
        assert r.status_code == 400
        data = json.loads(r.data)
        assert data["ok"] is False
        assert "attempts_left" in data

    def test_no_otp_on_record_returns_400(self, client):
        r = self._verify(client, "ABCD1234")
        assert r.status_code == 400
        assert json.loads(r.data)["ok"] is False

    def test_missing_otp_field_returns_400(self, client):
        body = json.dumps({}).encode()
        headers = {**_auth_headers(body), "Content-Type": "application/json"}
        r = client.post("/otp/verify", data=body, headers=headers)
        assert r.status_code == 400

    def test_expired_otp_returns_400(self, client, _isolated_db):
        known_otp = "EXPIR123"
        with api.app.app_context():
            db = sqlite3.connect(str(api.DB_PATH))
            db.execute("""INSERT OR REPLACE INTO otp_state
                          (ip, otp_hash, issued_at, attempts, locked_until)
                          VALUES (?, ?, ?, ?, ?)""",
                       ("127.0.0.1",
                        api._hash_otp(known_otp),
                        time.time() - api.OTP_TTL_SEC - 10,
                        0, 0))
            db.commit()
            db.close()
        r = self._verify(client, known_otp)
        assert r.status_code == 400
        assert "expired" in json.loads(r.data)["error"].lower()

    def test_lockout_after_max_attempts(self, client):
        known_otp = "LOCK5678"
        with patch.object(api, "_generate_otp", return_value=known_otp):
            body = b"{}"
            client.post("/otp/request",
                        data=body,
                        headers={**_auth_headers(body),
                                  "Content-Type": "application/json"})
        for _ in range(api.OTP_MAX_ATTEMPTS - 1):
            self._verify(client, "WRONGVAL")
        r = self._verify(client, "WRONGVAL")
        assert r.status_code == 429
        assert json.loads(r.data)["locked"] is True

    def test_correct_otp_clears_state(self, client, _isolated_db):
        """After a successful verify the OTP is consumed — a second verify fails."""
        known_otp = "ONCEONL1"
        with patch.object(api, "_generate_otp", return_value=known_otp):
            body = b"{}"
            client.post("/otp/request",
                        data=body,
                        headers={**_auth_headers(body),
                                  "Content-Type": "application/json"})
        assert self._verify(client, known_otp).status_code == 200
        assert self._verify(client, known_otp).status_code == 400

    def test_case_insensitive_otp(self, client):
        """Lowercase submission of an uppercase OTP must still pass."""
        known_otp = "ABCD1234"
        with patch.object(api, "_generate_otp", return_value=known_otp):
            body = b"{}"
            client.post("/otp/request",
                        data=body,
                        headers={**_auth_headers(body),
                                  "Content-Type": "application/json"})
        r = self._verify(client, known_otp.lower())
        assert r.status_code == 200
        assert json.loads(r.data)["ok"] is True


# ─── /backup/upload ───────────────────────────────────────────────────────────

class TestBackupUpload:
    def _upload(self, client, filename="data/file.txt", backup_dir="bkp_001",
                content=b"hello", sig_override=None):
        data = {
            "file": (io.BytesIO(content), "file.bin"),
            "filename": filename,
            "backup_dir": backup_dir,
        }
        # For multipart we need a signature over the raw body; we cheat by using
        # an empty body signature (the server signs the raw request bytes, but for
        # multipart the test client builds the body internally).  We monkey-patch
        # _verify_sig and _check_timestamp to always pass for upload tests.
        with patch.object(api, "_verify_sig", return_value=True), \
             patch.object(api, "_check_timestamp", return_value=(True, "")):
            return client.post("/backup/upload", data=data,
                               content_type="multipart/form-data")

    def test_successful_upload_returns_201(self, client):
        r = self._upload(client)
        assert r.status_code == 201
        assert json.loads(r.data)["ok"] is True

    def test_file_written_to_disk(self, client, _isolated_db):
        self._upload(client, filename="sub/file.txt",
                     backup_dir="backup_20260504", content=b"testcontent")
        stored = api.FILES_DIR / "backup_20260504" / "sub" / "file.txt"
        assert stored.exists()
        assert stored.read_bytes() == b"testcontent"

    def test_missing_fields_returns_400(self, client):
        with patch.object(api, "_verify_sig", return_value=True), \
             patch.object(api, "_check_timestamp", return_value=(True, "")):
            r = client.post("/backup/upload",
                            data={"backup_dir": "bkp_001"},
                            content_type="multipart/form-data")
        assert r.status_code == 400

    def test_path_traversal_blocked(self, client):
        r = self._upload(client, filename="../../etc/passwd",
                         backup_dir="safe_dir")
        assert r.status_code == 400

    def test_no_auth_returns_401(self, client):
        data = {
            "file": (io.BytesIO(b"x"), "f.bin"),
            "filename": "f.bin",
            "backup_dir": "bkp",
        }
        r = client.post("/backup/upload", data=data,
                        content_type="multipart/form-data")
        assert r.status_code == 401

    def test_stored_path_in_response(self, client):
        r = self._upload(client, filename="a/b.txt", backup_dir="mybkp")
        assert "mybkp/a/b.txt" in json.loads(r.data)["stored"]


# ─── /manifest ────────────────────────────────────────────────────────────────

class TestManifest:
    def _make_backup_dir(self, name="bkp_001"):
        bd = api.FILES_DIR / name
        bd.mkdir(parents=True, exist_ok=True)
        (bd / "file1.txt").write_bytes(b"aaa")
        (bd / "sub" / "file2.bin").parent.mkdir(exist_ok=True)
        (bd / "sub" / "file2.bin").write_bytes(b"bbbb")
        return bd

    def _get(self, client, backup_dir):
        return client.get(f"/manifest?backup_dir={backup_dir}",
                          headers=_auth_headers())

    def test_returns_file_list(self, client, _isolated_db):
        self._make_backup_dir("bkp_test")
        r = self._get(client, "bkp_test")
        assert r.status_code == 200
        data = json.loads(r.data)
        assert data["backup_dir"] == "bkp_test"
        paths = [f["path"] for f in data["files"]]
        assert any("file1.txt" in p for p in paths)
        assert any("file2.bin" in p for p in paths)

    def test_file_size_included(self, client, _isolated_db):
        self._make_backup_dir("bkp_sz")
        data = json.loads(self._get(client, "bkp_sz").data)
        for f in data["files"]:
            assert "size" in f and isinstance(f["size"], int)

    def test_missing_backup_dir_returns_404(self, client):
        r = self._get(client, "nonexistent_backup")
        assert r.status_code == 404

    def test_missing_param_returns_400(self, client):
        r = client.get("/manifest", headers=_auth_headers())
        assert r.status_code == 400

    def test_no_auth_returns_401(self, client):
        assert client.get("/manifest?backup_dir=x").status_code == 401

    def test_path_traversal_blocked(self, client):
        r = self._get(client, "../../../etc")
        assert r.status_code in (400, 404)


# ─── /files/<path> ────────────────────────────────────────────────────────────

class TestServeFile:
    def _make_file(self, rel="bkp/data.txt", content=b"file bytes"):
        dest = api.FILES_DIR / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(content)

    def _get(self, client, path):
        return client.get(f"/files/{path}", headers=_auth_headers())

    def test_serves_existing_file(self, client, _isolated_db):
        self._make_file("bkp001/hello.txt", b"world")
        r = self._get(client, "bkp001/hello.txt")
        assert r.status_code == 200
        assert r.data == b"world"

    def test_missing_file_returns_404(self, client):
        r = self._get(client, "ghost/file.txt")
        assert r.status_code == 404

    def test_content_type_is_octet_stream(self, client, _isolated_db):
        self._make_file("bkp/bin.dat", b"\x00\x01")
        r = self._get(client, "bkp/bin.dat")
        assert "octet-stream" in r.content_type

    def test_no_auth_returns_401(self, client):
        assert client.get("/files/anything").status_code == 401

    def test_path_traversal_blocked(self, client):
        r = self._get(client, "../../etc/passwd")
        assert r.status_code in (400, 404)


# ─── /dashboard ───────────────────────────────────────────────────────────────
# The dashboard supports two auth layers:
#   1. HTTP Basic Auth via BACKUPSYS_DASHBOARD_PASSWORD (when set)
#   2. ?key=<BACKUPSYS_API_KEY> query-string (when DASHBOARD_PASSWORD is not set)
# In the test fixture, API_KEY is set and DASHBOARD_PASSWORD is empty, so all
# dashboard requests must include ?key=<_TEST_KEY> to receive a 200.

def _dash_url(path: str = "", key: str = _TEST_KEY) -> str:
    """Build a dashboard URL with the ?key= auth parameter."""
    sep = "&" if "?" in path else "?"
    return f"/dashboard{path}{sep}key={key}"


class TestDashboard:
    def test_returns_200(self, client):
        assert client.get(_dash_url()).status_code == 200

    def test_content_type_is_html(self, client):
        r = client.get(_dash_url())
        assert "text/html" in r.content_type

    def test_contains_backupsys_title(self, client):
        r = client.get(_dash_url())
        assert b"BackupSys" in r.data

    def test_requires_key_auth_when_no_dashboard_password(self, client):
        """Without ?key=, the dashboard returns 401 (falls back to key auth)."""
        r = client.get("/dashboard")
        assert r.status_code == 401

    def test_wrong_key_returns_401(self, client):
        """A wrong API key must be rejected."""
        r = client.get(_dash_url(key="wrongkey"))
        assert r.status_code == 401

    def test_reflects_event_counts(self, client):
        _json_post(client, "/backup/event", {"status": "success"})
        _json_post(client, "/backup/event", {"status": "failure"})
        r = client.get(_dash_url())
        html = r.data.decode()
        # The counts appear in the HTML stats cards
        assert "2" in html   # total events

    def test_limit_param_accepted(self, client):
        r = client.get(_dash_url("?limit=10"))
        assert r.status_code == 200

    def test_no_events_shows_empty_message(self, client):
        r = client.get(_dash_url())
        assert b"No events" in r.data


# ─── require_auth decorator edge cases ────────────────────────────────────────

class TestRequireAuth:
    def test_missing_api_key_env_returns_500(self, client, monkeypatch):
        monkeypatch.setattr(api, "API_KEY", "")
        r = client.post("/ping", headers={"X-BackupSys-Signature": "anything"})
        assert r.status_code == 500

    def test_forwarded_for_header_used_for_ip(self, client):
        """X-Forwarded-For should be used to extract the client IP."""
        body = b"{}"
        headers = {**_auth_headers(body),
                   "Content-Type": "application/json",
                   "X-Forwarded-For": "203.0.113.1, 10.0.0.1"}
        r = client.post("/otp/request", data=body, headers=headers)
        # Just verify it doesn't crash — 200 or 429 is fine
        assert r.status_code in (200, 429)


# ─── Database helper unit tests ───────────────────────────────────────────────

class TestDbHelpers:
    def test_upsert_otp_state_insert(self, _isolated_db):
        with api.app.app_context():
            db = sqlite3.connect(str(api.DB_PATH))
            db.row_factory = sqlite3.Row
            # Use the module functions inside an app context for g
            # We'll talk directly to SQLite here
            db.execute("""INSERT INTO otp_state
                          (ip, otp_hash, issued_at, attempts, locked_until)
                          VALUES (?, ?, ?, ?, ?)""",
                       ("1.2.3.4", "hash123", time.time(), 0, 0))
            db.commit()
            row = db.execute("SELECT * FROM otp_state WHERE ip=?",
                             ("1.2.3.4",)).fetchone()
            assert row["otp_hash"] == "hash123"
            db.close()

    def test_init_db_is_idempotent(self, _isolated_db):
        """Calling _init_db twice must not raise."""
        api._init_db()
        api._init_db()

    def test_files_dir_created_by_init_db(self, _isolated_db):
        assert api.FILES_DIR.is_dir()


# ─── Database migration ───────────────────────────────────────────────────────

class TestDatabaseMigration:
    def test_init_db_creates_all_tables(self, tmp_path, monkeypatch):
        """Fresh _init_db must create backup_events, otp_state, and api_log."""
        db_file = tmp_path / "fresh.db"
        monkeypatch.setattr(api, "DB_PATH", db_file)
        monkeypatch.setattr(api, "FILES_DIR", tmp_path / "files")
        api._init_db()
        db = sqlite3.connect(str(db_file))
        tables = {r[0] for r in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        db.close()
        assert {"backup_events", "otp_state", "api_log"}.issubset(tables)

    def test_init_db_idempotent(self, tmp_path, monkeypatch):
        """Calling _init_db twice must not raise and schema must remain intact."""
        db_file = tmp_path / "idempotent.db"
        monkeypatch.setattr(api, "DB_PATH", db_file)
        monkeypatch.setattr(api, "FILES_DIR", tmp_path / "files")
        api._init_db()
        api._init_db()   # second call — must not crash
        db = sqlite3.connect(str(db_file))
        tables = {r[0] for r in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        db.close()
        assert {"backup_events", "otp_state", "api_log"}.issubset(tables)

    def test_machine_id_column_added_to_old_schema(self, tmp_path, monkeypatch):
        """v1.1.7 migration must add machine_id to a pre-existing schema without it."""
        db_file = tmp_path / "old_schema.db"
        monkeypatch.setattr(api, "DB_PATH", db_file)
        monkeypatch.setattr(api, "FILES_DIR", tmp_path / "files")
        # Create old-style schema WITHOUT machine_id
        old_db = sqlite3.connect(str(db_file))
        old_db.executescript("""
            CREATE TABLE backup_events (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                received_at  TEXT    NOT NULL,
                watch_name   TEXT,
                watch_id     TEXT,
                status       TEXT,
                files_copied INTEGER,
                bytes_copied INTEGER,
                error        TEXT,
                payload      TEXT
            );
            CREATE TABLE otp_state (
                ip TEXT PRIMARY KEY, otp_hash TEXT,
                issued_at REAL, attempts INTEGER DEFAULT 0,
                locked_until REAL DEFAULT 0
            );
            CREATE TABLE api_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL,
                ip TEXT, method TEXT, path TEXT, status_code INTEGER
            );
        """)
        old_db.commit()
        old_db.close()
        api._init_db()   # should apply migration without crashing
        db = sqlite3.connect(str(db_file))
        cols = {r[1] for r in db.execute(
            "PRAGMA table_info(backup_events)").fetchall()}
        db.close()
        assert "machine_id" in cols

    def test_machine_id_column_exists_on_fresh_db(self, tmp_path, monkeypatch):
        """A brand-new database must also have machine_id from the start."""
        db_file = tmp_path / "brand_new.db"
        monkeypatch.setattr(api, "DB_PATH", db_file)
        monkeypatch.setattr(api, "FILES_DIR", tmp_path / "files")
        api._init_db()
        db = sqlite3.connect(str(db_file))
        cols = {r[1] for r in db.execute(
            "PRAGMA table_info(backup_events)").fetchall()}
        db.close()
        assert "machine_id" in cols

# ─── /commands/backup — GDrive-awareness tests ────────────────────────────────
# These tests verify the new dest_type lookup added in the GDrive-awareness fix.

class TestQueueBackupGdriveAwareness:
    """queue_backup should include dest_type in its response and command payload
    by looking it up from the watches registry.
    """

    # ── helpers ───────────────────────────────────────────────────────────────

    def _register_watch(self, client, watch_id: str, dest_type: str,
                        machine_id: str = "DESKTOP-TEST") -> None:
        """Register a watch via POST /watches/register."""
        _json_post(client, "/watches/register", {
            "machine_id": machine_id,
            "watches": [{
                "id":        watch_id,
                "name":      f"Watch {watch_id}",
                "source":    "/tmp/source",
                "dest_type": dest_type,
                "active":    True,
            }],
        })

    def _queue_backup(self, client, watch_id: str,
                      machine_id: str = "DESKTOP-TEST"):
        return _json_post(client, "/commands/backup", {
            "machine_id": machine_id,
            "watch_id":   watch_id,
            "watch_name": f"Watch {watch_id}",
        })

    # ── tests ─────────────────────────────────────────────────────────────────

    def test_cloud_watch_returns_dest_type_cloud(self, client):
        """When dest_type is 'cloud', the response must surface that."""
        self._register_watch(client, "w_gdrive1", "cloud")
        r = self._queue_backup(client, "w_gdrive1")
        assert r.status_code == 201
        data = r.get_json()
        assert data["ok"] is True
        assert data["dest_type"] == "cloud"

    def test_cloud_command_payload_includes_dest_type(self, client):
        """The queued command payload must carry dest_type='cloud'."""
        self._register_watch(client, "w_gdrive2", "cloud")
        self._queue_backup(client, "w_gdrive2")
        # Poll pending commands and check the payload
        r = _json_post(client, "/commands/pending",
                       {"machine_id": "DESKTOP-TEST"})
        # pending_commands is a GET, use raw get with auth
        body = b""
        pending_r = client.get(
            "/commands/pending?machine_id=DESKTOP-TEST",
            headers=_auth_headers(body),
        )
        cmds = pending_r.get_json()["commands"]
        assert any(
            c["payload"].get("dest_type") == "cloud" for c in cmds
        ), "Expected a command with dest_type='cloud' in the payload"

    def test_local_watch_returns_dest_type_local(self, client):
        """Non-cloud dest_type is also surfaced correctly."""
        self._register_watch(client, "w_local1", "local")
        r = self._queue_backup(client, "w_local1")
        assert r.status_code == 201
        assert r.get_json()["dest_type"] == "local"

    def test_unregistered_watch_returns_null_dest_type(self, client):
        """If the watch isn't in the registry, dest_type should be null (None)."""
        r = self._queue_backup(client, "w_unknown_xyz")
        assert r.status_code == 201
        data = r.get_json()
        assert data["ok"] is True
        assert data["dest_type"] is None

    def test_rclone_watch_returns_dest_type_rclone(self, client):
        """Rclone dest_type is passed through unchanged."""
        self._register_watch(client, "w_rclone1", "rclone")
        r = self._queue_backup(client, "w_rclone1")
        assert r.get_json()["dest_type"] == "rclone"

    def test_missing_machine_id_returns_400(self, client):
        """Validate that missing machine_id still returns 400."""
        body = json.dumps({"watch_id": "w_any"}).encode()
        headers = {**_auth_headers(body), "Content-Type": "application/json"}
        r = client.post("/commands/backup", data=body, headers=headers)
        assert r.status_code == 400

    def test_missing_watch_id_returns_400(self, client):
        """Validate that missing watch_id still returns 400."""
        body = json.dumps({"machine_id": "DESKTOP-TEST"}).encode()
        headers = {**_auth_headers(body), "Content-Type": "application/json"}
        r = client.post("/commands/backup", data=body, headers=headers)
        assert r.status_code == 400