"""
tests/test_credential_store.py — Unit tests for credential_store.py.
Run:  pytest tests/test_credential_store.py
"""
import sys
from pathlib import Path
from unittest import mock
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import credential_store as cs


def _make_mock_kr():
    """Return a (MockKR, store_dict) pair backed by a simple dict."""
    store = {}
    class _MockKR:
        @staticmethod
        def get_password(service, username):
            return store.get((service, username))
        @staticmethod
        def set_password(service, username, password):
            store[(service, username)] = password
        @staticmethod
        def delete_password(service, username):
            store.pop((service, username), None)
    return _MockKR, store


@pytest.fixture(autouse=True)
def _inject_kr(monkeypatch):
    """Inject a mock keyring into credential_store for every test."""
    MockKR, store = _make_mock_kr()
    monkeypatch.setattr(cs, "_KEYRING_AVAILABLE", True, raising=False)
    # credential_store calls cs._kr.get_password etc., so create the attr
    monkeypatch.setattr(cs, "_kr", MockKR, raising=False)
    return MockKR, store


# ─── get_password / set_password ──────────────────────────────────────────────
class TestGetSetPassword:
    def test_set_then_get(self):
        cs.set_password("sftp", "myhost", "s3cr3t")
        assert cs.get_password("sftp", "myhost") == "s3cr3t"

    def test_get_missing_returns_fallback(self):
        assert cs.get_password("sftp", "no-such-host", fallback="fb") == "fb"

    def test_keyring_unavailable_returns_fallback(self, monkeypatch):
        monkeypatch.setattr(cs, "_KEYRING_AVAILABLE", False)
        assert cs.get_password("sftp", "host", fallback="cfg-pw") == "cfg-pw"

    def test_keyring_unavailable_set_returns_false(self, monkeypatch):
        monkeypatch.setattr(cs, "_KEYRING_AVAILABLE", False)
        assert cs.set_password("sftp", "host", "pw") is False

    def test_keyring_error_falls_back(self, monkeypatch):
        class _BrokenKR:
            @staticmethod
            def get_password(*_): raise RuntimeError("broken")
        monkeypatch.setattr(cs, "_kr", _BrokenKR)
        assert cs.get_password("sftp", "host", fallback="safe") == "safe"

    def test_set_returns_true_on_success(self):
        assert cs.set_password("ftp", "host", "pw") is True


# ─── delete_password ──────────────────────────────────────────────────────────
class TestDeletePassword:
    def test_delete_removes_entry(self):
        cs.set_password("ftp", "host", "pw")
        cs.delete_password("ftp", "host")
        assert cs.get_password("ftp", "host", fallback="gone") == "gone"

    def test_delete_nonexistent_returns_false(self, monkeypatch):
        class _FailDelete:
            @staticmethod
            def get_password(*_): return None
            @staticmethod
            def set_password(*_): pass
            @staticmethod
            def delete_password(*_): raise Exception("not found")
        monkeypatch.setattr(cs, "_kr", _FailDelete)
        assert cs.delete_password("ftp", "nope") is False

    def test_delete_returns_true_on_success(self):
        cs.set_password("smb", "nas", "pw")
        assert cs.delete_password("smb", "nas") is True


# ─── Convenience helpers ──────────────────────────────────────────────────────
class TestConvenienceHelpers:
    def test_sftp_helper_roundtrip(self):
        cfg = {"host": "sftp.example.com", "password": "config-pw"}
        cs.set_sftp_password(cfg, "keyring-pw")
        assert cs.get_sftp_password(cfg) == "keyring-pw"

    def test_ftp_helper_fallback_to_config(self, monkeypatch):
        monkeypatch.setattr(cs, "_KEYRING_AVAILABLE", False)
        cfg = {"host": "ftp.example.com", "password": "config-pw"}
        assert cs.get_ftp_password(cfg) == "config-pw"

    def test_smb_helper_roundtrip(self):
        cfg = {"server": "nas", "password": "nas-pw"}
        cs.set_smb_password(cfg, "keyring-smb")
        assert cs.get_smb_password(cfg) == "keyring-smb"

    def test_smtp_env_var_priority(self, monkeypatch):
        monkeypatch.setenv("BACKUPSYS_EMAIL_PASSWORD", "env-pw")
        assert cs.get_smtp_password({"password": "config-pw"}) == "env-pw"

    def test_smtp_falls_back_to_config(self, monkeypatch):
        monkeypatch.delenv("BACKUPSYS_EMAIL_PASSWORD", raising=False)
        monkeypatch.setattr(cs, "_KEYRING_AVAILABLE", False)
        assert cs.get_smtp_password({"password": "config-pw"}) == "config-pw"

    def test_smtp_keyring_takes_priority_over_config(self, monkeypatch):
        monkeypatch.delenv("BACKUPSYS_EMAIL_PASSWORD", raising=False)
        cs.set_smtp_password("keyring-smtp")
        assert cs.get_smtp_password({"password": "config-pw"}) == "keyring-smtp"
