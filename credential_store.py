"""
credential_store.py — Secure credential storage for BackupSys.

Passwords for SFTP, FTP, SMB, and SMTP are sensitive.  Storing them as
plaintext in config.json is convenient but risky — any process that reads
the file (or any backup of it) gets all credentials.

This module wraps the ``keyring`` library so passwords are stored in the
OS-native credential vault:
  - Windows  : Windows Credential Manager
  - macOS    : macOS Keychain
  - Linux    : SecretService (GNOME Keyring / KWallet)

Fallback behaviour
------------------
If ``keyring`` is not installed, or the OS vault is unavailable, every call
transparently falls back to the value stored in config.json.  Nothing breaks
— the user simply doesn't get the security upgrade until they install keyring.

Usage
-----
    from credential_store import get_password, set_password, delete_password

    # Store a password (call from Settings UI when user saves credentials)
    set_password("sftp", "myserver", "mypassword")

    # Retrieve — returns the keyring value if available, else falls back to
    # the plain value from config (passed as 'fallback')
    pw = get_password("sftp", "myserver", fallback=cfg["dest_sftp"]["password"])

    # Remove when a destination is deleted
    delete_password("sftp", "myserver")

Service name format
-------------------
Keys are stored as:  BackupSys/<service>/<identifier>

  service     : "sftp" | "ftp" | "smb" | "smtp" | "gdrive"
  identifier  : host/server name, or "default" for SMTP

This means passwords for different servers are stored under separate entries
so rotating one does not affect others.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

_KEYRING_APP = "BackupSys"

# ── Try to import keyring ──────────────────────────────────────────────────────

try:
    import keyring as _kr
    import keyring.errors as _kr_errors
    _KEYRING_AVAILABLE = True
    logger.debug("credential_store: keyring available (%s)", _kr.get_keyring().__class__.__name__)
except ImportError:
    _KEYRING_AVAILABLE = False
    _kr = None  # exposed as None so tests can monkeypatch it cleanly
    logger.debug("credential_store: keyring not installed — falling back to config.json passwords")


# ── Public API ─────────────────────────────────────────────────────────────────

def is_available() -> bool:
    """Return True if the OS keyring backend is usable."""
    return _KEYRING_AVAILABLE


def service_name(service: str) -> str:
    """Return the full keyring service string for *service*."""
    return f"{_KEYRING_APP}/{service}"


def get_password(service: str, identifier: str, fallback: str = "") -> str:
    """
    Return the password for *service* / *identifier*.

    If keyring is unavailable or the entry does not exist, returns *fallback*
    (which should be the value already in config.json so callers never break).
    """
    if not _KEYRING_AVAILABLE:
        return fallback
    try:
        stored = _kr.get_password(service_name(service), identifier)
        return stored if stored is not None else fallback
    except Exception as exc:
        logger.warning("credential_store.get_password(%s, %s) failed: %s", service, identifier, exc)
        return fallback


def set_password(service: str, identifier: str, password: str) -> bool:
    """
    Store *password* in the OS keyring under *service* / *identifier*.

    Returns True on success, False on failure (e.g. no keyring backend).
    Callers should still write an empty string to config.json on success so
    the config file does not contain the real password.
    """
    if not _KEYRING_AVAILABLE:
        logger.debug("credential_store.set_password: keyring unavailable — skipping")
        return False
    try:
        _kr.set_password(service_name(service), identifier, password)
        logger.info("credential_store: stored password for %s/%s in OS keyring", service, identifier)
        return True
    except Exception as exc:
        logger.warning("credential_store.set_password(%s, %s) failed: %s", service, identifier, exc)
        return False


def delete_password(service: str, identifier: str) -> bool:
    """
    Delete the keyring entry for *service* / *identifier*.

    Returns True if deleted, False if not found or on error.
    """
    if not _KEYRING_AVAILABLE:
        return False
    try:
        _kr.delete_password(service_name(service), identifier)
        logger.info("credential_store: deleted keyring entry for %s/%s", service, identifier)
        return True
    except Exception as exc:
        logger.debug("credential_store.delete_password(%s, %s): %s", service, identifier, exc)
        return False


# ── Convenience helpers for each destination type ─────────────────────────────

def _host_key(cfg_section: dict, key: str = "host") -> str:
    """Return a stable identifier string from a dest config dict."""
    return (cfg_section.get(key) or cfg_section.get("server") or "default").strip()


def get_sftp_password(sftp_cfg: dict) -> str:
    """Return SFTP password, preferring keyring over config.json."""
    return get_password("sftp", _host_key(sftp_cfg), fallback=sftp_cfg.get("password", ""))


def set_sftp_password(sftp_cfg: dict, password: str) -> bool:
    return set_password("sftp", _host_key(sftp_cfg), password)


def get_ftp_password(ftp_cfg: dict) -> str:
    """Return FTP/FTPS password, preferring keyring over config.json."""
    return get_password("ftp", _host_key(ftp_cfg), fallback=ftp_cfg.get("password", ""))


def set_ftp_password(ftp_cfg: dict, password: str) -> bool:
    return set_password("ftp", _host_key(ftp_cfg), password)


def get_smb_password(smb_cfg: dict) -> str:
    """Return SMB password, preferring keyring over config.json."""
    return get_password("smb", _host_key(smb_cfg, key="server"), fallback=smb_cfg.get("password", ""))


def set_smb_password(smb_cfg: dict, password: str) -> bool:
    return set_password("smb", _host_key(smb_cfg, key="server"), password)


def get_webdav_password(webdav_cfg: dict) -> str:
    """Return WebDAV password, preferring keyring over config.json."""
    return get_password("webdav", _host_key(webdav_cfg, key="url"), fallback=webdav_cfg.get("password", ""))


def set_webdav_password(webdav_cfg: dict, password: str) -> bool:
    return set_password("webdav", _host_key(webdav_cfg, key="url"), password)


def get_smtp_password(email_cfg: dict) -> str:
    """
    Return SMTP password.  Priority order:
      1. OS keyring
      2. BACKUPSYS_EMAIL_PASSWORD environment variable
      3. email_config.password in config.json (plaintext fallback)
    """
    import os
    env_pw = os.environ.get("BACKUPSYS_EMAIL_PASSWORD", "").strip()
    if env_pw:
        return env_pw
    return get_password("smtp", "default", fallback=email_cfg.get("password", ""))


def set_smtp_password(password: str) -> bool:
    return set_password("smtp", "default", password)
