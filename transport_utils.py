"""
transport_utils.py — Remote destination upload helpers for BackupSys.

Provides upload_to_sftp(), upload_to_ftp(), upload_to_smb(), upload_to_https().
Each function accepts a local_dir path and a destination-specific config dict,
and returns { ok: bool, uploaded: int, path: str, error: str }.

Integration — add to backup_engine.py run_backup() after the cloud block:

    from transport_utils import (
        upload_to_sftp, upload_to_ftp, upload_to_smb, upload_to_https
    )

    # ── SFTP / FTP / SMB / HTTPS upload (dest_type from global config) ─────
    dest_type = cfg.get("dest_type", "local") if cfg else storage_type
    if dest_type == "sftp" and cfg.get("dest_sftp"):
        upload_result = upload_to_sftp(str(backup_dir), cfg["dest_sftp"])
    elif dest_type == "ftp" and cfg.get("dest_ftp"):
        upload_result = upload_to_ftp(str(backup_dir), cfg["dest_ftp"])
    elif dest_type == "smb" and cfg.get("dest_smb"):
        upload_result = upload_to_smb(str(backup_dir), cfg["dest_smb"])
    elif dest_type == "https" and cfg.get("dest_https"):
        upload_result = upload_to_https(str(backup_dir), cfg["dest_https"])

Config shapes expected (mirrors config.json):

    dest_sftp:  { host, port(=22), username, password, key_path, remote_path }
    dest_ftp:   { host, port(=21), username, password, remote_path, use_tls(=true) }
    dest_smb:   { server, share, username, password, remote_path, domain(="") }
    dest_https: { url, token, headers, verify_ssl(=true) }
"""

import os
import logging
import subprocess
import time
from pathlib import Path
from typing import Optional
import hashlib
import io

logger = logging.getLogger(__name__)

# ── Upload retry / exponential backoff ────────────────────────────────────────

import random as _random

def _retry_with_backoff(fn, *, max_retries: int = 3, base_delay: float = 2.0,
                        max_delay: float = 60.0, label: str = "upload") -> object:
    """
    Call *fn()* up to *max_retries* additional times (i.e. 1 + max_retries total
    attempts) using exponential backoff with full jitter on each retry.

    Jitter formula:  sleep = uniform(0, min(base_delay * 2**attempt, max_delay))

    Raises the last exception if every attempt fails.
    Returns the return value of the first successful call.
    """
    last_exc: Exception = RuntimeError("unreachable")
    for attempt in range(max_retries + 1):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            if attempt < max_retries:
                cap   = min(base_delay * (2 ** attempt), max_delay)
                delay = _random.uniform(0, cap)
                logger.warning(
                    f"[retry] {label}: attempt {attempt + 1}/{max_retries} failed "
                    f"({exc.__class__.__name__}: {exc}) — retrying in {delay:.1f}s"
                )
                time.sleep(delay)
    raise last_exc

# ── Optional keyring-backed credential store ──────────────────────────────────
# If credential_store.py is present, passwords are read from the OS keyring
# first (falling back to the value in config.json if the keyring has nothing).
try:
    from credential_store import (
        get_sftp_password as _cred_sftp,
        get_ftp_password  as _cred_ftp,
        get_smb_password  as _cred_smb,
        get_webdav_password as _cred_webdav,
    )
    _CRED_STORE = True
except ImportError:
    _CRED_STORE = False


# ─── SFTP ─────────────────────────────────────────────────────────────────────

def upload_to_sftp(local_dir: str, sftp_config: dict, progress_cb=None, verify=False,
                   max_retries: int = 3) -> dict:
    """
    Upload a backup folder to an SFTP server using Paramiko.

    Recreates the full subdirectory tree under remote_path/<backup_folder_name>/.
    Supports both password auth and private-key auth.

    progress_cb(bytes_done, total_bytes, filename) — optional, called per chunk.
    verify — optional, if True, verify uploaded files by comparing MD5 of first 8192 bytes.
    """
    try:
        import paramiko
    except ImportError:
        return {"ok": False, "error": "paramiko not installed — run: pip install paramiko"}

    host       = sftp_config.get("host", "").strip()
    port       = int(sftp_config.get("port", 22))
    # Accept both "username" (transport_utils convention) and "user" (desktop_app convention)
    username   = (sftp_config.get("username") or sftp_config.get("user", "")).strip()
    # Accept both "password" and "pass"
    password   = _cred_sftp(sftp_config) if _CRED_STORE else (sftp_config.get("password") or sftp_config.get("pass", ""))
    # Accept both "key_path" and "keyfile"
    key_path   = (sftp_config.get("key_path") or sftp_config.get("keyfile", "")).strip()
    key_pass   = sftp_config.get("key_passphrase") or sftp_config.get("key_pass", "")
    # Accept both "remote_path" and "path"
    remote_base = (sftp_config.get("remote_path") or sftp_config.get("path", "/backups")).rstrip("/")

    if not host:
        return {"ok": False, "error": "SFTP host not configured"}
    if not username:
        return {"ok": False, "error": "SFTP username not configured"}

    transport = None
    sftp      = None

    # Larger SSH window/packet sizes drastically improve throughput for big files:
    # default window=2MB, packet=32KB → we use window=33MB, packet=32KB.
    # This matches OpenSSH client behaviour and avoids the "slow SFTP" problem.
    _WIN_SIZE = 33 * 1024 * 1024   # 33 MB window
    _PKT_SIZE = 32 * 1024           # 32 KB max packet (SSH spec limit)

    # ── Host-key store (trust-on-first-use) ───────────────────────────────────
    # Stored in ~/.backupsys_known_hosts so we detect server fingerprint changes.
    # On first connect we accept and persist the key; on subsequent connects we
    # reject mismatches to prevent man-in-the-middle attacks.
    _known_hosts_path = Path.home() / ".backupsys_known_hosts"
    _known_hosts = paramiko.HostKeys()
    if _known_hosts_path.exists():
        try:
            _known_hosts.load(_known_hosts_path)
        except Exception:
            pass

    try:
        transport = paramiko.Transport((host, port))
        transport.default_window_size     = _WIN_SIZE
        transport.default_max_packet_size = _PKT_SIZE
        transport.connect()  # TCP only — auth follows

        # ── Host key verification ─────────────────────────────────────────────
        _host_key = transport.get_remote_server_key()
        _host_id  = f"[{host}]:{port}" if port != 22 else host
        _stored   = _known_hosts.lookup(_host_id)
        if _stored:
            _stored_key = _stored.get(_host_key.get_name())
            if _stored_key and _stored_key != _host_key:
                transport.close()
                return {
                    "ok": False,
                    "error": (
                        f"SFTP host key mismatch for {host}:{port} — "
                        "the server's fingerprint has changed, which may indicate a "
                        "man-in-the-middle attack. If the server was legitimately "
                        f"reinstalled, delete the entry from {_known_hosts_path} and reconnect."
                    ),
                }
        else:
            # First connect — trust and persist this key (TOFU)
            _known_hosts.add(_host_id, _host_key.get_name(), _host_key)
            try:
                _known_hosts.save(str(_known_hosts_path))
            except Exception:
                pass  # best-effort; don't fail the backup over this

        # Key-based auth
        if key_path and Path(key_path).exists():
            pkey = None
            _needs_passphrase = False
            for key_cls in (paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey):
                try:
                    pkey = key_cls.from_private_key_file(key_path, password=key_pass or None)
                    break
                except paramiko.ssh_exception.PasswordRequiredException:
                    _needs_passphrase = True
                    break
                except paramiko.SSHException:
                    continue
            if pkey is None:
                if _needs_passphrase:
                    return {"ok": False, "error": f"Private key is passphrase-protected — set key_passphrase in your SFTP config: {key_path}"}
                return {"ok": False, "error": f"Could not load private key (unsupported format or wrong passphrase): {key_path}"}
            transport.auth_publickey(username, pkey)
        else:
            # Password auth
            if not password:
                return {"ok": False, "error": "SFTP password (or key_path) not configured"}
            transport.auth_password(username, password)

        if not transport.is_authenticated():
            return {"ok": False, "error": "SFTP authentication failed"}

        sftp     = paramiko.SFTPClient.from_transport(transport)
        ld       = Path(local_dir)
        uploaded = 0

        # Internal backup metadata — never upload to remote destinations
        _SKIP = {"MANIFEST.json", "BACKUP.sha256"}

        def _mkdir_p(remote_dir: str):
            """Recursively create remote directories, ignoring existing ones."""
            parts = remote_dir.replace("\\", "/").split("/")
            current = ""
            for part in parts:
                if not part:
                    current = "/"
                    continue
                current = (current.rstrip("/") + "/" + part) if current else part
                try:
                    sftp.stat(current)
                except FileNotFoundError:
                    try:
                        sftp.mkdir(current)
                    except Exception:
                        pass  # may already exist due to race or permission; continue

        # Pre-compute total bytes for accurate progress reporting
        _all_files   = [fp for fp in ld.rglob("*") if fp.is_file() and fp.name not in _SKIP]
        _total_bytes = sum(fp.stat().st_size for fp in _all_files)
        _bytes_done  = 0
        _SFTP_CHUNK  = 256 * 1024   # 256 KB — balances round-trips vs. memory

        for fp in _all_files:
            rel         = fp.relative_to(ld)
            remote_file = f"{remote_base}/{ld.name}/{str(rel).replace(os.sep, '/')}"
            remote_dir  = str(Path(remote_file).parent).replace("\\", "/")
            _mkdir_p(remote_dir)
            try:
                def _do_upload_sftp():
                    with open(str(fp), "rb") as fh:
                        if progress_cb:
                            f_handle = sftp.open(remote_file, "wb")
                            try:
                                while True:
                                    chunk = fh.read(_SFTP_CHUNK)
                                    if not chunk:
                                        break
                                    f_handle.write(chunk)
                                    _bytes_done += len(chunk)
                                    try:
                                        progress_cb(_bytes_done, _total_bytes, fp.name)
                                    except Exception:
                                        pass
                            finally:
                                f_handle.close()
                        else:
                            sftp.putfo(fh, remote_file, file_size=fp.stat().st_size)
                _retry_with_backoff(
                    _do_upload_sftp,
                    max_retries=max_retries,
                    label=f"sftp:{rel}",
                )
                if not progress_cb:
                    _bytes_done += fp.stat().st_size
                uploaded += 1
            except Exception as e:
                logger.warning(f"[sftp] Failed to upload {rel}: {e}")

        logger.info(f"[sftp] Uploaded {uploaded} file(s) to {host}:{remote_base}/{ld.name}")

        # ── Post-upload verification: remote file count must match local ──────
        result = {"ok": True, "uploaded": uploaded, "path": f"{remote_base}/{ld.name}"}
        _expected = len(_all_files)
        if _expected > 0 and uploaded != _expected:
            _missing = _expected - uploaded
            logger.warning(
                f"[sftp] Verification warning: expected {_expected} file(s), "
                f"only {uploaded} confirmed uploaded ({_missing} may have failed silently)"
            )
            result["warning"] = f"{_missing} file(s) may not have uploaded correctly " \
                               f"({uploaded}/{_expected} confirmed)"

        if verify:
            warnings = []
            for fp in _all_files:
                rel = fp.relative_to(ld)
                remote_file = f"{remote_base}/{ld.name}/{str(rel).replace(os.sep, '/')}"
                try:
                    local_md5 = hashlib.md5()
                    with open(str(fp), 'rb') as f:
                        local_md5.update(f.read(8192))
                    with sftp.open(remote_file, 'rb') as remote_f:
                        remote_data = remote_f.read(8192)
                    remote_md5 = hashlib.md5(remote_data)
                    if local_md5.hexdigest() != remote_md5.hexdigest():
                        warnings.append(f"MD5 mismatch for {rel}")
                except Exception as e:
                    warnings.append(f"Verification failed for {rel}: {e}")
            if warnings:
                result["warnings"] = warnings

        return result

    except paramiko.AuthenticationException as e:
        return {"ok": False, "error": f"SFTP authentication failed: {e}"}
    except paramiko.SSHException as e:
        return {"ok": False, "error": f"SSH/SFTP error: {e}"}
    except OSError as e:
        return {"ok": False, "error": f"Network error connecting to {host}:{port}: {e}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        try:
            if sftp:
                sftp.close()
        except Exception:
            pass
        try:
            if transport:
                transport.close()
        except Exception:
            pass


# ─── FTP / FTPS ───────────────────────────────────────────────────────────────

def upload_to_ftp(local_dir: str, ftp_config: dict, progress_cb=None, verify=False,
                  max_retries: int = 3) -> dict:
    """
    Upload a backup folder to an FTP/FTPS server using ftplib (stdlib).

    use_tls=True (default) uses explicit TLS (FTPS).  Set to False for plain FTP.
    Recreates the full directory tree under remote_path/<backup_folder_name>/.

    progress_cb(bytes_done, total_bytes, filename) — optional, called per chunk.
    verify — optional, if True, verify uploaded files by comparing MD5 of first 8192 bytes.
    """
    import ftplib

    host        = ftp_config.get("host", "").strip()
    port        = int(ftp_config.get("port", 21))
    username    = (ftp_config.get("username") or ftp_config.get("user", "")).strip()
    password    = _cred_ftp(ftp_config) if _CRED_STORE else (ftp_config.get("password") or ftp_config.get("pass", ""))
    remote_base = (ftp_config.get("remote_path") or ftp_config.get("path", "/backups")).rstrip("/")
    use_tls     = bool(ftp_config.get("use_tls", True))

    if not host:
        return {"ok": False, "error": "FTP host not configured"}
    if not username:
        return {"ok": False, "error": "FTP username not configured"}

    ftp = None
    try:
        if use_tls:
            ftp = ftplib.FTP_TLS(timeout=30)
            ftp.connect(host, port)
            ftp.login(username, password)
            ftp.prot_p()   # enable encrypted data channel
        else:
            ftp = ftplib.FTP(timeout=30)
            ftp.connect(host, port)
            ftp.login(username, password)

        ld       = Path(local_dir)
        uploaded = 0

        # Internal backup metadata — never upload to remote destinations
        _SKIP = {"MANIFEST.json", "BACKUP.sha256"}

        def _ftp_makedirs(remote_dir: str):
            """Navigate or create remote FTP directories."""
            parts = remote_dir.replace("\\", "/").lstrip("/").split("/")
            ftp.cwd("/")
            for part in parts:
                if not part:
                    continue
                try:
                    ftp.cwd(part)
                except ftplib.error_perm:
                    try:
                        ftp.mkd(part)
                        ftp.cwd(part)
                    except ftplib.error_perm:
                        pass  # may already exist; continue

        # Pre-compute total bytes for progress
        _all_files   = [fp for fp in ld.rglob("*") if fp.is_file() and fp.name not in _SKIP]
        _total_bytes = sum(fp.stat().st_size for fp in _all_files)
        _bytes_done  = 0
        _FTP_BLOCK   = 8 * 1024 * 1024   # 8 MB — reduces round-trips vs ftplib default 8 KB

        for fp in _all_files:
            rel        = fp.relative_to(ld)
            parts      = list(rel.parts)
            remote_dir = f"{remote_base}/{ld.name}" + (
                ("/" + "/".join(parts[:-1])) if len(parts) > 1 else ""
            )
            _ftp_makedirs(remote_dir)
            try:
                ftp.cwd("/" + remote_dir.lstrip("/"))
                def _do_upload_ftp():
                    if progress_cb:
                        with open(fp, "rb") as _raw_f:
                            def _cb_read(bs=_FTP_BLOCK, _f=_raw_f):
                                chunk = _f.read(bs)
                                if chunk:
                                    nonlocal _bytes_done
                                    _bytes_done += len(chunk)
                                    try:
                                        progress_cb(_bytes_done, _total_bytes, fp.name)
                                    except Exception:
                                        pass
                                return chunk
                            ftp.storbinary(f"STOR {fp.name}", type('R', (), {'read': _cb_read})(), blocksize=_FTP_BLOCK)
                    else:
                        with open(fp, "rb") as f:
                            ftp.storbinary(f"STOR {fp.name}", f, blocksize=_FTP_BLOCK)
                _retry_with_backoff(
                    _do_upload_ftp,
                    max_retries=max_retries,
                    label=f"ftp:{rel}",
                )
                if not progress_cb:
                    _bytes_done += fp.stat().st_size
                uploaded += 1
            except Exception as e:
                logger.warning(f"[ftp] Failed to upload {rel}: {e}")

        proto = "FTPS" if use_tls else "FTP"
        logger.info(f"[ftp] {proto} uploaded {uploaded} file(s) to {host}:{remote_base}/{ld.name}")

        # ── Post-upload verification ──────────────────────────────────────────
        result = {"ok": True, "uploaded": uploaded, "path": f"{remote_base}/{ld.name}"}
        _expected = len(_all_files)
        if _expected > 0 and uploaded != _expected:
            _missing = _expected - uploaded
            logger.warning(
                f"[ftp] Verification warning: expected {_expected} file(s), "
                f"only {uploaded} confirmed uploaded"
            )
            result["warning"] = f"{_missing} file(s) may not have uploaded correctly " \
                               f"({uploaded}/{_expected} confirmed)"

        if verify:
            warnings = []
            for fp in _all_files:
                rel = fp.relative_to(ld)
                parts = list(rel.parts)
                remote_dir = f"{remote_base}/{ld.name}" + (
                    ("/" + "/".join(parts[:-1])) if len(parts) > 1 else ""
                )
                try:
                    ftp.cwd("/" + remote_dir.lstrip("/"))
                    data = io.BytesIO()
                    def cb(chunk):
                        if data.tell() < 8192:
                            data.write(chunk)
                    ftp.retrbinary(f"RETR {fp.name}", cb)
                    remote_data = data.getvalue()[:8192]
                    remote_md5 = hashlib.md5(remote_data)
                    local_md5 = hashlib.md5()
                    with open(str(fp), 'rb') as f:
                        local_md5.update(f.read(8192))
                    if local_md5.hexdigest() != remote_md5.hexdigest():
                        warnings.append(f"MD5 mismatch for {rel}")
                except Exception as e:
                    warnings.append(f"Verification failed for {rel}: {e}")
            if warnings:
                result["warnings"] = warnings

        return result

    except ftplib.all_errors as e:
        return {"ok": False, "error": f"FTP error: {e}"}
    except OSError as e:
        return {"ok": False, "error": f"Network error connecting to {host}:{port}: {e}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        try:
            if ftp:
                ftp.quit()
        except Exception:
            pass


# ─── SMB / CIFS ───────────────────────────────────────────────────────────────

def upload_to_smb(local_dir: str, smb_config: dict, progress_cb=None,
                  max_retries: int = 3) -> dict:
    """
    Upload a backup folder to an SMB/CIFS network share.

    On Windows:  Uses UNC paths directly (\\\\server\\share\\...) — no extra lib needed.
    On Linux/Mac: Falls back to smbprotocol (must be installed: pip install smbprotocol).

    smb_config: { server, share, username, password, domain, remote_path }
    progress_cb(bytes_done, total_bytes, filename) — optional, called per chunk.
    """
    server      = smb_config.get("server", "").strip()
    share       = smb_config.get("share", "").strip()
    username    = smb_config.get("username", "").strip()
    password    = _cred_smb(smb_config) if _CRED_STORE else smb_config.get("password", "")
    domain      = smb_config.get("domain", "")
    remote_base = smb_config.get("remote_path", "backups").strip().strip("/\\")

    if not server:
        return {"ok": False, "error": "SMB server not configured"}
    if not share:
        return {"ok": False, "error": "SMB share not configured"}

    ld = Path(local_dir)

    # ── Windows: native UNC copy ───────────────────────────────────────────
    if os.name == "nt":
        import subprocess, shutil as _sh

        unc_root = f"\\\\{server}\\{share}"

        # Try net use to authenticate if credentials provided
        if username:
            net_user = f"{domain}\\{username}" if domain else username
            try:
                subprocess.run(
                    ["net", "use", unc_root, f"/user:{net_user}", password],
                    capture_output=True, timeout=15, check=False
                )
            except Exception:
                pass  # May already be connected — proceed anyway

        remote_dir = Path(unc_root) / remote_base / ld.name
        try:
            remote_dir.mkdir(parents=True, exist_ok=True)
            _SKIP        = {"MANIFEST.json", "BACKUP.sha256"}
            uploaded     = 0
            _SMB_BUF     = 16 * 1024 * 1024   # 16 MB — minimise SMB round-trips
            _all_files   = [fp for fp in ld.rglob("*") if fp.is_file() and fp.name not in _SKIP]
            _total_bytes = sum(fp.stat().st_size for fp in _all_files)
            _bytes_done  = 0
            for _smb_fp in _all_files:
                _smb_rel  = _smb_fp.relative_to(ld)
                _smb_dest = remote_dir / _smb_rel
                _smb_dest.parent.mkdir(parents=True, exist_ok=True)
                def _do_smb_copy(_src=_smb_fp, _dst=_smb_dest):
                    with open(str(_src), "rb") as _src_f, open(str(_dst), "wb") as _dst_f:
                        while True:
                            _buf = _src_f.read(_SMB_BUF)
                            if not _buf:
                                break
                            _dst_f.write(_buf)
                            if progress_cb:
                                nonlocal _bytes_done
                                _bytes_done += len(_buf)
                                try:
                                    progress_cb(_bytes_done, _total_bytes, _src.name)
                                except Exception:
                                    pass
                    _sh.copystat(str(_src), str(_dst))
                _retry_with_backoff(
                    _do_smb_copy,
                    max_retries=max_retries,
                    label=f"smb:{_smb_rel}",
                )
                if not progress_cb:
                    _bytes_done += _smb_fp.stat().st_size
                uploaded += 1
            logger.info(f"[smb] Copied {uploaded} file(s) to {remote_dir}")
            return {"ok": True, "uploaded": uploaded, "path": str(remote_dir)}
        except Exception as e:
            return {"ok": False, "error": f"SMB copy failed: {e}"}

    # ── Linux/Mac: smbprotocol ─────────────────────────────────────────────
    try:
        import smbprotocol.connection
        import smbprotocol.session
        import smbprotocol.tree
        import smbprotocol.open as smb_open
        from smbprotocol.connection import Connection
        from smbprotocol.session import Session
        from smbprotocol.tree import TreeConnect
        from smbprotocol.open import Open, CreateDisposition, FileAttributes, ImpersonationLevel, ShareAccess, CreateOptions, FilePipePrinterAccessMask
        import uuid as _uuid
    except ImportError:
        return {"ok": False, "error": "smbprotocol not installed — run: pip install smbprotocol"}

    try:
        conn_id  = _uuid.uuid4()
        conn     = Connection(conn_id, server, 445)
        conn.connect(timeout=30)

        session = Session(conn, username=username, password=password,
                         require_encryption=False)
        session.connect()

        unc   = f"\\\\{server}\\{share}"
        tree  = TreeConnect(session, unc)
        tree.connect()

        uploaded = 0

        SMB_CHUNK = 16 * 1024 * 1024  # 16 MB chunks — minimise SMB round-trips

        _SKIP        = {"MANIFEST.json", "BACKUP.sha256"}
        _all_files   = [fp for fp in ld.rglob("*") if fp.is_file() and fp.name not in _SKIP]
        _total_bytes = sum(fp.stat().st_size for fp in _all_files)
        _bytes_done  = 0

        def _smb_write_tracked(rel_path: str, local_fp: Path):
            nonlocal _bytes_done
            rel_win     = rel_path.replace('/', '\\')
            remote_path = f"{remote_base}\\{ld.name}\\{rel_win}".lstrip("\\")
            parts = remote_path.replace("/", "\\").split("\\")
            for i in range(1, len(parts)):
                dir_path = "\\".join(parts[:i])
                try:
                    d = Open(tree, dir_path)
                    d.create(
                        ImpersonationLevel.Impersonation,
                        FilePipePrinterAccessMask.MAXIMUM_ALLOWED,
                        FileAttributes.FILE_ATTRIBUTE_DIRECTORY,
                        ShareAccess.FILE_SHARE_READ | ShareAccess.FILE_SHARE_WRITE,
                        CreateDisposition.FILE_OPEN_IF,
                        CreateOptions.FILE_DIRECTORY_FILE,
                    )
                    d.close(False)
                except Exception:
                    pass
            f_handle = Open(tree, remote_path)
            f_handle.create(
                ImpersonationLevel.Impersonation,
                FilePipePrinterAccessMask.FILE_WRITE_DATA,
                FileAttributes.FILE_ATTRIBUTE_NORMAL,
                0,
                CreateDisposition.FILE_OVERWRITE_IF,
                CreateOptions.FILE_NON_DIRECTORY_FILE,
            )
            offset = 0
            with open(local_fp, "rb") as raw:
                while True:
                    chunk = raw.read(SMB_CHUNK)
                    if not chunk:
                        break
                    f_handle.write(chunk, offset)
                    offset += len(chunk)
                    _bytes_done += len(chunk)
                    if progress_cb:
                        try:
                            progress_cb(_bytes_done, _total_bytes, local_fp.name)
                        except Exception:
                            pass
            f_handle.close(False)

        for fp in _all_files:
            rel = str(fp.relative_to(ld))
            try:
                def _do_smb_write(_fp=fp, _rel=rel):
                    _smb_write_tracked(_rel, _fp)
                _retry_with_backoff(
                    _do_smb_write,
                    max_retries=max_retries,
                    label=f"smb:{rel}",
                )
                uploaded += 1
            except Exception as e:
                logger.warning(f"[smb] Failed to upload {rel}: {e}")

        tree.disconnect()
        session.disconnect()
        conn.disconnect()

        logger.info(f"[smb] Uploaded {uploaded} file(s) to \\\\{server}\\{share}\\{remote_base}\\{ld.name}")
        return {"ok": True, "uploaded": uploaded, "path": f"\\\\{server}\\{share}\\{remote_base}\\{ld.name}"}

    except Exception as e:
        return {"ok": False, "error": f"SMB error: {e}"}


# ─── HTTPS (webhook / REST upload endpoint) ───────────────────────────────────

def upload_to_https(local_dir: str, https_config: dict, progress_cb=None,
                    max_retries: int = 3) -> dict:
    """
    Upload each file in a backup folder to an HTTPS endpoint via multipart POST.

    Files are streamed in 256 KB chunks — no file is fully loaded into RAM,
    so this works correctly for large backups without any arbitrary size cap.

    Supports Bearer token auth and custom headers.
    verify_ssl=False disables certificate verification (useful for self-signed certs).

    https_config: { url, token, headers(dict), verify_ssl(=true) }

    The server receives multipart/form-data with:
        file       — the binary file content (streamed)
        filename   — relative path inside the backup folder
        backup_dir — the top-level backup folder name

    progress_cb(bytes_done, total_bytes, filename) — optional, called after each file upload.
    """
    import http.client
    import ssl
    import urllib.parse

    _CHUNK = 256 * 1024   # 256 KB streaming chunk
    _CRLF  = b"\r\n"
    _SKIP  = {"MANIFEST.json", "BACKUP.sha256"}

    url        = https_config.get("url", "").strip()
    token      = https_config.get("token", "").strip()
    extra_hdrs = https_config.get("headers", {}) or {}
    verify_ssl = bool(https_config.get("verify_ssl", True))

    if not url:
        return {"ok": False, "error": "HTTPS upload URL not configured"}

    ssl_ctx = ssl.create_default_context()
    if not verify_ssl:
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode    = ssl.CERT_NONE

    parsed    = urllib.parse.urlparse(url)
    host      = parsed.netloc
    # Always POST to /backup/upload regardless of whatever trailing path the
    # user may have appended to the configured URL.  Strip any trailing slash
    # from the configured path and append the canonical upload sub-path so
    # upload and restore endpoints (GET /manifest, GET /files/<path>) share
    # a common base URL.
    _base_path = parsed.path.rstrip("/")
    path_qs    = _base_path + "/backup/upload"
    use_https  = parsed.scheme.lower() == "https"

    ld           = Path(local_dir)
    uploaded     = 0
    errors       = []
    _all_files   = [fp for fp in ld.rglob("*") if fp.is_file() and fp.name not in _SKIP]
    _total_bytes = sum(fp.stat().st_size for fp in _all_files)
    _bytes_done  = 0

    def _stream_multipart(conn, fp: Path, rel: str) -> int:
        """Stream one file as multipart/form-data.  Returns HTTP status code."""
        boundary  = "----BackupSysBoundary" + os.urandom(8).hex()
        file_size = fp.stat().st_size

        preamble = b""
        for k, v in {"filename": rel, "backup_dir": ld.name}.items():
            preamble += f"--{boundary}".encode() + _CRLF
            preamble += f'Content-Disposition: form-data; name="{k}"'.encode() + _CRLF
            preamble += _CRLF
            preamble += str(v).encode() + _CRLF
        preamble += f"--{boundary}".encode() + _CRLF
        preamble += f'Content-Disposition: form-data; name="file"; filename="{fp.name}"'.encode() + _CRLF
        preamble += b"Content-Type: application/octet-stream" + _CRLF
        preamble += _CRLF

        epilogue  = _CRLF + f"--{boundary}--".encode() + _CRLF
        total_len = len(preamble) + file_size + len(epilogue)

        hdrs = {
            "Content-Type":   f"multipart/form-data; boundary={boundary}",
            "Content-Length": str(total_len),
        }
        if token:
            hdrs["Authorization"] = f"Bearer {token}"
        hdrs.update(extra_hdrs)

        conn.putrequest("POST", path_qs)
        for k, v in hdrs.items():
            conn.putheader(k, v)
        conn.endheaders()

        conn.send(preamble)
        with open(fp, "rb") as fh:
            while True:
                chunk = fh.read(_CHUNK)
                if not chunk:
                    break
                conn.send(chunk)
        conn.send(epilogue)

        resp = conn.getresponse()
        resp.read()   # drain so connection can be reused
        return resp.status

    for fp in _all_files:
        rel = str(fp.relative_to(ld)).replace("\\", "/")
        try:
            def _do_https_upload(_fp=fp, _rel=rel):
                conn = (
                    http.client.HTTPSConnection(host, context=ssl_ctx, timeout=120)
                    if use_https
                    else http.client.HTTPConnection(host, timeout=120)
                )
                status = _stream_multipart(conn, _fp, _rel)
                conn.close()
                if status not in (200, 201, 202, 204):
                    raise OSError(f"HTTP {status}")
                return status
            _retry_with_backoff(
                _do_https_upload,
                max_retries=max_retries,
                label=f"https:{rel}",
            )
            uploaded    += 1
            _bytes_done += fp.stat().st_size
            if progress_cb:
                try:
                    progress_cb(_bytes_done, _total_bytes, fp.name)
                except Exception:
                    pass
        except Exception as e:
            errors.append(f"{rel}: {e}")

    if errors:
        logger.warning(f"[https] {len(errors)} file(s) failed to upload: {errors[:5]}")

    ok = uploaded > 0 or (uploaded == 0 and not _all_files)
    logger.info(f"[https] Uploaded {uploaded} file(s) to {url}")
    return {
        "ok":       ok,
        "uploaded": uploaded,
        "errors":   errors[:20],
        "path":     url,
    }


def upload_to_rclone(local_dir: str, rclone_config: dict, progress_cb=None,
                     max_retries: int = 3) -> dict:
    """
    Upload a backup folder to an rclone remote using the installed rclone CLI.

    rclone_config: { remote, path }
    progress_cb(bytes_done, total_bytes, filename) — optional, called from rclone stderr lines.
    """
    remote_name = (rclone_config.get("remote") or rclone_config.get("remote_name", "")).strip()
    remote_path = (rclone_config.get("path") or rclone_config.get("remote_path", "/backups")).strip()
    if not remote_name:
        return {"ok": False, "error": "Rclone remote name not configured"}

    try:
        check = subprocess.run(["rclone", "version"], capture_output=True, text=True, timeout=15)
        if check.returncode != 0:
            return {"ok": False, "error": "rclone not installed or not available in PATH"}
    except FileNotFoundError:
        return {"ok": False, "error": "rclone not installed or not available in PATH"}
    except Exception as e:
        return {"ok": False, "error": f"Failed to run rclone: {e}"}

    dest = f"{remote_name}:{remote_path}" if remote_path else f"{remote_name}:"
    cmd = ["rclone", "copy", str(Path(local_dir)), dest, "--progress"]
    if os.name == "nt":
        # ensure rclone uses POSIX-like paths internally when passed a Windows path
        cmd[2] = str(Path(local_dir))

    stderr_lines = []
    def _do_rclone():
        nonlocal stderr_lines
        stderr_lines = []
        try:
            proc = subprocess.Popen(cmd, stderr=subprocess.PIPE, text=True, bufsize=1)
        except FileNotFoundError:
            raise RuntimeError("rclone not installed or not available in PATH")
        if proc.stderr:
            for line in proc.stderr:
                stderr_lines.append(line.rstrip("\n"))
                if progress_cb:
                    try:
                        progress_cb(0, None, line.rstrip("\n"))
                    except Exception:
                        pass
        proc.wait(timeout=3600)
        if proc.returncode != 0:
            raise RuntimeError("rclone copy failed: " + "\n".join(stderr_lines[-10:]))

    try:
        _retry_with_backoff(
            _do_rclone,
            max_retries=max_retries,
            label=f"rclone:{dest}",
        )
    except Exception as e:
        return {"ok": False, "error": str(e)}

    # rclone doesn't expose exact per-file success count here, just assume success if exit 0
    return {"ok": True, "uploaded": "rclone", "path": dest}


# ─── Remote free-space check ──────────────────────────────────────────────────
# Used by backup_engine.py before starting an upload to ensure the remote
# destination has enough room.  Each transport is queried with its own protocol
# where a reliable method exists; others fall through gracefully so a failed
# space check never silently blocks a valid backup.

def check_remote_free_space(dest_type: str, cfg: dict, needed_bytes: int) -> dict:
    """Check free space on a remote backup destination before uploading.

    Args:
        dest_type:    One of: sftp, ftp, ftps, smb, webdav, rclone, cloud, https
        cfg:          Full config dict (same structure as the watch/global config).
        needed_bytes: Estimated bytes the backup will consume on the remote.

    Returns:
        {
            "ok":    bool   — True = enough space (or check not supported),
            "free":  int    — free bytes on remote (-1 = unknown),
            "error": str    — human-readable message when ok=False,
            "skipped": bool — True = transport doesn't support space queries,
        }
    """
    _OK     = {"ok": True,  "free": -1, "error": "", "skipped": True}
    _needed = max(int(needed_bytes * 1.1), 1)  # add 10 % headroom

    # ── SFTP — statvfs() ──────────────────────────────────────────────────────
    if dest_type == "sftp":
        sftp_cfg     = cfg.get("dest_sftp", {})
        host         = sftp_cfg.get("host", "").strip()
        port         = int(sftp_cfg.get("port", 22))
        username     = (sftp_cfg.get("username") or sftp_cfg.get("user", "")).strip()
        password     = sftp_cfg.get("password") or sftp_cfg.get("pass", "")
        key_path     = (sftp_cfg.get("key_path") or sftp_cfg.get("keyfile", "")).strip()
        key_pass     = sftp_cfg.get("key_passphrase") or sftp_cfg.get("key_pass", "")
        remote_path  = (sftp_cfg.get("remote_path") or sftp_cfg.get("path", "/")).rstrip("/") or "/"

        if not host or not username:
            return _OK  # not configured — let the upload itself report the error

        try:
            import paramiko
            transport = paramiko.Transport((host, port))
            transport.connect()
            if key_path and Path(key_path).exists():
                for key_cls in (paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey):
                    try:
                        pkey = key_cls.from_private_key_file(key_path, password=key_pass or None)
                        transport.auth_publickey(username, pkey)
                        break
                    except Exception:
                        continue
            else:
                transport.auth_password(username, password)

            if not transport.is_authenticated():
                transport.close()
                return _OK

            sftp = paramiko.SFTPClient.from_transport(transport)
            try:
                stat = sftp.statvfs(remote_path)
                free = stat.f_bavail * stat.f_frsize
            except (AttributeError, IOError):
                # Server doesn't support statvfs — fall back to root
                try:
                    stat = sftp.statvfs("/")
                    free = stat.f_bavail * stat.f_frsize
                except Exception:
                    sftp.close(); transport.close()
                    return _OK
            sftp.close()
            transport.close()

            if free < _needed:
                return {
                    "ok":      False,
                    "free":    free,
                    "error":   (
                        f"SFTP remote has insufficient space. "
                        f"Need ~{_human_size(_needed)}, only {_human_size(free)} free on {host}."
                    ),
                    "skipped": False,
                }
            return {"ok": True, "free": free, "error": "", "skipped": False}

        except ImportError:
            return _OK  # paramiko not installed; upload will catch the error
        except Exception as e:
            logger.warning("[space-check] SFTP statvfs failed (non-fatal): %s", e)
            return _OK

    # ── FTP / FTPS — AVBL command (RFC draft extension) ──────────────────────
    if dest_type in ("ftp", "ftps"):
        ftp_cfg     = cfg.get("dest_ftp", {})
        host        = ftp_cfg.get("host", "").strip()
        port        = int(ftp_cfg.get("port", 21))
        username    = (ftp_cfg.get("username") or ftp_cfg.get("user", "")).strip()
        password    = ftp_cfg.get("password") or ftp_cfg.get("pass", "")
        use_tls     = bool(ftp_cfg.get("use_tls", dest_type == "ftps"))

        if not host:
            return _OK
        try:
            import ftplib
            ftp_cls = ftplib.FTP_TLS if use_tls else ftplib.FTP
            with ftp_cls(timeout=15) as ftp:
                ftp.connect(host, port, timeout=15)
                if use_tls:
                    ftp.prot_p()
                # AVBL is a non-standard but widely supported extension
                try:
                    resp = ftp.sendcmd("AVBL")
                    free = int(resp.split()[-1])
                    if free < _needed:
                        return {
                            "ok":      False,
                            "free":    free,
                            "error":   (
                                f"FTP remote has insufficient space. "
                                f"Need ~{_human_size(_needed)}, only {_human_size(free)} free on {host}."
                            ),
                            "skipped": False,
                        }
                    return {"ok": True, "free": free, "error": "", "skipped": False}
                except Exception:
                    return _OK  # AVBL not supported by this server
        except Exception as e:
            logger.warning("[space-check] FTP AVBL check failed (non-fatal): %s", e)
            return _OK

    # ── SMB — UNC disk_usage on Windows; skipped on Linux ────────────────────
    if dest_type == "smb":
        smb_cfg     = cfg.get("dest_smb", {})
        server      = smb_cfg.get("server", "").strip()
        share       = smb_cfg.get("share", "").strip()
        username    = smb_cfg.get("username", "").strip()
        password    = smb_cfg.get("password", "")
        domain      = smb_cfg.get("domain", "")

        if not server or not share:
            return _OK

        if os.name == "nt":
            import subprocess, shutil as _sh
            unc_root = f"\\\\{server}\\{share}"
            if username:
                net_user = f"{domain}\\{username}" if domain else username
                try:
                    subprocess.run(
                        ["net", "use", unc_root, f"/user:{net_user}", password],
                        capture_output=True, timeout=15, check=False,
                    )
                except Exception:
                    pass
            try:
                usage = _sh.disk_usage(unc_root)
                free  = usage.free
                if free < _needed:
                    return {
                        "ok":      False,
                        "free":    free,
                        "error":   (
                            f"SMB share has insufficient space. "
                            f"Need ~{_human_size(_needed)}, only {_human_size(free)} free on "
                            f"\\\\{server}\\{share}."
                        ),
                        "skipped": False,
                    }
                return {"ok": True, "free": free, "error": "", "skipped": False}
            except Exception as e:
                logger.warning("[space-check] SMB disk_usage failed (non-fatal): %s", e)
                return _OK
        # Linux SMB space query via smbprotocol QueryFSSize
        try:
            import smbprotocol.connection, smbprotocol.session, smbprotocol.tree
            import smbprotocol.query_info as smb_qi
            import uuid as _uuid
            conn_id = _uuid.uuid4()
            conn    = smbprotocol.connection.Connection(conn_id, server, 445)
            conn.connect(timeout=15)
            session = smbprotocol.session.Session(
                conn, username=username, password=password, require_encryption=False
            )
            session.connect()
            unc  = f"\\\\{server}\\{share}"
            tree = smbprotocol.tree.TreeConnect(session, unc)
            tree.connect()
            # FILE_FS_SIZE_INFORMATION = InfoClass 3
            try:
                raw = tree.query_info(
                    smbprotocol.query_info.InfoType.SMB2_0_INFO_FILESYSTEM,
                    smbprotocol.query_info.FileSystemInformationClass.FileFsSizeInformation,
                    output_buffer_length=24,
                )
                # Structure: total_allocation_units(8) + available_units(8) + sectors_per_unit(4) + bytes_per_sector(4)
                import struct as _struct
                _ta, _aa, _spu, _bps = _struct.unpack_from("<QQII", raw)
                free = _aa * _spu * _bps
                tree.disconnect(); session.disconnect(); conn.disconnect()
                if free < _needed:
                    return {
                        "ok":      False,
                        "free":    free,
                        "error":   (
                            f"SMB share has insufficient space. "
                            f"Need ~{_human_size(_needed)}, only {_human_size(free)} free on "
                            f"\\\\{server}\\{share}."
                        ),
                        "skipped": False,
                    }
                return {"ok": True, "free": free, "error": "", "skipped": False}
            except Exception:
                tree.disconnect(); session.disconnect(); conn.disconnect()
                return _OK
        except ImportError:
            return _OK
        except Exception as e:
            logger.warning("[space-check] SMB FileFsSizeInformation failed (non-fatal): %s", e)
            return _OK

    # ── WebDAV — DAV:quota-available-bytes PROPFIND ───────────────────────────
    if dest_type == "webdav":
        webdav_cfg  = cfg.get("dest_webdav", {})
        url_base    = (webdav_cfg.get("url") or "").rstrip("/")
        username    = (webdav_cfg.get("username") or webdav_cfg.get("user", "")).strip()
        password    = webdav_cfg.get("password") or webdav_cfg.get("pass", "")
        verify_ssl  = webdav_cfg.get("verify_ssl", True)
        webdav_root = (webdav_cfg.get("webdav_root") or "").rstrip("/")

        if not url_base:
            return _OK

        try:
            import urllib.request as _ur, urllib.error, ssl, base64 as _b64
            _creds = _b64.b64encode(f"{username}:{password}".encode()).decode()
            _auth  = f"Basic {_creds}"
            _ctx   = ssl.create_default_context() if verify_ssl else ssl._create_unverified_context()

            propfind_body = (
                b'<?xml version="1.0" encoding="utf-8"?>'
                b'<D:propfind xmlns:D="DAV:">'
                b'<D:prop>'
                b'<D:quota-available-bytes/>'
                b'<D:quota-used-bytes/>'
                b'</D:prop>'
                b'</D:propfind>'
            )
            target = url_base + webdav_root + "/"
            req = _ur.Request(
                target,
                data=propfind_body,
                headers={
                    "Authorization":  _auth,
                    "Content-Type":   "application/xml; charset=utf-8",
                    "Depth":          "0",
                },
                method="PROPFIND",
            )
            try:
                with _ur.urlopen(req, context=_ctx, timeout=15) as resp:
                    body = resp.read().decode("utf-8", errors="replace")
            except urllib.error.HTTPError as e:
                body = e.read().decode("utf-8", errors="replace") if e.fp else ""

            import re
            m = re.search(r"<[^>]*quota-available-bytes[^>]*>(\d+)<", body)
            if not m:
                return _OK  # server doesn't expose quota
            free = int(m.group(1))
            if free < _needed:
                return {
                    "ok":      False,
                    "free":    free,
                    "error":   (
                        f"WebDAV server has insufficient space. "
                        f"Need ~{_human_size(_needed)}, only {_human_size(free)} free."
                    ),
                    "skipped": False,
                }
            return {"ok": True, "free": free, "error": "", "skipped": False}

        except Exception as e:
            logger.warning("[space-check] WebDAV PROPFIND quota check failed (non-fatal): %s", e)
            return _OK

    # ── rclone — `rclone about <remote>: --json` ─────────────────────────────
    if dest_type == "rclone":
        rclone_cfg  = cfg.get("dest_rclone", {})
        remote_name = (rclone_cfg.get("remote") or rclone_cfg.get("remote_name", "")).strip()
        if not remote_name:
            return _OK
        try:
            import subprocess, json as _json
            result = subprocess.run(
                ["rclone", "about", f"{remote_name}:", "--json"],
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode != 0:
                return _OK  # rclone about not supported for this remote
            info = _json.loads(result.stdout)
            free = info.get("free", -1)
            if free == -1:
                return _OK  # remote doesn't report free space
            free = int(free)
            if free < _needed:
                return {
                    "ok":      False,
                    "free":    free,
                    "error":   (
                        f"rclone remote '{remote_name}' has insufficient space. "
                        f"Need ~{_human_size(_needed)}, only {_human_size(free)} free."
                    ),
                    "skipped": False,
                }
            return {"ok": True, "free": free, "error": "", "skipped": False}
        except FileNotFoundError:
            return _OK  # rclone not installed
        except Exception as e:
            logger.warning("[space-check] rclone about failed (non-fatal): %s", e)
            return _OK

    # All other types (https, cloud/gdrive) — no standard way to query
    return _OK


def _human_size(n: int) -> str:
    """Human-readable byte size (duplicated here so transport_utils is self-contained)."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024
    return f"{n:.1f} PB"


# ─── Google Drive quota ───────────────────────────────────────────────────────

def get_gdrive_quota(cloud_config: dict) -> dict:
    """Fetch Google Drive storage quota for the connected account.

    Calls the Drive API v3 /about endpoint.  Silently refreshes the access
    token if needed (same logic used in upload_to_gdrive).

    Returns:
        {
            "ok":         bool,
            "limit":      int   — total storage in bytes (-1 = unlimited),
            "usage":      int   — total bytes used across all Google products,
            "drive_used": int   — bytes used specifically in Drive (excl. Trash),
            "free":       int   — limit - usage  (-1 when limit is unknown),
            "error":      str,
        }
    """
    import urllib.request as _ur, json as _json

    access_token  = cloud_config.get("access_token", "").strip()
    refresh_token = cloud_config.get("refresh_token", "").strip()
    client_id     = cloud_config.get("client_id", "").strip()
    client_secret = cloud_config.get("client_secret", "").strip()

    if not access_token:
        return {"ok": False, "limit": -1, "usage": -1, "drive_used": -1, "free": -1,
                "error": "No access token — connect Google Drive first."}

    def _fetch(token: str) -> dict:
        req = _ur.Request(
            "https://www.googleapis.com/drive/v3/about?fields=storageQuota",
            headers={"Authorization": f"Bearer {token}"},
        )
        with _ur.urlopen(req, timeout=15) as resp:
            return _json.loads(resp.read())

    def _refresh() -> str | None:
        if not (refresh_token and client_id and client_secret):
            return None
        import urllib.parse
        data = urllib.parse.urlencode({
            "client_id":     client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type":    "refresh_token",
        }).encode()
        try:
            req = _ur.Request("https://oauth2.googleapis.com/token", data=data)
            tokens = _json.loads(_ur.urlopen(req, timeout=15).read())
            return tokens.get("access_token", "")
        except Exception:
            return None

    try:
        try:
            data = _fetch(access_token)
        except Exception:
            # Token may be expired — try a silent refresh
            new_token = _refresh()
            if not new_token:
                raise
            data = _fetch(new_token)

        sq = data.get("storageQuota", {})
        limit      = int(sq["limit"])      if "limit"      in sq else -1
        usage      = int(sq["usage"])      if "usage"      in sq else -1
        drive_used = int(sq.get("usageInDrive", 0))
        free       = (limit - usage) if (limit != -1 and usage != -1) else -1

        return {
            "ok":         True,
            "limit":      limit,
            "usage":      usage,
            "drive_used": drive_used,
            "free":       free,
            "error":      "",
        }

    except Exception as e:
        return {"ok": False, "limit": -1, "usage": -1, "drive_used": -1, "free": -1,
                "error": str(e)}


# ─── Test-connection helpers ──────────────────────────────────────────────────
# Used by desktop_app.py "Test Connection" buttons — centralises auth logic here
# so it stays in sync with the actual upload functions above.

def test_sftp_connection(sftp_config: dict) -> dict:
    """
    Verify SFTP credentials and list the remote_path directory.
    Tries RSA → Ed25519 → ECDSA key auth, then password auth — same order as
    upload_to_sftp() so the test result always matches backup behaviour.
    Returns { ok: bool, message: str }.
    """
    try:
        import paramiko
    except ImportError:
        return {"ok": False, "message": "paramiko not installed — run: pip install paramiko"}

    host       = sftp_config.get("host", "").strip()
    port       = int(sftp_config.get("port", 22))
    username   = (sftp_config.get("username") or sftp_config.get("user", "")).strip()
    password   = sftp_config.get("password") or sftp_config.get("pass", "")
    key_path   = (sftp_config.get("key_path") or sftp_config.get("keyfile", "")).strip()
    key_pass   = sftp_config.get("key_passphrase") or sftp_config.get("key_pass", "")
    remote_dir = (sftp_config.get("remote_path") or sftp_config.get("path", "/")).rstrip("/") or "/"

    if not host:
        return {"ok": False, "message": "SFTP host not configured"}
    if not username:
        return {"ok": False, "message": "SFTP username not configured"}

    transport = None
    try:
        transport = paramiko.Transport((host, port))
        transport.connect()

        # Host key verification — same TOFU logic as upload_to_sftp()
        _known_hosts_path = Path.home() / ".backupsys_known_hosts"
        _known_hosts = paramiko.HostKeys()
        if _known_hosts_path.exists():
            try:
                _known_hosts.load(_known_hosts_path)
            except Exception:
                pass
        _host_key = transport.get_remote_server_key()
        _host_id  = f"[{host}]:{port}" if port != 22 else host
        _stored   = _known_hosts.lookup(_host_id)
        if _stored:
            _stored_key = _stored.get(_host_key.get_name())
            if _stored_key and _stored_key != _host_key:
                transport.close()
                return {
                    "ok": False,
                    "message": (
                        f"Host key mismatch for {host}:{port} — fingerprint has changed. "
                        f"If the server was reinstalled, remove its entry from {_known_hosts_path}."
                    ),
                }
        else:
            _known_hosts.add(_host_id, _host_key.get_name(), _host_key)
            try:
                _known_hosts.save(str(_known_hosts_path))
            except Exception:
                pass

        if key_path and Path(key_path).exists():
            pkey = None
            _needs_passphrase = False
            for key_cls in (paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey):
                try:
                    pkey = key_cls.from_private_key_file(key_path, password=key_pass or None)
                    break
                except paramiko.ssh_exception.PasswordRequiredException:
                    _needs_passphrase = True
                    break  # key loaded but needs a passphrase — no point trying other types
                except paramiko.SSHException:
                    continue
            if pkey is None:
                if _needs_passphrase:
                    return {"ok": False, "message": f"Private key is passphrase-protected — set key_passphrase in your SFTP config: {key_path}"}
                return {"ok": False, "message": f"Could not load private key (unsupported format or wrong passphrase): {key_path}"}
            transport.auth_publickey(username, pkey)
        else:
            if not password:
                return {"ok": False, "message": "SFTP password (or key_path) not configured"}
            transport.auth_password(username, password)

        if not transport.is_authenticated():
            return {"ok": False, "message": "Authentication failed"}

        sftp    = paramiko.SFTPClient.from_transport(transport)
        entries = sftp.listdir(remote_dir)
        sftp.close()
        return {"ok": True, "message": f"✅ Connected to {host}:{port}  |  {len(entries)} item(s) in {remote_dir}"}

    except paramiko.AuthenticationException as e:
        return {"ok": False, "message": f"Authentication failed: {e}"}
    except paramiko.SSHException as e:
        return {"ok": False, "message": f"SSH error: {e}"}
    except OSError as e:
        return {"ok": False, "message": f"Network error: {e}"}
    except Exception as e:
        return {"ok": False, "message": str(e)}
    finally:
        try:
            if transport:
                transport.close()
        except Exception:
            pass


def test_ftp_connection(ftp_config: dict) -> dict:
    """
    Verify FTP/FTPS credentials and list the remote_path directory.
    Returns { ok: bool, message: str }.
    """
    import ftplib

    host     = ftp_config.get("host", "").strip()
    port     = int(ftp_config.get("port", 21))
    username = (ftp_config.get("username") or ftp_config.get("user", "")).strip()
    password = ftp_config.get("password") or ftp_config.get("pass", "")
    rpath    = (ftp_config.get("remote_path") or ftp_config.get("path", "/")).strip() or "/"
    use_tls  = bool(ftp_config.get("use_tls", True))

    if not host:
        return {"ok": False, "message": "FTP host not configured"}

    ftp = None
    try:
        if use_tls:
            ftp = ftplib.FTP_TLS(timeout=10)
            ftp.connect(host, port)
            ftp.login(username, password)
            ftp.prot_p()
        else:
            ftp = ftplib.FTP(timeout=10)
            ftp.connect(host, port)
            ftp.login(username, password)

        ftp.cwd(rpath)
        entries = ftp.nlst()
        proto   = "FTPS" if use_tls else "FTP"
        return {"ok": True, "message": f"✅ {proto} connected to {host}:{port}  |  {len(entries)} item(s) in {rpath}"}

    except ftplib.all_errors as e:
        return {"ok": False, "message": f"FTP error: {e}"}
    except OSError as e:
        return {"ok": False, "message": f"Network error: {e}"}
    except Exception as e:
        return {"ok": False, "message": str(e)}
    finally:
        try:
            if ftp:
                ftp.quit()
        except Exception:
            pass


def test_smb_connection(smb_config: dict) -> dict:
    """
    Verify SMB/CIFS share connectivity.
    On Windows uses UNC paths directly; on Linux/macOS uses smbprotocol.
    Returns { ok: bool, message: str }.
    """
    server   = smb_config.get("server", "").strip()
    share    = smb_config.get("share", "").strip()
    username = smb_config.get("username", "").strip()
    password = smb_config.get("password", "")
    domain   = smb_config.get("domain", "")
    rpath    = smb_config.get("remote_path", "").strip().strip("/\\")

    # Also accept a single UNC path string (desktop_app passes dest_smb_path directly)
    unc_path = smb_config.get("unc_path", "").strip()
    if unc_path and (not server or not share):
        import re
        m = re.match(r"[/\\]{2}([^/\\]+)[/\\]([^/\\]+)", unc_path)
        if m:
            server, share = m.group(1), m.group(2)

    if not server:
        return {"ok": False, "message": "SMB server not configured"}
    if not share:
        return {"ok": False, "message": "SMB share not configured"}

    unc = f"\\\\{server}\\{share}"
    test_dir = f"{unc}\\{rpath}" if rpath else unc

    if os.name == "nt":
        import subprocess as _sp
        if username:
            user_arg = f"{domain}\\{username}" if domain else username
            cmd = ["net", "use", unc, f"/user:{user_arg}"]
            if password:
                cmd.insert(3, password)
            cmd += ["/persistent:no"]
            try:
                res = _sp.run(cmd, capture_output=True, text=True, timeout=15)
                stderr = (res.stdout + res.stderr).lower()
                if res.returncode != 0 and "already" not in stderr and "local device" not in stderr:
                    return {"ok": False, "message": f"net use failed: {(res.stderr or res.stdout).strip()}"}
            except Exception as e:
                return {"ok": False, "message": str(e)}
        try:
            entries = list(Path(test_dir).iterdir())
            return {"ok": True, "message": f"✅ Connected to {unc}  |  {len(entries)} item(s) visible"}
        except Exception as e:
            return {"ok": False, "message": str(e)}
    else:
        try:
            import smbclient
            smbclient.register_session(server, username=username, password=password,
                                       connection_timeout=10)
            entries = smbclient.listdir(test_dir)
            return {"ok": True, "message": f"✅ Connected to {unc}  |  {len(entries)} item(s) visible"}
        except ImportError:
            return {"ok": False, "message": "smbprotocol not installed — run: pip install smbprotocol"}
        except Exception as e:
            return {"ok": False, "message": str(e)}


def test_https_connection(https_config: dict) -> dict:
    """
    Verify HTTPS endpoint reachability via HEAD request.
    Returns { ok: bool, message: str }.
    """
    import urllib.request
    import urllib.error
    import ssl

    url        = https_config.get("url", "").strip()
    token      = https_config.get("token", "").strip()
    verify_ssl = bool(https_config.get("verify_ssl", True))

    if not url:
        return {"ok": False, "message": "HTTPS URL not configured"}

    ctx = ssl.create_default_context()
    if not verify_ssl:
        ctx.check_hostname = False
        ctx.verify_mode    = ssl.CERT_NONE

    try:
        req = urllib.request.Request(url, method="HEAD")
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        with urllib.request.urlopen(req, context=ctx, timeout=10) as resp:
            code = resp.getcode()
        return {"ok": True, "message": f"✅ Endpoint reachable (HTTP {code})"}
    except urllib.error.HTTPError as e:
        # 4xx/5xx means the server responded — HEAD is often rejected on upload endpoints
        if e.code in (400, 401, 403, 405, 422):
            return {"ok": True, "message": f"✅ Server reachable (HTTP {e.code} — normal for HEAD on upload endpoints)"}
        return {"ok": False, "message": f"HTTP {e.code}: {e.reason}"}
    except urllib.error.URLError as e:
        return {"ok": False, "message": f"URL error: {e.reason}"}
    except Exception as e:
        return {"ok": False, "message": str(e)}


def test_rclone_connection(rclone_config: dict) -> dict:
    """
    Verify that rclone is installed and the configured remote:path is reachable.
    Returns { ok: bool, message: str }.
    """
    remote_name = (rclone_config.get("remote") or rclone_config.get("remote_name", "")).strip()
    remote_path = (rclone_config.get("path") or rclone_config.get("remote_path", "/backups")).strip()
    if not remote_name:
        return {"ok": False, "message": "Rclone remote name not configured"}

    try:
        check = subprocess.run(["rclone", "version"], capture_output=True, text=True, timeout=15)
        if check.returncode != 0:
            return {"ok": False, "message": "rclone not installed or not available in PATH"}
    except FileNotFoundError:
        return {"ok": False, "message": "rclone not installed or not available in PATH"}
    except Exception as e:
        return {"ok": False, "message": f"Failed to run rclone: {e}"}

    dest = f"{remote_name}:{remote_path}" if remote_path else f"{remote_name}:"
    try:
        proc = subprocess.run(["rclone", "lsd", dest], capture_output=True, text=True, timeout=30)
        if proc.returncode == 0:
            return {"ok": True, "message": f"✅ rclone can access {dest}"}
        return {"ok": False, "message": proc.stderr.strip() or proc.stdout.strip() or f"Failed to list {dest}"}
    except Exception as e:
        return {"ok": False, "message": str(e)}


def upload_to_webdav(local_dir: str, webdav_config: dict, progress_cb=None,
                     max_retries: int = 3, verify: bool = False) -> dict:
    """
    Upload a backup folder to a WebDAV server (Nextcloud, ownCloud, etc.).

    Uses webdavclient3 if installed, otherwise falls back to stdlib urllib
    so the feature works with zero extra dependencies.

    webdav_config keys:
        url          Full base URL, e.g. https://nextcloud.example.com
        username     Login username
        password     Login password
        remote_path  Remote destination path, e.g. /backups
        verify_ssl   bool, default True
        webdav_root  Optional DAV root prefix.
                     Nextcloud: /remote.php/dav/files/<USERNAME>/
                     ownCloud:  /remote.php/webdav/
                     Plain:     leave empty (server root)

    verify — if True, re-downloads the first 8 KB of each uploaded file and
             compares its MD5 against the local copy to detect silent corruption.

    Returns { ok: bool, uploaded: int, path: str, error: str }
    """
    import ssl
    import urllib.request
    import urllib.error
    import base64 as _b64
    import struct

    url_base    = (webdav_config.get("url") or "").rstrip("/")
    username    = (webdav_config.get("username") or webdav_config.get("user", "")).strip()
    password    = _cred_webdav(webdav_config) if _CRED_STORE else (webdav_config.get("password") or webdav_config.get("pass", ""))
    remote_path = (webdav_config.get("remote_path") or "/backups").strip("/")
    verify_ssl  = webdav_config.get("verify_ssl", True)
    webdav_root = (webdav_config.get("webdav_root") or "").rstrip("/")

    if not url_base:
        return {"ok": False, "error": "WebDAV URL not configured"}
    if not username:
        return {"ok": False, "error": "WebDAV username not configured"}

    # Build Basic-auth header
    _creds  = _b64.b64encode(f"{username}:{password}".encode()).decode()
    _auth   = f"Basic {_creds}"
    _ssl_ctx = ssl.create_default_context() if verify_ssl else (
        lambda: (ssl._create_unverified_context())()
    )

    def _make_opener():
        # Opener with Basic auth injected manually (avoids 401-redirect loop)
        return urllib.request.build_opener()

    def _req(method: str, path: str, data: bytes = None, content_type: str = "application/octet-stream") -> int:
        """Send a WebDAV method, return HTTP status code."""
        full = url_base + webdav_root + "/" + path.lstrip("/")
        headers = {
            "Authorization": _auth,
            "Content-Type":  content_type,
        }
        if data is not None:
            headers["Content-Length"] = str(len(data))
        r = urllib.request.Request(full, data=data, headers=headers, method=method)
        try:
            ctx = ssl.create_default_context() if verify_ssl else ssl._create_unverified_context()
            with urllib.request.urlopen(r, context=ctx, timeout=60) as resp:
                return resp.status
        except urllib.error.HTTPError as e:
            return e.code

    def _put_file(remote_file: str, local_path: Path, prog_ref: list) -> bool:
        """PUT a single file. Returns True on success."""
        full = url_base + webdav_root + "/" + remote_file.lstrip("/")
        file_size = local_path.stat().st_size
        headers = {
            "Authorization":  _auth,
            "Content-Type":   "application/octet-stream",
            "Content-Length": str(file_size),
        }
        try:
            ctx = ssl.create_default_context() if verify_ssl else ssl._create_unverified_context()
            CHUNK = 256 * 1024
            bytes_sent = [0]

            class _ReadWrapper:
                def __init__(self, fh):
                    self._fh = fh
                def read(self, n=-1):
                    chunk = self._fh.read(n)
                    if chunk and progress_cb:
                        bytes_sent[0] += len(chunk)
                        try:
                            progress_cb(bytes_sent[0], file_size, local_path.name)
                        except Exception:
                            pass
                    return chunk

            with open(str(local_path), "rb") as fh:
                r = urllib.request.Request(full, data=_ReadWrapper(fh), headers=headers, method="PUT")
                with urllib.request.urlopen(r, context=ctx, timeout=300) as resp:
                    prog_ref[0] = 1
                    return resp.status in (200, 201, 204)
        except urllib.error.HTTPError as e:
            if e.code in (200, 201, 204):
                prog_ref[0] = 1
                return True
            logger.warning(f"[webdav] PUT {remote_file}: HTTP {e.code}")
            return False
        except Exception as exc:
            logger.warning(f"[webdav] PUT {remote_file}: {exc}")
            return False

    def _mkcol(path: str) -> bool:
        """Create a remote collection (directory). Returns True if OK or already exists."""
        status = _req("MKCOL", path)
        return status in (200, 201, 405)  # 405 = already exists

    # ── Try webdavclient3 first (handles edge cases better for Nextcloud) ──────
    try:
        from webdav3.client import Client as _WDClient
        options = {
            "webdav_hostname": url_base,
            "webdav_login":    username,
            "webdav_password": password,
            "webdav_root":     webdav_root or "/",
            "webdav_cert_path":  "",
            "webdav_key_path":   "",
        }
        if not verify_ssl:
            options["webdav_disable_check"] = True
        _wdc = _WDClient(options)
        _wdc_available = True
    except ImportError:
        _wdc_available = False

    ld        = Path(local_dir)
    folder    = ld.name
    dest_root = f"{remote_path}/{folder}".lstrip("/")
    _SKIP     = {"MANIFEST.json", "BACKUP.sha256"}
    uploaded  = 0

    try:
        if _wdc_available:
            # ── webdavclient3 path ──────────────────────────────────────────
            if not _wdc.check(remote_path):
                _wdc.mkdir(remote_path)
            if not _wdc.check(dest_root):
                _wdc.mkdir(dest_root)

            all_files = [fp for fp in ld.rglob("*") if fp.is_file() and fp.name not in _SKIP]
            total_files = len(all_files)
            for i, fp in enumerate(all_files):
                rel         = fp.relative_to(ld)
                remote_file = f"{dest_root}/{str(rel).replace(os.sep, '/')}"
                remote_dir  = str(Path(remote_file).parent).replace("\\", "/")
                if remote_dir != dest_root and not _wdc.check(remote_dir):
                    _wdc.mkdir(remote_dir)
                _wdc.upload_sync(remote_path=remote_file, local_path=str(fp))
                uploaded += 1
                if progress_cb:
                    try:
                        progress_cb(i + 1, total_files, fp.name)
                    except Exception:
                        pass

        else:
            # ── stdlib urllib fallback ─────────────────────────────────────
            _mkcol(remote_path)
            _mkcol(dest_root)
            _seen_dirs = set()

            all_files = [fp for fp in ld.rglob("*") if fp.is_file() and fp.name not in _SKIP]
            for fp in all_files:
                rel        = fp.relative_to(ld)
                parts      = rel.parts
                # Ensure all parent dirs exist
                for depth in range(1, len(parts)):
                    dpath = dest_root + "/" + "/".join(parts[:depth])
                    if dpath not in _seen_dirs:
                        _mkcol(dpath)
                        _seen_dirs.add(dpath)

                remote_file = dest_root + "/" + "/".join(parts)
                _prog = [0]
                def _do_webdav_put(_rf=remote_file, _fp=fp):
                    _p = [0]
                    ok = _put_file(_rf, _fp, _p)
                    if not ok:
                        raise OSError(f"PUT failed for {_fp.name}")
                try:
                    _retry_with_backoff(
                        _do_webdav_put,
                        max_retries=max_retries,
                        label=f"webdav:{'/'.join(parts)}",
                    )
                    uploaded += 1
                except Exception:
                    logger.warning(f"[webdav] Failed to upload {fp.name} after {max_retries} retries")

    except Exception as e:
        logger.error(f"[webdav] Upload failed: {e}")
        return {"ok": False, "uploaded": uploaded, "path": dest_root, "error": str(e)}

    ok = uploaded > 0 or len([f for f in ld.rglob("*") if f.is_file() and f.name not in _SKIP]) == 0
    result = {
        "ok":       ok,
        "uploaded": uploaded,
        "path":     dest_root,
        "error":    None if ok else "No files uploaded",
    }

    # ── Post-upload checksum verification ─────────────────────────────────────
    # Re-downloads the first 8 KB of each uploaded file via HTTP GET and
    # compares the MD5 against the local copy, catching silent corruption or
    # truncated transfers that file-count checks would miss.
    if verify and ok:
        verify_warnings = []
        # Resolve a concrete SSL context (handle the case where _ssl_ctx is a lambda)
        _verify_ssl_ctx = _ssl_ctx if isinstance(_ssl_ctx, ssl.SSLContext) else ssl._create_unverified_context()
        _all_verify_files = [f for f in ld.rglob("*") if f.is_file() and f.name not in _SKIP]
        for fp in _all_verify_files:
            rel         = fp.relative_to(ld)
            remote_file = dest_root + "/" + str(rel).replace(os.sep, "/")
            full_url    = url_base + webdav_root + "/" + remote_file.lstrip("/")
            try:
                local_md5 = hashlib.md5()
                with open(str(fp), "rb") as lf:
                    local_md5.update(lf.read(8192))

                req = urllib.request.Request(full_url, method="GET")
                req.add_header("Authorization", _auth)
                req.add_header("Range", "bytes=0-8191")
                with urllib.request.urlopen(req, context=_verify_ssl_ctx, timeout=30) as resp:
                    remote_chunk = resp.read(8192)
                remote_md5 = hashlib.md5(remote_chunk)

                if local_md5.hexdigest() != remote_md5.hexdigest():
                    verify_warnings.append(f"MD5 mismatch for {rel}")
                    logger.warning(f"[webdav] Verify: MD5 mismatch for {rel}")
            except Exception as ve:
                verify_warnings.append(f"Verification failed for {rel}: {ve}")
                logger.warning(f"[webdav] Verify error for {rel}: {ve}")

        if verify_warnings:
            result["warnings"] = verify_warnings
            logger.warning(
                f"[webdav] Post-upload verification: {len(verify_warnings)} file(s) "
                f"may be corrupted or missing on the remote server"
            )
        else:
            logger.info(f"[webdav] Post-upload verification: all {len(_all_verify_files)} file(s) OK")

    return result


def test_webdav_connection(webdav_config: dict) -> dict:
    """
    Verify WebDAV credentials and connectivity by sending a PROPFIND
    request to the configured URL.  Returns { ok, error }.
    """
    import ssl
    import urllib.request
    import urllib.error
    import base64 as _b64

    url_base   = (webdav_config.get("url") or "").rstrip("/")
    username   = (webdav_config.get("username") or webdav_config.get("user", "")).strip()
    password   = _cred_webdav(webdav_config) if _CRED_STORE else (webdav_config.get("password") or webdav_config.get("pass", ""))
    verify_ssl = webdav_config.get("verify_ssl", True)
    webdav_root= (webdav_config.get("webdav_root") or "").rstrip("/")

    if not url_base:
        return {"ok": False, "error": "WebDAV URL not configured"}

    _creds = _b64.b64encode(f"{username}:{password}".encode()).decode()
    full   = url_base + webdav_root + "/"
    req    = urllib.request.Request(
        full,
        data=b'<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:prop><D:resourcetype/></D:prop></D:propfind>',
        headers={"Authorization": f"Basic {_creds}", "Depth": "0", "Content-Type": "application/xml"},
        method="PROPFIND",
    )
    try:
        ctx = ssl.create_default_context() if verify_ssl else ssl._create_unverified_context()
        with urllib.request.urlopen(req, context=ctx, timeout=15) as resp:
            if resp.status in (207, 200):
                return {"ok": True, "error": None}
            return {"ok": False, "error": f"HTTP {resp.status}"}
    except urllib.error.HTTPError as e:
        if e.code == 207:
            return {"ok": True, "error": None}
        return {"ok": False, "error": f"HTTP {e.code}: {e.reason}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def test_gdrive_connection(cloud_config: dict) -> dict:
    """
    Verify a Google Drive connection by checking the saved access token against
    Google's tokeninfo endpoint.  If the token is within 5 minutes of expiry (or
    already invalid), a silent refresh is attempted using the refresh token.

    cloud_config keys (all optional — falls back to QSettings values):
        access_token   — current OAuth access token
        refresh_token  — OAuth refresh token used for silent renewal
        client_id      — Google OAuth client ID
        client_secret  — Google OAuth client secret

    Returns { ok: bool, detail: str }  where detail is a human-readable message
    shown directly in the UI (e.g. "Connected · expires in 58 min" or the error).
    """
    import urllib.request as _ur
    import urllib.parse
    import json as _json

    access_token  = (cloud_config.get("access_token")  or "").strip()
    refresh_token = (cloud_config.get("refresh_token") or "").strip()
    client_id     = (cloud_config.get("client_id")     or "").strip()
    client_secret = (cloud_config.get("client_secret") or "").strip()

    if not access_token:
        return {"ok": False, "detail": "No access token — connect Google Drive first"}

    def _check_token(token: str):
        """Hit tokeninfo; return (ok, expires_in_seconds, error_str)."""
        try:
            req  = _ur.Request(
                f"https://www.googleapis.com/oauth2/v1/tokeninfo?access_token={token}"
            )
            resp = _ur.urlopen(req, timeout=10)
            info = _json.loads(resp.read())
            exp  = int(info.get("expires_in", 0))
            return True, exp, None
        except Exception as exc:
            return False, 0, str(exc)

    def _refresh(r_token: str, c_id: str, c_secret: str):
        """Attempt a silent token refresh; return new access_token or None."""
        if not (r_token and c_id and c_secret):
            return None
        try:
            data = urllib.parse.urlencode({
                "client_id":     c_id,
                "client_secret": c_secret,
                "refresh_token": r_token,
                "grant_type":    "refresh_token",
            }).encode()
            resp   = _ur.urlopen(
                _ur.Request("https://oauth2.googleapis.com/token", data=data), timeout=15
            )
            tokens = _json.loads(resp.read())
            return tokens.get("access_token") or None
        except Exception:
            return None

    ok, expires_in, err = _check_token(access_token)

    if ok and expires_in >= 300:
        mins = expires_in // 60
        return {"ok": True, "detail": f"Connected · token valid, expires in {mins} min"}

    if ok and expires_in < 300:
        # Token about to expire — try silent refresh
        new_token = _refresh(refresh_token, client_id, client_secret)
        if new_token:
            return {"ok": True, "detail": "Connected · token refreshed successfully"}
        return {
            "ok": False,
            "detail": f"Token expires in {expires_in}s and could not be refreshed — reconnect Google Drive",
        }

    # Token is invalid — try silent refresh before reporting failure
    new_token = _refresh(refresh_token, client_id, client_secret)
    if new_token:
        return {"ok": True, "detail": "Connected · token was expired but refreshed successfully"}

    return {
        "ok": False,
        "detail": f"Token invalid and refresh failed — reconnect Google Drive ({err or 'unknown error'})",
    }


# ─── DOWNLOAD FUNCTIONS (Remote Restore) ──────────────────────────────────────

def download_from_sftp(remote_dir: str, local_dest: str, sftp_config: dict,
                        progress_cb=None) -> dict:
    """
    Download a backup folder recursively from SFTP to local_dest.
    Preserves folder structure.
    Returns { status: "ok"|"error", downloaded: int, error: str|None }

    progress_cb(downloaded: int, filename: str) — called after each file is saved.
    """
    try:
        import paramiko
    except ImportError:
        return {"status": "error", "downloaded": 0, "error": "paramiko not installed"}

    host       = sftp_config.get("host", "").strip()
    port       = int(sftp_config.get("port", 22))
    username   = (sftp_config.get("username") or sftp_config.get("user", "")).strip()
    password   = _cred_sftp(sftp_config) if _CRED_STORE else (sftp_config.get("password") or sftp_config.get("pass", ""))
    key_path   = (sftp_config.get("key_path") or sftp_config.get("keyfile", "")).strip()
    key_pass   = sftp_config.get("key_passphrase") or ""

    if not host:
        return {"status": "error", "downloaded": 0, "error": "SFTP host not configured"}
    if not username:
        return {"status": "error", "downloaded": 0, "error": "SFTP username not configured"}

    transport = None
    sftp = None
    downloaded = 0

    try:
        _WIN_SIZE = 33 * 1024 * 1024
        _PKT_SIZE = 32 * 1024

        transport = paramiko.Transport((host, port))
        transport.default_window_size = _WIN_SIZE
        transport.default_max_packet_size = _PKT_SIZE
        transport.connect()

        if key_path and Path(key_path).exists():
            pkey = None
            for key_cls in (paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey):
                try:
                    pkey = key_cls.from_private_key_file(key_path, password=key_pass or None)
                    break
                except paramiko.ssh_exception.PasswordRequiredException:
                    return {"status": "error", "downloaded": 0, "error": "Key passphrase required"}
            if not pkey:
                return {"status": "error", "downloaded": 0, "error": "Could not load private key"}
            transport.auth_publickey(username, pkey)
        else:
            transport.auth_password(username, password)

        sftp = paramiko.SFTPClient.from_transport(transport)
        Path(local_dest).mkdir(parents=True, exist_ok=True)

        def _download_dir(remote_path, local_path):
            nonlocal downloaded
            Path(local_path).mkdir(parents=True, exist_ok=True)
            try:
                for item in sftp.listdir_attr(remote_path):
                    remote_item = f"{remote_path}/{item.filename}".replace("//", "/")
                    local_item = os.path.join(local_path, item.filename)
                    import stat
                    if stat.S_ISDIR(item.st_mode):
                        _download_dir(remote_item, local_item)
                    else:
                        try:
                            sftp.get(remote_item, local_item)
                            downloaded += 1
                            if progress_cb:
                                try:
                                    progress_cb(downloaded, item.filename)
                                except Exception:
                                    pass
                        except Exception as e:
                            logger.warning(f"[sftp] Failed to download {remote_item}: {e}")
            except Exception as e:
                logger.warning(f"[sftp] Failed to list {remote_path}: {e}")

        _download_dir(remote_dir, local_dest)
        return {"status": "ok", "downloaded": downloaded, "error": None}

    except paramiko.AuthenticationException as e:
        return {"status": "error", "downloaded": 0, "error": f"SFTP auth failed: {e}"}
    except Exception as e:
        return {"status": "error", "downloaded": 0, "error": f"SFTP error: {e}"}
    finally:
        if sftp:
            sftp.close()
        if transport:
            transport.close()


def download_from_ftp(remote_dir: str, local_dest: str, ftp_config: dict,
                       progress_cb=None) -> dict:
    """
    Download a backup folder recursively from FTP/FTPS to local_dest.
    Returns { status: "ok"|"error", downloaded: int, error: str|None }

    progress_cb(downloaded: int, filename: str) — called after each file is saved.
    """
    try:
        from ftplib import FTP, FTP_TLS, all_errors
    except ImportError:
        return {"status": "error", "downloaded": 0, "error": "ftplib not available"}

    host       = ftp_config.get("host", "").strip()
    port       = int(ftp_config.get("port", 21))
    username   = (ftp_config.get("username") or ftp_config.get("user", "")).strip()
    password   = _cred_ftp(ftp_config) if _CRED_STORE else (ftp_config.get("password") or ftp_config.get("pass", ""))
    use_tls    = ftp_config.get("use_tls", True)

    if not host:
        return {"status": "error", "downloaded": 0, "error": "FTP host not configured"}
    if not username:
        return {"status": "error", "downloaded": 0, "error": "FTP username not configured"}

    ftp = None
    downloaded = 0

    try:
        FTP_Class = FTP_TLS if use_tls else FTP
        ftp = FTP_Class()
        ftp.connect(host, port, timeout=30)
        ftp.login(username, password)
        if use_tls:
            ftp.prot_p()

        Path(local_dest).mkdir(parents=True, exist_ok=True)

        def _download_dir(remote_path, local_path):
            nonlocal downloaded
            Path(local_path).mkdir(parents=True, exist_ok=True)
            try:
                ftp.cwd(remote_path)
                items = ftp.mlsd()
                for name, facts in items:
                    if name in (".", ".."):
                        continue
                    remote_item = f"{remote_path}/{name}".replace("//", "/")
                    local_item = os.path.join(local_path, name)
                    if facts.get("type") == "dir":
                        _download_dir(remote_item, local_item)
                    else:
                        try:
                            with open(local_item, "wb") as local_fh:
                                ftp.retrbinary(f"RETR {name}", local_fh.write)
                            downloaded += 1
                            if progress_cb:
                                try:
                                    progress_cb(downloaded, name)
                                except Exception:
                                    pass
                        except Exception as e:
                            logger.warning(f"[ftp] Failed to download {name}: {e}")
            except Exception as e:
                logger.warning(f"[ftp] Failed to list {remote_path}: {e}")

        _download_dir(remote_dir, local_dest)
        return {"status": "ok", "downloaded": downloaded, "error": None}

    except Exception as e:
        return {"status": "error", "downloaded": 0, "error": f"FTP error: {e}"}
    finally:
        if ftp:
            try:
                ftp.quit()
            except:
                ftp.close()


def _unc_already_accessible(unc_path: str) -> bool:
    """
    Check if a UNC path is already accessible via Windows session credentials
    (i.e. the user already authenticated via Explorer or net use).
    Only works on Windows.
    """
    if os.name != "nt":
        return False
    try:
        return os.path.exists(unc_path)
    except Exception:
        return False


def download_from_smb(remote_dir: str, local_dest: str, smb_config: dict,
                       progress_cb=None) -> dict:
    """
    Download a backup folder recursively from SMB/CIFS to local_dest.

    On Windows, if the UNC path is already accessible (user authenticated via
    Explorer or net use), copies files directly using os.walk — no credentials
    needed.  Falls back to smbprotocol with explicit credentials otherwise.

    Returns { status: "ok"|"error", downloaded: int, error: str|None }
    progress_cb(downloaded: int, filename: str) — called after each file is saved.
    """
    server    = smb_config.get("server", "").strip()
    share     = smb_config.get("share", "").strip()
    username  = (smb_config.get("username") or smb_config.get("user", "")).strip()
    password  = _cred_smb(smb_config) if _CRED_STORE else (smb_config.get("password") or smb_config.get("pass", ""))
    domain    = smb_config.get("domain", "")
    remote_base = (smb_config.get("remote_path") or "").lstrip("/\\")

    # Auto-parse server and share from UNC path (\\server\share\...) if not set in config
    if (not server or not share) and remote_dir:
        _unc = remote_dir.replace("/", "\\").lstrip("\\")
        _parts = _unc.split("\\", 2)
        if len(_parts) >= 2:
            if not server:
                server = _parts[0].strip()
            if not share:
                share = _parts[1].strip()
            if len(_parts) == 3 and not remote_base:
                remote_base = _parts[2].strip("\\")

    if not server or not share:
        return {"status": "error", "downloaded": 0, "error": "SMB server/share not configured"}

    # ── Strategy 1: Use existing Windows session (no credentials needed) ──────
    # If the UNC path is already mounted/accessible (e.g. user opened it in
    # Explorer or ran net use), copy files directly without smbprotocol.
    _unc_source = f"\\\\{server}\\{share}"
    if remote_base:
        _unc_source = f"{_unc_source}\\{remote_base}"

    if _unc_already_accessible(_unc_source):
        logger.info(f"[smb] UNC path accessible via Windows session, copying directly: {_unc_source}")
        downloaded = 0
        try:
            import shutil
            Path(local_dest).mkdir(parents=True, exist_ok=True)
            for root, dirs, files in os.walk(_unc_source):
                rel_root = os.path.relpath(root, _unc_source)
                local_root = os.path.join(local_dest, rel_root) if rel_root != "." else local_dest
                os.makedirs(local_root, exist_ok=True)
                for fname in files:
                    src_file = os.path.join(root, fname)
                    dst_file = os.path.join(local_root, fname)
                    try:
                        shutil.copy2(src_file, dst_file)
                        downloaded += 1
                        if progress_cb:
                            try:
                                progress_cb(downloaded, fname)
                            except Exception:
                                pass
                    except Exception as e:
                        logger.warning(f"[smb] Failed to copy {src_file}: {e}")
            return {"status": "ok", "downloaded": downloaded, "error": None}
        except Exception as e:
            logger.warning(f"[smb] Direct UNC copy failed ({e}), falling back to smbprotocol")

    # ── Strategy 2: smbprotocol with explicit credentials ────────────────────
    try:
        from smbclient import walk, open_file
        import smbclient
    except ImportError:
        return {"status": "error", "downloaded": 0, "error": "smbprotocol not installed"}

    if not username:
        username = "guest"

    downloaded = 0

    try:
        smbclient.register_session(server, username=username, password=password)
        Path(local_dest).mkdir(parents=True, exist_ok=True)

        def _download_dir(remote_path, local_path):
            nonlocal downloaded
            Path(local_path).mkdir(parents=True, exist_ok=True)
            try:
                for root, dirs, files in walk(f"\\\\{server}\\{share}\\{remote_path}"):
                    for fname in files:
                        remote_file = os.path.join(root, fname)
                        rel = os.path.relpath(remote_file, f"\\\\{server}\\{share}\\{remote_path}")
                        local_file = os.path.join(local_path, rel)
                        os.makedirs(os.path.dirname(local_file), exist_ok=True)
                        try:
                            with open_file(remote_file, mode="rb") as remote_fh:
                                with open(local_file, "wb") as local_fh:
                                    CHUNK = 16 * 1024 * 1024
                                    while True:
                                        chunk = remote_fh.read(CHUNK)
                                        if not chunk:
                                            break
                                        local_fh.write(chunk)
                            downloaded += 1
                            if progress_cb:
                                try:
                                    progress_cb(downloaded, fname)
                                except Exception:
                                    pass
                        except Exception as e:
                            logger.warning(f"[smb] Failed to download {remote_file}: {e}")
            except Exception as e:
                logger.warning(f"[smb] Failed to walk {remote_path}: {e}")

        _download_dir(remote_base, local_dest)
        return {"status": "ok", "downloaded": downloaded, "error": None}

    except Exception as e:
        return {"status": "error", "downloaded": 0, "error": f"SMB error: {e}"}


def download_from_rclone(remote_dir: str, local_dest: str, rclone_config: dict, progress_cb=None) -> dict:
    """
    Download a backup folder from an rclone remote to local_dest.
    remote_dir is the path on the remote, e.g. "/backups/20240101_120000"
    Returns { status: "ok"|"error", downloaded: int, error: str|None }
    """
    remote_name = (rclone_config.get("remote") or rclone_config.get("remote_name", "")).strip()
    remote_path = (rclone_config.get("path") or rclone_config.get("remote_path", "/backups")).strip()
    if not remote_name:
        return {"status": "error", "downloaded": 0, "error": "Rclone remote name not configured"}

    try:
        check = subprocess.run(["rclone", "version"], capture_output=True, text=True, timeout=15)
        if check.returncode != 0:
            return {"status": "error", "downloaded": 0, "error": "rclone not installed or not available in PATH"}
    except FileNotFoundError:
        return {"status": "error", "downloaded": 0, "error": "rclone not installed or not available in PATH"}
    except Exception as e:
        return {"status": "error", "downloaded": 0, "error": f"Failed to run rclone: {e}"}

    # remote_dir is like "/backups/20240101_120000", so source is remote_name:remote_path/remote_dir
    # But remote_path might be "/backups", and remote_dir "/backups/20240101_120000", so need to combine properly
    source = f"{remote_name}:{remote_dir.lstrip('/')}"
    cmd = ["rclone", "copy", source, str(Path(local_dest)), "--progress"]
    if os.name == "nt":
        cmd[3] = str(Path(local_dest))

    stderr_lines = []
    try:
        proc = subprocess.Popen(cmd, stderr=subprocess.PIPE, text=True, bufsize=1)
    except FileNotFoundError:
        return {"status": "error", "downloaded": 0, "error": "rclone not installed or not available in PATH"}
    except Exception as e:
        return {"status": "error", "downloaded": 0, "error": str(e)}

    try:
        if proc.stderr:
            for line in proc.stderr:
                stderr_lines.append(line.rstrip("\n"))
                if progress_cb:
                    try:
                        progress_cb(line.rstrip("\n"))
                    except Exception:
                        pass
        proc.wait(timeout=3600)
    except Exception as e:
        proc.kill()
        return {"status": "error", "downloaded": 0, "error": f"rclone failed: {e}"}

    if proc.returncode != 0:
        return {"status": "error", "downloaded": 0, "error": "rclone copy failed: " + "\n".join(stderr_lines[-10:])}

    # rclone doesn't report exact file count, so return success with 0 (meaning unknown)
    return {"status": "ok", "downloaded": 0, "error": None}


def download_from_webdav(remote_dir: str, local_dest: str, webdav_config: dict,
                          progress_cb=None) -> dict:
    """
    Download a backup folder recursively from WebDAV to local_dest.
    Returns { status: "ok"|"error", downloaded: int, error: str|None }

    progress_cb(downloaded: int, filename: str) — called after each file is saved.
    """
    import ssl
    import urllib.request
    import urllib.error
    import base64 as _b64
    import xml.etree.ElementTree as ET

    url_base    = (webdav_config.get("url") or "").rstrip("/")
    username    = (webdav_config.get("username") or webdav_config.get("user", "")).strip()
    password    = _cred_webdav(webdav_config) if _CRED_STORE else (webdav_config.get("password") or webdav_config.get("pass", ""))
    remote_path = (webdav_config.get("remote_path") or "/backups").strip("/")
    verify_ssl  = webdav_config.get("verify_ssl", True)
    webdav_root = (webdav_config.get("webdav_root") or "").rstrip("/")

    if not url_base:
        return {"status": "error", "downloaded": 0, "error": "WebDAV URL not configured"}
    if not username:
        return {"status": "error", "downloaded": 0, "error": "WebDAV username not configured"}

    _creds = _b64.b64encode(f"{username}:{password}".encode()).decode()
    _auth = f"Basic {_creds}"
    downloaded = 0

    try:
        Path(local_dest).mkdir(parents=True, exist_ok=True)

        def _propfind(path):
            """Send PROPFIND, return list of resources."""
            full = url_base + webdav_root + "/" + path.lstrip("/")
            req = urllib.request.Request(
                full,
                data=b'<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:prop><D:displayname/><D:resourcetype/></D:prop></D:propfind>',
                headers={"Authorization": _auth, "Depth": "1", "Content-Type": "application/xml"},
                method="PROPFIND",
            )
            try:
                ctx = ssl.create_default_context() if verify_ssl else ssl._create_unverified_context()
                with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
                    xml_text = resp.read().decode("utf-8", errors="ignore")
                    root = ET.fromstring(xml_text)
                    ns = {"d": "DAV:"}
                    items = []
                    for response in root.findall("d:response", ns):
                        href = response.findtext("d:href", "", ns)
                        is_dir = response.find("d:propstat/d:prop/d:resourcetype/d:collection", ns) is not None
                        items.append({"href": href, "is_dir": is_dir})
                    return items
            except Exception as e:
                logger.warning(f"[webdav] PROPFIND {path} failed: {e}")
                return []

        def _get_file(remote_file, local_file):
            """GET a single file."""
            full = url_base + webdav_root + "/" + remote_file.lstrip("/")
            req = urllib.request.Request(full, headers={"Authorization": _auth}, method="GET")
            try:
                ctx = ssl.create_default_context() if verify_ssl else ssl._create_unverified_context()
                with urllib.request.urlopen(req, context=ctx, timeout=60) as resp:
                    with open(local_file, "wb") as local_fh:
                        local_fh.write(resp.read())
                return True
            except Exception as e:
                logger.warning(f"[webdav] GET {remote_file}: {e}")
                return False

        def _download_dir(remote_path_rel, local_path_rel):
            nonlocal downloaded
            local_full = os.path.join(local_dest, local_path_rel)
            Path(local_full).mkdir(parents=True, exist_ok=True)
            items = _propfind(remote_path_rel)
            for item in items:
                href = item["href"].rstrip("/")
                name = href.split("/")[-1]
                if not name:
                    continue
                remote_sub = f"{remote_path_rel}/{name}".lstrip("/")
                local_sub = os.path.join(local_path_rel, name) if local_path_rel else name
                if item["is_dir"]:
                    _download_dir(remote_sub, local_sub)
                else:
                    local_file_full = os.path.join(local_dest, local_sub)
                    if _get_file(remote_sub, local_file_full):
                        downloaded += 1
                        if progress_cb:
                            try:
                                progress_cb(downloaded, name)
                            except Exception:
                                pass

        _download_dir(remote_path, "")
        return {"status": "ok", "downloaded": downloaded, "error": None}

    except Exception as e:
        return {"status": "error", "downloaded": 0, "error": f"WebDAV error: {e}"}


def download_from_https(remote_dir: str, local_dest: str, https_config: dict,
                         progress_cb=None) -> dict:
    """
    Download a backup folder from an HTTPS API endpoint via GET requests.
    
    The server is expected to provide a manifest listing files at:
        GET {api_url}/manifest
    And individual files at:
        GET {api_url}/files/{filename}
    
    Both endpoints use Bearer token authentication.
    
    https_config: { url, token, headers(dict), verify_ssl(=true) }
    
    Returns { status: "ok"|"error", downloaded: int, error: str|None }

    progress_cb(downloaded: int, filename: str) — called after each file is saved.
    """
    import ssl
    import json as _json
    
    url        = https_config.get("url", "").strip()
    token      = https_config.get("token", "").strip()
    extra_hdrs = https_config.get("headers", {}) or {}
    verify_ssl = bool(https_config.get("verify_ssl", True))
    
    if not url:
        return {"status": "error", "downloaded": 0, "error": "HTTPS download URL not configured"}
    
    # Remove trailing slash for consistency
    url = url.rstrip("/")
    
    ssl_ctx = ssl.create_default_context()
    if not verify_ssl:
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode    = ssl.CERT_NONE
    
    downloaded = 0
    errors     = []
    
    try:
        Path(local_dest).mkdir(parents=True, exist_ok=True)
        
        # Prepare headers
        hdrs = {"Accept": "application/json"}
        if token:
            hdrs["Authorization"] = f"Bearer {token}"
        hdrs.update(extra_hdrs)
        
        # Fetch manifest to get list of files
        try:
            # Pass the backup folder name so the server knows which snapshot
            # to list.  remote_dir is the backup folder name (e.g.
            # "mywatch_20260504_120000") — URL-encode it for safety.
            import urllib.parse as _uparse
            _bdir_q = _uparse.urlencode({"backup_dir": remote_dir})
            manifest_url = f"{url}/manifest?{_bdir_q}"
            req = urllib.request.Request(manifest_url, headers=hdrs)
            with urllib.request.urlopen(req, context=ssl_ctx, timeout=30) as resp:
                manifest_data = _json.loads(resp.read().decode('utf-8'))
            
            # Manifest should be a dict with "files" list
            # Each file entry should have "path" and optionally "size"
            files_to_download = manifest_data.get("files", [])
            if not isinstance(files_to_download, list):
                files_to_download = []
                
        except Exception as e:
            logger.warning(f"[https] Failed to fetch manifest: {e}")
            return {"status": "error", "downloaded": 0, "error": f"Failed to fetch manifest: {e}"}
        
        # Download each file
        for file_entry in files_to_download:
            if isinstance(file_entry, str):
                file_path = file_entry
            else:
                file_path = file_entry.get("path", "")
            
            if not file_path:
                continue
            
            local_file = os.path.join(local_dest, file_path.replace("/", os.sep))
            os.makedirs(os.path.dirname(local_file), exist_ok=True)
            
            try:
                # Prepare file download request.
                # The server stores files under /files/<backup_dir>/<rel_path>.
                _rel_fwd = file_path.replace(os.sep, "/")
                file_url = f"{url}/files/{remote_dir}/{_rel_fwd}"
                req = urllib.request.Request(file_url, headers=hdrs)
                
                # Download with streaming (16 MB chunks)
                _CHUNK = 16 * 1024 * 1024
                with urllib.request.urlopen(req, context=ssl_ctx, timeout=120) as resp:
                    with open(local_file, "wb") as f_out:
                        while True:
                            chunk = resp.read(_CHUNK)
                            if not chunk:
                                break
                            f_out.write(chunk)
                
                downloaded += 1
                if progress_cb:
                    try:
                        progress_cb(downloaded, file_path)
                    except Exception:
                        pass
                logger.debug(f"[https] Downloaded {file_path}")
                
            except Exception as e:
                error_msg = f"{file_path}: {e}"
                errors.append(error_msg)
                logger.warning(f"[https] Failed to download {error_msg}")
        
        if errors:
            logger.warning(f"[https] {len(errors)} file(s) failed to download: {errors[:5]}")
        
        ok = downloaded > 0 or (downloaded == 0 and not files_to_download)
        logger.info(f"[https] Downloaded {downloaded} file(s) from {url}")
        return {
            "status":     "ok" if ok else "error",
            "downloaded": downloaded,
            "error":      None if ok else (errors[0] if errors else "No files downloaded"),
        }
    
    except Exception as e:
        return {"status": "error", "downloaded": 0, "error": f"HTTPS error: {e}"}