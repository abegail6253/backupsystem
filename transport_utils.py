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
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


# ─── SFTP ─────────────────────────────────────────────────────────────────────

def upload_to_sftp(local_dir: str, sftp_config: dict) -> dict:
    """
    Upload a backup folder to an SFTP server using Paramiko.

    Recreates the full subdirectory tree under remote_path/<backup_folder_name>/.
    Supports both password auth and private-key auth.
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
    password   = sftp_config.get("password") or sftp_config.get("pass", "")
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

    try:
        transport = paramiko.Transport((host, port))
        transport.connect()  # TCP only — auth follows

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

        for fp in ld.rglob("*"):
            if not fp.is_file():
                continue
            if fp.name in _SKIP:
                continue
            rel         = fp.relative_to(ld)
            remote_file = f"{remote_base}/{ld.name}/{str(rel).replace(os.sep, '/')}"
            remote_dir  = str(Path(remote_file).parent).replace("\\", "/")
            _mkdir_p(remote_dir)
            try:
                sftp.put(str(fp), remote_file)
                uploaded += 1
            except Exception as e:
                logger.warning(f"[sftp] Failed to upload {rel}: {e}")

        logger.info(f"[sftp] Uploaded {uploaded} file(s) to {host}:{remote_base}/{ld.name}")
        return {"ok": True, "uploaded": uploaded, "path": f"{remote_base}/{ld.name}"}

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

def upload_to_ftp(local_dir: str, ftp_config: dict) -> dict:
    """
    Upload a backup folder to an FTP/FTPS server using ftplib (stdlib).

    use_tls=True (default) uses explicit TLS (FTPS).  Set to False for plain FTP.
    Recreates the full directory tree under remote_path/<backup_folder_name>/.
    """
    import ftplib

    host        = ftp_config.get("host", "").strip()
    port        = int(ftp_config.get("port", 21))
    username    = (ftp_config.get("username") or ftp_config.get("user", "")).strip()
    password    = ftp_config.get("password") or ftp_config.get("pass", "")
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

        for fp in ld.rglob("*"):
            if not fp.is_file():
                continue
            if fp.name in _SKIP:
                continue
            rel         = fp.relative_to(ld)
            parts       = list(rel.parts)
            remote_dir  = f"{remote_base}/{ld.name}" + (
                ("/" + "/".join(parts[:-1])) if len(parts) > 1 else ""
            )
            _ftp_makedirs(remote_dir)
            try:
                ftp.cwd("/" + remote_dir.lstrip("/"))
                with open(fp, "rb") as f:
                    ftp.storbinary(f"STOR {fp.name}", f)
                uploaded += 1
            except Exception as e:
                logger.warning(f"[ftp] Failed to upload {rel}: {e}")

        proto = "FTPS" if use_tls else "FTP"
        logger.info(f"[ftp] {proto} uploaded {uploaded} file(s) to {host}:{remote_base}/{ld.name}")
        return {"ok": True, "uploaded": uploaded, "path": f"{remote_base}/{ld.name}"}

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

def upload_to_smb(local_dir: str, smb_config: dict) -> dict:
    """
    Upload a backup folder to an SMB/CIFS network share.

    On Windows:  Uses UNC paths directly (\\\\server\\share\\...) — no extra lib needed.
    On Linux/Mac: Falls back to smbprotocol (must be installed: pip install smbprotocol).

    smb_config: { server, share, username, password, domain, remote_path }
    """
    server      = smb_config.get("server", "").strip()
    share       = smb_config.get("share", "").strip()
    username    = smb_config.get("username", "").strip()
    password    = smb_config.get("password", "")
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
            # Skip internal metadata files
            _SKIP = {"MANIFEST.json", "BACKUP.sha256"}
            uploaded = 0
            for _smb_fp in ld.rglob("*"):
                if not _smb_fp.is_file() or _smb_fp.name in _SKIP:
                    continue
                _smb_rel  = _smb_fp.relative_to(ld)
                _smb_dest = remote_dir / _smb_rel
                _smb_dest.parent.mkdir(parents=True, exist_ok=True)
                _sh.copy2(str(_smb_fp), str(_smb_dest))
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

        SMB_CHUNK = 1024 * 1024  # 1 MB write chunks — avoids loading large files into RAM

        def _smb_write(rel_path: str, local_fp: Path):
            rel_win     = rel_path.replace('/', '\\')
            remote_path = f"{remote_base}\\{ld.name}\\{rel_win}".lstrip("\\")
            # Ensure parent dirs exist
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
            # Write file in chunks to avoid loading large files into RAM
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
            f_handle.close(False)

        _SKIP = {"MANIFEST.json", "BACKUP.sha256"}
        for fp in ld.rglob("*"):
            if not fp.is_file():
                continue
            if fp.name in _SKIP:
                continue
            rel = str(fp.relative_to(ld))
            try:
                _smb_write(rel, fp)
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

def upload_to_https(local_dir: str, https_config: dict) -> dict:
    """
    Upload each file in a backup folder to an HTTPS endpoint via multipart POST.

    Supports Bearer token auth and custom headers.
    verify_ssl=False disables certificate verification (useful for self-signed certs).

    https_config: { url, token, headers(dict), verify_ssl(=true) }

    The server receives multipart/form-data with:
        file       — the binary file content
        filename   — relative path inside the backup folder
        backup_dir — the top-level backup folder name
    """
    import urllib.request
    import urllib.error
    import ssl
    import json as _json

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

    ld       = Path(local_dir)
    uploaded = 0
    errors   = []

    def _multipart_encode(file_path: Path, field_name: str, filename: str, extra_fields: dict):
        """
        Build a multipart/form-data body.
        NOTE: The full body is assembled in memory. Files > 50 MB are warned;
        files > 200 MB are rejected to prevent OOM. Use SFTP for large files.
        """
        MAX_HTTPS_BYTES = 200 * 1024 * 1024   # 200 MB hard limit
        WARN_HTTPS_BYTES = 50 * 1024 * 1024   # 50 MB soft warning
        file_size = file_path.stat().st_size if file_path.exists() else 0
        if file_size > MAX_HTTPS_BYTES:
            raise ValueError(
                f"File too large for HTTPS multipart upload ({file_size // (1024*1024)} MB). "
                "Hard limit is 200 MB — use SFTP/FTP for large files."
            )
        if file_size > WARN_HTTPS_BYTES:
            logger.warning(
                f"[https] {file_path.name} is {file_size // (1024*1024)} MB — "
                "multipart upload requires the full body in memory; this may be slow."
            )

        boundary = "----BackupSysBoundary" + os.urandom(8).hex()
        CRLF = b"\r\n"
        chunks = []

        # Extra text fields
        for k, v in extra_fields.items():
            chunks.append(f"--{boundary}".encode() + CRLF)
            chunks.append(f'Content-Disposition: form-data; name="{k}"'.encode() + CRLF)
            chunks.append(CRLF)
            chunks.append(str(v).encode() + CRLF)

        # File field header
        chunks.append(f"--{boundary}".encode() + CRLF)
        chunks.append(
            f'Content-Disposition: form-data; name="{field_name}"; filename="{filename}"'.encode() + CRLF
        )
        chunks.append(b"Content-Type: application/octet-stream" + CRLF)
        chunks.append(CRLF)

        with open(file_path, "rb") as fh:
            chunks.append(fh.read())

        chunks.append(CRLF)
        chunks.append(f"--{boundary}--".encode() + CRLF)

        body = b"".join(chunks)
        ct   = f"multipart/form-data; boundary={boundary}"
        return body, ct

    _SKIP = {"MANIFEST.json", "BACKUP.sha256"}
    for fp in ld.rglob("*"):
        if not fp.is_file():
            continue
        if fp.name in _SKIP:
            continue
        rel  = str(fp.relative_to(ld)).replace("\\", "/")
        body, ct = _multipart_encode(
            fp, "file", rel,
            {"filename": rel, "backup_dir": ld.name}
        )

        headers = {"Content-Type": ct}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        headers.update(extra_hdrs)

        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, context=ssl_ctx, timeout=60) as resp:
                status = resp.status
                if status not in (200, 201, 202, 204):
                    errors.append(f"{rel}: HTTP {status}")
                else:
                    uploaded += 1
        except urllib.error.HTTPError as e:
            errors.append(f"{rel}: HTTP {e.code} {e.reason}")
        except urllib.error.URLError as e:
            errors.append(f"{rel}: {e.reason}")
        except Exception as e:
            errors.append(f"{rel}: {e}")

    if errors:
        logger.warning(f"[https] {len(errors)} file(s) failed to upload: {errors[:5]}")

    ok = uploaded > 0 or (uploaded == 0 and len(list(ld.rglob("*"))) == 0)
    logger.info(f"[https] Uploaded {uploaded} file(s) to {url}")
    return {
        "ok": ok,
        "uploaded": uploaded,
        "errors": errors[:20],   # cap to avoid enormous log entries
        "path": url,
    }

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

# ─── Remote Retention Cleanup ─────────────────────────────────────────────────

from datetime import datetime, timedelta

def _parse_backup_ts(folder_name: str):
    """Parse timestamp from backup folder name like 20260312_080607_..."""
    try:
        return datetime.strptime(folder_name[:15], "%Y%m%d_%H%M%S")
    except Exception:
        return None


def cleanup_remote_sftp(sftp_config: dict, retention_days: int, watch_id: str = "") -> dict:
    """Delete old backup folders from SFTP server older than retention_days."""
    try:
        import paramiko
    except ImportError:
        return {"ok": False, "deleted": 0, "error": "paramiko not installed"}

    host        = sftp_config.get("host", "").strip()
    port        = int(sftp_config.get("port", 22))
    username    = (sftp_config.get("username") or sftp_config.get("user", "")).strip()
    password    = sftp_config.get("password") or sftp_config.get("pass", "")
    key_path    = (sftp_config.get("key_path") or sftp_config.get("keyfile", "")).strip()
    key_pass    = sftp_config.get("key_passphrase") or sftp_config.get("key_pass", "")
    remote_base = (sftp_config.get("remote_path") or sftp_config.get("path", "/backups")).rstrip("/")

    transport = sftp = None
    result = {"ok": True, "deleted": 0, "freed_bytes": 0, "errors": []}
    cutoff = datetime.now() - timedelta(days=retention_days)

    try:
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

        sftp = paramiko.SFTPClient.from_transport(transport)

        def _rm_rf(remote_path):
            """Recursively delete a remote directory."""
            try:
                for item in sftp.listdir_attr(remote_path):
                    rp = f"{remote_path}/{item.filename}"
                    import stat
                    if stat.S_ISDIR(item.st_mode):
                        _rm_rf(rp)
                    else:
                        sftp.remove(rp)
                sftp.rmdir(remote_path)
            except Exception as e:
                result["errors"].append(str(e))

        for entry in sftp.listdir(remote_base):
            if watch_id and watch_id not in entry:
                continue
            ts = _parse_backup_ts(entry)
            if ts and ts < cutoff:
                remote_dir = f"{remote_base}/{entry}"
                try:
                    # Estimate size before deleting
                    for f in sftp.listdir_attr(remote_dir):
                        result["freed_bytes"] += getattr(f, "st_size", 0)
                except Exception:
                    pass
                _rm_rf(remote_dir)
                result["deleted"] += 1
                logger.info(f"[sftp-retention] Deleted old backup: {remote_dir}")

    except Exception as e:
        result["ok"] = False
        result["error"] = str(e)
    finally:
        try:
            if sftp: sftp.close()
            if transport: transport.close()
        except Exception:
            pass

    return result


def cleanup_remote_ftp(ftp_config: dict, retention_days: int, watch_id: str = "") -> dict:
    """Delete old backup folders from FTP/FTPS server older than retention_days."""
    import ftplib
    host        = ftp_config.get("host", "").strip()
    port        = int(ftp_config.get("port", 21))
    username    = (ftp_config.get("username") or ftp_config.get("user", "")).strip()
    password    = ftp_config.get("password") or ftp_config.get("pass", "")
    remote_base = (ftp_config.get("remote_path") or ftp_config.get("path", "/backups")).rstrip("/")
    use_tls     = bool(ftp_config.get("use_tls", True))

    result = {"ok": True, "deleted": 0, "freed_bytes": 0, "errors": []}
    cutoff = datetime.now() - timedelta(days=retention_days)

    try:
        ftp = ftplib.FTP_TLS() if use_tls else ftplib.FTP()
        ftp.connect(host, port, timeout=30)
        ftp.login(username, password)
        if use_tls:
            ftp.prot_p()

        def _rm_rf_ftp(path):
            try:
                items = []
                ftp.retrlines(f"LIST {path}", items.append)
                for line in items:
                    parts = line.split()
                    name  = parts[-1]
                    rp    = f"{path}/{name}"
                    if line.startswith("d"):
                        _rm_rf_ftp(rp)
                    else:
                        result["freed_bytes"] += int(parts[4]) if len(parts) > 4 else 0
                        ftp.delete(rp)
                ftp.rmd(path)
            except Exception as e:
                result["errors"].append(str(e))

        folders = []
        ftp.retrlines(f"LIST {remote_base}", folders.append)
        for line in folders:
            name = line.split()[-1]
            if watch_id and watch_id not in name:
                continue
            ts = _parse_backup_ts(name)
            if ts and ts < cutoff:
                _rm_rf_ftp(f"{remote_base}/{name}")
                result["deleted"] += 1
                logger.info(f"[ftp-retention] Deleted old backup: {remote_base}/{name}")

        ftp.quit()
    except Exception as e:
        result["ok"] = False
        result["error"] = str(e)

    return result


def cleanup_remote_smb(smb_config: dict, retention_days: int, watch_id: str = "") -> dict:
    """Delete old backup folders from SMB share older than retention_days."""
    import shutil as _sh
    server      = smb_config.get("server", "").strip()
    share       = smb_config.get("share", "").strip()
    username    = smb_config.get("username", "").strip()
    password    = smb_config.get("password", "")
    domain      = smb_config.get("domain", "")
    remote_base = smb_config.get("remote_path", "backups").strip().strip("/\\")

    result = {"ok": True, "deleted": 0, "freed_bytes": 0, "errors": []}
    cutoff = datetime.now() - timedelta(days=retention_days)

    if os.name == "nt":
        import subprocess
        unc_root = f"\\\\{server}\\{share}"
        if username:
            net_user = f"{domain}\\{username}" if domain else username
            try:
                subprocess.run(["net", "use", unc_root, f"/user:{net_user}", password],
                               capture_output=True, timeout=15, check=False)
            except Exception:
                pass
        base_path = Path(unc_root) / remote_base
        try:
            for d in base_path.iterdir():
                if not d.is_dir():
                    continue
                if watch_id and watch_id not in d.name:
                    continue
                ts = _parse_backup_ts(d.name)
                if ts and ts < cutoff:
                    result["freed_bytes"] += sum(f.stat().st_size for f in d.rglob("*") if f.is_file())
                    _sh.rmtree(str(d))
                    result["deleted"] += 1
                    logger.info(f"[smb-retention] Deleted old backup: {d}")
        except Exception as e:
            result["ok"] = False
            result["error"] = str(e)
    else:
        result["ok"] = False
        result["error"] = "SMB retention cleanup on Linux requires smbprotocol — not yet implemented"

    return result


def cleanup_remote_backups(cfg: dict, retention_days: int, watch_id: str = "") -> dict:
    """
    Unified entry point — calls the right cleanup function based on dest_type.
    Returns { ok, deleted, freed_bytes, error }.
    HTTPS is skipped (no standard delete API).
    """
    dest_type = cfg.get("dest_type", "local")

    if dest_type in ("sftp", "ftps"):
        return cleanup_remote_sftp(cfg.get("dest_sftp", {}), retention_days, watch_id)
    elif dest_type == "ftp":
        return cleanup_remote_ftp(cfg.get("dest_ftp", {}), retention_days, watch_id)
    elif dest_type == "smb":
        return cleanup_remote_smb(cfg.get("dest_smb", {}), retention_days, watch_id)
    elif dest_type == "https":
        return {"ok": True, "deleted": 0, "freed_bytes": 0,
                "error": "HTTPS retention skipped — no standard delete API"}
    else:
        return {"ok": True, "deleted": 0, "freed_bytes": 0}