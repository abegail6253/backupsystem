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

def upload_to_sftp(local_dir: str, sftp_config: dict, progress_cb=None) -> dict:
    """
    Upload a backup folder to an SFTP server using Paramiko.

    Recreates the full subdirectory tree under remote_path/<backup_folder_name>/.
    Supports both password auth and private-key auth.

    progress_cb(bytes_done, total_bytes, filename) — optional, called per chunk.
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
                with open(str(fp), "rb") as fh:
                    if progress_cb:
                        # Chunked so progress fires regularly on large files
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
                        _bytes_done += fp.stat().st_size
                uploaded += 1
            except Exception as e:
                logger.warning(f"[sftp] Failed to upload {rel}: {e}")

        logger.info(f"[sftp] Uploaded {uploaded} file(s) to {host}:{remote_base}/{ld.name}")

        # ── Post-upload verification: remote file count must match local ──────
        _expected = len(_all_files)
        if _expected > 0 and uploaded != _expected:
            _missing = _expected - uploaded
            logger.warning(
                f"[sftp] Verification warning: expected {_expected} file(s), "
                f"only {uploaded} confirmed uploaded ({_missing} may have failed silently)"
            )
            return {
                "ok": True,
                "uploaded": uploaded,
                "path": f"{remote_base}/{ld.name}",
                "warning": f"{_missing} file(s) may not have uploaded correctly "
                           f"({uploaded}/{_expected} confirmed)",
            }

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

def upload_to_ftp(local_dir: str, ftp_config: dict, progress_cb=None) -> dict:
    """
    Upload a backup folder to an FTP/FTPS server using ftplib (stdlib).

    use_tls=True (default) uses explicit TLS (FTPS).  Set to False for plain FTP.
    Recreates the full directory tree under remote_path/<backup_folder_name>/.

    progress_cb(bytes_done, total_bytes, filename) — optional, called per chunk.
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
                if progress_cb:
                    # Wrap file in a callback-firing reader
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
                    _bytes_done += fp.stat().st_size
                uploaded += 1
            except Exception as e:
                logger.warning(f"[ftp] Failed to upload {rel}: {e}")

        proto = "FTPS" if use_tls else "FTP"
        logger.info(f"[ftp] {proto} uploaded {uploaded} file(s) to {host}:{remote_base}/{ld.name}")

        # ── Post-upload verification ──────────────────────────────────────────
        _expected = len(_all_files)
        if _expected > 0 and uploaded != _expected:
            _missing = _expected - uploaded
            logger.warning(
                f"[ftp] Verification warning: expected {_expected} file(s), "
                f"only {uploaded} confirmed uploaded"
            )
            return {
                "ok": True,
                "uploaded": uploaded,
                "path": f"{remote_base}/{ld.name}",
                "warning": f"{_missing} file(s) may not have uploaded correctly "
                           f"({uploaded}/{_expected} confirmed)",
            }

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

def upload_to_smb(local_dir: str, smb_config: dict, progress_cb=None) -> dict:
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
                with open(str(_smb_fp), "rb") as _src_f, open(str(_smb_dest), "wb") as _dst_f:
                    while True:
                        _buf = _src_f.read(_SMB_BUF)
                        if not _buf:
                            break
                        _dst_f.write(_buf)
                        _bytes_done += len(_buf)
                        if progress_cb:
                            try:
                                progress_cb(_bytes_done, _total_bytes, _smb_fp.name)
                            except Exception:
                                pass
                _sh.copystat(str(_smb_fp), str(_smb_dest))
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
                _smb_write_tracked(rel, fp)
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

def upload_to_https(local_dir: str, https_config: dict, progress_cb=None) -> dict:
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
    path_qs   = parsed.path + (("?" + parsed.query) if parsed.query else "")
    use_https = parsed.scheme.lower() == "https"

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
            conn = (
                http.client.HTTPSConnection(host, context=ssl_ctx, timeout=120)
                if use_https
                else http.client.HTTPConnection(host, timeout=120)
            )
            status = _stream_multipart(conn, fp, rel)
            conn.close()
            if status not in (200, 201, 202, 204):
                errors.append(f"{rel}: HTTP {status}")
            else:
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
    import re as _re

    # Accept both transport_utils convention (server/share/username/password)
    # and desktop_app convention (path=full UNC / user / pass).
    server   = smb_config.get("server", "").strip()
    share    = smb_config.get("share", "").strip()
    username = (smb_config.get("username") or smb_config.get("user", "")).strip()
    password = smb_config.get("password") or smb_config.get("pass", "")
    domain   = smb_config.get("domain", "")

    # If server/share missing, parse them from the full UNC path key
    full_path = smb_config.get("path", "").strip()
    if (not server or not share) and full_path:
        m = _re.match(r"[/\\\\]{2}([^/\\\\]+)[/\\\\]([^/\\\\]+)", full_path)
        if m:
            server, share = m.group(1), m.group(2)

    # remote_base: for desktop_app SMB config the UNC IS the destination root — no subfolder.
    remote_base = smb_config.get("remote_path", "").strip().strip("/\\")

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

# ─── WebDAV / Nextcloud / ownCloud ────────────────────────────────────────────

def upload_to_webdav(local_dir: str, webdav_config: dict, progress_cb=None) -> dict:
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
                if _put_file(remote_file, fp, _prog):
                    uploaded += 1
                else:
                    logger.warning(f"[webdav] Failed to upload {fp.name}")

    except Exception as e:
        logger.error(f"[webdav] Upload failed: {e}")
        return {"ok": False, "uploaded": uploaded, "path": dest_root, "error": str(e)}

    ok = uploaded > 0 or len([f for f in ld.rglob("*") if f.is_file() and f.name not in _SKIP]) == 0
    return {
        "ok":       ok,
        "uploaded": uploaded,
        "path":     dest_root,
        "error":    None if ok else "No files uploaded",
    }


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
