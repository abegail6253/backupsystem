"""
Backup System  · Windows Desktop App
PyQt5-based system tray app with dashboard + admin panel.
Place this file in the same folder as backup_engine.py, config_manager.py, watcher.py
"""

import sys
import os
import shutil
import threading
import hashlib
import json
import socket
import logging
from datetime import datetime
from pathlib import Path

# winreg is Windows-only  · guard so the module can at least be imported on other platforms
try:
    import winreg
    WINREG_AVAILABLE = True
except ImportError:
    WINREG_AVAILABLE = False

# ── Load .env file at startup ──────────────────────────────────────────────────
# This makes BACKUPSYS_EMAIL_PASSWORD, BACKUPSYS_ENCRYPT_KEY_*, BACKUPSYS_DATA_DIR
# etc. work when set in a .env file next to the script (or in BACKUPSYS_DATA_DIR).
# Must run before config_manager.load() so env-var overrides take effect.
def _load_dotenv():
    # Accepted filenames in priority order.
    # Prefer ".env" (standard); "_env" is supported as a legacy fallback.
    # Rename your _env file to .env  · it will not be loaded by other tools otherwise.
    _env_candidates = [
        Path(__file__).parent / ".env",
        Path(__file__).parent / "_env",          # legacy fallback  · rename to .env
        Path(os.environ.get("BACKUPSYS_DATA_DIR", "")) / ".env" if os.environ.get("BACKUPSYS_DATA_DIR") else None,
    ]
    for _env_path in _env_candidates:
        if _env_path and _env_path.exists():
            try:
                from dotenv import load_dotenv
                load_dotenv(dotenv_path=_env_path, override=False)
                break
            except ImportError:
                # Fallback: manual parse (no python-dotenv installed)
                for _line in _env_path.read_text(encoding="utf-8").splitlines():
                    _line = _line.strip()
                    if not _line or _line.startswith("#") or "=" not in _line:
                        continue
                    _k, _, _v = _line.partition("=")
                    _k = _k.strip()
                    _v = _v.strip().strip("\"'")
                    if _k and _k not in os.environ:
                        os.environ[_k] = _v
                break

_load_dotenv()

# ── PyQt5 imports ─────────────────────────────────────────────────────────────
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QSystemTrayIcon, QMenu, QAction,
    QVBoxLayout, QHBoxLayout, QLabel, QPushButton, QFrame, QScrollArea,
    QDialog, QLineEdit, QFormLayout, QDialogButtonBox, QMessageBox,
    QFileDialog, QCheckBox, QSpinBox, QDoubleSpinBox, QTableWidget, QTableWidgetItem,
    QHeaderView, QSizePolicy, QStackedWidget, QProgressBar, QTextEdit,
    QSplitter, QComboBox, QGroupBox, QTabWidget, QToolButton, QStyle
)
from PyQt5.QtCore import (
    Qt, QTimer, QThread, pyqtSignal, QSize, QSettings, QPoint
)
from PyQt5.QtGui import (
    QIcon, QFont, QColor, QPalette, QPixmap, QPainter, QBrush,
    QLinearGradient, QFontDatabase
)

# ── Local imports ──────────────────────────────────────────────────────────────
try:
    import config_manager
    import backup_engine
    from watcher import WatcherManager
    BACKEND_AVAILABLE = True
except ImportError as e:
    BACKEND_AVAILABLE = False
    _IMPORT_ERROR = str(e)

# ── Constants ──────────────────────────────────────────────────────────────────
APP_NAME        = "Backup System"
APP_VERSION     = "2.0"
ADMIN_PASS_KEY  = "admin_password_hash"
SETTINGS_ORG    = "BackupSystem"
SETTINGS_APP    = "BackupSystem"
STARTUP_REG_KEY = r"Software\Microsoft\Windows\CurrentVersion\Run"
TRAY_ICON_SIZE  = 64

# ── Logging setup ──────────────────────────────────────────────────────────────
def _setup_logging():
    """Configure logging to both console and a rotating log file."""
    import logging.handlers
    log_dir  = Path(os.environ.get("BACKUPSYS_DATA_DIR", Path(__file__).parent)) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "backupsys.log"

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(name)s  · %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Rotating file handler: 2 MB per file, keep 5 files
    fh = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=2 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    fh.setFormatter(fmt)
    root.addHandler(fh)

    # Console handler (visible when running from terminal or during development)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.WARNING)   # Only warnings+ to console in production
    ch.setFormatter(fmt)
    root.addHandler(ch)

_setup_logging()
logger = logging.getLogger(__name__)

# ── Stylesheet ─────────────────────────────────────────────────────────────────
DARK_STYLE = """
QMainWindow, QDialog, QWidget {
    background-color: #1a1d23;
    color: #e8eaf0;
    font-family: 'Segoe UI', sans-serif;
    font-size: 13px;
}
QFrame#card {
    background-color: #22262f;
    border-radius: 10px;
    border: 1px solid #2e3340;
}
QFrame#topbar {
    background-color: #141720;
    border-bottom: 1px solid #2e3340;
}
QPushButton {
    background-color: #2563eb;
    color: white;
    border: none;
    border-radius: 6px;
    padding: 8px 18px;
    font-weight: 600;
    font-size: 12px;
}
QPushButton:hover   { background-color: #1d4ed8; }
QPushButton:pressed { background-color: #1e40af; }
QPushButton#danger  { background-color: #dc2626; }
QPushButton#danger:hover { background-color: #b91c1c; }
/* Table cell buttons: reduce padding so text is never clipped in tight cells */
QTableWidget QPushButton {
    padding: 4px 6px;
    border-radius: 4px;
    font-size: 12px;
    font-weight: 600;
}
QPushButton#secondary {
    background-color: #2e3340;
    color: #9ca3af;
    border: 1px solid #3d4455;
}
QPushButton#secondary:hover { background-color: #3d4455; color: #e8eaf0; }
QPushButton#success { background-color: #16a34a; }
QPushButton#success:hover { background-color: #15803d; }
QLineEdit, QSpinBox, QComboBox {
    background-color: #2e3340;
    border: 1px solid #3d4455;
    border-radius: 6px;
    padding: 7px 10px;
    color: #e8eaf0;
}
QLineEdit:focus, QSpinBox:focus, QComboBox:focus {
    border-color: #2563eb;
}
QTableWidget {
    background-color: #22262f;
    border: none;
    gridline-color: #2e3340;
    border-radius: 6px;
    outline: none;
}
QTableWidget::item {
    padding: 8px 12px;
    border-bottom: 1px solid #2e3340;
}
QTableWidget::item:selected {
    background-color: #1e3a5f;
    color: white;
}
QHeaderView::section {
    background-color: #1a1d23;
    color: #6b7280;
    font-weight: 600;
    font-size: 11px;
    text-transform: uppercase;
    padding: 8px 12px;
    border: none;
    border-bottom: 1px solid #2e3340;
    letter-spacing: 0.05em;
}
QScrollBar:vertical {
    background: #1a1d23;
    width: 8px;
    border-radius: 4px;
}
QScrollBar::handle:vertical {
    background: #3d4455;
    border-radius: 4px;
    min-height: 20px;
}
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0; }
QLabel#heading {
    font-size: 18px;
    font-weight: 700;
    color: #f1f3f9;
}
QLabel#subheading {
    font-size: 13px;
    color: #6b7280;
}
QLabel#status_ok  { color: #22c55e; font-weight: 600; }
QLabel#status_err { color: #ef4444; font-weight: 600; }
QLabel#status_warn{ color: #f59e0b; font-weight: 600; }
QProgressBar {
    background-color: #2e3340;
    border-radius: 4px;
    height: 6px;
    border: none;
    text-align: center;
    color: transparent;
}
QProgressBar::chunk {
    background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
        stop:0 #2563eb, stop:1 #7c3aed);
    border-radius: 4px;
}
QTabWidget::pane {
    border: 1px solid #2e3340;
    border-radius: 8px;
    background-color: #22262f;
}
QTabBar::tab {
    background-color: #1a1d23;
    color: #6b7280;
    padding: 8px 10px;
    min-width: 80px;
    border-top-left-radius: 6px;
    border-top-right-radius: 6px;
    font-weight: 600;
}
QTabBar::tab:selected {
    background-color: #22262f;
    color: #f1f3f9;
    border-bottom: 2px solid #2563eb;
}
QTabBar::tab:hover:!selected { color: #d1d5db; }
QGroupBox {
    border: 1px solid #2e3340;
    border-radius: 8px;
    margin-top: 14px;
    padding-top: 10px;
    color: #9ca3af;
    font-weight: 600;
    font-size: 11px;
}
QGroupBox::title {
    subcontrol-origin: margin;
    left: 12px;
    padding: 0 4px;
}
QCheckBox { color: #d1d5db; spacing: 8px; }
QCheckBox::indicator {
    width: 16px; height: 16px;
    border-radius: 4px;
    border: 1px solid #3d4455;
    background: #2e3340;
}
QCheckBox::indicator:checked {
    background-color: #2563eb;
    border-color: #2563eb;
    image: url(none);
}
QTextEdit {
    background-color: #141720;
    border: 1px solid #2e3340;
    border-radius: 6px;
    color: #9ca3af;
    font-family: 'Consolas', monospace;
    font-size: 11px;
    padding: 8px;
}
QMenu {
    background-color: #22262f;
    border: 1px solid #2e3340;
    border-radius: 8px;
    padding: 4px;
}
QMenu::item {
    padding: 8px 20px;
    border-radius: 4px;
    color: #e8eaf0;
}
QMenu::item:selected { background-color: #2563eb; }
QMenu::separator { background-color: #2e3340; height: 1px; margin: 4px 8px; }
"""


# ══════════════════════════════════════════════════════════════════════════════
# ── Tray Icon Generator ────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

def make_tray_icon(status: str = "ok") -> QIcon:
    """Generate a simple colored tray icon."""
    pix = QPixmap(TRAY_ICON_SIZE, TRAY_ICON_SIZE)
    pix.fill(Qt.transparent)
    painter = QPainter(pix)
    painter.setRenderHint(QPainter.Antialiasing)
    color = {"ok": "#22c55e", "warn": "#f59e0b", "error": "#ef4444", "busy": "#2563eb"}.get(status, "#22c55e")
    painter.setBrush(QBrush(QColor(color)))
    painter.setPen(Qt.NoPen)
    painter.drawRoundedRect(4, 4, TRAY_ICON_SIZE - 8, TRAY_ICON_SIZE - 8, 12, 12)
    painter.setPen(QColor("white"))
    f = QFont("Segoe UI", 26, QFont.Bold)
    painter.setFont(f)
    painter.drawText(pix.rect(), Qt.AlignCenter, "B")
    painter.end()
    return QIcon(pix)


# ══════════════════════════════════════════════════════════════════════════════
# ── Who edited helper ─────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

def _get_editor_info(filepath: str) -> dict:
    """
    Try to get the Windows user + machine that last modified a file.
    Falls back gracefully if not available.
    """
    import os, socket
    info = {
        "user":    "",
        "machine": "",
        "ip":      "",
    }
    try:
        info["machine"] = socket.gethostname()
    except Exception:
        pass
    try:
        info["ip"] = socket.gethostbyname(info["machine"]) if info["machine"] else ""
    except Exception:
        pass
    try:
        # Try pywin32 first for file owner
        import win32security
        sd   = win32security.GetFileSecurity(filepath, win32security.OWNER_SECURITY_INFORMATION)
        sid  = sd.GetSecurityDescriptorOwner()
        name, domain, _ = win32security.LookupAccountSid(None, sid)
        info["user"] = f"{domain}\\{name}"
    except Exception:
        # Fallback: current logged-in user (not perfect but better than nothing)
        try:
            info["user"] = os.getlogin()
        except Exception:
            try:
                info["user"] = os.environ.get("USERNAME", "")
            except Exception:
                pass
    return info


# ══════════════════════════════════════════════════════════════════════════════
# ── Remote Upload Helpers (SFTP / FTPS / FTP / SMB / HTTPS) ───────────────────
# ══════════════════════════════════════════════════════════════════════════════

try:
    from transport_utils import (
        upload_to_sftp as _tu_sftp,
        upload_to_ftp  as _tu_ftp,
        upload_to_smb  as _tu_smb,
        upload_to_https as _tu_https,
    )
    _TRANSPORT_UTILS_AVAILABLE = True
except ImportError:
    _TRANSPORT_UTILS_AVAILABLE = False


def _upload_sftp(local_dir: str, sftp_cfg: dict, proto: str = "sftp") -> dict:
    """Upload a backup folder to SFTP or FTPS server."""
    if _TRANSPORT_UTILS_AVAILABLE:
        return _tu_sftp(local_dir, sftp_cfg)

    # Fallback inline implementation (used only when transport_utils is unavailable)
    host    = sftp_cfg.get("host", "")
    port    = sftp_cfg.get("port", 22 if proto == "sftp" else 21)
    user    = sftp_cfg.get("user") or sftp_cfg.get("username", "")
    pw      = sftp_cfg.get("pass") or sftp_cfg.get("password", "")
    rpath   = (sftp_cfg.get("path") or sftp_cfg.get("remote_path", "/")).rstrip("/")
    keyfile = sftp_cfg.get("keyfile") or sftp_cfg.get("key_path", "")

    if not host or not user:
        return {"ok": False, "error": "SFTP host/user not configured"}

    local_path = Path(local_dir)
    if not local_path.exists():
        return {"ok": False, "error": "Local backup dir not found"}

    uploaded = 0
    try:
        import paramiko
        transport = paramiko.Transport((host, int(port)))
        transport.connect()

        if keyfile and Path(keyfile).exists():
            key_pass = sftp_cfg.get("key_pass") or None
            try:
                pkey = paramiko.RSAKey.from_private_key_file(keyfile, password=key_pass)
            except paramiko.SSHException:
                try:
                    pkey = paramiko.Ed25519Key.from_private_key_file(keyfile, password=key_pass)
                except paramiko.SSHException:
                    pkey = paramiko.ECDSAKey.from_private_key_file(keyfile, password=key_pass)
            transport.auth_publickey(user, pkey)
        else:
            if not pw:
                return {"ok": False, "error": "SFTP password (or keyfile) not configured"}
            transport.auth_password(user, pw)

        if not transport.is_authenticated():
            return {"ok": False, "error": "SFTP authentication failed"}

        sftp = paramiko.SFTPClient.from_transport(transport)

        def _mkdir_p(remote_dir):
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
                        pass

        for fp in local_path.rglob("*"):
            if not fp.is_file():
                continue
            rel         = fp.relative_to(local_path)
            remote_file = f"{rpath}/{local_path.name}/{str(rel).replace(os.sep, '/')}"
            _mkdir_p(str(Path(remote_file).parent).replace("\\", "/"))
            try:
                sftp.put(str(fp), remote_file)
                uploaded += 1
            except Exception as e:
                logger.warning(f"[sftp] Failed to upload {rel}: {e}")

        sftp.close()
        transport.close()
        return {"ok": True, "uploaded": uploaded, "path": f"{rpath}/{local_path.name}"}

    except Exception as e:
        return {"ok": False, "error": str(e)}


def _upload_ftp(local_dir: str, ftp_cfg: dict) -> dict:
    """Upload a backup folder to an FTP/FTPS server."""
    if _TRANSPORT_UTILS_AVAILABLE:
        return _tu_ftp(local_dir, ftp_cfg)

    import ftplib
    host     = ftp_cfg.get("host", "")
    port     = int(ftp_cfg.get("port", 21))
    user     = ftp_cfg.get("user") or ftp_cfg.get("username", "")
    pw       = ftp_cfg.get("pass") or ftp_cfg.get("password", "")
    rpath    = (ftp_cfg.get("path") or ftp_cfg.get("remote_path", "/backups")).rstrip("/")
    use_tls  = bool(ftp_cfg.get("use_tls", True))

    if not host or not user:
        return {"ok": False, "error": "FTP host/user not configured"}

    local_path = Path(local_dir)
    uploaded   = 0
    ftp        = None
    try:
        if use_tls:
            ftp = ftplib.FTP_TLS(timeout=30)
            ftp.connect(host, port)
            ftp.login(user, pw)
            ftp.prot_p()
        else:
            ftp = ftplib.FTP(timeout=30)
            ftp.connect(host, port)
            ftp.login(user, pw)

        def _makedirs(remote_dir):
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
                        pass

        for fp in local_path.rglob("*"):
            if not fp.is_file():
                continue
            rel    = fp.relative_to(local_path)
            parts  = list(rel.parts)
            rdir   = f"{rpath}/{local_path.name}" + (("/" + "/".join(parts[:-1])) if len(parts) > 1 else "")
            _makedirs(rdir)
            try:
                ftp.cwd("/" + rdir.lstrip("/"))
                with open(fp, "rb") as f:
                    ftp.storbinary(f"STOR {fp.name}", f)
                uploaded += 1
            except Exception as e:
                logger.warning(f"[ftp] Failed to upload {rel}: {e}")

        return {"ok": True, "uploaded": uploaded, "path": f"{rpath}/{local_path.name}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        try:
            if ftp:
                ftp.quit()
        except Exception:
            pass


def _ensure_smb_mounted(smb_cfg: dict):
    """
    On Windows: run 'net use' to authenticate the SMB share so the UNC path
    is accessible as a normal filesystem path.
    Returns (ok: bool, error: str).
    """
    if os.name != "nt":
        return False, "SMB auto-mount requires Windows"

    import re
    import subprocess as _sp

    path   = smb_cfg.get("path", "").strip()
    user   = smb_cfg.get("user", smb_cfg.get("username", "")).strip()
    pw     = smb_cfg.get("pass", smb_cfg.get("password", ""))
    domain = smb_cfg.get("domain", "").strip()

    if not path:
        return True, ""   # nothing to mount  · user mapped the drive manually

    # Extract \\server\share from a full UNC path
    norm = path.replace("/", "\\")
    m    = re.match(r"(\\\\[^\\]+\\[^\\]+)", norm)
    unc  = m.group(1) if m else norm

    if not user:
        return True, ""   # no credentials  · share may already be accessible

    user_arg = f"{domain}\\{user}" if domain else user
    pw_arg   = pw if pw else ""

    try:
        cmd = ["net", "use", unc]
        if pw_arg:
            cmd.append(pw_arg)
        cmd += [f"/user:{user_arg}", "/persistent:no"]

        res    = _sp.run(cmd, capture_output=True, text=True, timeout=15)
        stderr = (res.stdout + res.stderr).lower()
        # "already" or "local device" means the share is already connected  · fine
        if res.returncode != 0 and "already" not in stderr and "local device" not in stderr:
            return False, (res.stderr or res.stdout).strip()
        return True, ""
    except Exception as e:
        return False, str(e)


def _upload_smb(local_dir: str, smb_cfg: dict) -> dict:
    """
    Copy a backup folder to a Windows SMB / CIFS share.

    • Windows   · mount via 'net use' (if credentials provided) then copy
                 files directly over the UNC path using shutil.
    • Linux/macOS  · use the optional 'smbprotocol' package.

    smb_cfg keys  (from cfg["dest_smb"]):
        path    full UNC destination e.g. \\\\nas\\backups
        user    SMB username
        pass    SMB password
        domain  Windows domain (optional)
    """
    ld = Path(local_dir)
    if not ld.exists():
        return {"ok": False, "error": f"Local backup dir not found: {local_dir}"}

    dest_path = smb_cfg.get("path", "").strip()
    if not dest_path:
        return {"ok": False, "error": "SMB destination path not configured"}

    # ── Windows: UNC copy ─────────────────────────────────────────────────────
    if os.name == "nt":
        ok, err = _ensure_smb_mounted(smb_cfg)
        if not ok:
            return {"ok": False, "error": f"SMB mount failed: {err}"}

        dest_root = Path(dest_path) / ld.name
        uploaded  = 0
        errors    = []
        try:
            dest_root.mkdir(parents=True, exist_ok=True)
            for fp in ld.rglob("*"):
                if fp.is_file():
                    rel = fp.relative_to(ld)
                    dst = dest_root / rel
                    try:
                        dst.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(str(fp), str(dst))
                        uploaded += 1
                    except Exception as copy_err:
                        errors.append(f"{rel}: {copy_err}")
            return {"ok": True, "uploaded": uploaded, "path": str(dest_root), "errors": errors}
        except Exception as e:
            return {"ok": False, "error": f"UNC copy failed: {e}"}

    # ── Non-Windows: smbprotocol ──────────────────────────────────────────────
    try:
        import smbclient
        import smbclient.shutil as smb_shutil

        user   = smb_cfg.get("user", smb_cfg.get("username", "")).strip()
        pw     = smb_cfg.get("pass", smb_cfg.get("password", ""))
        domain = smb_cfg.get("domain", "").strip()

        import re
        m = re.match(r"[/\\]{2}([^/\\]+)", dest_path)
        if not m:
            return {"ok": False, "error": f"Cannot parse server from SMB path: {dest_path}"}
        server = m.group(1)

        smbclient.register_session(server, username=user, password=pw, connection_timeout=10)

        smb_dest = dest_path.replace("/", "\\").rstrip("\\") + "\\" + ld.name
        try:
            smbclient.makedirs(smb_dest, exist_ok=True)
        except Exception:
            pass

        uploaded = 0
        errors   = []
        for fp in ld.rglob("*"):
            if fp.is_file():
                rel      = str(fp.relative_to(ld)).replace("/", "\\")
                smb_file = smb_dest + "\\" + rel
                smb_par  = "\\".join(smb_file.split("\\")[:-1])
                try:
                    smbclient.makedirs(smb_par, exist_ok=True)
                except Exception:
                    pass
                try:
                    with open(str(fp), "rb") as lf, smbclient.open_file(smb_file, mode="wb") as rf:
                        shutil.copyfileobj(lf, rf)
                    uploaded += 1
                except Exception as copy_err:
                    errors.append(f"{rel}: {copy_err}")

        return {"ok": True, "uploaded": uploaded, "path": smb_dest, "errors": errors}

    except ImportError:
        return {
            "ok": False,
            "error": (
                "smbprotocol not installed  · run: pip install smbprotocol\n"
                "(on Windows the built-in UNC copy is used automatically)"
            ),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ══════════════════════════════════════════════════════════════════════════════
# ── HTTPS API Upload Helper ────────────────────────────────────════════════════
# ══════════════════════════════════════════════════════════════════════════════

def _upload_https(local_dir: str, api_cfg: dict) -> dict:
    """
    Upload a backup folder to a custom HTTPS API endpoint.

    Each file is POSTed as multipart/form-data with:
      - 'file'  : the file binary
      - 'path'  : the relative path (so the server can reconstruct the folder)

    api_cfg keys:
      url        : full HTTPS endpoint, e.g. https://backup.company.com/api/upload
      token      : Bearer token (optional)
      verify_ssl : True by default; set False to allow self-signed certs
    """
    if _TRANSPORT_UTILS_AVAILABLE:
        return _tu_https(local_dir, api_cfg)

    import urllib.request
    import urllib.error
    import ssl
    import uuid as _uuid
    import mimetypes

    url        = api_cfg.get("url", "").strip()
    token      = api_cfg.get("token", "").strip()
    verify_ssl = api_cfg.get("verify_ssl", True)

    if not url:
        return {"ok": False, "error": "HTTPS API URL not configured"}

    local_path = Path(local_dir)
    if not local_path.exists():
        return {"ok": False, "error": "Local backup dir not found"}

    uploaded = 0
    errors   = []

    ctx = ssl.create_default_context()
    if not verify_ssl:
        ctx.check_hostname = False
        ctx.verify_mode    = ssl.CERT_NONE

    for fp in local_path.rglob("*"):
        if not fp.is_file():
            continue

        rel       = str(fp.relative_to(local_path)).replace("\\", "/")
        mime_type = mimetypes.guess_type(str(fp))[0] or "application/octet-stream"
        boundary  = f"----BackupSysBoundary{_uuid.uuid4().hex}"

        try:
            with open(fp, "rb") as f:
                file_data = f.read()
        except Exception as e:
            errors.append(f"{rel}: {e}")
            continue

        # Build multipart/form-data body
        body = (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="path"\r\n\r\n'
            f"{rel}\r\n"
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="file"; filename="{fp.name}"\r\n'
            f"Content-Type: {mime_type}\r\n\r\n"
        ).encode() + file_data + f"\r\n--{boundary}--\r\n".encode()

        req = urllib.request.Request(url, data=body, method="POST")
        req.add_header("Content-Type", f"multipart/form-data; boundary={boundary}")
        req.add_header("Content-Length", str(len(body)))
        if token:
            req.add_header("Authorization", f"Bearer {token}")

        try:
            with urllib.request.urlopen(req, context=ctx, timeout=60) as resp:
                if resp.getcode() not in (200, 201, 202, 204):
                    errors.append(f"{rel}: HTTP {resp.getcode()}")
                    continue
            uploaded += 1
        except urllib.error.HTTPError as e:
            errors.append(f"{rel}: HTTP {e.code} {e.reason}")
        except Exception as e:
            errors.append(f"{rel}: {e}")

    if errors:
        return {
            "ok":       uploaded > 0,
            "uploaded": uploaded,
            "errors":   errors,
            "error":    f"{len(errors)} file(s) failed  · first: {errors[0]}" if uploaded == 0 else None,
        }
    return {"ok": True, "uploaded": uploaded}


# ══════════════════════════════════════════════════════════════════════════════
# ── Email + Webhook Notification Helpers ──────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

try:
    from notification_utils import (
        send_email_notification as _nu_send_email,
        send_webhook_notification as _nu_send_webhook,
    )
    _NOTIFICATION_UTILS_AVAILABLE = True
except ImportError:
    _NOTIFICATION_UTILS_AVAILABLE = False


def _send_email_notification(cfg: dict, subject: str, body: str):
    """Send an email notification  · delegates to notification_utils when available."""
    ec = cfg.get("email_config", {})
    if not ec.get("enabled", False):
        return

    if _NOTIFICATION_UTILS_AVAILABLE:
        result = _nu_send_email(ec, subject, body)
        if not result["ok"]:
            logger.warning(f"\u26a0 Email notification failed: {result['error']}")
        return

    # Fallback inline SMTP implementation
    smtp_host = ec.get("smtp_host", "").strip()
    smtp_port = int(ec.get("smtp_port", 587))
    use_ssl   = ec.get("smtp_use_ssl", False)
    username  = ec.get("username", "").strip()
    password  = ec.get("password", "")
    from_addr = ec.get("from_addr", "").strip() or username
    to_addr   = ec.get("to_addr", "").strip()

    if not smtp_host or not to_addr:
        logger.warning("\u26a0 Email notification skipped \u2014 smtp_host or to_addr not configured")
        return

    try:
        import smtplib, ssl as _ssl
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart

        msg = MIMEMultipart()
        msg["From"]    = from_addr
        msg["To"]      = to_addr
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        if use_ssl:
            ctx = _ssl.create_default_context()
            server = smtplib.SMTP_SSL(smtp_host, smtp_port, context=ctx, timeout=30)
        else:
            server = smtplib.SMTP(smtp_host, smtp_port, timeout=30)
            server.ehlo()
            if server.has_extn("STARTTLS"):
                ctx = _ssl.create_default_context()
                server.starttls(context=ctx)
                server.ehlo()

        if username and password:
            server.login(username, password)

        server.sendmail(from_addr, to_addr, msg.as_string())
        server.quit()
        logger.info(f"\U0001f4e7 Email notification sent to {to_addr}: {subject}")
    except Exception as e:
        logger.warning(f"\u26a0 Email notification failed: {e}")


def _send_webhook(cfg: dict, result: dict):
    """POST a JSON backup result summary to the configured webhook URL."""
    url = cfg.get("webhook_url", "").strip()
    if not url:
        return

    # Success webhooks are opt-in; failures are always sent.
    # BUG FIX: previous condition was inverted  · it suppressed failure webhooks
    # when webhook_on_success=True instead of suppressing success webhooks when
    # webhook_on_success=False.
    if result.get("status") == "success" and not cfg.get("webhook_on_success", False):
        return

    payload = {
        "event":         "backup_" + result.get("status", "unknown"),
        "status":        result.get("status", ""),
        "watch_id":      result.get("watch_id", ""),
        "watch_name":    result.get("watch_name", ""),
        "files_copied":  result.get("files_copied", 0),
        "files_changed": result.get("files_changed", 0),
        "total_size":    result.get("total_size", ""),
        "duration_s":    result.get("duration_s", 0),
        "timestamp":     result.get("timestamp", ""),
        "error":         result.get("error"),
        "triggered_by":  result.get("triggered_by", ""),
    }

    if _NOTIFICATION_UTILS_AVAILABLE:
        r = _nu_send_webhook(url, payload)
        if not r["ok"]:
            logger.warning(f"\u26a0 Webhook failed ({url}): {r['error']}")
        return

    # Fallback inline implementation
    try:
        import urllib.request as _req, json as _json
        data = _json.dumps(payload, default=str).encode()
        req  = _req.Request(url, data=data, method="POST")
        req.add_header("Content-Type",  "application/json")
        req.add_header("User-Agent",    f"BackupSystem/{APP_VERSION}")
        with _req.urlopen(req, timeout=10) as resp:
            logger.info(f"\U0001f517 Webhook delivered to {url} \u2014 HTTP {resp.getcode()}")
    except Exception as e:
        logger.warning(f"\u26a0 Webhook failed ({url}): {e}")


class BackupWorker(QThread):
    progress    = pyqtSignal(int, int, str)      # copied, total, filename
    finished    = pyqtSignal(dict)               # result dict
    log_message = pyqtSignal(str)                # status text

    def __init__(self, watch: dict, cfg: dict, triggered_by: str = "manual"):
        super().__init__()   # BUG FIX: was super().__init__(self)  · passing self as own parent
        self.watch        = watch
        self.cfg          = cfg
        self.triggered_by = triggered_by
        self._stop_event  = threading.Event()   # set this to interrupt retry sleep or signal cancel

    def request_stop(self):
        """Signal the worker to abort at the next opportunity (retry sleep or between attempts)."""
        self._stop_event.set()

    def run(self):
        w   = self.watch
        cfg = self.cfg

        self.log_message.emit(f"Starting backup: {w['name']} …")

        # Determine the primary destination type for snapshot keying.
        # Each destination (sftp, gdrive, local …) maintains its own
        # independent snapshot so that a file already on SFTP but not yet on
        # GDrive is correctly detected as "new" for the GDrive destination.
        dest_type = cfg.get("dest_type", "local")
        snapshot = config_manager.load_snapshot(w["id"], dest_type)

        def cb(copied, total, fname):
            # Raise InterruptedError so run_backup()'s inner loop propagates
            # the cancellation immediately rather than waiting until the next
            # retry window.
            if self._stop_event.is_set():
                raise InterruptedError("Backup cancelled by user")
            self.progress.emit(copied, total, fname)

        # Honour the bandwidth throttle setting
        max_mbps  = cfg.get("max_backup_mbps", 0.0)
        throttler = backup_engine.BackupThrottler(max_mbps) if max_mbps and max_mbps > 0 else None

        auto_retry   = cfg.get("auto_retry", False)
        retry_delay  = max(1, int(cfg.get("retry_delay_min", 5))) * 60
        max_attempts = 3 if auto_retry else 1

        result = {"status": "failed", "error": "Not started"}
        for attempt in range(1, max_attempts + 1):
            if attempt > 1:
                self.log_message.emit(
                    f"⏳ Retrying {w['name']} (attempt {attempt}/{max_attempts}) "
                    f"in {cfg.get('retry_delay_min', 5)} min…"
                )
                # Interruptible sleep  · wakes immediately if request_stop() is called
                interrupted = self._stop_event.wait(timeout=retry_delay)
                if interrupted:
                    result = {"status": "cancelled", "error": "Cancelled during retry wait", "watch_id": w["id"]}
                    break

            # ── Pre-backup: mount SMB share if dest_type is smb ──────────
            dest_type = cfg.get("dest_type", "local")
            if dest_type == "smb":
                smb_cfg = cfg.get("dest_smb", {})
                ok, smb_err = _ensure_smb_mounted(smb_cfg)
                if not ok:
                    self.log_message.emit(f"⚠ SMB mount failed: {smb_err}")
                    result = {"status": "failed", "error": f"SMB mount failed: {smb_err}", "watch_id": w["id"]}
                    continue  # try again on next attempt

            try:
                result = backup_engine.run_backup(
                    source            = w["path"],
                    destination       = cfg["destination"],
                    watch_id          = w["id"],
                    watch_name        = w["name"],
                    storage_type      = "local",  # cloud uploads handled below per-provider
                    previous_snapshot = snapshot or None,
                    incremental       = bool(snapshot),
                    progress_cb       = cb,
                    exclude_patterns  = w.get("exclude_patterns", []),
                    compress          = w.get("compression", False),
                    encrypt_key       = w.get("encrypt_key") or None,
                    cloud_config      = None,  # suppress internal cloud upload
                    triggered_by      = self.triggered_by,
                    throttler         = throttler,
                )
            except InterruptedError:
                # User pressed ▶ Cancel  · treat as a clean cancellation not a failure
                result = {"status": "cancelled", "error": "Cancelled by user", "watch_id": w["id"]}
                break
            except Exception as e:
                result = {"status": "failed", "error": str(e), "watch_id": w["id"]}

            if result.get("status") == "success":
                break
            if attempt < max_attempts:
                self.log_message.emit(f"⚠ Backup failed (attempt {attempt}): {result.get('error', '')}")

        if result.get("status") == "success":
            config_manager.update_watch_snapshot(
                cfg, w["id"],
                result.get("snapshot", {}),
                result["timestamp"],
                result.get("total_size_bytes", 0),
                dest_type=dest_type,
            )
            self.log_message.emit(
                f"▶ {w['name']}: {result['files_copied']} file(s) · {result['total_size']}"
            )

            # Remove internal metadata from local backup folder
            # NOTE: Keep MANIFEST.json for local backups so list_backups() can find them
            if result.get("backup_dir"):
                from pathlib import Path as _P
                for _meta in ("BACKUP.sha256",):  # Don't delete MANIFEST.json
                    try:
                        _mp = _P(result["backup_dir"]) / _meta
                        if _mp.exists(): _mp.unlink()
                    except Exception:
                        pass

            # ── Email notification on success ──────────────────────────
            ec = cfg.get("email_config", {})
            if ec.get("enabled") and ec.get("notify_on_success"):
                try:
                    # Use notification_utils rich format when available
                    from notification_utils import build_backup_email as _bld_email
                    subject, body = _bld_email({
                        **result,
                        "watch_name": w["name"],
                        "files_copied": result.get("files_copied", 0),
                        "total_size": result.get("total_size", "0 B"),
                        "duration_s": result.get("duration_s", 0),
                        "backup_id": result.get("backup_id", result.get("id", "N/A")),
                    })
                except ImportError:
                    subject = f"✅ Backup complete: {w['name']}"
                    body    = (
                        f"Watch:         {w['name']}\n"
                        f"Source:        {w['path']}\n"
                        f"Files copied:  {result['files_copied']}\n"
                        f"Total size:    {result['total_size']}\n"
                        f"Duration:      {result.get('duration_s', 0):.1f}s\n"
                        f"Triggered by:  {self.triggered_by}\n"
                        f"Timestamp:     {result['timestamp']}\n"
                    )
                _send_email_notification(cfg, subject, body)

            # ── Webhook notification ───────────────────────────────────
            _send_webhook(cfg, result)



            # Upload to remote destination if configured
            dest_type = cfg.get("dest_type", "local")
            if dest_type == "sftp" and result.get("backup_dir"):
                self.log_message.emit("📡 Uploading to SFTP…")
                sftp_result = _upload_sftp(
                    result["backup_dir"], cfg.get("dest_sftp", {}), dest_type
                )
                if sftp_result["ok"]:
                    self.log_message.emit(f"☁E{dest_type.upper()} upload done: {sftp_result.get('uploaded',0)} file(s)")
                    # Persist cloud upload result in MANIFEST before deleting local staging copy
                    try:
                        import json as _json, tempfile as _tmp
                        _mpath = Path(result["backup_dir"]) / "MANIFEST.json"
                        if _mpath.exists():
                            with open(_mpath) as _f:
                                _m = _json.load(_f)
                            _m["cloud_upload"] = sftp_result
                            _fd, _tp = _tmp.mkstemp(dir=Path(result["backup_dir"]), suffix=".tmp")
                            with os.fdopen(_fd, "w") as _f:
                                _json.dump(_m, _f, indent=2)
                            os.replace(_tp, str(_mpath))
                    except Exception:
                        pass
                    # Clean up local staging copy now that it's on the server
                    try:
                        import shutil as _sh
                        _sh.rmtree(result["backup_dir"], ignore_errors=True)
                        # Invalidate index so ghost entry doesn't appear in UI
                        backup_engine._backup_index.invalidate(cfg.get("destination", ""))
                    except Exception:
                        pass
                else:
                    _err_msg = sftp_result.get('error', 'unknown error')
                    self.log_message.emit(f"⚠ {dest_type.upper()} upload failed: {_err_msg}")
                    _ec = cfg.get("email_config", {})
                    if _ec.get("enabled") and _ec.get("notify_on_failure", True):
                        _send_email_notification(cfg,
                            f"⚠ BackupSys  · {dest_type.upper()} upload failed: {w['name']}",
                            f"Backup completed but remote upload failed.\n\n"
                            f"  Watch:      {w['name']}\n"
                            f"  Dest type:  {dest_type.upper()}\n"
                            f"  Error:      {_err_msg}\n"
                            f"  Backup ID:  {result.get('backup_id', 'N/A')}\n"
                            f"  Timestamp:  {result.get('timestamp', '')[:19]}\n\n"
                            f"The backup is stored locally and will be retried on the next run."
                        )
                    _send_webhook(cfg, {**result, "status": "upload_failed",
                                        "upload_error": _err_msg, "upload_dest": dest_type})

            elif dest_type == "ftps" and result.get("backup_dir"):
                # FTPS = FTP over TLS. The UI shares the SFTP form fields (host/port/user/pass/path)
                # but the actual transfer uses ftplib FTP_TLS, not Paramiko SSH.
                _ftps_raw = cfg.get("dest_sftp", {})
                _ftps_cfg = {
                    "host":        _ftps_raw.get("host", ""),
                    "port":        int(_ftps_raw.get("port", 21)),
                    "username":    _ftps_raw.get("user", "") or _ftps_raw.get("username", ""),
                    "password":    _ftps_raw.get("pass", "") or _ftps_raw.get("password", ""),
                    "remote_path": _ftps_raw.get("path", "/backups") or _ftps_raw.get("remote_path", "/backups"),
                    "use_tls":     True,   # FTPS always uses TLS
                }
                self.log_message.emit("📡 Uploading to FTPS (FTP over TLS)…")
                ftp_result = _upload_ftp(result["backup_dir"], _ftps_cfg)
                if ftp_result["ok"]:
                    self.log_message.emit(f"☁EFTPS upload done: {ftp_result.get('uploaded',0)} file(s)")
                    try:
                        import json as _json, tempfile as _tmp
                        _mpath = Path(result["backup_dir"]) / "MANIFEST.json"
                        if _mpath.exists():
                            with open(_mpath) as _f:
                                _m = _json.load(_f)
                            _m["cloud_upload"] = ftp_result
                            _fd, _tp = _tmp.mkstemp(dir=Path(result["backup_dir"]), suffix=".tmp")
                            with os.fdopen(_fd, "w") as _f:
                                _json.dump(_m, _f, indent=2)
                            os.replace(_tp, str(_mpath))
                    except Exception:
                        pass
                    try:
                        import shutil as _sh
                        _sh.rmtree(result["backup_dir"], ignore_errors=True)
                        backup_engine._backup_index.invalidate(cfg.get("destination", ""))
                    except Exception:
                        pass
                else:
                    _err_msg = ftp_result.get('error', 'unknown error')
                    self.log_message.emit(f"⚠ FTPS upload failed: {_err_msg}")
                    _ec = cfg.get("email_config", {})
                    if _ec.get("enabled") and _ec.get("notify_on_failure", True):
                        _send_email_notification(cfg,
                            f"⚠ BackupSys  · FTPS upload failed: {w['name']}",
                            f"Backup completed but FTPS upload failed.\n\n"
                            f"  Watch:      {w['name']}\n"
                            f"  Dest type:  FTPS\n"
                            f"  Error:      {_err_msg}\n"
                            f"  Backup ID:  {result.get('backup_id', 'N/A')}\n"
                            f"  Timestamp:  {result.get('timestamp', '')[:19]}\n\n"
                            f"The backup is stored locally and will be retried on the next run."
                        )
                    _send_webhook(cfg, {**result, "status": "upload_failed",
                                        "upload_error": _err_msg, "upload_dest": "ftps"})

            elif dest_type == "ftp" and result.get("backup_dir"):
                self.log_message.emit("📡 Uploading to FTP…")
                ftp_result = _upload_ftp(result["backup_dir"], cfg.get("dest_ftp", {}))
                if ftp_result["ok"]:
                    self.log_message.emit(f"☁EFTP upload done: {ftp_result.get('uploaded',0)} file(s)")
                    # Persist cloud upload result in MANIFEST before deleting local staging copy
                    try:
                        import json as _json, tempfile as _tmp
                        _mpath = Path(result["backup_dir"]) / "MANIFEST.json"
                        if _mpath.exists():
                            with open(_mpath) as _f:
                                _m = _json.load(_f)
                            _m["cloud_upload"] = ftp_result
                            _fd, _tp = _tmp.mkstemp(dir=Path(result["backup_dir"]), suffix=".tmp")
                            with os.fdopen(_fd, "w") as _f:
                                _json.dump(_m, _f, indent=2)
                            os.replace(_tp, str(_mpath))
                    except Exception:
                        pass
                    # Clean up local staging copy now that it's on the server
                    try:
                        import shutil as _sh
                        _sh.rmtree(result["backup_dir"], ignore_errors=True)
                        # Invalidate index so ghost entry doesn't appear in UI
                        backup_engine._backup_index.invalidate(cfg.get("destination", ""))
                    except Exception:
                        pass
                else:
                    _err_msg = ftp_result.get('error', 'unknown error')
                    self.log_message.emit(f"⚠ FTP upload failed: {_err_msg}")
                    _ec = cfg.get("email_config", {})
                    if _ec.get("enabled") and _ec.get("notify_on_failure", True):
                        _send_email_notification(cfg,
                            f"⚠ BackupSys  · FTP upload failed: {w['name']}",
                            f"Backup completed but FTP upload failed.\n\n"
                            f"  Watch:      {w['name']}\n"
                            f"  Dest type:  FTP\n"
                            f"  Error:      {_err_msg}\n"
                            f"  Backup ID:  {result.get('backup_id', 'N/A')}\n"
                            f"  Timestamp:  {result.get('timestamp', '')[:19]}\n\n"
                            f"The backup is stored locally and will be retried on the next run."
                        )
                    _send_webhook(cfg, {**result, "status": "upload_failed",
                                        "upload_error": _err_msg, "upload_dest": "ftp"})

            elif dest_type == "https" and result.get("backup_dir"):
                self.log_message.emit("📡 Uploading to HTTPS API…")
                https_result = _upload_https(result["backup_dir"], cfg.get("dest_https", {}))
                if https_result["ok"]:
                    self.log_message.emit(f"☁EHTTPS upload done: {https_result.get('uploaded',0)} file(s)")
                    # Persist cloud upload result in MANIFEST before deleting local staging copy
                    try:
                        import json as _json, tempfile as _tmp
                        _mpath = Path(result["backup_dir"]) / "MANIFEST.json"
                        if _mpath.exists():
                            with open(_mpath) as _f:
                                _m = _json.load(_f)
                            _m["cloud_upload"] = https_result
                            _fd, _tp = _tmp.mkstemp(dir=Path(result["backup_dir"]), suffix=".tmp")
                            with os.fdopen(_fd, "w") as _f:
                                _json.dump(_m, _f, indent=2)
                            os.replace(_tp, str(_mpath))
                    except Exception:
                        pass
                    # Clean up local staging copy now that it's on the server
                    try:
                        import shutil as _sh
                        _sh.rmtree(result["backup_dir"], ignore_errors=True)
                        # Invalidate index so ghost entry doesn't appear in UI
                        backup_engine._backup_index.invalidate(cfg.get("destination", ""))
                    except Exception:
                        pass
                else:
                    _err_msg = https_result.get('error', 'unknown error')
                    self.log_message.emit(f"⚠ HTTPS upload failed: {_err_msg}")
                    _ec = cfg.get("email_config", {})
                    if _ec.get("enabled") and _ec.get("notify_on_failure", True):
                        _send_email_notification(cfg,
                            f"⚠ BackupSys  · HTTPS upload failed: {w['name']}",
                            f"Backup completed but HTTPS upload failed.\n\n"
                            f"  Watch:      {w['name']}\n"
                            f"  Dest type:  HTTPS\n"
                            f"  Error:      {_err_msg}\n"
                            f"  Backup ID:  {result.get('backup_id', 'N/A')}\n"
                            f"  Timestamp:  {result.get('timestamp', '')[:19]}\n\n"
                            f"The backup is stored locally and will be retried on the next run."
                        )
                    _send_webhook(cfg, {**result, "status": "upload_failed",
                                        "upload_error": _err_msg, "upload_dest": "https"})

            elif dest_type == "smb" and result.get("backup_dir"):
                # SMB destination: the backup was written to the UNC path directly
                # (backup_engine used cfg["destination"] = the UNC path).
                # We still run _upload_smb to verify/copy in case the engine used a
                # local temp path or the direct write was attempted without credentials.
                smb_cfg = cfg.get("dest_smb", {})
                # Only run the explicit upload if the destination is NOT the UNC path
                # (i.e. backup_dir is inside a local temp dir, not already on the share).
                backup_dir_path = result.get("backup_dir", "")
                dest_unc        = cfg.get("destination", "")
                if dest_unc and not backup_dir_path.lower().startswith(dest_unc.lower()):
                    self.log_message.emit("📡 Copying backup to SMB share…")
                    smb_result = _upload_smb(backup_dir_path, smb_cfg)
                    if smb_result["ok"]:
                        self.log_message.emit(f"☁ESMB copy done: {smb_result.get('uploaded',0)} file(s)")
                        errs = smb_result.get("errors", [])
                        if errs:
                            self.log_message.emit(f"  ⚠ {len(errs)} file(s) had errors")
                    else:
                        self.log_message.emit(f"⚠ SMB copy failed: {smb_result.get('error','')}")
        else:
            self.log_message.emit(f"⚠ {w['name']}: {result.get('error', 'unknown error')}")

            # ── Email + webhook on failure ─────────────────────────────
            ec = cfg.get("email_config", {})
            if ec.get("enabled") and ec.get("notify_on_failure", True):
                try:
                    from notification_utils import build_backup_email as _bld_email
                    subject, body = _bld_email({
                        **result,
                        "watch_name": w["name"],
                        "status": result.get("status", "failed"),
                        "error": result.get("error", "Unknown error"),
                    })
                except ImportError:
                    subject = f"⚠ Backup failed: {w['name']}"
                    body    = (
                        f"Watch:     {w['name']}\n"
                        f"Source:    {w['path']}\n"
                        f"Error:     {result.get('error', 'unknown')}\n"
                        f"Triggered: {self.triggered_by}\n"
                        f"Timestamp: {result.get('timestamp', '')}\n"
                    )
                _send_email_notification(cfg, subject, body)
            _send_webhook(cfg, result)

        self.finished.emit(result)


# ══════════════════════════════════════════════════════════════════════════════
# ── Admin Password Dialog ──────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

class PasswordDialog(QDialog):
    def __init__(self, parent=None, mode="verify"):
        super().__init__(parent)
        self.mode = mode
        self.setWindowTitle("Admin Authentication")
        self.setMinimumWidth(360)
        self.setModal(True)
        self._attempts  = 0       # wrong-password counter
        self._locked    = False   # True while cooldown is active
        self._build_ui()

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(16)
        layout.setContentsMargins(24, 24, 24, 24)

        icon_lbl = QLabel("🔒")
        icon_lbl.setAlignment(Qt.AlignCenter)
        icon_lbl.setStyleSheet("font-size: 36px;")
        layout.addWidget(icon_lbl)

        self.title_lbl = QLabel("Admin Access Required" if self.mode == "verify" else "Set Admin Password")
        self.title_lbl.setObjectName("heading")
        self.title_lbl.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.title_lbl)

        self.sub_lbl = QLabel("Enter the admin password to continue" if self.mode == "verify"
                             else "Choose a password to protect admin settings")
        self.sub_lbl.setObjectName("subheading")
        self.sub_lbl.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.sub_lbl)

        self.pw_input = QLineEdit()
        self.pw_input.setEchoMode(QLineEdit.Password)
        self.pw_input.setPlaceholderText("Password")
        layout.addWidget(self.pw_input)

        self.pw_confirm = QLineEdit()
        self.pw_confirm.setEchoMode(QLineEdit.Password)
        self.pw_confirm.setPlaceholderText("Confirm password")
        self.pw_confirm.setVisible(self.mode != "verify")
        layout.addWidget(self.pw_confirm)

        self.error_lbl = QLabel("")
        self.error_lbl.setObjectName("status_err")
        self.error_lbl.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.error_lbl)

        btn_row = QHBoxLayout()
        self.forgot_btn = QPushButton("Forgot password?")
        self.forgot_btn.setObjectName("secondary")
        self.forgot_btn.setMaximumWidth(140)
        self.forgot_btn.clicked.connect(self._forgot_password)
        self.forgot_btn.setVisible(self.mode == "verify")

        cancel = QPushButton("Cancel")
        cancel.setObjectName("secondary")
        cancel.clicked.connect(self.reject)

        self.ok_btn = QPushButton("Confirm" if self.mode == "verify" else "Set Password")
        self.ok_btn.clicked.connect(self._submit)
        self.pw_input.returnPressed.connect(self._submit)

        btn_row.addWidget(self.forgot_btn)
        btn_row.addStretch()
        btn_row.addWidget(cancel)
        btn_row.addWidget(self.ok_btn)
        layout.addLayout(btn_row)

    def _submit(self):
        if self._locked:
            return  # silently ignore while locked
        pw = self.pw_input.text()
        if not pw:
            self.error_lbl.setText("Password cannot be empty")
            return

        if self.mode == "set":
            if pw != self.pw_confirm.text():
                self.error_lbl.setText("Passwords do not match")
                return
            self._save_password(pw)
            self.accept()
        else:
            if self._verify_password(pw):
                self.accept()
            else:
                self._attempts += 1
                self.pw_input.clear()
                # Lockout: 30-second cooldown after 5 consecutive failures
                if self._attempts >= 5:
                    self._locked = True
                    self.error_lbl.setText("Too many attempts  · locked for 30 seconds")
                    self.pw_input.setEnabled(False)
                    QTimer.singleShot(30_000, self._unlock)
                else:
                    remaining = 5 - self._attempts
                    self.error_lbl.setText(
                        f"Incorrect password ({remaining} attempt{'s' if remaining != 1 else ''} left)"
                    )

    def _forgot_password(self):
        reply = QMessageBox.question(
            self,
            "Reset Admin Password",
            "This will remove the existing admin password and let you set a new one. Continue?",
            QMessageBox.Yes | QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return

        s = QSettings(SETTINGS_ORG, SETTINGS_APP)
        s.remove(ADMIN_PASS_KEY)

        self.mode = "set"
        self.title_lbl.setText("Set Admin Password")
        self.sub_lbl.setText("Choose a password to protect admin settings")
        self.ok_btn.setText("Set Password")
        self.forgot_btn.setVisible(False)
        self.pw_confirm.setVisible(True)
        self.pw_input.clear()
        self.pw_confirm.clear()
        self.error_lbl.setText("")

    def _unlock(self):
        """Called after the 30-second lockout expires."""
        self._attempts = 0
        self._locked   = False
        self.pw_input.setEnabled(True)
        self.error_lbl.setText("You may try again")

    def _hash(self, pw: str) -> str:
        """Return a salted PBKDF2-HMAC-SHA256 hash of the password.

        Format: <hex-salt>:<hex-hash>   (salt is 16 random bytes)
        On verification the stored salt is reused so the hash is deterministic.
        """
        import os as _os, hashlib as _hl
        salt = _os.urandom(16)
        h = _hl.pbkdf2_hmac("sha256", pw.encode(), salt, 260_000)
        return salt.hex() + ":" + h.hex()

    def _hash_verify(self, pw: str, stored: str) -> bool:
        """Verify *pw* against a stored '<salt_hex>:<hash_hex>' string.
        Also accepts legacy plain-SHA256 hashes (64-char hex, no colon) so
        existing passwords continue to work after the upgrade.
        """
        import hashlib as _hl
        if ":" not in stored:
            # Legacy plain-SHA256  · accept it but user should reset password
            return _hl.sha256(pw.encode()).hexdigest() == stored
        try:
            salt_hex, hash_hex = stored.split(":", 1)
            salt = bytes.fromhex(salt_hex)
            h    = _hl.pbkdf2_hmac("sha256", pw.encode(), salt, 260_000)
            return h.hex() == hash_hex
        except Exception:
            return False

    def _save_password(self, pw: str):
        s = QSettings(SETTINGS_ORG, SETTINGS_APP)
        s.setValue(ADMIN_PASS_KEY, self._hash(pw))

    def _verify_password(self, pw: str) -> bool:
        s = QSettings(SETTINGS_ORG, SETTINGS_APP)
        stored = s.value(ADMIN_PASS_KEY, "")
        if not stored:
            # No password set yet >any input grants access
            return True
        return self._hash_verify(pw, stored)

    @staticmethod
    def has_password() -> bool:
        s = QSettings(SETTINGS_ORG, SETTINGS_APP)
        return bool(s.value(ADMIN_PASS_KEY, ""))


# ══════════════════════════════════════════════════════════════════════════════
# ── Add Watch Dialog ───────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

class AddWatchDialog(QDialog):
    def __init__(self, parent=None, cfg=None):
        super().__init__(parent)
        self.cfg = cfg or {}
        self.setWindowTitle("Add Watched Folder / Network Path")
        self.setMinimumWidth(520)
        self._build_ui()

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(14)
        layout.setContentsMargins(24, 24, 24, 24)

        title = QLabel("Add Folder / File to Watch")
        title.setObjectName("heading")
        layout.addWidget(title)

        form = QFormLayout()
        form.setSpacing(10)

        self.name_input = QLineEdit()
        self.name_input.setPlaceholderText("e.g. My Documents")
        form.addRow("Name:", self.name_input)

        # Source type selector
        self.source_type = QComboBox()
        self.source_type.addItems(["Local / Mapped Drive", "Network Share (SMB)"])
        self.source_type.currentIndexChanged.connect(self._on_source_type_changed)
        form.addRow("Source Type:", self.source_type)

        # Local path row
        self.local_widget = QWidget()
        path_row = QHBoxLayout(self.local_widget)
        path_row.setContentsMargins(0,0,0,0)
        self.path_input = QLineEdit()
        self.path_input.setPlaceholderText("C:\\Users\\you\\Documents")
        browse_btn = QPushButton("Browse")
        browse_btn.setObjectName("secondary")
        browse_btn.setMaximumWidth(80)
        browse_btn.clicked.connect(self._browse)
        path_row.addWidget(self.path_input)
        path_row.addWidget(browse_btn)
        form.addRow("Path:", self.local_widget)

        # SMB row
        self.smb_widget = QWidget()
        smb_layout = QVBoxLayout(self.smb_widget)
        smb_layout.setContentsMargins(0,0,0,0)
        smb_layout.setSpacing(6)

        self.smb_path_input = QLineEdit()
        self.smb_path_input.setPlaceholderText("\\\\server\\share\\folder  or  //server/share/folder")
        smb_layout.addWidget(self.smb_path_input)

        smb_cred_row = QHBoxLayout()
        self.smb_user = QLineEdit()
        self.smb_user.setPlaceholderText("Username (optional)")
        self.smb_pass = QLineEdit()
        self.smb_pass.setPlaceholderText("Password (optional)")
        self.smb_pass.setEchoMode(QLineEdit.Password)
        self.smb_domain = QLineEdit()
        self.smb_domain.setPlaceholderText("Domain (optional)")
        smb_cred_row.addWidget(self.smb_user)
        smb_cred_row.addWidget(self.smb_pass)
        smb_cred_row.addWidget(self.smb_domain)
        smb_layout.addLayout(smb_cred_row)

        smb_help = QLabel("Example: \\\\192.168.1.100\\shared\\Documents")
        smb_help.setStyleSheet("color:#6b7280; font-size:10px;")
        smb_layout.addWidget(smb_help)

        self.smb_widget.setVisible(False)
        form.addRow("SMB Path:", self.smb_widget)

        self.interval_spin = QSpinBox()
        self.interval_spin.setRange(0, 1440)
        self.interval_spin.setValue(0)
        self.interval_spin.setSuffix(" min (0 = use global)")
        form.addRow("Interval:", self.interval_spin)

        self.compress_check = QCheckBox("Enable compression")
        form.addRow("", self.compress_check)

        layout.addLayout(form)

        self.error_lbl = QLabel("")
        self.error_lbl.setObjectName("status_err")
        layout.addWidget(self.error_lbl)

        btn_row = QHBoxLayout()
        cancel = QPushButton("Cancel")
        cancel.setObjectName("secondary")
        cancel.clicked.connect(self.reject)
        add = QPushButton("Add Watch")
        add.setObjectName("success")
        add.clicked.connect(self._submit)
        btn_row.addWidget(cancel)
        btn_row.addWidget(add)
        layout.addLayout(btn_row)

    def _on_source_type_changed(self, idx):
        self.local_widget.setVisible(idx == 0)
        self.smb_widget.setVisible(idx == 1)

    def _browse(self):
        msg = QMessageBox(self)
        msg.setWindowTitle("What to watch?")
        msg.setText("Do you want to watch a folder or a single file?")
        folder_btn = msg.addButton("Folder", QMessageBox.AcceptRole)
        file_btn   = msg.addButton("File",   QMessageBox.AcceptRole)
        msg.addButton("Cancel", QMessageBox.RejectRole)
        msg.exec_()
        clicked = msg.clickedButton()
        if clicked == folder_btn:
            path = QFileDialog.getExistingDirectory(self, "Select Folder to Watch")
        elif clicked == file_btn:
            path, _ = QFileDialog.getOpenFileName(self, "Select File to Watch")
        else:
            return
        if path:
            self.path_input.setText(path)
            if not self.name_input.text():
                self.name_input.setText(Path(path).name)

    def _validate_and_warn(self, path_str: str, name: str) -> bool:
        """
        Run all validations on path_str. Shows error_lbl for hard failures,
        QMessageBox warnings for soft issues (user can proceed).
        Returns True if OK to proceed, False to abort.
        """
        import os, stat as _stat, re

        p = Path(path_str)

        # ── Hard failures ─────────────────────────────────────────────────────

        # 1. Empty
        if not path_str:
            self.error_lbl.setText("Path is required.")
            return False

        # 2. Suspicious / malicious characters
        if any(c in path_str for c in ('\x00', '\r', '\n')):
            self.error_lbl.setText("Path contains invalid characters.")
            return False

        # 3. Overly long path (Windows MAX_PATH = 260)
        if len(path_str) > 32767:
            self.error_lbl.setText("Path is too long (max 32767 characters).")
            return False

        # 4. Does not exist
        if not p.exists():
            self.error_lbl.setText("Path does not exist. Check the spelling or connect the drive.")
            return False

        # 5. Neither file nor directory (device node, pipe, etc.)
        if not p.is_file() and not p.is_dir():
            self.error_lbl.setText("Path must point to a file or folder, not a device or pipe.")
            return False

        # 6. Read permission check
        try:
            if p.is_dir():
                os.listdir(path_str)
            else:
                open(path_str, "rb").close()
        except PermissionError:
            self.error_lbl.setText("No read permission on this path. Run as administrator or check folder permissions.")
            return False
        except Exception as e:
            self.error_lbl.setText(f"Cannot access path: {e}")
            return False

        # 7. Duplicate path  · already being watched
        existing_paths = [w.get("path", "").strip().lower() for w in self.cfg.get("watches", [])]
        if path_str.strip().lower() in existing_paths:
            self.error_lbl.setText("This path is already in your watch list.")
            return False

        # 8. Duplicate name  · already used
        existing_names = [w.get("name", "").strip().lower() for w in self.cfg.get("watches", [])]
        if name.strip().lower() in existing_names:
            self.error_lbl.setText(f"A watch named \"{name}\" already exists. Choose a different name.")
            return False

        # 9. Name too long
        if len(name) > 64:
            self.error_lbl.setText("Name is too long (max 64 characters).")
            return False

        # 10. Name contains only valid characters (no / \ : * ? " < > |)
        if re.search(r'[/\\:*?"<>|]', name):
            self.error_lbl.setText("Name cannot contain: / \\ : * ? \" < > |")
            return False

        # 11. Watching a dangerous system root (e.g. C:\ or /)
        try:
            resolved = p.resolve()
            if len(resolved.parts) <= 1:
                self.error_lbl.setText(
                    "Watching a root drive (e.g. C:\\) is not allowed.\n"
                    "Please choose a specific folder instead."
                )
                return False
        except Exception:
            pass

        # 12. Path is inside an existing watched folder (sub-folder overlap)
        for w in self.cfg.get("watches", []):
            wp = w.get("path", "")
            try:
                if path_str.lower().startswith(wp.lower().rstrip("/\\") + os.sep) \
                        or wp.lower().startswith(path_str.lower().rstrip("/\\") + os.sep):
                    self.error_lbl.setText(
                        f"This path overlaps with existing watch \"{w.get('name', wp)}\"."
                        " Nested watches can cause duplicate backups."
                    )
                    return False
            except Exception:
                pass

        # ── Soft warnings (user may still proceed) ────────────────────────────

        warnings = []

        # 13. Hidden folder / file
        try:
            if os.name == "nt":
                import ctypes
                attrs = ctypes.windll.kernel32.GetFileAttributesW(path_str)
                if attrs != -1 and (attrs & 0x2):
                    warnings.append("This path is hidden. Make sure you intend to back it up.")
            else:
                if p.name.startswith("."):
                    warnings.append("This path appears to be a hidden file or folder.")
        except Exception:
            pass

        # 14. Very large source folder (>2 GB warning)
        try:
            if p.is_dir():
                total = 0
                for fp in p.rglob("*"):
                    if fp.is_file():
                        try:
                            total += fp.stat().st_size
                        except Exception:
                            pass
                    if total > 2 * 1024 ** 3:
                        break
                if total > 2 * 1024 ** 3:
                    import math
                    gb = total / 1024 ** 3
                    warnings.append(
                        f"This folder appears to be larger than 2 GB ({gb:.1f} GB estimated).\n"
                        "First backup may take a long time."
                    )
        except Exception:
            pass

        # 15. Destination disk space check (local destination only)
        try:
            dest = self.cfg.get("destination", "")
            if dest and Path(dest).exists():
                free = shutil.disk_usage(dest).free
                if p.is_file():
                    src_size = p.stat().st_size
                else:
                    src_size = sum(
                        fp.stat().st_size for fp in p.rglob("*") if fp.is_file()
                    )
                if src_size > free * 0.9:
                    warnings.append(
                        f"Destination may not have enough free space.\n"
                        f"Source: {src_size // 1024 ** 2} MB  |  "
                        f"Destination free: {free // 1024 ** 2} MB"
                    )
        except Exception:
            pass

        # 16. Network path is slow / unreliable warning
        try:
            if path_str.startswith("//") or path_str.startswith("\\\\"):
                import time as _time
                t0 = _time.time()
                os.listdir(path_str)
                elapsed = _time.time() - t0
                if elapsed > 3.0:
                    warnings.append(
                        f"Network share responded slowly ({elapsed:.1f}s).\n"
                        "Backups may time out on a slow connection."
                    )
        except Exception:
            pass

        # ── Show soft warning dialog if any ──────────────────────────────────
        if warnings:
            msg = "\n\n".join(f"⚠ {w}" for w in warnings)
            reply = QMessageBox.warning(
                self, "Warning  · Review Before Adding",
                msg + "\n\nDo you want to add this watch anyway?",
                QMessageBox.Yes | QMessageBox.Cancel
            )
            if reply != QMessageBox.Yes:
                return False

        return True

    def _submit(self):
        name = self.name_input.text().strip()

        # Name: required
        if not name:
            self.error_lbl.setText("Name is required.")
            return

        is_smb = self.source_type.currentIndex() == 1

        if is_smb:
            path_str = self.smb_path_input.text().strip()
            # SMB must start with // or \\
            if not path_str:
                self.error_lbl.setText("SMB path is required.")
                return
            if not (path_str.startswith("//") or path_str.startswith("\\\\")):
                self.error_lbl.setText("SMB path must start with // or \\\\ (e.g. //server/share)")
                return
        else:
            path_str = self.path_input.text().strip()

        self.error_lbl.setText("")
        if self._validate_and_warn(path_str, name):
            self.accept()

    def get_values(self):
        is_smb = self.source_type.currentIndex() == 1
        path   = self.smb_path_input.text().strip() if is_smb else self.path_input.text().strip()
        return {
            "name":         self.name_input.text().strip(),
            "path":         path,
            "interval_min": self.interval_spin.value(),
            "compression":  self.compress_check.isChecked(),
            "is_smb":       is_smb,
            "smb_user":     self.smb_user.text().strip() if is_smb else "",
            "smb_pass":     self.smb_pass.text() if is_smb else "",
            "smb_domain":   self.smb_domain.text().strip() if is_smb else "",
        }


# ══════════════════════════════════════════════════════════════════════════════
# ── Edit Watch Dialog ──────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

class EditWatchDialog(QDialog):
    """Edit per-watch settings: name, interval, compression, retention, max_backups, exclusions."""

    def __init__(self, watch: dict, parent=None):
        super().__init__(parent)
        self.watch = watch
        self.setWindowTitle(f"Edit Watch  · {watch.get('name', '')}")
        self.setMinimumWidth(500)
        self._build_ui()

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(14)
        layout.setContentsMargins(24, 24, 24, 24)

        title = QLabel("Edit Watch Settings")
        title.setObjectName("heading")
        layout.addWidget(title)

        form = QFormLayout()
        form.setSpacing(10)

        self.name_input = QLineEdit(self.watch.get("name", ""))
        form.addRow("Name:", self.name_input)

        path_lbl = QLabel(self.watch.get("path", ""))
        path_lbl.setStyleSheet("color:#6b7280; font-size:11px;")
        form.addRow("Path:", path_lbl)

        self.interval_spin = QSpinBox()
        self.interval_spin.setRange(0, 1440)
        self.interval_spin.setValue(self.watch.get("interval_min", 0))
        self.interval_spin.setSuffix(" min  (0 = use global)")
        form.addRow("Interval:", self.interval_spin)

        self.retention_spin = QSpinBox()
        self.retention_spin.setRange(0, 365)
        self.retention_spin.setValue(self.watch.get("retention_days", 0))
        self.retention_spin.setSuffix(" days  (0 = use global)")
        form.addRow("Retention:", self.retention_spin)

        self.max_backups_spin = QSpinBox()
        self.max_backups_spin.setRange(0, 9999)
        self.max_backups_spin.setValue(self.watch.get("max_backups", 0))
        self.max_backups_spin.setSuffix("  (0 = unlimited)")
        form.addRow("Max backups:", self.max_backups_spin)

        self.compress_check = QCheckBox("Enable gzip compression")
        self.compress_check.setChecked(self.watch.get("compression", False))
        form.addRow("", self.compress_check)

        self.skip_auto_check = QCheckBox("Skip auto backup  (manual only)")
        self.skip_auto_check.setChecked(self.watch.get("skip_auto_backup", False))
        form.addRow("", self.skip_auto_check)

        # ── Encryption ──────────────────────────────────────────────────────
        enc_row = QHBoxLayout()
        self.encrypt_input = QLineEdit(self.watch.get("encrypt_key", ""))
        self.encrypt_input.setEchoMode(QLineEdit.Password)
        self.encrypt_input.setPlaceholderText("44-char Fernet key  (leave blank to disable)")
        self.encrypt_input.setToolTip(
            "AES encryption key for this watch.\n"
            "Generate one: python -c \"from backup_engine import generate_encryption_key; print(generate_encryption_key())\"\n"
            "⚠ Store your key safely  · without it backups cannot be restored!"
        )
        gen_key_btn = QPushButton("Generate")
        gen_key_btn.setObjectName("secondary")
        gen_key_btn.setFixedWidth(80)
        gen_key_btn.clicked.connect(self._generate_key)
        show_key_btn = QPushButton("👁")
        show_key_btn.setObjectName("secondary")
        show_key_btn.setFixedWidth(36)
        show_key_btn.setCheckable(True)
        show_key_btn.toggled.connect(
            lambda on: self.encrypt_input.setEchoMode(
                QLineEdit.Normal if on else QLineEdit.Password
            )
        )
        enc_row.addWidget(self.encrypt_input)
        enc_row.addWidget(gen_key_btn)
        enc_row.addWidget(show_key_btn)
        form.addRow("Encrypt key:", enc_row)

        self.color_input = QLineEdit(self.watch.get("color", ""))
        self.color_input.setPlaceholderText("#2563eb  (optional color label)")
        color_row = QHBoxLayout()
        color_row.setContentsMargins(0, 0, 0, 0)
        color_row.addWidget(self.color_input)
        pick_color_btn = QPushButton("🎨")
        pick_color_btn.setObjectName("secondary")
        pick_color_btn.setFixedWidth(36)
        pick_color_btn.setToolTip("Open color picker")
        def _pick_color():
            from PyQt5.QtWidgets import QColorDialog
            from PyQt5.QtGui import QColor
            current = self.color_input.text().strip()
            initial = QColor(current) if current else QColor("#2563eb")
            chosen = QColorDialog.getColor(initial, self, "Pick a label color")
            if chosen.isValid():
                self.color_input.setText(chosen.name())
        pick_color_btn.clicked.connect(_pick_color)
        color_row.addWidget(pick_color_btn)
        color_widget = QWidget()
        color_widget.setLayout(color_row)
        form.addRow("Color:", color_widget)

        self.notes_input = QLineEdit(self.watch.get("notes", ""))
        self.notes_input.setPlaceholderText("Optional notes")
        form.addRow("Notes:", self.notes_input)

        self.tags_input = QLineEdit(", ".join(self.watch.get("tags", [])))
        self.tags_input.setPlaceholderText("e.g. work, important, daily  (comma-separated)")
        form.addRow("Tags:", self.tags_input)

        # Exclusions
        excl_label = QLabel("Exclude patterns  (one per line):")
        excl_label.setStyleSheet("color:#9ca3af;")
        form.addRow("", excl_label)
        self.excl_edit = QTextEdit()
        self.excl_edit.setMaximumHeight(100)
        self.excl_edit.setPlainText("\n".join(self.watch.get("exclude_patterns", [])))
        form.addRow("Exclusions:", self.excl_edit)

        layout.addLayout(form)

        self.error_lbl = QLabel("")
        self.error_lbl.setObjectName("status_err")
        layout.addWidget(self.error_lbl)

        btn_row = QHBoxLayout()
        cancel = QPushButton("Cancel")
        cancel.setObjectName("secondary")
        cancel.clicked.connect(self.reject)
        save = QPushButton("Save Changes")
        save.setObjectName("success")
        save.clicked.connect(self._submit)
        btn_row.addWidget(cancel)
        btn_row.addWidget(save)
        layout.addLayout(btn_row)

    def _submit(self):
        name = self.name_input.text().strip()
        if not name:
            self.error_lbl.setText("Name is required.")
            return
        # Validate encryption key length if one is provided
        key = self.encrypt_input.text().strip()
        if key and len(key) != 44:
            self.error_lbl.setText(f"Encryption key must be exactly 44 characters (got {len(key)}).")
            return
        self.accept()

    def _generate_key(self):
        """Generate a new Fernet encryption key and populate the field."""
        try:
            if BACKEND_AVAILABLE:
                key = backup_engine.generate_encryption_key()
            else:
                from cryptography.fernet import Fernet
                key = Fernet.generate_key().decode()
            self.encrypt_input.setEchoMode(QLineEdit.Normal)
            self.encrypt_input.setText(key)
            QMessageBox.information(
                self, "Key Generated",
                f"A new encryption key has been generated and placed in the field.\n\n"
                f"⚠ IMPORTANT: Copy and store this key somewhere safe!\n"
                f"Without it you cannot restore your encrypted backups.\n\n{key}"
            )
        except Exception as e:
            self.error_lbl.setText(f"Key generation failed: {e}")

    def get_values(self) -> dict:
        excl = [
            ln.strip() for ln in self.excl_edit.toPlainText().splitlines()
            if ln.strip()
        ]
        tags = [t.strip() for t in self.tags_input.text().split(",") if t.strip()]
        return {
            "name":             self.name_input.text().strip(),
            "interval_min":     self.interval_spin.value(),
            "retention_days":   self.retention_spin.value(),
            "max_backups":      self.max_backups_spin.value(),
            "compression":      self.compress_check.isChecked(),
            "skip_auto_backup": self.skip_auto_check.isChecked(),
            "color":            self.color_input.text().strip(),
            "notes":            self.notes_input.text().strip(),
            "tags":             tags,
            "exclude_patterns": excl,
            "encrypt_key":      self.encrypt_input.text().strip(),
        }


# ══════════════════════════════════════════════════════════════════════════════
# ── Admin Panel ────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

class AdminPanel(QDialog):
    watches_changed = pyqtSignal()

    def __init__(self, cfg: dict, parent=None):
        super().__init__(parent)
        self.cfg = cfg
        self.setWindowTitle("Admin Settings  · Backup System")
        self.setMinimumSize(880, 580)
        self.setModal(True)
        self._build_ui()
        self._load_values()

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(0)
        layout.setContentsMargins(0, 0, 0, 0)

        # Header
        header = QFrame()
        header.setObjectName("topbar")
        header.setFixedHeight(56)
        hl = QHBoxLayout(header)
        hl.setContentsMargins(20, 0, 20, 0)
        title = QLabel("Admin Settings")
        title.setStyleSheet("font-size:15px; font-weight:700; color:#f1f3f9;")
        hl.addWidget(title)
        hl.addStretch()
        close_btn = QPushButton("✕")
        close_btn.setObjectName("secondary")
        close_btn.setFixedSize(32, 32)
        close_btn.clicked.connect(self.close)
        hl.addWidget(close_btn)
        layout.addWidget(header)

        tabs = QTabWidget()
        tabs.tabBar().setExpanding(False)
        tabs.tabBar().setElideMode(Qt.ElideNone)
        tabs.setContentsMargins(16, 16, 16, 16)

        # ── Tab 1: General ─────────────────────────────────────────────────
        general_inner = QWidget()
        gl = QVBoxLayout(general_inner)
        gl.setSpacing(16)
        gl.setContentsMargins(20, 20, 20, 20)

        dest_group = QGroupBox("Backup Destination")
        dg_main = QVBoxLayout(dest_group)
        dg_main.setSpacing(8)

        dest_type_row = QHBoxLayout()
        dest_type_row.addWidget(QLabel("Type:"))
        self.dest_type_combo = QComboBox()
        self.dest_type_combo.addItems(["Local / Mapped Drive", "Network Share (SMB)", "SFTP", "FTPS", "FTP", "HTTPS API"])
        self.dest_type_combo.currentIndexChanged.connect(self._on_dest_type_changed)
        dest_type_row.addWidget(self.dest_type_combo, stretch=1)
        dg_main.addLayout(dest_type_row)

        # Local destination
        self.dest_local_widget = QWidget()
        dl = QHBoxLayout(self.dest_local_widget)
        dl.setContentsMargins(0,0,0,0)
        self.dest_input = QLineEdit()
        self.dest_input.setPlaceholderText("C:\\BackupData")
        browse_dest = QPushButton("Browse")
        browse_dest.setObjectName("secondary")
        browse_dest.setMaximumWidth(80)
        browse_dest.clicked.connect(self._browse_dest)
        dl.addWidget(self.dest_input)
        dl.addWidget(browse_dest)
        dg_main.addWidget(self.dest_local_widget)

        # SMB destination
        self.dest_smb_widget = QWidget()
        dsl = QVBoxLayout(self.dest_smb_widget)
        dsl.setContentsMargins(0,0,0,0)
        dsl.setSpacing(4)
        self.dest_smb_path = QLineEdit()
        self.dest_smb_path.setPlaceholderText("\\\\nas\\backups")
        dsl.addWidget(self.dest_smb_path)
        smb_creds = QHBoxLayout()
        self.dest_smb_user   = QLineEdit(); self.dest_smb_user.setPlaceholderText("Username")
        self.dest_smb_pass   = QLineEdit(); self.dest_smb_pass.setPlaceholderText("Password"); self.dest_smb_pass.setEchoMode(QLineEdit.Password)
        self.dest_smb_domain = QLineEdit(); self.dest_smb_domain.setPlaceholderText("Domain")
        smb_creds.addWidget(self.dest_smb_user)
        smb_creds.addWidget(self.dest_smb_pass)
        smb_creds.addWidget(self.dest_smb_domain)
        dsl.addLayout(smb_creds)
        smb_test_btn = QPushButton("Test Connection")
        smb_test_btn.setObjectName("secondary")
        smb_test_btn.clicked.connect(self._test_smb)
        dsl.addWidget(smb_test_btn)
        self.dest_smb_widget.setVisible(False)
        dg_main.addWidget(self.dest_smb_widget)

        # SFTP / FTPS destination
        self.dest_sftp_widget = QWidget()
        sfl = QFormLayout(self.dest_sftp_widget)
        sfl.setContentsMargins(0,0,0,0)
        sfl.setSpacing(4)
        self.sftp_host = QLineEdit(); self.sftp_host.setPlaceholderText("192.168.1.100 or hostname")
        self.sftp_port = QSpinBox();  self.sftp_port.setRange(1, 65535); self.sftp_port.setValue(22)
        self.sftp_user = QLineEdit(); self.sftp_user.setPlaceholderText("username")
        self.sftp_pass = QLineEdit(); self.sftp_pass.setPlaceholderText("password"); self.sftp_pass.setEchoMode(QLineEdit.Password)
        self.sftp_path = QLineEdit(); self.sftp_path.setPlaceholderText("/remote/backup/path")
        self.sftp_keyfile = QLineEdit(); self.sftp_keyfile.setPlaceholderText("Path to private key file (optional)")
        sftp_key_row = QHBoxLayout()
        sftp_key_row.addWidget(self.sftp_keyfile)
        sftp_browse_key = QPushButton("Browse"); sftp_browse_key.setObjectName("secondary"); sftp_browse_key.setMaximumWidth(70)
        sftp_browse_key.clicked.connect(lambda: self.sftp_keyfile.setText(
            QFileDialog.getOpenFileName(self, "Select Key File")[0] or self.sftp_keyfile.text()
        ))
        sftp_key_row.addWidget(sftp_browse_key)
        self.sftp_key_pass = QLineEdit(); self.sftp_key_pass.setPlaceholderText("Passphrase (if key is password-protected)"); self.sftp_key_pass.setEchoMode(QLineEdit.Password)
        sfl.addRow("Host:", self.sftp_host)
        sfl.addRow("Port:", self.sftp_port)
        sfl.addRow("User:", self.sftp_user)
        sfl.addRow("Password:", self.sftp_pass)
        sfl.addRow("Remote Path:", self.sftp_path)
        sfl.addRow("Key File:", sftp_key_row)
        sfl.addRow("Key Passphrase:", self.sftp_key_pass)
        sftp_test_btn = QPushButton("Test Connection")
        sftp_test_btn.setObjectName("secondary")
        sftp_test_btn.clicked.connect(self._test_sftp)
        sfl.addRow("", sftp_test_btn)
        self.dest_sftp_widget.setVisible(False)
        dg_main.addWidget(self.dest_sftp_widget)

        # ── Plain FTP destination ──────────────────────────────────────────
        self.dest_ftp_widget = QWidget()
        ftpl = QFormLayout(self.dest_ftp_widget)
        ftpl.setContentsMargins(0, 0, 0, 0)
        ftpl.setSpacing(4)
        self.ftp_host = QLineEdit(); self.ftp_host.setPlaceholderText("192.168.1.100 or hostname")
        self.ftp_port = QSpinBox();  self.ftp_port.setRange(1, 65535); self.ftp_port.setValue(21)
        self.ftp_user = QLineEdit(); self.ftp_user.setPlaceholderText("username")
        self.ftp_pass = QLineEdit(); self.ftp_pass.setPlaceholderText("password"); self.ftp_pass.setEchoMode(QLineEdit.Password)
        self.ftp_path = QLineEdit(); self.ftp_path.setPlaceholderText("/remote/backup/path")
        ftpl.addRow("Host:",        self.ftp_host)
        ftpl.addRow("Port:",        self.ftp_port)
        ftpl.addRow("User:",        self.ftp_user)
        ftpl.addRow("Password:",    self.ftp_pass)
        ftpl.addRow("Remote Path:", self.ftp_path)
        ftp_warn = QLabel("⚠ FTP sends credentials in plaintext. Use FTPS/SFTP when possible.")
        ftp_warn.setStyleSheet("color: #f59e0b; font-size: 11px;")
        ftp_warn.setWordWrap(True)
        ftpl.addRow("", ftp_warn)
        ftp_test_btn = QPushButton("Test Connection")
        ftp_test_btn.setObjectName("secondary")
        ftp_test_btn.clicked.connect(self._test_ftp)
        ftpl.addRow("", ftp_test_btn)
        self.dest_ftp_widget.setVisible(False)
        dg_main.addWidget(self.dest_ftp_widget)

        # ── HTTPS API destination ──────────────────────────────────────────
        self.dest_https_widget = QWidget()
        htal = QFormLayout(self.dest_https_widget)
        htal.setContentsMargins(0, 0, 0, 0)
        htal.setSpacing(4)
        self.https_url   = QLineEdit(); self.https_url.setPlaceholderText("https://backup.company.com/api/upload")
        self.https_token = QLineEdit(); self.https_token.setPlaceholderText("Bearer token (optional)"); self.https_token.setEchoMode(QLineEdit.Password)
        self.https_verify_ssl = QCheckBox("Verify SSL certificate")
        self.https_verify_ssl.setChecked(True)
        htal.addRow("Endpoint URL:", self.https_url)
        htal.addRow("Auth Token:",   self.https_token)
        htal.addRow("",              self.https_verify_ssl)
        https_note = QLabel(
            "Files are POSTed as multipart/form-data with fields 'file' and 'path'.\n"
            "Your server must accept POST requests at the endpoint above."
        )
        https_note.setStyleSheet("color: #94a3b8; font-size: 11px;")
        https_note.setWordWrap(True)
        htal.addRow("", https_note)
        https_test_btn = QPushButton("Test Connection")
        https_test_btn.setObjectName("secondary")
        https_test_btn.clicked.connect(self._test_https)
        htal.addRow("", https_test_btn)
        self.dest_https_widget.setVisible(False)
        dg_main.addWidget(self.dest_https_widget)

        gl.addWidget(dest_group)

        sched_group = QGroupBox("Schedule & Limits")
        sg = QFormLayout(sched_group)
        self.auto_check = QCheckBox("Enable auto backup")
        sg.addRow("", self.auto_check)
        interval_row = QHBoxLayout()
        self.interval_spin = QSpinBox()
        self.interval_spin.setRange(1, 1440)
        self.interval_spin.setValue(30)
        interval_row.addWidget(self.interval_spin)
        self.interval_unit = QComboBox()
        self.interval_unit.addItems(["minutes", "seconds (test only)"])
        self.interval_unit.currentIndexChanged.connect(self._on_interval_unit_changed)
        interval_row.addWidget(self.interval_unit)
        sg.addRow("Interval:", interval_row)
        self.retention_spin = QSpinBox()
        self.retention_spin.setRange(1, 365)
        self.retention_spin.setSuffix(" days")
        sg.addRow("Retention:", self.retention_spin)

        # Scheduled backup times
        sched_row = QHBoxLayout()
        self.schedule_times_input = QLineEdit()
        self.schedule_times_input.setPlaceholderText("e.g. 02:00, 14:00  (leave blank to use interval only)")
        self.schedule_times_input.setToolTip(
            "Comma-separated HH:MM times to run a full backup every day.\n"
            "These fire in addition to the regular interval.\n"
            "Example: 02:00, 14:00 backs up at 2 AM and 2 PM daily."
        )
        sched_row.addWidget(self.schedule_times_input)
        sg.addRow("Run at times:", sched_row)

        # Bandwidth throttle
        self.bw_spin = QDoubleSpinBox()
        self.bw_spin.setRange(0.0, 1000.0)
        self.bw_spin.setDecimals(1)
        self.bw_spin.setSuffix(" MB/s  (0 = unlimited)")
        self.bw_spin.setValue(0.0)
        sg.addRow("Max bandwidth:", self.bw_spin)

        # Auto-retry
        retry_row = QHBoxLayout()
        self.retry_check = QCheckBox("Auto-retry on failure")
        self.retry_delay_spin = QSpinBox()
        self.retry_delay_spin.setRange(1, 60)
        self.retry_delay_spin.setSuffix(" min delay")
        self.retry_delay_spin.setValue(5)
        retry_row.addWidget(self.retry_check)
        retry_row.addWidget(self.retry_delay_spin)
        sg.addRow("Retry:", retry_row)

        gl.addWidget(sched_group)

        startup_group = QGroupBox("Windows Startup")
        stl = QVBoxLayout(startup_group)
        self.startup_check = QCheckBox("Start with Windows (runs in background)")
        self.startup_check.stateChanged.connect(self._toggle_startup)
        stl.addWidget(self.startup_check)
        gl.addWidget(startup_group)

        pass_group = QGroupBox("Admin Password")
        pl = QVBoxLayout(pass_group)
        change_pw = QPushButton("Change Admin Password")
        change_pw.setObjectName("secondary")
        change_pw.clicked.connect(self._change_password)
        pl.addWidget(change_pw)
        gl.addWidget(pass_group)

        gl.addStretch()

        save_btn = QPushButton("Save Settings")
        save_btn.setObjectName("success")
        save_btn.clicked.connect(self._save_general)
        gl.addWidget(save_btn)

        general = QScrollArea()
        general.setWidget(general_inner)
        general.setWidgetResizable(True)
        general.setFrameShape(QFrame.NoFrame)
        general.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        tabs.addTab(general, "General")

        # ── Tab 2: Watches ──────────────────────────────────────────────────
        watches_tab = QWidget()
        wl = QVBoxLayout(watches_tab)
        wl.setContentsMargins(20, 20, 20, 20)
        wl.setSpacing(12)

        btn_row = QHBoxLayout()
        add_btn = QPushButton("➕ Add Watch")
        add_btn.setObjectName("success")
        add_btn.clicked.connect(self._add_watch)
        refresh_btn = QPushButton("↻ Refresh")
        refresh_btn.setObjectName("secondary")
        refresh_btn.clicked.connect(self._refresh_watch_table)
        btn_row.addStretch()
        btn_row.addWidget(refresh_btn)
        btn_row.addWidget(add_btn)
        wl.addLayout(btn_row)

        self.watch_table = QTableWidget(0, 13)
        self.watch_table.setHorizontalHeaderLabels([
            "Name", "Path", "Status", "Last Backup", "Duration",
            "Next Backup", "Runs", "Failed", "Size", "History", "Destination", "", ""
        ])
        for col in range(11):
            self.watch_table.horizontalHeader().setSectionResizeMode(col, QHeaderView.ResizeToContents)
        self.watch_table.horizontalHeader().setSectionResizeMode(1,  QHeaderView.Stretch)
        self.watch_table.horizontalHeader().setSectionResizeMode(10, QHeaderView.Stretch)
        self.watch_table.horizontalHeader().setSectionResizeMode(11, QHeaderView.Fixed)
        self.watch_table.horizontalHeader().setSectionResizeMode(12, QHeaderView.Fixed)
        self.watch_table.horizontalHeader().setMinimumSectionSize(60)
        self.watch_table.horizontalHeader().resizeSection(11, 80)
        self.watch_table.horizontalHeader().resizeSection(12, 80)
        self.watch_table.setMouseTracking(True)
        self.watch_table.viewport().setMouseTracking(True)
        self.watch_table.verticalHeader().setVisible(False)
        self.watch_table.verticalHeader().setDefaultSectionSize(44)
        self.watch_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.watch_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.watch_table.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.watch_table.setMinimumHeight(160)
        wl.addWidget(self.watch_table)

        tabs.addTab(watches_tab, "Watches")

        # ── Tab 3: Cloud ────────────────────────────────────────────────────
        cloud_tab = QWidget()
        cl = QVBoxLayout(cloud_tab)
        cl.setContentsMargins(20, 20, 20, 20)
        cl.setSpacing(16)

        cloud_info = QLabel(
            "Connect your cloud accounts below. Once connected, select a watch and assign "
            "a cloud provider  · backups will upload automatically after each local backup."
        )
        cloud_info.setWordWrap(True)
        cloud_info.setStyleSheet("color:#6b7280; font-size:11px;")
        cl.addWidget(cloud_info)

        # ── Google Drive card ────────────────────────────────────────────────
        gd_card = QFrame()
        gd_card.setObjectName("card")
        gd_layout = QHBoxLayout(gd_card)
        gd_layout.setContentsMargins(16, 14, 16, 14)

        gd_icon = QLabel("🔵")
        gd_icon.setStyleSheet("font-size:28px;")
        gd_layout.addWidget(gd_icon)

        gd_text = QVBoxLayout()
        gd_text.setSpacing(2)
        gd_title = QLabel("Google Drive")
        gd_title.setStyleSheet("font-size:14px; font-weight:700; color:#f1f3f9;")
        gd_text.addWidget(gd_title)
        self.gd_status_lbl = QLabel("Not connected")
        self.gd_status_lbl.setObjectName("status_err")
        gd_text.addWidget(self.gd_status_lbl)
        gd_layout.addLayout(gd_text, stretch=1)

        gd_btn_col = QVBoxLayout()
        self.gd_connect_btn = QPushButton("Connect Google Drive")
        self.gd_connect_btn.setObjectName("success")
        self.gd_connect_btn.clicked.connect(self._connect_gdrive)
        gd_btn_col.addWidget(self.gd_connect_btn)
        self.gd_disconnect_btn = QPushButton("Disconnect")
        self.gd_disconnect_btn.setObjectName("danger")
        self.gd_disconnect_btn.setVisible(False)
        self.gd_disconnect_btn.clicked.connect(self._disconnect_gdrive)
        gd_btn_col.addWidget(self.gd_disconnect_btn)
        gd_layout.addLayout(gd_btn_col)
        cl.addWidget(gd_card)

        # ── Assign cloud to watch ────────────────────────────────────────────
        assign_group = QGroupBox("Assign Cloud to Watch")
        agl = QFormLayout(assign_group)

        self.cloud_watch_combo = QComboBox()
        self.cloud_watch_combo.currentIndexChanged.connect(self._on_cloud_watch_changed)
        agl.addRow("Watch:", self.cloud_watch_combo)

        # Checkboxes for multi-cloud selection
        self.chk_gdrive  = QCheckBox("Google Drive")
        chk_row = QHBoxLayout()
        chk_row.addWidget(self.chk_gdrive)
        chk_row.addStretch()
        chk_widget = QWidget()
        chk_widget.setLayout(chk_row)
        agl.addRow("Upload to:", chk_widget)

        self.db_remote_path = QLineEdit()
        self.db_remote_path.setPlaceholderText("/backups")

        self.gd_folder_id = QLineEdit()
        self.gd_folder_id.setPlaceholderText("Optional Google Drive folder ID")
        agl.addRow("GDrive folder ID:", self.gd_folder_id)

        save_assign_btn = QPushButton("Save Assignment")
        save_assign_btn.setObjectName("success")
        save_assign_btn.clicked.connect(self._save_cloud)
        agl.addRow("", save_assign_btn)
        cl.addWidget(assign_group)

        cl.addStretch()

        tabs.addTab(cloud_tab, "Cloud")

        # ── Tab 4: Notifications ────────────────────────────────────────────
        notif_inner = QWidget()
        nl = QVBoxLayout(notif_inner)
        nl.setContentsMargins(20, 20, 20, 20)
        nl.setSpacing(16)

        # ── Email section ────────────────────────────────────────────────────
        email_group = QGroupBox("Email Notifications")
        egl = QFormLayout(email_group)
        egl.setSpacing(8)

        self.email_enabled_check = QCheckBox("Enable email notifications")
        egl.addRow("", self.email_enabled_check)

        self.email_notify_success_check = QCheckBox("Send on successful backup")
        egl.addRow("", self.email_notify_success_check)

        self.email_notify_failure_check = QCheckBox("Send on failed backup")
        self.email_notify_failure_check.setChecked(True)   # default on  · failure alerts are more important
        egl.addRow("", self.email_notify_failure_check)

        self.email_smtp_host = QLineEdit()
        self.email_smtp_host.setPlaceholderText("smtp.gmail.com")
        egl.addRow("SMTP Host:", self.email_smtp_host)

        self.email_smtp_port = QSpinBox()
        self.email_smtp_port.setRange(1, 65535)
        self.email_smtp_port.setValue(587)
        egl.addRow("SMTP Port:", self.email_smtp_port)

        self.email_use_ssl = QCheckBox("Use SSL (port 465)")
        egl.addRow("", self.email_use_ssl)

        self.email_username = QLineEdit()
        self.email_username.setPlaceholderText("your@email.com")
        egl.addRow("Username:", self.email_username)

        self.email_password = QLineEdit()
        self.email_password.setEchoMode(QLineEdit.Password)
        self.email_password.setPlaceholderText("App password or SMTP password")
        egl.addRow("Password:", self.email_password)

        self.email_from = QLineEdit()
        self.email_from.setPlaceholderText("backupsys@yourdomain.com (optional)")
        egl.addRow("From Address:", self.email_from)

        self.email_to = QLineEdit()
        self.email_to.setPlaceholderText("alerts@yourdomain.com")
        egl.addRow("To Address:", self.email_to)

        email_btn_row = QHBoxLayout()
        save_email_btn = QPushButton("Save Email Settings")
        save_email_btn.setObjectName("success")
        save_email_btn.clicked.connect(self._save_email_settings)
        test_email_btn = QPushButton("Send Test Email")
        test_email_btn.setObjectName("secondary")
        test_email_btn.clicked.connect(self._test_email)
        email_btn_row.addWidget(save_email_btn)
        email_btn_row.addWidget(test_email_btn)
        egl.addRow("", email_btn_row)

        nl.addWidget(email_group)

        # ── Webhook section ──────────────────────────────────────────────────
        webhook_group = QGroupBox("Webhook Notifications")
        wgl = QFormLayout(webhook_group)
        wgl.setSpacing(8)

        self.webhook_url_input = QLineEdit()
        self.webhook_url_input.setPlaceholderText("https://hooks.slack.com/… or https://your-api.com/webhook")
        wgl.addRow("Webhook URL:", self.webhook_url_input)

        self.webhook_success_only = QCheckBox("Only send on successful backup")
        wgl.addRow("", self.webhook_success_only)

        webhook_note = QLabel(
            "Backup results are sent as JSON via HTTP POST.  Works with Slack, Discord,\n"
            "Make/Zapier, or any custom API that accepts POST requests."
        )
        webhook_note.setStyleSheet("color:#94a3b8; font-size:11px;")
        webhook_note.setWordWrap(True)
        wgl.addRow("", webhook_note)

        webhook_btn_row = QHBoxLayout()
        save_webhook_btn = QPushButton("Save Webhook")
        save_webhook_btn.setObjectName("success")
        save_webhook_btn.clicked.connect(self._save_webhook_settings)
        test_webhook_btn = QPushButton("Send Test Ping")
        test_webhook_btn.setObjectName("secondary")
        test_webhook_btn.clicked.connect(self._test_webhook)
        webhook_btn_row.addWidget(save_webhook_btn)
        webhook_btn_row.addWidget(test_webhook_btn)
        wgl.addRow("", webhook_btn_row)

        nl.addWidget(webhook_group)
        nl.addStretch()

        notif_scroll = QScrollArea()
        notif_scroll.setWidgetResizable(True)
        notif_scroll.setFrameShape(QScrollArea.NoFrame)
        notif_scroll.setWidget(notif_inner)

        tabs.addTab(notif_scroll, "Notifications")

        layout.addWidget(tabs)

    def _on_dest_type_changed(self, idx):
        self.dest_local_widget.setVisible(idx == 0)
        self.dest_smb_widget.setVisible(idx == 1)
        self.dest_sftp_widget.setVisible(idx == 2)
        self.dest_ftp_widget.setVisible(idx == 3 or idx == 4)
        self.dest_https_widget.setVisible(idx == 5)

    def _browse_dest(self):
        path = QFileDialog.getExistingDirectory(self, "Select Backup Destination")
        if path:
            self.dest_input.setText(path)

    def _test_smb(self):
        QMessageBox.information(self, "Test", "SMB connection test not implemented yet.")

    def _test_sftp(self):
        QMessageBox.information(self, "Test", "SFTP connection test not implemented yet.")

    def _test_ftp(self):
        QMessageBox.information(self, "Test", "FTP connection test not implemented yet.")

    def _test_https(self):
        QMessageBox.information(self, "Test", "HTTPS API connection test not implemented yet.")

    def _on_interval_unit_changed(self, idx):
        # Keep interval limits reasonable for seconds mode
        if idx == 0:
            self.interval_spin.setRange(1, 1440)
        else:
            self.interval_spin.setRange(1, 60)
        self.interval_spin.setSuffix("")

    def _load_values(self):
        dtype   = self.cfg.get("dest_type", "local")
        idx_map = {"local": 0, "smb": 1, "sftp": 2, "ftps": 3, "ftp": 4, "https": 5}
        idx     = idx_map.get(dtype, 0)
        self.dest_type_combo.setCurrentIndex(idx)
        self._on_dest_type_changed(idx)
        self.dest_input.setText(self.cfg.get("destination", ""))
        smb = self.cfg.get("dest_smb", {})
        self.dest_smb_path.setText(smb.get("path", ""))
        self.dest_smb_user.setText(smb.get("user", ""))
        self.dest_smb_pass.setText(smb.get("pass", ""))
        self.dest_smb_domain.setText(smb.get("domain", ""))
        sftp = self.cfg.get("dest_sftp", {})
        self.sftp_host.setText(sftp.get("host", ""))
        self.sftp_port.setValue(sftp.get("port", 22))
        self.sftp_user.setText(sftp.get("user", ""))
        self.sftp_pass.setText(sftp.get("pass", ""))
        self.sftp_path.setText(sftp.get("path", ""))
        self.sftp_keyfile.setText(sftp.get("keyfile", ""))
        self.sftp_key_pass.setText(sftp.get("key_pass", ""))
        ftp = self.cfg.get("dest_ftp", {})
        self.ftp_host.setText(ftp.get("host", ""))
        self.ftp_port.setValue(ftp.get("port", 21))
        self.ftp_user.setText(ftp.get("user", ""))
        self.ftp_pass.setText(ftp.get("pass", ""))
        self.ftp_path.setText(ftp.get("path", ""))
        api = self.cfg.get("dest_https", {})
        self.https_url.setText(api.get("url", ""))
        self.https_token.setText(api.get("token", ""))
        self.https_verify_ssl.setChecked(api.get("verify_ssl", True))
        self.auto_check.setChecked(self.cfg.get("auto_backup", False))
        unit = self.cfg.get("interval_unit", "minutes")
        self.interval_unit.setCurrentIndex(1 if unit == "seconds" else 0)
        self._on_interval_unit_changed(1 if unit == "seconds" else 0)
        self.interval_spin.setValue(self.cfg.get("interval_min", 30))
        self.retention_spin.setValue(self.cfg.get("retention_days", 30))
        # Scheduled backup times  · display as comma-separated "HH:MM" string
        sched = self.cfg.get("backup_schedule_times", [])
        self.schedule_times_input.setText(", ".join(sched) if sched else "")
        try:
            self.bw_spin.setValue(float(self.cfg.get("max_backup_mbps", 0.0)))
        except Exception:
            pass
        self.retry_check.setChecked(self.cfg.get("auto_retry", False))
        self.retry_delay_spin.setValue(int(self.cfg.get("retry_delay_min", 5)))
        self.startup_check.setChecked(self._is_startup_enabled())
        self._refresh_watch_table()
        self._refresh_cloud_combo()
        self._check_cloud_connections()
        # Load email + webhook settings
        ec = self.cfg.get("email_config", {})
        self.email_enabled_check.setChecked(ec.get("enabled", False))
        self.email_notify_success_check.setChecked(ec.get("notify_on_success", False))
        self.email_notify_failure_check.setChecked(ec.get("notify_on_failure", True))
        self.email_smtp_host.setText(ec.get("smtp_host", ""))
        self.email_smtp_port.setValue(int(ec.get("smtp_port", 587)))
        self.email_use_ssl.setChecked(ec.get("smtp_use_ssl", False))
        self.email_username.setText(ec.get("username", ""))
        self.email_password.setText(ec.get("password", ""))
        self.email_from.setText(ec.get("from_addr", ""))
        self.email_to.setText(ec.get("to_addr", ""))
        self.webhook_url_input.setText(self.cfg.get("webhook_url", ""))
        self.webhook_success_only.setChecked(self.cfg.get("webhook_on_success", False))

    def _refresh_cloud_combo(self):
        self.cloud_watch_combo.blockSignals(True)
        self.cloud_watch_combo.clear()
        for w in self.cfg.get("watches", []):
            self.cloud_watch_combo.addItem(w.get("name", w["id"]), w["id"])
        self.cloud_watch_combo.blockSignals(False)
        self._on_cloud_watch_changed()

    def _on_cloud_watch_changed(self):
        idx = self.cloud_watch_combo.currentIndex()
        if idx < 0:
            return
        wid   = self.cloud_watch_combo.itemData(idx)
        watch = next((w for w in self.cfg.get("watches", []) if w["id"] == wid), None)
        if not watch:
            return

        # Support both old single cloud_config and new cloud_configs list
        configs   = watch.get("cloud_configs", [])
        old_cfg   = watch.get("cloud_config", {})
        if not configs and old_cfg:
            configs = [old_cfg]

        providers = {c.get("provider") for c in configs}
        if hasattr(self, "chk_gdrive"):
            self.chk_gdrive.setChecked("gdrive" in providers)

        # Populate fields from existing configs
        for c in configs:
                self.db_remote_path.setText(c.get("remote_path", "/backups"))
                if c.get("provider") == "gdrive" and hasattr(self, "gd_folder_id"):
                        self.gd_folder_id.setText(c.get("folder_id", ""))

    def _on_cloud_provider_ui_changed(self, provider: str):
        """Kept for backward compatibility  · no-op since we now use checkboxes."""
        pass

    def _on_provider_changed(self, provider: str):
        """Kept for backward compatibility."""
        pass

    # ── OAuth credentials  · loaded from .env file ────────────────────────────
    # Create a .env file in your project folder with:
    #   GDRIVE_CLIENT_ID=your_client_id
    #   GDRIVE_CLIENT_SECRET=your_client_secret

    @staticmethod
    def _load_env_credentials():
        """Load OAuth credentials from .env file."""
        import os
        from pathlib import Path
        env_path = Path(__file__).parent / ".env"
        creds = {
            "GDRIVE_CLIENT_ID":     "",
            "GDRIVE_CLIENT_SECRET": "",
        }
        # Try python-dotenv first
        try:
            from dotenv import dotenv_values
            loaded = dotenv_values(env_path)
            for k in creds:
                if k in loaded:
                    creds[k] = loaded[k]
            return creds
        except ImportError:
            pass
        # Fallback: manual parse
        if env_path.exists():
            for line in env_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                k = k.strip()
                v = v.strip().strip('"\'\' ')
                if k in creds:
                    creds[k] = v
        # Also check environment variables
        for k in creds:
            if not creds[k]:
                creds[k] = os.environ.get(k, "")
        return creds

    @property
    def GDRIVE_CLIENT_ID(self):
        return self._load_env_credentials()["GDRIVE_CLIENT_ID"]

    @property
    def GDRIVE_CLIENT_SECRET(self):
        return self._load_env_credentials()["GDRIVE_CLIENT_SECRET"]

    def _connect_gdrive(self):
        """Open browser for Google OAuth, catch callback on localhost."""
        import urllib.parse, threading, webbrowser
        from http.server import HTTPServer, BaseHTTPRequestHandler

        REDIRECT_URI = "http://localhost:8765/oauth/gdrive"
        SCOPES       = "https://www.googleapis.com/auth/drive.file"

        client_id = self.GDRIVE_CLIENT_ID
        if not client_id:
            from pathlib import Path
            env_path = Path(__file__).parent / ".env"
            env_exists = env_path.exists()
            raw = ""
            if env_exists:
                try:
                    raw = env_path.read_text(encoding="utf-8")[:300]
                except Exception as re:
                    raw = f"(read error: {re})"
            QMessageBox.warning(self, "Not configured",
                f"GDRIVE_CLIENT_ID not found.\n\n"
                f".env path: {env_path}\n"
                f".env exists: {env_exists}\n\n"
                f"Contents preview:\n{raw if env_exists else '(file not found)'}\n\n"
                f"Make sure your .env file has:\n"
                f"GDRIVE_CLIENT_ID=your_client_id"
            )
            return

        params = {
            "client_id":     client_id,
            "redirect_uri":  REDIRECT_URI,
            "response_type": "code",
            "scope":         SCOPES,
            "access_type":   "offline",
            "prompt":        "consent",
        }
        url = "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params)

        self.gd_connect_btn.setText("Waiting for login…")
        self.gd_connect_btn.setEnabled(False)
        webbrowser.open(url)

        # Use a result queue so background thread can pass result safely
        self._gdrive_result = {}

        class _Handler(BaseHTTPRequestHandler):
            def do_GET(self_handler):
                parsed = urllib.parse.urlparse(self_handler.path)
                qp     = urllib.parse.parse_qs(parsed.query)
                code   = qp.get("code", [None])[0]
                self_handler.send_response(200)
                self_handler.send_header("Content-type", "text/html")
                self_handler.end_headers()
                if code:
                    self_handler.wfile.write(
                        b"<html><body style='font-family:sans-serif;text-align:center;padding:60px'>"
                        b"<h2 style='color:#22c55e'>Connected! You can close this tab.</h2>"
                        b"<p>Return to the Backup System app.</p></body></html>"
                    )
                    self._gdrive_result["code"] = code
                else:
                    self_handler.wfile.write(b"<h2>Login failed. Please try again.</h2>")
                    self._gdrive_result["error"] = "No code returned"
            def log_message(self, *a): pass

        def _serve():
            try:
                srv = HTTPServer(("localhost", 8765), _Handler)
                srv.timeout = 180
                srv.handle_request()
                srv.server_close()
            except Exception as e:
                self._gdrive_result["error"] = str(e)

        threading.Thread(target=_serve, daemon=True).start()

        # Poll every 500ms for result (max 3 min)
        self._gdrive_poll_count = 0
        self._gdrive_poll_timer = QTimer(self)
        self._gdrive_poll_timer.timeout.connect(self._poll_gdrive_result)
        self._gdrive_poll_timer.start(500)

    def _poll_gdrive_result(self):
        self._gdrive_poll_count += 1
        if self._gdrive_poll_count > 360:  # 3 min timeout
            self._gdrive_poll_timer.stop()
            self._gdrive_connect_failed("Timed out waiting for login")
            return
        if "error" in self._gdrive_result:
            self._gdrive_poll_timer.stop()
            self._gdrive_connect_failed(self._gdrive_result["error"])
        elif "code" in self._gdrive_result:
            self._gdrive_poll_timer.stop()
            code = self._gdrive_result.pop("code")
            self._exchange_gdrive_code(code, "http://localhost:8765/oauth/gdrive")

    def _exchange_gdrive_code(self, code: str, redirect_uri: str):
        """Exchange auth code for access + refresh tokens."""
        import urllib.request, urllib.parse, json as _json
        data = urllib.parse.urlencode({
            "code":          code,
            "client_id":     self.GDRIVE_CLIENT_ID,
            "client_secret": self.GDRIVE_CLIENT_SECRET,
            "redirect_uri":  redirect_uri,
            "grant_type":    "authorization_code",
        }).encode()
        try:
            req    = urllib.request.Request("https://oauth2.googleapis.com/token", data=data)
            resp   = urllib.request.urlopen(req, timeout=15)
            tokens = _json.loads(resp.read())
            self._gdrive_connected(tokens)
        except Exception as e:
            self._gdrive_connect_failed(str(e))

    def _gdrive_connected(self, tokens: dict):
        s = QSettings(SETTINGS_ORG, SETTINGS_APP)
        s.setValue("gdrive_access_token",  tokens.get("access_token", ""))
        s.setValue("gdrive_refresh_token", tokens.get("refresh_token", ""))
        self.gd_status_lbl.setText("▶  Connected")
        self.gd_status_lbl.setObjectName("status_ok")
        self.gd_status_lbl.style().unpolish(self.gd_status_lbl)
        self.gd_status_lbl.style().polish(self.gd_status_lbl)
        self.gd_connect_btn.setVisible(False)
        self.gd_disconnect_btn.setVisible(True)
        QMessageBox.information(self, "Google Drive", "Google Drive connected successfully!")

    def _gdrive_connect_failed(self, err=""):
        self.gd_connect_btn.setText("Connect Google Drive")
        self.gd_connect_btn.setEnabled(True)
        QMessageBox.critical(self, "Google Drive", f"Connection failed: {err}")

    def _disconnect_gdrive(self):
        s = QSettings(SETTINGS_ORG, SETTINGS_APP)
        s.remove("gdrive_access_token")
        s.remove("gdrive_refresh_token")
        self.gd_status_lbl.setText("Not connected")
        self.gd_status_lbl.setObjectName("status_err")
        self.gd_status_lbl.style().unpolish(self.gd_status_lbl)
        self.gd_status_lbl.style().polish(self.gd_status_lbl)
        self.gd_connect_btn.setVisible(True)
        self.gd_connect_btn.setText("Connect Google Drive")
        self.gd_connect_btn.setEnabled(True)
        self.gd_disconnect_btn.setVisible(False)


    def _check_cloud_connections(self):
        """Update connect/disconnect state and validate tokens."""
        s = QSettings(SETTINGS_ORG, SETTINGS_APP)
        if s.value("gdrive_access_token", ""):
            self.gd_status_lbl.setText("✓ Connected")
            self.gd_status_lbl.setObjectName("status_ok")
            self.gd_connect_btn.setVisible(False)
            self.gd_disconnect_btn.setVisible(True)
        # Validate tokens in background and warn if expired
        self._validate_cloud_tokens()

    def _validate_cloud_tokens(self):
        """Check if saved tokens are still valid  · runs in a background thread."""
        import threading
        def _check():
            s        = QSettings(SETTINGS_ORG, SETTINGS_APP)
            warnings = []
            # Check GDrive
            gd_token = s.value("gdrive_access_token", "")
            if gd_token:
                try:
                    import urllib.request as _ur
                    req = _ur.Request(
                        "https://www.googleapis.com/oauth2/v1/tokeninfo"
                        f"?access_token={gd_token}"
                    )
                    resp = _ur.urlopen(req, timeout=10)
                    info = __import__("json").loads(resp.read())
                    expires_in = int(info.get("expires_in", 0))
                    if expires_in < 300:  # less than 5 mins
                        # Try silent refresh first
                        refreshed = self._silent_refresh_gdrive()
                        if not refreshed:
                            warnings.append("gdrive")
                except Exception:
                    # Token invalid  · try refresh
                    refreshed = self._silent_refresh_gdrive()
                    if not refreshed:
                        warnings.append("gdrive")


            if warnings:
                # Use QTimer to update UI on main thread
                from PyQt5.QtCore import QTimer
                QTimer.singleShot(0, lambda: self._on_token_warnings(warnings))

        t = threading.Thread(target=_check, daemon=True)
        t.start()

    def _silent_refresh_gdrive(self) -> bool:
        """Try to refresh GDrive token silently. Returns True if successful."""
        try:
            import urllib.request as _ur, urllib.parse, json as _json
            s             = QSettings(SETTINGS_ORG, SETTINGS_APP)
            refresh_token = s.value("gdrive_refresh_token", "")
            if not refresh_token:
                return False
            data = urllib.parse.urlencode({
                "client_id":     self.GDRIVE_CLIENT_ID,
                "client_secret": self.GDRIVE_CLIENT_SECRET,
                "refresh_token": refresh_token,
                "grant_type":    "refresh_token",
            }).encode()
            req    = _ur.Request("https://oauth2.googleapis.com/token", data=data)
            tokens = _json.loads(_ur.urlopen(req, timeout=15).read())
            if tokens.get("access_token"):
                s.setValue("gdrive_access_token", tokens["access_token"])
                # Update cloud_configs in all watches
                for w in self.cfg.get("watches", []):
                    for cc in w.get("cloud_configs", []):
                        if cc.get("provider") == "gdrive":
                            cc["access_token"] = tokens["access_token"]
                    if w.get("cloud_config", {}).get("provider") == "gdrive":
                        w["cloud_config"]["access_token"] = tokens["access_token"]
                import config_manager as _cm
                _cm.save(self.cfg)
                from PyQt5.QtCore import QTimer
                QTimer.singleShot(0, lambda: self._append_log("🔄 Google Drive token refreshed silently"))
                return True
        except Exception:
            pass
        return False


    def _on_token_warnings(self, warnings: list):
        """Called on main thread when token validation finds expired tokens."""
        for provider in warnings:
            name = "Google Drive"
            # 1. Update Cloud tab status label
            if provider == "gdrive" and hasattr(self, "gd_status_lbl"):
                self.gd_status_lbl.setText("⚠ Token expired  · reconnect")
                self.gd_status_lbl.setObjectName("status_err")
                self.gd_status_lbl.style().unpolish(self.gd_status_lbl)
                self.gd_status_lbl.style().polish(self.gd_status_lbl)
                self.gd_connect_btn.setVisible(True)
                self.gd_connect_btn.setText("🔄 Reconnect Google Drive")
            # 2. Tray notification
            if hasattr(self, "_tray"):
                self._tray.showMessage(
                    f"⚠ {name}  · Reconnect Required",
                    f"Your {name} token has expired.\n"
                    f"Open Settings >Cloud tab >Reconnect to continue cloud backups.",
                    QSystemTrayIcon.Warning, 8000
                )
            # 3. Log
            if hasattr(self, "log_text"):
                self._append_log(
                    f"⚠ {name} token expired  · go to Settings >Cloud tab to reconnect"
                )

    def _save_cloud(self):
        idx = self.cloud_watch_combo.currentIndex()
        if idx < 0:
            QMessageBox.warning(self, "No Watch", "Please add a watch first.")
            return
        wid    = self.cloud_watch_combo.itemData(idx)
        s      = QSettings(SETTINGS_ORG, SETTINGS_APP)
        use_gd = hasattr(self, "chk_gdrive")  and self.chk_gdrive.isChecked()
        use_db = False

        cloud_configs = []

        if use_gd:
            token = s.value("gdrive_access_token", "")
            if not token:
                QMessageBox.warning(self, "Not Connected", "Please connect Google Drive first.")
                return
            cloud_configs.append({
                "provider":      "gdrive",
                "access_token":  token,
                "refresh_token": s.value("gdrive_refresh_token", ""),
                "client_id":     self.GDRIVE_CLIENT_ID,
                "client_secret": self.GDRIVE_CLIENT_SECRET,
                "folder_id":     self.gd_folder_id.text().strip(),
            })

        if use_db:
            self.cfg["dest_sftp"] = {}
        elif dtype in (2, 3):  # SFTP or FTPS
            import tempfile
            proto = "sftp" if dtype == 2 else "ftps"
            staging = os.path.join(tempfile.gettempdir(), f"backupsys_{proto}_staging")
            os.makedirs(staging, exist_ok=True)
            self.cfg["dest_type"] = proto
            self.cfg["dest_sftp"] = {
                "proto":   proto,
                "host":    self.sftp_host.text().strip(),
                "port":    self.sftp_port.value(),
                "user":    self.sftp_user.text().strip(),
                "pass":    self.sftp_pass.text(),
                "path":    self.sftp_path.text().strip(),
                "keyfile": self.sftp_keyfile.text().strip(),
                "key_pass": self.sftp_key_pass.text(),
            }
            self.cfg["destination"] = staging   # local staging; uploaded to SFTP/FTPS after backup
            self.cfg["dest_smb"]    = {}
            self.cfg["dest_ftp"]    = {}
            self.cfg["dest_https"]  = {}

        elif dtype == 4:  # Plain FTP
            import tempfile
            staging = os.path.join(tempfile.gettempdir(), "backupsys_ftp_staging")
            os.makedirs(staging, exist_ok=True)
            self.cfg["dest_type"]   = "ftp"
            self.cfg["dest_ftp"]    = {
                "host": self.ftp_host.text().strip(),
                "port": self.ftp_port.value(),
                "user": self.ftp_user.text().strip(),
                "pass": self.ftp_pass.text(),
                "path": self.ftp_path.text().strip(),
            }
            self.cfg["destination"] = staging   # local staging; uploaded to FTP after backup
            self.cfg["dest_sftp"]   = {}
            self.cfg["dest_smb"]    = {}
            self.cfg["dest_https"]  = {}

        elif dtype == 5:  # HTTPS API
            import tempfile
            staging = os.path.join(tempfile.gettempdir(), "backupsys_https_staging")
            os.makedirs(staging, exist_ok=True)
            self.cfg["dest_type"]   = "https"
            self.cfg["dest_https"]  = {
                "url":        self.https_url.text().strip(),
                "token":      self.https_token.text().strip(),
                "verify_ssl": self.https_verify_ssl.isChecked(),
            }
            self.cfg["destination"] = staging   # local staging; uploaded to API after backup
            self.cfg["dest_sftp"]   = {}
            self.cfg["dest_smb"]    = {}
            self.cfg["dest_ftp"]    = {}

        self.cfg["auto_backup"]    = self.auto_check.isChecked()
        self.cfg["interval_unit"]  = "seconds" if self.interval_unit.currentIndex() == 1 else "minutes"
        self.cfg["interval_min"]   = self.interval_spin.value()
        self.cfg["retention_days"] = self.retention_spin.value()
        # Parse and validate scheduled backup times (comma-separated HH:MM)
        raw_sched = self.schedule_times_input.text().strip()
        if raw_sched:
            import re as _re
            times = []
            for tok in raw_sched.split(","):
                tok = tok.strip()
                if _re.match(r"^\d{2}:\d{2}$", tok):
                    h, m = int(tok[:2]), int(tok[3:5])
                    if 0 <= h <= 23 and 0 <= m <= 59:
                        times.append(tok)
            self.cfg["backup_schedule_times"] = times
        else:
            self.cfg["backup_schedule_times"] = []
        try:
            self.cfg["max_backup_mbps"] = float(self.bw_spin.value())
        except Exception:
            self.cfg["max_backup_mbps"] = 0.0
        self.cfg["auto_retry"]      = self.retry_check.isChecked()
        self.cfg["retry_delay_min"] = self.retry_delay_spin.value()
        try:
            # ── Auto-reset snapshots when destination changes ──────────────
            # If the user changed the remote host/path, the old snapshots are
            # no longer valid  · reset them so the next backup is a full backup.
            _old_sftp = getattr(self, "_saved_dest_sftp", {})
            _new_sftp = self.cfg.get("dest_sftp", {})
            _dest_changed = (
                _old_sftp.get("host") != _new_sftp.get("host") or
                _old_sftp.get("port") != _new_sftp.get("port") or
                _old_sftp.get("path") != _new_sftp.get("path") or
                getattr(self, "_saved_dest_type", None) != self.cfg.get("dest_type")
            )
            if _dest_changed and BACKEND_AVAILABLE:
                for w in self.cfg.get("watches", []):
                    try:
                        config_manager.delete_snapshot(w["id"])
                        w["needs_full_backup"] = True
                    except Exception:
                        pass
                config_manager.save(self.cfg)

            config_manager.save(self.cfg)

            # Remember current dest for next change-detection
            self._saved_dest_sftp = dict(self.cfg.get("dest_sftp", {}))
            self._saved_dest_type = self.cfg.get("dest_type")

            if _dest_changed:
                QMessageBox.information(self, "Saved",
                    "Settings saved.\n\nDestination changed  · next backup will be a full backup automatically.")
            else:
                QMessageBox.information(self, "Saved", "Settings saved successfully.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to save: {e}")

    def _add_watch(self):
        dlg = AddWatchDialog(self, cfg=self.cfg)
        if dlg.exec_() == QDialog.Accepted:
            v = dlg.get_values()
            try:
                config_manager.add_watch(
                    self.cfg, v["name"], v["path"],
                    interval_min=v["interval_min"]
                )
                if v["compression"]:
                    w = config_manager.get_watch_by_path(self.cfg, v["path"])
                    if w:
                        config_manager.update_watch_meta(self.cfg, w["id"], compression=True)
                self._refresh_watch_table()
                self.watches_changed.emit()
            except Exception as e:
                QMessageBox.critical(self, "Error", str(e))

    def _remove_watch(self):
        btn = self.sender()
        wid = btn.property("watch_id")
        watch = next((w for w in self.cfg.get("watches", []) if w["id"] == wid), None)
        watch_name = watch.get("name", "this watch") if watch else "this watch"
        reply = QMessageBox.question(
            self, "Delete Watch",
            f"Delete <b>{watch_name}</b> from the watch list?<br><br>Your backup files will <b>not</b> be deleted.",
            QMessageBox.Yes | QMessageBox.Cancel
        )
        if reply == QMessageBox.Yes:
            config_manager.remove_watch(self.cfg, wid)
            self._refresh_watch_table()
            self.watches_changed.emit()

    def _refresh_watch_table(self):
        self.watch_table.setRowCount(0)
        watches = self.cfg.get("watches", [])
        for row, w in enumerate(watches):
            self.watch_table.insertRow(row)
            self.watch_table.setItem(row, 0, QTableWidgetItem(w.get("name", "")))
            self.watch_table.setItem(row, 1, QTableWidgetItem(w.get("path", "")))
            self.watch_table.setItem(row, 2, QTableWidgetItem(w.get("status", "")))
            self.watch_table.setItem(row, 3, QTableWidgetItem(w.get("last_backup", "")))
            self.watch_table.setItem(row, 4, QTableWidgetItem(str(w.get("duration", ""))))
            self.watch_table.setItem(row, 5, QTableWidgetItem(w.get("next_backup", "")))
            self.watch_table.setItem(row, 6, QTableWidgetItem(str(w.get("runs", ""))))
            self.watch_table.setItem(row, 7, QTableWidgetItem(str(w.get("failed", ""))))
            self.watch_table.setItem(row, 8, QTableWidgetItem(w.get("size", "")))
            self.watch_table.setItem(row, 9, QTableWidgetItem(w.get("history", "")))
            self.watch_table.setItem(row, 10, QTableWidgetItem(w.get("destination", "")))
            self.watch_table.setItem(row, 11, QTableWidgetItem(""))
            self.watch_table.setItem(row, 12, QTableWidgetItem(""))

    def _save_email_settings(self):
        ec = self.cfg.setdefault("email_config", {})
        ec["enabled"]           = self.email_enabled_check.isChecked()
        ec["notify_on_success"] = self.email_notify_success_check.isChecked()
        ec["notify_on_failure"] = self.email_notify_failure_check.isChecked()
        ec["smtp_host"]        = self.email_smtp_host.text().strip()
        ec["smtp_port"]        = self.email_smtp_port.value()
        ec["smtp_use_ssl"]     = self.email_use_ssl.isChecked()
        ec["username"]         = self.email_username.text().strip()
        ec["password"]         = self.email_password.text()
        ec["from_addr"]        = self.email_from.text().strip()
        ec["to_addr"]          = self.email_to.text().strip()
        try:
            config_manager.save(self.cfg)
            QMessageBox.information(self, "Saved", "Email settings saved.")
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

    def _test_email(self):
        """Send a quick test email using the current (unsaved) form values."""
        ec = {
            "enabled":      True,
            "smtp_host":    self.email_smtp_host.text().strip(),
            "smtp_port":    self.email_smtp_port.value(),
            "smtp_use_ssl": self.email_use_ssl.isChecked(),
            "username":     self.email_username.text().strip(),
            "password":     self.email_password.text(),
            "from_addr":    self.email_from.text().strip(),
            "to_addr":      self.email_to.text().strip(),
        }
        to = ec["to_addr"]
        if not to:
            QMessageBox.warning(self, "Missing", "Please enter a To Address first.")
            return
        try:
            # Prefer notification_utils.test_email() which returns a proper result dict
            if _NOTIFICATION_UTILS_AVAILABLE:
                from notification_utils import test_email as _nu_test_email
                result = _nu_test_email(ec)
            else:
                # Inline fallback using send_email_notification directly
                from notification_utils import send_email_notification as _nu_send_email
                result = _nu_send_email(
                    ec,
                    "▶ Backup System  · Test Email",
                    "This is a test email from your Backup System app.\n\nIf you received this, email notifications are working correctly.",
                )

            if result.get("ok"):
                QMessageBox.information(
                    self, "Test Email Sent",
                    f"Test email sent to {to}.\nCheck your inbox (and spam folder)."
                )
            else:
                QMessageBox.critical(
                    self, "Test Failed",
                    f"Could not send test email:\n\n{result.get('error', 'Unknown error')}"
                )
        except Exception as e:
            QMessageBox.critical(self, "Test Failed", str(e))

    def _save_webhook_settings(self):
        self.cfg["webhook_url"]        = self.webhook_url_input.text().strip()
        self.cfg["webhook_on_success"] = self.webhook_success_only.isChecked()
        try:
            config_manager.save(self.cfg)
            QMessageBox.information(self, "Saved", "Webhook settings saved.")
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

    def _test_webhook(self):
        """Send a test ping to the webhook URL."""
        url = self.webhook_url_input.text().strip()
        if not url:
            QMessageBox.warning(self, "Missing", "Please enter a Webhook URL first.")
            return
        test_cfg = {"webhook_url": url, "webhook_on_success": False}
        test_result = {
            "status": "success",
            "watch_id": "test",
            "watch_name": "Test Watch",
            "files_copied": 42,
            "files_changed": 3,
            "total_size": "1.2 MB",
            "duration_s": 0.5,
            "timestamp": datetime.now().isoformat(),
            "triggered_by": "test",
            "error": None,
        }
        try:
            _send_webhook(test_cfg, test_result)
            QMessageBox.information(self, "Webhook Test", f"Test ping sent to:\n{url}\n\nCheck your endpoint for the request.")
        except Exception as e:
            QMessageBox.critical(self, "Test Failed", str(e))

    def _change_password(self):
        dlg = PasswordDialog(self, mode="set")
        dlg.exec_()

    def _save_general(self):
        self.cfg["dest_type"] = ["local", "smb", "sftp", "ftps", "ftp", "https"][self.dest_type_combo.currentIndex()]
        self.cfg["destination"] = self.dest_input.text().strip()
        self.cfg["dest_smb"] = {
            "path": self.dest_smb_path.text().strip(),
            "user": self.dest_smb_user.text().strip(),
            "pass": self.dest_smb_pass.text(),
            "domain": self.dest_smb_domain.text().strip(),
        }
        self.cfg["dest_sftp"] = {
            "host": self.sftp_host.text().strip(),
            "port": self.sftp_port.value(),
            "user": self.sftp_user.text().strip(),
            "pass": self.sftp_pass.text(),
            "path": self.sftp_path.text().strip(),
            "keyfile": self.sftp_keyfile.text().strip(),
            "key_pass": self.sftp_key_pass.text(),
        }
        self.cfg["dest_ftp"] = {
            "host": self.ftp_host.text().strip(),
            "port": self.ftp_port.value(),
            "user": self.ftp_user.text().strip(),
            "pass": self.ftp_pass.text(),
            "path": self.ftp_path.text().strip(),
        }
        self.cfg["dest_https"] = {
            "url": self.https_url.text().strip(),
            "token": self.https_token.text().strip(),
            "verify_ssl": self.https_verify_ssl.isChecked(),
        }
        self.cfg["auto_backup"] = self.auto_check.isChecked()
        self.cfg["interval_unit"] = "seconds" if self.interval_unit.currentIndex() == 1 else "minutes"
        self.cfg["interval_min"] = self.interval_spin.value()
        self.cfg["retention_days"] = self.retention_spin.value()
        self.cfg["backup_schedule_times"] = [t.strip() for t in self.schedule_times_input.text().split(",") if t.strip()]
        self.cfg["max_backup_mbps"] = self.bw_spin.value()
        self.cfg["auto_retry"] = self.retry_check.isChecked()
        self.cfg["retry_delay_min"] = self.retry_delay_spin.value()
        try:
            config_manager.save(self.cfg)
            QMessageBox.information(self, "Saved", "Settings saved.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to save settings: {e}")

    def _toggle_startup(self, state):
        if state == Qt.Checked:
            self._set_startup(True)
        else:
            self._set_startup(False)

    def _set_startup(self, enable: bool):
        if not WINREG_AVAILABLE:
            QMessageBox.warning(self, "Startup", "Run-at-startup is only supported on Windows.")
            return
        try:
            key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, STARTUP_REG_KEY, 0, winreg.KEY_SET_VALUE)
            if enable:
                exe = sys.executable
                script = os.path.abspath(__file__)
                winreg.SetValueEx(key, APP_NAME, 0, winreg.REG_SZ, f'"{exe}" "{script}"')
            else:
                try:
                    winreg.DeleteValue(key, APP_NAME)
                except FileNotFoundError:
                    pass
            winreg.CloseKey(key)
        except Exception as e:
            QMessageBox.warning(self, "Startup", f"Could not update startup: {e}")

    def _is_startup_enabled(self) -> bool:
        if not WINREG_AVAILABLE:
            return False
        try:
            key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, STARTUP_REG_KEY, 0, winreg.KEY_READ)
            try:
                winreg.QueryValueEx(key, APP_NAME)
                winreg.CloseKey(key)
                return True
            except FileNotFoundError:
                winreg.CloseKey(key)
                return False
        except Exception:
            return False


# ══════════════════════════════════════════════════════════════════════════════
# ── Watch Card Widget ──────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

class WatchCard(QFrame):
    backup_requested          = pyqtSignal(dict)
    full_backup_requested     = pyqtSignal(dict)  # watch  · force full backup
    validate_requested        = pyqtSignal(dict)
    restore_requested         = pyqtSignal(dict)
    pause_requested           = pyqtSignal(str, bool)   # watch_id, paused
    cancel_requested          = pyqtSignal(str)         # watch_id
    open_backup_requested     = pyqtSignal(str)      # watch_id

    def __init__(self, watch: dict, parent=None):
        super().__init__(parent)
        self.watch        = watch
        self._changes     = []   # list of recent change entries
        self._expanded    = False
        self.setObjectName("card")
        self.setMinimumHeight(90)
        self._build_ui()

    def _build_ui(self):
        self._root_layout = QVBoxLayout(self)
        self._root_layout.setContentsMargins(0, 0, 0, 0)
        self._root_layout.setSpacing(0)

        # ── Top row ───────────────────────────────────────────────────────
        top = QWidget()
        layout = QHBoxLayout(top)
        layout.setContentsMargins(16, 14, 16, 14)
        layout.setSpacing(12)

        color = self.watch.get("color", "") or "#2563eb"
        strip = QFrame()
        strip.setFixedWidth(4)
        strip.setStyleSheet(f"background-color: {color}; border-radius: 2px;")
        layout.addWidget(strip)

        info_layout = QVBoxLayout()
        info_layout.setSpacing(4)

        name_row = QHBoxLayout()
        name_lbl = QLabel(self.watch.get("name", "Unknown"))
        name_lbl.setStyleSheet("font-size: 14px; font-weight: 700; color: #f1f3f9;")
        name_row.addWidget(name_lbl)

        paused = self.watch.get("paused", False)
        if paused:
            badge = QLabel("PAUSED")
            badge.setStyleSheet("background:#f59e0b; color:#000; font-size:9px; font-weight:700;"
                                "padding:2px 6px; border-radius:3px;")
            name_row.addWidget(badge)

        # Change count badge
        self.change_badge = QLabel("")
        self.change_badge.setStyleSheet(
            "background:#dc2626; color:white; font-size:9px; font-weight:700;"
            "padding:2px 7px; border-radius:8px;"
        )
        self.change_badge.setVisible(False)
        name_row.addWidget(self.change_badge)

        # Expand/collapse toggle
        self.toggle_btn = QPushButton("▾ Changes")
        self.toggle_btn.setObjectName("secondary")
        self.toggle_btn.setFixedHeight(22)
        self.toggle_btn.setStyleSheet(
            "font-size:10px; padding:0 8px; border-radius:4px;"
            "background:#2e3340; color:#6b7280; border:1px solid #3d4455;"
        )
        self.toggle_btn.setVisible(False)
        self.toggle_btn.clicked.connect(self._toggle_changes)
        name_row.addWidget(self.toggle_btn)

        name_row.addStretch()
        info_layout.addLayout(name_row)

        path_lbl = QLabel(self.watch.get("path", ""))
        path_lbl.setStyleSheet("color: #6b7280; font-size: 11px;")
        path_lbl.setWordWrap(True)
        info_layout.addWidget(path_lbl)

        lb = self.watch.get("last_backup", "")
        lb_text = "Never backed up"
        if lb:
            try:
                dt = datetime.fromisoformat(lb)
                lb_text = f"Last backup: {dt.strftime('%b %d, %Y %H:%M')}"
            except Exception:
                lb_text = f"Last backup: {lb}"

        count = self.watch.get("backup_count", 0)
        self.meta_lbl = QLabel(f"{lb_text}   ·   {count} backup(s)")
        self.meta_lbl.setStyleSheet("color: #4b5563; font-size: 11px;")
        info_layout.addWidget(self.meta_lbl)

        layout.addLayout(info_layout, stretch=1)

        self.status_widget = QWidget()
        status_layout = QVBoxLayout(self.status_widget)
        status_layout.setSpacing(4)
        status_layout.setContentsMargins(0, 0, 0, 0)

        self.status_lbl = QLabel("▶ Watching")
        self.status_lbl.setObjectName("status_ok")
        self.status_lbl.setAlignment(Qt.AlignRight)
        status_layout.addWidget(self.status_lbl)

        self.progress_bar = QProgressBar()
        self.progress_bar.setMinimumWidth(130)
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(False)
        status_layout.addWidget(self.progress_bar)

        self.backup_btn = QPushButton("Backup Now")
        self.backup_btn.setMinimumWidth(130)
        self.backup_btn.clicked.connect(lambda: self.backup_requested.emit(self.watch))
        status_layout.addWidget(self.backup_btn)

        self.full_backup_btn = QPushButton("⟳ Force Full Backup")
        self.full_backup_btn.setObjectName("secondary")
        self.full_backup_btn.setMinimumWidth(130)
        self.full_backup_btn.clicked.connect(lambda: self.full_backup_requested.emit(self.watch))
        status_layout.addWidget(self.full_backup_btn)

        paused_now = self.watch.get("paused", False)
        self.pause_btn = QPushButton("⏸ Pause" if not paused_now else "▶ Resume")
        self.pause_btn.setObjectName("secondary")
        self.pause_btn.setMinimumWidth(130)
        self.pause_btn.clicked.connect(self._toggle_pause)
        status_layout.addWidget(self.pause_btn)

        self.validate_btn = QPushButton("▶ Validate")
        self.validate_btn.setObjectName("secondary")
        self.validate_btn.setMinimumWidth(130)
        self.validate_btn.clicked.connect(lambda: self.validate_requested.emit(self.watch))
        status_layout.addWidget(self.validate_btn)

        self.restore_btn = QPushButton("↩ Restore")
        self.restore_btn.setObjectName("secondary")
        self.restore_btn.setMinimumWidth(130)
        self.restore_btn.clicked.connect(lambda: self.restore_requested.emit(self.watch))
        status_layout.addWidget(self.restore_btn)

        self.cancel_btn = QPushButton("▶ Cancel")
        self.cancel_btn.setObjectName("danger")
        self.cancel_btn.setMinimumWidth(130)
        self.cancel_btn.setVisible(False)
        self.cancel_btn.clicked.connect(lambda: self.cancel_requested.emit(self.watch["id"]))
        status_layout.addWidget(self.cancel_btn)

        self.open_backup_btn = QPushButton("Open Backup")
        self.open_backup_btn.setObjectName("secondary")
        self.open_backup_btn.setMinimumWidth(130)
        self.open_backup_btn.clicked.connect(lambda: self.open_backup_requested.emit(self.watch["id"]))
        status_layout.addWidget(self.open_backup_btn)

        layout.addWidget(self.status_widget)
        self._root_layout.addWidget(top)

        # ── Changes panel (hidden by default) ─────────────────────────────
        self.changes_panel = QFrame()
        self.changes_panel.setStyleSheet(
            "background:#141720; border-top:1px solid #2e3340;"
            "border-bottom-left-radius:10px; border-bottom-right-radius:10px;"
        )
        self.changes_panel.setVisible(False)
        cp_layout = QVBoxLayout(self.changes_panel)
        cp_layout.setContentsMargins(20, 10, 20, 10)
        cp_layout.setSpacing(4)

        changes_title = QLabel("RECENT CHANGES")
        changes_title.setStyleSheet("color:#374151; font-size:10px; font-weight:700; letter-spacing:0.08em;")
        cp_layout.addWidget(changes_title)

        self.changes_list = QTextEdit()
        self.changes_list.setReadOnly(True)
        self.changes_list.setMaximumHeight(120)
        self.changes_list.setStyleSheet(
            "background:#141720; border:none; color:#9ca3af;"
            "font-family:'Consolas',monospace; font-size:11px;"
        )
        cp_layout.addWidget(self.changes_list)

        self._root_layout.addWidget(self.changes_panel)

    def add_change(self, entry: dict):
        """Add a detected file change to the card."""
        self._changes.append(entry)
        count = len(self._changes)

        # Update badge
        self.change_badge.setText(str(count))
        self.change_badge.setVisible(True)
        self.toggle_btn.setVisible(True)
        self.toggle_btn.setText(f"{'▴' if self._expanded else '▾'}  {count} change(s)")

        # Update changes list
        icon_map = {"modified": "✏", "added": "➕", "deleted": "➖", "renamed": "↗"}
        lines = []
        for e in reversed(self._changes[-30:]):  # show last 30
            ts = e.get("timestamp", "")
            try:
                ts = datetime.fromisoformat(ts).strftime("%H:%M:%S")
            except Exception:
                pass
            icon    = icon_map.get(e.get("type", ""), "·")
            path    = e.get("path", "")
            user    = e.get("editor_user", "")
            machine = e.get("editor_machine", "")
            ip      = e.get("editor_ip", "")
            who     = ""
            if user:
                who = f"  👤 {user}"
            if machine and machine not in (user or ""):
                who += f"  💻 {machine}"
            if ip and ip not in ("127.0.0.1", ""):
                who += f"  🌐 {ip}"
            lines.append(f"{ts}  {icon}  {path}{who}")
        self.changes_list.setPlainText("\n".join(lines))

        # Update status label
        self.status_lbl.setText(f"⚠  {count} change(s)")
        self.status_lbl.setObjectName("status_warn")
        self.status_lbl.style().unpolish(self.status_lbl)
        self.status_lbl.style().polish(self.status_lbl)

    def clear_changes(self):
        """Clear changes after a successful backup."""
        self._changes.clear()
        self.change_badge.setVisible(False)
        self.toggle_btn.setVisible(False)
        self.changes_panel.setVisible(False)
        self._expanded = False
        self.changes_list.clear()
        self.status_lbl.setText("▶  Watching")
        self.status_lbl.setObjectName("status_ok")
        self.status_lbl.style().unpolish(self.status_lbl)
        self.status_lbl.style().polish(self.status_lbl)

    def _toggle_changes(self):
        self._expanded = not self._expanded
        self.changes_panel.setVisible(self._expanded)
        count = len(self._changes)
        self.toggle_btn.setText(f"{'▴' if self._expanded else '▾'}  {count} change(s)")

    def set_backing_up(self, active: bool):
        self.backup_btn.setEnabled(not active)
        self.backup_btn.setText("Backing up…" if active else "Backup Now")
        self.full_backup_btn.setEnabled(not active)
        self.validate_btn.setEnabled(not active)
        self.restore_btn.setEnabled(not active)
        self.cancel_btn.setVisible(active)
        self.progress_bar.setVisible(active)
        if active:
            self.status_lbl.setText("▶ Backing up…")
            self.status_lbl.setObjectName("status_warn")
        else:
            self.status_lbl.setText("▶ Watching")
            self.status_lbl.setObjectName("status_ok")
        self.status_lbl.style().unpolish(self.status_lbl)
        self.status_lbl.style().polish(self.status_lbl)

    def set_progress(self, copied: int, total: int):
        if total > 0:
            self.progress_bar.setValue(int(copied / total * 100))

    def set_done(self, success: bool):
        self.set_backing_up(False)
        if success:
            self.status_lbl.setText("▶  Done")
            self.status_lbl.setObjectName("status_ok")
            self.clear_changes()
        else:
            self.status_lbl.setText("▶  Failed")
            self.status_lbl.setObjectName("status_err")
        self.status_lbl.style().unpolish(self.status_lbl)
        self.status_lbl.style().polish(self.status_lbl)

    def _toggle_pause(self):
        """Toggle paused state and emit signal for the main window to persist."""
        paused = not self.watch.get("paused", False)
        self.watch["paused"] = paused
        self.pause_btn.setText("▶ Resume" if paused else "⏸ Pause")
        self.pause_requested.emit(self.watch["id"], paused)

    def update_watch(self, watch: dict):
        self.watch = watch
        # Refresh last-backup / count meta label
        lb = watch.get("last_backup", "")
        lb_text = "Never backed up"
        if lb:
            try:
                dt = datetime.fromisoformat(lb)
                lb_text = f"Last backup: {dt.strftime('%b %d, %Y %H:%M')}"
            except Exception:
                lb_text = f"Last backup: {lb}"
        count = watch.get("backup_count", 0)
        size  = watch.get("last_backup_size", 0)
        size_h = ""
        if size and BACKEND_AVAILABLE:
            try:
                size_h = f"   ·   {backup_engine._human_size(size)}"
            except Exception:
                pass
        if hasattr(self, "meta_lbl"):
            self.meta_lbl.setText(f"{lb_text}   ·   {count} backup(s){size_h}")

        # Refresh pause button label
        paused = watch.get("paused", False)
        if hasattr(self, "pause_btn"):
            self.pause_btn.setText("▶ Resume" if paused else "⏸ Pause")

        # Rebuild the entire card UI if name/path/color changed significantly
        # (cheaply update the known labels instead of rebuilding)
        try:
            # name label is the first bold label in the top row  · walk the layout
            top_widget = self._root_layout.itemAt(0).widget()
            top_layout = top_widget.layout()
            # slot 0: color strip, slot 1: info_layout, slot 2: status_widget
            info_item = top_layout.itemAt(1)
            if info_item:
                info_layout = info_item.layout()
                # Row 0: name_row layout >first widget is name_lbl
                name_row_item = info_layout.itemAt(0)
                if name_row_item:
                    name_row = name_row_item.layout()
                    if name_row and name_row.count() > 0:
                        name_lbl_item = name_row.itemAt(0)
                        if name_lbl_item and name_lbl_item.widget():
                            name_lbl_item.widget().setText(watch.get("name", "Unknown"))
                # Row 1: path label
                path_item = info_layout.itemAt(1)
                if path_item and path_item.widget():
                    path_item.widget().setText(watch.get("path", ""))
            # Color strip is slot 0
            strip_item = top_layout.itemAt(0)
            if strip_item and strip_item.widget():
                color = watch.get("color", "") or "#2563eb"
                strip_item.widget().setStyleSheet(
                    f"background-color: {color}; border-radius: 2px;"
                )
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# ── Main Window ────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(APP_NAME)
        self.setMinimumSize(780, 620)
        self.resize(860, 680)

        self._workers: dict          = {}   # watch_id >BackupWorker
        self._cards: dict            = {}   # watch_id >WatchCard
        self._watcher_mgr            = WatcherManager() if BACKEND_AVAILABLE else None
        self._change_counts: dict    = {}
        self._pending_entries: dict  = {}   # watch_id >[entries]
        self._last_notif_time: dict  = {}   # watch_id >timestamp
        self._history_log: list      = []   # all change entries across all watches
        self._history_save_counter   = 0    # throttle disk saves
        self._history_window         = None

        # Load persisted history from previous sessions
        if BACKEND_AVAILABLE:
            try:
                self._history_log = config_manager.load_history()
            except Exception:
                pass

        self._load_config()
        self._build_ui()
        self._start_watchers()
        self._start_auto_timer()
        # Resume any backups that were queued but not completed in a previous session
        QTimer.singleShot(3000, self._process_startup_queue)
        # Check cloud tokens 5 seconds after startup
        QTimer.singleShot(5000, self._validate_cloud_tokens)

    # ── Config ─────────────────────────────────────────────────────────────────

    def _load_config(self):
        if BACKEND_AVAILABLE:
            self.cfg = config_manager.load()
        else:
            self.cfg = {"watches": [], "destination": "", "auto_backup": False,
                        "interval_min": 30, "retention_days": 30}

    # ── UI Build ───────────────────────────────────────────────────────────────

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setSpacing(0)
        root.setContentsMargins(0, 0, 0, 0)

        # ── Top bar ──────────────────────────────────────────────────────────
        topbar = QFrame()
        topbar.setObjectName("topbar")
        topbar.setFixedHeight(60)
        tl = QHBoxLayout(topbar)
        tl.setContentsMargins(20, 0, 20, 0)

        logo = QLabel(f"⬡  {APP_NAME}")
        logo.setStyleSheet("font-size:16px; font-weight:800; color:#f1f3f9; letter-spacing:-0.3px;")
        tl.addWidget(logo)
        tl.addStretch()

        self.status_dot = QLabel("● Active")
        self.status_dot.setObjectName("status_ok")
        tl.addWidget(self.status_dot)

        tl.addSpacing(16)

        history_btn = QPushButton("📋  History")
        history_btn.setObjectName("secondary")
        history_btn.clicked.connect(self._open_history)
        tl.addWidget(history_btn)

        tl.addSpacing(8)

        admin_btn = QPushButton("🔧 Admin")
        admin_btn.setObjectName("secondary")
        admin_btn.clicked.connect(self._open_admin)
        tl.addWidget(admin_btn)

        root.addWidget(topbar)

        # ── Body ─────────────────────────────────────────────────────────────
        body = QWidget()
        bl = QHBoxLayout(body)
        bl.setContentsMargins(0, 0, 0, 0)
        bl.setSpacing(0)

        # Sidebar
        sidebar = QFrame()
        sidebar.setFixedWidth(220)
        sidebar.setStyleSheet("background:#141720; border-right:1px solid #2e3340;")
        sl = QVBoxLayout(sidebar)
        sl.setContentsMargins(16, 24, 16, 16)
        sl.setSpacing(8)

        sl.addWidget(self._sidebar_label("OVERVIEW"))

        self._stat_cards = {}
        for key, icon, label in [
            ("watches",  "📁", "Watched Folders"),
            ("backups",  "🗄", "Total Backups"),
            ("changes",  "🔄", "Pending Changes"),
            ("next",     "⏰", "Next Backup"),
            ("disk",     "💾", "Backup Storage"),
        ]:
            card = self._make_stat_card(icon, label, "0")
            self._stat_cards[key] = card
            sl.addWidget(card)

        sl.addSpacing(16)
        sl.addWidget(self._sidebar_label("QUICK ACTIONS"))

        backup_all_btn = QPushButton("⚡  Backup All Now")
        backup_all_btn.clicked.connect(self._backup_all)
        sl.addWidget(backup_all_btn)

        sl.addStretch()

        version_lbl = QLabel(f"v{APP_VERSION}")
        version_lbl.setStyleSheet("color:#374151; font-size:10px;")
        sl.addWidget(version_lbl)

        bl.addWidget(sidebar)

        # Main content
        content = QWidget()
        cl = QVBoxLayout(content)
        cl.setContentsMargins(24, 24, 24, 24)
        cl.setSpacing(16)

        # Header row
        header_row = QHBoxLayout()
        watches_lbl = QLabel("Watched Folders")
        watches_lbl.setObjectName("heading")
        header_row.addWidget(watches_lbl)
        header_row.addStretch()
        cl.addLayout(header_row)

        # Auto backup status bar
        self.auto_bar = QFrame()
        self.auto_bar.setObjectName("card")
        abl = QHBoxLayout(self.auto_bar)
        abl.setContentsMargins(16, 10, 16, 10)
        self.auto_lbl = QLabel()
        self._update_auto_label()
        abl.addWidget(self.auto_lbl)
        abl.addStretch()
        cl.addWidget(self.auto_bar)

        # Watches scroll area
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)

        self.watches_container = QWidget()
        self.watches_layout = QVBoxLayout(self.watches_container)
        self.watches_layout.setSpacing(10)
        self.watches_layout.setContentsMargins(0, 0, 0, 0)
        self.watches_layout.addStretch()

        scroll.setWidget(self.watches_container)
        cl.addWidget(scroll, stretch=1)

        # Log area
        log_frame = QFrame()
        log_frame.setObjectName("card")
        ll = QVBoxLayout(log_frame)
        ll.setContentsMargins(12, 8, 12, 8)
        ll.setSpacing(6)
        log_title = QLabel("Activity Log")
        log_title.setStyleSheet("color:#6b7280; font-size:11px; font-weight:700; text-transform:uppercase;")
        ll.addWidget(log_title)
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setMaximumHeight(100)
        ll.addWidget(self.log_text)
        cl.addWidget(log_frame)

        bl.addWidget(content, stretch=1)
        root.addWidget(body, stretch=1)

        self._refresh_watches()
        self._update_stats()

    def _sidebar_label(self, text: str) -> QLabel:
        lbl = QLabel(text)
        lbl.setStyleSheet("color:#374151; font-size:10px; font-weight:700; letter-spacing:0.08em;")
        return lbl

    def _make_stat_card(self, icon: str, label: str, value: str) -> QFrame:
        card = QFrame()
        card.setObjectName("card")
        card.setFixedHeight(64)
        layout = QHBoxLayout(card)
        layout.setContentsMargins(12, 8, 12, 8)
        icon_lbl = QLabel(icon)
        icon_lbl.setStyleSheet("font-size:18px;")
        layout.addWidget(icon_lbl)
        text_layout = QVBoxLayout()
        text_layout.setSpacing(1)
        val_lbl = QLabel(value)
        val_lbl.setStyleSheet("font-size:16px; font-weight:700; color:#f1f3f9;")
        val_lbl.setObjectName("stat_value")
        lbl_lbl = QLabel(label)
        lbl_lbl.setStyleSheet("font-size:10px; color:#6b7280;")
        text_layout.addWidget(val_lbl)
        text_layout.addWidget(lbl_lbl)
        layout.addLayout(text_layout)
        card._value_label = val_lbl
        return card

    def _update_auto_label(self):
        auto = self.cfg.get("auto_backup", False)
        interval = self.cfg.get("interval_min", 30)
        if auto:
            self.auto_lbl.setText(
                f"<span style='color:#22c55e; font-weight:700;'>▶ Auto Backup ON</span>"
                f"  ·  Every <b>{interval} min</b>"
                f"  ·  Destination: <code style='color:#9ca3af;'>{self.cfg.get('destination','Unknown')}</code>"
            )
        else:
            self.auto_lbl.setText(
                "<span style='color:#6b7280; font-weight:700;'>▶ Auto Backup OFF</span>"
                "  ·  Manual backups only"
            )

    def _refresh_watches(self):
        # Clear existing cards
        for wid, card in self._cards.items():
            self.watches_layout.removeWidget(card)
            card.deleteLater()
        self._cards.clear()

        watches = self.cfg.get("watches", [])
        if not watches:
            placeholder = QLabel("No folders are being watched.\nClick Admin > Watches > Add Watch to get started.")
            placeholder.setAlignment(Qt.AlignCenter)
            placeholder.setStyleSheet("color:#374151; font-size:13px; padding:40px;")
            self.watches_layout.insertWidget(0, placeholder)
        else:
            for w in watches:
                card = WatchCard(w)
                card.backup_requested.connect(self._backup_single)
                card.full_backup_requested.connect(self._force_full_backup)
                card.validate_requested.connect(self._validate_watch)
                card.restore_requested.connect(self._restore_watch)
                card.pause_requested.connect(self._on_pause_requested)
                card.cancel_requested.connect(self._on_cancel_requested)
                card.open_backup_requested.connect(self._on_open_backup_folder)
                self._cards[w["id"]] = card
                self.watches_layout.insertWidget(self.watches_layout.count() - 1, card)

    def _update_stats(self):
        watches = self.cfg.get("watches", [])
        total_changes = sum(self._change_counts.get(w["id"], 0) for w in watches)

        self._stat_cards["watches"]._value_label.setText(str(len(watches)))
        self._stat_cards["changes"]._value_label.setText(str(total_changes))

        if BACKEND_AVAILABLE:
            dest = self.cfg.get("destination", "")
            try:
                all_backups = backup_engine.list_backups(dest)
                self._stat_cards["backups"]._value_label.setText(str(len(all_backups)))
            except Exception:
                pass

            # Disk usage across all backups
            try:
                from pathlib import Path as _P
                dest_path = _P(dest) if dest else None
                if dest_path and dest_path.exists():
                    total_bytes = sum(
                        backup_engine._safe_size(b.get("backup_dir", ""))
                        for b in backup_engine.list_backups(dest)
                        if b.get("backup_dir")
                    )
                    self._stat_cards["disk"]._value_label.setText(
                        backup_engine._human_size(total_bytes)
                    )
            except Exception:
                self._stat_cards["disk"]._value_label.setText("Error")

        auto = self.cfg.get("auto_backup", False)
        interval = self.cfg.get("interval_min", 30)
        if auto and hasattr(self, "_last_auto_time"):
            pass
        self._stat_cards["next"]._value_label.setText(
            f"{interval}m" if auto else "Manual"
        )

    # ── Startup Queue ──────────────────────────────────────────────────────────

    def _process_startup_queue(self):
        """
        On launch, check if any backups were queued but not completed in the
        previous session (e.g. app was killed mid-backup) and re-run them.
        """
        if not BACKEND_AVAILABLE:
            return
        queue = config_manager.load_backup_queue()
        if not queue:
            return
        config_manager.clear_backup_queue()
        logger.info(f"[startup] Resuming {len(queue)} queued backup(s) from previous session")
        for item in queue:
            wid   = item.get("watch_id")
            watch = next((w for w in self.cfg.get("watches", []) if w["id"] == wid), None)
            if watch:
                self._append_log(f"⏳ Resuming queued backup: {watch.get('name', wid)}")
                self._backup_single(watch, triggered_by="queue")

    # ── Watchers ───────────────────────────────────────────────────────────────

    def _start_watchers(self):
        if not BACKEND_AVAILABLE or not self._watcher_mgr:
            return
        for w in self.cfg.get("watches", []):
            if w.get("active", True) and not w.get("paused", False):
                self._watcher_mgr.start(
                    w["id"], w["path"],
                    on_change=self._on_file_change,
                    exclude_patterns=w.get("exclude_patterns", []),
                    interval_min=w.get("interval_min", 0) or self.cfg.get("interval_min", 30)
                )

    def _on_file_change(self, watch_id: str, entry: dict):
        """Called from watcher thread when a file changes."""
        self._change_counts[watch_id] = self._change_counts.get(watch_id, 0) + 1

        # Attach who/where info
        editor = _get_editor_info(entry.get("path", ""))
        entry["editor_user"]    = editor["user"]
        entry["editor_machine"] = editor["machine"]
        entry["editor_ip"]      = editor["ip"]

        # Attach watch name for history display
        entry["watch_name"] = self._watch_name_for(watch_id)

        # Store in global history log (capped to avoid unbounded memory growth)
        self._history_log.append(entry)
        if len(self._history_log) > 5000:
            self._history_log = self._history_log[-2500:]

        # Persist history to disk every 25 new entries
        self._history_save_counter += 1
        if BACKEND_AVAILABLE and self._history_save_counter % 25 == 0:
            try:
                config_manager.save_history(self._history_log)
            except Exception:
                pass

        # Store entry per watch for the card
        if watch_id not in self._pending_entries:
            self._pending_entries[watch_id] = []
        self._pending_entries[watch_id].append(entry)
        # Schedule UI update on main thread
        QTimer.singleShot(300, lambda: self._apply_file_change(watch_id, entry))

    def _apply_file_change(self, watch_id: str, entry: dict):
        """Update card badge + tray toast on main thread."""
        # Update card
        if watch_id in self._cards:
            self._cards[watch_id].add_change(entry)

        # Live-update history window if open
        if self._history_window and self._history_window.isVisible():
            self._history_window.append_entry(entry)

        self._update_stats()

        # Tray toast  · throttle to max 1 per 10s per watch
        now_ts = datetime.now().timestamp()
        last_notif = self._last_notif_time.get(watch_id, 0)
        if now_ts - last_notif > 10 and hasattr(self, "_tray"):
            self._last_notif_time[watch_id] = now_ts
            etype = entry.get("type", "changed")
            path  = entry.get("path", "")
            name  = self._watch_name_for(watch_id)
            icon_map = {"modified": "✏", "added": "➕", "deleted": "➖", "renamed": "↗"}
            icon = icon_map.get(etype, "·")
            user    = entry.get("editor_user", "")
            machine = entry.get("editor_machine", "")
            who     = f" by {user}" if user else (f" on {machine}" if machine else "")
            self._tray.showMessage(
                f"Change detected  · {name}",
                f"{icon}  {etype.capitalize()}: {path}{who}",
                QSystemTrayIcon.Information, 3000
            )

    def _watch_name_for(self, watch_id: str) -> str:
        for w in self.cfg.get("watches", []):
            if w["id"] == watch_id:
                return w.get("name", watch_id)
        return watch_id

    # ── Auto Timer ─────────────────────────────────────────────────────────────

    def _start_auto_timer(self):
        self._auto_timer = QTimer(self)
        self._auto_timer.timeout.connect(self._auto_backup_tick)
        self._auto_timer.start(5_000)  # check every 5s (supports seconds interval)

    def _auto_backup_tick(self):
        if not BACKEND_AVAILABLE:
            return
        cfg = config_manager.load()
        self.cfg = cfg

        # ── Watcher health check: restart any dead observers ───────────────────
        # Runs every tick (every 5s) so network shares that drop and come back
        # are automatically re-watched without requiring an app restart.
        if self._watcher_mgr:
            for w in cfg.get("watches", []):
                if w.get("active", True) and not w.get("paused", False):
                    self._watcher_mgr.check_and_restart_dead(
                        w["id"], w["path"],
                        on_change=self._on_file_change,
                        exclude_patterns=w.get("exclude_patterns", []),
                        interval_min=w.get("interval_min", 0) or cfg.get("interval_min", 30),
                    )

        if not cfg.get("auto_backup", False):
            return

        interval_val  = cfg.get("interval_min", 30)
        interval_unit = cfg.get("interval_unit", "minutes")
        global_secs   = interval_val if interval_unit == "seconds" else interval_val * 60
        now = datetime.now()

        # ── Time-of-day schedule check ────────────────────────────────────────
        # If backup_schedule_times is set, those times ARE the auto backup.
        # The interval is completely ignored  · the backup only fires at the
        # exact scheduled times (e.g. 17:50). This way the backup runs when
        # the PC is idle and doesn't eat resources during working hours.
        # If no scheduled times are configured, the interval runs as normal.
        schedule_times = cfg.get("backup_schedule_times", [])
        _schedule_due  = False
        now_hhmm       = now.strftime("%H:%M")
        now_secs_day   = now.hour * 3600 + now.minute * 60 + now.second

        if schedule_times:
            # Scheduled mode  · check if it's time to fire
            for sched_str in schedule_times:
                try:
                    sh, sm    = int(sched_str[:2]), int(sched_str[3:5])
                    sched_sec = sh * 3600 + sm * 60
                    if abs(now_secs_day - sched_sec) <= 5:
                        if not hasattr(self, "_last_sched_fire"):
                            self._last_sched_fire = {}
                        if self._last_sched_fire.get(sched_str) != now_hhmm:
                            self._last_sched_fire[sched_str] = now_hhmm
                            _schedule_due = True
                            logger.info(f"Scheduled backup triggered at {sched_str}")
                            break
                except Exception:
                    continue

            if not _schedule_due:
                return  # Not the scheduled time yet  · skip interval entirely

        for w in cfg.get("watches", []):
            if not w.get("active", True) or w.get("paused", False):
                continue
            if w.get("skip_auto_backup", False):
                continue
            if w["id"] in self._workers:
                continue  # already running

            if _schedule_due:
                self._backup_single(w, triggered_by="scheduled")
                continue

            watch_interval_min = w.get("interval_min", 0)
            watch_secs = (watch_interval_min * 60) if watch_interval_min else global_secs
            lb = w.get("last_backup")
            if lb:
                try:
                    last       = datetime.fromisoformat(lb)
                    secs_since = (now - last).total_seconds()
                    if secs_since < watch_secs:
                        continue
                except Exception:
                    pass

            # Check if there are pending changes
            if self._watcher_mgr:
                pending = self._watcher_mgr.pending_count(w["id"])
                if pending == 0 and w.get("backup_count", 0) > 0 and not w.get("needs_full_backup", False):
                    continue  # nothing changed, skip

            self._backup_single(w, triggered_by="auto")

    # ── Backup Logic ───────────────────────────────────────────────────────────

    def _backup_single(self, watch: dict, triggered_by="manual"):
        wid = watch["id"]
        if wid in self._workers:
            return  # already running

        worker = BackupWorker(watch, self.cfg, triggered_by=triggered_by)
        worker.progress.connect(lambda c, t, f, _wid=wid: self._on_progress(_wid, c, t))
        worker.finished.connect(lambda r, _wid=wid: self._on_backup_done(_wid, r))
        worker.log_message.connect(self._append_log)
        self._workers[wid] = worker

        # ── Persist to queue so crash recovery works on next startup ────────
        if BACKEND_AVAILABLE:
            try:
                queue = config_manager.load_backup_queue()
                if not any(q.get("watch_id") == wid for q in queue):
                    queue.append({"watch_id": wid, "triggered_by": triggered_by})
                    config_manager.save_backup_queue(queue)
            except Exception:
                pass

        if wid in self._cards:
            self._cards[wid].set_backing_up(True)

        self.status_dot.setText("● Backing up…")
        self.status_dot.setObjectName("status_warn")
        self.status_dot.style().unpolish(self.status_dot)
        self.status_dot.style().polish(self.status_dot)

        worker.start()

    def _force_full_backup(self, watch: dict):
        """Delete the snapshot so the next backup is a full backup, then run it."""
        from PyQt5.QtWidgets import QMessageBox
        wid = watch["id"]
        reply = QMessageBox.question(
            self, "Force Full Backup",
            "This will re-upload ALL files regardless of changes.\n\nContinue?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply != QMessageBox.Yes:
            return
        if BACKEND_AVAILABLE:
            try:
                # Delete snapshots for ALL destinations so a full backup runs
                # to SFTP, GDrive, etc.  · not just the primary dest.
                config_manager.delete_snapshot(wid)   # deletes all variants
                # Mark watch so auto backup ignores "no changes" check
                for w in self.cfg.get("watches", []):
                    if w["id"] == wid:
                        w["needs_full_backup"] = True
                config_manager.save(self.cfg)
            except Exception:
                # Fallback: delete snapshot file directly
                import os
                snap_path = config_manager.SNAPSHOTS_DIR / f"{wid}.json"
                try:
                    os.remove(snap_path)
                except Exception:
                    pass
        self._backup_single(watch, triggered_by="manual")

    def _backup_all(self):
        for w in self.cfg.get("watches", []):
            if w.get("active", True) and not w.get("paused", False) and not w.get("skip_auto_backup", False):
                self._backup_single(w)

    def _on_progress(self, wid: str, copied: int, total: int):
        if wid in self._cards:
            self._cards[wid].set_progress(copied, total)

    def _on_backup_done(self, wid: str, result: dict):
        if wid in self._workers:
            del self._workers[wid]

        # ── Remove from persistent queue now that this backup is done ───────
        if BACKEND_AVAILABLE:
            try:
                queue = config_manager.load_backup_queue()
                queue = [q for q in queue if q.get("watch_id") != wid]
                if queue:
                    config_manager.save_backup_queue(queue)
                else:
                    config_manager.clear_backup_queue()
            except Exception:
                pass

        success = result.get("status") == "success"

        if wid in self._cards:
            self._cards[wid].set_done(success)

        # Reset change count for this watch
        if success:
            self._change_counts[wid] = 0
            self._pending_entries[wid] = []
            if self._watcher_mgr:
                self._watcher_mgr.clear_pending(wid)
            # Clear needs_full_backup flag now that full backup is done
            if BACKEND_AVAILABLE:
                try:
                    for w in self.cfg.get("watches", []):
                        if w["id"] == wid and w.get("needs_full_backup"):
                            w["needs_full_backup"] = False
                    config_manager.save(self.cfg)
                except Exception:
                    pass

        # Save extra stats BEFORE reloading config so they persist correctly
        if BACKEND_AVAILABLE:
            try:
                for w in self.cfg.get("watches", []):
                    if w["id"] == wid:
                        w["last_backup_status"]   = "success" if success else "failed"
                        w["last_backup_duration"] = round(result.get("duration_s", 0), 1)
                        w["last_failed_files"]    = len(result.get("failed_files", []))
                config_manager.save(self.cfg)
            except Exception:
                pass

        # Reload config to get updated last_backup time
        self._load_config()
        for w in self.cfg.get("watches", []):
            if w["id"] == wid and wid in self._cards:
                self._cards[wid].update_watch(w)

        # Enforce per-watch max_backups limit
        if success and BACKEND_AVAILABLE:
            watch = next((w for w in self.cfg.get("watches", []) if w["id"] == wid), None)
            if watch:
                max_b = watch.get("max_backups", 0)
                if max_b > 0:
                    prune = backup_engine.prune_excess_backups(
                        self.cfg.get("destination", ""), wid, max_b
                    )
                    if prune["pruned"] > 0:
                        self._append_log(
                            f"▶  Pruned {prune['pruned']} old backup(s) for {watch.get('name',wid)} "
                            f"(max={max_b}), freed {prune['freed_human']}"
                        )

                # ── Auto retention cleanup (global + per-watch) ────────────
                dest = self.cfg.get("destination", "")
                if dest:
                    # Per-watch retention overrides global if set
                    retention = watch.get("retention_days", 0) or self.cfg.get("retention_days", 0)
                    if retention > 0:
                        cleaned = backup_engine.cleanup_old_backups(dest, retention, wid)
                        if cleaned.get("deleted", 0) > 0:
                            self._append_log(
                                f"🗑 Cleaned {cleaned['deleted']} backup(s) older than "
                                f"{retention}d for {watch.get('name', wid)}, "
                                f"freed {cleaned.get('freed_human', '0 B')}"
                            )

                # ── Remote retention cleanup (SFTP/FTP/SMB/FTPS) ──────────
                dest_type = self.cfg.get("dest_type", "local")
                if dest_type not in ("local",):
                    retention = watch.get("retention_days", 0) or self.cfg.get("retention_days", 0)
                    if retention > 0:
                        try:
                            from transport_utils import cleanup_remote_backups
                            rc = cleanup_remote_backups(self.cfg, retention, wid)
                            if rc.get("deleted", 0) > 0:
                                freed_mb = rc.get("freed_bytes", 0) / (1024 * 1024)
                                freed_str = f"{freed_mb:.1f} MB" if freed_mb >= 1 else f"{rc.get('freed_bytes', 0) // 1024} KB"
                                self._append_log(
                                    f"🗑 Remote: Cleaned {rc['deleted']} backup(s) older than "
                                    f"{retention}d on {dest_type.upper()}, freed ~{freed_str}"
                                )
                            if rc.get("error") and rc.get("deleted", 0) == 0:
                                self._append_log(f"⚠ Remote retention skipped: {rc['error']}")
                        except ImportError:
                            pass
                        except Exception as e:
                            self._append_log(f"⚠ Remote retention error: {e}")

        self._update_stats()
        self._update_auto_label()

        if not self._workers:
            self.status_dot.setText("● Active")
            self.status_dot.setObjectName("status_ok")
            self.status_dot.style().unpolish(self.status_dot)
            self.status_dot.style().polish(self.status_dot)

        # Tray notification
        if hasattr(self, "_tray"):
            msg = (f"✅ Backup complete: {result.get('files_copied',0)} file(s)"
                   if success else f"⚠ Backup failed: {result.get('error','')}")
            self._tray.showMessage(APP_NAME, msg, QSystemTrayIcon.Information, 3000)

    def _append_log(self, text: str):
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_text.append(f"[{ts}]  {text}")

        # Prevent unbounded memory growth: keep only the last 500 lines.
        doc   = self.log_text.document()
        limit = 500
        while doc.blockCount() > limit:
            cursor = self.log_text.textCursor()
            cursor.movePosition(cursor.Start)
            cursor.select(cursor.BlockUnderCursor)
            cursor.removeSelectedText()
            cursor.deleteChar()   # remove the trailing newline left behind

    # ── Validate ───────────────────────────────────────────────────────────────

    def _validate_watch(self, watch: dict):
        if not BACKEND_AVAILABLE:
            return
        dest = self.cfg.get("destination", "")
        backups = backup_engine.list_backups(dest, watch["id"])
        if not backups:
            QMessageBox.warning(self, "Validate",
                f"No backups found for \"{watch['name']}\".\nRun a backup first.")
            return

        latest = backups[0]
        backup_dir = latest.get("backup_dir", "")
        ts = latest.get("timestamp", "")
        try:
            ts = datetime.fromisoformat(ts).strftime("%b %d, %Y %H:%M")
        except Exception:
            pass

        self._append_log(f"Validating backup: {watch['name']} ({ts}) …")

        try:
            result = backup_engine.validate_backup(backup_dir)
        except Exception as e:
            QMessageBox.critical(self, "Validate Error", str(e))
            return

        if result.get("valid") and result.get("manifest_ok"):
            msg = (
                f"▶  Backup is valid\n\n"
                f"Watch:     {watch['name']}\n"
                f"Date:      {ts}\n"
                f"Hash:      {result.get('stored_hash', '')[:16]}…\n"
                f"Files OK:  {result.get('manifest_ok')}"
            )
            QMessageBox.information(self, "Validate  · Passed", msg)
            self._append_log(f"▶ Validate passed: {watch['name']}")
        else:
            missing   = result.get("missing_files", [])
            corrupted = result.get("corrupted_files", [])
            err       = result.get("error", "")
            details   = ""
            if missing:
                details += f"\nMissing files ({len(missing)}):\n  " + "\n  ".join(missing[:5])
            if corrupted:
                details += f"\nCorrupted files ({len(corrupted)}):\n  " + "\n  ".join(corrupted[:5])
            if err:
                details += f"\nError: {err}"
            QMessageBox.critical(self, "Validate  · Failed",
                f"⚠  Backup validation failed\n\nWatch: {watch['name']}\nDate:  {ts}{details}")
            self._append_log(f"⚠ Validate failed: {watch['name']}")

    # ── Restore ────────────────────────────────────────────────────────────────

    def _restore_watch(self, watch: dict):
        if not BACKEND_AVAILABLE:
            return
        dest = self.cfg.get("destination", "")
        backups = backup_engine.list_backups(dest, watch["id"])
        if not backups:
            QMessageBox.warning(self, "Restore",
                f"No backups found for \"{watch['name']}\".\nRun a backup first.")
            return

        # Let user pick which backup to restore
        from PyQt5.QtWidgets import QInputDialog
        items = []
        for b in backups[:100]:  # show latest 100
            ts = b.get("timestamp", "")
            try:
                ts = datetime.fromisoformat(ts).strftime("%b %d, %Y %H:%M")
            except Exception:
                pass
            files = b.get("files_copied", 0)
            size  = b.get("total_size_bytes", 0)
            size_h = f"{size // 1024} KB" if size < 1024*1024 else f"{size // (1024*1024)} MB"
            incremental = "incremental" if b.get("incremental") else "full"
            items.append(f"{ts}   ·  {files} file(s)  {size_h}  [{incremental}]")

        chosen, ok = QInputDialog.getItem(
            self, "Restore Backup",
            f"Select a restore point for \"{watch['name']}\":\n"
            "(Full Chain Restore replays ALL backups up to the chosen point  · recommended for incremental setups)",
            items, 0, False
        )
        if not ok:
            return

        chosen_idx    = items.index(chosen)
        chosen_backup = backups[chosen_idx]
        backup_dir    = chosen_backup.get("backup_dir", "")
        chosen_id     = chosen_backup.get("backup_id", "")

        # Determine if any backup in the chain is incremental
        is_incremental = any(b.get("incremental") for b in backups[:chosen_idx + 1])
        if is_incremental:
            mode_reply = QMessageBox.question(
                self, "Restore Mode",
                "<b>Full Chain Restore (Recommended)</b><br>"
                "Replays every backup from the oldest up to your chosen point.<br>"
                "Gives you the exact folder state at that point in time.<br><br>"
                "<b>Single Snapshot Restore</b><br>"
                "Only restores files changed in the selected backup (delta only).<br>"
                "Use this only if you know what you're doing.<br><br>"
                "Use Full Chain Restore?",
                QMessageBox.Yes | QMessageBox.No | QMessageBox.Cancel
            )
            if mode_reply == QMessageBox.Cancel:
                return
            use_chain = (mode_reply == QMessageBox.Yes)
        else:
            use_chain = False

        # Optionally browse backup contents before restoring
        browse_reply = QMessageBox.question(
            self, "Preview Backup Contents",
            "Would you like to preview the files in this backup snapshot before restoring?",
            QMessageBox.Yes | QMessageBox.No
        )
        if browse_reply == QMessageBox.Yes:
            try:
                contents = backup_engine.browse_backup_contents(backup_dir)
                files_txt = "\n".join(
                    f"  + {f['path']}  ({f['size_human']})" for f in contents.get("files", [])[:30]
                )
                deleted_txt = ""
                if contents.get("deleted"):
                    deleted_txt = "\n\nDeleted markers:\n" + "\n".join(
                        f"  - {p}" for p in contents["deleted"][:10]
                    )
                total = contents.get("total", 0)
                overflow = f"\n  … and {total - 30} more" if total > 30 else ""
                QMessageBox.information(
                    self, f"Backup Contents  · {chosen}",
                    f"Files in this snapshot ({total} total):\n\n{files_txt}{overflow}{deleted_txt}"
                )
            except Exception as e:
                QMessageBox.warning(self, "Preview Error", str(e))

        # Ask for target folder
        target = QFileDialog.getExistingDirectory(
            self, "Select Restore Destination Folder"
        )
        if not target:
            return

        # Confirm
        mode_label = "Full Chain Restore" if use_chain else "Single Snapshot Restore"
        reply = QMessageBox.question(
            self, "Confirm Restore",
            f"Mode:  {mode_label}\n"
            f"Restore to:  {target}\n\n"
            f"Existing files with the same name will be overwritten.\nContinue?",
            QMessageBox.Yes | QMessageBox.Cancel
        )
        if reply != QMessageBox.Yes:
            return

        self._append_log(f"Restoring backup: {watch['name']} >{target} ({mode_label}) …")

        try:
            if use_chain:
                result = backup_engine.restore_full_chain(
                    destination=dest,
                    watch_id=watch["id"],
                    target_path=target,
                    up_to_backup_id=chosen_id,
                    encrypt_key=watch.get("encrypt_key") or None,
                )
                steps = result.get("steps_applied", 0)
            else:
                result = backup_engine.restore_backup(
                    backup_dir, target,
                    encrypt_key=watch.get("encrypt_key") or None,
                )
                steps = None
        except Exception as e:
            QMessageBox.critical(self, "Restore Error", str(e))
            return

        if result.get("ok"):
            extra = f"\nChain steps applied:  {steps}" if steps is not None else ""
            QMessageBox.information(self, "Restore Complete",
                f"▶  Restore complete\n\n"
                f"Files restored:  {result.get('files_restored', 0)}\n"
                f"Files skipped:   {result.get('skipped', 0)}\n"
                f"Destination:     {target}{extra}"
            )
            self._append_log(
                f"▶ Restore complete: {watch['name']}  · "
                f"{result.get('files_restored', 0)} file(s) >{target}"
            )
        else:
            errors = result.get("errors", [])
            err_preview = "\n".join(errors[:5]) if errors else result.get("error", "Unknown error")
            QMessageBox.critical(self, "Restore Failed",
                f"⚠  Restore failed\n\n{err_preview}")
            self._append_log(f"⚠ Restore failed: {watch['name']}")

    # ── Admin ──────────────────────────────────────────────────────────────────

    def _validate_cloud_tokens(self):
        """Validate cloud tokens from MainWindow  · delegates to AdminPanel logic in background."""
        import threading
        def _check():
            try:
                from PyQt5.QtCore import QSettings, QTimer
                s        = QSettings(SETTINGS_ORG, SETTINGS_APP)
                warnings = []
                # Check GDrive
                gd_token = s.value("gdrive_access_token", "")
                if gd_token:
                    try:
                        import urllib.request as _ur
                        req  = _ur.Request(
                            f"https://www.googleapis.com/oauth2/v1/tokeninfo?access_token={gd_token}"
                        )
                        resp = _ur.urlopen(req, timeout=10)
                        info = __import__("json").loads(resp.read())
                        if int(info.get("expires_in", 9999)) < 300:
                            warnings.append("gdrive")
                    except Exception:
                        warnings.append("gdrive")
                if warnings:
                    QTimer.singleShot(0, lambda: self._on_cloud_token_warnings(warnings))
            except Exception:
                pass
        threading.Thread(target=_check, daemon=True).start()

    def _on_cloud_token_warnings(self, warnings: list):
        """Show tray and log warnings for expired tokens."""
        for provider in warnings:
            name = "Google Drive"
            if hasattr(self, "_tray"):
                self._tray.showMessage(
                    f"⚠ {name}  · Reconnect Required",
                    f"Your {name} token has expired.\n"
                    f"Open Settings >Cloud tab >Reconnect.",
                    QSystemTrayIcon.Warning, 8000
                )
            self._append_log(
                f"⚠ {name} token expired  · go to Settings >Cloud tab to reconnect"
            )

    def _open_admin(self):
        if not PasswordDialog.has_password():
            # First time  · prompt to set password
            reply = QMessageBox.question(self, "Set Admin Password",
                "No admin password is set. Would you like to set one now?\n"
                "(If you skip, any user can access admin settings)",
                QMessageBox.Yes | QMessageBox.No)
            if reply == QMessageBox.Yes:
                dlg = PasswordDialog(self, mode="set")
                if dlg.exec_() != QDialog.Accepted:
                    return

        dlg = PasswordDialog(self, mode="verify")
        if dlg.exec_() != QDialog.Accepted:
            return

        panel = AdminPanel(self.cfg, self)
        panel.watches_changed.connect(self._on_watches_changed)
        # Validate cloud tokens when opening admin panel so user sees warning immediately
        self._validate_cloud_tokens()
        panel.exec_()

        self._load_config()
        self._update_auto_label()
        self._update_stats()

    def _on_cancel_requested(self, watch_id: str):
        """Cancel an in-progress backup for the given watch."""
        worker = self._workers.get(watch_id)
        if worker:
            worker.request_stop()
            self._append_log(f"⏹ Cancel requested for: {self._watch_name_for(watch_id)}")
        if watch_id in self._cards:
            self._cards[watch_id].cancel_btn.setEnabled(False)
            self._cards[watch_id].cancel_btn.setText("Cancelling…")

    def _on_open_backup_folder(self, watch_id: str):
        """Open the latest backup directory for a watch in the system file explorer."""
        if not BACKEND_AVAILABLE:
            return
        dest = self.cfg.get("destination", "")
        backups = backup_engine.list_backups(dest, watch_id)
        if not backups:
            QMessageBox.information(self, "No Backups",
                f"No backups found for \"{self._watch_name_for(watch_id)}\".\\nRun a backup first.")
            return
        folder = backups[0].get("backup_dir", "")
        if not folder or not Path(folder).exists():
            QMessageBox.warning(self, "Folder Missing", f"Backup folder not found:\\n{folder}")
            return
        try:
            import subprocess as _sp
            if os.name == "nt":
                _sp.Popen(["explorer", folder])
            elif sys.platform == "darwin":
                _sp.Popen(["open", folder])
            else:
                _sp.Popen(["xdg-open", folder])
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Could not open folder:\\n{e}")

    def _on_pause_requested(self, watch_id: str, paused: bool):
        """Persist pause/resume state and restart or stop the watcher accordingly."""
        if BACKEND_AVAILABLE:
            config_manager.pause_watch(self.cfg, watch_id, paused)
            self._load_config()
        if self._watcher_mgr:
            if paused:
                self._watcher_mgr.stop(watch_id)
            else:
                watch = next((w for w in self.cfg.get("watches", []) if w["id"] == watch_id), None)
                if watch:
                    self._watcher_mgr.start(
                        watch_id, watch["path"],
                        on_change=self._on_file_change,
                        exclude_patterns=watch.get("exclude_patterns", []),
                        interval_min=watch.get("interval_min", 0) or self.cfg.get("interval_min", 30),
                    )
        self._append_log(f"{'⏸ Paused' if paused else '▶ Resumed'} watch: {self._watch_name_for(watch_id)}")

    def _on_watches_changed(self):
        self._load_config()
        if self._watcher_mgr:
            self._watcher_mgr.stop_all()
            self._watcher_mgr = WatcherManager()
        self._refresh_watches()
        self._start_watchers()
        self._update_stats()

    # ── History ───────────────────────────────────────────────────────────────

    def _open_history(self):
        self._history_window = HistoryWindow(list(self._history_log), self)
        self._history_window.show()
        self._history_window.raise_()

    # ── Window behavior ────────────────────────────────────────────────────────

    def closeEvent(self, event):
        """Minimize to tray instead of closing."""
        event.ignore()
        self.hide()
        if hasattr(self, "_tray"):
            self._tray.showMessage(
                APP_NAME,
                "Running in background. Click the tray icon to reopen.",
                QSystemTrayIcon.Information, 2500
            )

    def set_tray(self, tray):
        self._tray = tray


# ══════════════════════════════════════════════════════════════════════════════
# ── History Window ─────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

class HistoryWindow(QDialog):
    """Full change history table  · all edits across all watches."""

    def __init__(self, history: list, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Change History")
        self.setMinimumSize(900, 580)
        self._all_history = history   # list of dicts
        self._build_ui()
        self._populate(history)

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(0)
        layout.setContentsMargins(0, 0, 0, 0)

        # ── Header ───────────────────────────────────────────────────────────
        header = QFrame()
        header.setObjectName("topbar")
        header.setFixedHeight(56)
        hl = QHBoxLayout(header)
        hl.setContentsMargins(20, 0, 20, 0)
        title = QLabel("📋  Change History")
        title.setStyleSheet("font-size:15px; font-weight:700; color:#f1f3f9;")
        hl.addWidget(title)
        hl.addStretch()

        # Filter bar
        self.filter_input = QLineEdit()
        self.filter_input.setPlaceholderText("Filter by file, user, machine…")
        self.filter_input.setFixedWidth(240)
        self.filter_input.textChanged.connect(self._filter)
        hl.addWidget(self.filter_input)

        self.type_filter = QComboBox()
        self.type_filter.addItems(["All Types", "modified", "added", "deleted", "renamed"])
        self.type_filter.setFixedWidth(120)
        self.type_filter.currentTextChanged.connect(self._filter)
        hl.addWidget(self.type_filter)

        export_btn = QPushButton("Export CSV")
        export_btn.setObjectName("secondary")
        export_btn.clicked.connect(self._export_csv)
        hl.addWidget(export_btn)

        close_btn = QPushButton("✕")
        close_btn.setObjectName("secondary")
        close_btn.setFixedSize(32, 32)
        close_btn.clicked.connect(self.close)
        hl.addWidget(close_btn)
        layout.addWidget(header)

        # ── Stats bar ────────────────────────────────────────────────────────
        stats_bar = QFrame()
        stats_bar.setStyleSheet("background:#141720; border-bottom:1px solid #2e3340;")
        stats_bar.setFixedHeight(38)
        sl = QHBoxLayout(stats_bar)
        sl.setContentsMargins(20, 0, 20, 0)
        sl.setSpacing(24)

        self.stat_total   = QLabel("Total: 0")
        self.stat_mod     = QLabel("Modified: 0")
        self.stat_added   = QLabel("➕ Added: 0")
        self.stat_deleted = QLabel("Deleted: 0")

        for lbl in (self.stat_total, self.stat_mod, self.stat_added, self.stat_deleted):
            lbl.setStyleSheet("color:#6b7280; font-size:11px; font-weight:600;")
            sl.addWidget(lbl)
        sl.addStretch()

        self.result_lbl = QLabel("")
        self.result_lbl.setStyleSheet("color:#374151; font-size:11px;")
        sl.addWidget(self.result_lbl)
        layout.addWidget(stats_bar)

        # ── Table ────────────────────────────────────────────────────────────
        self.table = QTableWidget(0, 7)
        self.table.setHorizontalHeaderLabels([
            "Time", "Watch", "Type", "File / Path", "👤 User", "💻 Machine", "🌐 IP"
        ])
        hh = self.table.horizontalHeader()
        hh.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        hh.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        hh.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        hh.setSectionResizeMode(3, QHeaderView.Stretch)
        hh.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        hh.setSectionResizeMode(5, QHeaderView.ResizeToContents)
        hh.setSectionResizeMode(6, QHeaderView.ResizeToContents)
        self.table.verticalHeader().setVisible(False)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setAlternatingRowColors(True)
        self.table.setStyleSheet(
            self.table.styleSheet() +
            "QTableWidget { alternate-background-color: #1e2128; }"
        )
        layout.addWidget(self.table)

    def _populate(self, entries: list):
        icon_map  = {"modified": "✏", "added": "➕", "deleted": "➖", "renamed": "↗"}
        color_map = {
            "modified": "#f59e0b",
            "added":    "#22c55e",
            "deleted":  "#ef4444",
            "renamed":  "#3b82f6",
        }

        self.table.setRowCount(len(entries))
        for i, e in enumerate(reversed(entries)):
            ts = e.get("timestamp", "")
            try:
                ts = datetime.fromisoformat(ts).strftime("%Y-%m-%d  %H:%M:%S")
            except Exception:
                pass

            etype   = e.get("type", "")
            icon    = icon_map.get(etype, "·")
            color   = color_map.get(etype, "#9ca3af")
            watch   = e.get("watch_name", "")
            path    = e.get("path", "")
            user    = e.get("editor_user", "")
            machine = e.get("editor_machine", "")
            ip      = e.get("editor_ip", "")

            def _item(text, clr=None):
                item = QTableWidgetItem(str(text))
                item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                if clr:
                    item.setForeground(QColor(clr))
                return item

            self.table.setItem(i, 0, _item(ts))
            self.table.setItem(i, 1, _item(watch, "#9ca3af"))
            type_item = _item(f"{icon}  {etype}", color)
            type_item.setFont(QFont("Segoe UI", 11, QFont.Bold))
            self.table.setItem(i, 2, type_item)
            self.table.setItem(i, 3, _item(path))
            self.table.setItem(i, 4, _item(user,    "#60a5fa"))
            self.table.setItem(i, 5, _item(machine, "#a78bfa"))
            self.table.setItem(i, 6, _item(ip,      "#34d399"))

        self.table.resizeRowsToContents()
        self._update_stats(entries)

    def _update_stats(self, entries: list):
        total   = len(entries)
        mod     = sum(1 for e in entries if e.get("type") == "modified")
        added   = sum(1 for e in entries if e.get("type") == "added")
        deleted = sum(1 for e in entries if e.get("type") == "deleted")

        self.stat_total.setText(f"Total: {total}")
        self.stat_mod.setText(f"Modified: {mod}")
        self.stat_added.setText(f"➕ Added: {added}")
        self.stat_deleted.setText(f"Deleted: {deleted}")

    def _filter(self):
        text      = self.filter_input.text().lower()
        type_sel  = self.type_filter.currentText()
        filtered  = []

        for e in self._all_history:
            if type_sel != "All Types" and e.get("type") != type_sel:
                continue
            searchable = " ".join([
                e.get("path", ""),
                e.get("editor_user", ""),
                e.get("editor_machine", ""),
                e.get("editor_ip", ""),
                e.get("watch_name", ""),
            ]).lower()
            if text and text not in searchable:
                continue
            filtered.append(e)

        self._populate(filtered)
        if text or type_sel != "All Types":
            self.result_lbl.setText(f"Showing {len(filtered)} of {len(self._all_history)}")
        else:
            self.result_lbl.setText("")

    def _export_csv(self):
        path, _ = QFileDialog.getSaveFileName(
            self, "Export History", "change_history.csv", "CSV Files (*.csv)"
        )
        if not path:
            return
        try:
            import csv
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["Time", "Watch", "Type", "File/Path", "User", "Machine", "IP"])
                for e in reversed(self._all_history):
                    ts = e.get("timestamp", "")
                    try:
                        ts = datetime.fromisoformat(ts).strftime("%Y-%m-%d %H:%M:%S")
                    except Exception:
                        pass
                    writer.writerow([
                        ts,
                        e.get("watch_name", ""),
                        e.get("type", ""),
                        e.get("path", ""),
                        e.get("editor_user", ""),
                        e.get("editor_machine", ""),
                        e.get("editor_ip", ""),
                    ])
            QMessageBox.information(self, "Exported", f"History exported to:\n{path}")
        except Exception as ex:
            QMessageBox.critical(self, "Error", str(ex))

    def append_entry(self, entry: dict):
        """Live-add a new entry to the top without full reload.

        Previously this called _filter() >_populate() >resizeRowsToContents()
        on *every* incoming file-change event, which caused visible UI freezes
        on busy watches.  Now we insert a single row at position 0.
        """
        self._all_history.append(entry)

        # If a filter is active, check whether this entry passes before inserting
        text     = self.filter_input.text().lower()
        type_sel = self.type_filter.currentText()
        if type_sel != "All Types" and entry.get("type") != type_sel:
            return
        if text:
            searchable = " ".join([
                entry.get("path", ""),
                entry.get("editor_user", ""),
                entry.get("editor_machine", ""),
                entry.get("editor_ip", ""),
                entry.get("watch_name", ""),
            ]).lower()
            if text not in searchable:
                return

        icon_map  = {"modified": "✏", "added": "➕", "deleted": "➖", "renamed": "↗"}
        color_map = {
            "modified": "#f59e0b",
            "added":    "#22c55e",
            "deleted":  "#ef4444",
            "renamed":  "#3b82f6",
        }

        ts = entry.get("timestamp", "")
        try:
            ts = datetime.fromisoformat(ts).strftime("%Y-%m-%d  %H:%M:%S")
        except Exception:
            pass

        etype   = entry.get("type", "")
        icon    = icon_map.get(etype, "·")
        color   = color_map.get(etype, "#9ca3af")

        def _item(text, clr=None):
            item = QTableWidgetItem(str(text))
            item.setFlags(item.flags() & ~Qt.ItemIsEditable)
            if clr:
                item.setForeground(QColor(clr))
            return item

        self.table.insertRow(0)
        self.table.setItem(0, 0, _item(ts))
        self.table.setItem(0, 1, _item(entry.get("watch_name", ""), "#9ca3af"))
        type_item = _item(f"{icon}  {etype}", color)
        type_item.setFont(QFont("Segoe UI", 11, QFont.Bold))
        self.table.setItem(0, 2, type_item)
        self.table.setItem(0, 3, _item(entry.get("path", "")))
        self.table.setItem(0, 4, _item(entry.get("editor_user",    ""), "#60a5fa"))
        self.table.setItem(0, 5, _item(entry.get("editor_machine", ""), "#a78bfa"))
        self.table.setItem(0, 6, _item(entry.get("editor_ip",      ""), "#34d399"))

        # Update counters without rebuilding the whole stats bar
        self._update_stats([e for e in self._all_history
                            if not type_sel or type_sel == "All Types"
                            or e.get("type") == type_sel])


# ══════════════════════════════════════════════════════════════════════════════
# ── System Tray ────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

class TrayApp:
    def __init__(self, app: QApplication):
        self.app    = app
        self.window = MainWindow()

        self.tray = QSystemTrayIcon()
        self.tray.setIcon(make_tray_icon("ok"))
        self.tray.setToolTip(APP_NAME)

        menu = QMenu()

        open_action = QAction("Open Dashboard", menu)
        open_action.triggered.connect(self._show_window)

        backup_action = QAction("⚡  Backup All Now", menu)
        backup_action.triggered.connect(self.window._backup_all)

        admin_action = QAction("🔧 Admin Settings", menu)
        admin_action.triggered.connect(self.window._open_admin)

        history_action = QAction("📋  Change History", menu)
        history_action.triggered.connect(self.window._open_history)

        menu.addAction(open_action)
        menu.addSeparator()
        menu.addAction(backup_action)
        menu.addAction(admin_action)
        menu.addAction(history_action)
        menu.addSeparator()

        quit_action = QAction("Quit", menu)
        quit_action.triggered.connect(self._quit)
        menu.addAction(quit_action)

        self.tray.setContextMenu(menu)
        self.tray.activated.connect(self._on_tray_activated)

        self.window.set_tray(self.tray)
        self.tray.show()

    def _show_window(self):
        self.window.show()
        self.window.raise_()
        self.window.activateWindow()

    def _on_tray_activated(self, reason):
        if reason == QSystemTrayIcon.Trigger:  # single click
            if self.window.isVisible():
                self.window.hide()
            else:
                self._show_window()

    def _quit(self):
        if self.window._watcher_mgr:
            self.window._watcher_mgr.stop_all()
        # Persist history before exit
        if BACKEND_AVAILABLE:
            try:
                config_manager.save_history(self.window._history_log)
            except Exception:
                pass
        self.tray.hide()
        self.app.quit()


# ══════════════════════════════════════════════════════════════════════════════
# ── Entry Point ────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

def _acquire_single_instance_lock():
    """
    Prevent multiple instances of the app running simultaneously.
    Returns a file handle that must be kept open for the lifetime of the process.
    Raises SystemExit if another instance is already running.
    """
    import tempfile
    lock_path = Path(tempfile.gettempdir()) / "backupsys.lock"
    try:
        if os.name == "nt":
            import msvcrt
            fh = open(lock_path, "w")
            try:
                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError:
                fh.close()
                return None   # another instance is running
            return fh
        else:
            import fcntl
            fh = open(lock_path, "w")
            try:
                fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                fh.close()
                return None
            return fh
    except Exception:
        return None   # can't lock >allow app to start


def main():
    # Single-instance guard
    _lock_fh = _acquire_single_instance_lock()
    if _lock_fh is None:
        # Another instance is already running  · show a quick error and exit
        _tmp_app = QApplication.instance() or QApplication(sys.argv)
        QMessageBox.warning(None, APP_NAME,
            "Backup System is already running.\n\nCheck your system tray.")
        sys.exit(0)

    app = QApplication(sys.argv)
    app.setApplicationName(APP_NAME)
    app.setOrganizationName(SETTINGS_ORG)
    app.setQuitOnLastWindowClosed(False)   # keep alive when window is closed

    # Apply stylesheet
    app.setStyleSheet(DARK_STYLE)

    if not QSystemTrayIcon.isSystemTrayAvailable():
        QMessageBox.critical(None, APP_NAME,
            "System tray is not available on this system.")
        sys.exit(1)

    tray_app = TrayApp(app)

    # Show window on first launch if no password set yet
    s = QSettings(SETTINGS_ORG, SETTINGS_APP)
    first_launch = not s.value("launched_before", False)
    if first_launch:
        s.setValue("launched_before", True)
        tray_app._show_window()
    else:
        # Start silently in tray
        tray_app.tray.showMessage(
            APP_NAME,
            "Running in background. Click the tray icon to open.",
            QSystemTrayIcon.Information, 2000
        )

    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
