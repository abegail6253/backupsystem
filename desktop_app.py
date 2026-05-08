"""
Backup System  · Windows Desktop App
PyQt6-based system tray app with dashboard + admin panel.
Place this file in the same folder as backup_engine.py, config_manager.py, watcher.py
"""

import sys
import os
import shutil
import threading
import hashlib
import json
import urllib.request
import socket
import logging
import time as _time_mod
from datetime import datetime, timedelta
from pathlib import Path


def _fmt_eta(seconds: float) -> str:
    """Format a number of seconds into a human-readable ETA string, e.g. '~2m 30s'."""
    if seconds <= 0:
        return ""
    s = int(seconds)
    if s < 60:
        return f"~{s}s"
    m, s = divmod(s, 60)
    if m < 60:
        return f"~{m}m {s:02d}s"
    h, m = divmod(m, 60)
    return f"~{h}h {m:02d}m"


def _fmt_duration(seconds: float) -> str:
    """Format elapsed seconds into a compact duration string, e.g. '2m 30s'."""
    if seconds <= 0:
        return ""
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    m, s = divmod(s, 60)
    if m < 60:
        return f"{m}m {s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h {m:02d}m"

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
        # When running as a compiled exe, __file__ is inside the _internal
        # subfolder.  Also search the folder containing the .exe itself so
        # users can place .env next to BackupSystem.exe without digging into
        # _internal.
        Path(sys.executable).parent / ".env",
        Path(sys.executable).parent / "_env",
        # When bundled with PyInstaller via --add-data, the .env is extracted
        # to the _MEIPASS temp directory at runtime — invisible to customers.
        Path(getattr(sys, "_MEIPASS", "")) / ".env" if getattr(sys, "_MEIPASS", None) else None,
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

# ── Startup validation: GDrive credentials ────────────────────────────────────
# Check immediately after .env is loaded so the user gets a clear log warning
# rather than a cryptic dialog later when they click "Connect to Google Drive".
def _warn_missing_gdrive_env() -> None:
    """Log a clear warning if the GDrive OAuth credentials are absent."""
    missing = [
        k for k in ("GDRIVE_CLIENT_ID", "GDRIVE_CLIENT_SECRET")
        if not os.environ.get(k, "").strip()
    ]
    if missing:
        import logging as _logging
        _logging.getLogger(__name__).warning(
            "GDrive credentials not found in environment: %s.  "
            "Google Drive backups will fail when you click 'Connect'.  "
            "Create a .env file next to this script (see .env.example) and "
            "add your OAuth client ID and secret from Google Cloud Console.",
            ", ".join(missing),
        )

_warn_missing_gdrive_env()

from PyQt6.QtGui import QAction
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QSystemTrayIcon, QMenu,
    QVBoxLayout, QHBoxLayout, QLabel, QPushButton, QFrame, QScrollArea,
    QDialog, QLineEdit, QFormLayout, QDialogButtonBox, QMessageBox,
    QFileDialog, QCheckBox, QSpinBox, QDoubleSpinBox, QTableWidget, QTableWidgetItem,
    QHeaderView, QSizePolicy, QStackedWidget, QProgressBar, QTextEdit,
    QSplitter, QComboBox, QGroupBox, QTabWidget, QToolButton, QStyle,
    QRadioButton, QTimeEdit, QListWidget, QListWidgetItem, QAbstractItemView,
    QPlainTextEdit, QDateEdit
)
from PyQt6.QtCore import (
    Qt, QTimer, QThread, QObject, pyqtSignal, QSize, QSettings, QPoint, QRectF, QTime,
    QDate
)
from PyQt6.QtGui import (
    QIcon, QFont, QColor, QPalette, QPixmap, QPainter, QBrush,
    QLinearGradient, QFontDatabase, QTextCursor
)

# ── Local imports ──────────────────────────────────────────────────────────────
try:
    import config_manager
    import backup_engine
    from watcher import WatcherManager
    from integrity_scheduler import IntegrityScheduler
    import credential_store
    BACKEND_AVAILABLE = True
except ImportError as e:
    BACKEND_AVAILABLE = False
    _IMPORT_ERROR = str(e)

# ── Constants ──────────────────────────────────────────────────────────────────
APP_NAME        = "Backup System"
APP_VERSION     = "1.1.8"
ADMIN_PASS_KEY      = "admin_password_hash"
ADMIN_ATTEMPTS_KEY  = "admin_failed_attempts"
ADMIN_LOCKOUT_KEY   = "admin_lockout_until"   # epoch seconds (float)
SETTINGS_ORG    = "BackupSystem"
SETTINGS_APP    = "BackupSystem"
STARTUP_REG_KEY = r"Software\Microsoft\Windows\CurrentVersion\Run"
TRAY_ICON_SIZE  = 64

# ── Update check — change these two lines if you fork or rename the repo ───────
GITHUB_REPO          = "abegail6253/backupsystem"   # "<owner>/<repo>"
GITHUB_RELEASES_URL  = f"https://github.com/{GITHUB_REPO}/releases"

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
    # sys.stdout is None in --windowed PyInstaller builds, so guard against it.
    if sys.stdout is not None:
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.WARNING)
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
    padding: 8px 16px;
    min-width: 120px;
    border-top-left-radius: 6px;
    border-top-right-radius: 6px;
    font-weight: 600;
}
QTabBar::tab:first {
    margin-left: 8px;
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

LIGHT_STYLE = """
QMainWindow, QDialog, QWidget {
    background-color: #f3f4f6;
    color: #111827;
    font-family: 'Segoe UI', sans-serif;
    font-size: 13px;
}
QFrame#card {
    background-color: #ffffff;
    border: 1px solid #e5e7eb;
    border-radius: 8px;
    padding: 8px;
}
QPushButton {
    background-color: #2563eb;
    color: #ffffff;
    border: none;
    border-radius: 5px;
    padding: 6px 14px;
    font-weight: 600;
}
QPushButton:hover   { background-color: #1d4ed8; }
QPushButton:disabled { background-color: #d1d5db; color: #9ca3af; }
QPushButton[objectName="secondary"] {
    background-color: #e5e7eb;
    color: #374151;
    border: 1px solid #d1d5db;
    font-size: 13px;
}
QPushButton[objectName="secondary"]:hover { background-color: #d1d5db; }
QPushButton[objectName="danger"] {
    background-color: #dc2626;
    color: #ffffff;
}
QPushButton[objectName="danger"]:hover { background-color: #b91c1c; }
QLabel[objectName="status_ok"]   { color: #16a34a; font-weight: 600; }
QLabel[objectName="status_warn"] { color: #d97706; font-weight: 600; }
QLabel[objectName="status_err"]  { color: #dc2626; font-weight: 600; }
QLineEdit, QTextEdit, QPlainTextEdit, QComboBox, QSpinBox {
    background-color: #ffffff;
    border: 1px solid #d1d5db;
    border-radius: 4px;
    padding: 4px 8px;
    color: #111827;
}
QLineEdit:focus, QTextEdit:focus, QPlainTextEdit:focus {
    border: 1px solid #2563eb;
}
QScrollBar:vertical {
    background: #f3f4f6;
    width: 8px;
    border-radius: 4px;
}
QScrollBar::handle:vertical {
    background: #d1d5db;
    border-radius: 4px;
    min-height: 30px;
}
QScrollBar::handle:vertical:hover { background: #9ca3af; }
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0; }
QTabWidget::pane { border: 1px solid #e5e7eb; border-radius: 6px; }
QTabBar::tab {
    background: #e5e7eb;
    color: #374151;
    padding: 6px 16px;
    min-width: 120px;
    border-top-left-radius: 6px;
    border-top-right-radius: 6px;
    margin-right: 2px;
}
QTabBar::tab:first { margin-left: 8px; }
QTabBar::tab:selected { background: #2563eb; color: #ffffff; }
QTabBar::tab:hover    { background: #d1d5db; }
QHeaderView::section {
    background-color: #f9fafb;
    color: #6b7280;
    border: none;
    border-bottom: 1px solid #e5e7eb;
    padding: 4px 8px;
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}
QTreeWidget, QTableWidget, QListWidget {
    background-color: #ffffff;
    alternate-background-color: #f9fafb;
    border: 1px solid #e5e7eb;
    border-radius: 4px;
    gridline-color: #f3f4f6;
}
QTreeWidget::item:selected, QTableWidget::item:selected {
    background-color: #eff6ff;
    color: #1e40af;
}
QMenu {
    background-color: #ffffff;
    border: 1px solid #e5e7eb;
    border-radius: 6px;
    padding: 4px;
}
QMenu::item { padding: 6px 20px; border-radius: 4px; color: #111827; }
QMenu::item:selected { background-color: #eff6ff; color: #1e40af; }
QMenu::separator { background-color: #e5e7eb; height: 1px; margin: 4px 8px; }
"""



def is_metered_connection() -> bool:
    """Detect if the current network connection is metered on Windows.

    Uses the WinRT NetworkInformation API (via PowerShell) to query the real
    metered/cost status of the active internet connection profile.  Returns
    False on non-Windows platforms or when the query fails for any reason.

    NetworkCostType values returned by GetConnectionCost():
      Unrestricted (1) — unlimited connection, never metered
      Fixed (2)        — data-capped plan, treated as metered
      Variable (3)     — pay-per-byte plan, treated as metered
      Unknown (0)      — cost unknown; treated as NOT metered (safe default)

    NOTE: NetworkCategory (Public/Private/Domain) is a *firewall profile*
    setting and is completely unrelated to whether a connection is metered.
    An earlier implementation used NetworkCategory == "Public" as a proxy,
    which caused false positives on any public Wi-Fi that is not actually
    metered.  This implementation uses the correct API.
    """
    if sys.platform != "win32":
        return False
    try:
        import subprocess
        # Load the WinRT Windows.Networking.Connectivity namespace and query
        # the active internet connection profile's cost type.  Exit code 1
        # means metered (Fixed or Variable), exit code 0 means not metered.
        ps_script = (
            "try {"
            "  $nil=[Windows.Networking.Connectivity.NetworkInformation,"
            "        Windows.Networking.Connectivity,"
            "        ContentType=WindowsRuntime];"
            "  $p=[Windows.Networking.Connectivity.NetworkInformation]"
            "       ::GetInternetConnectionProfile();"
            "  if ($p -eq $null) { exit 0 }"
            "  $ct=$p.GetConnectionCost().NetworkCostType;"
            "  if ($ct -eq 'Fixed' -or $ct -eq 'Variable') { exit 1 }"
            "  exit 0"
            "} catch { exit 0 }"
        )
        result = subprocess.run(
            ["powershell", "-NonInteractive", "-NoProfile", "-Command", ps_script],
            capture_output=True,
            timeout=6,
        )
        return result.returncode == 1
    except Exception:
        pass
    return False

# ══════════════════════════════════════════════════════════════════════════════
# ── Tray Icon Generator ────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

def make_tray_icon(status: str = "ok") -> QIcon:
    """Generate a simple colored tray icon."""
    pix = QPixmap(TRAY_ICON_SIZE, TRAY_ICON_SIZE)
    pix.fill(Qt.GlobalColor.transparent)
    painter = QPainter(pix)
    painter.setRenderHint(QPainter.RenderHint.Antialiasing)
    color = {"ok": "#22c55e", "warn": "#f59e0b", "error": "#ef4444", "busy": "#2563eb"}.get(status, "#22c55e")
    painter.setBrush(QBrush(QColor(color)))
    painter.setPen(Qt.PenStyle.NoPen)
    painter.drawRoundedRect(4, 4, TRAY_ICON_SIZE - 8, TRAY_ICON_SIZE - 8, 12, 12)
    painter.setPen(QColor("white"))
    f = QFont("Segoe UI", 26, QFont.Weight.Bold)
    painter.setFont(f)
    painter.drawText(pix.rect(), Qt.AlignmentFlag.AlignCenter, "B")
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
#
# All upload logic lives in transport_utils.py.  Uploads are handled directly
# through backup_engine._upload_to_destination() which calls transport_utils
# functions.  If transport_utils cannot be imported (e.g. a packaging edge case)
# a clear error dict is returned instead of crashing — but in normal use the
# module is always present.
# ──────────────────────────────────────────────────────────────────────────────

from transport_utils import (
    upload_to_sftp  as _tu_sftp,
    upload_to_ftp   as _tu_ftp,
    upload_to_smb   as _tu_smb,
    upload_to_https as _tu_https,
)
_TRANSPORT_UTILS_AVAILABLE = True


def _ensure_smb_mounted(smb_cfg: dict):
    """
    Ensure SMB share is accessible and mounted if needed.
    
    smb_cfg: { path, user, pass, domain }
    Returns: (ok: bool, error_msg: str)
    """
    import subprocess
    import os
    
    path = smb_cfg.get("path", "").strip()
    user = smb_cfg.get("user", "").strip()
    password = smb_cfg.get("pass", "")
    domain = smb_cfg.get("domain", "").strip()
    
    if not path:
        return False, "SMB path not provided"
    
    # Normalize path to UNC format
    path = path.replace("/", "\\")
    if not path.startswith("\\\\"):
        return False, "SMB path must start with \\\\"
    
    # Extract server and share from UNC path
    parts = path.strip("\\").split("\\")
    if len(parts) < 2:
        return False, "Invalid SMB path format"
    server = parts[0]
    share = parts[1]
    
    unc_root = f"\\\\{server}\\{share}"
    
    # Try to authenticate if credentials provided
    if user:
        net_user = f"{domain}\\{user}" if domain else user
        try:
            result = subprocess.run(
                ["net", "use", unc_root, f"/user:{net_user}", password],
                capture_output=True, text=True, timeout=15, check=False
            )
            # net use returns 0 on success, 2 if already connected
            if result.returncode not in (0, 2):
                return False, f"Failed to authenticate with SMB share: {result.stderr.strip()}"
        except subprocess.TimeoutExpired:
            return False, "Timeout authenticating with SMB share"
        except Exception as e:
            return False, f"SMB authentication error: {e}"
    
    # Check if the share is accessible
    try:
        if not os.path.exists(unc_root):
            return False, f"SMB share {unc_root} is not accessible"
    except Exception as e:
        return False, f"Cannot access SMB share: {e}"
    
    return True, ""


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

try:
    from notification_utils import (
        dispatch_telegram as _nu_dispatch_telegram,
        dispatch_pushover as _nu_dispatch_pushover,
        dispatch_ntfy     as _nu_dispatch_ntfy,
    )
    _NOTIFICATION_DISPATCH_AVAILABLE = True
except ImportError:
    _NOTIFICATION_DISPATCH_AVAILABLE = False
    _nu_dispatch_telegram = None
    _nu_dispatch_pushover = None
    _nu_dispatch_ntfy     = None


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

    # Resolve machine hostname once; fall back to a safe placeholder.
    try:
        import socket as _socket
        _machine_id = _socket.gethostname()
    except Exception:
        _machine_id = "unknown"

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
        "machine_id":    _machine_id,
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



class ScheduleTableWidget(QWidget):
    """Compact table widget for day-of-week + time-of-day backup scheduling.

    Each row stores one scheduled time together with a 7-bit day bitmask
    (bit 0 = Monday … bit 6 = Sunday, 127 = every day).  Replaces the old
    plain-text HH:MM comma-separated QLineEdit.
    """

    _DAY_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

    def __init__(self, parent=None):
        super().__init__(parent)
        self._build_ui()

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)

        # Table: Time | Mon | Tue | Wed | Thu | Fri | Sat | Sun | Remove
        self._table = QTableWidget(0, 9)
        self._table.setHorizontalHeaderLabels(
            ["Time (HH:MM)"] + self._DAY_LABELS + [""]
        )
        hh = self._table.horizontalHeader()
        hh.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        for col in range(1, 8):
            hh.setSectionResizeMode(col, QHeaderView.ResizeMode.ResizeToContents)
        hh.setSectionResizeMode(8, QHeaderView.ResizeMode.ResizeToContents)
        self._table.verticalHeader().setVisible(False)
        self._table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._table.setMaximumHeight(160)
        layout.addWidget(self._table)

        add_btn = QPushButton("＋ Add time")
        add_btn.setObjectName("secondary")
        add_btn.setFixedWidth(110)
        add_btn.clicked.connect(lambda: self._add_row())
        layout.addWidget(add_btn)

    def _add_row(self, time_str: str = "", days: int = 127):
        row = self._table.rowCount()
        self._table.insertRow(row)

        # Time cell — editable QLineEdit inside the cell
        time_edit = QLineEdit(time_str)
        time_edit.setPlaceholderText("HH:MM")
        time_edit.setMaxLength(5)
        time_edit.setFixedWidth(70)
        time_edit.setStyleSheet("background: #1e2128; color: #e8eaf0; border: 1px solid #374151; padding: 2px 4px;")
        self._table.setCellWidget(row, 0, time_edit)

        # Day checkboxes
        for col, bit in enumerate(range(7), start=1):
            cb = QCheckBox()
            cb.setChecked(bool(days & (1 << bit)))
            wrapper = QWidget()
            wl = QHBoxLayout(wrapper)
            wl.setContentsMargins(4, 0, 4, 0)
            wl.addWidget(cb)
            self._table.setCellWidget(row, col, wrapper)

        # Remove button
        rm_btn = QPushButton("✕")
        rm_btn.setObjectName("secondary")
        rm_btn.setFixedSize(28, 24)
        rm_btn.clicked.connect(lambda _, r=row: self._remove_row(r))
        self._table.setCellWidget(row, 8, rm_btn)
        self._table.setRowHeight(row, 32)

    def _remove_row(self, row: int):
        # Re-wire remove buttons after deletion
        self._table.removeRow(row)
        for r in range(self._table.rowCount()):
            btn = self._table.cellWidget(r, 8)
            if btn:
                try:
                    btn.clicked.disconnect()
                except Exception:
                    pass
                btn.clicked.connect(lambda _, rr=r: self._remove_row(rr))

    def get_entries(self) -> list:
        """Return list of {"time": "HH:MM", "days": int} dicts."""
        entries = []
        for row in range(self._table.rowCount()):
            te = self._table.cellWidget(row, 0)
            t = te.text().strip() if te else ""
            if len(t) != 5 or t[2] != ":":
                continue
            days = 0
            for col, bit in enumerate(range(7), start=1):
                wrapper = self._table.cellWidget(row, col)
                if wrapper:
                    cb = wrapper.findChild(QCheckBox)
                    if cb and cb.isChecked():
                        days |= (1 << bit)
            entries.append({"time": t, "days": days if days else 127})
        return entries

    def set_entries(self, entries: list):
        """Populate the table from a list of {"time", "days"} dicts or plain "HH:MM" strings."""
        while self._table.rowCount():
            self._table.removeRow(0)
        for e in entries:
            if isinstance(e, str):
                self._add_row(e, 127)
            elif isinstance(e, dict):
                self._add_row(e.get("time", ""), int(e.get("days", 127)))


# ══════════════════════════════════════════════════════════════════════════════
# ── Drive Trigger Monitor ─────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

class DriveTriggerMonitor(QObject):
    """
    Detects when a USB / external drive is connected and emits drive_connected
    with (label, serial, mount_point) so MainWindow can fire triggered backups.

    Strategy (two-layer):
      1. Windows:  WM_DEVICECHANGE message posted to the main window's HWND.
                   nativeEvent() on MainWindow re-emits drive_connected via a
                   signal so it arrives safely on the Qt main thread.
                   DriveTriggerMonitor itself is used only on non-Windows.
      2. Fallback (macOS / Linux / when win32api not available):
                   Poll QStorageInfo every 3 s for newly mounted volumes.

    The monitor is always started; on Windows the HWND approach is preferred
    and the polling loop also runs as a belt-and-suspenders fallback for the
    brief window between plug-in and WM_DEVICECHANGE delivery.

    Emitted signal fields
    ---------------------
    drive_connected(label: str, serial: str, mount_point: str)
        label       — volume label  (may be empty on some filesystems)
        serial      — volume serial as 8-char uppercase hex  (Windows) or
                      "" on macOS/Linux where the concept doesn't exist
        mount_point — root path of the newly mounted volume
    """

    drive_connected = pyqtSignal(str, str, str)   # label, serial, mount_point

    _POLL_INTERVAL_MS = 3_000   # polling cadence in milliseconds

    def __init__(self, parent=None):
        super().__init__(parent)
        self._known_roots: set = set()
        self._running = True
        self._timer = QTimer(self)
        self._timer.setInterval(self._POLL_INTERVAL_MS)
        self._timer.timeout.connect(self._poll)

    # ── Public API ────────────────────────────────────────────────────────────

    def start(self):
        """Seed known roots then start polling timer on main thread."""
        try:
            self._known_roots = set(self._snapshot().keys())
        except Exception:
            self._known_roots = set()
        self._timer.start()

    def stop(self):
        self._running = False
        self._timer.stop()

    def quit(self):
        self.stop()

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _snapshot() -> dict:
        """Return {root_path: (label, serial)} for all currently mounted volumes.
        Uses only thread-safe OS APIs — QStorageInfo must NOT be called from
        background threads as it accesses Qt internals without locking.
        """
        result = {}
        if os.name == "nt":
            import ctypes
            bitmask = ctypes.windll.kernel32.GetLogicalDrives()
            for i in range(26):
                if bitmask & (1 << i):
                    root = f"{chr(65 + i)}:\\"
                    label, serial = "", ""
                    try:
                        vol_name   = ctypes.create_unicode_buffer(261)
                        serial_num = ctypes.c_ulong()
                        ctypes.windll.kernel32.GetVolumeInformationW(
                            root, vol_name, 261,
                            ctypes.byref(serial_num),
                            None, None, None, 0
                        )
                        label  = vol_name.value or ""
                        serial = f"{serial_num.value & 0xFFFFFFFF:08X}"
                    except Exception:
                        pass
                    result[root] = (label, serial)
        else:
            # Linux/macOS: parse /proc/mounts or use os.popen
            try:
                import subprocess
                out = subprocess.check_output(
                    ["mount"], text=True, timeout=3
                )
                for line in out.splitlines():
                    parts = line.split()
                    if len(parts) >= 3:
                        mp = parts[2]
                        result[mp] = ("", "")
            except Exception:
                pass
        return result

    # ── Timer slot (runs on main thread) ─────────────────────────────────────

    def _poll(self):
        """Called by QTimer every _POLL_INTERVAL_MS ms on the main thread."""
        if not self._running:
            return
        try:
            current = self._snapshot()
            new_roots = set(current.keys()) - self._known_roots
            self._known_roots = set(current.keys())
            for root in new_roots:
                label, serial = current[root]
                logger.info(
                    f"[drive-trigger] New volume detected: label={label!r} "
                    f"serial={serial!r} root={root!r}"
                )
                self.drive_connected.emit(label, serial, root)
        except Exception as exc:
            logger.debug(f"[drive-trigger] Poll error: {exc}")


class BackupWorker(QThread):
    # current, total, fname, elapsed_s, is_scanning, bytes_done, total_bytes
    progress    = pyqtSignal(int, int, str, float, bool, int, int)
    finished    = pyqtSignal(dict)               # result dict
    log_message = pyqtSignal(str)                # status text

    def __init__(self, watch: dict, cfg: dict, triggered_by: str = "manual", changed_paths=None):
        super().__init__()   # BUG FIX: was super().__init__(self)  · passing self as own parent
        self.watch         = watch
        self.cfg           = cfg
        self.triggered_by  = triggered_by
        self.changed_paths = changed_paths  # watcher-tracked changed files for fast scan
        self._stop_event   = threading.Event()   # set this to interrupt retry sleep or signal cancel
        self._pause_event  = threading.Event()   # set = running, cleared = paused
        self._pause_event.set()                  # FIX: must start SET (running); cleared only on pause()
        self.pre_backup_cmd  = watch.get("pre_backup_cmd", "")
        self.post_backup_cmd = watch.get("post_backup_cmd", "")
        self.verify_remote_uploads = bool(cfg.get("verify_remote_uploads", False))
        self.verify_after          = bool(cfg.get("verify_after", False))

    def request_stop(self):
        """Signal the worker to abort at the next opportunity (retry sleep or between attempts)."""
        self._stop_event.set()

    def pause(self):
        """Pause the backup. Worker will wait at file boundaries."""
        self._pause_event.clear()

    def resume(self):
        """Resume a paused backup."""
        self._pause_event.set()

    def run(self):
        w   = self.watch
        cfg = self.cfg

        # For network source paths, warn user the scan may take a while
        _src_path = w.get("path", "")
        _is_network_src = _src_path.startswith("\\\\") or _src_path.startswith("//")
        if _is_network_src:
            self.log_message.emit(
                f"Starting backup: {w['name']} … (network source — scanning may take several minutes)"
            )
        else:
            self.log_message.emit(f"Starting backup: {w['name']} …")

        # Determine the primary destination type for snapshot keying.
        # Each destination (sftp, gdrive, local …) maintains its own
        # independent snapshot so that a file already on SFTP but not yet on
        # GDrive is correctly detected as "new" for the GDrive destination.
        dest_type = cfg.get("dest_type", "local")
        snapshot = config_manager.load_snapshot(w["id"], dest_type)

        # ── Scheduled force-full backup ──────────────────────────────────────
        # If force_full_interval_days is set (globally or per-watch) and enough
        # days have elapsed since the last forced full, discard the snapshot so
        # this run becomes a full backup.
        _force_full_interval = int(
            w.get("force_full_interval_days") or
            cfg.get("force_full_interval_days") or 0
        )
        if _force_full_interval > 0 and snapshot:
            import datetime as _dt
            _last_ff_str = w.get("last_force_full_at") or ""
            _do_force_full = False
            if not _last_ff_str:
                # Never done a forced full — force one now
                _do_force_full = True
            else:
                try:
                    _last_ff = _dt.datetime.fromisoformat(_last_ff_str.replace("Z", "+00:00"))
                    _last_ff = _last_ff.replace(tzinfo=None)  # work in naive UTC
                    _elapsed_days = (_dt.datetime.utcnow() - _last_ff).days
                    _do_force_full = _elapsed_days >= _force_full_interval
                except Exception:
                    _do_force_full = True  # unparseable timestamp → force full to be safe
            if _do_force_full:
                self.log_message.emit(
                    f"🔄 {w['name']}: scheduled force-full every {_force_full_interval}d — "
                    f"discarding snapshot to run a full backup."
                )
                snapshot = {}
                # Record the timestamp so the interval resets from today
                for _ww in cfg.get("watches", []):
                    if _ww["id"] == w["id"]:
                        _ww["last_force_full_at"] = _dt.datetime.utcnow().isoformat() + "Z"
                        break
                try:
                    config_manager.save(cfg)
                except Exception:
                    pass  # non-fatal; next run will re-evaluate based on stale timestamp


        # If the snapshot lists files that no longer exist on disk, it is stale
        # (e.g. user emptied and refilled the folder). A stale snapshot causes:
        #   (a) misleading "Scanning (1/N)" progress where N is the old count
        #   (b) a huge diff marking thousands of files as "deleted"
        # Fast check: sample up to 20 paths from the snapshot; if more than
        # half are missing, the snapshot is stale — discard it so the next
        # backup builds a fresh one without scanning ghost files.
        if snapshot:
            try:
                import pathlib as _pl
                _src_root = _pl.Path(w.get("path", ""))
                _sample_keys = list(snapshot.keys())[:20]
                _missing = sum(
                    1 for _k in _sample_keys
                    if not (_src_root / _k).exists()
                )
                if _sample_keys and _missing > len(_sample_keys) // 2:
                    self.log_message.emit(
                        f"[snapshot] Stale snapshot detected ({_missing}/{len(_sample_keys)} "
                        f"sampled files missing) — resetting for clean scan"
                    )
                    snapshot = {}
            except Exception:
                pass  # guard failure is non-fatal

        # Track when the copy phase begins so we can compute ETA.
        _copy_start: list  = [None]   # list so the inner closure can mutate it
        # Rolling speed window: keep the last N (bytes_done, timestamp) samples
        # so the displayed MB/s and ETA reflect recent throughput rather than
        # average-since-start.  This prevents the ETA from shooting to "2 hours"
        # just because the throttler paused between files.
        _speed_window: list = []   # [(time, bytes_done), ...]
        _SPEED_WINDOW_SEC   = 8    # look back 8 seconds for rate calculation

        def cb(copied, total, fname, bytes_done=0, total_bytes=0):
            # Raise InterruptedError so run_backup()'s inner loop propagates
            # the cancellation immediately rather than waiting until the next
            # retry window.
            if self._stop_event.is_set():
                raise InterruptedError("Backup cancelled by user")
            now = _time_mod.time()
            if _copy_start[0] is None:
                _copy_start[0] = now
            elapsed = now - _copy_start[0]

            # Maintain rolling window — drop samples older than _SPEED_WINDOW_SEC
            _speed_window.append((now, bytes_done))
            cutoff = now - _SPEED_WINDOW_SEC
            while len(_speed_window) > 2 and _speed_window[0][0] < cutoff:
                _speed_window.pop(0)

            self.progress.emit(copied, total, fname, elapsed, False, bytes_done, total_bytes)

        # For scanning ETA: use previous snapshot file count as estimated total.
        # This gives a meaningful ETA on repeat backups. First-time backups
        # will show elapsed time only (no estimate available).
        # Cap at 0 so a stale snapshot (e.g. after user deleted most files)
        # doesn't make the scan appear stuck at "Scanning (1/10000)".
        # We reset the estimate to the actual count once scan finishes.
        _estimated_scan_total = len(snapshot)   # 0 on first backup
        _scan_count: list = [0]
        _scan_start: list = [_time_mod.time()]

        def scan_cb(fname):
            """Called for each file during snapshot scan."""
            if self._stop_event.is_set():
                raise InterruptedError("Backup cancelled by user")
            _scan_count[0] += 1
            # If we've already found more files than the old snapshot had,
            # the snapshot was stale — stop using it as the total estimate.
            _est = _estimated_scan_total if _scan_count[0] <= _estimated_scan_total else 0
            elapsed = _time_mod.time() - _scan_start[0]
            self.progress.emit(_scan_count[0], _est, fname, elapsed, True, 0, 0)

        # Honour the bandwidth throttle setting.
        # Per-watch value (max_backup_mbps > 0) takes precedence over the global cfg value.
        watch_max_mbps   = float(w.get("max_backup_mbps", 0.0))
        watch_schedule   = w.get("bandwidth_schedule", [])
        global_max_mbps  = float(cfg.get("max_backup_mbps", 0.0))
        global_schedule  = cfg.get("bandwidth_schedule", [])
        max_mbps = watch_max_mbps if watch_max_mbps > 0 else global_max_mbps
        schedule = watch_schedule if watch_max_mbps > 0 else global_schedule
        throttler = backup_engine.BackupThrottler(max_mbps, schedule) if max_mbps > 0 else None

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
                smb_unc = smb_cfg.get("path", "").strip()
                # Self-heal: fix stale local destination saved before the SMB _save_general fix
                dest_cur = cfg.get("destination", "")
                if smb_unc and not (dest_cur.startswith("\\") or dest_cur.startswith("//")):
                    cfg["destination"] = smb_unc
                ok, smb_err = _ensure_smb_mounted(smb_cfg)
                if not ok:
                    self.log_message.emit(f"⚠ SMB mount failed: {smb_err}")
                    result = {"status": "failed", "error": f"SMB mount failed: {smb_err}", "watch_id": w["id"]}
                    continue  # try again on next attempt

            try:
                _src = w["path"]
                if os.name == "nt" and (_src.startswith("\\\\") or _src.startswith("//")):
                    _src_smb = dict(w.get("smb_cfg") or {})
                    _src_smb["path"] = _src
                    ok, smb_err = _ensure_smb_mounted(_src_smb)
                    if not ok:
                        self.log_message.emit(f"⚠ {w['name']}: SMB source mount failed: {smb_err}")
                        result = {"status": "failed", "error": f"SMB source mount failed: {smb_err}", "watch_id": w["id"]}
                        continue

                # Resolve destination: per-watch overrides global cfg destination
                _w_dest = w.get("destination", "").strip() or cfg.get("destination", "")
                # Build destinations list for multi-destination support
                _destinations = w.get("destinations", [])
                if not _destinations:
                    # Fallback to global dest_type
                    _dest_type  = cfg.get("dest_type", "local")
                    _cloud_cfg  = None
                    if _dest_type == "sftp":
                        _cloud_cfg = {**cfg.get("dest_sftp", {}), "_dest_type": "sftp"}
                    elif _dest_type in ("ftp", "ftps"):
                        _cloud_cfg = {**cfg.get("dest_ftp", {}), "_dest_type": _dest_type}
                    elif _dest_type == "https":
                        _cloud_cfg = {**cfg.get("dest_https", {}), "_dest_type": "https"}
                    elif _dest_type == "webdav":
                        _cloud_cfg = {**cfg.get("dest_webdav", {}), "_dest_type": "webdav"}
                    elif _dest_type == "rclone":
                        _cloud_cfg = {**cfg.get("dest_rclone", {}), "_dest_type": "rclone"}
                    elif _dest_type == "gdrive":
                        _w_cloud = w.get("cloud_config") or {}
                        if _w_cloud:
                            _cloud_cfg = {**_w_cloud, "_dest_type": "gdrive"}

                    # BUG FIX: Per-watch cloud_config should ALWAYS apply, even when
                    # the global dest_type is "local". Previously, GDrive assignments
                    # saved in the GDrive tab were silently ignored unless the user also
                    # changed the global dest_type to "gdrive".
                    if _cloud_cfg is None:
                        _w_cloud = w.get("cloud_config") or {}
                        if _w_cloud and _w_cloud.get("access_token"):
                            _cloud_cfg = {**_w_cloud, "_dest_type": "gdrive"}

                    _destinations = None  # Use legacy gdrive_config
                else:
                    _dest_type = "local"  # For legacy compatibility
                    _cloud_cfg = None

                # ── Resolve source type and credentials ───────────────────────
                _src_type = w.get("type", "local")
                _src_smb_cfg    = w.get("smb_cfg", {}) if _src_type == "smb" else None
                _src_webdav_cfg = w.get("webdav_cfg", {}) if _src_type == "webdav" else None
                _src_sftp_cfg   = w.get("sftp_cfg", {}) if _src_type in ("sftp", "ftps") else None
                _src_ftp_cfg    = w.get("ftp_cfg", {}) if _src_type == "ftp" else None

                result = backup_engine.run_backup(
                    source            = w["path"],
                    destination       = _w_dest,
                    watch_id          = w["id"],
                    watch_name        = w["name"],
                    storage_type      = _dest_type,
                    previous_snapshot = snapshot or None,
                    incremental       = bool(snapshot),
                    progress_cb       = cb,
                    scan_cb           = scan_cb,
                    exclude_patterns  = w.get("exclude_patterns", []),
                    compress          = w.get("compression", False),
                    encrypt_key       = w.get("encrypt_key") or None,
                    cloud_config      = _cloud_cfg,
                    destinations      = _destinations,
                    triggered_by      = self.triggered_by,
                    throttler         = throttler,
                    cancel_event      = self._stop_event,
                    pause_event       = self._pause_event,
                    sync_mode         = w.get("sync_mode", False),
                    max_file_size_mb  = w.get("max_file_size_mb", 0),
                    pre_backup_cmd    = self.pre_backup_cmd,
                    post_backup_cmd   = self.post_backup_cmd,
                    changed_paths     = self.changed_paths or None,
                    verify_remote_upload = self.verify_remote_uploads,
                    source_type       = _src_type,
                    source_sftp_cfg   = _src_sftp_cfg,
                    source_ftp_cfg    = _src_ftp_cfg,
                    source_smb_cfg    = _src_smb_cfg,
                    source_webdav_cfg = _src_webdav_cfg,
                    verify_after      = self.verify_after,
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
                f" · {_fmt_duration(result.get('duration_s', 0.0))}"
            )

            # ── Email notification on success ──────────────────────────
            # Build effective config with per-watch notification overrides applied.
            _notify_ov = w.get("notify_overrides", {})
            _eff_cfg = dict(cfg)
            if _notify_ov.get("webhook_url"):
                _eff_cfg = {**_eff_cfg, "webhook_url": _notify_ov["webhook_url"]}
            if _notify_ov.get("ntfy_topic"):
                _nc = dict(_eff_cfg.get("ntfy_config", {}))
                _nc["topic"] = _notify_ov["ntfy_topic"]
                _eff_cfg = {**_eff_cfg, "ntfy_config": _nc}

            ec = _eff_cfg.get("email_config", {})
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
                _send_email_notification(_eff_cfg, subject, body)

            # ── Webhook notification ───────────────────────────────────
            _send_webhook(_eff_cfg, result)

            # ── ntfy push notification ─────────────────────────────────
            if _NOTIFICATION_DISPATCH_AVAILABLE:
                try:
                    _nu_dispatch_ntfy(_eff_cfg, {**result, "watch_name": w["name"]})
                except Exception as _ntfy_exc:
                    logger.warning("ntfy dispatch error: %s", _ntfy_exc)
            else:
                logger.debug("dispatch_ntfy unavailable (notification_utils missing or broken)")

            # ── Telegram + Pushover notifications ──────────────────────
            if _NOTIFICATION_DISPATCH_AVAILABLE:
                try:
                    _nu_dispatch_telegram(_eff_cfg, {**result, "watch_name": w["name"]})
                    _nu_dispatch_pushover(_eff_cfg, {**result, "watch_name": w["name"]})
                except Exception as _tg_exc:
                    logger.warning("Telegram/Pushover dispatch error: %s", _tg_exc)
            else:
                logger.debug("dispatch_telegram/dispatch_pushover unavailable (notification_utils missing or broken)")

            # ── Remote upload result (surfaced from backup_engine.run_backup) ──
            # The engine handles all SFTP/FTP/FTPS/SMB/HTTPS/GDrive uploads
            # internally and stores the outcome in result["cloud_upload"].
            dest_type = cfg.get("dest_type", "local")
            # BUG FIX: also show upload result when watch has per-watch cloud_config (e.g. GDrive)
            _has_watch_cloud = bool((w.get("cloud_config") or {}).get("access_token"))
            if dest_type not in ("local",) or _has_watch_cloud:
                upload_res = result.get("cloud_upload") or {}
                if upload_res.get("ok"):
                    # FIX: use the actual provider name, not the global dest_type.
                    # When global dest_type is "local" but per-watch GDrive is set,
                    # dest_type.upper() was incorrectly showing "LOCAL upload done".
                    _provider = (w.get("cloud_config") or {}).get("provider", dest_type).upper()
                    self.log_message.emit(
                        f"☁  {_provider} upload done: "
                        f"{upload_res.get('uploaded', 0)} file(s)"
                    )
                    # ── Post-upload verification warnings ──────────────────────
                    _verify_warns = upload_res.get("warnings") or upload_res.get("verify_warnings", [])
                    if _verify_warns:
                        for _vw in _verify_warns:
                            self.log_message.emit(f"⚠ Remote verify ({_provider}): {_vw}")
                        self.log_message.emit(
                            f"⚠ {_provider}: {len(_verify_warns)} file(s) failed post-upload "
                            f"checksum verification — transfer may be corrupted. "
                            f"Re-running the backup is recommended."
                        )
                elif upload_res:
                    _err_msg = upload_res.get("error", "unknown error")
                    self.log_message.emit(f"⚠ {dest_type.upper()} upload failed: {_err_msg}")
                    _ec = cfg.get("email_config", {})
                    if _ec.get("enabled") and _ec.get("notify_on_failure", True):
                        _send_email_notification(cfg,
                            f"⚠ BackupSys · {dest_type.upper()} upload failed: {w['name']}",
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

        else:
            self.log_message.emit(f"⚠ {w['name']}: {result.get('error', 'unknown error')}")

            # ── Email + webhook on failure ─────────────────────────────
            # Apply per-watch notification overrides (same logic as success block).
            _notify_ov = w.get("notify_overrides", {})
            _eff_cfg = dict(cfg)
            if _notify_ov.get("webhook_url"):
                _eff_cfg = {**_eff_cfg, "webhook_url": _notify_ov["webhook_url"]}
            if _notify_ov.get("ntfy_topic"):
                _nc = dict(_eff_cfg.get("ntfy_config", {}))
                _nc["topic"] = _notify_ov["ntfy_topic"]
                _eff_cfg = {**_eff_cfg, "ntfy_config": _nc}

            ec = _eff_cfg.get("email_config", {})
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
                _send_email_notification(_eff_cfg, subject, body)
            _send_webhook(_eff_cfg, result)
            # ── ntfy push notification ─────────────────────────────────
            if _NOTIFICATION_DISPATCH_AVAILABLE:
                try:
                    _nu_dispatch_ntfy(_eff_cfg, {**result, "watch_name": w["name"]})
                except Exception as _ntfy_exc:
                    logger.warning("ntfy dispatch error: %s", _ntfy_exc)
            else:
                logger.debug("dispatch_ntfy unavailable (notification_utils missing or broken)")
            # ── Telegram + Pushover notifications ──────────────────────
            if _NOTIFICATION_DISPATCH_AVAILABLE:
                try:
                    _nu_dispatch_telegram(_eff_cfg, {**result, "watch_name": w["name"]})
                    _nu_dispatch_pushover(_eff_cfg, {**result, "watch_name": w["name"]})
                except Exception as _tg_exc:
                    logger.warning("Telegram/Pushover dispatch error: %s", _tg_exc)
            else:
                logger.debug("dispatch_telegram/dispatch_pushover unavailable (notification_utils missing or broken)")

        self.finished.emit(result)


# ══════════════════════════════════════════════════════════════════════════════
# ── Restore Worker ─────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

class RestoreWorker(QThread):
    """Runs restore_backup / restore_full_chain off the main thread so the UI stays responsive."""
    progress  = pyqtSignal(int, int, str)   # step, total_steps, label
    finished  = pyqtSignal(dict)            # result dict

    def __init__(self, mode: str, kwargs: dict, parent=None):
        super().__init__(parent)
        self.mode   = mode    # "single" or "chain"
        self.kwargs = kwargs

    def run(self):
        try:
            if self.mode == "chain":
                def _progress_cb(step, total, label):
                    self.progress.emit(step, total, label)
                result = backup_engine.restore_full_chain(
                    progress_cb=_progress_cb, **self.kwargs
                )
            else:
                result = backup_engine.restore_backup(**self.kwargs)
        except Exception as e:
            result = {"ok": False, "error": str(e), "files_restored": 0, "skipped": 0, "errors": [str(e)]}
        self.finished.emit(result)


# ══════════════════════════════════════════════════════════════════════════════
# ── Auto-Shutdown Countdown Dialog ────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

class ShutdownCountdownDialog(QDialog):
    """60-second countdown before auto-shutdown.  Cancel button aborts shutdown."""

    def __init__(self, parent=None, countdown: int = 60):
        super().__init__(parent)
        self._remaining = countdown
        self._cancelled = False
        self.setWindowTitle("Auto-Shutdown")
        self.setModal(True)
        self.setFixedSize(400, 180)
        self.setWindowFlag(Qt.WindowType.WindowStaysOnTopHint)

        layout = QVBoxLayout(self)
        layout.setSpacing(16)
        layout.setContentsMargins(24, 24, 24, 24)

        icon_lbl = QLabel("🖥  All backups completed")
        icon_lbl.setStyleSheet("font-size: 14px; font-weight: 700;")
        layout.addWidget(icon_lbl)

        self._msg_lbl = QLabel()
        self._msg_lbl.setWordWrap(True)
        self._msg_lbl.setStyleSheet("font-size: 12px; color: #d1d5db;")
        layout.addWidget(self._msg_lbl)

        btn_row = QHBoxLayout()
        cancel_btn = QPushButton("Cancel Shutdown")
        cancel_btn.setObjectName("secondary")
        cancel_btn.clicked.connect(self._cancel)
        shutdown_now_btn = QPushButton("Shut Down Now")
        shutdown_now_btn.setObjectName("danger")
        shutdown_now_btn.clicked.connect(self._shutdown_now)
        btn_row.addWidget(cancel_btn)
        btn_row.addWidget(shutdown_now_btn)
        layout.addLayout(btn_row)

        self._update_label()

        self._timer = QTimer(self)
        self._timer.setInterval(1000)
        self._timer.timeout.connect(self._tick)
        self._timer.start()

    def _update_label(self):
        self._msg_lbl.setText(
            f"The computer will shut down in <b>{self._remaining}</b> second(s).\n"
            "Click <b>Cancel Shutdown</b> to abort."
        )

    def _tick(self):
        self._remaining -= 1
        if self._remaining <= 0:
            self._timer.stop()
            self.accept()   # accepted → caller triggers shutdown
        else:
            self._update_label()

    def _cancel(self):
        self._timer.stop()
        self._cancelled = True
        self.reject()

    def _shutdown_now(self):
        self._timer.stop()
        self.accept()

    def was_cancelled(self) -> bool:
        return self._cancelled


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
        self._pending_reset = False  # True when forgot-password flow is in progress

        # ── Load persistent lockout state ─────────────────────────────────────
        import time as _time
        s = QSettings(SETTINGS_ORG, SETTINGS_APP)
        self._attempts = int(s.value(ADMIN_ATTEMPTS_KEY, 0))
        lockout_until  = float(s.value(ADMIN_LOCKOUT_KEY, 0))
        remaining_secs = lockout_until - _time.time()
        if remaining_secs > 0:
            self._locked = True
        else:
            self._locked = False
            if self._attempts >= 5:
                # Lockout expired — clear persisted counter
                self._attempts = 0
                s.setValue(ADMIN_ATTEMPTS_KEY, 0)
                s.setValue(ADMIN_LOCKOUT_KEY, 0)

        self._build_ui()

        # If already locked, start the UI countdown for the remaining time
        if self._locked:
            ms_left = max(1000, int(remaining_secs * 1000))
            self.pw_input.setEnabled(False)
            self.error_lbl.setText(
                f"Too many attempts · locked for {int(remaining_secs)}s"
            )
            QTimer.singleShot(ms_left, self._unlock)

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(16)
        layout.setContentsMargins(24, 24, 24, 24)

        icon_lbl = QLabel("🔒")
        icon_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        icon_lbl.setStyleSheet("font-size: 36px;")
        layout.addWidget(icon_lbl)

        self.title_lbl = QLabel("Admin Access Required" if self.mode == "verify" else "Set Admin Password")
        self.title_lbl.setObjectName("heading")
        self.title_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.title_lbl)

        self.sub_lbl = QLabel("Enter the admin password to continue" if self.mode == "verify"
                             else "Choose a password to protect admin settings")
        self.sub_lbl.setObjectName("subheading")
        self.sub_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.sub_lbl)

        self.pw_input = QLineEdit()
        self.pw_input.setEchoMode(QLineEdit.EchoMode.Password)
        self.pw_input.setPlaceholderText("Password")
        layout.addWidget(self.pw_input)

        self.pw_confirm = QLineEdit()
        self.pw_confirm.setEchoMode(QLineEdit.EchoMode.Password)
        self.pw_confirm.setPlaceholderText("Confirm password")
        self.pw_confirm.setVisible(self.mode != "verify")
        layout.addWidget(self.pw_confirm)

        self.error_lbl = QLabel("")
        self.error_lbl.setObjectName("status_err")
        self.error_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
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
            # If this was triggered by "Forgot password?", the old key is now
            # safely replaced — nothing extra to remove (save overwrites it).
            self._pending_reset = False
            self.accept()
        else:
            if self._verify_password(pw):
                # Successful login — clear the failed attempt counter
                s = QSettings(SETTINGS_ORG, SETTINGS_APP)
                s.setValue(ADMIN_ATTEMPTS_KEY, 0)
                s.setValue(ADMIN_LOCKOUT_KEY, 0)
                self.accept()
            else:
                import time as _time
                self._attempts += 1
                self.pw_input.clear()
                s = QSettings(SETTINGS_ORG, SETTINGS_APP)
                s.setValue(ADMIN_ATTEMPTS_KEY, self._attempts)
                # Lockout: 30-second cooldown after 5 consecutive failures
                if self._attempts >= 5:
                    self._locked = True
                    lockout_until = _time.time() + 30
                    s.setValue(ADMIN_LOCKOUT_KEY, lockout_until)
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
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        # Don't delete the old password yet — only wipe it once the user
        # successfully saves a new one (handled in _submit via _pending_reset).
        self._pending_reset = True

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
        s = QSettings(SETTINGS_ORG, SETTINGS_APP)
        s.setValue(ADMIN_ATTEMPTS_KEY, 0)
        s.setValue(ADMIN_LOCKOUT_KEY, 0)
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
    def __init__(self, parent=None, cfg=None, edit_watch_id=None):
        super().__init__(parent)
        self.cfg = cfg or {}
        # When set, this dialog is editing an existing watch — skip duplicate
        # checks for the watch's own path/name so saves are never blocked.
        self._edit_watch_id = edit_watch_id
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
        self.source_type.addItems([
            "Local / Mapped Drive",
            "Network Share (SMB)",
            "WebDAV / Nextcloud",
            "SFTP",
            "FTPS",
            "FTP (plain)",
        ])
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
        self.smb_pass.setEchoMode(QLineEdit.EchoMode.Password)
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

        # WebDAV source row
        self.webdav_widget = QWidget()
        webdav_layout = QVBoxLayout(self.webdav_widget)
        webdav_layout.setContentsMargins(0, 0, 0, 0)
        webdav_layout.setSpacing(6)

        self.webdav_url = QLineEdit()
        self.webdav_url.setPlaceholderText("https://cloud.example.com/remote.php/dav/files/user/")
        webdav_layout.addWidget(self.webdav_url)

        webdav_cred_row = QHBoxLayout()
        self.webdav_user = QLineEdit()
        self.webdav_user.setPlaceholderText("Username")
        self.webdav_pass = QLineEdit()
        self.webdav_pass.setPlaceholderText("Password / App token")
        self.webdav_pass.setEchoMode(QLineEdit.EchoMode.Password)
        webdav_cred_row.addWidget(self.webdav_user)
        webdav_cred_row.addWidget(self.webdav_pass)
        webdav_layout.addLayout(webdav_cred_row)

        webdav_help = QLabel("Example: https://nextcloud.example.com/remote.php/dav/files/alice/Docs")
        webdav_help.setStyleSheet("color:#6b7280; font-size:10px;")
        webdav_layout.addWidget(webdav_help)

        self.webdav_widget.setVisible(False)
        form.addRow("WebDAV URL:", self.webdav_widget)

        # ── SFTP / FTPS source ────────────────────────────────────────────────
        self.src_sftp_widget = QWidget()
        sftp_src_layout = QFormLayout(self.src_sftp_widget)
        sftp_src_layout.setContentsMargins(0, 0, 0, 0)
        sftp_src_layout.setSpacing(4)
        self.src_sftp_host = QLineEdit(); self.src_sftp_host.setPlaceholderText("hostname or IP")
        self.src_sftp_port = QSpinBox(); self.src_sftp_port.setRange(1, 65535); self.src_sftp_port.setValue(22)
        self.src_sftp_user = QLineEdit(); self.src_sftp_user.setPlaceholderText("username")
        self.src_sftp_pass = QLineEdit(); self.src_sftp_pass.setPlaceholderText("password")
        self.src_sftp_pass.setEchoMode(QLineEdit.EchoMode.Password)
        self.src_sftp_path = QLineEdit(); self.src_sftp_path.setPlaceholderText("/remote/folder/to/watch")
        self.src_sftp_key  = QLineEdit(); self.src_sftp_key.setPlaceholderText("path to private key (optional)")
        sftp_src_layout.addRow("Host:", self.src_sftp_host)
        sftp_src_layout.addRow("Port:", self.src_sftp_port)
        sftp_src_layout.addRow("User:", self.src_sftp_user)
        sftp_src_layout.addRow("Password:", self.src_sftp_pass)
        sftp_src_layout.addRow("Remote Path:", self.src_sftp_path)
        sftp_src_layout.addRow("Key File:", self.src_sftp_key)
        self.src_sftp_widget.setVisible(False)
        form.addRow("SFTP:", self.src_sftp_widget)

        # ── FTP (plain) source ────────────────────────────────────────────────
        self.src_ftp_widget = QWidget()
        ftp_src_layout = QFormLayout(self.src_ftp_widget)
        ftp_src_layout.setContentsMargins(0, 0, 0, 0)
        ftp_src_layout.setSpacing(4)
        self.src_ftp_host = QLineEdit(); self.src_ftp_host.setPlaceholderText("hostname or IP")
        self.src_ftp_port = QSpinBox(); self.src_ftp_port.setRange(1, 65535); self.src_ftp_port.setValue(21)
        self.src_ftp_user = QLineEdit(); self.src_ftp_user.setPlaceholderText("username")
        self.src_ftp_pass = QLineEdit(); self.src_ftp_pass.setPlaceholderText("password")
        self.src_ftp_pass.setEchoMode(QLineEdit.EchoMode.Password)
        self.src_ftp_path = QLineEdit(); self.src_ftp_path.setPlaceholderText("/remote/folder/to/watch")
        _ftp_src_warn = QLabel("⚠ FTP sends credentials in plaintext — prefer FTPS or SFTP.")
        _ftp_src_warn.setWordWrap(True)
        _ftp_src_warn.setStyleSheet("color:#f59e0b; font-size:11px;")
        ftp_src_layout.addRow("Host:", self.src_ftp_host)
        ftp_src_layout.addRow("Port:", self.src_ftp_port)
        ftp_src_layout.addRow("User:", self.src_ftp_user)
        ftp_src_layout.addRow("Password:", self.src_ftp_pass)
        ftp_src_layout.addRow("Remote Path:", self.src_ftp_path)
        ftp_src_layout.addRow("", _ftp_src_warn)
        self.src_ftp_widget.setVisible(False)
        form.addRow("FTP:", self.src_ftp_widget)

        self.interval_spin = QSpinBox()
        self.interval_spin.setRange(0, 1440)
        self.interval_spin.setValue(0)
        self.interval_spin.setSuffix(" min (0 = use global)")
        form.addRow("Interval:", self.interval_spin)

        # Per-watch destination
        dest_widget = QWidget()
        dest_row = QHBoxLayout(dest_widget)
        dest_row.setContentsMargins(0, 0, 0, 0)
        self.dest_input = QLineEdit()
        self.dest_input.setPlaceholderText(
            "Leave blank to use global destination  ·  or enter e.g. \\\\server\\share\\folder"
        )
        dest_browse_btn = QPushButton("Browse")
        dest_browse_btn.setObjectName("secondary")
        dest_browse_btn.setMaximumWidth(80)
        dest_browse_btn.clicked.connect(self._browse_dest)
        dest_row.addWidget(self.dest_input)
        dest_row.addWidget(dest_browse_btn)
        form.addRow("Destination:", dest_widget)

        self.compress_combo = QComboBox()
        self.compress_combo.addItem("Off", 0)
        self.compress_combo.addItem("Fast (level 1)", 1)
        self.compress_combo.addItem("Balanced (level 6)", 6)
        self.compress_combo.addItem("Best (level 9)", 9)
        self.compress_combo.setCurrentIndex(2)  # Default to "Balanced (level 6)"
        form.addRow("Compression:", self.compress_combo)

        # Sync mode is always ON — files are copied directly into the destination.
        # No versioned timestamped subfolders are created.
        self._sync_mode = True

        layout.addLayout(form)

        # ── "More Options…" collapsible section ──────────────────────────────
        self._more_btn = QPushButton("▸  More Options…")
        self._more_btn.setObjectName("secondary")
        self._more_btn.setCheckable(True)
        self._more_btn.setChecked(False)
        self._more_btn.toggled.connect(self._toggle_more_options)
        layout.addWidget(self._more_btn)

        self._more_widget = QWidget()
        self._more_widget.setVisible(False)
        more_form = QFormLayout(self._more_widget)
        more_form.setSpacing(10)
        more_form.setContentsMargins(0, 4, 0, 4)

        # Schedule times
        self.add_schedule_widget = ScheduleTableWidget()
        more_form.addRow("Schedule times:", self.add_schedule_widget)

        # Retention

        # Max backups

        # Max file size
        self.add_max_file_size_spin = QSpinBox()
        self.add_max_file_size_spin.setRange(0, 100000)
        self.add_max_file_size_spin.setValue(0)
        self.add_max_file_size_spin.setSuffix(" MB  (0 = no limit)")
        self.add_max_file_size_spin.setToolTip(
            "Files larger than this are skipped during backup. Set to 0 to back up all files."
        )
        more_form.addRow("Skip files over:", self.add_max_file_size_spin)

        # Exclude patterns
        self.add_excl_edit = QTextEdit()
        self.add_excl_edit.setMaximumHeight(80)
        self.add_excl_edit.setPlaceholderText(
            "One glob per line, e.g.  *.tmp  or  __pycache__"
        )
        more_form.addRow("Exclusions:", self.add_excl_edit)

        # Encryption key
        enc_container = QWidget()
        enc_row = QHBoxLayout(enc_container)
        enc_row.setContentsMargins(0, 0, 0, 0)
        self.add_encrypt_input = QLineEdit()
        self.add_encrypt_input.setEchoMode(QLineEdit.EchoMode.Password)
        self.add_encrypt_input.setPlaceholderText("44-char encryption key  (leave blank to disable)")
        enc_show = QCheckBox("Show")
        enc_show.toggled.connect(
            lambda on: self.add_encrypt_input.setEchoMode(
                QLineEdit.EchoMode.Normal if on else QLineEdit.EchoMode.Password
            )
        )
        enc_gen = QPushButton("Generate")
        enc_gen.setObjectName("secondary")
        enc_gen.setMaximumWidth(80)
        enc_gen.clicked.connect(self._generate_encrypt_key)
        enc_row.addWidget(self.add_encrypt_input)
        enc_row.addWidget(enc_show)
        enc_row.addWidget(enc_gen)
        more_form.addRow("Encrypt key:", enc_container)

        # Pre / post backup commands
        self.add_pre_cmd_input = QLineEdit()
        self.add_pre_cmd_input.setPlaceholderText(
            "Command to run before backup  (e.g. net stop myservice)"
        )
        more_form.addRow("Pre-backup:", self.add_pre_cmd_input)

        self.add_post_cmd_input = QLineEdit()
        self.add_post_cmd_input.setPlaceholderText(
            "Command to run after backup  (e.g. net start myservice)"
        )
        more_form.addRow("Post-backup:", self.add_post_cmd_input)

        layout.addWidget(self._more_widget)

        self.error_lbl = QLabel("")
        self.error_lbl.setObjectName("status_err")
        layout.addWidget(self.error_lbl)

        btn_row = QHBoxLayout()
        cancel = QPushButton("Cancel")
        cancel.setObjectName("secondary")
        cancel.clicked.connect(self.reject)
        self._submit_btn = QPushButton("Add Watch")
        self._submit_btn.setObjectName("success")
        self._submit_btn.clicked.connect(self._submit)
        btn_row.addWidget(cancel)
        btn_row.addWidget(self._submit_btn)
        layout.addLayout(btn_row)

    def _toggle_more_options(self, checked: bool):
        self._more_widget.setVisible(checked)
        self._more_btn.setText(
            "▾  More Options…" if checked else "▸  More Options…"
        )
        self.adjustSize()

    def _generate_encrypt_key(self):
        import secrets, base64
        raw = secrets.token_bytes(33)   # 33 bytes → 44 base64 chars
        key = base64.urlsafe_b64encode(raw).decode()[:44]
        self.add_encrypt_input.setText(key)
        self.add_encrypt_input.setEchoMode(QLineEdit.EchoMode.Normal)

    def _on_source_type_changed(self, idx):
        self.local_widget.setVisible(idx == 0)
        self.smb_widget.setVisible(idx == 1)
        self.webdav_widget.setVisible(idx == 2)
        is_sftp = idx in (3, 4)   # SFTP or FTPS
        is_ftp  = idx == 5        # FTP plain
        self.src_sftp_widget.setVisible(is_sftp)
        self.src_ftp_widget.setVisible(is_ftp)
        # For remote sources the path field is not used — hide the local path row
        is_remote = idx in (3, 4, 5)
        self.local_widget.setVisible(idx == 0 and not is_remote)

    def _browse_dest(self):
        """Browse for a per-watch destination folder."""
        path = QFileDialog.getExistingDirectory(self, "Select Destination Folder")
        if path:
            self.dest_input.setText(path)

    def _browse(self):
        msg = QMessageBox(self)
        msg.setWindowTitle("What to watch?")
        msg.setText("Do you want to watch a folder or a single file?")
        folder_btn = msg.addButton("Folder", QMessageBox.ButtonRole.AcceptRole)
        file_btn   = msg.addButton("File",   QMessageBox.ButtonRole.AcceptRole)
        msg.addButton("Cancel", QMessageBox.ButtonRole.RejectRole)
        msg.exec()
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
        existing_paths = [
            w.get("path", "").strip().lower()
            for w in self.cfg.get("watches", [])
            if w.get("id") != self._edit_watch_id          # skip self when editing
        ]
        if path_str.strip().lower() in existing_paths:
            self.error_lbl.setText("This path is already in your watch list.")
            return False

        # 8. Duplicate name  · already used
        existing_names = [
            w.get("name", "").strip().lower()
            for w in self.cfg.get("watches", [])
            if w.get("id") != self._edit_watch_id          # skip self when editing
        ]
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
            if w.get("id") == self._edit_watch_id:         # skip self when editing
                continue
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
        # Skip recursive size scan for network paths — scanning a large SMB share
        # (e.g. 2 TB) over the network on the main thread causes the UI to freeze.
        _is_network_path = path_str.startswith("//") or path_str.startswith("\\\\")
        _src_size_bytes = 0   # shared with check 15 to avoid a second full scan
        _src_size_complete = False  # True only if scan finished without hitting the timeout
        try:
            import time as _time
            if p.is_dir() and not _is_network_path:
                total = 0
                _scan_deadline = _time.monotonic() + 2.0  # never block main thread > 2 s
                for fp in p.rglob("*"):
                    if _time.monotonic() > _scan_deadline:
                        break
                    if fp.is_file():
                        try:
                            total += fp.stat().st_size
                        except Exception:
                            pass
                    if total > 2 * 1024 ** 3:
                        break
                else:
                    _src_size_complete = True  # loop finished normally — scan is authoritative
                _src_size_bytes = total
                if total > 2 * 1024 ** 3:
                    gb = total / 1024 ** 3
                    warnings.append(
                        f"This folder appears to be larger than 2 GB ({gb:.1f} GB estimated).\n"
                        "First backup may take a long time."
                    )
        except Exception:
            pass

        # 15. Destination disk space check (local destination only)
        # Reuses the size already measured in check 14 — no second rglob scan.
        try:
            dest = self.cfg.get("destination", "")
            if dest and not (dest.startswith("//") or dest.startswith("\\\\")) and Path(dest).exists():
                free = shutil.disk_usage(dest).free
                if p.is_file():
                    src_size = p.stat().st_size
                elif not _is_network_path and _src_size_complete:
                    # _src_size_bytes was fully measured in check 14 — safe to compare
                    src_size = _src_size_bytes
                else:
                    src_size = 0  # skip: network source, or check-14 scan timed out
                if src_size and src_size > free * 0.9:
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
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel
            )
            if reply != QMessageBox.StandardButton.Yes:
                return False

        return True

    def _submit(self):
        try:
            self._do_submit()
        except Exception as e:
            self.error_lbl.setText(f"Unexpected error: {e}")

    def _do_submit(self):
        name = self.name_input.text().strip()

        # Name: required
        if not name:
            self.error_lbl.setText("Name is required.")
            return

        src_idx   = self.source_type.currentIndex()
        is_smb    = src_idx == 1
        is_webdav = src_idx == 2
        is_sftp   = src_idx in (3, 4)
        is_ftp    = src_idx == 5

        if is_smb:
            path_str = self.smb_path_input.text().strip().replace("/", "\\")
            if not path_str:
                self.error_lbl.setText("SMB path is required.")
                return
            if not (path_str.startswith("//") or path_str.startswith("\\\\")):
                self.error_lbl.setText("SMB path must start with // or \\\\ (e.g. //server/share)")
                return
        elif is_webdav:
            path_str = self.webdav_url.text().strip()
            if not path_str:
                self.error_lbl.setText("WebDAV URL is required.")
                return
            if not path_str.startswith(("http://", "https://")):
                self.error_lbl.setText("WebDAV URL must start with http:// or https://")
                return
            if not self.webdav_user.text().strip():
                self.error_lbl.setText("WebDAV username is required.")
                return
        elif is_sftp:
            if not self.src_sftp_host.text().strip():
                self.error_lbl.setText("SFTP host is required.")
                return
            if not self.src_sftp_user.text().strip():
                self.error_lbl.setText("SFTP username is required.")
                return
            if not self.src_sftp_path.text().strip():
                self.error_lbl.setText("SFTP remote path is required.")
                return
            path_str = self.src_sftp_path.text().strip()
        elif is_ftp:
            if not self.src_ftp_host.text().strip():
                self.error_lbl.setText("FTP host is required.")
                return
            if not self.src_ftp_user.text().strip():
                self.error_lbl.setText("FTP username is required.")
                return
            if not self.src_ftp_path.text().strip():
                self.error_lbl.setText("FTP remote path is required.")
                return
            path_str = self.src_ftp_path.text().strip()
        else:
            path_str = self.path_input.text().strip()

        self.error_lbl.setText("")
        if is_smb:
            ok, err = _ensure_smb_mounted({
                "path":   path_str,
                "user":   self.smb_user.text().strip(),
                "pass":   self.smb_pass.text(),
                "domain": self.smb_domain.text().strip(),
            })
            if not ok:
                if self._edit_watch_id:
                    pass   # path unchanged — don't block save on connectivity
                else:
                    self.error_lbl.setText(f"Cannot connect to SMB share: {err}")
                    return

        # Remote sources (SFTP/FTPS/FTP/WebDAV) — skip local filesystem validation
        if is_sftp or is_ftp or is_webdav:
            self.accept()
            return

        if self._validate_and_warn(path_str, name):
            self.accept()

    def get_values(self):
        src_idx   = self.source_type.currentIndex()
        is_smb    = src_idx == 1
        is_webdav = src_idx == 2
        is_sftp   = src_idx in (3, 4)
        is_ftps   = src_idx == 4
        is_ftp    = src_idx == 5

        if is_smb:
            path     = self.smb_path_input.text().strip()
            src_type = "smb"
        elif is_webdav:
            path     = self.webdav_url.text().strip()
            src_type = "webdav"
        elif is_sftp:
            path     = self.src_sftp_path.text().strip()
            src_type = "ftps" if is_ftps else "sftp"
        elif is_ftp:
            path     = self.src_ftp_path.text().strip()
            src_type = "ftp"
        else:
            path     = self.path_input.text().strip()
            src_type = "local"

        excl = [
            ln.strip() for ln in self.add_excl_edit.toPlainText().splitlines()
            if ln.strip()
        ]

        return {
            "name":             self.name_input.text().strip(),
            "path":             path,
            "interval_min":     self.interval_spin.value(),
            "compression":      self.compress_combo.currentData(),
            "sync_mode":        True,
            "destination":      self.dest_input.text().strip(),
            "source_type":      src_type,
            "is_smb":           is_smb,
            "smb_user":         self.smb_user.text().strip() if is_smb else "",
            "smb_pass":         self.smb_pass.text() if is_smb else "",
            "smb_domain":       self.smb_domain.text().strip() if is_smb else "",
            "is_webdav":        is_webdav,
            "webdav_user":      self.webdav_user.text().strip() if is_webdav else "",
            "webdav_pass":      self.webdav_pass.text() if is_webdav else "",
            "is_sftp":          is_sftp,
            "sftp_host":        self.src_sftp_host.text().strip() if is_sftp else "",
            "sftp_port":        self.src_sftp_port.value() if is_sftp else 22,
            "sftp_user":        self.src_sftp_user.text().strip() if is_sftp else "",
            "sftp_pass":        self.src_sftp_pass.text() if is_sftp else "",
            "sftp_path":        self.src_sftp_path.text().strip() if is_sftp else "",
            "sftp_key":         self.src_sftp_key.text().strip() if is_sftp else "",
            "is_ftp":           is_ftp,
            "ftp_host":         self.src_ftp_host.text().strip() if is_ftp else "",
            "ftp_port":         self.src_ftp_port.value() if is_ftp else 21,
            "ftp_user":         self.src_ftp_user.text().strip() if is_ftp else "",
            "ftp_pass":         self.src_ftp_pass.text() if is_ftp else "",
            "ftp_path":         self.src_ftp_path.text().strip() if is_ftp else "",
            # Advanced fields (from "More Options…" expander)
            "schedule_times":   self.add_schedule_widget.get_entries(),
            "max_file_size_mb": self.add_max_file_size_spin.value(),
            "exclude_patterns": excl,
            "encrypt_key":      self.add_encrypt_input.text().strip(),
            "pre_backup_cmd":   self.add_pre_cmd_input.text().strip(),
            "post_backup_cmd":  self.add_post_cmd_input.text().strip(),
        }


# ══════════════════════════════════════════════════════════════════════════════
# ── Destination Entry Dialog (used by multi-destination list in EditWatch) ─────
# ══════════════════════════════════════════════════════════════════════════════

class _DestinationEntryDialog(QDialog):
    """Small dialog to configure one extra backup destination for the multi-dest list."""

    _TYPE_LABELS = [
        ("sftp",   "SFTP"),
        ("ftps",   "FTPS"),
        ("ftp",    "FTP (plain)"),
        ("smb",    "Network Share (SMB)"),
        ("https",  "HTTPS API"),
        ("webdav", "WebDAV / Nextcloud"),
        ("rclone", "rclone"),
    ]

    def __init__(self, parent=None, existing: dict = None):
        super().__init__(parent)
        self.setWindowTitle("Configure Destination")
        self.setMinimumWidth(460)
        self._existing = existing or {}
        self._build_ui()
        if existing:
            self._populate(existing)

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(12)
        layout.setContentsMargins(20, 20, 20, 20)

        form = QFormLayout()
        form.setSpacing(8)

        self._type_combo = QComboBox()
        for _, label in self._TYPE_LABELS:
            self._type_combo.addItem(label)
        self._type_combo.currentIndexChanged.connect(self._on_type_changed)
        form.addRow("Type:", self._type_combo)

        # ── SFTP / FTPS ─────────────────────────────────────────────────────
        self._sftp_widget = QWidget()
        sl = QFormLayout(self._sftp_widget)
        sl.setContentsMargins(0, 0, 0, 0); sl.setSpacing(4)
        self._sftp_host = QLineEdit(); self._sftp_host.setPlaceholderText("hostname or IP")
        self._sftp_port = QSpinBox(); self._sftp_port.setRange(1, 65535); self._sftp_port.setValue(22)
        self._sftp_user = QLineEdit(); self._sftp_user.setPlaceholderText("username")
        self._sftp_pass = QLineEdit(); self._sftp_pass.setPlaceholderText("password")
        self._sftp_pass.setEchoMode(QLineEdit.EchoMode.Password)
        self._sftp_path = QLineEdit(); self._sftp_path.setPlaceholderText("/remote/backup/path")
        sl.addRow("Host:", self._sftp_host); sl.addRow("Port:", self._sftp_port)
        sl.addRow("User:", self._sftp_user); sl.addRow("Password:", self._sftp_pass)
        sl.addRow("Remote Path:", self._sftp_path)
        
        # Test button for SFTP
        self._sftp_test_btn = QPushButton("Test Connection")
        self._sftp_test_btn.clicked.connect(self._test_sftp_connection)
        sl.addRow("", self._sftp_test_btn)
        
        form.addRow("", self._sftp_widget)

        # ── FTP (plain) ──────────────────────────────────────────────────────
        self._ftp_widget = QWidget()
        fl = QFormLayout(self._ftp_widget)
        fl.setContentsMargins(0, 0, 0, 0); fl.setSpacing(4)
        self._ftp_host = QLineEdit(); self._ftp_host.setPlaceholderText("hostname or IP")
        self._ftp_port = QSpinBox(); self._ftp_port.setRange(1, 65535); self._ftp_port.setValue(21)
        self._ftp_user = QLineEdit(); self._ftp_user.setPlaceholderText("username")
        self._ftp_pass = QLineEdit(); self._ftp_pass.setPlaceholderText("password")
        self._ftp_pass.setEchoMode(QLineEdit.EchoMode.Password)
        self._ftp_path = QLineEdit(); self._ftp_path.setPlaceholderText("/remote/path")
        _ftp_warn = QLabel("⚠ FTP sends credentials in plaintext — use FTPS/SFTP when possible.")
        _ftp_warn.setWordWrap(True)
        _ftp_warn.setStyleSheet("color:#f59e0b; font-size:11px;")
        fl.addRow("Host:", self._ftp_host); fl.addRow("Port:", self._ftp_port)
        fl.addRow("User:", self._ftp_user); fl.addRow("Password:", self._ftp_pass)
        fl.addRow("Remote Path:", self._ftp_path)
        
        # Test button for FTP
        self._ftp_test_btn = QPushButton("Test Connection")
        self._ftp_test_btn.clicked.connect(self._test_ftp_connection)
        fl.addRow("", self._ftp_test_btn)
        
        fl.addRow("", _ftp_warn)
        form.addRow("", self._ftp_widget)
        self._ftp_widget.setVisible(False)

        # ── SMB ───────────────────────────────────────────────────────────────
        self._smb_widget = QWidget()
        ml = QFormLayout(self._smb_widget)
        ml.setContentsMargins(0, 0, 0, 0); ml.setSpacing(4)
        self._smb_server = QLineEdit(); self._smb_server.setPlaceholderText("nas or 192.168.1.100")
        self._smb_share  = QLineEdit(); self._smb_share.setPlaceholderText("backups")
        self._smb_user   = QLineEdit(); self._smb_user.setPlaceholderText("username (optional)")
        self._smb_pass   = QLineEdit(); self._smb_pass.setPlaceholderText("password (optional)")
        self._smb_pass.setEchoMode(QLineEdit.EchoMode.Password)
        self._smb_path   = QLineEdit(); self._smb_path.setPlaceholderText("subfolder (optional)")
        ml.addRow("Server:", self._smb_server); ml.addRow("Share:", self._smb_share)
        ml.addRow("User:", self._smb_user);     ml.addRow("Password:", self._smb_pass)
        ml.addRow("Path:", self._smb_path)
        
        # Test button for SMB
        self._smb_test_btn = QPushButton("Test Connection")
        self._smb_test_btn.clicked.connect(self._test_smb_connection)
        ml.addRow("", self._smb_test_btn)
        
        form.addRow("", self._smb_widget)
        self._smb_widget.setVisible(False)

        # ── HTTPS API ─────────────────────────────────────────────────────────
        self._https_widget = QWidget()
        hl2 = QFormLayout(self._https_widget)
        hl2.setContentsMargins(0, 0, 0, 0); hl2.setSpacing(4)
        self._https_url   = QLineEdit(); self._https_url.setPlaceholderText("https://api.example.com/backup")
        self._https_token = QLineEdit(); self._https_token.setPlaceholderText("Bearer token (optional)")
        self._https_token.setEchoMode(QLineEdit.EchoMode.Password)
        self._https_ssl   = QCheckBox("Verify SSL certificate"); self._https_ssl.setChecked(True)
        hl2.addRow("URL:", self._https_url); hl2.addRow("Auth Token:", self._https_token)
        hl2.addRow("", self._https_ssl)
        
        # Test button for HTTPS
        self._https_test_btn = QPushButton("Test Connection")
        self._https_test_btn.clicked.connect(self._test_https_connection)
        hl2.addRow("", self._https_test_btn)
        
        form.addRow("", self._https_widget)
        self._https_widget.setVisible(False)

        # ── WebDAV / Nextcloud ────────────────────────────────────────────────
        self._webdav_widget = QWidget()
        wl2 = QFormLayout(self._webdav_widget)
        wl2.setContentsMargins(0, 0, 0, 0); wl2.setSpacing(4)
        self._wdav_url  = QLineEdit(); self._wdav_url.setPlaceholderText("https://nextcloud.example.com")
        self._wdav_user = QLineEdit(); self._wdav_user.setPlaceholderText("username")
        self._wdav_pass = QLineEdit(); self._wdav_pass.setPlaceholderText("password")
        self._wdav_pass.setEchoMode(QLineEdit.EchoMode.Password)
        self._wdav_path = QLineEdit(); self._wdav_path.setPlaceholderText("/backups")
        self._wdav_root = QLineEdit(); self._wdav_root.setPlaceholderText("/remote.php/dav/files/username/")
        self._wdav_ssl  = QCheckBox("Verify SSL certificate"); self._wdav_ssl.setChecked(True)
        wl2.addRow("URL:", self._wdav_url);       wl2.addRow("User:", self._wdav_user)
        wl2.addRow("Password:", self._wdav_pass); wl2.addRow("Remote Path:", self._wdav_path)
        wl2.addRow("DAV Root:", self._wdav_root); wl2.addRow("", self._wdav_ssl)
        
        # Test button for WebDAV
        self._webdav_test_btn = QPushButton("Test Connection")
        self._webdav_test_btn.clicked.connect(self._test_webdav_connection)
        wl2.addRow("", self._webdav_test_btn)
        
        form.addRow("", self._webdav_widget)
        self._webdav_widget.setVisible(False)

        # ── rclone ────────────────────────────────────────────────────────────
        self._rclone_widget = QWidget()
        rl2 = QFormLayout(self._rclone_widget)
        rl2.setContentsMargins(0, 0, 0, 0); rl2.setSpacing(6)

        # Remote picker (populated by Detect Remotes)
        _rclone_picker_row = QHBoxLayout()
        self._rclone_picker_combo = QComboBox()
        self._rclone_picker_combo.setPlaceholderText("— detect remotes first —")
        self._rclone_picker_combo.setMinimumWidth(140)
        self._rclone_picker_combo.currentTextChanged.connect(self._on_rclone_picker_changed)
        _rclone_detect_btn = QPushButton("🔍 Detect Remotes")
        _rclone_detect_btn.setObjectName("secondary")
        _rclone_detect_btn.setToolTip("Run 'rclone listremotes' to find configured remotes")
        _rclone_detect_btn.clicked.connect(self._detect_rclone_remotes)
        _rclone_config_btn = QPushButton("⚙ rclone config…")
        _rclone_config_btn.setObjectName("secondary")
        _rclone_config_btn.setToolTip("Open a terminal running 'rclone config' to add / edit remotes")
        _rclone_config_btn.clicked.connect(self._launch_rclone_config)
        _rclone_picker_row.addWidget(self._rclone_picker_combo, stretch=1)
        _rclone_picker_row.addWidget(_rclone_detect_btn)
        _rclone_picker_row.addWidget(_rclone_config_btn)
        rl2.addRow("Pick remote:", _rclone_picker_row)

        self._rclone_remote = QLineEdit(); self._rclone_remote.setPlaceholderText("myremote")
        self._rclone_path   = QLineEdit(); self._rclone_path.setPlaceholderText("/backups")
        rl2.addRow("Remote name:", self._rclone_remote)
        rl2.addRow("Remote path:", self._rclone_path)

        _rclone_note = QLabel(
            "Click 'Detect Remotes' to list remotes from your rclone config, or type a name "
            "manually. Use 'rclone config…' to add a new provider (70+ supported)."
        )
        _rclone_note.setWordWrap(True)
        _rclone_note.setStyleSheet("color:#94a3b8; font-size:11px;")
        rl2.addRow("", _rclone_note)

        _rclone_test_btn = QPushButton("Test Connection")
        _rclone_test_btn.setObjectName("secondary")
        _rclone_test_btn.clicked.connect(self._test_rclone_dest)
        rl2.addRow("", _rclone_test_btn)

        form.addRow("", self._rclone_widget)
        self._rclone_widget.setVisible(False)

        layout.addLayout(form)

        self._err_lbl = QLabel("")
        self._err_lbl.setObjectName("status_err")
        layout.addWidget(self._err_lbl)

        btn_row = QHBoxLayout()
        cancel_btn = QPushButton("Cancel")
        cancel_btn.setObjectName("secondary")
        cancel_btn.clicked.connect(self.reject)
        ok_btn = QPushButton("Save")
        ok_btn.setObjectName("success")
        ok_btn.clicked.connect(self._submit)
        btn_row.addWidget(cancel_btn)
        btn_row.addWidget(ok_btn)
        layout.addLayout(btn_row)

    def _on_type_changed(self, idx):
        type_key = self._TYPE_LABELS[idx][0]
        self._sftp_widget.setVisible(type_key in ("sftp", "ftps"))
        self._ftp_widget.setVisible(type_key == "ftp")
        self._smb_widget.setVisible(type_key == "smb")
        self._https_widget.setVisible(type_key == "https")
        self._webdav_widget.setVisible(type_key == "webdav")
        self._rclone_widget.setVisible(type_key == "rclone")
        if type_key == "sftp":
            self._sftp_port.setValue(22)
        elif type_key == "ftps":
            self._sftp_port.setValue(990)
        self.adjustSize()

    def _populate(self, dest: dict):
        """Pre-fill fields when editing an existing destination."""
        type_key = dest.get("dest_type", "sftp")
        for i, (k, _) in enumerate(self._TYPE_LABELS):
            if k == type_key:
                self._type_combo.setCurrentIndex(i)
                break
        cfg = dest.get("config", {})
        if type_key in ("sftp", "ftps"):
            self._sftp_host.setText(cfg.get("host", ""))
            self._sftp_port.setValue(int(cfg.get("port", 22)))
            self._sftp_user.setText(cfg.get("username", ""))
            self._sftp_pass.setText(cfg.get("password", ""))
            self._sftp_path.setText(cfg.get("remote_path", ""))
        elif type_key == "ftp":
            self._ftp_host.setText(cfg.get("host", ""))
            self._ftp_port.setValue(int(cfg.get("port", 21)))
            self._ftp_user.setText(cfg.get("username", ""))
            self._ftp_pass.setText(cfg.get("password", ""))
            self._ftp_path.setText(cfg.get("remote_path", ""))
        elif type_key == "smb":
            self._smb_server.setText(cfg.get("server", ""))
            self._smb_share.setText(cfg.get("share", ""))
            self._smb_user.setText(cfg.get("username", ""))
            self._smb_pass.setText(cfg.get("password", ""))
            self._smb_path.setText(cfg.get("remote_path", ""))
        elif type_key == "https":
            self._https_url.setText(cfg.get("url", ""))
            self._https_token.setText(cfg.get("token", ""))
            self._https_ssl.setChecked(cfg.get("verify_ssl", True))
        elif type_key == "webdav":
            self._wdav_url.setText(cfg.get("url", ""))
            self._wdav_user.setText(cfg.get("username", ""))
            self._wdav_pass.setText(cfg.get("password", ""))
            self._wdav_path.setText(cfg.get("remote_path", ""))
            self._wdav_root.setText(cfg.get("webdav_root", ""))
            self._wdav_ssl.setChecked(cfg.get("verify_ssl", True))
        elif type_key == "rclone":
            self._rclone_remote.setText(cfg.get("remote", ""))
            self._rclone_path.setText(cfg.get("path", ""))

    def _submit(self):
        dest = self.get_dest()
        if not dest:
            self._err_lbl.setText("Please fill in the required fields.")
            return
        self.accept()

    def get_dest(self) -> dict:
        """Return a destination dict {dest_type, config} or {} if validation fails."""
        idx      = self._type_combo.currentIndex()
        type_key = self._TYPE_LABELS[idx][0]

        if type_key in ("sftp", "ftps"):
            host = self._sftp_host.text().strip()
            if not host:
                return {}
            config = {
                "host": host,
                "port": self._sftp_port.value(),
                "username": self._sftp_user.text().strip(),
                "password": self._sftp_pass.text(),
                "remote_path": self._sftp_path.text().strip() or "/backups",
            }
            if type_key == "ftps":
                config["use_tls"] = True

        elif type_key == "ftp":
            host = self._ftp_host.text().strip()
            if not host:
                return {}
            config = {
                "host": host,
                "port": self._ftp_port.value(),
                "username": self._ftp_user.text().strip(),
                "password": self._ftp_pass.text(),
                "remote_path": self._ftp_path.text().strip() or "/backups",
                "use_tls": False,
            }

        elif type_key == "smb":
            server = self._smb_server.text().strip()
            if not server:
                return {}
            config = {
                "server": server,
                "share": self._smb_share.text().strip(),
                "username": self._smb_user.text().strip(),
                "password": self._smb_pass.text(),
                "remote_path": self._smb_path.text().strip(),
            }

        elif type_key == "https":
            url = self._https_url.text().strip()
            if not url:
                return {}
            config = {
                "url": url,
                "token": self._https_token.text(),
                "verify_ssl": self._https_ssl.isChecked(),
            }

        elif type_key == "webdav":
            url = self._wdav_url.text().strip()
            if not url:
                return {}
            config = {
                "url": url,
                "username": self._wdav_user.text().strip(),
                "password": self._wdav_pass.text(),
                "remote_path": self._wdav_path.text().strip() or "/backups",
                "webdav_root": self._wdav_root.text().strip(),
                "verify_ssl": self._wdav_ssl.isChecked(),
            }

        elif type_key == "rclone":
            remote = self._rclone_remote.text().strip()
            if not remote:
                return {}
            config = {
                "remote": remote,
                "path": self._rclone_path.text().strip() or "/backups",
            }

        else:
            return {}

        return {"dest_type": type_key, "config": config}

    def _test_sftp_connection(self):
        cfg = {
            "host":     self._sftp_host.text().strip(),
            "port":     self._sftp_port.value(),
            "user":     self._sftp_user.text().strip(),
            "pass":     self._sftp_pass.text(),
            "path":     self._sftp_path.text().strip(),
        }
        if not cfg["host"]:
            QMessageBox.warning(self, "Missing", "Please enter an SFTP host first.")
            return
        try:
            from transport_utils import test_sftp_connection
            result = test_sftp_connection(cfg)
        except Exception as e:
            QMessageBox.critical(self, "SFTP Test Failed", str(e))
            return
        if result.get("ok"):
            QMessageBox.information(self, "SFTP  ·  Connected ✓",
                f"Successfully connected to:\n{cfg['host']}:{cfg['port']}")
        else:
            QMessageBox.critical(self, "SFTP  ·  Failed",
                f'Could not connect:\n\n{result.get("error", "Unknown error")}')

    def _test_ftp_connection(self):
        cfg = {
            "host": self._ftp_host.text().strip(),
            "port": self._ftp_port.value(),
            "user": self._ftp_user.text().strip(),
            "pass": self._ftp_pass.text(),
            "path": self._ftp_path.text().strip(),
        }
        if not cfg["host"]:
            QMessageBox.warning(self, "Missing", "Please enter an FTP host first.")
            return
        try:
            from transport_utils import test_ftp_connection
            result = test_ftp_connection(cfg)
        except Exception as e:
            QMessageBox.critical(self, "FTP Test Failed", str(e))
            return
        if result.get("ok"):
            QMessageBox.information(self, "FTP  ·  Connected ✓",
                f"Successfully connected to:\n{cfg['host']}:{cfg['port']}")
        else:
            QMessageBox.critical(self, "FTP  ·  Failed",
                f'Could not connect:\n\n{result.get("error", "Unknown error")}')

    def _test_smb_connection(self):
        # Build UNC path from server and share
        server = self._smb_server.text().strip()
        share = self._smb_share.text().strip()
        if not server or not share:
            QMessageBox.warning(self, "Missing", "Please enter both SMB server and share.")
            return
        cfg = {
            "path":   f"\\\\{server}\\{share}",
            "user":   self._smb_user.text().strip(),
            "pass":   self._smb_pass.text(),
            "domain": "",  # Dialog doesn't have domain field
        }
        try:
            from transport_utils import test_smb_connection
            result = test_smb_connection(cfg)
        except Exception as e:
            QMessageBox.critical(self, "SMB Test Failed", str(e))
            return
        if result.get("ok"):
            QMessageBox.information(self, "SMB  ·  Connected ✓",
                f"Successfully connected to:\n{cfg['path']}")
        else:
            QMessageBox.critical(self, "SMB  ·  Failed",
                f'Could not connect:\n\n{result.get("error", "Unknown error")}')

    def _test_https_connection(self):
        cfg = {
            "url":        self._https_url.text().strip(),
            "token":      self._https_token.text().strip(),
            "verify_ssl": self._https_ssl.isChecked(),
        }
        if not cfg["url"]:
            QMessageBox.warning(self, "Missing", "Please enter an endpoint URL first.")
            return
        try:
            from transport_utils import test_https_connection
            result = test_https_connection(cfg)
        except Exception as e:
            QMessageBox.critical(self, "HTTPS Test Failed", str(e))
            return
        if result.get("ok"):
            QMessageBox.information(self, "HTTPS  ·  Connected ✓",
                f"Endpoint reachable:\n{cfg['url']}\n\nHTTP status: {result.get('status_code', 'n/a')}")
        else:
            QMessageBox.critical(self, "HTTPS  ·  Failed",
                f'Could not reach endpoint:\n\n{result.get("error", "Unknown error")}')

    def _test_webdav_connection(self):
        cfg = {
            "url":         self._wdav_url.text().strip(),
            "username":    self._wdav_user.text().strip(),
            "password":    self._wdav_pass.text(),
            "webdav_root": self._wdav_root.text().strip(),
            "verify_ssl":  self._wdav_ssl.isChecked(),
        }
        if not cfg["url"]:
            QMessageBox.warning(self, "Missing", "Please enter the WebDAV URL first.")
            return
        try:
            from transport_utils import test_webdav_connection
            result = test_webdav_connection(cfg)
        except Exception as e:
            QMessageBox.critical(self, "WebDAV Test Failed", str(e))
            return
        if result.get("ok"):
            QMessageBox.information(self, "WebDAV  ·  Connected ✓",
                f"WebDAV server reachable:\n{cfg['url']}\n\n"
                "PROPFIND succeeded — credentials and URL are correct.")
        else:
            QMessageBox.critical(self, "WebDAV  ·  Failed",
                f'Could not connect:\n\n{result.get("error", "Unknown error")}\n\n'
                "Tips:\n"
                "• Nextcloud DAV root: /remote.php/dav/files/<USERNAME>/\n"
                "• ownCloud DAV root: /remote.php/webdav/\n"
                "• Plain WebDAV: leave DAV root empty")

    # ── rclone helpers ─────────────────────────────────────────────────────

    def _detect_rclone_remotes(self):
        """Run 'rclone listremotes' and populate the picker combo."""
        import subprocess
        try:
            proc = subprocess.run(
                ["rclone", "listremotes"],
                capture_output=True, text=True, timeout=10,
            )
        except FileNotFoundError:
            QMessageBox.critical(
                self, "rclone not found",
                "rclone is not installed or not on PATH.\n\n"
                "Download it from https://rclone.org/downloads/ and re-try.",
            )
            return
        except subprocess.TimeoutExpired:
            QMessageBox.warning(self, "Timeout", "rclone listremotes timed out after 10 s.")
            return
        except Exception as exc:
            QMessageBox.critical(self, "Error", str(exc))
            return

        remotes = [r.rstrip(":").strip() for r in proc.stdout.splitlines() if r.strip()]
        if not remotes:
            QMessageBox.information(
                self, "No remotes found",
                "rclone reported no configured remotes.\n\n"
                "Click '⚙ rclone config…' to add one.",
            )
            return

        self._rclone_picker_combo.clear()
        self._rclone_picker_combo.addItems(remotes)
        # Pre-select the currently configured remote if it's in the list
        current = self._rclone_remote.text().strip()
        if current in remotes:
            self._rclone_picker_combo.setCurrentText(current)

    def _on_rclone_picker_changed(self, text: str):
        """Fill the Remote name field when the user picks from the combo."""
        if text:
            self._rclone_remote.setText(text)

    def _launch_rclone_config(self):
        """Open a terminal running 'rclone config' so the user can add providers."""
        import subprocess, sys
        try:
            if sys.platform == "win32":
                subprocess.Popen(
                    ["cmd.exe", "/k", "rclone config"],
                    creationflags=subprocess.CREATE_NEW_CONSOLE,
                )
            elif sys.platform == "darwin":
                subprocess.Popen(
                    ["open", "-a", "Terminal", "--args", "rclone", "config"]
                )
            else:
                for term in ("x-terminal-emulator", "gnome-terminal", "konsole", "xterm"):
                    try:
                        subprocess.Popen([term, "-e", "rclone config"])
                        break
                    except FileNotFoundError:
                        continue
        except Exception as exc:
            QMessageBox.warning(
                self, "Could not open terminal",
                f"Please open a terminal manually and run:\n    rclone config\n\nError: {exc}",
            )

    def _test_rclone_dest(self):
        cfg = {
            "remote": self._rclone_remote.text().strip(),
            "path":   self._rclone_path.text().strip(),
        }
        if not cfg["remote"]:
            QMessageBox.warning(self, "Missing", "Please enter an rclone remote name first.")
            return
        try:
            from transport_utils import test_rclone_connection
            result = test_rclone_connection(cfg)
        except Exception as e:
            QMessageBox.critical(self, "rclone Test Failed", str(e))
            return
        if result.get("ok"):
            QMessageBox.information(self, "rclone  ·  Connected ✓",
                f"{result.get('message', 'rclone can access the remote')}")
        else:
            QMessageBox.critical(self, "rclone  ·  Failed",
                f"Could not connect:\n\n{result.get('message', 'Unknown error')}")

    @staticmethod
    def dest_label(dest: dict) -> str:
        """Human-readable one-line summary of a destination dict."""
        type_key = dest.get("dest_type", "")
        cfg      = dest.get("config", {})
        host     = (cfg.get("host") or cfg.get("server") or cfg.get("url") or "").split("//")[-1]
        path     = cfg.get("remote_path") or cfg.get("path") or ""
        return f"{type_key.upper()} — {host}{path}" if host else type_key.upper()


# ══════════════════════════════════════════════════════════════════════════════
# ── Edit Watch Dialog ──────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

class EditWatchDialog(QDialog):
    """Edit per-watch settings: name, interval, compression, exclusions."""

    def __init__(self, watch: dict, dest_type: str = "local", parent=None):
        super().__init__(parent)
        self.watch     = watch
        self.dest_type = dest_type
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

        self.watch_schedule_widget = ScheduleTableWidget()
        _w_sched = self.watch.get("schedule_times", [])
        self.watch_schedule_widget.set_entries(_w_sched)
        form.addRow("Schedule times:", self.watch_schedule_widget)

        self.force_full_interval_spin = QSpinBox()
        self.force_full_interval_spin.setRange(0, 3650)
        self.force_full_interval_spin.setValue(int(self.watch.get("force_full_interval_days", 0)))
        self.force_full_interval_spin.setSuffix(" days  (0 = use global / disabled)")
        self.force_full_interval_spin.setToolTip(
            "Force a full backup every N days for this watch, regardless of the "
            "incremental chain length.  0 = inherit the global setting (or disabled "
            "if the global setting is also 0).  -1 disables forced-full even when "
            "the global setting is active."
        )
        form.addRow("Force full every:", self.force_full_interval_spin)

        # ── Drive trigger ──────────────────────────────────────────────────────
        self.drive_trigger_label_input = QLineEdit()
        self.drive_trigger_label_input.setText(self.watch.get("drive_trigger_label", ""))
        self.drive_trigger_label_input.setPlaceholderText("MY_BACKUP  (case-insensitive volume label)")
        self.drive_trigger_label_input.setToolTip(
            "Back up this watch automatically when a drive with this volume label "
            "is connected.  Leave blank to disable.  Case-insensitive."
        )
        form.addRow("Drive trigger (label):", self.drive_trigger_label_input)

        self.drive_trigger_serial_input = QLineEdit()
        self.drive_trigger_serial_input.setText(self.watch.get("drive_trigger_serial", ""))
        self.drive_trigger_serial_input.setPlaceholderText("ABCD1234  (8-char hex serial — Windows only)")
        self.drive_trigger_serial_input.setToolTip(
            "Back up this watch automatically when a drive with this volume serial "
            "is connected.  Find the serial with:  vol C:  (or the drive letter) "
            "in CMD.  Either label OR serial match triggers the backup."
        )
        form.addRow("Drive trigger (serial):", self.drive_trigger_serial_input)

        _dt_note = QLabel(
            "💡 Tip: use volume label for portability across machines; use serial "
            "to distinguish two drives with the same label."
        )
        _dt_note.setStyleSheet("color: #94a3b8; font-size: 11px;")
        _dt_note.setWordWrap(True)
        form.addRow("", _dt_note)

        self.max_file_size_spin = QDoubleSpinBox()
        self.max_file_size_spin.setRange(0, 100000)
        self.max_file_size_spin.setDecimals(0)
        self.max_file_size_spin.setValue(self.watch.get("max_file_size_mb", 0))
        self.max_file_size_spin.setSuffix(" MB  (0 = no limit)")
        self.max_file_size_spin.setToolTip(
            "Skip any single file larger than this size. "
            "Useful to avoid accidentally backing up video files or database dumps."
        )
        form.addRow("Skip files over:", self.max_file_size_spin)

        self.max_backup_bytes_spin = QDoubleSpinBox()
        self.max_backup_bytes_spin.setRange(0, 1_000_000)
        self.max_backup_bytes_spin.setDecimals(0)
        _cur_quota_mb = round(self.watch.get("max_backup_bytes", 0) / (1024 * 1024))
        self.max_backup_bytes_spin.setValue(_cur_quota_mb)
        self.max_backup_bytes_spin.setSuffix(" MB  (0 = no limit)")
        self.max_backup_bytes_spin.setToolTip(
            "Stop new backups for this watch once total backup storage exceeds this limit. "
            "Old backups must be deleted to free space."
        )
        form.addRow("Storage quota:", self.max_backup_bytes_spin)

        self.compress_combo = QComboBox()
        self.compress_combo.addItem("Off", 0)
        self.compress_combo.addItem("Fast (level 1)", 1)
        self.compress_combo.addItem("Balanced (level 6)", 6)
        self.compress_combo.addItem("Best (level 9)", 9)
        # Set current index based on existing compression value
        current_compression = self.watch.get("compression", False)
        if current_compression is True or current_compression == 6:
            self.compress_combo.setCurrentIndex(2)  # Balanced
        elif current_compression == 1:
            self.compress_combo.setCurrentIndex(1)  # Fast
        elif current_compression == 9:
            self.compress_combo.setCurrentIndex(3)  # Best
        else:
            self.compress_combo.setCurrentIndex(0)  # Off
        form.addRow("Compression:", self.compress_combo)

        # Per-watch destination
        edit_dest_widget = QWidget()
        edit_dest_row = QHBoxLayout(edit_dest_widget)
        edit_dest_row.setContentsMargins(0, 0, 0, 0)
        self.dest_input = QLineEdit(self.watch.get("destination", ""))
        self.dest_input.setPlaceholderText(
            "Leave blank to use global destination  ·  or enter e.g. \\\\server\\share\\folder"
        )
        edit_dest_browse = QPushButton("Browse")
        edit_dest_browse.setObjectName("secondary")
        edit_dest_browse.setMaximumWidth(80)
        edit_dest_browse.clicked.connect(
            lambda: self.dest_input.setText(
                QFileDialog.getExistingDirectory(self, "Select Destination Folder")
                or self.dest_input.text()
            )
        )
        edit_dest_row.addWidget(self.dest_input)
        edit_dest_row.addWidget(edit_dest_browse)
        form.addRow("Destination:", edit_dest_widget)

        # ── Multi-destinations (proper list widget) ─────────────────────────────
        from PyQt6.QtWidgets import QListWidget, QListWidgetItem
        dest_list_group = QGroupBox("Additional Destinations")
        dest_list_group.setStyleSheet("QGroupBox { color:#9ca3af; font-size:11px; }")
        dest_list_outer = QVBoxLayout(dest_list_group)
        dest_list_outer.setSpacing(6)
        dest_list_outer.setContentsMargins(8, 8, 8, 8)

        _dest_note = QLabel("Backups are copied to every destination listed here after each run.")
        _dest_note.setStyleSheet("color:#6b7280; font-size:10px;")
        _dest_note.setWordWrap(True)
        dest_list_outer.addWidget(_dest_note)

        self._dest_list_widget = QListWidget()
        self._dest_list_widget.setMaximumHeight(100)
        self._dest_list_widget.setAlternatingRowColors(True)
        for _d in self.watch.get("destinations", []):
            _item = QListWidgetItem(_DestinationEntryDialog.dest_label(_d))
            _item.setData(Qt.ItemDataRole.UserRole, _d)
            self._dest_list_widget.addItem(_item)
        dest_list_outer.addWidget(self._dest_list_widget)

        dest_btn_row = QHBoxLayout()
        _add_dest_btn = QPushButton("➕ Add Destination")
        _add_dest_btn.setObjectName("secondary")
        _add_dest_btn.clicked.connect(self._add_destination)
        _edit_dest_btn = QPushButton("✏ Edit")
        _edit_dest_btn.setObjectName("secondary")
        _edit_dest_btn.clicked.connect(self._edit_destination)
        _remove_dest_btn = QPushButton("🗑 Remove")
        _remove_dest_btn.setObjectName("danger")
        _remove_dest_btn.clicked.connect(self._remove_destination)
        dest_btn_row.addWidget(_add_dest_btn)
        dest_btn_row.addWidget(_edit_dest_btn)
        dest_btn_row.addWidget(_remove_dest_btn)
        dest_btn_row.addStretch()
        dest_list_outer.addLayout(dest_btn_row)
        form.addRow("", dest_list_group)

        # Sync mode is always ON — files are copied directly into the destination.
        self._sync_mode = True

        self.skip_auto_check = QCheckBox("Skip auto backup  (manual only)")
        self.skip_auto_check.setChecked(self.watch.get("skip_auto_backup", False))
        form.addRow("", self.skip_auto_check)

        # ── Pre / Post backup hooks ─────────────────────────────────────────
        hooks_group = QGroupBox("Backup Hooks (optional)")
        hooks_group.setStyleSheet("QGroupBox { color:#9ca3af; font-size:11px; }")
        hooks_layout = QFormLayout(hooks_group)
        hooks_layout.setSpacing(6)
        hooks_layout.setContentsMargins(8, 10, 8, 8)

        _hook_note = QLabel(
            "Runs a shell command before/after backup. If pre-command fails, backup is skipped."
        )
        _hook_note.setStyleSheet("color:#6b7280; font-size:10px;")
        _hook_note.setWordWrap(True)
        hooks_layout.addRow(_hook_note)

        self.pre_cmd_input = QLineEdit(self.watch.get("pre_backup_cmd", ""))
        self.pre_cmd_input.setPlaceholderText(
            "e.g.  net stop MyService  or  /scripts/flush_db.sh"
        )
        hooks_layout.addRow("Pre-backup:", self.pre_cmd_input)

        self.post_cmd_input = QLineEdit(self.watch.get("post_backup_cmd", ""))
        self.post_cmd_input.setPlaceholderText(
            "e.g.  net start MyService  or  /scripts/notify.sh"
        )
        hooks_layout.addRow("Post-backup:", self.post_cmd_input)
        form.addRow("", hooks_group)

        # ── Per-watch Notification Overrides ────────────────────────────────
        _pn = self.watch.get("notify_overrides", {})
        notify_ov_group = QGroupBox("Notification Overrides (optional)")
        notify_ov_group.setStyleSheet("QGroupBox { color:#9ca3af; font-size:11px; }")
        notify_ov_layout = QFormLayout(notify_ov_group)
        notify_ov_layout.setSpacing(6)
        notify_ov_layout.setContentsMargins(8, 10, 8, 8)

        _pn_note = QLabel(
            "Leave blank to use the global settings. "
            "Set a value here to override for this watch only."
        )
        _pn_note.setStyleSheet("color:#6b7280; font-size:10px;")
        _pn_note.setWordWrap(True)
        notify_ov_layout.addRow(_pn_note)

        self.watch_webhook_input = QLineEdit(_pn.get("webhook_url", ""))
        self.watch_webhook_input.setPlaceholderText(
            "https://hooks.slack.com/…  or  https://discord.com/api/webhooks/…"
        )
        self.watch_webhook_input.setToolTip(
            "Override the global webhook URL for this watch only.\n"
            "Useful for routing alerts to a specific Slack channel or Discord server."
        )
        notify_ov_layout.addRow("Webhook URL:", self.watch_webhook_input)

        self.watch_ntfy_topic_input = QLineEdit(_pn.get("ntfy_topic", ""))
        self.watch_ntfy_topic_input.setPlaceholderText(
            "e.g.  my-critical-watch-alerts"
        )
        self.watch_ntfy_topic_input.setToolTip(
            "Override the global ntfy topic for this watch only.\n"
            "The server URL and token are still taken from global ntfy settings."
        )
        notify_ov_layout.addRow("ntfy topic:", self.watch_ntfy_topic_input)

        form.addRow("", notify_ov_group)

        # ── Encryption ──────────────────────────────────────────────────────
        enc_row = QHBoxLayout()
        self.encrypt_input = QLineEdit(self.watch.get("encrypt_key", ""))
        self.encrypt_input.setEchoMode(QLineEdit.EchoMode.Password)
        self.encrypt_input.setPlaceholderText("44-char encryption key  (leave blank to disable)")
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
                QLineEdit.EchoMode.Normal if on else QLineEdit.EchoMode.Password
            )
        )
        rotate_key_btn = QPushButton("🔄 Rotate…")
        rotate_key_btn.setObjectName("secondary")
        rotate_key_btn.setToolTip(
            "Re-encrypt all existing backups for this watch with a new key.\n"
            "The old key must match what was used when the backups were created."
        )
        rotate_key_btn.clicked.connect(self._rotate_key)
        enc_row.addWidget(self.encrypt_input)
        enc_row.addWidget(gen_key_btn)
        enc_row.addWidget(show_key_btn)
        copy_key_btn = QPushButton("📋")
        copy_key_btn.setObjectName("secondary")
        copy_key_btn.setFixedWidth(36)
        copy_key_btn.setToolTip("Copy key to clipboard")
        copy_key_btn.clicked.connect(
            lambda: QApplication.clipboard().setText(self.encrypt_input.text().strip())
            if self.encrypt_input.text().strip()
            else None
        )
        enc_row.addWidget(copy_key_btn)
        enc_row.addWidget(rotate_key_btn)
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
            from PyQt6.QtWidgets import QColorDialog
            from PyQt6.QtGui import QColor
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

        # Include-only (whitelist) patterns
        incl_label = QLabel(
            "Include-only patterns  (one per line, e.g. <code>*.docx</code>):<br>"
            "<span style='color:#6b7280; font-size:11px;'>"
            "When set, <b>only</b> files matching these patterns are backed up. "
            "Leave blank to back up everything (minus exclusions).</span>"
        )
        incl_label.setTextFormat(Qt.TextFormat.RichText)
        incl_label.setWordWrap(True)
        incl_label.setStyleSheet("color:#9ca3af;")
        form.addRow("", incl_label)
        # Load: strip leading "!" that the engine uses internally
        _raw_excl = [p for p in self.watch.get("exclude_patterns", []) if not p.startswith("!")]
        _raw_incl = [p[1:] for p in self.watch.get("exclude_patterns", []) if p.startswith("!")]
        self.excl_edit.setPlainText("\n".join(_raw_excl))
        self.incl_edit = QTextEdit()
        self.incl_edit.setMaximumHeight(80)
        self.incl_edit.setPlaceholderText("e.g.\n*.docx\n*.xlsx\n*.pdf")
        self.incl_edit.setPlainText("\n".join(_raw_incl))
        form.addRow("Include only:", self.incl_edit)

        # ── Per-watch bandwidth override ─────────────────────────────────────
        bw_group = QGroupBox("Bandwidth Override  (leave at 0 to use global setting)")
        bw_group.setStyleSheet("QGroupBox { color:#9ca3af; font-size:11px; }")
        bw_outer = QVBoxLayout(bw_group)
        bw_outer.setSpacing(6)
        bw_outer.setContentsMargins(8, 12, 8, 8)

        bw_form = QFormLayout()
        bw_form.setSpacing(8)
        self._watch_bw_spin = QDoubleSpinBox()
        self._watch_bw_spin.setRange(0.0, 1000.0)
        self._watch_bw_spin.setDecimals(1)
        self._watch_bw_spin.setSuffix(" MB/s  (0 = use global)")
        self._watch_bw_spin.setValue(float(self.watch.get("max_backup_mbps", 0.0)))
        bw_form.addRow("Max bandwidth:", self._watch_bw_spin)
        bw_outer.addLayout(bw_form)

        bw_sched_lbl = QLabel("Per-watch schedule (optional — overrides max bandwidth during time windows):")
        bw_sched_lbl.setStyleSheet("color:#6b7280; font-size:10px;")
        bw_outer.addWidget(bw_sched_lbl)

        self._watch_bw_table = QTableWidget()
        self._watch_bw_table.setColumnCount(3)
        self._watch_bw_table.setHorizontalHeaderLabels(["Start (HH:MM)", "End (HH:MM)", "Max MB/s"])
        self._watch_bw_table.horizontalHeader().setStretchLastSection(False)
        self._watch_bw_table.setColumnWidth(0, 110)
        self._watch_bw_table.setColumnWidth(1, 110)
        self._watch_bw_table.setColumnWidth(2, 90)
        self._watch_bw_table.setMaximumHeight(120)
        self._watch_bw_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        bw_outer.addWidget(self._watch_bw_table)

        bw_btn_row = QHBoxLayout()
        _bw_add = QPushButton("Add Rule")
        _bw_add.setObjectName("secondary")
        _bw_add.clicked.connect(self._watch_bw_add_rule)
        _bw_remove = QPushButton("Remove Rule")
        _bw_remove.setObjectName("secondary")
        _bw_remove.clicked.connect(self._watch_bw_remove_rule)
        bw_btn_row.addWidget(_bw_add)
        bw_btn_row.addWidget(_bw_remove)
        bw_btn_row.addStretch()
        bw_outer.addLayout(bw_btn_row)

        # Populate saved schedule
        for rule in self.watch.get("bandwidth_schedule", []):
            self._watch_bw_add_rule_data(
                rule.get("start", "00:00"),
                rule.get("end", "06:00"),
                rule.get("max_mbps", 0.0),
            )

        layout.addWidget(bw_group)

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

    def _watch_bw_add_rule(self):
        """Add a blank bandwidth schedule row to the per-watch table."""
        self._watch_bw_add_rule_data("00:00", "06:00", 0.0)

    def _watch_bw_add_rule_data(self, start: str, end: str, max_mbps: float):
        """Insert one row into the per-watch bandwidth schedule table."""
        row = self._watch_bw_table.rowCount()
        self._watch_bw_table.insertRow(row)
        self._watch_bw_table.setItem(row, 0, QTableWidgetItem(start))
        self._watch_bw_table.setItem(row, 1, QTableWidgetItem(end))
        self._watch_bw_table.setItem(row, 2, QTableWidgetItem(str(max_mbps)))

    def _watch_bw_remove_rule(self):
        """Remove the selected row from the per-watch bandwidth schedule table."""
        row = self._watch_bw_table.currentRow()
        if row >= 0:
            self._watch_bw_table.removeRow(row)

    def _watch_bw_get_schedule(self) -> list:
        """Read the per-watch bandwidth schedule table into a list of dicts."""
        rules = []
        for r in range(self._watch_bw_table.rowCount()):
            def _cell(c, _r=r):
                item = self._watch_bw_table.item(_r, c)
                return item.text().strip() if item else ""
            try:
                mbps = float(_cell(2))
            except ValueError:
                mbps = 0.0
            rules.append({"start": _cell(0), "end": _cell(1), "max_mbps": mbps})
        return rules

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
        """Generate a new encryption key and populate the field."""
        try:
            if BACKEND_AVAILABLE:
                key = backup_engine.generate_encryption_key()
            else:
                from cryptography.fernet import Fernet
                key = Fernet.generate_key().decode()
            self.encrypt_input.setEchoMode(QLineEdit.EchoMode.Normal)
            self.encrypt_input.setText(key)
            QApplication.clipboard().setText(key)
            QMessageBox.information(
                self, "Key Generated",
                f"A new encryption key has been generated and copied to your clipboard.\n\n"
                f"⚠ IMPORTANT: Save this key somewhere safe!\n"
                f"Without it you cannot restore your encrypted backups.\n\n{key}"
            )
        except Exception as e:
            self.error_lbl.setText(f"Key generation failed: {e}")

    def _rotate_key(self):
        """Rotate the encryption key: re-encrypt all existing backups for this watch."""
        from PyQt6.QtWidgets import QDialog, QVBoxLayout, QFormLayout, QLineEdit, QLabel, QDialogButtonBox, QProgressDialog
        from PyQt6.QtCore import Qt

        old_key = self.watch.get("encrypt_key", "").strip()
        if not old_key:
            QMessageBox.warning(self, "Key Rotation",
                "This watch has no encryption key set. Enable encryption first, then rotate.")
            return

        # Dialog to collect new key
        dlg = QDialog(self)
        dlg.setWindowTitle("Rotate Encryption Key")
        dlg.setMinimumWidth(480)
        vlay = QVBoxLayout(dlg)
        vlay.addWidget(QLabel(
            "<b>Re-encrypt all backups for this watch with a new key.</b><br><br>"
            "The current key (shown below) will be used to decrypt existing files.<br>"
            "Enter or generate a new key; all backups will be re-encrypted in place.<br>"
            "<span style='color:#f59e0b;'>⚠  This cannot be undone. Keep the new key safe.</span>"
        ))
        fl = QFormLayout()
        old_key_lbl = QLineEdit(old_key)
        old_key_lbl.setReadOnly(True)
        old_key_lbl.setEchoMode(QLineEdit.EchoMode.Password)
        fl.addRow("Current key (read-only):", old_key_lbl)

        new_key_edit = QLineEdit()
        new_key_edit.setPlaceholderText("44-char new key")
        fl.addRow("New key:", new_key_edit)
        gen_btn = QPushButton("Generate new key")
        gen_btn.setObjectName("secondary")
        def _gen():
            try:
                if BACKEND_AVAILABLE:
                    k = backup_engine.generate_encryption_key()
                else:
                    from cryptography.fernet import Fernet
                    k = Fernet.generate_key().decode()
                new_key_edit.setText(k)
                new_key_edit.setEchoMode(QLineEdit.EchoMode.Normal)
            except Exception as e:
                QMessageBox.warning(dlg, "Error", str(e))
        gen_btn.clicked.connect(_gen)
        fl.addRow("", gen_btn)
        vlay.addLayout(fl)

        btns = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        btns.accepted.connect(dlg.accept)
        btns.rejected.connect(dlg.reject)
        vlay.addWidget(btns)

        if dlg.exec() != QDialog.DialogCode.Accepted:
            return

        new_key = new_key_edit.text().strip()
        if not new_key:
            QMessageBox.warning(self, "Key Rotation", "New key cannot be empty.")
            return
        if len(new_key) != 44:
            QMessageBox.warning(self, "Key Rotation",
                f"New key must be exactly 44 characters (got {len(new_key)}).")
            return
        if new_key == old_key:
            QMessageBox.information(self, "Key Rotation", "New key is the same as the current key — nothing to do.")
            return

        # Find backup directories for this watch
        dest = ""
        watch_id = self.watch.get("id", "")
        try:
            if hasattr(self, "_parent_cfg"):
                dest = self._parent_cfg.get("destination", "")
            elif self.parent() and hasattr(self.parent(), "cfg"):
                dest = self.parent().cfg.get("destination", "")
        except Exception:
            pass

        if not dest:
            QMessageBox.warning(self, "Key Rotation",
                "Could not determine backup destination. Save the watch first, then rotate.")
            return

        if not BACKEND_AVAILABLE:
            QMessageBox.critical(self, "Key Rotation", "Backend not available — cannot rotate key.")
            return

        backups = backup_engine.list_backups(dest, watch_id)
        if not backups:
            # No existing backups — just update the key in the field
            self.encrypt_input.setText(new_key)
            QMessageBox.information(self, "Key Rotation",
                "No existing backups found — key updated in the field.\n"
                "Click Save Changes to apply.")
            return

        reply = QMessageBox.question(self, "Confirm Key Rotation",
            f"<b>{len(backups)} backup snapshot(s)</b> will be re-encrypted in place.<br><br>"
            "This may take a while depending on backup size.<br>"
            "The app will be unresponsive during rotation.<br><br>"
            "Proceed?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if reply != QMessageBox.StandardButton.Yes:
            return

        progress = QProgressDialog("Rotating encryption key…", None, 0, len(backups), self)
        progress.setWindowTitle("Key Rotation")
        progress.setWindowModality(Qt.WindowModality.WindowModal)
        progress.setValue(0)
        progress.show()

        errors = []
        for i, b in enumerate(backups):
            bd = b.get("backup_dir", "")
            if not bd:
                continue
            progress.setLabelText(f"Rotating snapshot {i+1}/{len(backups)}…\n{bd}")
            from PyQt6.QtWidgets import QApplication
            QApplication.processEvents()

            def _prog(rel, idx, total, _i=i, _total=len(backups)):
                pass  # per-file progress would need a nested dialog; omit for simplicity

            result = backup_engine.rotate_encryption_key(bd, old_key, new_key, _prog)
            if not result.get("ok"):
                errors.extend(result.get("errors", []))
            progress.setValue(i + 1)
            QApplication.processEvents()

        progress.close()

        if errors:
            QMessageBox.warning(self, "Key Rotation — Partial Errors",
                f"Key rotation completed with {len(errors)} error(s):\n\n" +
                "\n".join(errors[:10]) +
                (f"\n…and {len(errors)-10} more" if len(errors) > 10 else ""))
        else:
            QMessageBox.information(self, "Key Rotation Complete",
                f"All {len(backups)} snapshot(s) re-encrypted successfully.\n\n"
                f"New key has been placed in the field — click Save Changes to apply.")

        self.encrypt_input.setText(new_key)
        self.encrypt_input.setEchoMode(QLineEdit.EchoMode.Normal)

    def _add_destination(self):
        """Open the destination entry dialog and append the result to the list."""
        from PyQt6.QtWidgets import QListWidgetItem
        dlg = _DestinationEntryDialog(self)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            dest = dlg.get_dest()
            if dest:
                item = QListWidgetItem(_DestinationEntryDialog.dest_label(dest))
                item.setData(Qt.ItemDataRole.UserRole, dest)
                self._dest_list_widget.addItem(item)

    def _edit_destination(self):
        """Open the entry dialog pre-filled with the selected destination."""
        from PyQt6.QtWidgets import QListWidgetItem
        row = self._dest_list_widget.currentRow()
        if row < 0:
            QMessageBox.information(self, "Edit Destination", "Select a destination from the list first.")
            return
        existing = self._dest_list_widget.item(row).data(Qt.ItemDataRole.UserRole)
        dlg = _DestinationEntryDialog(self, existing=existing)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            dest = dlg.get_dest()
            if dest:
                item = QListWidgetItem(_DestinationEntryDialog.dest_label(dest))
                item.setData(Qt.ItemDataRole.UserRole, dest)
                self._dest_list_widget.takeItem(row)
                self._dest_list_widget.insertItem(row, item)
                self._dest_list_widget.setCurrentRow(row)

    def _remove_destination(self):
        """Remove the currently selected destination from the list."""
        row = self._dest_list_widget.currentRow()
        if row >= 0:
            self._dest_list_widget.takeItem(row)

    def get_values(self) -> dict:
        excl = [
            ln.strip() for ln in self.excl_edit.toPlainText().splitlines()
            if ln.strip()
        ]
        # Merge include-only patterns as "!pattern" entries understood by the engine
        incl = [
            "!" + ln.strip() for ln in self.incl_edit.toPlainText().splitlines()
            if ln.strip()
        ]
        combined_patterns = excl + incl
        tags = [t.strip() for t in self.tags_input.text().split(",") if t.strip()]
        destinations = []
        try:
            for _i in range(self._dest_list_widget.count()):
                _item = self._dest_list_widget.item(_i)
                if _item:
                    destinations.append(_item.data(Qt.ItemDataRole.UserRole))
        except Exception:
            pass
        return {
            "name":               self.name_input.text().strip(),
            "interval_min":       self.interval_spin.value(),
            "schedule_times":     self.watch_schedule_widget.get_entries(),
            "max_file_size_mb":   int(self.max_file_size_spin.value()),
            "max_backup_bytes":   int(self.max_backup_bytes_spin.value()) * 1024 * 1024,
            "compression":        self.compress_combo.currentData(),
            "sync_mode":          True,
            "destination":        self.dest_input.text().strip(),
            "destinations":       destinations,
            "skip_auto_backup":   self.skip_auto_check.isChecked(),
            "color":              self.color_input.text().strip(),
            "notes":              self.notes_input.text().strip(),
            "tags":               tags,
            "exclude_patterns":   combined_patterns,
            "encrypt_key":        self.encrypt_input.text().strip(),
            "pre_backup_cmd":     self.pre_cmd_input.text().strip(),
            "post_backup_cmd":    self.post_cmd_input.text().strip(),
            # Per-watch bandwidth — 0 means "use global"
            "max_backup_mbps":    self._watch_bw_spin.value(),
            "bandwidth_schedule": self._watch_bw_get_schedule(),
            # Scheduled force-full backup
            "force_full_interval_days": self.force_full_interval_spin.value(),
            # Drive trigger
            "drive_trigger_label":  self.drive_trigger_label_input.text().strip(),
            "drive_trigger_serial": self.drive_trigger_serial_input.text().strip().upper(),
            # Per-watch notification overrides (blank = use global)
            "notify_overrides": {
                "webhook_url": self.watch_webhook_input.text().strip(),
                "ntfy_topic":  self.watch_ntfy_topic_input.text().strip(),
            },
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
        # Cache OAuth credentials once at init — avoids re-reading .env on
        # every GDRIVE_CLIENT_ID / GDRIVE_CLIENT_SECRET property access.
        _creds = self._load_env_credentials()
        self._gdrive_client_id     = _creds["GDRIVE_CLIENT_ID"]
        self._gdrive_client_secret = _creds["GDRIVE_CLIENT_SECRET"]
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
        close_btn.setStyleSheet("padding: 0px; font-size: 15px;")
        close_btn.clicked.connect(self.close)
        hl.addWidget(close_btn)
        layout.addWidget(header)

        self._tabs = QTabWidget()
        tabs = self._tabs
        tabs.tabBar().setExpanding(False)
        tabs.tabBar().setElideMode(Qt.TextElideMode.ElideNone)
        tabs.setContentsMargins(16, 16, 16, 16)

        # ── Tab 1: General ─────────────────────────────────────────────────
        self._general_inner = QWidget()
        general_inner = self._general_inner
        gl = QVBoxLayout(general_inner)
        gl.setSpacing(16)
        gl.setContentsMargins(20, 20, 20, 20)

        dest_group = QGroupBox("Backup Destination")
        dg_main = QVBoxLayout(dest_group)
        dg_main.setSpacing(8)

        dest_type_row = QHBoxLayout()
        dest_type_row.addWidget(QLabel("Type:"))
        self.dest_type_combo = QComboBox()
        self.dest_type_combo.addItems([
            "Local / Mapped Drive",
            "Network Share (SMB)",
            "SFTP",
            "FTPS",
            "FTP",
            "HTTPS API",
            "rclone",
            "WebDAV / Nextcloud",
            "Google Drive",
        ])
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
        self.dest_smb_pass   = QLineEdit(); self.dest_smb_pass.setPlaceholderText("Password"); self.dest_smb_pass.setEchoMode(QLineEdit.EchoMode.Password)
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
        self.sftp_pass = QLineEdit(); self.sftp_pass.setPlaceholderText("password"); self.sftp_pass.setEchoMode(QLineEdit.EchoMode.Password)
        self.sftp_path = QLineEdit(); self.sftp_path.setPlaceholderText("/remote/backup/path")
        self.sftp_keyfile = QLineEdit(); self.sftp_keyfile.setPlaceholderText("Path to private key file (optional)")
        sftp_key_row = QHBoxLayout()
        sftp_key_row.addWidget(self.sftp_keyfile)
        sftp_browse_key = QPushButton("Browse"); sftp_browse_key.setObjectName("secondary"); sftp_browse_key.setMaximumWidth(70)
        sftp_browse_key.clicked.connect(lambda: self.sftp_keyfile.setText(
            QFileDialog.getOpenFileName(self, "Select Key File")[0] or self.sftp_keyfile.text()
        ))
        sftp_key_row.addWidget(sftp_browse_key)
        self.sftp_key_pass = QLineEdit(); self.sftp_key_pass.setPlaceholderText("Passphrase (if key is password-protected)"); self.sftp_key_pass.setEchoMode(QLineEdit.EchoMode.Password)
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
        self.ftp_pass = QLineEdit(); self.ftp_pass.setPlaceholderText("password"); self.ftp_pass.setEchoMode(QLineEdit.EchoMode.Password)
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
        self.https_token = QLineEdit(); self.https_token.setPlaceholderText("Bearer token (optional)"); self.https_token.setEchoMode(QLineEdit.EchoMode.Password)
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

        # ── rclone destination ───────────────────────────────────────────────
        self.dest_rclone_widget = QWidget()
        rcl = QFormLayout(self.dest_rclone_widget)
        rcl.setContentsMargins(0, 0, 0, 0)
        rcl.setSpacing(6)

        # ── In-app remote picker ─────────────────────────────────────────
        rclone_picker_row = QHBoxLayout()
        self.rclone_picker_combo = QComboBox()
        self.rclone_picker_combo.setPlaceholderText("— detect remotes first —")
        self.rclone_picker_combo.setMinimumWidth(160)
        self.rclone_picker_combo.currentTextChanged.connect(self._on_rclone_picker_changed)
        rclone_detect_btn = QPushButton("🔍 Detect Remotes")
        rclone_detect_btn.setObjectName("secondary")
        rclone_detect_btn.setToolTip("Run 'rclone listremotes' to find configured remotes")
        rclone_detect_btn.clicked.connect(self._detect_rclone_remotes)
        rclone_config_btn = QPushButton("⚙ rclone config…")
        rclone_config_btn.setObjectName("secondary")
        rclone_config_btn.setToolTip("Open a terminal running 'rclone config' to add / edit remotes")
        rclone_config_btn.clicked.connect(self._launch_rclone_config)
        rclone_picker_row.addWidget(self.rclone_picker_combo, stretch=1)
        rclone_picker_row.addWidget(rclone_detect_btn)
        rclone_picker_row.addWidget(rclone_config_btn)
        rcl.addRow("Pick remote:", rclone_picker_row)

        self.rclone_remote = QLineEdit(); self.rclone_remote.setPlaceholderText("myremote")
        self.rclone_path = QLineEdit(); self.rclone_path.setPlaceholderText("/backups")
        rcl.addRow("Remote name:", self.rclone_remote)
        rcl.addRow("Remote path:", self.rclone_path)
        rclone_note = QLabel(
            "Click \u2018Detect Remotes\u2019 to list remotes from your rclone config, or type a name "
            "manually. Use \u2018rclone config\u2026\u2019 to add a new provider (70+ supported)."
        )
        rclone_note.setStyleSheet("color: #94a3b8; font-size: 11px;")
        rclone_note.setWordWrap(True)
        rcl.addRow("", rclone_note)
        rclone_test_btn = QPushButton("Test Connection")
        rclone_test_btn.setObjectName("secondary")
        rclone_test_btn.clicked.connect(self._test_rclone)
        rcl.addRow("", rclone_test_btn)
        self.dest_rclone_widget.setVisible(False)
        dg_main.addWidget(self.dest_rclone_widget)

        # ── WebDAV / Nextcloud destination ─────────────────────────────────
        self.dest_webdav_widget = QWidget()
        wdvl = QFormLayout(self.dest_webdav_widget)
        self.webdav_url  = QLineEdit(); self.webdav_url.setPlaceholderText("https://nextcloud.example.com")
        self.webdav_user = QLineEdit(); self.webdav_user.setPlaceholderText("username")
        self.webdav_pass = QLineEdit(); self.webdav_pass.setPlaceholderText("password"); self.webdav_pass.setEchoMode(QLineEdit.EchoMode.Password)
        self.webdav_path = QLineEdit(); self.webdav_path.setPlaceholderText("/backups")
        self.webdav_root = QLineEdit(); self.webdav_root.setPlaceholderText("/remote.php/dav/files/username/  (Nextcloud)")
        self.webdav_ssl  = QCheckBox("Verify SSL certificate"); self.webdav_ssl.setChecked(True)
        wdv_btn_row = QHBoxLayout()
        self.webdav_test_btn = QPushButton("Test WebDAV")
        self.webdav_test_btn.clicked.connect(self._test_webdav)
        wdv_btn_row.addWidget(self.webdav_test_btn); wdv_btn_row.addStretch()
        wdvl.addRow("URL:",         self.webdav_url)
        wdvl.addRow("Username:",    self.webdav_user)
        wdvl.addRow("Password:",    self.webdav_pass)
        wdvl.addRow("Remote path:", self.webdav_path)
        wdvl.addRow("DAV root:",    self.webdav_root)
        wdvl.addRow("",             self.webdav_ssl)
        wdvl.addRow("",             wdv_btn_row)
        self.dest_webdav_widget.setVisible(False)
        dg_main.addWidget(self.dest_webdav_widget)

        # ── Google Drive destination ──────────────────────────────────────────
        self.dest_gdrive_widget = QWidget()
        gdl = QVBoxLayout(self.dest_gdrive_widget)
        gdl.setContentsMargins(0, 4, 0, 4)
        gdrive_info = QLabel(
            "<b>Google Drive</b> is configured in the <b>Cloud</b> tab of Settings.<br>"
            "Connect your account there, then assign this watch to Google Drive.<br>"
            "Once assigned the <i>dest_type</i> for this watch is managed automatically."
        )
        gdrive_info.setWordWrap(True)
        gdrive_info.setStyleSheet("color: #94a3b8; font-size: 12px;")
        gdl.addWidget(gdrive_info)
        self.dest_gdrive_widget.setVisible(False)
        dg_main.addWidget(self.dest_gdrive_widget)

        gl.addWidget(dest_group)  # BUG FIX: dest_group was never added to gl, causing Qt to GC
                                  # the C++ QGroupBox (and all children, including dest_type_combo)
                                  # as soon as _build_ui() returned, making _load_values() crash
                                  # with "wrapped C/C++ object of type QComboBox has been deleted"

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
        self.seconds_warning_label = QLabel(
            "⚠️  Seconds mode is for testing only — do not use in production. "
            "Backups will run every few seconds and may hammer your filesystem."
        )
        self.seconds_warning_label.setStyleSheet("color: #c0392b; font-weight: bold;")
        self.seconds_warning_label.setWordWrap(True)
        self.seconds_warning_label.setVisible(False)
        sg.addRow("", self.seconds_warning_label)
        # Disk space alert threshold
        self.disk_alert_spin = QSpinBox()
        self.disk_alert_spin.setRange(0, 1000)
        self.disk_alert_spin.setSuffix(" GB")
        self.disk_alert_spin.setSpecialValueText("0 — Disabled")
        sg.addRow("Alert when free space below:", self.disk_alert_spin)

        # Scheduled backup times (day-of-week aware)
        self.schedule_times_widget = ScheduleTableWidget()
        self.schedule_times_widget.setToolTip(
            "Add one row per scheduled time.\n"
            "Tick the day checkboxes to restrict which days of the week each time fires.\n"
            "Leave all days ticked to run every day (the original behaviour)."
        )
        sg.addRow("Run at times:", self.schedule_times_widget)

        # Backup window — start and stop times
        self.backup_window_start_input = QLineEdit()
        self.backup_window_start_input.setPlaceholderText("e.g. 01:00  (leave blank for no start limit)")
        self.backup_window_start_input.setToolTip(
            "If set, auto-backups will not START before this time each day.\n"
            "Combine with Stop by to define a quiet-hours window.\n"
            "Example: Start after 01:00 + Stop by 06:00 = backups only between 1 AM and 6 AM."
        )
        sg.addRow("Start after:", self.backup_window_start_input)

        window_row = QHBoxLayout()
        self.backup_window_end_input = QLineEdit()
        self.backup_window_end_input.setPlaceholderText("e.g. 06:00  (leave blank for no cutoff)")
        self.backup_window_end_input.setToolTip(
            "If set, auto-backups that START after this time are skipped until the next day.\n"
            "Useful to avoid backups running into business hours.\n"
            "Example: set Start after to 01:00 and Stop by to 06:00."
        )
        window_row.addWidget(self.backup_window_end_input)
        sg.addRow("Stop by:", window_row)
        self.bw_spin = QDoubleSpinBox()
        self.bw_spin.setRange(0.0, 1000.0)
        self.bw_spin.setDecimals(1)
        self.bw_spin.setSuffix(" MB/s  (0 = unlimited)")
        self.bw_spin.setValue(0.0)
        sg.addRow("Max bandwidth:", self.bw_spin)

        # Bandwidth schedule table
        bw_sched_label = QLabel("Schedule (optional — overrides max bandwidth during time windows):")
        sg.addRow("", bw_sched_label)
        self.bw_table = QTableWidget()
        self.bw_table.setColumnCount(3)
        self.bw_table.setHorizontalHeaderLabels(["Start (HH:MM)", "End (HH:MM)", "Max MB/s"])
        self.bw_table.horizontalHeader().setStretchLastSection(False)
        self.bw_table.setColumnWidth(0, 120)
        self.bw_table.setColumnWidth(1, 120)
        self.bw_table.setColumnWidth(2, 100)
        self.bw_table.setMaximumHeight(150)
        self.bw_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        sg.addRow("", self.bw_table)
        
        # Bandwidth schedule buttons
        btn_row = QHBoxLayout()
        add_bw_btn = QPushButton("Add Schedule Rule")
        add_bw_btn.setMinimumWidth(150)
        add_bw_btn.clicked.connect(self._add_bw_rule)
        remove_bw_btn = QPushButton("Remove Rule")
        remove_bw_btn.setMinimumWidth(110)
        remove_bw_btn.clicked.connect(self._remove_bw_rule)
        btn_row.addWidget(add_bw_btn)
        btn_row.addWidget(remove_bw_btn)
        btn_row.addStretch()
        sg.addRow("", btn_row)

        # System idle threshold
        self.idle_spin = QSpinBox()
        self.idle_spin.setRange(0, 100)
        self.idle_spin.setSuffix("%  CPU  (0 = always run)")
        self.idle_spin.setToolTip(
            "Auto-backups are deferred while CPU usage exceeds this threshold.\n"
            "Example: 60 = only run auto-backups when CPU is below 60%.\n"
            "Requires psutil (pip install psutil). Set to 0 to disable."
        )
        self.idle_spin.setValue(0)
        sg.addRow("Idle threshold:", self.idle_spin)

        # Metered connection pause
        self.metered_check = QCheckBox("Pause auto-backups on metered connections (Windows only)")
        self.metered_check.setToolTip(
            "When enabled, scheduled backups are skipped if Windows detects a metered network\n"
            "(e.g. mobile hotspot, cellular connection). Check only applies on Windows.\n"
            "Non-Windows systems will ignore this setting."
        )
        sg.addRow("", self.metered_check)

        # Battery pause
        self.battery_check = QCheckBox("Pause auto-backups when running on battery")
        self.battery_check.setToolTip(
            "When enabled, scheduled backups are skipped while the laptop is unplugged.\n"
            "Backups resume automatically once the power adapter is reconnected.\n"
            "Uses psutil.sensors_battery() \u2014 works on Windows, macOS, and Linux."
        )
        sg.addRow("", self.battery_check)

        # Remote upload verification
        self.verify_remote_cb = QCheckBox("Verify remote uploads after transfer (SFTP / FTP / WebDAV)")
        self.verify_remote_cb.setToolTip(
            "After each remote upload, BackupSys re-reads the first 8 KB of every file\n"
            "and compares its MD5 against the local copy.\n"
            "Catches silent corruption and truncated transfers.\n"
            "Adds a small overhead — enable for critical or slow/unreliable connections."
        )
        sg.addRow("", self.verify_remote_cb)

        self.verify_after_cb = QCheckBox("Verify backup integrity after each backup (local)")
        self.verify_after_cb.setToolTip(
            "After each backup completes successfully, BackupSys will re-read every\n"
            "copied file and compare its checksum against the source.\n"
            "Detects silent write errors and storage corruption.\n"
            "Adds extra time proportional to backup size — recommended for critical data."
        )
        sg.addRow("", self.verify_after_cb)

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

        # ── Integrity Checks ───────────────────────────────────────────────
        integ_group = QGroupBox("Integrity Checks")
        integ_layout = QVBoxLayout(integ_group)
        integ_layout.setSpacing(8)
        self.integrity_enabled_cb = QCheckBox("Enable scheduled backup integrity checks")
        self.integrity_enabled_cb.setToolTip(
            "Periodically re-hashes each watch's most recent backup and compares\n"
            "it against the stored SHA-256 and manifest.  Failures trigger email\n"
            "and webhook notifications using your existing notification settings."
        )
        integ_layout.addWidget(self.integrity_enabled_cb)
        integ_row = QHBoxLayout()
        integ_row.addWidget(QLabel("Check every"))
        self.integrity_interval_spin = QSpinBox()
        self.integrity_interval_spin.setRange(1, 365)
        self.integrity_interval_spin.setSuffix(" day(s)")
        self.integrity_interval_spin.setFixedWidth(110)
        integ_row.addWidget(self.integrity_interval_spin)
        integ_row.addStretch()
        integ_layout.addLayout(integ_row)
        _integ_note = QLabel(
            "Each watch is checked on its own independent timer.\n"
            "Requires at least one completed backup before the first check fires."
        )
        _integ_note.setStyleSheet("color: #888; font-size: 11px;")
        integ_layout.addWidget(_integ_note)
        self._run_integrity_btn = QPushButton("🔍  Run Integrity Check Now")
        self._run_integrity_btn.setToolTip(
            "Immediately run an integrity check on all watches, bypassing the\n"
            "scheduled interval. Useful after a restore or when troubleshooting."
        )
        self._run_integrity_btn.clicked.connect(self._trigger_integrity_check_now)
        integ_layout.addWidget(self._run_integrity_btn)
        gl.addWidget(integ_group)

        # ── Global Scheduled Force-Full Backup ──────────────────────────────
        ff_group = QGroupBox("Scheduled Force-Full Backup")
        ff_layout = QVBoxLayout(ff_group)
        ff_layout.setSpacing(8)
        ff_row = QHBoxLayout()
        ff_row.addWidget(QLabel("Force full backup every"))
        self.force_full_global_spin = QSpinBox()
        self.force_full_global_spin.setRange(0, 3650)
        self.force_full_global_spin.setSuffix(" days  (0 = disabled)")
        self.force_full_global_spin.setFixedWidth(160)
        self.force_full_global_spin.setToolTip(
            "Discard the incremental snapshot and run a full backup every N days "
            "across all watches.  Individual watches can override this in their "
            "own settings (Watch Settings → Force full every)."
        )
        ff_row.addWidget(self.force_full_global_spin)
        ff_row.addStretch()
        ff_layout.addLayout(ff_row)
        _ff_note = QLabel(
            "0 = disabled (default).  Set e.g. 7 to force a full backup weekly.\n"
            "Per-watch overrides take priority; set −1 on a watch to exempt it."
        )
        _ff_note.setStyleSheet("color: #888; font-size: 11px;")
        ff_layout.addWidget(_ff_note)
        gl.addWidget(ff_group)

        # ── Startup / Auto-launch ───────────────────────────────────────────
        startup_group = QGroupBox("Startup")
        stl = QVBoxLayout(startup_group)
        self.startup_check = QCheckBox("Start with Windows (runs in background)")
        self.startup_check.stateChanged.connect(self._toggle_startup)
        stl.addWidget(self.startup_check)
        _startup_note = QLabel("Uncheck this box to stop BackupSys from launching at login.")
        _startup_note.setWordWrap(True)
        _startup_note.setStyleSheet("color: #888; font-size: 11px;")
        stl.addWidget(_startup_note)
        gl.addWidget(startup_group)

        # ── Auto-Shutdown ───────────────────────────────────────────────────
        shutdown_group = QGroupBox("Auto-Shutdown")
        sdl = QVBoxLayout(shutdown_group)
        self.shutdown_check = QCheckBox("Shut down PC automatically when all backups complete")
        self.shutdown_check.setToolTip(
            "When enabled, BackupSys will shut down this computer once every\n"
            "active backup finishes. A 60-second countdown dialog will appear\n"
            "first so you can cancel if needed."
        )
        sdl.addWidget(self.shutdown_check)
        _shutdown_note = QLabel(
            "Only triggers when a backup was started manually or by the scheduler — "
            "not on app launch.  A 60-second countdown lets you cancel."
        )
        _shutdown_note.setWordWrap(True)
        _shutdown_note.setStyleSheet("color: #888; font-size: 11px;")
        sdl.addWidget(_shutdown_note)
        gl.addWidget(shutdown_group)

        # ── Portable mode indicator ─────────────────────────────────────────
        _portable_group = QGroupBox("Portable Mode")
        _pl = QVBoxLayout(_portable_group)
        try:
            _is_portable = config_manager._IS_PORTABLE
        except AttributeError:
            _is_portable = False
        if _is_portable:
            _pm_label = QLabel(
                "✅  Portable mode active — all data is stored inside the app folder.\n"
                f"Data path: {config_manager._DATA_DIR}"
            )
        else:
            _pm_label = QLabel(
                "Portable mode is OFF.  To enable it, create an empty file named\n"
                "\"portable.flag\" next to desktop_app.py, then restart BackupSys.\n"
                "All config, snapshots and logs will move into the app folder."
            )
        _pm_label.setWordWrap(True)
        _pm_label.setStyleSheet("font-size: 11px;")
        _pl.addWidget(_pm_label)
        gl.addWidget(_portable_group)

        pass_group = QGroupBox("Admin Password")
        pl = QVBoxLayout(pass_group)
        change_pw = QPushButton("Change Admin Password")
        change_pw.setObjectName("secondary")
        change_pw.clicked.connect(self._change_password)
        pl.addWidget(change_pw)
        gl.addWidget(pass_group)

        # ── Theme ──────────────────────────────────────────────────────────────
        theme_group = QGroupBox("Appearance")
        thl = QVBoxLayout(theme_group)
        thl.addWidget(QLabel("Choose a colour theme (takes effect immediately):"))
        theme_row = QHBoxLayout()
        _s_theme = QSettings(SETTINGS_ORG, SETTINGS_APP)
        _cur = _s_theme.value("theme", "dark")   # "dark" or "light"
        self.theme_dark  = QRadioButton("Dark")
        self.theme_light = QRadioButton("Light")
        from PyQt6.QtWidgets import QButtonGroup
        self._theme_btn_group = QButtonGroup(self)
        self._theme_btn_group.addButton(self.theme_dark)
        self._theme_btn_group.addButton(self.theme_light)
        if _cur == "light":
            self.theme_light.setChecked(True)
        else:
            self.theme_dark.setChecked(True)
        for rb in (self.theme_dark, self.theme_light):
            rb.toggled.connect(self._apply_theme)
            theme_row.addWidget(rb)
        theme_row.addStretch()
        thl.addLayout(theme_row)
        gl.addWidget(theme_group)

        gl.addStretch()

        footer_row = QHBoxLayout()
        save_btn = QPushButton("Save Settings")
        save_btn.setObjectName("success")
        save_btn.clicked.connect(self._save_general)
        footer_row.addWidget(save_btn)

        export_btn = QPushButton("📤 Export Config…")
        export_btn.setObjectName("secondary")
        export_btn.setToolTip(
            "Save a copy of config.json (with passwords redacted) to a file you choose.\n"
            "Use this to back up your BackupSys configuration or move it to another PC."
        )
        export_btn.clicked.connect(self._export_config)
        footer_row.addWidget(export_btn)

        import_btn = QPushButton("📥 Import Config…")
        import_btn.setObjectName("secondary")
        import_btn.setToolTip(
            "Load a previously exported config file and merge it into your current configuration.\n"
            "Passwords will need to be re-entered."
        )
        import_btn.clicked.connect(self._import_config)
        footer_row.addWidget(import_btn)
        footer_row.addStretch()
        gl.addLayout(footer_row)

        self._general_scroll = QScrollArea()
        general = self._general_scroll
        general.setWidget(general_inner)
        general.setWidgetResizable(True)
        general.setFrameShape(QFrame.Shape.NoFrame)
        general.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        tabs.addTab(general, "General")

        # ── Tab 2: Watches ──────────────────────────────────────────────────
        self._watches_tab = QWidget()
        watches_tab = self._watches_tab
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
            self.watch_table.horizontalHeader().setSectionResizeMode(col, QHeaderView.ResizeMode.ResizeToContents)
        self.watch_table.horizontalHeader().setSectionResizeMode(1,  QHeaderView.ResizeMode.Stretch)
        self.watch_table.horizontalHeader().setSectionResizeMode(10, QHeaderView.ResizeMode.Stretch)
        self.watch_table.horizontalHeader().setSectionResizeMode(11, QHeaderView.ResizeMode.Fixed)
        self.watch_table.horizontalHeader().setSectionResizeMode(12, QHeaderView.ResizeMode.Fixed)
        self.watch_table.horizontalHeader().setMinimumSectionSize(60)
        self.watch_table.horizontalHeader().resizeSection(11, 90)
        self.watch_table.horizontalHeader().resizeSection(12, 110)  # was 80 — too narrow for "🗑 Remove"
        self.watch_table.setMouseTracking(True)
        self.watch_table.viewport().setMouseTracking(True)
        self.watch_table.verticalHeader().setVisible(False)
        self.watch_table.verticalHeader().setDefaultSectionSize(44)
        self.watch_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.watch_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.watch_table.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.watch_table.setMinimumHeight(160)
        wl.addWidget(self.watch_table)

        tabs.addTab(watches_tab, "Watches")

        # ── Tab 3: Cloud ────────────────────────────────────────────────────
        self._cloud_tab = QWidget()
        cloud_tab = self._cloud_tab
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
        self.gd_quota_lbl = QLabel("")
        self.gd_quota_lbl.setStyleSheet("color: #94a3b8; font-size: 11px;")
        self.gd_quota_lbl.setVisible(False)
        gd_text.addWidget(self.gd_quota_lbl)
        gd_layout.addLayout(gd_text, stretch=1)

        gd_btn_col = QVBoxLayout()
        self.gd_connect_btn = QPushButton("Connect Google Drive")
        self.gd_connect_btn.setObjectName("success")
        self.gd_connect_btn.clicked.connect(self._connect_gdrive)
        gd_btn_col.addWidget(self.gd_connect_btn)
        self.gd_test_btn = QPushButton("Test Connection")
        self.gd_test_btn.setObjectName("secondary")
        self.gd_test_btn.setVisible(False)
        self.gd_test_btn.setToolTip(
            "Verify that the saved Google Drive token is still valid.\n"
            "A silent token refresh is attempted automatically if it is about to expire."
        )
        self.gd_test_btn.clicked.connect(self._test_gdrive)
        gd_btn_col.addWidget(self.gd_test_btn)
        self.gd_disconnect_btn = QPushButton("Disconnect")
        self.gd_disconnect_btn.setObjectName("danger")
        self.gd_disconnect_btn.setVisible(False)
        self.gd_disconnect_btn.clicked.connect(self._disconnect_gdrive)
        gd_btn_col.addWidget(self.gd_disconnect_btn)
        gd_layout.addLayout(gd_btn_col)
        cl.addWidget(gd_card)

        # ── Assign cloud to watches (multi-select) ──────────────────────────────
        assign_group = QGroupBox("Assign Cloud to Watches")
        agl = QFormLayout(assign_group)

        # Multi-watch checklist: check any number of watches; Save applies to all
        self.cloud_watch_list = QListWidget()
        self.cloud_watch_list.setFixedHeight(90)
        self.cloud_watch_list.setToolTip("Check every watch that should upload to this GDrive folder.")
        agl.addRow("Watches:", self.cloud_watch_list)

        # Provider checkbox
        # (Removed chk_gdrive checkbox and related row)

        self.gd_folder_id = QLineEdit()
        self.gd_folder_id.setPlaceholderText("Google Drive folder ID (leave blank for root)")
        self.gd_folder_id.setToolTip(
            "The ID of the Drive folder where backups are stored.\n"
            "Click 'Browse…' to pick a folder from your Drive."
        )
        gd_folder_row = QHBoxLayout()
        gd_folder_row.addWidget(self.gd_folder_id)
        gd_browse_btn = QPushButton("Browse…")
        gd_browse_btn.setObjectName("secondary")
        gd_browse_btn.setToolTip("List your Drive folders and select one.")
        gd_browse_btn.clicked.connect(self._pick_gdrive_folder)
        gd_folder_row.addWidget(gd_browse_btn)
        gd_folder_widget = QWidget()
        gd_folder_widget.setLayout(gd_folder_row)
        agl.addRow("GDrive folder:", gd_folder_widget)

        save_assign_btn = QPushButton("Save Assignment to All Checked Watches")
        save_assign_btn.setObjectName("success")
        save_assign_btn.clicked.connect(self._save_cloud)
        agl.addRow("", save_assign_btn)
        cl.addWidget(assign_group)

        cl.addStretch()

        tabs.addTab(cloud_tab, "Cloud")

        # ── Tab 4: Notifications ────────────────────────────────────────────
        self._notif_inner = QWidget()
        notif_inner = self._notif_inner
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
        self.email_password.setEchoMode(QLineEdit.EchoMode.Password)
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
            "Make/Zapier, or any custom API that accepts POST requests.\n"
            "Each payload includes a machine_id field (this machine's hostname) "
            "so you can distinguish events when multiple machines share the same webhook URL."
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

        # ── ntfy.sh Push Notifications section ──────────────────────────────
        ntfy_group = QGroupBox("Push Notifications (ntfy.sh)")
        ngl = QFormLayout(ntfy_group)
        ngl.setSpacing(8)

        self.ntfy_enabled_check = QCheckBox("Enable ntfy push notifications")
        ngl.addRow("", self.ntfy_enabled_check)

        self.ntfy_notify_success_check = QCheckBox("Send on successful backup")
        ngl.addRow("", self.ntfy_notify_success_check)

        self.ntfy_notify_failure_check = QCheckBox("Send on failed backup")
        self.ntfy_notify_failure_check.setChecked(True)
        ngl.addRow("", self.ntfy_notify_failure_check)

        self.ntfy_server_input = QLineEdit()
        self.ntfy_server_input.setPlaceholderText("https://ntfy.sh  (or your self-hosted URL)")
        self.ntfy_server_input.setText("https://ntfy.sh")
        ngl.addRow("Server URL:", self.ntfy_server_input)

        self.ntfy_topic_input = QLineEdit()
        self.ntfy_topic_input.setPlaceholderText("my-backupsys-alerts  (required)")
        ngl.addRow("Topic:", self.ntfy_topic_input)

        self.ntfy_token_input = QLineEdit()
        self.ntfy_token_input.setEchoMode(QLineEdit.EchoMode.Password)
        self.ntfy_token_input.setPlaceholderText("Bearer token (optional — for protected topics)")
        ngl.addRow("Auth Token:", self.ntfy_token_input)

        self.ntfy_priority_combo = QComboBox()
        for _p in ["min", "low", "default", "high", "urgent"]:
            self.ntfy_priority_combo.addItem(_p)
        self.ntfy_priority_combo.setCurrentText("default")
        ngl.addRow("Priority:", self.ntfy_priority_combo)

        ntfy_note = QLabel(
            "Delivers instant push notifications to your phone via the free ntfy.sh service\n"
            "or a self-hosted ntfy server.  Install the ntfy app (iOS / Android) and\n"
            "subscribe to your topic to receive alerts.\n"
            "Tip: use a hard-to-guess topic name as a lightweight secret."
        )
        ntfy_note.setStyleSheet("color:#94a3b8; font-size:11px;")
        ntfy_note.setWordWrap(True)
        ngl.addRow("", ntfy_note)

        ntfy_btn_row = QHBoxLayout()
        save_ntfy_btn = QPushButton("Save Push Settings")
        save_ntfy_btn.setObjectName("success")
        save_ntfy_btn.clicked.connect(self._save_ntfy_settings)
        test_ntfy_btn = QPushButton("Send Test Push")
        test_ntfy_btn.setObjectName("secondary")
        test_ntfy_btn.clicked.connect(self._test_ntfy)
        ntfy_btn_row.addWidget(save_ntfy_btn)
        ntfy_btn_row.addWidget(test_ntfy_btn)
        ngl.addRow("", ntfy_btn_row)

        nl.addWidget(ntfy_group)

        # ── Telegram Bot Notifications section ──────────────────────────────
        tg_group = QGroupBox("Telegram Bot Notifications")
        tgl = QFormLayout(tg_group)
        tgl.setSpacing(8)

        self.tg_enabled_check = QCheckBox("Enable Telegram notifications")
        tgl.addRow("", self.tg_enabled_check)

        self.tg_notify_success_check = QCheckBox("Send on successful backup")
        tgl.addRow("", self.tg_notify_success_check)

        self.tg_notify_failure_check = QCheckBox("Send on failed backup")
        self.tg_notify_failure_check.setChecked(True)
        tgl.addRow("", self.tg_notify_failure_check)

        self.tg_token_input = QLineEdit()
        self.tg_token_input.setEchoMode(QLineEdit.EchoMode.Password)
        self.tg_token_input.setPlaceholderText("123456:ABC-DEFxxxxxxxxxxxxxxxxxxxxxxxx")
        tgl.addRow("Bot Token:", self.tg_token_input)

        self.tg_chat_id_input = QLineEdit()
        self.tg_chat_id_input.setPlaceholderText("Your user ID or group/channel chat_id")
        tgl.addRow("Chat ID:", self.tg_chat_id_input)

        tg_note = QLabel(
            "Get a bot token from @BotFather on Telegram (/newbot).\n"
            "Find your chat_id by messaging @userinfobot on Telegram.\n"
            "Works for private messages, groups, and channels."
        )
        tg_note.setStyleSheet("color:#94a3b8; font-size:11px;")
        tg_note.setWordWrap(True)
        tgl.addRow("", tg_note)

        tg_btn_row = QHBoxLayout()
        save_tg_btn = QPushButton("Save Telegram Settings")
        save_tg_btn.setObjectName("success")
        save_tg_btn.clicked.connect(self._save_telegram_settings)
        test_tg_btn = QPushButton("Send Test Message")
        test_tg_btn.setObjectName("secondary")
        test_tg_btn.clicked.connect(self._test_telegram)
        tg_btn_row.addWidget(save_tg_btn)
        tg_btn_row.addWidget(test_tg_btn)
        tgl.addRow("", tg_btn_row)

        nl.addWidget(tg_group)

        # ── Pushover Notifications section ──────────────────────────────────
        po_group = QGroupBox("Pushover Notifications")
        pol = QFormLayout(po_group)
        pol.setSpacing(8)

        self.po_enabled_check = QCheckBox("Enable Pushover notifications")
        pol.addRow("", self.po_enabled_check)

        self.po_notify_success_check = QCheckBox("Send on successful backup")
        pol.addRow("", self.po_notify_success_check)

        self.po_notify_failure_check = QCheckBox("Send on failed backup")
        self.po_notify_failure_check.setChecked(True)
        pol.addRow("", self.po_notify_failure_check)

        self.po_user_key_input = QLineEdit()
        self.po_user_key_input.setEchoMode(QLineEdit.EchoMode.Password)
        self.po_user_key_input.setPlaceholderText("Your Pushover user key")
        pol.addRow("User Key:", self.po_user_key_input)

        self.po_api_token_input = QLineEdit()
        self.po_api_token_input.setEchoMode(QLineEdit.EchoMode.Password)
        self.po_api_token_input.setPlaceholderText("Your application API token")
        pol.addRow("API Token:", self.po_api_token_input)

        self.po_device_input = QLineEdit()
        self.po_device_input.setPlaceholderText("Device name (leave blank for all devices)")
        pol.addRow("Device:", self.po_device_input)

        self.po_priority_combo = QComboBox()
        for _lbl, _val in [("Lowest (-2)", -2), ("Low / Quiet (-1)", -1),
                            ("Normal (0)", 0), ("High (1)", 1)]:
            self.po_priority_combo.addItem(_lbl, _val)
        self.po_priority_combo.setCurrentIndex(2)   # Normal
        pol.addRow("Failure Priority:", self.po_priority_combo)

        po_note = QLabel(
            "Register at pushover.net — free 30-day trial, then a one-time $5 per platform.\n"
            "Create an application at pushover.net/apps/build to get an API token.\n"
            "Priority 'High' will bypass Do Not Disturb on iOS/Android."
        )
        po_note.setStyleSheet("color:#94a3b8; font-size:11px;")
        po_note.setWordWrap(True)
        pol.addRow("", po_note)

        po_btn_row = QHBoxLayout()
        save_po_btn = QPushButton("Save Pushover Settings")
        save_po_btn.setObjectName("success")
        save_po_btn.clicked.connect(self._save_pushover_settings)
        test_po_btn = QPushButton("Send Test Push")
        test_po_btn.setObjectName("secondary")
        test_po_btn.clicked.connect(self._test_pushover)
        po_btn_row.addWidget(save_po_btn)
        po_btn_row.addWidget(test_po_btn)
        pol.addRow("", po_btn_row)

        nl.addWidget(po_group)
        nl.addStretch()


        self._notif_scroll = QScrollArea()
        notif_scroll = self._notif_scroll
        notif_scroll.setWidgetResizable(True)
        notif_scroll.setFrameShape(QFrame.Shape.NoFrame)
        notif_scroll.setWidget(notif_inner)

        tabs.addTab(notif_scroll, "Notifications")

        # ── Tab 5: Logs ────────────────────────────────────────────────────────
        logs_tab = QWidget()
        ll = QVBoxLayout(logs_tab)
        ll.setContentsMargins(12, 12, 12, 12)
        ll.setSpacing(8)

        # ── Toolbar ────────────────────────────────────────────────────────────
        log_toolbar = QHBoxLayout()

        self._log_filter_input = QLineEdit()
        self._log_filter_input.setPlaceholderText("Filter logs…")
        self._log_filter_input.setClearButtonEnabled(True)
        self._log_filter_input.textChanged.connect(self._apply_log_filter)
        log_toolbar.addWidget(self._log_filter_input, stretch=1)

        self._log_tail_check = QCheckBox("Tail (follow)")
        self._log_tail_check.setChecked(True)
        log_toolbar.addWidget(self._log_tail_check)

        log_refresh_btn = QPushButton("🔄 Refresh")
        log_refresh_btn.setObjectName("secondary")
        log_refresh_btn.clicked.connect(self._load_log_tab)
        log_toolbar.addWidget(log_refresh_btn)

        log_clear_btn = QPushButton("🗑 Clear Log")
        log_clear_btn.setObjectName("secondary")
        log_clear_btn.clicked.connect(self._clear_log_file)
        log_toolbar.addWidget(log_clear_btn)

        ll.addLayout(log_toolbar)

        # ── Log viewer ─────────────────────────────────────────────────────────
        self._log_viewer = QPlainTextEdit()
        self._log_viewer.setReadOnly(True)
        _log_font = QFont("Courier New" if sys.platform == "win32" else "Courier")
        _log_font.setPointSize(9)
        self._log_viewer.setFont(_log_font)
        self._log_viewer.setStyleSheet("background:#0a0e18; color:#d1d5db; border:none; border-radius:6px;")
        self._log_viewer.setMaximumBlockCount(5000)   # prevent memory blowup
        ll.addWidget(self._log_viewer)

        # ── Footer: file path + auto-refresh timer ─────────────────────────────
        _log_data_dir = Path(os.environ.get("BACKUPSYS_DATA_DIR", Path(__file__).parent))
        self._log_file_path = _log_data_dir / "logs" / "backupsys.log"
        log_path_lbl = QLabel(str(self._log_file_path))
        log_path_lbl.setStyleSheet("color:#6b7280; font-size:10px;")
        ll.addWidget(log_path_lbl)

        # Poll every 3 s so the tab stays fresh without blocking the UI
        self._log_poll_timer = QTimer(self)
        self._log_poll_timer.setInterval(3000)
        self._log_poll_timer.timeout.connect(self._poll_log_file)
        self._log_last_mtime = 0.0
        self._log_raw_lines: list = []   # unfiltered lines cache

        # Start polling when the Logs tab is visible; stop otherwise
        self._tabs.currentChanged.connect(self._on_tab_changed_log_poll)

        tabs.addTab(logs_tab, "Logs")

        # ── Tab 6: SSH Keys ────────────────────────────────────────────────────
        ssh_tab = QWidget()
        ssh_l = QVBoxLayout(ssh_tab)
        ssh_l.setContentsMargins(20, 20, 20, 20)
        ssh_l.setSpacing(14)

        # Key generation group
        keygen_group = QGroupBox("SSH Key Pair")
        keygen_layout = QVBoxLayout(keygen_group)
        keygen_layout.setSpacing(8)

        keygen_info = QLabel(
            "BackupSys can generate an Ed25519 key pair for password-less SFTP authentication. "
            "The private key is saved to <code>~/.backupsys_keys/id_ed25519</code> and used "
            "automatically when no password is entered in the SFTP settings."
        )
        keygen_info.setTextFormat(Qt.TextFormat.RichText)
        keygen_info.setWordWrap(True)
        keygen_info.setStyleSheet("color:#94a3b8; font-size:11px;")
        keygen_layout.addWidget(keygen_info)

        keygen_btn_row = QHBoxLayout()
        self._keygen_btn = QPushButton("⚡ Generate Ed25519 Key Pair")
        self._keygen_btn.setObjectName("secondary")
        self._keygen_btn.clicked.connect(self._generate_ssh_key)
        keygen_btn_row.addWidget(self._keygen_btn)
        keygen_btn_row.addStretch()
        keygen_layout.addLayout(keygen_btn_row)

        self._pubkey_edit = QPlainTextEdit()
        self._pubkey_edit.setReadOnly(True)
        self._pubkey_edit.setPlaceholderText("No key generated yet — click Generate above.")
        self._pubkey_edit.setMaximumHeight(72)
        self._pubkey_edit.setStyleSheet(
            "background:#0f172a; color:#a3e635; font-family:monospace; font-size:11px; border-radius:4px;"
        )
        keygen_layout.addWidget(self._pubkey_edit)

        copy_key_btn = QPushButton("📋 Copy Public Key")
        copy_key_btn.setObjectName("secondary")
        copy_key_btn.clicked.connect(self._copy_public_key)
        keygen_layout.addWidget(copy_key_btn)

        ssh_l.addWidget(keygen_group)

        # Known-hosts management group
        hosts_group = QGroupBox("Trusted Host Fingerprints  (~/.backupsys_known_hosts)")
        hosts_layout = QVBoxLayout(hosts_group)
        hosts_layout.setSpacing(6)

        hosts_info = QLabel(
            "On first SFTP connection BackupSys trusts the server automatically (TOFU). "
            "Remove a host entry here to force re-verification on the next connection."
        )
        hosts_info.setWordWrap(True)
        hosts_info.setStyleSheet("color:#94a3b8; font-size:11px;")
        hosts_layout.addWidget(hosts_info)

        self._known_hosts_table = QTableWidget(0, 3)
        self._known_hosts_table.setHorizontalHeaderLabels(["Host", "Key Type", "Fingerprint (SHA-256)"])
        self._known_hosts_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Interactive)
        self._known_hosts_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self._known_hosts_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        self._known_hosts_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._known_hosts_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._known_hosts_table.setMaximumHeight(200)
        hosts_layout.addWidget(self._known_hosts_table)

        hosts_btn_row = QHBoxLayout()
        refresh_hosts_btn = QPushButton("↻ Refresh")
        refresh_hosts_btn.setObjectName("secondary")
        refresh_hosts_btn.clicked.connect(self._load_known_hosts)
        remove_host_btn = QPushButton("🗑 Remove Selected")
        remove_host_btn.setObjectName("danger")
        remove_host_btn.clicked.connect(self._remove_selected_known_host)
        hosts_btn_row.addWidget(refresh_hosts_btn)
        hosts_btn_row.addWidget(remove_host_btn)
        hosts_btn_row.addStretch()
        hosts_layout.addLayout(hosts_btn_row)

        ssh_l.addWidget(hosts_group)
        ssh_l.addStretch()

        tabs.addTab(ssh_tab, "SSH Keys")
        # Populate known-hosts and existing key (if any) immediately
        QTimer.singleShot(0, self._load_known_hosts)
        QTimer.singleShot(0, self._refresh_pubkey_display)

        layout.addWidget(tabs)

    def _on_dest_type_changed(self, idx: int) -> None:
        """Show/hide destination sub-panels based on combo selection."""
        self.dest_local_widget.setVisible(idx == 0)
        self.dest_smb_widget.setVisible(idx == 1)
        self.dest_sftp_widget.setVisible(idx == 2)
        self.dest_ftp_widget.setVisible(idx == 3 or idx == 4)
        self.dest_https_widget.setVisible(idx == 5)
        self.dest_rclone_widget.setVisible(idx == 6)
        self.dest_webdav_widget.setVisible(idx == 7)
        self.dest_gdrive_widget.setVisible(idx == 8)

    def _browse_dest(self):
        path = QFileDialog.getExistingDirectory(self, "Select Backup Destination")
        if path:
            self.dest_input.setText(path)

    def _test_smb(self):
        cfg = {
            "path":   self.dest_smb_path.text().strip(),
            "user":   self.dest_smb_user.text().strip(),
            "pass":   self.dest_smb_pass.text(),
            "domain": self.dest_smb_domain.text().strip(),
        }
        if not cfg["path"]:
            QMessageBox.warning(self, "Missing", "Please enter an SMB path first.")
            return
        try:
            from transport_utils import test_smb_connection
            result = test_smb_connection(cfg)
        except Exception as e:
            QMessageBox.critical(self, "SMB Test Failed", str(e))
            return
        if result.get("ok"):
            QMessageBox.information(self, "SMB  ·  Connected ✓",
                f"Successfully connected to:\n{cfg['path']}")
        else:
            QMessageBox.critical(self, "SMB  ·  Failed",
                f"Could not connect:\n\n{result.get('message', 'Unknown error')}")

    def _test_rclone(self):
        cfg = {
            "remote": self.rclone_remote.text().strip(),
            "path": self.rclone_path.text().strip(),
        }
        if not cfg["remote"]:
            QMessageBox.warning(self, "Missing", "Please enter an rclone remote name first.")
            return
        try:
            from transport_utils import test_rclone_connection
            result = test_rclone_connection(cfg)
        except Exception as e:
            QMessageBox.critical(self, "rclone Test Failed", str(e))
            return
        if result.get("ok"):
            QMessageBox.information(self, "rclone  ·  Connected ✓",
                f"{result.get('message', 'rclone can access the remote')}")
        else:
            QMessageBox.critical(self, "rclone  ·  Failed",
                f"Could not connect:\n\n{result.get('message', 'Unknown error')}")

    # ── rclone wizard helpers ──────────────────────────────────────────────

    def _detect_rclone_remotes(self):
        """Run 'rclone listremotes' and populate the picker combo."""
        import subprocess
        try:
            proc = subprocess.run(
                ["rclone", "listremotes"],
                capture_output=True, text=True, timeout=10,
            )
        except FileNotFoundError:
            QMessageBox.critical(
                self, "rclone not found",
                "rclone is not installed or not on PATH.\n\n"
                "Download it from https://rclone.org/downloads/ and re-try.",
            )
            return
        except subprocess.TimeoutExpired:
            QMessageBox.warning(self, "Timeout", "rclone listremotes timed out after 10 s.")
            return
        except Exception as exc:
            QMessageBox.critical(self, "Error", str(exc))
            return

        remotes = [r.rstrip(":").strip() for r in proc.stdout.splitlines() if r.strip()]
        if not remotes:
            QMessageBox.information(
                self, "No remotes found",
                "rclone reported no configured remotes.\n\n"
                "Click '⚙ rclone config…' to add one.",
            )
            return

        self.rclone_picker_combo.clear()
        self.rclone_picker_combo.addItems(remotes)
        # Pre-select the currently configured remote if it's in the list
        current = self.rclone_remote.text().strip()
        if current in remotes:
            self.rclone_picker_combo.setCurrentText(current)

    def _on_rclone_picker_changed(self, text: str):
        """Fill the Remote name field when the user picks from the combo."""
        if text:
            self.rclone_remote.setText(text)

    def _launch_rclone_config(self):
        """Open a terminal running 'rclone config' so the user can add providers."""
        import subprocess, sys
        try:
            if sys.platform == "win32":
                subprocess.Popen(
                    ["cmd.exe", "/k", "rclone config"],
                    creationflags=subprocess.CREATE_NEW_CONSOLE,
                )
            elif sys.platform == "darwin":
                subprocess.Popen(
                    ["open", "-a", "Terminal", "--args", "rclone", "config"]
                )
            else:
                for term in ("x-terminal-emulator", "gnome-terminal", "konsole", "xterm"):
                    try:
                        subprocess.Popen([term, "-e", "rclone config"])
                        break
                    except FileNotFoundError:
                        continue
        except Exception as exc:
            QMessageBox.warning(
                self, "Could not open terminal",
                f"Please open a terminal manually and run:\n    rclone config\n\nError: {exc}",
            )

    # ── SSH key management helpers ─────────────────────────────────────────

    def _ssh_keys_dir(self):
        return Path.home() / ".backupsys_keys"

    def _ssh_privkey_path(self):
        return self._ssh_keys_dir() / "id_ed25519"

    def _ssh_pubkey_path(self):
        return self._ssh_keys_dir() / "id_ed25519.pub"

    def _refresh_pubkey_display(self):
        pubkey_path = self._ssh_pubkey_path()
        if pubkey_path.exists():
            try:
                self._pubkey_edit.setPlainText(pubkey_path.read_text(encoding="utf-8").strip())
            except Exception:
                pass

    def _generate_ssh_key(self):
        """Generate an Ed25519 SSH key pair into ~/.backupsys_keys/."""
        keys_dir = self._ssh_keys_dir()
        privkey  = self._ssh_privkey_path()
        pubkey   = self._ssh_pubkey_path()

        if privkey.exists():
            ans = QMessageBox.question(
                self, "Key already exists",
                f"A key already exists at:\n{privkey}\n\nOverwrite it?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            )
            if ans != QMessageBox.StandardButton.Yes:
                return

        try:
            import paramiko
        except ImportError:
            QMessageBox.critical(
                self, "paramiko not installed",
                "Install paramiko to use SSH key generation:\n    pip install paramiko",
            )
            return

        try:
            keys_dir.mkdir(parents=True, exist_ok=True)
            key = paramiko.Ed25519Key.generate()
            key.write_private_key_file(str(privkey))
            pub_line = f"ssh-ed25519 {key.get_base64()} backupsys@{socket.gethostname()}"
            pubkey.write_text(pub_line + "\n", encoding="utf-8")
            self._pubkey_edit.setPlainText(pub_line)
            QMessageBox.information(
                self, "Key generated ✓",
                f"Ed25519 key pair created.\n\nPrivate key: {privkey}\nPublic key:  {pubkey}\n\n"
                "Copy the public key and add it to ~/.ssh/authorized_keys on your SFTP server.",
            )
        except Exception as exc:
            QMessageBox.critical(self, "Key generation failed", str(exc))

    def _copy_public_key(self):
        text = self._pubkey_edit.toPlainText().strip()
        if not text:
            QMessageBox.information(self, "Nothing to copy", "Generate a key pair first.")
            return
        QApplication.clipboard().setText(text)
        QMessageBox.information(self, "Copied", "Public key copied to clipboard.")

    def _load_known_hosts(self):
        """Load ~/.backupsys_known_hosts into the table widget."""
        import hashlib, base64
        self._known_hosts_table.setRowCount(0)
        kh_path = Path.home() / ".backupsys_known_hosts"
        if not kh_path.exists():
            return
        try:
            with open(kh_path, "r", encoding="utf-8") as f:
                lines = [l.strip() for l in f if l.strip() and not l.startswith("#")]
        except Exception:
            return

        for line in lines:
            parts = line.split()
            if len(parts) < 3:
                continue
            host_id, key_type, key_b64 = parts[0], parts[1], parts[2]
            try:
                raw = base64.b64decode(key_b64)
                sha256 = base64.b64encode(hashlib.sha256(raw).digest()).decode().rstrip("=")
                fingerprint = f"SHA256:{sha256}"
            except Exception:
                fingerprint = "(invalid)"
            row = self._known_hosts_table.rowCount()
            self._known_hosts_table.insertRow(row)
            self._known_hosts_table.setItem(row, 0, QTableWidgetItem(host_id))
            self._known_hosts_table.setItem(row, 1, QTableWidgetItem(key_type))
            self._known_hosts_table.setItem(row, 2, QTableWidgetItem(fingerprint))

    def _remove_selected_known_host(self):
        """Remove the selected host entry from ~/.backupsys_known_hosts."""
        rows = self._known_hosts_table.selectedItems()
        if not rows:
            QMessageBox.information(self, "No selection", "Select a row to remove.")
            return
        row_idx = self._known_hosts_table.currentRow()
        host_id = self._known_hosts_table.item(row_idx, 0).text()

        ans = QMessageBox.question(
            self, "Remove host?",
            f"Remove trusted fingerprint for:\n  {host_id}\n\n"
            "BackupSys will re-verify on the next SFTP connection.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if ans != QMessageBox.StandardButton.Yes:
            return

        kh_path = Path.home() / ".backupsys_known_hosts"
        try:
            lines = kh_path.read_text(encoding="utf-8").splitlines(keepends=True)
            kept  = [l for l in lines if not l.startswith(host_id + " ") and not l.startswith(host_id + ",")]
            kh_path.write_text("".join(kept), encoding="utf-8")
            self._known_hosts_table.removeRow(row_idx)
        except Exception as exc:
            QMessageBox.critical(self, "Error removing host", str(exc))

    def _browse_smb_network(self):
        def parse_net_view_hosts(output: str) -> list:
            hosts = []
            for line in output.splitlines():
                line = line.strip()
                if line.startswith("\\\\"):
                    token = line.split()[0]
                    if token.startswith("\\\\"):
                        host = token.lstrip("\\")
                        if host and host not in hosts:
                            hosts.append(host)
            return hosts

        def parse_net_view_shares(output: str, host: str) -> list:
            shares = []
            for line in output.splitlines():
                line = line.strip()
                if not line.startswith("\\\\"):
                    continue
                token = line.split()[0]
                if token.startswith("\\\\"):
                    path = token[len(f"\\\\{host}\\"):]
                    if path and path not in shares:
                        shares.append(path)
            return shares

        try:
            proc = subprocess.run(["net", "view"], capture_output=True, text=True, timeout=10)
            if proc.returncode != 0:
                raise RuntimeError(proc.stderr.strip() or "Failed to enumerate network computers")
            hosts = parse_net_view_hosts(proc.stdout)
            if not hosts:
                raise RuntimeError("No hosts")
        except Exception:
            QMessageBox.warning(self, "Browse Network", "No network computers found. Enter the server and share name manually.")
            return

        dlg = QDialog(self)
        dlg.setWindowTitle("Browse Network")
        dlg.setModal(True)
        dlg.setMinimumSize(420, 320)
        layout = QVBoxLayout(dlg)
        layout.addWidget(QLabel("Select a network computer:"))
        host_list = QListWidget()
        host_list.addItems(hosts)
        host_list.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        layout.addWidget(host_list)
        btn_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        btn_box.button(QDialogButtonBox.StandardButton.Ok).setText("Next")
        btn_box.accepted.connect(dlg.accept)
        btn_box.rejected.connect(dlg.reject)
        layout.addWidget(btn_box)
        if dlg.exec() != QDialog.DialogCode.Accepted or not host_list.selectedItems():
            return

        selected_host = host_list.selectedItems()[0].text()
        try:
            proc = subprocess.run(["net", "view", f"\\\\{selected_host}"], capture_output=True, text=True, timeout=10)
            if proc.returncode != 0:
                raise RuntimeError(proc.stderr.strip() or "Failed to enumerate shares")
            shares = parse_net_view_shares(proc.stdout, selected_host)
            if not shares:
                raise RuntimeError("No shares")
        except Exception:
            QMessageBox.warning(self, "Browse Network", "No network computers found. Enter the server and share name manually.")
            return

        dlg2 = QDialog(self)
        dlg2.setWindowTitle("Select SMB Share")
        dlg2.setModal(True)
        dlg2.setMinimumSize(420, 320)
        layout2 = QVBoxLayout(dlg2)
        layout2.addWidget(QLabel(f"Select a share on \\\\{selected_host}:"))
        share_list = QListWidget()
        share_list.addItems(shares)
        share_list.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        layout2.addWidget(share_list)
        btn_box2 = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        btn_box2.accepted.connect(dlg2.accept)
        btn_box2.rejected.connect(dlg2.reject)
        layout2.addWidget(btn_box2)
        if dlg2.exec() != QDialog.DialogCode.Accepted or not share_list.selectedItems():
            return

        selected_share = share_list.selectedItems()[0].text()
        self.dest_smb_path.setText(f"\\\\{selected_host}\\{selected_share}")

    def _test_sftp(self):
        cfg = {
            "host":     self.sftp_host.text().strip(),
            "port":     self.sftp_port.value(),
            "user":     self.sftp_user.text().strip(),
            "pass":     self.sftp_pass.text(),
            "path":     self.sftp_path.text().strip(),
            "keyfile":  self.sftp_keyfile.text().strip(),
            "key_pass": self.sftp_key_pass.text(),
        }
        if not cfg["host"]:
            QMessageBox.warning(self, "Missing", "Please enter an SFTP host first.")
            return
        try:
            from transport_utils import test_sftp_connection
            result = test_sftp_connection(cfg)
        except Exception as e:
            QMessageBox.critical(self, "SFTP Test Failed", str(e))
            return
        if result.get("ok"):
            QMessageBox.information(self, "SFTP  ·  Connected ✓",
                f"Successfully connected to:\n{cfg['host']}:{cfg['port']}")
        else:
            QMessageBox.critical(self, "SFTP  ·  Failed",
                f'Could not connect:\n\n{result.get("error", "Unknown error")}')

    def _test_ftp(self):
        cfg = {
            "host": self.ftp_host.text().strip(),
            "port": self.ftp_port.value(),
            "user": self.ftp_user.text().strip(),
            "pass": self.ftp_pass.text(),
            "path": self.ftp_path.text().strip(),
        }
        if not cfg["host"]:
            QMessageBox.warning(self, "Missing", "Please enter an FTP host first.")
            return
        try:
            from transport_utils import test_ftp_connection
            result = test_ftp_connection(cfg)
        except Exception as e:
            QMessageBox.critical(self, "FTP Test Failed", str(e))
            return
        if result.get("ok"):
            QMessageBox.information(self, "FTP  ·  Connected ✓",
                f"Successfully connected to:\n{cfg['host']}:{cfg['port']}")
        else:
            QMessageBox.critical(self, "FTP  ·  Failed",
                f'Could not connect:\n\n{result.get("error", "Unknown error")}')

    def _test_https(self):
        cfg = {
            "url":        self.https_url.text().strip(),
            "token":      self.https_token.text().strip(),
            "verify_ssl": self.https_verify_ssl.isChecked(),
        }
        if not cfg["url"]:
            QMessageBox.warning(self, "Missing", "Please enter an endpoint URL first.")
            return
        try:
            from transport_utils import test_https_connection
            result = test_https_connection(cfg)
        except Exception as e:
            QMessageBox.critical(self, "HTTPS Test Failed", str(e))
            return
        if result.get("ok"):
            QMessageBox.information(self, "HTTPS  ·  Connected ✓",
                f"Endpoint reachable:\n{cfg['url']}\n\nHTTP status: {result.get('status_code', 'n/a')}")
        else:
            QMessageBox.critical(self, "HTTPS  ·  Failed",
                f'Could not reach endpoint:\n\n{result.get("error", "Unknown error")}')

    def _test_webdav(self):
        cfg = {
            "url":         self.webdav_url.text().strip(),
            "username":    self.webdav_user.text().strip(),
            "password":    self.webdav_pass.text(),
            "webdav_root": self.webdav_root.text().strip(),
            "verify_ssl":  self.webdav_ssl.isChecked(),
        }
        if not cfg["url"]:
            QMessageBox.warning(self, "Missing", "Please enter the WebDAV URL first.")
            return
        try:
            from transport_utils import test_webdav_connection
            result = test_webdav_connection(cfg)
        except Exception as e:
            QMessageBox.critical(self, "WebDAV Test Failed", str(e))
            return
        if result.get("ok"):
            QMessageBox.information(self, "WebDAV  ·  Connected ✓",
                f"WebDAV server reachable:\n{cfg['url']}\n\n"
                "PROPFIND succeeded — credentials and URL are correct.")
        else:
            QMessageBox.critical(self, "WebDAV  ·  Failed",
                f'Could not connect:\n\n{result.get("error", "Unknown error")}\n\n'
                "Tips:\n"
                "• Nextcloud DAV root: /remote.php/dav/files/<USERNAME>/\n"
                "• ownCloud DAV root: /remote.php/webdav/\n"
                "• Plain WebDAV: leave DAV root empty")

    def _trigger_integrity_check_now(self):
        """Delegate integrity check to the MainWindow instance (parent)."""
        main_win = self.parent()
        if main_win is not None and hasattr(main_win, "_trigger_integrity_check_now"):
            main_win._trigger_integrity_check_now()
        else:
            QMessageBox.information(
                self, "Integrity Check",
                "Could not reach the main window to trigger an integrity check.\n"
                "Please use the tray menu or restart the application."
            )

    def _on_interval_unit_changed(self, idx):
        # Keep interval limits reasonable for seconds mode
        if idx == 0:
            self.interval_spin.setRange(1, 1440)
        else:
            self.interval_spin.setRange(1, 60)
        self.interval_spin.setSuffix("")
        # Show a prominent warning when seconds (test-only) mode is active
        self.seconds_warning_label.setVisible(idx == 1)

    def _load_values(self):
        dtype   = self.cfg.get("dest_type", "local")
        idx_map = {
            "local": 0,
            "smb": 1,
            "sftp": 2,
            "ftps": 3,
            "ftp": 4,
            "https": 5,
            "rclone": 6,
            "webdav": 7,
            "cloud":  8,
            "gdrive": 8,
        }
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
        rclone = self.cfg.get("dest_rclone", {})
        self.rclone_remote.setText(rclone.get("remote", ""))
        self.rclone_path.setText(rclone.get("path", "/backups"))
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
        wdv = self.cfg.get("dest_webdav", {})
        self.webdav_url.setText(wdv.get("url", ""))
        self.webdav_user.setText(wdv.get("username", wdv.get("user", "")))
        self.webdav_pass.setText(credential_store.get_webdav_password(wdv))
        self.webdav_path.setText(wdv.get("remote_path", "/backups"))
        self.webdav_root.setText(wdv.get("webdav_root", ""))
        self.webdav_ssl.setChecked(wdv.get("verify_ssl", True))
        # Sync dest_type combo for webdav and cloud/gdrive
        _dt = self.cfg.get("dest_type", "local")
        if _dt == "webdav":
            self.dest_type_combo.setCurrentIndex(7)
        elif _dt == "gdrive":
            self.dest_type_combo.setCurrentIndex(8)
        self.auto_check.setChecked(self.cfg.get("auto_backup", False))
        unit = self.cfg.get("interval_unit", "minutes")
        self.interval_unit.setCurrentIndex(1 if unit == "seconds" else 0)
        self._on_interval_unit_changed(1 if unit == "seconds" else 0)
        self.interval_spin.setValue(self.cfg.get("interval_min", 30))
        # Scheduled backup times
        sched = self.cfg.get("backup_schedule_times", [])
        self.schedule_times_widget.set_entries(sched)
        self.backup_window_start_input.setText(self.cfg.get("backup_window_start", ""))
        self.backup_window_end_input.setText(self.cfg.get("backup_window_end", ""))
        try:
            bw_val = float(self.cfg.get("max_backup_mbps", 0.0))
            self.bw_spin.setValue(bw_val)
        except Exception:
            pass
        # Load bandwidth schedule
        self._populate_bandwidth_schedule(self.cfg.get("bandwidth_schedule", []))
        try:
            self.idle_spin.setValue(int(self.cfg.get("idle_threshold_cpu", 0)))
        except Exception:
            pass
        self.metered_check.setChecked(self.cfg.get("pause_on_metered", False))
        self.battery_check.setChecked(self.cfg.get("pause_on_battery", False))
        self.verify_remote_cb.setChecked(self.cfg.get("verify_remote_uploads", False))
        self.verify_after_cb.setChecked(self.cfg.get("verify_after", False))
        self.retry_check.setChecked(self.cfg.get("auto_retry", False))
        self.retry_delay_spin.setValue(int(self.cfg.get("retry_delay_min", 5)))
        self.integrity_enabled_cb.setChecked(self.cfg.get("integrity_check_enabled", False))
        self.integrity_interval_spin.setValue(int(self.cfg.get("integrity_check_interval_days", 7)))
        self.force_full_global_spin.setValue(int(self.cfg.get("force_full_interval_days", 0)))
        self.startup_check.setChecked(self._is_startup_enabled())
        self.shutdown_check.setChecked(self.cfg.get("auto_shutdown_on_complete", False))
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
        # Load ntfy settings
        nc = self.cfg.get("ntfy_config", {})
        self.ntfy_enabled_check.setChecked(nc.get("enabled", False))
        self.ntfy_notify_success_check.setChecked(nc.get("notify_on_success", False))
        self.ntfy_notify_failure_check.setChecked(nc.get("notify_on_failure", True))
        self.ntfy_server_input.setText(nc.get("server", "https://ntfy.sh"))
        self.ntfy_topic_input.setText(nc.get("topic", ""))
        self.ntfy_token_input.setText(nc.get("token", ""))
        _ntfy_pri = nc.get("priority", "default")
        _ntfy_pri_idx = self.ntfy_priority_combo.findText(_ntfy_pri)
        if _ntfy_pri_idx >= 0:
            self.ntfy_priority_combo.setCurrentIndex(_ntfy_pri_idx)

        # Load Telegram settings
        tc = self.cfg.get("telegram_config", {})
        self.tg_enabled_check.setChecked(tc.get("enabled", False))
        self.tg_notify_success_check.setChecked(tc.get("notify_on_success", False))
        self.tg_notify_failure_check.setChecked(tc.get("notify_on_failure", True))
        self.tg_token_input.setText(tc.get("bot_token", ""))
        self.tg_chat_id_input.setText(str(tc.get("chat_id", "")))

        # Load Pushover settings
        pc = self.cfg.get("pushover_config", {})
        self.po_enabled_check.setChecked(pc.get("enabled", False))
        self.po_notify_success_check.setChecked(pc.get("notify_on_success", False))
        self.po_notify_failure_check.setChecked(pc.get("notify_on_failure", True))
        self.po_user_key_input.setText(pc.get("user_key", ""))
        self.po_api_token_input.setText(pc.get("api_token", ""))
        self.po_device_input.setText(pc.get("device", ""))
        _po_pri = int(pc.get("priority", 0))
        for _i in range(self.po_priority_combo.count()):
            if self.po_priority_combo.itemData(_i) == _po_pri:
                self.po_priority_combo.setCurrentIndex(_i)
                break


    def _refresh_cloud_combo(self):
        """Populate the multi-select watch checklist.
        Pre-checks any watch that already has a GDrive cloud_config assigned,
        and pre-fills the GDrive folder ID from the first assigned watch found.
        """
        self.cloud_watch_list.clear()
        first_gdrive_folder = ""
        for w in self.cfg.get("watches", []):
            item = QListWidgetItem(w.get("name", w["id"]))
            item.setData(Qt.ItemDataRole.UserRole, w["id"])
            item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            # Pre-check if this watch already has GDrive assigned
            configs = w.get("cloud_configs", [])
            if not configs and w.get("cloud_config", {}).get("provider") == "gdrive":
                configs = [w["cloud_config"]]
            has_gdrive = any(c.get("provider") == "gdrive" and c.get("access_token") for c in configs)
            item.setCheckState(Qt.CheckState.Checked if has_gdrive else Qt.CheckState.Unchecked)
            if has_gdrive and not first_gdrive_folder:
                for c in configs:
                    if c.get("provider") == "gdrive":
                        first_gdrive_folder = c.get("folder_id", "")
                        break
            self.cloud_watch_list.addItem(item)
        # Pre-fill folder ID from the first watch that already has GDrive
        if first_gdrive_folder and hasattr(self, "gd_folder_id"):
            self.gd_folder_id.setText(first_gdrive_folder)

        # (Removed chk_gdrive logic; all watches with GDrive are shown in checklist)


    def _on_cloud_watch_changed(self):
        """No-op — replaced by multi-select checklist (_refresh_cloud_combo)."""
        pass

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
        # Search same candidates as _load_dotenv: _internal folder first,
        # then the exe's parent folder (where users usually place .env).
        _env_candidates = [
            Path(__file__).parent / ".env",
            Path(sys.executable).parent / ".env",
            Path(__file__).parent / "_env",
            Path(sys.executable).parent / "_env",
        ]
        env_path = next((p for p in _env_candidates if p.exists()), Path(__file__).parent / ".env")
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
        return self._gdrive_client_id

    @property
    def GDRIVE_CLIENT_SECRET(self):
        return self._gdrive_client_secret

    def _connect_gdrive(self):
        """Open browser for Google OAuth, catch callback on localhost."""
        import urllib.parse, threading, webbrowser
        from http.server import HTTPServer, BaseHTTPRequestHandler

        SCOPES = "https://www.googleapis.com/auth/drive.file"

        client_id = self.GDRIVE_CLIENT_ID
        if not client_id:
            from pathlib import Path
            _env_candidates = [
                Path(__file__).parent / ".env",
                Path(sys.executable).parent / ".env",
                Path(__file__).parent / "_env",
                Path(sys.executable).parent / "_env",
            ]
            env_path = next((p for p in _env_candidates if p.exists()), Path(sys.executable).parent / ".env")
            env_exists = env_path.exists()
            raw = ""
            if env_exists:
                try:
                    raw = env_path.read_text(encoding="utf-8")[:300]
                except Exception as re:
                    raw = f"(read error: {re})"
            # Build a helpful dialog with a "Create .env template" button
            # so the user can get started without manual file editing.
            _dlg = QDialog(self)
            _dlg.setWindowTitle("Google Drive — not configured")
            _dlg.setMinimumWidth(480)
            _vlay = QVBoxLayout(_dlg)
            _vlay.setSpacing(10)

            _msg = QLabel(
                f"<b>GDRIVE_CLIENT_ID not found.</b><br><br>"
                f"Create a <code>.env</code> file at:<br>"
                f"<code>{env_path}</code><br><br>"
                f"with your Google OAuth credentials:<br>"
                f"<code>GDRIVE_CLIENT_ID=your_client_id<br>"
                f"GDRIVE_CLIENT_SECRET=your_client_secret</code><br><br>"
                f"Don't have credentials yet? "
                f"Go to <b>console.cloud.google.com</b> → APIs &amp; Services → Credentials "
                f"→ Create OAuth 2.0 Client ID (Desktop app)."
            )
            _msg.setWordWrap(True)
            _msg.setOpenExternalLinks(True)
            _vlay.addWidget(_msg)

            _btn_row = QHBoxLayout()
            _create_btn = QPushButton("📄  Create .env template")
            _open_btn   = QPushButton("📂  Open folder")
            _close_btn  = QPushButton("Close")
            _close_btn.setDefault(True)
            _btn_row.addWidget(_create_btn)
            _btn_row.addWidget(_open_btn)
            _btn_row.addStretch()
            _btn_row.addWidget(_close_btn)
            _vlay.addLayout(_btn_row)

            def _create_template():
                try:
                    if not env_path.exists():
                        env_path.write_text(
                            "# Google Drive OAuth credentials\n"
                            "# Get these from console.cloud.google.com\n"
                            "GDRIVE_CLIENT_ID=your_client_id\n"
                            "GDRIVE_CLIENT_SECRET=your_client_secret\n",
                            encoding="utf-8"
                        )
                    import subprocess, os
                    subprocess.Popen(["notepad.exe", str(env_path)])
                    _dlg.accept()
                except Exception as _ce:
                    QMessageBox.warning(_dlg, "Error", f"Could not create file:\n{_ce}")

            def _open_folder():
                import subprocess
                subprocess.Popen(["explorer.exe", str(env_path.parent)])

            _create_btn.clicked.connect(_create_template)
            _open_btn.clicked.connect(_open_folder)
            _close_btn.clicked.connect(_dlg.reject)
            _dlg.exec()
            return

        # Use a result dict so the background thread can pass the code back safely.
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

        # Bind to port 0 so the OS picks any free port — eliminates the
        # "address already in use" failure that occurred with the old hardcoded
        # port 8765.  Binding is synchronous, so the actual port is available
        # immediately; only handle_request() blocks (done inside the thread).
        try:
            srv = HTTPServer(("localhost", 0), _Handler)
        except Exception as e:
            QMessageBox.critical(self, "Google Drive",
                f"Could not start local OAuth server:\n{e}\n\n"
                "Try again or check your firewall settings.")
            return

        actual_port = srv.server_address[1]
        REDIRECT_URI = f"http://localhost:{actual_port}/oauth/gdrive"
        # Stored on self so _poll_gdrive_result can pass the exact URI to the
        # token exchange — Google rejects any mismatch between authorize and
        # token calls.
        self._gdrive_redirect_uri = REDIRECT_URI

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

        def _serve():
            try:
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
            self._exchange_gdrive_code(code, self._gdrive_redirect_uri)

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
        # Sync tokens to every watch that uses GDrive so config.json and
        # QSettings stay consistent and backups don't use stale tokens.
        _access  = tokens.get("access_token", "")
        _refresh = tokens.get("refresh_token", "")
        _changed = False
        for w in self.cfg.get("watches", []):
            for cc in w.get("cloud_configs", []):
                if cc.get("provider") == "gdrive":
                    cc["access_token"]  = _access
                    cc["refresh_token"] = _refresh
                    _changed = True
            if w.get("cloud_config", {}).get("provider") == "gdrive":
                w["cloud_config"]["access_token"]  = _access
                w["cloud_config"]["refresh_token"] = _refresh
                _changed = True
        if _changed and BACKEND_AVAILABLE:
            try:
                config_manager.save(self.cfg)
            except Exception:
                pass

        # Show a placeholder label immediately so the UI updates without waiting
        # for the userinfo network call.
        self.gd_status_lbl.setText("✓ Connected")
        self.gd_status_lbl.setObjectName("status_ok")
        self.gd_status_lbl.style().unpolish(self.gd_status_lbl)
        self.gd_status_lbl.style().polish(self.gd_status_lbl)
        self.gd_connect_btn.setVisible(False)
        self.gd_test_btn.setVisible(True)
        self.gd_disconnect_btn.setVisible(True)
        QMessageBox.information(self, "Google Drive", "Google Drive connected successfully!")

        # Fetch the connected account email in the background so the status
        # label updates to "✓ Connected as you@gmail.com" without blocking the UI.
        import threading, urllib.request as _ur, json as _json
        def _fetch_email():
            try:
                req  = _ur.Request(
                    f"https://www.googleapis.com/oauth2/v1/userinfo"
                    f"?access_token={_access}"
                )
                info  = _json.loads(_ur.urlopen(req, timeout=10).read())
                email = info.get("email", "").strip()
                if email:
                    _s = QSettings(SETTINGS_ORG, SETTINGS_APP)
                    _s.setValue("gdrive_email", email)
                    QTimer.singleShot(0, lambda e=email: self._set_gdrive_status_label(e))
            except Exception:
                pass  # Non-fatal — label stays "✓ Connected"
        threading.Thread(target=_fetch_email, daemon=True).start()

        # Run a silent write+delete test ~1.5 s after the panel settles so the
        # user sees a warning immediately if the Drive scope was not granted,
        # rather than discovering it only when the first real backup fails.
        QTimer.singleShot(1500, lambda: self._auto_test_gdrive_write(_access))

    def _auto_test_gdrive_write(self, access_token: str):
        """Create and immediately delete a tiny Drive file to verify write access.

        Called automatically ~1.5 s after OAuth completes.  Silent on success
        (the user already saw the "Connected" dialog and email label); shows a
        warning on failure so the user learns about scope / permission problems
        before the first real backup attempts and fails silently.

        Uses raw urllib so no extra packages are required beyond what the rest
        of the OAuth flow already uses.
        """
        import threading, urllib.request as _ur, json as _json

        def _run():
            try:
                # ── Step 1: upload a tiny multipart file ─────────────────────
                boundary = "bksys_write_test"
                body = (
                    f"--{boundary}\r\n"
                    f"Content-Type: application/json; charset=UTF-8\r\n\r\n"
                    + _json.dumps({"name": "backupsys_connection_test.txt"})
                    + f"\r\n--{boundary}\r\n"
                    f"Content-Type: text/plain\r\n\r\n"
                    f"backupsys_ok\r\n"
                    f"--{boundary}--"
                ).encode()
                req = _ur.Request(
                    "https://www.googleapis.com/upload/drive/v3/files"
                    "?uploadType=multipart",
                    data=body,
                    headers={
                        "Authorization": f"Bearer {access_token}",
                        "Content-Type":  f"multipart/related; boundary={boundary}",
                    },
                    method="POST",
                )
                resp     = _ur.urlopen(req, timeout=15)
                file_info = _json.loads(resp.read())
                file_id   = file_info.get("id", "")

                if not file_id:
                    raise ValueError(f"Upload returned no file ID — response: {file_info}")

                # ── Step 2: delete the test file ─────────────────────────────
                del_req = _ur.Request(
                    f"https://www.googleapis.com/drive/v3/files/{file_id}",
                    method="DELETE",
                    headers={"Authorization": f"Bearer {access_token}"},
                )
                _ur.urlopen(del_req, timeout=10)

                # Success — log only; no dialog (user already dismissed "Connected" dialog)
                logger.info("✓ Google Drive write test passed — upload and delete OK")

            except Exception as exc:
                err_str = str(exc)
                logger.warning(f"⚠ Google Drive write test failed: {err_str}")
                QTimer.singleShot(0, lambda: QMessageBox.warning(
                    self, "Google Drive — Write Test Failed",
                    f"Connected but the upload test failed:\n\n{err_str}\n\n"
                    "Possible causes:\n"
                    "  • The drive.file scope was not granted\n"
                    "  • The account lacks Drive storage space\n"
                    "  • A network error occurred\n\n"
                    "Backups may fail. Try disconnecting and reconnecting Google Drive."
                ))

        threading.Thread(target=_run, daemon=True).start()

    def _set_gdrive_status_label(self, email: str = ""):
        """Update the GDrive status label with the connected account email.

        Called on the main thread after a successful userinfo fetch.
        Also used by _check_cloud_connections() on panel open to restore the
        saved email without making a network call.
        """
        if email:
            self.gd_status_lbl.setText(f"✓ Connected as {email}")
        else:
            self.gd_status_lbl.setText("✓ Connected")
        self.gd_status_lbl.setObjectName("status_ok")
        self.gd_status_lbl.style().unpolish(self.gd_status_lbl)
        self.gd_status_lbl.style().polish(self.gd_status_lbl)

    def _gdrive_connect_failed(self, err=""):
        self.gd_connect_btn.setText("Connect Google Drive")
        self.gd_connect_btn.setEnabled(True)
        QMessageBox.critical(self, "Google Drive", f"Connection failed: {err}")

    def _disconnect_gdrive(self):
        s = QSettings(SETTINGS_ORG, SETTINGS_APP)
        s.remove("gdrive_access_token")
        s.remove("gdrive_refresh_token")
        s.remove("gdrive_email")
        self.gd_status_lbl.setText("Not connected")
        self.gd_status_lbl.setObjectName("status_err")
        self.gd_status_lbl.style().unpolish(self.gd_status_lbl)
        self.gd_status_lbl.style().polish(self.gd_status_lbl)
        self.gd_quota_lbl.setVisible(False)
        self.gd_quota_lbl.setText("")
        self.gd_connect_btn.setVisible(True)
        self.gd_connect_btn.setText("Connect Google Drive")
        self.gd_connect_btn.setEnabled(True)
        self.gd_test_btn.setVisible(False)
        self.gd_disconnect_btn.setVisible(False)

    def _test_gdrive(self):
        """Test the saved Google Drive token and show a result dialog.

        Runs the network call in a daemon thread so the UI stays responsive,
        then pops a QMessageBox on the main thread with the outcome.
        A successful test also silently updates the stored access token if a
        refresh was needed (delegated to test_gdrive_connection).
        """
        import threading
        from transport_utils import test_gdrive_connection

        self.gd_test_btn.setEnabled(False)
        self.gd_test_btn.setText("Testing…")

        s = QSettings(SETTINGS_ORG, SETTINGS_APP)
        cloud_config = {
            "access_token":  s.value("gdrive_access_token",  ""),
            "refresh_token": s.value("gdrive_refresh_token", ""),
            "client_id":     self.GDRIVE_CLIENT_ID,
            "client_secret": self.GDRIVE_CLIENT_SECRET,
        }

        def _run():
            result = test_gdrive_connection(cloud_config)
            QTimer.singleShot(0, lambda: _done(result))

        def _done(result):
            self.gd_test_btn.setEnabled(True)
            self.gd_test_btn.setText("Test Connection")
            if result["ok"]:
                QMessageBox.information(
                    self, "Google Drive — Connection OK",
                    f"✓ {result['detail']}"
                )
                saved_email = QSettings(SETTINGS_ORG, SETTINGS_APP).value("gdrive_email", "")
                self._set_gdrive_status_label(saved_email)
            else:
                QMessageBox.warning(
                    self, "Google Drive — Connection Failed",
                    f"✗ {result['detail']}"
                )
                self.gd_status_lbl.setText("⚠ Token issue — test again or reconnect")
                self.gd_status_lbl.setObjectName("status_err")
                self.gd_status_lbl.style().unpolish(self.gd_status_lbl)
                self.gd_status_lbl.style().polish(self.gd_status_lbl)

        threading.Thread(target=_run, daemon=True).start()

    def _check_cloud_connections(self):
        """Update connect/disconnect state, validate tokens, and fetch Drive quota."""
        s = QSettings(SETTINGS_ORG, SETTINGS_APP)
        if s.value("gdrive_access_token", ""):
            # Restore the saved email so the label reads "✓ Connected as you@gmail.com"
            # without making a network call every time the panel is opened.
            saved_email = s.value("gdrive_email", "")
            self._set_gdrive_status_label(saved_email)
            self.gd_connect_btn.setVisible(False)
            self.gd_test_btn.setVisible(True)
            self.gd_disconnect_btn.setVisible(True)
            # Fetch Drive quota in background so the panel opens instantly
            self._fetch_gdrive_quota_async()
        # Validate tokens in background and warn if expired
        self._validate_cloud_tokens()

    def _fetch_gdrive_quota_async(self):
        """Fetch Google Drive storage quota in a background thread and update the UI label."""
        import threading
        try:
            from transport_utils import get_gdrive_quota
        except ImportError:
            return

        s = QSettings(SETTINGS_ORG, SETTINGS_APP)
        cloud_config = {
            "access_token":  s.value("gdrive_access_token",  ""),
            "refresh_token": s.value("gdrive_refresh_token", ""),
            "client_id":     self.GDRIVE_CLIENT_ID,
            "client_secret": self.GDRIVE_CLIENT_SECRET,
        }

        def _run():
            quota = get_gdrive_quota(cloud_config)
            QTimer.singleShot(0, lambda: _apply(quota))

        def _apply(quota: dict):
            if not quota.get("ok"):
                return  # silently skip — don't surface quota errors in the panel
            usage      = quota.get("usage", -1)
            limit      = quota.get("limit", -1)
            drive_used = quota.get("drive_used", -1)

            def _fmt(n):
                if n < 0:
                    return "?"
                for unit in ("B", "KB", "MB", "GB", "TB"):
                    if n < 1024:
                        return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
                    n /= 1024
                return f"{n:.1f} PB"

            if limit > 0:
                pct  = min(int(usage / limit * 100), 100)
                free = limit - usage
                text = (
                    f"Storage: {_fmt(usage)} used of {_fmt(limit)} ({pct}% full)"
                    f" · {_fmt(free)} free"
                )
                if drive_used >= 0:
                    text += f"  ·  Drive files: {_fmt(drive_used)}"
            else:
                # Unlimited plan (e.g. Workspace) — show usage only
                text = f"Storage used: {_fmt(usage)}"
                if drive_used >= 0:
                    text += f"  ·  Drive files: {_fmt(drive_used)}"

            self.gd_quota_lbl.setText(text)
            self.gd_quota_lbl.setVisible(True)

        threading.Thread(target=_run, daemon=True).start()

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
                from PyQt6.QtCore import QTimer
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
                from PyQt6.QtCore import QTimer
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
                self.gd_test_btn.setVisible(False)
            # 2. Tray notification
            if hasattr(self, "_tray"):
                self._tray.showMessage(
                    f"⚠ {name}  · Reconnect Required",
                    f"Your {name} token has expired.\n"
                    f"Open Settings >Cloud tab >Reconnect to continue cloud backups.",
                    QSystemTrayIcon.MessageIcon.Warning, 8000
                )
            # 3. Log
            if hasattr(self, "log_text"):
                self._append_log(
                    f"⚠ {name} token expired  · go to Settings >Cloud tab to reconnect"
                )

    def _pick_gdrive_folder(self):
        """Fetch the user's Drive folders and let them pick one from a dialog."""
        from PyQt6.QtWidgets import QDialog, QVBoxLayout, QListWidget, QListWidgetItem, QDialogButtonBox, QLabel

        # Load current cloud config to get tokens
        cfg = config_manager.load()
        cloud_cfg = None
        for w in cfg.get("watches", []):
            cc = w.get("cloud_config", {})
            if cc.get("provider") == "gdrive":
                cloud_cfg = cc
                break
        if cloud_cfg is None:
            cloud_cfg = cfg.get("_cloud_default_gdrive", {})

        if not cloud_cfg or not cloud_cfg.get("access_token"):
            QMessageBox.warning(self, "Not Connected",
                "Connect your Google Drive account first (click 'Connect Google Drive').")
            return

        try:
            from googleapiclient.discovery import build
            import google.oauth2.credentials as _gc
            _creds = _gc.Credentials(
                token         = cloud_cfg.get("access_token"),
                refresh_token = cloud_cfg.get("refresh_token"),
                client_id     = cloud_cfg.get("client_id"),
                client_secret = cloud_cfg.get("client_secret"),
                token_uri     = "https://oauth2.googleapis.com/token",
            )
            service = build("drive", "v3", credentials=_creds)
            result  = service.files().list(
                q="mimeType='application/vnd.google-apps.folder' and trashed=false",
                fields="files(id, name)",
                orderBy="name",
                pageSize=100,
            ).execute()
            folders = result.get("files", [])
        except Exception as e:
            QMessageBox.critical(self, "Drive Error",
                f"Could not list Drive folders:\n{e}\n\nTry reconnecting your account.")
            return

        dlg = QDialog(self)
        dlg.setWindowTitle("Select Google Drive Folder")
        dlg.setMinimumSize(420, 380)
        vlay = QVBoxLayout(dlg)
        vlay.addWidget(QLabel("Choose the Drive folder where backups will be stored:"))

        lst = QListWidget()
        root_item = QListWidgetItem("📁  My Drive (root)")
        root_item.setData(0x100, "")           # folder_id = ""
        root_item.setData(0x101, "My Drive (root)")
        lst.addItem(root_item)
        for f in folders:
            item = QListWidgetItem(f"📁  {f['name']}")
            item.setData(0x100, f["id"])
            item.setData(0x101, f["name"])
            lst.addItem(item)
        lst.setCurrentRow(0)
        vlay.addWidget(lst)

        btns = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        btns.accepted.connect(dlg.accept)
        btns.rejected.connect(dlg.reject)
        vlay.addWidget(btns)

        if dlg.exec() == QDialog.DialogCode.Accepted:
            sel = lst.currentItem()
            if sel:
                fid   = sel.data(0x100)
                fname = sel.data(0x101)
                self.gd_folder_id.setText(fid)
                self.gd_folder_id.setToolTip(f"Folder: {fname}  (ID: {fid or 'root'})")
                QMessageBox.information(self, "Folder Selected",
                    f"Backups will be stored in:\n📁 {fname}"
                    + (f"\n(ID: {fid})" if fid else ""))

    def _save_cloud(self):
        """Save the current cloud assignment to ALL checked watches at once."""
        # Collect checked watches from the checklist
        checked_wids = []
        for i in range(self.cloud_watch_list.count()):
            item = self.cloud_watch_list.item(i)
            if item.checkState() == Qt.CheckState.Checked:
                checked_wids.append(item.data(Qt.ItemDataRole.UserRole))

        if not checked_wids:
            QMessageBox.warning(self, "No Watch Selected",
                "Please check at least one watch in the list above.")
            return

        s      = QSettings(SETTINGS_ORG, SETTINGS_APP)
        # Always use GDrive for all checked watches
        token = s.value("gdrive_access_token", "")
        if not token:
            QMessageBox.warning(self, "Not Connected", "Please connect Google Drive first.")
            return
        cloud_config = {
            "provider":      "gdrive",
            "access_token":  token,
            "refresh_token": s.value("gdrive_refresh_token", ""),
            "client_id":     self.GDRIVE_CLIENT_ID,
            "client_secret": self.GDRIVE_CLIENT_SECRET,
            "folder_id":     self.gd_folder_id.text().strip(),
        }

        # Apply cloud_config to every checked watch
        saved_names = []
        for wid in checked_wids:
            watch = next((w for w in self.cfg.get("watches", []) if w["id"] == wid), None)
            if not watch:
                continue
            watch["cloud_config"] = cloud_config.copy()
            watch["cloud_configs"] = [watch["cloud_config"]]  # backward compatibility
            saved_names.append(watch.get("name", wid))

        # Also clear cloud config from any UNCHECKED watches (user unticked them)
        for i in range(self.cloud_watch_list.count()):
            item = self.cloud_watch_list.item(i)
            if item.checkState() == Qt.CheckState.Unchecked:
                wid = item.data(Qt.ItemDataRole.UserRole)
                watch = next((w for w in self.cfg.get("watches", []) if w["id"] == wid), None)
                if watch:
                    watch["cloud_configs"] = []
                    watch["cloud_config"]  = {}

        try:
            config_manager.save(self.cfg)
            if cloud_configs:
                providers = ", ".join(c["provider"].upper() for c in cloud_configs)
                names_str = ", ".join(saved_names)
                QMessageBox.information(self, "Saved",
                    f"Cloud assignment saved to {len(saved_names)} watch(es).\n\n"
                    f"Watches:   {names_str}\n"
                    f"Providers: {providers}")
            else:
                QMessageBox.information(self, "Saved",
                    "Cloud assignment cleared for all watches.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to save: {e}")

    def _add_watch(self):
        dlg = AddWatchDialog(self, cfg=self.cfg)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            try:
                v        = dlg.get_values()
                src_type = v.get("source_type", "local")
                config_manager.add_watch(
                    self.cfg,
                    v["name"],
                    v["path"],
                    watch_type=src_type,
                    interval_min=v["interval_min"],
                    smb_cfg={
                        "user":   v["smb_user"],
                        "pass":   v["smb_pass"],
                        "domain": v["smb_domain"],
                    } if v.get("is_smb") else {},
                )
                w = config_manager.get_watch_by_path(self.cfg, v["path"])
                if w:
                    extra_meta: dict = {
                        "compression":      v.get("compression", False),
                        "sync_mode":        True,
                        "destination":      v.get("destination", "") or None,
                        # Advanced fields set via "More Options…"
                        "schedule_times":   v.get("schedule_times", []),
                        "max_file_size_mb": v.get("max_file_size_mb", 0),
                        "exclude_patterns": v.get("exclude_patterns", []),
                        "encrypt_key":      v.get("encrypt_key", ""),
                        "pre_backup_cmd":   v.get("pre_backup_cmd", ""),
                        "post_backup_cmd":  v.get("post_backup_cmd", ""),
                    }
                    # Persist WebDAV source credentials on the watch dict so
                    # BackupWorker can pass them to run_backup(source_webdav_cfg=…)
                    if v.get("is_webdav"):
                        extra_meta["webdav_cfg"] = {
                            "url":      v["path"],
                            "username": v.get("webdav_user", ""),
                            "password": v.get("webdav_pass", ""),
                        }
                    # Persist SFTP/FTPS source credentials
                    if v.get("is_sftp"):
                        extra_meta["sftp_cfg"] = {
                            "host":       v.get("sftp_host", ""),
                            "port":       v.get("sftp_port", 22),
                            "username":   v.get("sftp_user", ""),
                            "password":   v.get("sftp_pass", ""),
                            "key_path":   v.get("sftp_key", ""),
                        }
                    # Persist FTP source credentials
                    if v.get("is_ftp"):
                        extra_meta["ftp_cfg"] = {
                            "host":     v.get("ftp_host", ""),
                            "port":     v.get("ftp_port", 21),
                            "username": v.get("ftp_user", ""),
                            "password": v.get("ftp_pass", ""),
                            "use_tls":  False,
                        }
                    config_manager.update_watch_meta(self.cfg, w["id"], **extra_meta)
                self._refresh_watch_table()
                self._refresh_cloud_combo()
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
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel
        )
        if reply == QMessageBox.StandardButton.Yes:
            config_manager.remove_watch(self.cfg, wid)
            self._refresh_watch_table()
            self._refresh_cloud_combo()
            self.watches_changed.emit()

    def _edit_watch(self):
        btn = self.sender()
        wid = btn.property("watch_id")
        watch = next((w for w in self.cfg.get("watches", []) if w["id"] == wid), None)
        if not watch:
            return

        dlg = AddWatchDialog(self, cfg=self.cfg, edit_watch_id=wid)
        dlg.setWindowTitle("Edit Watched Folder")
        dlg._submit_btn.setText("Save Changes")

        # Pre-fill existing values
        dlg.name_input.setText(watch.get("name", ""))
        is_smb = watch.get("type", "local") == "smb" or watch.get("path", "").startswith("\\\\") or watch.get("path", "").startswith("//")
        if is_smb:
            dlg.source_type.setCurrentIndex(1)
            dlg.smb_path_input.setText(watch.get("path", ""))
            smb_cfg = watch.get("smb_cfg", {})
            dlg.smb_user.setText(smb_cfg.get("user", ""))
            # Load password from credential store if available, else from config
            try:
                from credential_store import get_smb_password
                _stored_pass = get_smb_password(watch.get("path", ""))
                dlg.smb_pass.setText(_stored_pass or smb_cfg.get("pass", ""))
            except Exception:
                dlg.smb_pass.setText(smb_cfg.get("pass", ""))
            dlg.smb_domain.setText(smb_cfg.get("domain", ""))
        else:
            dlg.source_type.setCurrentIndex(0)
            dlg.path_input.setText(watch.get("path", ""))
        dlg.interval_spin.setValue(watch.get("interval_min", 0))
        # Pre-fill destination if set on this watch
        _watch_dest = watch.get("destination", "")
        if _watch_dest:
            dlg.dest_input.setText(_watch_dest)
        # Set compression combo box based on existing value
        current_compression = watch.get("compression", False)
        if current_compression is True or current_compression == 6:
            dlg.compress_combo.setCurrentIndex(2)  # Balanced
        elif current_compression == 1:
            dlg.compress_combo.setCurrentIndex(1)  # Fast
        elif current_compression == 9:
            dlg.compress_combo.setCurrentIndex(3)  # Best
        else:
            dlg.compress_combo.setCurrentIndex(0)  # Off

        if dlg.exec() == QDialog.DialogCode.Accepted:
            v = dlg.get_values()
            try:
                config_manager.update_watch_meta(
                    self.cfg, wid,
                    name=v["name"],
                    interval_min=v["interval_min"],
                    compression=v.get("compression", False),
                    sync_mode=v.get("sync_mode", False),
                    destination=v.get("destination", "") or None,
                )
                # Update path and SMB credentials directly (not in update_watch_meta)
                for w in self.cfg.get("watches", []):
                    if w["id"] == wid:
                        w["path"] = v["path"]   # get_values() always returns "path" key
                        w["type"] = "smb" if v["is_smb"] else "local"
                        if v["is_smb"]:
                            w["smb_cfg"] = {
                                "user":   v["smb_user"],
                                "pass":   v["smb_pass"],
                                "domain": v["smb_domain"],
                            }
                        break
                config_manager.save(self.cfg)
                self._refresh_watch_table()
                self._refresh_cloud_combo()
                self.watches_changed.emit()
                # ── Success feedback ──────────────────────────────────────
                msg = QMessageBox(self)
                msg.setWindowTitle("Saved")
                msg.setText(f"✅  <b>{v['name']}</b> settings saved successfully.")
                msg.setIcon(QMessageBox.Icon.Information)
                msg.exec()
            except Exception as e:
                QMessageBox.critical(self, "Error", str(e))

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

            # ── Edit button (col 11) ────────────────────────────────────────
            edit_btn = QPushButton("✏ Edit")
            edit_btn.setObjectName("secondary")
            edit_btn.setFixedHeight(26)
            edit_btn.setProperty("watch_id", w["id"])
            edit_btn.clicked.connect(self._edit_watch)
            self.watch_table.setCellWidget(row, 11, edit_btn)

            # ── Delete button (col 12) ──────────────────────────────────────
            del_btn = QPushButton("🗑 Remove")
            del_btn.setObjectName("danger")
            del_btn.setFixedHeight(26)
            del_btn.setProperty("watch_id", w["id"])
            del_btn.clicked.connect(self._remove_watch)
            self.watch_table.setCellWidget(row, 12, del_btn)

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
            # Use the local _send_email_notification function with test subject/body
            test_cfg = {"email_config": ec}
            _send_email_notification(
                test_cfg,
                "BackupSys — test email",
                "This is a test email from BackupSys.\n\nEmail notifications are working correctly."
            )
            QMessageBox.information(
                self, "Test Email Sent",
                f"Test email sent to {to}.\nCheck your inbox (and spam folder)."
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

    def _save_ntfy_settings(self):
        nc = self.cfg.setdefault("ntfy_config", {})
        nc["enabled"]           = self.ntfy_enabled_check.isChecked()
        nc["server"]            = self.ntfy_server_input.text().strip() or "https://ntfy.sh"
        nc["topic"]             = self.ntfy_topic_input.text().strip()
        nc["token"]             = self.ntfy_token_input.text().strip()
        nc["priority"]          = self.ntfy_priority_combo.currentText()
        nc["notify_on_success"] = self.ntfy_notify_success_check.isChecked()
        nc["notify_on_failure"] = self.ntfy_notify_failure_check.isChecked()
        try:
            config_manager.save(self.cfg)
            QMessageBox.information(self, "Saved", "ntfy push settings saved.")
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

    def _test_ntfy(self):
        """Send a test push via ntfy to verify topic/token."""
        topic = self.ntfy_topic_input.text().strip()
        if not topic:
            QMessageBox.warning(self, "Missing", "Please enter an ntfy Topic first.")
            return
        nc = {
            "enabled": True,
            "server":  self.ntfy_server_input.text().strip() or "https://ntfy.sh",
            "topic":   topic,
            "token":   self.ntfy_token_input.text().strip(),
        }
        try:
            from notification_utils import test_ntfy as _test_ntfy_fn
            res = _test_ntfy_fn(nc)
            if res["ok"]:
                QMessageBox.information(
                    self, "Test Push Sent",
                    f"Test notification sent to topic '{topic}'.\n"
                    "Check the ntfy app on your phone."
                )
            else:
                QMessageBox.critical(self, "Test Failed", res.get("error", "Unknown error"))
        except ImportError:
            QMessageBox.warning(self, "Unavailable", "notification_utils.py not found.")
        except Exception as e:
            QMessageBox.critical(self, "Test Failed", str(e))

    # ── Telegram settings ──────────────────────────────────────────────────────

    def _save_telegram_settings(self):
        tc = self.cfg.setdefault("telegram_config", {})
        tc["enabled"]           = self.tg_enabled_check.isChecked()
        tc["bot_token"]         = self.tg_token_input.text().strip()
        tc["chat_id"]           = self.tg_chat_id_input.text().strip()
        tc["notify_on_success"] = self.tg_notify_success_check.isChecked()
        tc["notify_on_failure"] = self.tg_notify_failure_check.isChecked()
        tc["parse_mode"]        = "HTML"
        try:
            config_manager.save(self.cfg)
            QMessageBox.information(self, "Saved", "Telegram notification settings saved.")
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

    def _test_telegram(self):
        """Send a test message via Telegram to verify bot_token and chat_id."""
        token   = self.tg_token_input.text().strip()
        chat_id = self.tg_chat_id_input.text().strip()
        if not token:
            QMessageBox.warning(self, "Missing", "Please enter a Bot Token first.")
            return
        if not chat_id:
            QMessageBox.warning(self, "Missing", "Please enter a Chat ID first.")
            return
        tc = {"bot_token": token, "chat_id": chat_id, "parse_mode": "HTML"}
        try:
            from notification_utils import test_telegram as _test_tg
            res = _test_tg(tc)
            if res["ok"]:
                QMessageBox.information(
                    self, "Message Sent",
                    f"Test message sent to chat_id '{chat_id}'.\n"
                    "Check your Telegram."
                )
            else:
                QMessageBox.critical(self, "Test Failed", res.get("error", "Unknown error"))
        except ImportError:
            QMessageBox.warning(self, "Unavailable", "notification_utils.py not found.")
        except Exception as e:
            QMessageBox.critical(self, "Test Failed", str(e))

    # ── Pushover settings ──────────────────────────────────────────────────────

    def _save_pushover_settings(self):
        pc = self.cfg.setdefault("pushover_config", {})
        pc["enabled"]           = self.po_enabled_check.isChecked()
        pc["user_key"]          = self.po_user_key_input.text().strip()
        pc["api_token"]         = self.po_api_token_input.text().strip()
        pc["device"]            = self.po_device_input.text().strip()
        pc["priority"]          = self.po_priority_combo.currentData()
        pc["notify_on_success"] = self.po_notify_success_check.isChecked()
        pc["notify_on_failure"] = self.po_notify_failure_check.isChecked()
        try:
            config_manager.save(self.cfg)
            QMessageBox.information(self, "Saved", "Pushover notification settings saved.")
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

    def _test_pushover(self):
        """Send a test push via Pushover to verify user_key and api_token."""
        user_key  = self.po_user_key_input.text().strip()
        api_token = self.po_api_token_input.text().strip()
        if not user_key:
            QMessageBox.warning(self, "Missing", "Please enter your Pushover User Key first.")
            return
        if not api_token:
            QMessageBox.warning(self, "Missing", "Please enter your Pushover API Token first.")
            return
        pc = {"user_key": user_key, "api_token": api_token,
              "device": self.po_device_input.text().strip()}
        try:
            from notification_utils import test_pushover as _test_po
            res = _test_po(pc)
            if res["ok"]:
                QMessageBox.information(
                    self, "Test Push Sent",
                    "Test notification sent via Pushover.\n"
                    "Check your device."
                )
            else:
                QMessageBox.critical(self, "Test Failed", res.get("error", "Unknown error"))
        except ImportError:
            QMessageBox.warning(self, "Unavailable", "notification_utils.py not found.")
        except Exception as e:
            QMessageBox.critical(self, "Test Failed", str(e))

    # ── Logs tab ───────────────────────────────────────────────────────────────

    def _on_tab_changed_log_poll(self, index: int):
        """Start/stop the log-poll timer based on whether the Logs tab is active."""
        # The Logs tab is the last tab; find it by title to be robust
        logs_tab_index = self._tabs.count() - 1
        for i in range(self._tabs.count()):
            if self._tabs.tabText(i) == "Logs":
                logs_tab_index = i
                break
        if index == logs_tab_index:
            self._load_log_tab()
            self._log_poll_timer.start()
        else:
            self._log_poll_timer.stop()

    def _load_log_tab(self):
        """Load (or reload) the full log file into the viewer."""
        try:
            if not self._log_file_path.exists():
                self._log_viewer.setPlainText(f"Log file not found:\n{self._log_file_path}")
                self._log_raw_lines = []
                self._log_last_mtime = 0.0
                return
            mtime = self._log_file_path.stat().st_mtime
            self._log_last_mtime = mtime
            content = self._log_file_path.read_text(encoding="utf-8", errors="replace")
            self._log_raw_lines = content.splitlines()
            self._apply_log_filter(self._log_filter_input.text())
        except Exception as e:
            self._log_viewer.setPlainText(f"Error reading log: {e}")

    def _poll_log_file(self):
        """Called every 3 s — only reload if the file has changed on disk."""
        try:
            if not self._log_file_path.exists():
                return
            mtime = self._log_file_path.stat().st_mtime
            if mtime != self._log_last_mtime:
                self._load_log_tab()
        except Exception:
            pass

    def _apply_log_filter(self, text: str):
        """Filter displayed lines by the text in the filter box (case-insensitive)."""
        needle = text.strip().lower()
        if needle:
            visible = [ln for ln in self._log_raw_lines if needle in ln.lower()]
        else:
            visible = self._log_raw_lines

        self._log_viewer.setPlainText("\n".join(visible))

        if self._log_tail_check.isChecked():
            cursor = self._log_viewer.textCursor()
            # PyQt5 < 5.15 uses QTextCursor.End; newer PyQt5/PyQt6 uses
            # QTextCursor.MoveOperation.End — try both for compatibility.
            try:
                _end = QTextCursor.MoveOperation.End
            except AttributeError:
                _end = QTextCursor.End
            cursor.movePosition(_end)
            self._log_viewer.setTextCursor(cursor)

    def _clear_log_file(self):
        """Prompt and then truncate the log file."""
        reply = QMessageBox.question(
            self, "Clear Log",
            "This will permanently delete all log entries.\nContinue?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
            return
        try:
            self._log_file_path.write_text("", encoding="utf-8")
            self._log_raw_lines = []
            self._log_viewer.clear()
            QMessageBox.information(self, "Cleared", "Log file cleared.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Could not clear log file:\n{e}")


        dlg = PasswordDialog(self, mode="set")
        dlg.exec()

    def _apply_theme(self):
        """Apply the selected theme immediately and persist the choice."""
        choice = "light" if self.theme_light.isChecked() else "dark"
        s = QSettings(SETTINGS_ORG, SETTINGS_APP)
        s.setValue("theme", choice)
        sheet = LIGHT_STYLE if choice == "light" else DARK_STYLE
        from PyQt6.QtWidgets import QApplication as _QApp
        _QApp.instance().setStyleSheet(sheet)
        _QApp.instance().setProperty("theme", choice)

    def _export_config(self):
        """Export a redacted copy of config.json that the user can save anywhere."""
        import copy, json as _json
        from PyQt6.QtWidgets import QFileDialog

        try:
            raw = copy.deepcopy(config_manager.load())
        except Exception as e:
            QMessageBox.critical(self, "Export Failed", f"Could not load config: {e}")
            return

        # ── Redact all known secret fields ────────────────────────────────
        _SECRET_KEYS = {
            "password", "pass", "token", "encrypt_key",
            "access_token", "refresh_token", "client_secret",
            "api_key", "webhook_url",
        }

        def _redact(obj):
            if isinstance(obj, dict):
                return {
                    k: ("*** REDACTED ***" if k.lower() in _SECRET_KEYS else _redact(v))
                    for k, v in obj.items()
                }
            if isinstance(obj, list):
                return [_redact(i) for i in obj]
            return obj

        redacted = _redact(raw)
        redacted["_export_note"] = (
            "Passwords and secrets have been removed. "
            "Re-enter them after importing on a new machine."
        )

        out_path, _ = QFileDialog.getSaveFileName(
            self, "Export BackupSys Config",
            "backupsys_config_export.json",
            "JSON files (*.json)"
        )
        if not out_path:
            return

        try:
            with open(out_path, "w", encoding="utf-8") as fh:
                _json.dump(redacted, fh, indent=2)
            QMessageBox.information(
                self, "Config Exported",
                f"Configuration exported to:\n{out_path}\n\n"
                "Passwords have been redacted — re-enter them after importing."
            )
        except Exception as e:
            QMessageBox.critical(self, "Export Failed", f"Could not write file: {e}")

    def _import_config(self):
        """Import a previously exported config file and merge it into the current config."""
        import json as _json
        from PyQt6.QtWidgets import QFileDialog

        in_path, _ = QFileDialog.getOpenFileName(
            self, "Import BackupSys Config",
            "",
            "JSON files (*.json)"
        )
        if not in_path:
            return

        try:
            with open(in_path, "r", encoding="utf-8") as fh:
                imported = _json.load(fh)
        except _json.JSONDecodeError as e:
            QMessageBox.critical(self, "Import Failed", f"Invalid JSON file: {e}")
            return
        except Exception as e:
            QMessageBox.critical(self, "Import Failed", f"Could not read file: {e}")
            return

        # ── Validate required keys ──────────────────────────────────────────
        if not isinstance(imported, dict):
            QMessageBox.critical(
                self, "Import Failed",
                "Config file must be a JSON object (not an array or scalar)."
            )
            return

        if "watches" not in imported:
            QMessageBox.critical(
                self, "Import Failed",
                "Config file is missing required 'watches' key."
            )
            return

        # ── Merge: Update self.cfg with imported values ──────────────────────
        try:
            # Merge all top-level keys from imported config into self.cfg
            self.cfg.update(imported)
            config_manager.save(self.cfg)
            QMessageBox.information(
                self, "Config Imported",
                "Config imported. Restart may be required for all changes to take effect."
            )
        except Exception as e:
            QMessageBox.critical(self, "Import Failed", f"Could not merge config: {e}")

    def _save_general(self):
        self.cfg["dest_smb"] = {
            "path": self.dest_smb_path.text().strip(),
            "user": self.dest_smb_user.text().strip(),
            "pass": self.dest_smb_pass.text(),
            "domain": self.dest_smb_domain.text().strip(),
        }
        # For SMB, the backup destination IS the UNC path — not the local dest_input.
        # Using a local path when dest_type is smb causes WinError 21 (device not ready)
        # because the engine tries to create the backup folder on a non-existent local drive.
        if self.cfg["dest_type"] == "smb":
            self.cfg["destination"] = self.cfg["dest_smb"]["path"]
        else:
            self.cfg["destination"] = self.dest_input.text().strip()
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
        self.cfg["dest_rclone"] = {
            "remote": self.rclone_remote.text().strip(),
            "path": self.rclone_path.text().strip() or "/backups",
        }
        self.cfg["dest_webdav"] = {
            "url":         self.webdav_url.text().strip(),
            "username":    self.webdav_user.text().strip(),
            "password":    self.webdav_pass.text(),
            "remote_path": self.webdav_path.text().strip() or "/backups",
            "webdav_root": self.webdav_root.text().strip(),
            "verify_ssl":  self.webdav_ssl.isChecked(),
        }
        # credential_store is imported inside the same try/except ImportError block
        # that sets BACKEND_AVAILABLE. Guard against NameError when running without
        # the full backend installed (e.g. UI-only stub / missing dependencies).
        if BACKEND_AVAILABLE:
            credential_store.set_sftp_password(self.cfg["dest_sftp"], self.sftp_pass.text())
            credential_store.set_ftp_password(self.cfg["dest_ftp"], self.ftp_pass.text())
            credential_store.set_smb_password(self.cfg["dest_smb"], self.dest_smb_pass.text())
            credential_store.set_webdav_password(self.cfg["dest_webdav"], self.webdav_pass.text())
        idx = self.dest_type_combo.currentIndex()
        dest_map = {
            0: "local",
            1: "smb",
            2: "sftp",
            3: "ftps",
            4: "ftp",
            5: "https",
            6: "rclone",
            7: "webdav",
            8: "gdrive",
        }
        self.cfg["dest_type"] = dest_map.get(idx, "local")
        self.cfg["auto_backup"] = self.auto_check.isChecked()
        self.cfg["interval_unit"] = "seconds" if self.interval_unit.currentIndex() == 1 else "minutes"
        self.cfg["interval_min"] = self.interval_spin.value()
        self.cfg["low_disk_threshold_gb"] = self.disk_alert_spin.value()
        self.cfg["backup_schedule_times"] = self.schedule_times_widget.get_entries()
        self.cfg["backup_window_start"]   = self.backup_window_start_input.text().strip()
        self.cfg["backup_window_end"]     = self.backup_window_end_input.text().strip()
        self.cfg["max_backup_mbps"]    = self.bw_spin.value()
        self.cfg["bandwidth_schedule"]  = self._get_bandwidth_schedule()
        self.cfg["idle_threshold_cpu"] = self.idle_spin.value()
        self.cfg["pause_on_metered"]   = self.metered_check.isChecked()
        self.cfg["pause_on_battery"]   = self.battery_check.isChecked()
        self.cfg["verify_remote_uploads"] = self.verify_remote_cb.isChecked()
        self.cfg["verify_after"]          = self.verify_after_cb.isChecked()
        self.cfg["auto_retry"]         = self.retry_check.isChecked()
        self.cfg["retry_delay_min"] = self.retry_delay_spin.value()
        self.cfg["integrity_check_enabled"]       = self.integrity_enabled_cb.isChecked()
        self.cfg["integrity_check_interval_days"] = self.integrity_interval_spin.value()
        self.cfg["force_full_interval_days"]      = self.force_full_global_spin.value()
        self.cfg["auto_shutdown_on_complete"]     = self.shutdown_check.isChecked()
        try:
            config_manager.save(self.cfg)
            QMessageBox.information(self, "Saved", "Settings saved.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to save settings: {e}")

    def _change_password(self):
        """Open the password-change dialog from within the Admin panel."""
        dlg = PasswordDialog(self, mode="set")
        dlg.exec()

    def _toggle_startup(self, state):
        if state == Qt.CheckState.Checked:
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

    def _populate_bandwidth_schedule(self, schedule: list):
        """Load bandwidth schedule rules into the table."""
        self.bw_table.setRowCount(0)
        for rule in schedule:
            self._add_bw_rule_with_data(
                rule.get("start", "09:00"),
                rule.get("end", "17:00"),
                rule.get("max_mbps", 10.0)
            )

    def _add_bw_rule_with_data(self, start: str, end: str, max_mbps: float):
        """Add a bandwidth schedule rule with specific data."""
        row_count = self.bw_table.rowCount()
        self.bw_table.insertRow(row_count)
        
        start_edit = QTimeEdit()
        start_edit.setDisplayFormat("HH:mm")
        try:
            h, m = map(int, start.split(":"))
            start_edit.setTime(QTime(h, m))
        except:
            start_edit.setTime(QTime(9, 0))
        self.bw_table.setCellWidget(row_count, 0, start_edit)
        
        end_edit = QTimeEdit()
        end_edit.setDisplayFormat("HH:mm")
        try:
            h, m = map(int, end.split(":"))
            end_edit.setTime(QTime(h, m))
        except:
            end_edit.setTime(QTime(17, 0))
        self.bw_table.setCellWidget(row_count, 1, end_edit)
        
        mbps_spin = QDoubleSpinBox()
        mbps_spin.setRange(0.0, 1000.0)
        mbps_spin.setDecimals(1)
        mbps_spin.setValue(max_mbps)
        self.bw_table.setCellWidget(row_count, 2, mbps_spin)

    def _add_bw_rule(self):
        """Add a new bandwidth schedule rule with defaults."""
        self._add_bw_rule_with_data("09:00", "17:00", 10.0)

    def _remove_bw_rule(self):
        """Remove the selected bandwidth schedule rule."""
        current_row = self.bw_table.currentRow()
        if current_row >= 0:
            self.bw_table.removeRow(current_row)

    def _get_bandwidth_schedule(self) -> list:
        """Extract bandwidth schedule from table."""
        schedule = []
        for row in range(self.bw_table.rowCount()):
            start_widget = self.bw_table.cellWidget(row, 0)
            end_widget = self.bw_table.cellWidget(row, 1)
            mbps_widget = self.bw_table.cellWidget(row, 2)
            
            if start_widget and end_widget and mbps_widget:
                start_time = start_widget.time().toString("HH:mm")
                end_time = end_widget.time().toString("HH:mm")
                max_mbps = mbps_widget.value()
                schedule.append({
                    "start": start_time,
                    "end": end_time,
                    "max_mbps": max_mbps
                })
        return schedule


class StorageChartWidget(QWidget):
    def __init__(self, watch: dict, parent=None):
        super().__init__(parent)
        self.watch = watch
        self._data = []  # list of (timestamp, size_mb)
        self._load_data()

    def _load_data(self):
        """Load last 30 backup sizes from manifests."""
        try:
            dest = self.watch.get("destination", "")
            if not dest:
                from config_manager import load_config
                cfg = load_config()
                dest = cfg.get("destination", "")
            if not dest:
                return
            dest_path = Path(dest)
            if not dest_path.exists():
                return
            safe_name = "".join(c if c.isalnum() else "_" for c in self.watch.get("name", ""))
            # List backup dirs
            backup_dirs = [d for d in dest_path.iterdir() if d.is_dir() and safe_name in d.name]
            backup_dirs.sort(key=lambda d: d.name, reverse=True)  # newest first
            for bd in backup_dirs[:30]:
                manifest_path = bd / "MANIFEST.json"
                if manifest_path.exists():
                    try:
                        with open(manifest_path, 'r', encoding='utf-8') as f:
                            manifest = json.load(f)
                        ts = manifest.get("timestamp", "")
                        size_bytes = manifest.get("total_size_bytes", 0)
                        size_mb = size_bytes / (1024 * 1024)
                        self._data.append((ts, size_mb))
                    except Exception:
                        pass
            self._data.reverse()  # oldest first for chart
        except Exception:
            pass

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        rect = self.rect()
        if not self._data:
            painter.setPen(QColor("#6b7280"))
            painter.drawText(rect, Qt.AlignmentFlag.AlignCenter, "No backup data")
            return
        # Draw bars
        bar_color = QColor("#2563eb")
        label_color = QColor("#e8eaf0")
        painter.setPen(label_color)
        painter.setFont(QFont("Arial", 8))
        max_size = max(s for _, s in self._data) if self._data else 1
        bar_width = rect.width() / len(self._data) if self._data else 1
        for i, (ts, size) in enumerate(self._data):
            bar_height = (size / max_size) * (rect.height() - 20) if max_size > 0 else 0
            bar_rect = QRectF(i * bar_width, rect.height() - bar_height - 15, bar_width - 2, bar_height)
            painter.fillRect(bar_rect, bar_color)
            # Date label
            try:
                date = datetime.fromisoformat(ts).strftime("%m/%d")
                painter.drawText(int(i * bar_width), rect.height() - 5, date)
            except:
                pass
        # Y axis label
        painter.drawText(5, 10, f"Size (MB, max {max_size:.0f})")


# ══════════════════════════════════════════════════════════════════════════════
# ── Watch Card Widget ──────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

class WatchCard(QFrame):
    backup_requested          = pyqtSignal(dict)
    full_backup_requested     = pyqtSignal(dict)  # watch  · force full backup
    dry_run_requested         = pyqtSignal(dict)  # watch  · preview only
    validate_requested        = pyqtSignal(dict)
    restore_requested         = pyqtSignal(dict)
    restore_to_original_requested = pyqtSignal(dict)
    pause_requested           = pyqtSignal(str, bool)   # watch_id, paused
    pause_backup_requested    = pyqtSignal(str)         # watch_id  · pause running backup
    resume_backup_requested   = pyqtSignal(str)         # watch_id  · resume paused backup
    cancel_requested          = pyqtSignal(str)         # watch_id
    open_backup_requested     = pyqtSignal(str)      # watch_id
    watch_settings_requested  = pyqtSignal(dict)     # watch  · open advanced settings

    def __init__(self, watch: dict, dest_type: str = "local", parent=None):
        super().__init__(parent)
        self.watch        = watch
        self.dest_type    = dest_type
        self._changes     = []   # list of recent change entries
        self._expanded    = False
        self._backup_paused = False  # track if backup is currently paused
        self.setObjectName("card")
        self.setMinimumHeight(90)
        # Rolling speed window for ETA: list of (timestamp, bytes_done) pairs.
        # Using recent samples (last 8 s) avoids the "ETA explodes after a
        # throttle pause" problem caused by using total-elapsed as the divisor.
        self._speed_window: list = []
        self._SPEED_WIN_SEC: int = 8
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

        if self.watch.get("sync_mode", False):
            sync_badge = QLabel("SYNC")
            sync_badge.setStyleSheet(
                "background:#0ea5e9; color:#fff; font-size:9px; font-weight:700;"
                "padding:2px 6px; border-radius:3px;"
            )
            sync_badge.setToolTip("Sync mode: files are copied directly into the destination folder, no versioned subfolders.")
            name_row.addWidget(sync_badge)

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

        # Inline last-backup status indicator (✔ / ✘ / —)
        _last_status = self.watch.get("last_backup_status", "")
        if _last_status == "success":
            _status_icon = "✔"
            _status_color = "#22c55e"   # green
        elif _last_status == "failed":
            _status_icon = "✘"
            _status_color = "#ef4444"   # red
        else:
            _status_icon = "—"
            _status_color = "#6b7280"   # grey

        meta_row = QHBoxLayout()
        meta_row.setSpacing(6)
        meta_row.setContentsMargins(0, 0, 0, 0)

        self.meta_lbl = QLabel(f"{lb_text}   ·   {count} backup(s)")
        self.meta_lbl.setStyleSheet("color: #4b5563; font-size: 11px;")
        meta_row.addWidget(self.meta_lbl)

        self._status_dot = QLabel(_status_icon)
        self._status_dot.setStyleSheet(
            f"color: {_status_color}; font-size: 13px; font-weight: 700;"
        )
        self._status_dot.setToolTip(
            f"Last backup status: {_last_status or 'unknown'}"
        )
        meta_row.addWidget(self._status_dot)
        meta_row.addStretch()
        info_layout.addLayout(meta_row)

        self.next_lbl = QLabel("")
        self.next_lbl.setStyleSheet("color: #374151; font-size: 10px;")
        self.next_lbl.setVisible(True)
        info_layout.addWidget(self.next_lbl)


        tags = self.watch.get("tags", [])
        if tags:
            tags_row = QHBoxLayout()
            tags_row.setSpacing(4)
            tags_row.setContentsMargins(0, 2, 0, 0)
            for tag in tags[:8]:   # cap at 8 badges to avoid overflow
                badge = QLabel(tag)
                badge.setStyleSheet(
                    "font-size: 10px; padding: 1px 6px; border-radius: 8px;"
                    "background: #2e3a55; color: #7aa2d4; border: 1px solid #3d5278;"
                )
                tags_row.addWidget(badge)
            tags_row.addStretch()
            info_layout.addLayout(tags_row)

        layout.addLayout(info_layout, stretch=1)

        self.status_widget = QWidget()
        # Fix: lock the width so the indeterminate progress bar animation
        # cannot push or shift the buttons during scanning/backing up.
        self.status_widget.setFixedWidth(170)
        status_layout = QVBoxLayout(self.status_widget)
        status_layout.setSpacing(4)
        status_layout.setContentsMargins(0, 0, 0, 0)

        self.status_lbl = QLabel("▶ Watching")
        self.status_lbl.setObjectName("status_ok")
        self.status_lbl.setAlignment(Qt.AlignmentFlag.AlignRight)
        status_layout.addWidget(self.status_lbl)

        self.progress_bar = QProgressBar()
        self.progress_bar.setFixedWidth(160)   # fixed — never resizes during marquee animation
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(False)
        status_layout.addWidget(self.progress_bar)

        self.file_lbl = QLabel("")
        self.file_lbl.setObjectName("file_scan_lbl")
        self.file_lbl.setStyleSheet(
            "font-size: 10px; color: #8b95a8; padding: 0 2px;"
        )
        self.file_lbl.setMaximumWidth(300)
        self.file_lbl.setVisible(False)
        self.file_lbl.setWordWrap(False)
        status_layout.addWidget(self.file_lbl)

        # ── Windows-Explorer-style details row ────────────────────────────
        # Shows: "8% · 70.1 GB left · 71,307 files left · 4.3 MB/s"
        self.details_lbl = QLabel("")
        self.details_lbl.setStyleSheet(
            "font-size: 10px; color: #6b7280; padding: 0 2px;"
        )
        self.details_lbl.setMaximumWidth(300)
        self.details_lbl.setVisible(False)
        self.details_lbl.setWordWrap(False)
        status_layout.addWidget(self.details_lbl)

        self.backup_btn = QPushButton("Backup Now")
        self.backup_btn.setFixedWidth(160)
        self.backup_btn.clicked.connect(lambda: self.backup_requested.emit(self.watch))
        status_layout.addWidget(self.backup_btn)

        # ── "More ▾" dropdown for secondary actions ────────────────────────
        self._more_btn = QToolButton()
        self._more_btn.setText("More ▾")
        self._more_btn.setFixedWidth(160)
        self._more_btn.setObjectName("secondary")
        self._more_btn.setPopupMode(
            QToolButton.ToolButtonPopupMode.InstantPopup
            if hasattr(QToolButton, "ToolButtonPopupMode")
            else QToolButton.InstantPopup
        )
        self._more_btn.setStyleSheet(
            "QToolButton { text-align:center; padding:4px 8px; }"
            "QToolButton::menu-indicator { image: none; }"
        )

        more_menu = QMenu(self._more_btn)

        paused_now = self.watch.get("paused", False)
        self._pause_action = more_menu.addAction("⏸ Pause" if not paused_now else "▶ Resume")
        self._pause_action.triggered.connect(self._toggle_pause)

        more_menu.addSeparator()

        act_full = more_menu.addAction("⟳ Force Full Backup")
        act_full.triggered.connect(lambda: self.full_backup_requested.emit(self.watch))

        act_dry = more_menu.addAction("🔍 Dry Run")
        act_dry.setToolTip("Preview what would be backed up without copying any files")
        act_dry.triggered.connect(lambda: self.dry_run_requested.emit(self.watch))

        act_validate = more_menu.addAction("▶ Validate")
        act_validate.triggered.connect(lambda: self.validate_requested.emit(self.watch))

        more_menu.addSeparator()

        act_restore = more_menu.addAction("↩ Restore")
        act_restore.triggered.connect(lambda: self.restore_requested.emit(self.watch))

        act_restore_orig = more_menu.addAction("🏠 Restore to Original")
        act_restore_orig.setToolTip("Restore files to their original source location")
        act_restore_orig.triggered.connect(lambda: self.restore_to_original_requested.emit(self.watch))

        if self.dest_type == "local":
            more_menu.addSeparator()
            act_open = more_menu.addAction("📂 Open Folder")
            act_open.triggered.connect(lambda: self.open_backup_requested.emit(self.watch["id"]))

        more_menu.addSeparator()
        act_settings = more_menu.addAction("⚙ Watch Settings…")
        act_settings.setToolTip("Edit encryption, exclusions, hooks and more for this watch")
        act_settings.triggered.connect(lambda: self.watch_settings_requested.emit(self.watch))

        # Store action references so external code can enable/disable them
        self._act_full_backup   = act_full
        self._act_dry_run       = act_dry
        self._act_validate      = act_validate
        self._act_restore       = act_restore
        self._act_restore_orig  = act_restore_orig

        self._more_btn.setMenu(more_menu)
        status_layout.addWidget(self._more_btn)

        # Stub QPushButtons kept for API compatibility — their setEnabled is
        # overridden to also toggle the corresponding menu action.
        class _StubBtn(QPushButton):
            def __init__(self, action=None):
                super().__init__()
                self._action = action
                self.setVisible(False)
            def setEnabled(self, v):
                super().setEnabled(v)
                if self._action:
                    self._action.setEnabled(v)

        self.full_backup_btn      = _StubBtn(act_full)
        self.dry_run_btn          = _StubBtn(act_dry)
        self.pause_btn            = _StubBtn(None)
        self.validate_btn         = _StubBtn(act_validate)
        self.restore_btn          = _StubBtn(act_restore)
        self.restore_original_btn = _StubBtn(act_restore_orig)
        self.open_backup_btn      = _StubBtn(None)

        # ── Pause/Resume and Cancel buttons (shown during backup) ─────
        self.pause_backup_btn = QPushButton("⏸ Pause")
        self.pause_backup_btn.setObjectName("secondary")
        self.pause_backup_btn.setFixedWidth(160)
        self.pause_backup_btn.setVisible(False)
        self.pause_backup_btn.clicked.connect(self._on_pause_backup_clicked)
        status_layout.addWidget(self.pause_backup_btn)

        self.cancel_btn = QPushButton("▶ Cancel")
        self.cancel_btn.setObjectName("danger")
        self.cancel_btn.setFixedWidth(160)
        self.cancel_btn.setVisible(False)
        self.cancel_btn.clicked.connect(lambda: self.cancel_requested.emit(self.watch["id"]))
        status_layout.addWidget(self.cancel_btn)

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

        # Storage trend chart
        chart_title = QLabel("STORAGE TREND")
        chart_title.setStyleSheet("color:#374151; font-size:10px; font-weight:700; letter-spacing:0.08em;")
        cp_layout.addWidget(chart_title)

        self.storage_chart = StorageChartWidget(self.watch)
        self.storage_chart.setFixedHeight(100)
        cp_layout.addWidget(self.storage_chart)

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

    def _on_pause_backup_clicked(self):
        """Toggle pause/resume for a running backup."""
        if self._backup_paused:
            # Resume the backup
            self._backup_paused = False
            self.pause_backup_btn.setText("⏸ Pause")
            self.resume_backup_requested.emit(self.watch["id"])
        else:
            # Pause the backup
            self._backup_paused = True
            self.pause_backup_btn.setText("▶ Resume")
            self.pause_backup_requested.emit(self.watch["id"])

    def set_backing_up(self, active: bool):
        self.backup_btn.setEnabled(not active)
        self.backup_btn.setText("Backing up…" if active else "Backup Now")
        # Disable/enable secondary actions in the More menu during backup
        if hasattr(self, "_more_btn"):
            self._more_btn.setEnabled(not active)
        self.pause_backup_btn.setVisible(active)
        self.cancel_btn.setVisible(active)
        self.progress_bar.setVisible(active)
        self.file_lbl.setVisible(active)
        self.details_lbl.setVisible(active)
        if active:
            self._speed_window.clear()   # fresh window for each backup run
            self._backup_paused = False   # track pause state during this backup run
            self.pause_backup_btn.setText("⏸ Pause")  # reset to Pause
            # FIX: always reset cancel button so a previous "Cancelling…" state
            # doesn't leave it permanently disabled on the next backup run.
            self.cancel_btn.setEnabled(True)
            self.cancel_btn.setText("▶ Cancel")
            self.status_lbl.setText("▶ Backing up…")
            self.status_lbl.setObjectName("status_warn")
            # Start in indeterminate (scanning) mode
            self.progress_bar.setRange(0, 0)
            self.file_lbl.setText("Scanning…")
            self.details_lbl.setText("")
        else:
            self.progress_bar.setRange(0, 100)
            self.file_lbl.setText("")
            self.details_lbl.setText("")
            self.status_lbl.setText("▶ Watching")
            self.status_lbl.setObjectName("status_ok")
        self.status_lbl.style().unpolish(self.status_lbl)
        self.status_lbl.style().polish(self.status_lbl)

    def set_progress(self, current: int, total: int, fname: str = "", elapsed: float = 0.0, is_scanning: bool = False, bytes_done: int = 0, total_bytes: int = 0):
        short_name = fname.replace("\\", "/").split("/")[-1] if fname else ""

        if is_scanning:
            # ── Scanning phase ────────────────────────────────────────────
            if self.progress_bar.maximum() != 0:
                self.progress_bar.setRange(0, 0)   # ensure marquee during scan

            if total > 0 and current > 0 and elapsed > 2.0:
                # Repeat backup — use previous snapshot count as estimated total
                pct = min(int(current / total * 100), 99)
                self.progress_bar.setRange(0, 100)
                self.progress_bar.setValue(pct)
                rate = current / elapsed
                eta_s = (total - current) / rate
                eta_text = _fmt_eta(eta_s)
                eta_part = f"  ·  ETA {eta_text}" if eta_text else ""
                lbl = f"Scanning ({current}/{total}){eta_part}"
            elif elapsed > 1.0:
                # First-time backup — no estimate, show elapsed + count
                elapsed_text = _fmt_duration(elapsed)
                count_part = f"  {current} files" if current > 0 else ""
                lbl = f"Scanning{count_part}  ·  {elapsed_text} elapsed"
            else:
                lbl = f"Scanning: {short_name}" if short_name else "Scanning…"

            self.file_lbl.setText(lbl)

        else:
            # ── Copying / Uploading phase ──────────────────────────────────
            # Use byte-based progress when available (accurate for large files).
            # Falls back to file-count progress for edge cases (0-byte files, etc.)
            # When current==1 and total==1 the signal is coming from the remote
            # upload leg (_upload_progress in backup_engine) — label as "Uploading".
            _is_uploading = (current == 1 and total == 1 and bytes_done > 0 and total_bytes > 0)
            use_bytes = total_bytes > 0 and bytes_done >= 0

            if total == 0 and not use_bytes:
                self.progress_bar.setRange(0, 0)
                self.file_lbl.setText(f"Copying: {short_name}" if short_name else "Copying…")
            else:
                if self.progress_bar.maximum() == 0:
                    self.progress_bar.setRange(0, 100)

                if use_bytes:
                    pct = int(min(bytes_done / total_bytes * 100, 99)) if bytes_done < total_bytes else 100
                elif total > 0:
                    pct = int(current / total * 100)
                else:
                    pct = 0
                self.progress_bar.setValue(pct)

                byte_rate  = 0.0
                eta_part   = ""
                speed_text = ""
                if elapsed > 1.0:
                    if use_bytes and bytes_done > 0 and bytes_done < total_bytes:
                        # ── Rolling-window speed (last _SPEED_WIN_SEC seconds) ──
                        # This stays accurate even after throttle pauses, because
                        # we measure only recent activity — not total-since-start.
                        import time as _t
                        now = _t.time()
                        self._speed_window.append((now, bytes_done))
                        cutoff = now - self._SPEED_WIN_SEC
                        while len(self._speed_window) > 2 and self._speed_window[0][0] < cutoff:
                            self._speed_window.pop(0)

                        if len(self._speed_window) >= 2:
                            dt = self._speed_window[-1][0] - self._speed_window[0][0]
                            db = self._speed_window[-1][1] - self._speed_window[0][1]
                            if dt > 0.1 and db > 0:
                                byte_rate = db / dt

                        if byte_rate > 0:
                            eta_s      = (total_bytes - bytes_done) / byte_rate
                            mbps       = byte_rate / (1024 * 1024)
                            eta_text   = _fmt_eta(eta_s)
                            speed_text = f"{mbps:.1f} MB/s"
                            if eta_text:
                                eta_part = f"  ·  ETA {eta_text}  ·  {speed_text}"
                            else:
                                eta_part = f"  ·  {speed_text}"
                    elif not use_bytes and current > 0 and total > 0 and current < total:
                        # Fallback: file-count ETA
                        rate     = current / elapsed
                        eta_s    = (total - current) / rate
                        eta_text = _fmt_eta(eta_s)
                        if eta_text:
                            eta_part = f"  ·  ETA {eta_text}"

                # ── Windows-Explorer-style details row ────────────────────
                # "8% complete  ·  70.1 GB left  ·  71,307 files left  ·  4.3 MB/s"
                details_parts = []
                if pct > 0:
                    details_parts.append(f"{pct}% complete")
                if use_bytes and total_bytes > 0 and bytes_done < total_bytes:
                    bytes_left = total_bytes - bytes_done
                    if bytes_left >= 1024 ** 3:
                        details_parts.append(f"{bytes_left / (1024**3):.1f} GB left")
                    elif bytes_left >= 1024 ** 2:
                        details_parts.append(f"{bytes_left / (1024**2):.0f} MB left")
                    else:
                        details_parts.append(f"{bytes_left / 1024:.0f} KB left")
                if not _is_uploading and total > 0 and current < total:
                    files_left = total - current
                    details_parts.append(f"{files_left:,} files left")
                if speed_text:
                    details_parts.append(speed_text)
                self.details_lbl.setText("  ·  ".join(details_parts))

                if short_name:
                    # Truncate long filenames so the ETA is never clipped off
                    max_name     = 35
                    display_name = (short_name[:max_name] + "…") if len(short_name) > max_name else short_name
                    verb         = "Uploading" if _is_uploading else "Copying"
                    # Show ETA first so it's always visible even on narrow cards
                    if eta_part:
                        self.file_lbl.setText(f"{eta_part.strip()}  ·  {display_name}")
                    else:
                        self.file_lbl.setText(f"{verb}: {display_name}")

    def set_done(self, success: bool, duration_s: float = 0.0):
        self.set_backing_up(False)
        dur_str = _fmt_duration(duration_s)
        if success:
            done_text = f"▶  Done  ·  {dur_str}" if dur_str else "▶  Done"
            self.status_lbl.setText(done_text)
            self.status_lbl.setObjectName("status_ok")
            self.clear_changes()
        else:
            self.status_lbl.setText("▶  Failed")
            self.status_lbl.setObjectName("status_err")
        self.status_lbl.style().unpolish(self.status_lbl)
        self.status_lbl.style().polish(self.status_lbl)

    def refresh_next_backup_lbl(self, cfg: dict):
        """Update the 'Next backup in …' countdown label.  Called every 30 s by MainWindow."""
        if not hasattr(self, "next_lbl"):
            return

        # If watch opts out of auto-backup, show "Manual only"
        if self.watch.get("skip_auto_backup", False):
            self.next_lbl.setText("Manual only")
            self.next_lbl.setVisible(True)
            return

        # Resolve schedule: per-watch schedule_times takes priority over global
        w_sched         = self.watch.get("schedule_times", [])
        global_sched    = cfg.get("backup_schedule_times", [])
        schedule_times  = w_sched if w_sched else global_sched

        if schedule_times:
            now = datetime.now()
            now_secs_day = now.hour * 3600 + now.minute * 60 + now.second
            today_bit = 1 << now.weekday()   # Mon=0 → bit 1, Sun=6 → bit 64
            next_time = None
            min_diff = float('inf')

            for entry in schedule_times:
                if isinstance(entry, str):
                    sched_str = entry
                    days_mask = 127
                else:
                    sched_str = entry.get("time", "")
                    days_mask = int(entry.get("days", 127))
                try:
                    sh, sm = int(sched_str[:2]), int(sched_str[3:5])
                    sched_sec = sh * 3600 + sm * 60
                    # If today is not in the mask, fast-forward to next valid day
                    if days_mask & today_bit:
                        if sched_sec > now_secs_day:
                            diff = sched_sec - now_secs_day
                        else:
                            diff = (24 * 3600) - now_secs_day + sched_sec
                    else:
                        # Count forward days until a valid weekday
                        extra_days = next(
                            (d for d in range(1, 8) if days_mask & (1 << ((now.weekday() + d) % 7))),
                            1
                        )
                        diff = extra_days * 86400 - now_secs_day + sched_sec
                    if diff < min_diff:
                        min_diff = diff
                        next_time = sched_str
                except Exception:
                    continue

            if next_time:
                label_prefix = "Next (watch):" if w_sched else "Next:"
                self.next_lbl.setText(f"{label_prefix} {next_time}")
                self.next_lbl.setVisible(True)
                return

        # Otherwise, show countdown based on interval (existing logic)
        # Hide when auto-backup is globally off or watch is paused
        if not cfg.get("auto_backup", False) or self.watch.get("paused", False):
            self.next_lbl.setVisible(False)
            return

        lb = self.watch.get("last_backup", "")
        if not lb:
            self.next_lbl.setText("Auto-backup pending")
            self.next_lbl.setVisible(True)
            return

        # Effective interval in seconds (per-watch overrides global)
        w_interval_min = self.watch.get("interval_min", 0)
        if w_interval_min:
            interval_secs = w_interval_min * 60
        else:
            interval_val  = cfg.get("interval_min", 30)
            interval_unit = cfg.get("interval_unit", "minutes")
            interval_secs = interval_val if interval_unit == "seconds" else interval_val * 60

        try:
            last_dt   = datetime.fromisoformat(lb)
            remaining = (last_dt.timestamp() + interval_secs) - datetime.now().timestamp()
            if remaining <= 0:
                text = "Backup due soon"
            elif remaining < 60:
                text = f"Next: in {int(remaining)}s"
            elif remaining < 3600:
                text = f"Next: in {int(remaining / 60)} min"
            else:
                h = int(remaining / 3600)
                m = int((remaining % 3600) / 60)
                if h == 1:
                    text = f"Next: in {h}h {m:02d}m"
                else:
                    text = f"Next: in {h}h {m:02d}m"
            self.next_lbl.setText(text)
            self.next_lbl.setVisible(True)
        except Exception:
            self.next_lbl.setVisible(False)

    def _toggle_pause(self):
        """Toggle paused state and emit signal for the main window to persist."""
        paused = not self.watch.get("paused", False)
        self.watch["paused"] = paused
        if hasattr(self, "_pause_action"):
            self._pause_action.setText("▶ Resume" if paused else "⏸ Pause")
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

        # Refresh the status dot
        if hasattr(self, "_status_dot"):
            _ls = watch.get("last_backup_status", "")
            if _ls == "success":
                self._status_dot.setText("✔")
                self._status_dot.setStyleSheet("color: #22c55e; font-size: 13px; font-weight: 700;")
            elif _ls == "failed":
                self._status_dot.setText("✘")
                self._status_dot.setStyleSheet("color: #ef4444; font-size: 13px; font-weight: 700;")
            else:
                self._status_dot.setText("—")
                self._status_dot.setStyleSheet("color: #6b7280; font-size: 13px; font-weight: 700;")
            self._status_dot.setToolTip(f"Last backup status: {_ls or 'unknown'}")

        # Refresh pause button label
        paused = watch.get("paused", False)
        if hasattr(self, "_pause_action"):
            self._pause_action.setText("▶ Resume" if paused else "⏸ Pause")

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
        self._backup_history: list   = []   # backup run history records
        self._history_save_counter   = 0    # throttle disk saves
        self._history_window         = None
        self._user_cancelled_watches: set = set()  # watches cancelled by user — suppress auto-restart
        self._skipped_notified: dict = {}   # watch_id > {'window': bool, 'idle': bool}

        # Load persisted history from previous sessions
        if BACKEND_AVAILABLE:
            try:
                self._history_log = config_manager.load_history()
            except Exception:
                pass
            try:
                self._backup_history = config_manager.load_backup_history()
            except Exception:
                pass

        self._load_config()
        self._build_ui()
        self._start_watchers()
        self._start_auto_timer()
        # ── Integrity scheduler — weekly background validation ─────────────────
        if BACKEND_AVAILABLE:
            self._integrity_scheduler = IntegrityScheduler(self)
            self._integrity_scheduler.watch_result.connect(self._on_integrity_result)
            self._integrity_scheduler.run_finished.connect(self._on_integrity_run_finished)
            self._integrity_scheduler.disk_space_warning.connect(self._on_disk_space_warning)
            self._integrity_scheduler.start()
        QTimer.singleShot(3000, self._process_startup_queue)
        QTimer.singleShot(5000, self._validate_cloud_tokens)
        QTimer.singleShot(10000, self._check_for_updates)
        QTimer.singleShot(4000, self._check_migration_notice)
        self._drive_monitor = DriveTriggerMonitor(self)
        self._drive_monitor.drive_connected.connect(self._on_drive_connected)
        self._drive_monitor.start()



    def _maybe_run_setup_wizard(self):
        """
        FIX #2 — Setup wizard integration.

        Called once on first launch (via QTimer from main()).  If the user has
        no watches configured yet, we run the non-interactive parts of
        setup_wizard (create .env + starter config.json) and then offer a
        startup-on-login dialog — replacing the old pattern where first-time
        users had to discover and run ``python setup_wizard.py`` manually.

        The wizard import is deferred so that desktop_app.py does not hard-depend
        on setup_wizard.py being present (the app still works without it).
        """
        try:
            watches = self.cfg.get("watches", [])
            if watches:
                # User already has watches — skip wizard entirely.
                return

            # Run the non-interactive setup steps (safe to call from GUI).
            try:
                import setup_wizard as _sw
                _sw.run_for_app()
            except Exception:
                pass   # wizard not present or failed — non-fatal

            # Welcome dialog: explain what to do next.
            msg = QMessageBox(self)
            msg.setWindowTitle("Welcome to BackupSys!")
            msg.setIcon(QMessageBox.Icon.Information)
            msg.setText(
                "<b>Welcome — BackupSys is set up and ready to go.</b><br><br>"
                "To get started:<br>"
                "1. Click <b>Add Watch</b> to choose a folder to back up.<br>"
                "2. Open <b>Settings</b> to set your backup destination.<br>"
                "3. Enable <b>Auto-Backup</b> to run on a schedule.<br><br>"
                "You can also run <code>python setup_wizard.py</code> in a terminal "
                "for a guided CLI walkthrough."
            )

            # Offer startup-on-login (Windows / macOS / Linux).
            startup_btn = msg.addButton(
                "Add to Startup", QMessageBox.ButtonRole.ActionRole
            )
            msg.addButton("Skip", QMessageBox.ButtonRole.RejectRole)
            msg.exec()

            if msg.clickedButton() == startup_btn:
                try:
                    import setup_wizard as _sw
                    ok = _sw.offer_startup_gui()
                    if ok:
                        QMessageBox.information(
                            self, "Startup entry added",
                            "BackupSys will now launch automatically when you log in.\n"
                            "You can remove this later from Settings → General → 'Start on login'."
                        )
                    else:
                        QMessageBox.warning(
                            self, "Startup entry failed",
                            "Could not write the startup entry automatically.\n"
                            "You can enable this later from Settings → General → 'Start on login'."
                        )
                except Exception as exc:
                    QMessageBox.warning(
                        self, "Startup entry failed",
                        f"Could not write the startup entry: {exc}"
                    )

        except Exception:
            pass   # wizard errors must never crash the app

    def _check_for_updates(self):
        """Non-blocking background update check against GitHub releases API.

        Compares APP_VERSION against the latest published release tag.
        Runs in a daemon thread so it never blocks the UI.
        Shows a tray balloon (not a blocking dialog) if a new version is found.
        Suppressed entirely if the check fails for any reason — never crashes the app.
        """
        def _worker():
            try:
                import urllib.request, urllib.error, json as _json
                url = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
                req = urllib.request.Request(
                    url,
                    headers={"User-Agent": f"BackupSys/{APP_VERSION}",
                             "Accept": "application/vnd.github+json"},
                )
                with urllib.request.urlopen(req, timeout=8) as resp:
                    data = _json.loads(resp.read().decode())
                latest_tag = data.get("tag_name", "").lstrip("v")
                if not latest_tag:
                    return
                # Version comparison: pad to equal length so (1,2) == (1,2,0)
                def _ver(s):
                    try:
                        parts = tuple(int(x) for x in s.split("."))
                        return parts + (0,) * max(0, 3 - len(parts))
                    except Exception:
                        return (0, 0, 0)
                if _ver(latest_tag) > _ver(APP_VERSION):
                    html_url = data.get("html_url", GITHUB_RELEASES_URL)
                    _QTimer_call(lambda: self._notify_update(latest_tag, html_url))
            except Exception:
                pass  # network error, rate-limit, wrong URL — all silently ignored

        def _QTimer_call(fn):
            """Schedule fn on the Qt main thread (safe to call from a worker thread)."""
            QTimer.singleShot(0, fn)

        threading.Thread(target=_worker, daemon=True, name="update-check").start()

    def _notify_update(self, latest_tag: str, html_url: str):
        """Show a non-intrusive tray message about the new version."""
        try:
            # Try tray balloon first (least intrusive)
            tray = self.parent()
            if hasattr(tray, "tray") and tray.tray.isVisible():
                tray.tray.showMessage(
                    f"BackupSys {latest_tag} available",
                    f"A new version is available. Visit:\n{html_url}",
                    QSystemTrayIcon.MessageIcon.Information, 8000,
                )
                return
        except Exception:
            pass
        # Fallback: log it
        logger.info(f"[update] New version available: {latest_tag}  →  {html_url}")

    def _check_migration_notice(self):
        """Show a one-time notice to users upgrading from BackupSys v1.0.x."""
        _s = QSettings(SETTINGS_ORG, SETTINGS_APP)
        _seen_key = "migration_notice_v110_shown"
        if _s.value(_seen_key, False, type=bool):
            return  # already shown — don't nag again

        # Detect v1.0.x users: they have watches but no backup_window_end key
        # (introduced in v1.1.0) and may have Fernet-encrypted backups
        cfg = config_manager.load()
        has_watches = bool(cfg.get("watches"))
        already_has_v11_keys = "backup_window_end" in cfg
        if not has_watches or already_has_v11_keys:
            # Brand-new install or already migrated — mark done silently
            _s.setValue(_seen_key, True)
            return

        msg = QMessageBox(self)
        msg.setWindowTitle("BackupSys v1.1.0 — What's New")
        msg.setIcon(QMessageBox.Icon.Information)
        msg.setText("<b>Welcome to BackupSys v1.1.0!</b>")
        msg.setInformativeText(
            "Here's what changed since v1.0.x:\n\n"
            "🔐  Encryption upgraded to AES-256-GCM streaming\n"
            "     (old Fernet backups still open automatically — no action needed)\n\n"
            "📁  Google Drive folder picker — choose exactly where backups land\n\n"
            "⚙️  Pre/post backup script hooks per watch\n"
            "     (Settings → Edit Watch → Backup Hooks)\n\n"
            "📄  Single-file restore — restore one file from any snapshot\n"
            "     (History → Preview → Restore Selected File)\n\n"
            "🕐  Backup window stop time — limit auto-backups to a time window\n"
            "     (Settings → Run at times / Stop by)\n\n"
            "📤  Config export — back up your settings\n"
            "     (Settings → General → Export Config)\n\n"
            "✔  Per-watch last-backup status shown inline on each watch card\n\n"
            "No migration steps required — all existing watches and backups continue to work."
        )
        msg.setStandardButtons(QMessageBox.StandardButton.Ok)
        msg.exec()
        _s.setValue(_seen_key, True)

    # ── Drive trigger — USB / external drive auto-backup ───────────────────────


    def _on_drive_connected(self, label: str, serial: str, mount_point: str):
        """
        Called when any new volume is mounted.  Checks every watch for a
        matching drive_trigger_label or drive_trigger_serial and fires a backup
        for each match.
        """
        if not BACKEND_AVAILABLE:
            return
        cfg = config_manager.load()
        self.cfg = cfg
        matched_any = False
        for w in cfg.get("watches", []):
            if not w.get("active", True) or w.get("paused", False):
                continue
            trigger_label  = (w.get("drive_trigger_label")  or "").strip()
            trigger_serial = (w.get("drive_trigger_serial") or "").strip().upper()
            if not trigger_label and not trigger_serial:
                continue  # no trigger configured for this watch
            label_match  = trigger_label  and label.lower()  == trigger_label.lower()
            serial_match = trigger_serial and serial.upper() == trigger_serial.upper()
            if not (label_match or serial_match):
                continue
            if w["id"] in self._workers:
                logger.info(
                    f"[drive-trigger] {w['name']!r}: drive matched but backup already running"
                )
                continue
            logger.info(
                f"[drive-trigger] Triggering backup for {w['name']!r} "
                f"(label={label!r} serial={serial!r})"
            )
            self._append_log(
                f"🔌 Drive connected ({label or serial or mount_point}) — "
                f"triggering backup: {w['name']}"
            )
            self._backup_single(w, triggered_by="drive_trigger")
            matched_any = True

        if not matched_any:
            logger.debug(
                f"[drive-trigger] Volume mounted (label={label!r} serial={serial!r} "
                f"root={mount_point!r}) — no watch trigger matched"
            )

    def _load_config(self):
        if BACKEND_AVAILABLE:
            self.cfg = config_manager.load()
        else:
            self.cfg = {"watches": [], "destination": "", "auto_backup": False,
                        "interval_min": 30}

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

        dashboard_btn = QPushButton("📈  Dashboard")
        dashboard_btn.setObjectName("secondary")
        dashboard_btn.clicked.connect(self._open_global_dashboard)
        tl.addWidget(dashboard_btn)

        tl.addSpacing(8)

        logs_btn = QPushButton("📜  Logs")
        logs_btn.setObjectName("secondary")
        logs_btn.clicked.connect(self._open_logs)
        tl.addWidget(logs_btn)

        tl.addSpacing(8)

        admin_btn = QPushButton("🔧 Admin")
        admin_btn.setObjectName("secondary")
        admin_btn.clicked.connect(self._open_admin)
        tl.addWidget(admin_btn)

        tl.addSpacing(8)

        self.quit_btn = QPushButton("❌ Quit")
        self.quit_btn.setObjectName("secondary")
        self.quit_btn.clicked.connect(self._quit_app)
        tl.addWidget(self.quit_btn)

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

        self._pause_all_btn = QPushButton("⏸  Pause All Backups")
        self._pause_all_btn.setObjectName("secondary")
        self._pause_all_btn.setToolTip(
            "Pause all watched folders at once.\n"
            "Useful before presentations or on slow connections.\n"
            "Click again to resume all watches."
        )
        self._pause_all_btn.clicked.connect(self._toggle_pause_all)
        sl.addWidget(self._pause_all_btn)

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
        self.watch_search_input = QLineEdit()
        self.watch_search_input.setPlaceholderText("🔍  Filter watches…")
        self.watch_search_input.setFixedWidth(200)
        self.watch_search_input.setToolTip("Filter the watch list by name")
        self.watch_search_input.textChanged.connect(self._filter_watch_cards)
        header_row.addWidget(self.watch_search_input)
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

        # ── Google Drive disconnected banner ──────────────────────────────
        self.gdrive_banner = QFrame()
        self.gdrive_banner.setObjectName("card")
        self.gdrive_banner.setStyleSheet(
            "QFrame { background: #7c2d12; border: 1px solid #ea580c; border-radius: 6px; }"
        )
        gdb_row = QHBoxLayout(self.gdrive_banner)
        gdb_row.setContentsMargins(14, 8, 14, 8)
        gdb_icon = QLabel("⚠")
        gdb_icon.setStyleSheet("color:#fbbf24; font-size:16px; font-weight:bold;")
        gdb_row.addWidget(gdb_icon)
        self._gdrive_banner_lbl = QLabel(
            "<b style='color:#fef3c7;'>Google Drive disconnected</b> "
            "<span style='color:#fcd34d;'>— your token has expired or been revoked. "
            "Backups to Google Drive will fail until you reconnect.</span>"
        )
        self._gdrive_banner_lbl.setTextFormat(Qt.TextFormat.RichText)
        self._gdrive_banner_lbl.setWordWrap(True)
        gdb_row.addWidget(self._gdrive_banner_lbl, stretch=1)
        gdrive_reconnect_btn = QPushButton("Reconnect Drive →")
        gdrive_reconnect_btn.setObjectName("secondary")
        gdrive_reconnect_btn.setStyleSheet(
            "QPushButton { background:#ea580c; color:#fff; border:none; border-radius:4px; padding:4px 12px; }"
            "QPushButton:hover { background:#f97316; }"
        )
        gdrive_reconnect_btn.clicked.connect(self._open_gdrive_reconnect)
        gdb_row.addWidget(gdrive_reconnect_btn)
        gdrive_dismiss_btn = QPushButton("✕")
        gdrive_dismiss_btn.setObjectName("secondary")
        gdrive_dismiss_btn.setFixedWidth(28)
        gdrive_dismiss_btn.setToolTip("Dismiss until next backup")
        gdrive_dismiss_btn.setStyleSheet(
            "QPushButton { background:transparent; color:#fcd34d; border:none; font-size:14px; }"
            "QPushButton:hover { color:#fff; }"
        )
        gdrive_dismiss_btn.clicked.connect(lambda: self.gdrive_banner.hide())
        gdb_row.addWidget(gdrive_dismiss_btn)
        self.gdrive_banner.hide()   # hidden until a token failure is detected
        cl.addWidget(self.gdrive_banner)

        # Watches scroll area
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)

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
        # Sync the Pause All button label to reflect persisted watch states
        if hasattr(self, "_pause_all_btn"):
            all_paused = bool(watches) and all(w.get("paused", False) for w in watches)
            if all_paused:
                self._pause_all_btn.setText("▶  Resume All Backups")
                self._pause_all_btn.setToolTip("Resume all watched folders (backups were globally paused).")
            else:
                self._pause_all_btn.setText("⏸  Pause All Backups")
                self._pause_all_btn.setToolTip(
                    "Pause all watched folders at once.\n"
                    "Useful before presentations or on slow connections.\n"
                    "Click again to resume all watches."
                )
        if not watches:
            placeholder = QLabel("No folders are being watched.\nClick Admin > Watches > Add Watch to get started.")
            placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
            placeholder.setStyleSheet("color:#374151; font-size:13px; padding:40px;")
            self.watches_layout.insertWidget(0, placeholder)
        else:
            for w in watches:
                dest_type = self.cfg.get("dest_type", "local")
                card = WatchCard(w, dest_type)
                card.backup_requested.connect(self._backup_single)
                card.full_backup_requested.connect(self._force_full_backup)
                card.dry_run_requested.connect(self._dry_run_watch)
                card.validate_requested.connect(self._validate_watch)
                card.restore_requested.connect(self._restore_watch)
                card.restore_to_original_requested.connect(self._restore_to_original)
                card.pause_requested.connect(self._on_pause_requested)
                card.pause_backup_requested.connect(self._on_pause_backup_requested)
                card.resume_backup_requested.connect(self._on_resume_backup_requested)
                card.cancel_requested.connect(self._on_cancel_requested)
                card.open_backup_requested.connect(self._on_open_backup_folder)
                card.watch_settings_requested.connect(self._on_watch_settings_requested)
                self._cards[w["id"]] = card
                self.watches_layout.insertWidget(self.watches_layout.count() - 1, card)
                # Seed countdown label immediately so it shows on first load
                try:
                    card.refresh_next_backup_lbl(self.cfg)
                except Exception:
                    pass
        # Re-apply any active filter after rebuild
        if hasattr(self, "watch_search_input"):
            self._filter_watch_cards(self.watch_search_input.text())

    def _filter_watch_cards(self, text: str = ""):
        """Show/hide watch cards based on search text."""
        text = text.strip().lower()
        for wid, card in self._cards.items():
            watch_name = card.watch.get("name", "").lower()
            watch_path = card.watch.get("path", "").lower()
            visible = (not text) or (text in watch_name) or (text in watch_path)
            card.setVisible(visible)

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
                # Skip watches whose source is unreachable (e.g. SMB share not
                # yet authenticated after reboot) — log a warning instead of crashing.
                src_type = watch.get("type", "local")
                src_path = watch.get("path", "")
                if src_type in ("local", "smb"):
                    try:
                        accessible = Path(src_path).exists()
                    except OSError:
                        accessible = False
                    if not accessible:
                        self._append_log(
                            f"⚠ Skipping queued backup '{watch.get('name', wid)}' "
                            f"— source path not accessible at startup "
                            f"(SMB share may need credentials). Will retry on next scheduled run."
                        )
                        continue
                self._append_log(f"⏳ Resuming queued backup: {watch.get('name', wid)}")
                try:
                    self._backup_single(watch, triggered_by="queue")
                except Exception as e:
                    self._append_log(f"⚠ Could not resume queued backup '{watch.get('name', wid)}': {e}")

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
                QSystemTrayIcon.MessageIcon.Information, 3000
            )

    def _watch_name_for(self, watch_id: str) -> str:
        for w in self.cfg.get("watches", []):
            if w["id"] == watch_id:
                return w.get("name", watch_id)
        return watch_id

    def _watch_dest(self, watch: dict) -> str:
        """Return the effective destination for a watch.
        Per-watch destination takes priority over the global cfg destination.
        Falls back to global cfg[destination] if not set on the watch.
        """
        dest_type = self.cfg.get("dest_type", "local")
        per_watch = watch.get("destination", "").strip()
        if per_watch:
            return per_watch
        if dest_type == "smb":
            return self.cfg.get("dest_smb", {}).get("path", "").strip()
        return self.cfg.get("destination", "")


    # ── Auto Timer ─────────────────────────────────────────────────────────────

    def _start_auto_timer(self):
        self._auto_timer = QTimer(self)
        self._auto_timer.timeout.connect(self._auto_backup_tick)
        self._auto_timer.start(5_000)  # check every 5s (supports seconds interval)

        # Refresh "next backup in …" labels every 60 seconds
        self._countdown_timer = QTimer(self)
        self._countdown_timer.timeout.connect(self._refresh_countdown_labels)
        self._countdown_timer.start(60_000)

    def _refresh_countdown_labels(self):
        """Refresh the 'Next backup in …' label on every watch card."""
        if not BACKEND_AVAILABLE:
            return
        cfg = getattr(self, "cfg", None)
        if not cfg:
            return
        for card in getattr(self, "_cards", {}).values():
            try:
                card.refresh_next_backup_lbl(cfg)
            except Exception:
                pass

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

        # ── Metered connection check (Windows only) ───────────────────────────
        # Moved to per-watch below

        interval_val  = cfg.get("interval_min", 30)
        interval_unit = cfg.get("interval_unit", "minutes")
        global_secs   = interval_val if interval_unit == "seconds" else interval_val * 60
        now = datetime.now()

        # ── Time-of-day schedule check ────────────────────────────────────────
        # Schedules are resolved per-watch: the watch's own schedule_times takes
        # priority; if empty, the global backup_schedule_times is used; if that
        # is also empty, the interval runs as normal.
        global_schedule_times = cfg.get("backup_schedule_times", [])
        now_hhmm              = now.strftime("%H:%M")
        now_secs_day          = now.hour * 3600 + now.minute * 60 + now.second

        if not hasattr(self, "_last_sched_fire"):
            self._last_sched_fire = {}

        def _sched_due_for(times: list) -> bool:
            """Return True if any entry in times matches the current HH:MM (±5 s) and today's weekday."""
            today_bit = 1 << now.weekday()   # Mon=0 → bit 1, Sun=6 → bit 64
            for entry in times:
                if isinstance(entry, str):
                    sched_str = entry
                    days_mask = 127
                else:
                    sched_str = entry.get("time", "")
                    days_mask = int(entry.get("days", 127))
                if not (days_mask & today_bit):
                    continue   # not scheduled for today
                try:
                    sh, sm    = int(sched_str[:2]), int(sched_str[3:5])
                    sched_sec = sh * 3600 + sm * 60
                    if abs(now_secs_day - sched_sec) <= 5:
                        fire_key = sched_str + "@" + now_hhmm
                        if self._last_sched_fire.get(fire_key) != now_hhmm:
                            self._last_sched_fire[fire_key] = now_hhmm
                            logger.info(f"Scheduled backup triggered at {sched_str}")
                            return True
                except Exception:
                    continue
            return False

        # ── Global-only pre-check: if a global schedule is set and NO watch has
        # its own schedule_times, bail out early when no global time is due.
        # This preserves the original behaviour for deployments that don't use
        # per-watch schedules (avoids iterating every watch on every tick).
        watches_with_own_schedule = [
            w for w in cfg.get("watches", []) if w.get("schedule_times")
        ]
        if global_schedule_times and not watches_with_own_schedule:
            if not _sched_due_for(global_schedule_times):
                return  # nothing to do yet

        for w in cfg.get("watches", []):
            wid = w["id"]
            if not w.get("active", True) or w.get("paused", False):
                continue
            if w.get("skip_auto_backup", False):
                continue
            if wid in self._workers:
                continue  # already running
            if wid in self._user_cancelled_watches:
                continue  # user explicitly cancelled — don't auto-restart

            # ── Backup window check — only START new backups inside the allowed window ──
            _window_start = cfg.get("backup_window_start", "").strip()
            _window_end   = cfg.get("backup_window_end",   "").strip()
            if _window_start or _window_end:
                try:
                    _now = datetime.now()
                    _now_secs = _now.hour * 3600 + _now.minute * 60 + _now.second

                    # Parse whichever bounds are set
                    _ws_secs = None
                    _we_secs = None
                    if _window_start:
                        _ws_h, _ws_m = int(_window_start[:2]), int(_window_start[3:5])
                        _ws_secs = _ws_h * 3600 + _ws_m * 60
                    if _window_end:
                        _we_h, _we_m = int(_window_end[:2]), int(_window_end[3:5])
                        _we_secs = _we_h * 3600 + _we_m * 60

                    # Determine whether we are currently inside the allowed window.
                    # Two cases:
                    #   Normal window  (start < end):  e.g. 01:00–06:00 — inside iff start <= now < end
                    #   Overnight wrap (start > end):  e.g. 22:00–06:00 — inside iff now >= start OR now < end
                    _in_window = True  # assume allowed when only one bound is set
                    if _ws_secs is not None and _we_secs is not None:
                        if _ws_secs < _we_secs:
                            # Same-day window
                            _in_window = _ws_secs <= _now_secs < _we_secs
                        else:
                            # Overnight window (wraps midnight)
                            _in_window = _now_secs >= _ws_secs or _now_secs < _we_secs
                    elif _ws_secs is not None:
                        # Only start bound set — allowed from start onwards (no end)
                        _in_window = _now_secs >= _ws_secs
                    elif _we_secs is not None:
                        # Only end bound set (legacy: stop-only) — allowed until end
                        _in_window = _now_secs < _we_secs

                    if not _in_window:
                        _bounds_str = (
                            (f"{_window_start}–" if _window_start else "–") +
                            (_window_end if _window_end else "")
                        )
                        logger.debug(
                            f"[window] Auto-backup suppressed for {w['name']}: current time "
                            f"{_now.strftime('%H:%M')} is outside backup window {_bounds_str}"
                        )
                        if not self._skipped_notified.get(wid, {}).get('window', False):
                            self.tray_icon.showMessage(
                                "BackupSys — Backup Skipped",
                                f"Scheduled backup skipped — outside the allowed backup window ({_bounds_str}).",
                                QSystemTrayIcon.MessageIcon.Information, 4000,
                            )
                            if wid not in self._skipped_notified:
                                self._skipped_notified[wid] = {}
                            self._skipped_notified[wid]['window'] = True
                        continue
                except Exception:
                    pass  # malformed time — ignore silently

            # idle_threshold_cpu: 0 = disabled; e.g. 50 = only backup when CPU < 50%
            _idle_threshold = cfg.get("idle_threshold_cpu", 0)
            if _idle_threshold and _idle_threshold > 0:
                try:
                    import psutil
                    _cpu = psutil.cpu_percent(interval=0)   # non-blocking sample
                    if _cpu > _idle_threshold:
                        logger.debug(
                            f"[idle] CPU at {_cpu:.0f}% > threshold {_idle_threshold}% "
                            f"for {w['name']} — deferring auto-backup until system is idle"
                        )
                        if not self._skipped_notified.get(wid, {}).get('idle', False):
                            self.tray_icon.showMessage("BackupSys — Backup Skipped", "Scheduled backup deferred — system is not idle (CPU above threshold).", QSystemTrayIcon.MessageIcon.Information, 4000)
                            if wid not in self._skipped_notified:
                                self._skipped_notified[wid] = {}
                            self._skipped_notified[wid]['idle'] = True
                        continue
                except ImportError:
                    pass   # psutil not installed — skip idle check silently

            # ── Metered connection check (Windows only) ───────────────────────────
            if cfg.get("pause_on_metered", False):
                if is_metered_connection():
                    logger.info(f"Auto-backup skipped for {w['name']} — metered network connection detected.")
                    continue

            # ── Battery check — skip backup when running on battery ────────────────────
            if cfg.get("pause_on_battery", False):
                try:
                    import psutil
                    _bat = psutil.sensors_battery()
                    # sensors_battery() returns None on desktops (no battery).
                    # Only suppress when a battery is present AND not plugged in.
                    if _bat is not None and not _bat.power_plugged:
                        logger.debug(
                            f"[battery] Auto-backup deferred for {w['name']} — "
                            f"running on battery ({_bat.percent:.0f}% remaining)"
                        )
                        if not self._skipped_notified.get(wid, {}).get('battery', False):
                            self.tray_icon.showMessage(
                                "BackupSys — Backup Skipped",
                                "Scheduled backup deferred — laptop is running on battery.",
                                QSystemTrayIcon.MessageIcon.Information, 4000,
                            )
                            if wid not in self._skipped_notified:
                                self._skipped_notified[wid] = {}
                            self._skipped_notified[wid]['battery'] = True
                        continue
                except ImportError:
                    pass  # psutil not installed — skip battery check silently

            # ── Per-watch schedule resolution ─────────────────────────────────
            # Priority: per-watch schedule_times > global backup_schedule_times > interval
            w_sched = w.get("schedule_times", [])
            effective_schedule = w_sched if w_sched else global_schedule_times

            if effective_schedule:
                # Scheduled mode for this watch — only fire at the named times
                if _sched_due_for(effective_schedule):
                    self._backup_single(w, triggered_by="scheduled")
                # else: not yet time for this watch — skip without falling through to interval
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

        # A new backup starting (manual or scheduled) clears any prior
        # user-cancel so auto-backups resume normally after this run.
        self._user_cancelled_watches.discard(wid)

        # Reset skip notification flags when backup actually runs
        self._skipped_notified[wid] = {}

        # ── Per-watch storage quota check ─────────────────────────────────────
        # max_backup_bytes: if > 0, refuse to start a new backup once that
        # watch has already consumed more than N bytes of backup storage.
        _max_bytes = watch.get("max_backup_bytes", 0)
        if _max_bytes and _max_bytes > 0 and BACKEND_AVAILABLE:
            _dest_type_for_quota = self.cfg.get("dest_type", "local")
            _non_local_dests = {"sftp", "ftp", "ftps", "smb", "webdav", "rclone", "cloud", "https", "gdrive"}
            if _dest_type_for_quota in _non_local_dests:
                # Storage quota cannot be enforced for remote destinations because
                # get_watch_disk_usage scans the local backup_dir path which does
                # not exist for remote-only targets.  Warn once and skip the check.
                self._append_log(
                    f"⚠ Storage quota for '{watch.get('name', wid)}' is set but cannot be "
                    f"enforced for remote destination '{_dest_type_for_quota}'. "
                    "The quota is only supported for local destinations. "
                    "Proceeding with backup."
                )
            else:
                try:
                    _used = backup_engine._backup_index.get_watch_disk_usage(
                        self.cfg.get("destination", ""), wid
                    )
                    # Add warning thresholds before hard refusal
                    _usage_pct = (_used / _max_bytes) * 100
                    if _usage_pct >= 90:
                        # Add warning_90 to the result dict (will be checked in completion handler)
                        watch["_quota_warning_90"] = True
                    elif _usage_pct >= 80:
                        # Add warning_80 to the result dict (will be checked in completion handler)
                        watch["_quota_warning_80"] = True

                    if _used >= _max_bytes:
                        _used_h  = backup_engine._human_size(_used)
                        _limit_h = backup_engine._human_size(_max_bytes)
                        self._append_log(
                            f"⚠ Skipped backup for '{watch.get('name', wid)}' — "
                            f"storage quota exceeded ({_used_h} used of {_limit_h} limit). "
                            "Delete old backups or raise the quota in Settings → Edit Watch."
                        )
                        if hasattr(self, "_tray"):
                            self._tray.showMessage(
                                APP_NAME,
                                f"⚠ Quota exceeded for {watch.get('name',wid)}: "
                                f"{_used_h} / {_limit_h}",
                                QSystemTrayIcon.MessageIcon.Warning, 6000,
                            )
                        return
                except Exception:
                    pass  # quota check failure is non-fatal; proceed with backup

        # Collect watcher-tracked changed paths for the fast-scan optimisation.
        # Only use for incremental (non-manual-full) triggers; for "force full"
        # changed_paths is left None so build_snapshot does a complete rglob.
        _changed_paths = None
        if self._watcher_mgr and triggered_by != "full":
            try:
                _pending = self._watcher_mgr.get_pending(wid)
                if _pending:
                    _changed_paths = [e.get("path", "") for e in _pending if e.get("path")]
            except Exception:
                pass

        worker = BackupWorker(watch, self.cfg, triggered_by=triggered_by, changed_paths=_changed_paths)
        worker.progress.connect(lambda c, t, f, e, s, bd, tb, _wid=wid: self._on_progress(_wid, c, t, f, e, s, bd, tb))
        worker.finished.connect(lambda r, _wid=wid: self._on_backup_done(_wid, r))
        worker.log_message.connect(self._append_log)
        self._workers[wid] = worker
        # Track how many backups have started this session (used by auto-shutdown)
        self._backups_started_this_session = getattr(self, "_backups_started_this_session", 0) + 1

        # ── Background size estimate (non-blocking, manual triggers only) ───
        # Shows "Estimated: X files / Y MB" in the log before the backup starts.
        # Skipped for scheduled/watcher triggers to avoid double scanning.
        if triggered_by == "manual" and BACKEND_AVAILABLE:
            def _run_estimate():
                try:
                    snapshot = (config_manager.load_snapshot(self.cfg, wid) or {}).get("files")
                    est = backup_engine.estimate_backup_size(
                        source            = watch["path"],
                        exclude_patterns  = watch.get("exclude_patterns", []),
                        previous_snapshot = snapshot,
                        max_file_size_mb  = watch.get("max_file_size_mb", 0),
                    )
                    if est.get("error"):
                        return
                    mode  = "incremental" if est["incremental"] else "full"
                    files = est["total_files"]
                    size  = est["total_human"]
                    parts = [f"~{files} file(s)  ·  ~{size}  ({mode})"]
                    if est["incremental"]:
                        parts.append(
                            f"{est['new_files']} new  ·  {est['changed_files']} changed  ·  "
                            f"{est['deleted_files']} deleted"
                        )
                    if est.get("skipped_files"):
                        parts.append(f"{est['skipped_files']} skipped")
                    from PyQt6.QtCore import QTimer
                    msg = "📊 Estimate:  " + "   |   ".join(parts)
                    QTimer.singleShot(0, lambda m=msg: self._append_log(m))
                except Exception:
                    pass  # size estimate failure is always non-fatal
            import threading as _thr
            _thr.Thread(target=_run_estimate, daemon=True).start()

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
        from PyQt6.QtWidgets import QMessageBox
        wid = watch["id"]
        reply = QMessageBox.question(
            self, "Force Full Backup",
            "This will re-upload ALL files regardless of changes.\n\nContinue?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if reply != QMessageBox.StandardButton.Yes:
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

    def _on_progress(self, wid: str, current: int, total: int, fname: str = "", elapsed: float = 0.0, is_scanning: bool = False, bytes_done: int = 0, total_bytes: int = 0):
        if wid in self._cards:
            self._cards[wid].set_progress(current, total, fname, elapsed, is_scanning, bytes_done, total_bytes)

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

        # Record backup history for export
        started_at = result.get("timestamp", datetime.now().isoformat())
        finished_at = started_at
        try:
            started_dt = datetime.fromisoformat(started_at)
            finished_at = (started_dt + timedelta(seconds=float(result.get("duration_s", 0) or 0))).isoformat()
        except Exception:
            finished_at = datetime.now().isoformat()

        history_entry = {
            "watch_name": self._watch_name_for(wid),
            "watch_id": wid,
            "backup_id": result.get("backup_id", result.get("id", "")),
            "status": result.get("status", ""),
            "started_at": started_at,
            "finished_at": finished_at,
            "file_count": int(result.get("files_copied", 0) or 0),
            "size_bytes": int(result.get("total_size_bytes", 0) or 0),
            "destination": result.get("destination", ""),
            "error": result.get("error", "") or "",
        }
        self._backup_history.append(history_entry)
        if len(self._backup_history) > 1000:
            self._backup_history = self._backup_history[-1000:]
        if BACKEND_AVAILABLE:
            try:
                config_manager.save_backup_history(self._backup_history)
            except Exception:
                pass

        if wid in self._cards:
            self._cards[wid].set_done(success, result.get("duration_s", 0.0))

        # Reset change count for this watch
        if success:
            self._change_counts[wid] = 0
            self._pending_entries[wid] = []
            if self._watcher_mgr:
                self._watcher_mgr.clear_pending(wid)

            # ── Inject backup-diff changes into history ─────────────────
            # Works for ALL source types (local, SMB, network) because it
            # uses snapshot diffing, not live file system events.
            diff_changes = result.get("changes", [])
            if diff_changes:
                watch_name = self._watch_name_for(wid)
                ts_iso     = result.get("timestamp", datetime.now().isoformat())
                # Use the source path of the first changed file for file-owner lookup.
                # Falls back to machine/user via os.getlogin() if pywin32 isn't installed.
                _watch_src  = result.get("source", "")
                _first_path = diff_changes[0].get("path", "") if diff_changes else ""
                _sample_fp  = str(Path(_watch_src) / _first_path) if _watch_src and _first_path else ""
                editor      = _get_editor_info(_sample_fp)
                for ch in diff_changes:
                    hist_entry = {
                        "type":           ch.get("type", "modified"),
                        "path":           ch.get("path", ""),
                        "timestamp":      ts_iso,
                        "watch_name":     watch_name,
                        "watch_id":       wid,
                        "editor_user":    editor["user"],
                        "editor_machine": editor["machine"],
                        "editor_ip":      editor["ip"],
                        "source":         "backup_diff",
                    }
                    self._history_log.append(hist_entry)
                if len(self._history_log) > 5000:
                    self._history_log = self._history_log[-2500:]
                # Persist immediately after backup (don't wait for 25-entry threshold)
                if BACKEND_AVAILABLE:
                    try:
                        config_manager.save_history(self._history_log)
                    except Exception:
                        pass
                # Live-update history window if open
                if self._history_window and self._history_window.isVisible():
                    for ch in diff_changes:
                        hist_entry = {
                            "type":           ch.get("type", "modified"),
                            "path":           ch.get("path", ""),
                            "timestamp":      ts_iso,
                            "watch_name":     watch_name,
                            "watch_id":       wid,
                            "editor_user":    editor["user"],
                            "editor_machine": editor["machine"],
                            "editor_ip":      editor["ip"],
                            "source":         "backup_diff",
                        }
                        self._history_window.append_entry(hist_entry)
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
                self._cards[wid].refresh_next_backup_lbl(self.cfg)

        self._update_stats()
        self._update_auto_label()

        if not self._workers:
            self.status_dot.setText("● Active")
            self.status_dot.setObjectName("status_ok")
            self.status_dot.style().unpolish(self.status_dot)
            self.status_dot.style().polish(self.status_dot)

        # Tray notification
        if hasattr(self, "_tray"):
            # Check for quota warnings first
            watch = next((w for w in self.cfg.get("watches", []) if w["id"] == wid), None)
            if watch:
                if watch.get("_quota_warning_90"):
                    self._tray.showMessage(
                        APP_NAME,
                        f"Watch '{watch.get('name', wid)}' is at 90% of its storage quota — consider cleaning up old backups.",
                        QSystemTrayIcon.MessageIcon.Warning, 8000
                    )
                elif watch.get("_quota_warning_80"):
                    self._tray.showMessage(
                        APP_NAME,
                        f"Watch '{watch.get('name', wid)}' is at 80% of its storage quota — consider cleaning up old backups.",
                        QSystemTrayIcon.MessageIcon.Warning, 6000
                    )
                # Clean up the temporary flags
                watch.pop("_quota_warning_80", None)
                watch.pop("_quota_warning_90", None)

            if success:
                dur_str  = _fmt_duration(result.get("duration_s", 0.0))
                dur_part = f"  ·  {dur_str}" if dur_str else ""
                sz_part  = f"  ·  {result.get('total_size','')}" if result.get("total_size") else ""
                msg = f"✅ Backup complete: {result.get('files_copied',0)} file(s){sz_part}{dur_part}"
                _icon = QSystemTrayIcon.MessageIcon.Information
            elif result.get("status") == "cancelled":
                msg   = f"⏹ Backup cancelled: {result.get('watch_name', self._watch_name_for(wid))}"
                _icon = QSystemTrayIcon.MessageIcon.Information
            else:
                err_detail = result.get("error", "unknown error")
                msg   = f"❌ Backup FAILED: {self._watch_name_for(wid)} — {err_detail}"
                # Surface the Drive reconnect banner if the error is a token failure
                if err_detail and "reconnect Google Drive" in err_detail and hasattr(self, "gdrive_banner"):
                    self.gdrive_banner.show()
                _icon = QSystemTrayIcon.MessageIcon.Warning
            self._tray.showMessage(APP_NAME, msg, _icon, 5000 if not success else 3000)

        # ── Auto-shutdown: trigger only when ALL backups are done ────────────
        # _workers is empty → no backups running.  Check the config flag and
        # that at least one backup ran in this session (avoid firing on launch).
        if (
            not self._workers
            and self.cfg.get("auto_shutdown_on_complete", False)
            and getattr(self, "_backups_started_this_session", 0) > 0
        ):
            self._trigger_auto_shutdown()

    def _trigger_auto_shutdown(self):
        """Show countdown dialog then shut down the OS."""
        dlg = ShutdownCountdownDialog(self, countdown=60)
        result = dlg.exec()
        if dlg.was_cancelled():
            self._append_log("⏹ Auto-shutdown cancelled by user.")
            return
        # Accepted (timer expired or 'Shut Down Now')
        self._append_log("🖥  Auto-shutdown initiated — all backups complete.")
        import platform, subprocess
        try:
            if platform.system() == "Windows":
                subprocess.run(["shutdown", "/s", "/t", "0"], check=True)
            elif platform.system() == "Darwin":
                subprocess.run(["osascript", "-e", 'tell application "System Events" to shut down'], check=True)
            else:  # Linux / BSD
                subprocess.run(["systemctl", "poweroff"], check=True)
        except Exception as exc:
            QMessageBox.critical(
                self, "Shutdown Failed",
                f"Could not shut down the computer:\n{exc}\n\n"
                "You may need to run BackupSys as administrator."
            )

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

    def _dry_run_watch(self, watch: dict):
        """Run backup preview and display results in a dialog.

        Strategy:
          - Normal (script) mode : spawn backupsys_cli.py as a subprocess so the
            output mirrors exactly what the user sees in the terminal.
          - Frozen (.exe) mode   : call backup_engine.run_backup(dry_run=True)
            directly, because backupsys_cli.py does not exist on disk in a
            PyInstaller bundle.
        Both paths run in a daemon thread so the Qt main thread never blocks.
        """
        watch_id   = watch["id"]
        watch_name = watch.get("name", "Unknown")

        self._append_log(f"🔍 Running backup preview for \'{watch_name}\' …")

        if watch_id in self._cards:
            self._cards[watch_id].dry_run_btn.setEnabled(False)

        def _run_cli():
            """Subprocess path — used when running as a .py script."""
            import subprocess
            _app_dir = str(Path(__file__).parent)
            _cli     = str(Path(__file__).parent / "backupsys_cli.py")
            try:
                result = subprocess.run(
                    [sys.executable, _cli, "dry-run", "--watch", watch_id],
                    capture_output=True,
                    text=True,
                    timeout=120,
                    cwd=_app_dir,
                )
                return {
                    "status": "ok" if result.returncode == 0 else "error",
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                }
            except subprocess.TimeoutExpired:
                return {"status": "error", "stdout": "", "stderr": "Preview timed out after 120 seconds"}
            except Exception as e:
                return {"status": "error", "stdout": "", "stderr": str(e)}

        def _run_inprocess():
            """In-process path — used when running as a frozen PyInstaller .exe."""
            try:
                cfg      = config_manager.load()
                dest     = watch.get("destination", "").strip() or cfg.get("destination", "")
                dest_type = cfg.get("dest_type", "local")
                snapshot = config_manager.load_snapshot(watch_id, dest_type)
                result   = backup_engine.run_backup(
                    source            = watch["path"],
                    destination       = dest,
                    watch_id          = watch_id,
                    watch_name        = watch_name,
                    storage_type      = dest_type,
                    previous_snapshot = snapshot or None,
                    incremental       = bool(snapshot),
                    exclude_patterns  = watch.get("exclude_patterns", []),
                    max_file_size_mb  = watch.get("max_file_size_mb", 0),
                    dry_run           = True,
                )
                # Format the result the same way backupsys_cli does
                changes  = result.get("changes", [])
                added    = [c for c in changes if c["type"] == "added"]
                modified = [c for c in changes if c["type"] == "modified"]
                deleted  = [c for c in changes if c["type"] == "deleted"]
                lines = [
                    f"  Previewing: {watch_name}  ({watch.get('path', '')})",
                    f"",
                    f"  ✅  {watch_name}: {result.get('files_to_copy', 0)} file(s) would be copied"
                    f"  ({result.get('total_size', '0 B')})",
                ]
                if added:
                    lines.append(f"      + {len(added)} new file(s)")
                if modified:
                    lines.append(f"      ~ {len(modified)} modified file(s)")
                if deleted:
                    lines.append(f"      - {len(deleted)} deleted file(s) (marker only)")
                if not changes:
                    lines.append("      (nothing to back up — source is unchanged)")
                lines.append("")
                for c in sorted(changes, key=lambda x: x.get("path", "")):
                    sym = {"added": "+", "modified": "~", "deleted": "-"}.get(c["type"], "?")
                    sz  = backup_engine._human_size(c.get("size", 0))
                    lines.append(f"    {sym} {c['path']:<60}  {sz}")
                return {"status": "ok", "stdout": "\n".join(lines), "stderr": ""}
            except Exception as e:
                return {"status": "error", "stdout": "", "stderr": str(e)}
        
        def _show_dialog(result):
            """Display the preview output in a modal dialog."""
            from PyQt6.QtWidgets import QDialog, QVBoxLayout, QTextEdit, QPushButton, QHBoxLayout
            from PyQt6.QtCore import Qt
            
            dlg = QDialog(self)
            dlg.setWindowTitle(f"Backup Preview — {watch_name}")
            dlg.setGeometry(100, 100, 700, 500)
            
            layout = QVBoxLayout(dlg)
            
            # Read-only text edit for output
            text_edit = QTextEdit()
            text_edit.setReadOnly(True)
            text_edit.setStyleSheet(
                "background:#141720; color:#e5e7eb; font-family:'Courier New'; font-size:10px;"
            )
            
            # Combine stdout and stderr for display
            output = result.get("stdout", "")
            if result.get("stderr"):
                if output:
                    output += "\n\n--- STDERR ---\n"
                output += result.get("stderr", "")
            
            if result.get("status") == "error" and not output:
                output = f"Error running preview: {result.get('stderr', 'Unknown error')}"
            
            text_edit.setPlainText(output if output else "(No output)")
            text_edit.moveCursor(text_edit.textCursor().__class__.Start)
            layout.addWidget(text_edit)
            
            # Close button
            btn_layout = QHBoxLayout()
            btn_layout.addStretch()
            close_btn = QPushButton("Close")
            close_btn.clicked.connect(dlg.accept)
            btn_layout.addWidget(close_btn)
            layout.addLayout(btn_layout)
            
            dlg.exec()
            
            # Re-enable the dry run button after dialog closes
            if watch_id in self._cards:
                self._cards[watch_id].dry_run_btn.setEnabled(True)
            
            # Log summary
            if result.get("status") == "ok":
                self._append_log(f"✔ Backup preview for '{watch_name}' completed")
            else:
                self._append_log(f"❌ Backup preview for '{watch_name}' failed")
        
        # Run in background thread.
        # In frozen (.exe) mode backupsys_cli.py does not exist on disk, so
        # we call backup_engine directly.  In script mode we spawn the CLI
        # subprocess so the output is identical to the terminal command.
        import threading
        _is_frozen = getattr(sys, "frozen", False)
        def _thread():
            result = _run_inprocess() if _is_frozen else _run_cli()
            from PyQt6.QtCore import QTimer
            QTimer.singleShot(0, lambda: _show_dialog(result))

        threading.Thread(target=_thread, daemon=True).start()

    def _validate_watch(self, watch: dict):
        if not BACKEND_AVAILABLE:
            return

        # Sync mode: validate the destination folder directly.
        if watch.get("sync_mode", False):
            folder = self._watch_dest(watch)
            if not folder:
                QMessageBox.warning(self, "Validate", "No destination path configured.")
                return
            self._append_log(f"Validating sync destination: {watch['name']} …")
            try:
                result = backup_engine.validate_backup(folder)
            except Exception as e:
                QMessageBox.critical(self, "Validate Error", str(e))
                return
            if result.get("valid"):
                QMessageBox.information(self, "Validate  · Passed",
                    f"▶  Destination is valid\n\nWatch:  {watch['name']}\nFolder: {folder}")
                self._append_log(f"▶ Validate passed: {watch['name']}")
            else:
                err = result.get("error", "Unknown error")
                QMessageBox.critical(self, "Validate  · Failed",
                    f"⚠  Validation failed\n\nWatch: {watch['name']}\nError: {err}")
                self._append_log(f"⚠ Validate failed: {watch['name']}")
            return

        dest = self._watch_dest(watch)
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

    def _pick_restore_destination(self, watch: dict) -> tuple:
        """
        Return (dest_path, dest_type, temp_dir_or_None) for a restore operation.

        Priority:
          1. If the global dest_type is non-local, use it (existing behaviour).
          2. If the watch has per-watch destinations (watch["destinations"]),
             let the user choose which remote to restore from.
          3. Fall back to the local destination path.

        Returns (dest_path, dest_type, temp_dir) where:
          - dest_path  — local path to use for list_backups / restore_backup
          - dest_type  — resolved type string (for display only after this call)
          - temp_dir   — path to clean up after restore, or None if not a temp dir
        Returns (None, None, None) if the user cancelled or download failed.
        """
        global_dest_type = self.cfg.get("dest_type", "local")

        # ── Global non-local destination ──────────────────────────────────────
        # "gdrive" is the canonical dest_type saved by the Settings dialog;
        # "cloud" is kept as a legacy alias for configs saved by older versions.
        _REMOTE_TYPES = {"sftp", "ftps", "ftp", "smb", "webdav", "https", "rclone", "cloud", "gdrive"}
        if global_dest_type in _REMOTE_TYPES:
            local_path = self._download_for_restore(
                self._watch_dest(watch), global_dest_type, watch)
            if local_path is None:
                return None, None, None
            return local_path["path"], global_dest_type, local_path["temp_dir"]

        # ── Per-watch destinations (watch["destinations"] list) ───────────────
        per_watch_dests = watch.get("destinations", [])
        remote_dests = [
            d for d in per_watch_dests
            if d.get("dest_type", "local") in _REMOTE_TYPES
        ]

        if remote_dests:
            # Build choice list: local first (if configured), then each remote
            choices = []
            local_path = self._watch_dest(watch)
            local_accessible = bool(local_path) and Path(local_path).exists()
            if local_accessible:
                choices.append(f"Local  —  {local_path}")
            for d in remote_dests:
                dt = d.get("dest_type", "?").upper()
                cfg_d = d.get("config", {})
                host = cfg_d.get("host", "") or cfg_d.get("remote", "") or dt
                choices.append(f"{dt}  —  {host}")

            if len(choices) > 1:
                from PyQt6.QtWidgets import QInputDialog
                chosen_label, ok = QInputDialog.getItem(
                    self, "Choose Restore Source",
                    f"Watch \"{watch['name']}\" has multiple backup destinations.\n"
                    "Choose which to restore from:",
                    choices, 0, False,
                )
                if not ok:
                    return None, None, None
                chosen_idx = choices.index(chosen_label)
                if local_accessible and chosen_idx == 0:
                    return local_path, "local", None
                # Adjust index if local was prepended
                remote_idx = chosen_idx - (1 if local_accessible else 0)
                chosen_dest = remote_dests[remote_idx]
            elif remote_dests:
                chosen_dest = remote_dests[0]
            else:
                return self._watch_dest(watch), "local", None

            dt = chosen_dest.get("dest_type", "local")
            rpath = chosen_dest.get("config", {}).get("path", self._watch_dest(watch))
            result = self._download_for_restore(rpath, dt, watch,
                                                 dest_cfg=chosen_dest.get("config", {}))
            if result is None:
                return None, None, None
            return result["path"], dt, result["temp_dir"]

        # ── Plain local destination ───────────────────────────────────────────
        return self._watch_dest(watch), "local", None

    def _download_for_restore(self, dest: str, dest_type: str, watch: dict,
                               dest_cfg: dict = None) -> dict | None:
        """
        Download a remote backup store to a local temp directory.
        Returns {"path": str, "temp_dir": str} on success, or None on failure/cancel.
        Uses a modal progress dialog while downloading.
        dest_cfg overrides the global cfg section when supplied (for per-watch dests).
        """
        import tempfile as _tmpmod
        import shutil   as _sh
        from PyQt6.QtWidgets import QProgressDialog
        from PyQt6.QtCore    import Qt

        label_map = {
            "sftp": "SFTP", "ftps": "SFTP/TLS", "ftp": "FTP",
            "smb": "SMB", "webdav": "WebDAV", "https": "HTTPS",
            "rclone": "rclone", "cloud": "Google Drive", "gdrive": "Google Drive",
        }
        label = label_map.get(dest_type, dest_type.upper())

        temp_dir = _tmpmod.mkdtemp(prefix="backupsys_restore_")

        progress = QProgressDialog(f"Downloading backup from {label}…", "Cancel", 0, 0, self)
        progress.setWindowModality(Qt.WindowModality.WindowModal)
        progress.setMinimumDuration(0)
        progress.show()

        def _prog(n, fname):
            if progress.wasCanceled():
                return
            progress.setLabelText(f"Downloading from {label}… ({n} file(s))\n{fname}")
            from PyQt6.QtWidgets import QApplication
            QApplication.processEvents()

        try:
            from transport_utils import (
                download_from_sftp, download_from_ftp, download_from_smb,
                download_from_webdav, download_from_https, download_from_rclone,
            )

            def _cfg(key):
                # Use dest_cfg when explicitly supplied (even if empty);
                # only fall back to global config when dest_cfg is None.
                return dest_cfg if dest_cfg is not None else self.cfg.get(key, {})

            if dest_type in ("sftp", "ftps"):
                result = download_from_sftp(dest, temp_dir, _cfg("dest_sftp"), progress_cb=_prog)
            elif dest_type == "ftp":
                result = download_from_ftp(dest, temp_dir, _cfg("dest_ftp"), progress_cb=_prog)
            elif dest_type == "smb":
                result = download_from_smb(dest, temp_dir, _cfg("dest_smb"), progress_cb=_prog)
            elif dest_type == "webdav":
                result = download_from_webdav(dest, temp_dir, _cfg("dest_webdav"), progress_cb=_prog)
            elif dest_type == "https":
                result = download_from_https(dest, temp_dir, _cfg("dest_https"), progress_cb=_prog)
            elif dest_type == "rclone":
                result = download_from_rclone(dest, temp_dir, _cfg("dest_rclone"), progress_cb=_prog)
            elif dest_type in ("cloud", "gdrive"):
                w_cloud = watch.get("cloud_config") or {}
                result  = backup_engine.download_from_gdrive(w_cloud, temp_dir)
                # gdrive uses "ok" key not "status"
                if result.get("ok"):
                    result["status"] = "ok"
            else:
                _sh.rmtree(temp_dir, ignore_errors=True)
                from PyQt6.QtWidgets import QMessageBox
                QMessageBox.critical(self, "Download Failed",
                    f"Unsupported destination type: {dest_type}")
                return None

            if result.get("status") != "ok":
                _sh.rmtree(temp_dir, ignore_errors=True)
                from PyQt6.QtWidgets import QMessageBox
                QMessageBox.critical(self, "Download Failed",
                    f"Failed to download from {label}:\n{result.get('error', 'Unknown error')}")
                return None

            return {"path": temp_dir, "temp_dir": temp_dir}

        except Exception as exc:
            _sh.rmtree(temp_dir, ignore_errors=True)
            from PyQt6.QtWidgets import QMessageBox
            QMessageBox.critical(self, "Download Failed",
                f"Failed to download from {label}:\n{exc}")
            return None
        finally:
            progress.close()

    def _restore_watch(self, watch: dict):
        if not BACKEND_AVAILABLE:
            return

        # Sync mode: the destination IS the live copy — no restore needed.
        if watch.get("sync_mode", False):
            folder = self._watch_dest(watch)
            QMessageBox.information(self, "Restore  · Sync Mode",
                f"This watch uses sync mode.\n\n"
                f"Your files are stored directly at:\n{folder}\n\n"
                f"To recover a file, open that folder and copy it back manually.")
            return

        # Resolve destination — handles global remote types AND per-watch destinations.
        dest, _resolved_type, temp_dir = self._pick_restore_destination(watch)
        if dest is None:
            return   # user cancelled or download failed
        backups = backup_engine.list_backups(dest, watch["id"])
        if not backups:
            QMessageBox.warning(self, "Restore",
                f"No backups found for \"{watch['name']}\".\nRun a backup first.")
            return

        # Let user pick which backup to restore
        from PyQt6.QtWidgets import QInputDialog
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
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No | QMessageBox.StandardButton.Cancel
            )
            if mode_reply == QMessageBox.StandardButton.Cancel:
                return
            use_chain = (mode_reply == QMessageBox.StandardButton.Yes)
        else:
            use_chain = False

        # Optionally browse backup contents before restoring
        browse_reply = QMessageBox.question(
            self, "Preview Backup Contents",
            "Would you like to preview the files in this backup snapshot before restoring?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        if browse_reply == QMessageBox.StandardButton.Yes:
            try:
                contents = backup_engine.browse_backup_contents(backup_dir)
                total    = contents.get("total", 0)

                # ── Scrollable tree preview dialog ────────────────────────────
                from PyQt6.QtWidgets import QDialog, QVBoxLayout, QHBoxLayout, QTreeWidget, QTreeWidgetItem, QLabel, QDialogButtonBox
                from PyQt6.QtCore import Qt

                dlg = QDialog(self)
                dlg.setWindowTitle(f"Backup Preview  ·  {chosen}")
                dlg.setMinimumSize(680, 480)
                dlg.resize(760, 540)

                vlay = QVBoxLayout(dlg)
                vlay.addWidget(QLabel(f"<b>{total} file(s)</b> in this snapshot — scroll to see all:"))

                tree = QTreeWidget()
                tree.setHeaderLabels(["Path", "Size", "Status"])
                tree.setColumnWidth(0, 420)
                tree.setColumnWidth(1, 90)
                tree.setColumnWidth(2, 80)
                tree.setSortingEnabled(True)
                tree.setRootIsDecorated(False)
                tree.setAlternatingRowColors(True)

                for f in contents.get("files", []):
                    item = QTreeWidgetItem([
                        f.get("path", ""),
                        f.get("size_human", ""),
                        "added/modified"
                    ])
                    tree.addTopLevelItem(item)

                for p in contents.get("deleted", []):
                    item = QTreeWidgetItem([p, "", "deleted"])
                    item.setForeground(0, tree.palette().highlight())
                    tree.addTopLevelItem(item)

                vlay.addWidget(tree)

                # ── Single-file restore button ─────────────────────────────
                sfr_note = QLabel(
                    "💡 Select a file above then click <b>Restore Selected File</b> "
                    "to restore just that one file."
                )
                sfr_note.setWordWrap(True)
                sfr_note.setStyleSheet("color:#9ca3af; font-size:11px; padding:4px 0;")
                vlay.addWidget(sfr_note)

                sfr_btn_row = QHBoxLayout()
                sfr_btn = QPushButton("📄 Restore Selected File…")
                sfr_btn.setObjectName("secondary")
                sfr_btn.setToolTip("Restore only the selected file from this backup snapshot.")

                def _do_single_file_restore():
                    sel = tree.selectedItems()
                    if not sel:
                        QMessageBox.information(dlg, "No Selection",
                            "Please select a file in the list first.")
                        return
                    rel_path = sel[0].text(0)
                    if not rel_path or sel[0].text(2) == "deleted":
                        QMessageBox.warning(dlg, "Invalid Selection",
                            "The selected entry is a deleted file and cannot be restored.")
                        return
                    dest_dir = QFileDialog.getExistingDirectory(
                        dlg, f"Choose folder to restore  '{rel_path}'  into"
                    )
                    if not dest_dir:
                        return
                    _enc_key = watch.get("encrypt_key") or None
                    res = backup_engine.restore_single_file(
                        backup_dir  = backup_dir,
                        relative_path = rel_path,
                        target_path = dest_dir,
                        encrypt_key = _enc_key,
                        overwrite   = True,
                    )
                    if res.get("ok"):
                        QMessageBox.information(
                            dlg, "File Restored",
                            f"✔  Restored successfully\n\n"
                            f"File: {rel_path}\n"
                            f"Destination: {res['restored_to']}\n"
                            f"Size: {backup_engine._human_size(res['size_bytes'])}"
                        )
                    else:
                        QMessageBox.critical(
                            dlg, "Restore Failed",
                            f"Could not restore file:\n{res.get('error', 'Unknown error')}"
                        )

                sfr_btn.clicked.connect(_do_single_file_restore)
                sfr_btn_row.addWidget(sfr_btn)
                sfr_btn_row.addStretch()
                vlay.addLayout(sfr_btn_row)

                btns = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok)
                btns.accepted.connect(dlg.accept)
                vlay.addWidget(btns)
                dlg.exec()

            except Exception as e:
                QMessageBox.warning(self, "Preview Error", str(e))


        # Ask for target folder
        target = QFileDialog.getExistingDirectory(
            self, "Select Restore Destination Folder"
        )
        if not target:
            return

        # Check for restore conflicts
        overwrite = True
        if os.path.exists(target) and os.listdir(target):
            from PyQt6.QtWidgets import QDialog, QVBoxLayout, QRadioButton, QLabel, QDialogButtonBox
            dlg = QDialog(self)
            dlg.setWindowTitle("Restore Conflict")
            dlg.setModal(True)
            vlay = QVBoxLayout(dlg)
            vlay.addWidget(QLabel("The target folder is not empty. Choose how to handle conflicts:"))
            rb1 = QRadioButton("Overwrite existing files")
            rb1.setChecked(True)
            rb2 = QRadioButton("Skip files that already exist")
            rb3 = QRadioButton("Restore to new folder (add '_restored' suffix)")
            vlay.addWidget(rb1)
            vlay.addWidget(rb2)
            vlay.addWidget(rb3)
            btns = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
            btns.accepted.connect(dlg.accept)
            btns.rejected.connect(dlg.reject)
            vlay.addWidget(btns)
            if dlg.exec() == QDialog.DialogCode.Accepted:
                if rb1.isChecked():
                    overwrite = True
                elif rb2.isChecked():
                    overwrite = False
                elif rb3.isChecked():
                    target = target.rstrip(os.sep) + "_restored"
                    overwrite = True
            else:
                return

        # Confirm
        mode_label = "Full Chain Restore" if use_chain else "Single Snapshot Restore"
        conflict_msg = ""
        if overwrite:
            conflict_msg = "Existing files with the same name will be overwritten."
        elif not os.path.exists(target) or not os.listdir(target):
            conflict_msg = "Existing files with the same name will be overwritten."
        else:
            conflict_msg = "Existing files will be skipped."
        reply = QMessageBox.question(
            self, "Confirm Restore",
            f"Mode:  {mode_label}\n"
            f"Restore to:  {target}\n\n"
            f"{conflict_msg}\nContinue?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        self._append_log(f"Restoring backup: {watch['name']} >{target} ({mode_label}) …")

        # Disable restore button while running to prevent double-trigger
        if watch["id"] in self._cards:
            self._cards[watch["id"]].restore_btn.setEnabled(False)

        def _on_restore_progress(step, total, label):
            self._append_log(f"  ↳ Step {step}/{total}: {label}")

        def _on_restore_done(result):
            # Re-enable restore button
            if watch["id"] in self._cards:
                self._cards[watch["id"]].restore_btn.setEnabled(True)
            if result.get("ok"):
                steps = result.get("steps_applied")
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
            # Clean up temp dir
            if temp_dir:
                import shutil
                try:
                    shutil.rmtree(temp_dir)
                except Exception:
                    pass

        if use_chain:
            kwargs = dict(
                destination=dest,
                watch_id=watch["id"],
                target_path=target,
                up_to_backup_id=chosen_id,
                encrypt_key=watch.get("encrypt_key") or None,
                overwrite=overwrite,
            )
            worker = RestoreWorker("chain", kwargs, parent=self)
        else:
            kwargs = dict(
                backup_dir=backup_dir,
                target_path=target,
                encrypt_key=watch.get("encrypt_key") or None,
                overwrite=overwrite,
            )
            worker = RestoreWorker("single", kwargs, parent=self)

        worker.progress.connect(_on_restore_progress)
        worker.finished.connect(_on_restore_done)
        worker.finished.connect(worker.deleteLater)
        worker.start()

    def _restore_to_original(self, watch: dict):
        if not BACKEND_AVAILABLE:
            return

        # Sync mode: the destination IS the live copy — no restore needed.
        if watch.get("sync_mode", False):
            folder = self._watch_dest(watch)
            QMessageBox.information(self, "Restore  · Sync Mode",
                f"This watch uses sync mode.\n\n"
                f"Your files are stored directly at:\n{folder}\n\n"
                f"To recover a file, open that folder and copy it back manually.")
            return

        # Resolve destination — handles global remote types AND per-watch destinations.
        dest, _resolved_type, temp_dir = self._pick_restore_destination(watch)
        if dest is None:
            return   # user cancelled or download failed

        backups = backup_engine.list_backups(dest, watch["id"])
        if not backups:
            QMessageBox.warning(self, "Restore",
                f"No backups found for \"{watch['name']}\".\nRun a backup first.")
            return

        # Let user pick which backup to restore
        from PyQt6.QtWidgets import QInputDialog
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
            self, "Restore to Original Location",
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

        # Load manifest to get source_path
        manifest_path = os.path.join(backup_dir, "MANIFEST.json")
        try:
            with open(manifest_path, 'r') as f:
                manifest = json.load(f)
            source_path = manifest.get("source")
            if not source_path:
                QMessageBox.warning(self, "Restore Failed",
                    "Original location not recorded in this backup. Use the Restore… button to choose a target folder manually.")
                return
        except Exception as e:
            QMessageBox.warning(self, "Restore Failed",
                f"Could not read backup manifest: {e}")
            return

        # Confirm
        reply = QMessageBox.question(
            self, "Confirm Restore to Original Location",
            f"This will restore files to their original location:\n{source_path}\n\n"
            f"Existing files may be overwritten.\nContinue?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

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
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No | QMessageBox.StandardButton.Cancel
            )
            if mode_reply == QMessageBox.StandardButton.Cancel:
                return
            use_chain = (mode_reply == QMessageBox.StandardButton.Yes)
        else:
            use_chain = False

        self._append_log(f"Restoring to original location: {watch['name']} >{source_path} ({'Full Chain' if use_chain else 'Single Snapshot'}) …")

        # Disable restore button while running to prevent double-trigger
        if watch["id"] in self._cards:
            self._cards[watch["id"]].restore_btn.setEnabled(False)
            self._cards[watch["id"]].restore_original_btn.setEnabled(False)

        def _on_restore_progress(step, total, label):
            self._append_log(f"  ↳ Step {step}/{total}: {label}")

        def _on_restore_done(result):
            # Re-enable restore buttons
            if watch["id"] in self._cards:
                self._cards[watch["id"]].restore_btn.setEnabled(True)
                self._cards[watch["id"]].restore_original_btn.setEnabled(True)
            if result.get("ok"):
                steps = result.get("steps_applied")
                extra = f"\nChain steps applied:  {steps}" if steps is not None else ""
                QMessageBox.information(self, "Restore Complete",
                    f"▶  Restore complete\n\n"
                    f"Files restored:  {result.get('files_restored', 0)}\n"
                    f"Files skipped:   {result.get('skipped', 0)}\n"
                    f"Destination:     {source_path}{extra}"
                )
                self._append_log(
                    f"▶ Restore complete: {watch['name']}  · "
                    f"{result.get('files_restored', 0)} file(s) >{source_path}"
                )
            else:
                errors = result.get("errors", [])
                err_preview = "\n".join(errors[:5]) if errors else result.get("error", "Unknown error")
                QMessageBox.critical(self, "Restore Failed",
                    f"⚠  Restore failed\n\n{err_preview}")
                self._append_log(f"⚠ Restore failed: {watch['name']}")
            # Clean up temp dir
            if temp_dir:
                import shutil
                try:
                    shutil.rmtree(temp_dir)
                except Exception:
                    pass

        if use_chain:
            kwargs = dict(
                destination=dest,
                watch_id=watch["id"],
                target_path=source_path,
                up_to_backup_id=chosen_id,
                encrypt_key=watch.get("encrypt_key") or None,
                overwrite=True,
            )
            worker = RestoreWorker("chain", kwargs, parent=self)
        else:
            kwargs = dict(
                backup_dir=backup_dir,
                target_path=source_path,
                encrypt_key=watch.get("encrypt_key") or None,
                overwrite=True,
            )
            worker = RestoreWorker("single", kwargs, parent=self)

        worker.progress.connect(_on_restore_progress)
        worker.finished.connect(_on_restore_done)
        worker.finished.connect(worker.deleteLater)
        worker.start()

    # ── Admin ──────────────────────────────────────────────────────────────────

    def _validate_cloud_tokens(self):
        """Validate cloud tokens from MainWindow  · delegates to AdminPanel logic in background."""
        import threading
        def _check():
            try:
                from PyQt6.QtCore import QSettings, QTimer
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
        """Show tray and log warnings for expired tokens, and surface the persistent banner."""
        for provider in warnings:
            name = "Google Drive"
            if hasattr(self, "_tray"):
                self._tray.showMessage(
                    f"⚠ {name}  · Reconnect Required",
                    f"Your {name} token has expired.\n"
                    f"Open Settings >Cloud tab >Reconnect.",
                    QSystemTrayIcon.MessageIcon.Warning, 8000
                )
            self._append_log(
                f"⚠ {name} token expired  · go to Settings >Cloud tab to reconnect"
            )
            # Show the persistent in-window reconnect banner
            if hasattr(self, "gdrive_banner"):
                self.gdrive_banner.show()

    def _open_gdrive_reconnect(self):
        """Open the Settings (AdminPanel) dialog directly on the Cloud tab to reconnect Google Drive."""
        try:
            panel = AdminPanel(self.cfg, self)
            panel.watches_changed.connect(self._on_watches_changed)
            panel._tabs.setCurrentIndex(2)  # Cloud tab (0=General, 1=Watches, 2=Cloud)
            panel.exec()
            self._load_config()
            self._update_auto_label()
            self._update_stats()
            # If we re-opened settings, assume the user reconnected — hide the banner.
            # It will reappear on the next backup cycle if still disconnected.
            self.gdrive_banner.hide()
        except Exception as e:
            QMessageBox.information(
                self, "Reconnect Google Drive",
                f"Open Settings → Cloud tab → click Reconnect next to Google Drive.\n\n({e})"
            )

    def _open_admin(self):
        if not PasswordDialog.has_password():
            # First time  · prompt to set password
            reply = QMessageBox.question(self, "Set Admin Password",
                "No admin password is set. Would you like to set one now?\n"
                "(If you skip, any user can access admin settings)",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
            if reply == QMessageBox.StandardButton.Yes:
                dlg = PasswordDialog(self, mode="set")
                if dlg.exec() != QDialog.DialogCode.Accepted:
                    return

        dlg = PasswordDialog(self, mode="verify")
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return

        panel = AdminPanel(self.cfg, self)
        panel.watches_changed.connect(self._on_watches_changed)
        # Validate cloud tokens when opening admin panel so user sees warning immediately
        self._validate_cloud_tokens()
        panel.exec()

        self._load_config()
        self._update_auto_label()
        self._update_stats()

    def _quit_app(self):
        """Quit the application."""
        reply = QMessageBox.question(self, "Quit Backup System",
            "Are you sure you want to quit Backup System?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            if self._watcher_mgr:
                self._watcher_mgr.stop_all()
            # Persist history before exit
            if BACKEND_AVAILABLE:
                try:
                    config_manager.save_history(self._history_log)
                except Exception:
                    pass
            QApplication.quit()

    def _on_pause_backup_requested(self, watch_id: str):
        """Pause a running backup."""
        worker = self._workers.get(watch_id)
        if worker:
            worker.pause()
            self._append_log(f"⏸ Backup paused: {self._watch_name_for(watch_id)}")

    def _on_resume_backup_requested(self, watch_id: str):
        """Resume a paused backup."""
        worker = self._workers.get(watch_id)
        if worker:
            worker.resume()
            self._append_log(f"▶ Backup resumed: {self._watch_name_for(watch_id)}")

    def _on_cancel_requested(self, watch_id: str):
        """Cancel an in-progress backup for the given watch."""
        worker = self._workers.get(watch_id)
        if worker:
            worker.request_stop()
            self._append_log(f"⏹ Cancel requested for: {self._watch_name_for(watch_id)}")
        # Remember this was a deliberate user cancel so the auto-timer does
        # not immediately re-trigger a new backup for the same watch.
        self._user_cancelled_watches.add(watch_id)
        if watch_id in self._cards:
            self._cards[watch_id].cancel_btn.setEnabled(False)
            self._cards[watch_id].cancel_btn.setText("Cancelling…")

    def _on_open_backup_folder(self, watch_id: str):
        """Open the backup destination folder in the system file explorer.
        Only works for local destinations.
        """
        watch = next((w for w in self.cfg.get("watches", []) if w["id"] == watch_id), None)
        if not watch:
            return

        # Get the destination path for this watch
        dest_path = self._watch_dest(watch)
        if not dest_path:
            QMessageBox.warning(self, "No Destination",
                "No destination path is configured for this watch.")
            return

        # Check if the folder exists
        if not os.path.exists(dest_path):
            QMessageBox.warning(self, "Folder Not Found",
                f"The destination folder does not exist yet:\n{dest_path}\n\nRun a backup first to create it.")
            return

        # Open the folder using QDesktopServices
        try:
            from PyQt6.QtCore import QUrl
            from PyQt6.QtGui import QDesktopServices
            QDesktopServices.openUrl(QUrl.fromLocalFile(dest_path))
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Could not open folder:\n{e}")

    def _on_watch_settings_requested(self, watch: dict):
        """Open the EditWatchDialog for per-watch advanced settings (encryption, exclusions, hooks)."""
        wid = watch.get("id", "")
        # Get a fresh copy from config in case it was modified since the card was built
        live_watch = next((w for w in self.cfg.get("watches", []) if w["id"] == wid), watch)

        dlg = EditWatchDialog(live_watch, dest_type=self.cfg.get("dest_type", "local"), parent=self)
        # Stash cfg reference so _rotate_key can resolve the destination
        dlg._parent_cfg = self.cfg

        if dlg.exec() != QDialog.DialogCode.Accepted:
            return

        v = dlg.get_values()
        if BACKEND_AVAILABLE:
            try:
                config_manager.update_watch_meta(
                    self.cfg, wid,
                    name=v.get("name", live_watch.get("name", "")),
                    interval_min=v.get("interval_min", 0),
                    schedule_times=v.get("schedule_times", []),
                    compression=v.get("compression", False),
                    sync_mode=v.get("sync_mode", True),
                    destination=v.get("destination", "") or None,
                    max_file_size_mb=v.get("max_file_size_mb", 0),
                    max_backup_bytes=v.get("max_backup_bytes", 0),
                    skip_auto_backup=v.get("skip_auto_backup", False),
                    color=v.get("color", ""),
                    notes=v.get("notes", ""),
                    tags=v.get("tags", []),
                    exclude_patterns=v.get("exclude_patterns", []),
                    encrypt_key=v.get("encrypt_key", ""),
                    pre_backup_cmd=v.get("pre_backup_cmd", ""),
                    post_backup_cmd=v.get("post_backup_cmd", ""),
                )
                config_manager.save(self.cfg)
            except Exception as e:
                QMessageBox.critical(self, "Save Error", f"Could not save watch settings:\n{e}")
                return

        self._load_config()
        self._refresh_watches()
        self._update_auto_label()

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

    def _toggle_pause_all(self):
        """Pause or resume every watch at once (global toggle)."""
        watches = self.cfg.get("watches", [])
        if not watches:
            return

        # Decide target state: if ANY watch is running → pause all; if all paused → resume all
        any_running = any(not w.get("paused", False) for w in watches)
        target_paused = any_running  # True = we're about to pause everything

        for w in watches:
            wid = w["id"]
            if w.get("paused", False) != target_paused:
                self._on_pause_requested(wid, target_paused)
                # Also update card UI to reflect the new state
                card = self._cards.get(wid)
                if card:
                    w["paused"] = target_paused
                    card.update_watch(w)

        # Update sidebar button label
        if target_paused:
            self._pause_all_btn.setText("▶  Resume All Backups")
            self._pause_all_btn.setToolTip("Resume all watched folders (backups were globally paused).")
            self._append_log("⏸ All watches paused globally")
        else:
            self._pause_all_btn.setText("⏸  Pause All Backups")
            self._pause_all_btn.setToolTip(
                "Pause all watched folders at once.\n"
                "Useful before presentations or on slow connections.\n"
                "Click again to resume all watches."
            )
            self._append_log("▶ All watches resumed globally")

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
        queue = []
        try:
            if BACKEND_AVAILABLE:
                queue = config_manager.load_backup_queue()
        except Exception:
            pass
        self._history_window = HistoryWindow(
            list(self._history_log),
            list(self._backup_history),
            backup_queue=queue,
            cfg=self.cfg,
            parent=self,
        )
        self._history_window.show()
        self._history_window.raise_()

    def _open_global_dashboard(self):
        """Open the global backup trend dashboard."""
        dlg = GlobalTrendDialog(list(self._backup_history), parent=self)
        dlg.exec()

    def _open_logs(self):
        """Open the log viewer dialog."""
        log_dialog = LogViewerDialog(self)
        log_dialog.exec()

    # ── Window behavior ────────────────────────────────────────────────────────

    # ── Integrity Scheduler Handlers ───────────────────────────────────────────

    def _trigger_integrity_check_now(self):
        """Immediately trigger an integrity check for all watches, ignoring the schedule."""
        sched = getattr(self, "_integrity_scheduler", None)
        if sched is None:
            QMessageBox.information(
                self, "Integrity Check",
                "The integrity scheduler is not running.\n"
                "Enable scheduled integrity checks in Settings and restart the app."
            )
            return
        sched.run_now()
        self._append_log("🔍 Manual integrity check triggered — results will appear in the log.")
        if hasattr(self, "_tray"):
            self._tray.showMessage(
                APP_NAME,
                "Integrity check started. Results will appear in the Activity Log.",
                QSystemTrayIcon.MessageIcon.Information, 3000
            )

    def _on_integrity_result(self, watch_name: str, result: dict):
        """Called once per watch after its scheduled integrity check completes."""
        ok = result.get("valid") and result.get("manifest_ok", True)
        if ok:
            self._append_log(f"✔ Integrity OK: {watch_name}")
        else:
            missing   = result.get("missing_files", [])
            corrupted = result.get("corrupted_files", [])
            err       = result.get("error", "")
            detail    = ""
            if missing:
                detail += f"  Missing: {', '.join(missing[:3])}"
                if len(missing) > 3:
                    detail += f" (+{len(missing)-3} more)"
            if corrupted:
                detail += f"  Corrupted: {', '.join(corrupted[:3])}"
                if len(corrupted) > 3:
                    detail += f" (+{len(corrupted)-3} more)"
            if err:
                detail += f"  Error: {err}"
            self._append_log(f"⚠ Integrity FAILED: {watch_name}{detail}")
            if hasattr(self, "_tray"):
                self._tray.showMessage(
                    APP_NAME,
                    f"⚠ Integrity check failed for {watch_name}. Check Activity Log.",
                    QSystemTrayIcon.MessageIcon.Warning, 5000
                )

    def _on_integrity_run_finished(self, summary: dict):
        """Called after all watches have been integrity-checked in one scheduler run."""
        total  = summary.get("total", 0)
        passed = summary.get("passed", 0)
        failed = summary.get("failed", 0)
        if total > 0:
            self._append_log(
                f"✔ Integrity check complete: {passed}/{total} passed"
                + (f", {failed} failed" if failed else "")
            )

    def _on_disk_space_warning(self, free_gb: float):
        """Called when backup destination has low disk space."""
        self._append_log(f"⚠ Low disk space on backup destination: {free_gb:.1f} GB free")
        if hasattr(self, "_tray"):
            self._tray.showMessage(
                APP_NAME,
                f"⚠ Low disk space on backup destination: {free_gb:.1f} GB free",
                QSystemTrayIcon.MessageIcon.Warning, 5000
            )

    def closeEvent(self, event):
        """Minimize to tray instead of closing, or minimize to taskbar in window mode."""
        if hasattr(self, "_tray") and self._tray is not None:
            # Tray mode: hide to tray
            event.ignore()
            self.hide()
            self._tray.showMessage(
                APP_NAME,
                "Running in background. Click the tray icon to reopen.",
                QSystemTrayIcon.MessageIcon.Information, 2500
            )
        else:
            # Window mode: minimize to taskbar instead of closing
            event.ignore()
            self.showMinimized()

    def set_tray(self, tray):
        self._tray = tray
        if tray is None:
            # Window mode: show quit button
            self.quit_btn.show()
        else:
            # Tray mode: hide quit button
            self.quit_btn.hide()


# ══════════════════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════════════════════
# ── Global Backup Trend Dashboard ─────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

class _TrendChart(QWidget):
    """Draws a dual-layer bar chart: total bytes per day (blue) overlaid with
    success/failure counts, directly on top of a dark background."""

    def __init__(self, daily_data: list, parent=None):
        """daily_data: list of (date_str, bytes_mb, success, failure)"""
        super().__init__(parent)
        self.daily_data = daily_data
        self.setMinimumHeight(160)

    def paintEvent(self, event):          # noqa: N802
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        rect = self.rect()
        w, h = rect.width(), rect.height()
        padding_left, padding_right, padding_top, padding_bottom = 54, 12, 20, 30

        # Background
        painter.fillRect(rect, QColor("#0f172a"))

        data = self.daily_data
        if not data:
            painter.setPen(QColor("#6b7280"))
            painter.drawText(rect, Qt.AlignmentFlag.AlignCenter, "No backup data yet")
            return

        max_mb    = max((d[1] for d in data), default=1) or 1
        bar_area_w = w - padding_left - padding_right
        bar_area_h = h - padding_top - padding_bottom
        bar_w = bar_area_w / len(data)

        # Grid lines
        painter.setPen(QColor("#1e293b"))
        for i in range(1, 5):
            y = padding_top + bar_area_h - int(bar_area_h * i / 4)
            painter.drawLine(padding_left, y, w - padding_right, y)

        # Y-axis labels
        painter.setPen(QColor("#64748b"))
        painter.setFont(QFont("Arial", 8))
        for i in range(5):
            y = padding_top + bar_area_h - int(bar_area_h * i / 4)
            mb_label = f"{max_mb * i / 4:.0f}"
            painter.drawText(2, y + 4, 46, 14, Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter, mb_label)

        # Bars
        for idx, (date_str, mb, ok, fail) in enumerate(data):
            bx = padding_left + idx * bar_w
            bh = int((mb / max_mb) * bar_area_h) if max_mb > 0 else 0
            by = padding_top + bar_area_h - bh

            # Size bar (blue gradient)
            grad = QLinearGradient(0, by, 0, by + bh)
            grad.setColorAt(0, QColor("#3b82f6"))
            grad.setColorAt(1, QColor("#1d4ed8"))
            painter.fillRect(int(bx + 1), by, int(bar_w - 3), bh, QBrush(grad))

            # Thin success overlay (green top)
            if ok and bh > 0:
                painter.fillRect(int(bx + 1), by, int(bar_w - 3), 4, QColor("#22c55e"))

            # Failure accent (red notch at bottom)
            if fail:
                painter.fillRect(int(bx + 1), padding_top + bar_area_h - 6,
                                 int(bar_w - 3), 6, QColor("#ef4444"))

            # X-axis date label
            try:
                lbl = datetime.fromisoformat(date_str).strftime("%m/%d")
            except Exception:
                lbl = date_str[-5:]
            painter.setPen(QColor("#64748b"))
            painter.setFont(QFont("Arial", 7))
            painter.drawText(int(bx), h - padding_bottom + 4,
                             int(bar_w), padding_bottom - 4,
                             Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignTop, lbl)

        # Legend
        lx = padding_left
        painter.setFont(QFont("Arial", 8))
        for color, label in [(QColor("#3b82f6"), "Size (MB)"), (QColor("#22c55e"), "Success"),
                             (QColor("#ef4444"), "Failure")]:
            painter.fillRect(lx, 4, 10, 10, color)
            painter.setPen(QColor("#94a3b8"))
            painter.drawText(lx + 13, 13, label)
            lx += 80


class GlobalTrendDialog(QDialog):
    """Global backup trend dashboard — aggregates across all watches."""

    def __init__(self, backup_history: list, parent=None):
        super().__init__(parent)
        self.backup_history = backup_history
        self.setWindowTitle("📈 Global Backup Dashboard")
        self.setMinimumSize(760, 520)
        self._build_ui()

    # ── helpers ──────────────────────────────────────────────────────────────

    @staticmethod
    def _fmt_bytes(n: int) -> str:
        if n <= 0:
            return "0 B"
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if n < 1024:
                return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
            n /= 1024
        return f"{n:.1f} PB"

    def _aggregate(self):
        """Compute summary stats and per-day series from backup_history."""
        total, success, failure = 0, 0, 0
        total_bytes = 0
        daily: dict[str, dict] = {}   # date -> {bytes, success, failure}

        for entry in self.backup_history:
            total += 1
            st = (entry.get("status") or "").lower()
            if st == "success":
                success += 1
            elif st == "failure":
                failure += 1
            bc = entry.get("bytes_copied") or 0
            total_bytes += bc

            ts = entry.get("timestamp") or entry.get("time") or ""
            try:
                day = datetime.fromisoformat(ts).strftime("%Y-%m-%d")
            except Exception:
                day = "unknown"
            rec = daily.setdefault(day, {"bytes": 0, "success": 0, "failure": 0})
            rec["bytes"] += bc
            if st == "success":
                rec["success"] += 1
            elif st == "failure":
                rec["failure"] += 1

        # Build sorted daily list (last 60 days)
        sorted_days = sorted(daily.keys())[-60:]
        daily_series = [
            (d, daily[d]["bytes"] / (1024 * 1024), daily[d]["success"], daily[d]["failure"])
            for d in sorted_days
        ]

        # Watch breakdown
        watch_stats: dict[str, dict] = {}
        for entry in self.backup_history:
            wn = entry.get("watch_name") or "(unnamed)"
            rec = watch_stats.setdefault(wn, {"total": 0, "success": 0, "failure": 0, "bytes": 0})
            rec["total"] += 1
            st = (entry.get("status") or "").lower()
            if st == "success":
                rec["success"] += 1
            elif st == "failure":
                rec["failure"] += 1
            rec["bytes"] += entry.get("bytes_copied") or 0

        return total, success, failure, total_bytes, daily_series, watch_stats

    # ── UI ────────────────────────────────────────────────────────────────────

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(0)
        layout.setContentsMargins(0, 0, 0, 0)

        # Header bar
        header = QFrame()
        header.setObjectName("topbar")
        header.setFixedHeight(52)
        hl = QHBoxLayout(header)
        hl.setContentsMargins(20, 0, 16, 0)
        title = QLabel("Global Backup Trend Dashboard")
        title.setStyleSheet("font-size:14px; font-weight:700; color:#f1f3f9;")
        hl.addWidget(title)
        hl.addStretch()
        close_btn = QPushButton("✕")
        close_btn.setObjectName("secondary")
        close_btn.setFixedSize(32, 32)
        close_btn.setStyleSheet("padding: 0px; font-size: 15px;")
        close_btn.clicked.connect(self.close)
        hl.addWidget(close_btn)
        layout.addWidget(header)

        body = QWidget()
        bl = QVBoxLayout(body)
        bl.setContentsMargins(20, 16, 20, 16)
        bl.setSpacing(14)

        total, success, failure, total_bytes, daily_series, watch_stats = self._aggregate()
        rate = f"{100*success/total:.1f}%" if total else "—"
        cancelled = total - success - failure

        # ── KPI cards ────────────────────────────────────────────────────────
        cards_row = QHBoxLayout()
        for color, icon, label, value in [
            ("#2563eb", "🗄", "Total Runs",    str(total)),
            ("#22c55e", "✅", "Success Rate",  rate),
            ("#ef4444", "❌", "Failures",      str(failure)),
            ("#f59e0b", "⏹", "Cancelled",     str(cancelled)),
            ("#8b5cf6", "💾", "Total Backed Up", self._fmt_bytes(total_bytes)),
        ]:
            card = QFrame()
            card.setObjectName("card")
            card.setFixedHeight(74)
            cl = QVBoxLayout(card)
            cl.setContentsMargins(14, 8, 14, 8)
            cl.setSpacing(2)
            top_row = QHBoxLayout()
            ic = QLabel(icon)
            ic.setStyleSheet("font-size:16px;")
            top_row.addWidget(ic)
            lbl = QLabel(label)
            lbl.setStyleSheet("color:#6b7280; font-size:10px;")
            top_row.addWidget(lbl)
            top_row.addStretch()
            cl.addLayout(top_row)
            val = QLabel(value)
            val.setStyleSheet(f"color:{color}; font-size:20px; font-weight:800;")
            cl.addWidget(val)
            cards_row.addWidget(card)
        bl.addLayout(cards_row)

        # ── Trend chart ───────────────────────────────────────────────────────
        chart_frame = QFrame()
        chart_frame.setObjectName("card")
        cfl = QVBoxLayout(chart_frame)
        cfl.setContentsMargins(12, 10, 12, 10)
        chart_title = QLabel("Backup size & outcome per day  (last 60 days)")
        chart_title.setStyleSheet("color:#6b7280; font-size:11px; font-weight:700;")
        cfl.addWidget(chart_title)
        chart = _TrendChart(daily_series)
        chart.setMinimumHeight(170)
        cfl.addWidget(chart)
        bl.addWidget(chart_frame)

        # ── Per-watch breakdown table ─────────────────────────────────────────
        watches_lbl = QLabel("Per-watch breakdown")
        watches_lbl.setStyleSheet("color:#6b7280; font-size:11px; font-weight:700; text-transform:uppercase;")
        bl.addWidget(watches_lbl)

        tbl = QTableWidget(len(watch_stats), 5)
        tbl.setHorizontalHeaderLabels(["Watch", "Total", "✅ OK", "❌ Fail", "Total Size"])
        tbl.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        tbl.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        tbl.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        tbl.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        tbl.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        tbl.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        tbl.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        tbl.setAlternatingRowColors(True)
        for r, (wname, wdata) in enumerate(sorted(watch_stats.items())):
            tbl.setItem(r, 0, QTableWidgetItem(wname))
            tbl.setItem(r, 1, QTableWidgetItem(str(wdata["total"])))
            tbl.setItem(r, 2, QTableWidgetItem(str(wdata["success"])))
            tbl.setItem(r, 3, QTableWidgetItem(str(wdata["failure"])))
            tbl.setItem(r, 4, QTableWidgetItem(self._fmt_bytes(wdata["bytes"])))
        bl.addWidget(tbl)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        scroll.setWidget(body)
        layout.addWidget(scroll)


# ── History Window ─────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

class HistoryWindow(QDialog):
    """Full change history table  · all edits across all watches."""

    def __new__(cls, *args, **kwargs):
        # Allow object.__new__(HistoryWindow) in unit tests (bypasses QDialog init).
        return super().__new__(cls)

    def __init__(self, history: list, backup_history: list, backup_queue: list = None, cfg: dict = None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("History")
        self.setMinimumSize(900, 580)
        self.resize(1100, 680)
        self._all_history = history          # change-history list of dicts
        self._backup_history = backup_history
        self._backup_queue = backup_queue or []
        self._cfg = cfg or {}
        self._build_ui()
        self._populate_changes(history)

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
        title = QLabel("📋  History")
        title.setStyleSheet("font-size:15px; font-weight:700; color:#f1f3f9;")
        hl.addWidget(title)
        hl.addStretch()

        export_btn = QPushButton("Export History…")
        export_btn.setObjectName("secondary")
        export_btn.clicked.connect(self._export_csv)
        hl.addWidget(export_btn)

        close_btn = QPushButton("✕")
        close_btn.setObjectName("secondary")
        close_btn.setFixedSize(32, 32)
        close_btn.setStyleSheet("padding: 0px; font-size: 15px;")
        close_btn.clicked.connect(self.close)
        hl.addWidget(close_btn)
        layout.addWidget(header)

        # Tab container
        self.tabs = QTabWidget()
        self.tabs.tabBar().setExpanding(False)
        self.tabs.tabBar().setElideMode(Qt.TextElideMode.ElideNone)
        layout.addWidget(self.tabs)

        # Tab 1: Change History  (builds self.table)
        self._build_change_history_tab()

        # Tab 2: Backup History  (builds self.backup_table)
        self._build_backup_history_tab()

        # Tab 3: Backup Queue  (builds self.queue_table)
        self._build_queue_tab()

        # Tab 4: Cross-watch File Search
        self._build_file_search_tab()

    def _build_change_history_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(0)
        layout.setContentsMargins(0, 0, 0, 0)

        # Filter bar
        filter_bar = QFrame()
        filter_bar.setFixedHeight(50)
        fl = QHBoxLayout(filter_bar)
        fl.setContentsMargins(20, 0, 20, 0)
        fl.setSpacing(12)

        self.filter_input = QLineEdit()
        self.filter_input.setPlaceholderText("Filter by file, user, machine…")
        self.filter_input.setFixedWidth(240)
        self.filter_input.textChanged.connect(self._filter_changes)
        fl.addWidget(self.filter_input)

        self.type_filter = QComboBox()
        self.type_filter.addItems(["All Types", "modified", "added", "deleted", "renamed"])
        self.type_filter.setFixedWidth(120)
        self.type_filter.currentTextChanged.connect(self._filter_changes)
        fl.addWidget(self.type_filter)

        # Date range filters
        from_label = QLabel("From:")
        from_label.setStyleSheet("color:#9ca3af; font-size:11px;")
        fl.addWidget(from_label)

        self.date_from = QDateEdit()
        self.date_from.setCalendarPopup(True)
        self.date_from.setDate(QDate(1900, 1, 1))  # Special date to indicate no filter
        self.date_from.dateChanged.connect(self._filter_changes)
        fl.addWidget(self.date_from)

        to_label = QLabel("To:")
        to_label.setStyleSheet("color:#9ca3af; font-size:11px;")
        fl.addWidget(to_label)

        self.date_to = QDateEdit()
        self.date_to.setCalendarPopup(True)
        self.date_to.setDate(QDate(1900, 1, 1))  # Special date to indicate no filter
        self.date_to.dateChanged.connect(self._filter_changes)
        fl.addWidget(self.date_to)

        clear_dates_btn = QPushButton("Clear dates")
        clear_dates_btn.setObjectName("secondary")
        clear_dates_btn.setMinimumWidth(100)
        clear_dates_btn.clicked.connect(self._clear_dates)
        fl.addWidget(clear_dates_btn)

        fl.addStretch()
        layout.addWidget(filter_bar)

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
        hh.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        hh.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        hh.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        hh.setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        hh.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        hh.setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)
        hh.setSectionResizeMode(6, QHeaderView.ResizeMode.ResizeToContents)
        self.table.verticalHeader().setVisible(False)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setAlternatingRowColors(True)
        self.table.setStyleSheet(
            self.table.styleSheet() +
            "QTableWidget { alternate-background-color: #1e2128; }"
        )
        layout.addWidget(self.table)

        self.tabs.addTab(tab, "Change History")

    def _build_backup_history_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(0)
        layout.setContentsMargins(0, 0, 0, 0)

        # Filter bar
        filter_bar = QFrame()
        filter_bar.setFixedHeight(50)
        fl = QHBoxLayout(filter_bar)
        fl.setContentsMargins(20, 0, 20, 0)
        fl.setSpacing(12)

        self.backup_filter_input = QLineEdit()
        self.backup_filter_input.setPlaceholderText("Search by watch name or date…")
        self.backup_filter_input.setFixedWidth(240)
        self.backup_filter_input.textChanged.connect(self._filter_backups)
        fl.addWidget(self.backup_filter_input)

        self.status_filter = QComboBox()
        self.status_filter.addItems(["All", "Success", "Failed", "Cancelled"])
        self.status_filter.setFixedWidth(120)
        self.status_filter.currentTextChanged.connect(self._filter_backups)
        fl.addWidget(self.status_filter)

        clear_filters_btn = QPushButton("Clear Filters")
        clear_filters_btn.setObjectName("secondary")
        clear_filters_btn.setFixedWidth(100)
        clear_filters_btn.clicked.connect(self._clear_backup_filters)
        fl.addWidget(clear_filters_btn)

        fl.addStretch()
        layout.addWidget(filter_bar)

        # ── Table ────────────────────────────────────────────────────────────
        self.backup_table = QTableWidget(0, 6)
        self.backup_table.setHorizontalHeaderLabels([
            "Started", "Watch", "Status", "Files", "Size", "Duration"
        ])
        hh = self.backup_table.horizontalHeader()
        hh.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        hh.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        hh.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        hh.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        hh.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        hh.setSectionResizeMode(5, QHeaderView.ResizeMode.Stretch)
        self.backup_table.verticalHeader().setVisible(False)
        self.backup_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.backup_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.backup_table.setAlternatingRowColors(True)
        self.backup_table.setStyleSheet(
            self.backup_table.styleSheet() +
            "QTableWidget { alternate-background-color: #1e2128; }"
        )
        layout.addWidget(self.backup_table)

        self._populate_backups(self._backup_history)
        self.tabs.addTab(tab, "Backup History")

    def _build_queue_tab(self):
        """Build the Queue tab — shows items currently waiting to be backed up."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(0)
        layout.setContentsMargins(0, 0, 0, 0)

        # ── Toolbar ──────────────────────────────────────────────────────────
        toolbar = QFrame()
        toolbar.setFixedHeight(50)
        tl = QHBoxLayout(toolbar)
        tl.setContentsMargins(20, 0, 20, 0)
        tl.setSpacing(12)

        queue_title_lbl = QLabel("⏳  Pending backup queue")
        queue_title_lbl.setStyleSheet("font-size:13px; font-weight:600; color:#9ca3af;")
        tl.addWidget(queue_title_lbl)
        tl.addStretch()

        self._queue_count_lbl = QLabel("")
        self._queue_count_lbl.setStyleSheet("color:#6b7280; font-size:11px;")
        tl.addWidget(self._queue_count_lbl)

        refresh_btn = QPushButton("Refresh")
        refresh_btn.setObjectName("secondary")
        refresh_btn.setFixedWidth(80)
        refresh_btn.clicked.connect(self._refresh_queue_from_disk)
        tl.addWidget(refresh_btn)
        layout.addWidget(toolbar)

        # ── Info bar ─────────────────────────────────────────────────────────
        info_bar = QFrame()
        info_bar.setStyleSheet("background:#141720; border-bottom:1px solid #2e3340;")
        info_bar.setFixedHeight(34)
        il = QHBoxLayout(info_bar)
        il.setContentsMargins(20, 0, 20, 0)
        note = QLabel(
            "Items shown here will be retried automatically the next time BackupSys starts. "
            "This queue is stored in backup_queue.json inside the data directory."
        )
        note.setStyleSheet("color:#6b7280; font-size:10px;")
        il.addWidget(note)
        layout.addWidget(info_bar)

        # ── Table ────────────────────────────────────────────────────────────
        self.queue_table = QTableWidget(0, 3)
        self.queue_table.setHorizontalHeaderLabels(["Watch name / ID", "Triggered by", "Queued at"])
        hh = self.queue_table.horizontalHeader()
        hh.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        hh.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        hh.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        self.queue_table.verticalHeader().setVisible(False)
        self.queue_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.queue_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.queue_table.setAlternatingRowColors(True)
        self.queue_table.setStyleSheet(
            self.queue_table.styleSheet() +
            "QTableWidget { alternate-background-color: #1e2128; }"
        )
        layout.addWidget(self.queue_table)

        self._populate_queue(self._backup_queue)
        # Show item count in tab label
        label = f"Queue  ({len(self._backup_queue)})" if self._backup_queue else "Queue  (empty)"
        self.tabs.addTab(tab, label)

    def _build_file_search_tab(self):
        """Tab 4: Cross-watch file search — scans all snapshot manifests."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(0)
        layout.setContentsMargins(0, 0, 0, 0)

        # ── Search bar ───────────────────────────────────────────────────────
        search_bar = QFrame()
        search_bar.setFixedHeight(54)
        sl = QHBoxLayout(search_bar)
        sl.setContentsMargins(20, 0, 20, 0)
        sl.setSpacing(10)

        self._fs_input = QLineEdit()
        self._fs_input.setPlaceholderText("Search filename across all watches…  e.g. report.docx")
        self._fs_input.setMinimumWidth(280)
        self._fs_input.returnPressed.connect(self._run_file_search)
        sl.addWidget(self._fs_input)

        # Watch filter
        self._fs_watch_combo = QComboBox()
        self._fs_watch_combo.setFixedWidth(180)
        self._fs_watch_combo.addItem("All watches", userData=None)
        for w in self._cfg.get("watches", []):
            self._fs_watch_combo.addItem(w.get("name", w["id"]), userData=w["id"])
        sl.addWidget(self._fs_watch_combo)

        search_btn = QPushButton("🔍  Search")
        search_btn.setObjectName("primary")
        search_btn.setFixedWidth(100)
        search_btn.clicked.connect(self._run_file_search)
        sl.addWidget(search_btn)

        self._fs_status = QLabel("")
        self._fs_status.setStyleSheet("color:#6b7280; font-size:11px;")
        sl.addWidget(self._fs_status)
        sl.addStretch()
        layout.addWidget(search_bar)

        # ── Results table ────────────────────────────────────────────────────
        self._fs_table = QTableWidget(0, 5)
        self._fs_table.setHorizontalHeaderLabels([
            "Watch", "Backup Date", "File Path", "Size", "Backup Dir"
        ])
        hh = self._fs_table.horizontalHeader()
        hh.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        hh.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        hh.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        hh.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        hh.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        self._fs_table.verticalHeader().setVisible(False)
        self._fs_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._fs_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._fs_table.setAlternatingRowColors(True)
        self._fs_table.setStyleSheet(
            self._fs_table.styleSheet() +
            "QTableWidget { alternate-background-color: #1e2128; }"
        )
        self._fs_table.setSortingEnabled(True)
        layout.addWidget(self._fs_table)

        self.tabs.addTab(tab, "🔍 File Search")

    def _run_file_search(self):
        """Scan every backup manifest for files matching the search query."""
        query = self._fs_input.text().strip().lower()
        if not query:
            return

        global_dest = self._cfg.get("destination", "")
        filter_wid  = self._fs_watch_combo.currentData()

        # Build a watch-id → name lookup and collect all distinct destinations
        watch_names = {}
        dest_set: set = set()
        if global_dest:
            dest_set.add(global_dest)
        for w in self._cfg.get("watches", []):
            watch_names[w["id"]] = w.get("name", w["id"])
            wd = w.get("destination", "").strip()
            if wd:
                dest_set.add(wd)

        if not dest_set:
            self._fs_status.setText("No destination configured.")
            return

        self._fs_table.setSortingEnabled(False)
        self._fs_table.setRowCount(0)
        matches   = 0
        scanned   = 0
        errors    = 0

        for dest in dest_set:
            dest_path = Path(dest)
            if not dest_path.exists():
                continue
            try:
                for backup_dir in sorted(dest_path.iterdir(), reverse=True):
                    if not backup_dir.is_dir():
                        continue
                    manifest_p = backup_dir / "MANIFEST.json"
                    if not manifest_p.exists():
                        continue
                    scanned += 1
                    try:
                        with open(manifest_p, "r", encoding="utf-8") as f:
                            manifest = json.load(f)
                    except Exception:
                        errors += 1
                        continue

                    wid = manifest.get("watch_id", "")
                    if filter_wid and wid != filter_wid:
                        continue

                    watch_label = watch_names.get(wid, wid or backup_dir.name)
                    ts_raw = manifest.get("timestamp", "")
                    try:
                        ts_display = datetime.fromisoformat(ts_raw).strftime("%Y-%m-%d %H:%M")
                    except Exception:
                        ts_display = ts_raw

                    for entry in manifest.get("changes", []):
                        if entry.get("type") not in ("added", "modified"):
                            continue
                        rel_path = entry.get("path", "")
                        filename = Path(rel_path).name.lower()
                        if query not in filename and query not in rel_path.lower():
                            continue

                        size_bytes = entry.get("size", 0)
                        if size_bytes >= 1024 * 1024:
                            size_h = f"{size_bytes // (1024*1024)} MB"
                        elif size_bytes >= 1024:
                            size_h = f"{size_bytes // 1024} KB"
                        else:
                            size_h = f"{size_bytes} B"

                        row = self._fs_table.rowCount()
                        self._fs_table.insertRow(row)
                        self._fs_table.setItem(row, 0, QTableWidgetItem(watch_label))
                        self._fs_table.setItem(row, 1, QTableWidgetItem(ts_display))
                        self._fs_table.setItem(row, 2, QTableWidgetItem(rel_path))
                        self._fs_table.setItem(row, 3, QTableWidgetItem(size_h))
                        self._fs_table.setItem(row, 4, QTableWidgetItem(str(backup_dir)))
                        matches += 1

            except Exception as e:
                self._fs_status.setText(f"Error scanning {dest}: {e}")
                errors += 1

        self._fs_table.setSortingEnabled(True)
        noun = "match" if matches == 1 else "matches"
        detail = f"  ({scanned} snapshots scanned)" if scanned else "  (no snapshots found)"
        if errors:
            detail += f"  ⚠ {errors} unreadable"
        self._fs_status.setText(f'{matches} {noun} for \u201c{query}\u201d{detail}')

    def _populate_queue(self, queue: list):
        """Fill the queue table from a list of queue-item dicts."""
        self.queue_table.setRowCount(0)
        for item in queue:
            row = self.queue_table.rowCount()
            self.queue_table.insertRow(row)

            # Watch name — look it up from watch_id if no watch_name stored
            watch_label = item.get("watch_name") or item.get("watch_id", "unknown")
            triggered   = item.get("triggered_by", "")
            queued_at   = item.get("queued_at", "")

            def _cell(text, clr=None):
                cell = QTableWidgetItem(str(text))
                cell.setFlags(cell.flags() & ~Qt.ItemFlag.ItemIsEditable)
                if clr:
                    cell.setForeground(QColor(clr))
                return cell

            self.queue_table.setItem(row, 0, _cell(watch_label))
            self.queue_table.setItem(row, 1, _cell(triggered,  "#f59e0b"))
            self.queue_table.setItem(row, 2, _cell(queued_at,  "#9ca3af"))

        self.queue_table.resizeRowsToContents()
        count = len(queue)
        self._queue_count_lbl.setText(f"{count} item{'s' if count != 1 else ''} pending")

        # Update tab label
        idx = self.tabs.indexOf(self.queue_table.parent())
        if idx >= 0:
            label = f"Queue  ({count})" if count else "Queue  (empty)"
            self.tabs.setTabText(idx, label)

    def _refresh_queue_from_disk(self):
        """Re-read backup_queue.json from disk and refresh the table."""
        try:
            if BACKEND_AVAILABLE:
                import config_manager as _cm
                self._backup_queue = _cm.load_backup_queue()
            else:
                self._backup_queue = []
        except Exception:
            self._backup_queue = []
        self._populate_queue(self._backup_queue)

    def refresh_queue(self, queue: list):
        """Called by MainWindow to live-update the queue tab after a change."""
        self._backup_queue = queue
        self._populate_queue(queue)

    def _populate_backups(self, entries: list):
        self.backup_table.setRowCount(len(entries))
        for i, e in enumerate(reversed(entries)):
            started = e.get("started_at", "")
            try:
                started = datetime.fromisoformat(started).strftime("%Y-%m-%d %H:%M")
            except Exception:
                pass

            watch = e.get("watch_name", "")
            status = e.get("status", "")
            files = e.get("file_count", 0)
            size_bytes = e.get("size_bytes", 0)
            size_h = f"{size_bytes // 1024} KB" if size_bytes < 1024*1024 else f"{size_bytes // (1024*1024)} MB"
            duration = ""
            if e.get("started_at") and e.get("finished_at"):
                try:
                    start = datetime.fromisoformat(e.get("started_at"))
                    end = datetime.fromisoformat(e.get("finished_at"))
                    dur = end - start
                    duration = f"{dur.seconds // 60}m {dur.seconds % 60}s"
                except Exception:
                    pass

            def _item(text, clr=None):
                item = QTableWidgetItem(str(text))
                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                if clr:
                    item.setForeground(QColor(clr))
                return item

            status_color = {
                "success": "#22c55e",
                "failed": "#ef4444",
                "cancelled": "#f59e0b"
            }.get(status.lower(), "#9ca3af")

            self.backup_table.setItem(i, 0, _item(started))
            self.backup_table.setItem(i, 1, _item(watch))
            self.backup_table.setItem(i, 2, _item(status, status_color))
            self.backup_table.setItem(i, 3, _item(files))
            self.backup_table.setItem(i, 4, _item(size_h))
            self.backup_table.setItem(i, 5, _item(duration))

        self.backup_table.resizeRowsToContents()

    def _populate_changes(self, entries: list):
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
                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                if clr:
                    item.setForeground(QColor(clr))
                return item

            self.table.setItem(i, 0, _item(ts))
            self.table.setItem(i, 1, _item(watch, "#9ca3af"))
            type_item = _item(f"{icon}  {etype}", color)
            type_item.setFont(QFont("Segoe UI", 11, QFont.Weight.Bold))
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

    def _filter_changes(self):
        text      = self.filter_input.text().lower()
        type_sel  = self.type_filter.currentText()
        date_from = self.date_from.date()
        date_to   = self.date_to.date()
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

            # Date range filtering
            if date_from.year() != 1900 or date_to.year() != 1900:  # If dates are set (not the special "no filter" date)
                try:
                    entry_date = datetime.fromisoformat(e.get("timestamp", "")).date()
                    if date_from.year() != 1900 and entry_date < date_from.toPyDate():
                        continue
                    if date_to.year() != 1900 and entry_date > date_to.toPyDate():
                        continue
                except (ValueError, AttributeError):
                    # If we can't parse the date, include the entry (don't filter it out)
                    pass

            filtered.append(e)

        self._populate_changes(filtered)
        filter_active = text or type_sel != "All Types" or date_from.year() != 1900 or date_to.year() != 1900
        if filter_active:
            self.result_lbl.setText(f"Showing {len(filtered)} of {len(self._all_history)}")
        else:
            self.result_lbl.setText("")

    def _filter_backups(self):
        text = self.backup_filter_input.text().lower()
        status_sel = self.status_filter.currentText()
        filtered = []

        for e in self._backup_history:
            if status_sel != "All" and e.get("status", "").lower() != status_sel.lower():
                continue
            searchable = " ".join([
                e.get("watch_name", ""),
                e.get("started_at", ""),
                e.get("finished_at", ""),
            ]).lower()
            if text and text not in searchable:
                continue
            filtered.append(e)

        self._populate_backups(filtered)

    def _clear_backup_filters(self):
        self.backup_filter_input.setText("")
        self.status_filter.setCurrentText("All")
        self._filter_backups()

    def _clear_dates(self):
        """Clear the date filters by setting them to null/empty."""
        # Set dates to current date as default, but we'll treat invalid dates as "no filter"
        self.date_from.setDate(QDate.currentDate().addDays(-30))
        self.date_to.setDate(QDate.currentDate())
        # Actually, let's use special dates to indicate "no filter"
        self.date_from.setDate(QDate(1900, 1, 1))  # Special date to indicate no filter
        self.date_to.setDate(QDate(1900, 1, 1))    # Special date to indicate no filter
        self._filter_changes()

    def _export_csv(self):
        path, _ = QFileDialog.getSaveFileName(
            self, "Export History", "backupsys_history.csv", "CSV Files (*.csv)"
        )
        if not path:
            return
        try:
            import csv
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "watch_name",
                    "watch_id",
                    "backup_id",
                    "status",
                    "started_at",
                    "finished_at",
                    "file_count",
                    "size_bytes",
                    "destination",
                    "error",
                ])
                for entry in self._backup_history:
                    writer.writerow([
                        entry.get("watch_name", ""),
                        entry.get("watch_id", ""),
                        entry.get("backup_id", ""),
                        entry.get("status", ""),
                        entry.get("started_at", ""),
                        entry.get("finished_at", ""),
                        entry.get("file_count", 0),
                        entry.get("size_bytes", 0),
                        entry.get("destination", ""),
                        entry.get("error", ""),
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
            item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            if clr:
                item.setForeground(QColor(clr))
            return item

        self.table.insertRow(0)
        self.table.setItem(0, 0, _item(ts))
        self.table.setItem(0, 1, _item(entry.get("watch_name", ""), "#9ca3af"))
        type_item = _item(f"{icon}  {etype}", color)
        type_item.setFont(QFont("Segoe UI", 11, QFont.Weight.Bold))
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
    def __init__(self, app: QApplication, window_mode=False):
        self.app    = app
        self.window = MainWindow()
        self.window_mode = window_mode

        if not window_mode:
            self.tray = QSystemTrayIcon()
            self.tray.setIcon(make_tray_icon("ok"))
            self.tray.setToolTip(APP_NAME)

            menu = QMenu()

            open_action = QAction("Open Dashboard", menu)
            open_action.triggered.connect(self._show_window)

            backup_action = QAction("⚡  Backup All Now", menu)
            backup_action.triggered.connect(self.window._backup_all)

            integrity_action = QAction("🔍  Run Integrity Check Now", menu)
            integrity_action.triggered.connect(self.window._trigger_integrity_check_now)

            admin_action = QAction("🔧 Admin Settings", menu)
            admin_action.triggered.connect(self.window._open_admin)

            history_action = QAction("📋  Change History", menu)
            history_action.triggered.connect(self.window._open_history)

            menu.addAction(open_action)
            menu.addSeparator()
            menu.addAction(backup_action)
            menu.addAction(integrity_action)
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
        else:
            # Window mode: modify title and set up for taskbar minimization
            self.window.setWindowTitle(f"{APP_NAME} (no system tray — running in window mode)")
            self.window.set_tray(None)  # No tray available

    def _show_window(self):
        saved_ss = self.app.styleSheet()
        self.app.setStyleSheet("")
        self.window.show()
        self.app.setStyleSheet(saved_ss)
        self.window.raise_()
        self.window.activateWindow()

    def _on_tray_activated(self, reason):
        if not self.window_mode:
            if reason == QSystemTrayIcon.ActivationReason.Trigger:  # single click
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
        if not self.window_mode:
            self.tray.hide()
        self.app.quit()


# ══════════════════════════════════════════════════════════════════════════════
# ── Entry Point ────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

def _acquire_single_instance_lock():
    """
    Prevent multiple instances of the app running simultaneously.
    Returns a file handle that must be kept open for the lifetime of the process.
    Returns None only if another instance is confirmed running.
    """
    import tempfile
    lock_path = Path(tempfile.gettempdir()) / "backupsys.lock"
    try:
        if os.name == "nt":
            import msvcrt
            fh = open(lock_path, "w")
            try:
                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
                return fh
            except OSError:
                fh.close()
                return None
        else:
            import fcntl
            fh = open(lock_path, "w")
            try:
                fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return fh
            except OSError:
                fh.close()
                return None
    except Exception as e:
        logger.warning(f"[lock] exception acquiring lock (allowing start): {e}")
        return "FALLBACK"


def main():
    import faulthandler
    if sys.stderr is not None:
        faulthandler.enable()
    else:
        try:
            _fh_log = open(os.path.join(os.path.expanduser("~"), "backupsys_fault.log"), "a")
            faulthandler.enable(file=_fh_log)
        except Exception:
            pass
    # ── Global crash handler — catches unhandled exceptions in the Qt main thread ──
    # Without this, crashes in the .exe produce no output (stdout is hidden).
    def _excepthook(exc_type, exc_value, exc_tb):
        import traceback
        tb_str = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
        logger.critical(f"Unhandled exception:\n{tb_str}")

        # ── SMB authentication errors are non-fatal — show a friendly warning
        # instead of crashing the app. WinError 1326 = wrong username/password.
        err_str = str(exc_value)
        is_smb_auth = (
            isinstance(exc_value, OSError) and
            ("1326" in err_str or "1219" in err_str or
             "ユーザー名またはパスワード" in err_str or
             "wrong password" in err_str.lower() or
             "logon failure" in err_str.lower())
        )
        if is_smb_auth:
            logger.warning(f"[smb] Authentication error (non-fatal): {exc_value}")
            try:
                _tmp_app = QApplication.instance() or QApplication(sys.argv)
                QMessageBox.warning(
                    None,
                    f"{APP_NAME} — Network Share Unavailable",
                    f"A watched network share could not be accessed.\n\n"
                    f"Path: {err_str}\n\n"
                    f"The app will continue running. To fix this permanently, save your\n"
                    f"NAS credentials in Windows Credential Manager so they survive reboots."
                )
            except Exception:
                pass
            return  # don't exit — app continues normally

        # ── Crash notification — attempt email + webhook ───────────────────
        try:
            _crash_cfg = config_manager.load()
            _crash_subject = f"{APP_NAME} crashed: {exc_type.__name__}"
            _crash_body    = (
                f"{APP_NAME} v{APP_VERSION} encountered an unhandled error and stopped.\n\n"
                f"Error: {exc_type.__name__}: {exc_value}\n\n"
                f"Traceback:\n{tb_str}"
            )
            _email_cfg = _crash_cfg.get("email_config", {})
            if _email_cfg.get("enabled") and _email_cfg.get("notify_on_failure", True):
                _send_email_notification(_crash_cfg, _crash_subject, _crash_body)

            _wh_url = _crash_cfg.get("webhook_url", "").strip()
            if _wh_url:
                _send_webhook(_crash_cfg, {
                    "status":     "crashed",
                    "watch_name": APP_NAME,
                    "error":      f"{exc_type.__name__}: {exc_value}",
                    "traceback":  tb_str[:2000],
                })
        except Exception as _ne:
            logger.debug(f"Crash notification failed: {_ne}")

        try:
            _tmp_app = QApplication.instance() or QApplication(sys.argv)
            QMessageBox.critical(
                None,
                f"{APP_NAME} — Unexpected Error",
                f"An unexpected error occurred. Please check the log file for details.\n\n"
                f"{exc_type.__name__}: {exc_value}\n\n"
                f"Log: {_setup_logging()}"
            )
        except Exception:
            pass  # if the dialog itself fails, at least the log was written
        sys.exit(1)
    sys.excepthook = _excepthook

    # Single-instance guard
    _lock_fh = _acquire_single_instance_lock()
    if _lock_fh is None:
        _tmp_app = QApplication.instance() or QApplication(sys.argv)
        QMessageBox.warning(None, APP_NAME,
            "Backup System is already running.\n\nCheck your system tray.")
        sys.exit(0)

    app = QApplication(sys.argv)
    app.setApplicationName(APP_NAME)
    app.setOrganizationName(SETTINGS_ORG)
    app.setQuitOnLastWindowClosed(False)   # keep alive when window is closed

    # ── Theme selection ───────────────────────────────────────────────────────
    _s = QSettings(SETTINGS_ORG, SETTINGS_APP)
    _active_theme = _s.value("theme", "dark")   # "dark" or "light"
    if _active_theme not in ("dark", "light"):
        _active_theme = "dark"                  # sanitise stale ""/auto values
    app.setStyleSheet(LIGHT_STYLE if _active_theme == "light" else DARK_STYLE)
    app.setProperty("theme", _active_theme)

    if not QSystemTrayIcon.isSystemTrayAvailable():
        # QMessageBox.critical(None, APP_NAME,
        #     "System tray is not available on this system.")
        # sys.exit(1)
        window_mode = True
    else:
        window_mode = False

    tray_app = TrayApp(app, window_mode=window_mode)

    s = QSettings(SETTINGS_ORG, SETTINGS_APP)
    first_launch = not s.value("launched_before", False)
    if first_launch:
        s.setValue("launched_before", True)
        QTimer.singleShot(800, tray_app.window._maybe_run_setup_wizard)

    QTimer.singleShot(0, tray_app._show_window)
    ret = app.exec()
    sys.exit(ret)


class LogViewerDialog(QDialog):
    """Full log viewer dialog with refresh capability."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("View Logs")
        self.setMinimumSize(700, 500)
        self.resize(900, 600)
        self._log_file = Path(os.environ.get("BACKUPSYS_DATA_DIR", Path(__file__).parent)) / "logs" / "backupsys.log"
        self._build_ui()
        self._load_logs()

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(0)
        layout.setContentsMargins(0, 0, 0, 0)

        # ── Header bar ───────────────────────────────────────────────────────
        header = QFrame()
        header.setObjectName("topbar")
        header.setFixedHeight(48)
        hl = QHBoxLayout(header)
        hl.setContentsMargins(16, 0, 16, 0)

        title = QLabel("📜  Application Logs")
        title.setStyleSheet("font-size:14px; font-weight:700; color:#f1f3f9;")
        hl.addWidget(title)

        log_path_lbl = QLabel(f"({self._log_file})")
        log_path_lbl.setStyleSheet("color:#6b7280; font-size:11px;")
        hl.addWidget(log_path_lbl)
        hl.addStretch()

        refresh_btn = QPushButton("🔄 Refresh")
        refresh_btn.setObjectName("secondary")
        refresh_btn.clicked.connect(self._load_logs)
        hl.addWidget(refresh_btn)

        hl.addSpacing(8)

        close_btn = QPushButton("✕ Close")
        close_btn.setObjectName("secondary")
        close_btn.clicked.connect(self.close)
        hl.addWidget(close_btn)

        layout.addWidget(header)

        # ── Log viewer ───────────────────────────────────────────────────────
        self.log_text = QPlainTextEdit()
        self.log_text.setReadOnly(True)
        # Set monospace font
        font = QFont("Courier New" if sys.platform == "win32" else "Courier")
        font.setPointSize(9)
        self.log_text.setFont(font)
        self.log_text.setStyleSheet("background:#0a0e18; color:#d1d5db; border:none;")
        layout.addWidget(self.log_text)

    def _load_logs(self):
        """Load and display log file content."""
        try:
            if self._log_file.exists():
                content = self._log_file.read_text(encoding='utf-8', errors='replace')
                self.log_text.setPlainText(content)
                # Auto-scroll to bottom
                cursor = self.log_text.textCursor()
                try:
                    _end = QTextCursor.MoveOperation.End
                except AttributeError:
                    _end = QTextCursor.End
                cursor.movePosition(_end)
                self.log_text.setTextCursor(cursor)
            else:
                self.log_text.setPlainText(f"Log file not found: {self._log_file}")
        except Exception as e:
            self.log_text.setPlainText(f"Error reading log file: {e}")


if __name__ == "__main__":
    main()