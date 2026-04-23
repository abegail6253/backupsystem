"""
build_exe.py — Package Backup System as a Windows .exe

HOW TO USE:
1.  pip install pyinstaller
2.  Run from anywhere — paths are resolved relative to this script:
        python build_exe.py
    OR:
        python C:/projects/backupsys/build_exe.py

Output: dist/BackupSystem/BackupSystem.exe
"""

import subprocess
import sys
import os
from pathlib import Path

# ── Python version guard ──────────────────────────────────────────────────────
# PyInstaller 6+ requires Python 3.8+; PyQt5 on Windows needs 3.8+ as well.
if sys.version_info < (3, 8):
    print(f"❌ Python 3.8 or newer is required to build BackupSystem.")
    print(f"   You are running Python {sys.version.split()[0]}.")
    print("   Please upgrade: https://www.python.org/downloads/")
    sys.exit(1)

# ── PyInstaller availability check ───────────────────────────────────────────
try:
    import importlib.util
    if importlib.util.find_spec("PyInstaller") is None:
        raise ImportError
except ImportError:
    print("❌ PyInstaller is not installed.")
    print("   Run:  pip install pyinstaller>=6.0.0")
    print("   Then re-run this script.")
    sys.exit(1)

APP_NAME   = "BackupSystem"
MAIN_FILE  = "desktop_app.py"
ICON_FILE  = "icon.ico"      # Optional — place an icon.ico in the same folder

# ── Resolve all paths relative to THIS script, not the cwd ──────────────────
# This ensures `python build_exe.py` works correctly regardless of which
# directory you run it from (e.g. from Desktop, from CI, from a task scheduler).
SCRIPT_DIR   = Path(__file__).resolve().parent
MAIN_PATH    = SCRIPT_DIR / MAIN_FILE
CONFIG_PATH  = SCRIPT_DIR / "config.json"
ICON_PATH    = SCRIPT_DIR / ICON_FILE
SNAP_DIR     = SCRIPT_DIR / "snapshots"

# Verify the main entry point exists before invoking PyInstaller
if not MAIN_PATH.exists():
    print(f"❌ Cannot find {MAIN_FILE} in {SCRIPT_DIR}")
    print("   Make sure build_exe.py is in the same folder as desktop_app.py")
    sys.exit(1)

# ── Safety: never bundle a live config that could contain passwords/tokens ────
# We always generate a clean blank config for the .exe.
# The real config.json (with the user's settings) lives at runtime in
# BACKUPSYS_DATA_DIR (or next to the .exe), not inside the bundle.
_BLANK_CONFIG = {
    "destination": "./backups",
    "dest_type": "local",
    "auto_backup": False,
    "interval_min": 30,
    "interval_unit": "minutes",
    "retention_days": 30,
    "compression_enabled": False,
    "auto_retry": True,
    "retry_delay_min": 5,
    "max_backup_mbps": 0,
    "webhook_url": "",
    "webhook_on_success": False,
    "watches": [],
    "default_exclude_patterns": [
        "*.tmp", "*.log", "~$*", ".DS_Store", "Thumbs.db",
        "__pycache__", "*.pyc", ".git"
    ],
    "email_config": {
        "enabled": False, "smtp_host": "", "smtp_port": 587,
        "smtp_use_ssl": False, "username": "", "password": "",
        "from_addr": "", "to_addr": "",
        "notify_on_success": False, "notify_on_failure": True
    },
    "dest_sftp":  {"host": "", "port": 22, "username": "", "password": "", "remote_path": ""},
    "dest_ftp":   {"host": "", "port": 21, "username": "", "password": "", "use_tls": True},
    "dest_smb":   {"server": "", "share": "", "username": "", "password": "", "remote_path": ""},
    "dest_https": {"url": "", "token": "", "verify_ssl": True},
}

import json as _json
import re   as _re

# Warn loudly if the live config looks like it contains real credentials,
# but proceed with the blank config regardless — we never bundle the live one.
if CONFIG_PATH.exists():
    try:
        _live = _json.loads(CONFIG_PATH.read_text(encoding="utf-8", errors="ignore"))
        _suspicious = []
        def _scan(obj, path=""):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    _scan(v, f"{path}.{k}")
            elif isinstance(obj, str) and len(obj) > 8:
                if _re.search(r'(?i)(password|secret|token|key)', path) and obj.strip():
                    _suspicious.append(f"  {path} = {'*' * min(len(obj), 6)}...")
        _scan(_live)
        if _suspicious:
            print("⚠  Live config.json appears to contain credentials (NOT bundled):")
            for s in _suspicious:
                print(s)
            print("   The .exe will start with a blank config — users set it up on first run.\n")
    except Exception:
        pass

# Always write a clean blank config for PyInstaller to bundle.
_bundle_config = SCRIPT_DIR / ".bundle_config.json"
_bundle_config.write_text(_json.dumps(_BLANK_CONFIG, indent=2))
CONFIG_PATH = _bundle_config   # point PyInstaller at the clean copy
print(f"✅ Bundling clean blank config (live config.json excluded from .exe)")

# Ensure snapshots directory exists so PyInstaller can include it
SNAP_DIR.mkdir(parents=True, exist_ok=True)

# ── Change working directory so PyInstaller output lands next to the source ──
os.chdir(SCRIPT_DIR)

args = [
    sys.executable, "-m", "PyInstaller",
    "--name",       APP_NAME,
    "--onedir",                          # Single folder (faster startup than --onefile)
    "--windowed",                        # No console window
    "--noconfirm",                       # Overwrite without asking
    "--clean",
    # Use absolute paths so --add-data works from any cwd
    "--add-data",   f"{CONFIG_PATH}{os.pathsep}.",
    "--add-data",   f"{SNAP_DIR}{os.pathsep}snapshots",
    "--add-data",   f"{SCRIPT_DIR / 'transport_utils.py'}{os.pathsep}.",
    "--add-data",   f"{SCRIPT_DIR / 'notification_utils.py'}{os.pathsep}.",
    # Watchdog needs --collect-all to bundle its platform observer correctly
    "--collect-all", "watchdog",
    "--hidden-import", "watchdog.observers",
    "--hidden-import", "watchdog.observers.polling",
    "--hidden-import", "watchdog.events",
    # cryptography: --hidden-import alone misses compiled OpenSSL backends;
    # --collect-all ensures Fernet/AES/HMAC all work inside the .exe.
    "--collect-all", "cryptography",
    "--hidden-import", "cryptography.fernet",
    "--hidden-import", "cryptography.hazmat.primitives.ciphers",
    "--hidden-import", "cryptography.hazmat.backends",
    "--hidden-import", "cryptography.hazmat.backends.openssl",
    "--hidden-import", "psutil",
    "--hidden-import", "email.mime.text",
    "--hidden-import", "email.mime.multipart",
    "--hidden-import", "smtplib",
    "--hidden-import", "ftplib",
    "--hidden-import", "urllib.request",
    "--hidden-import", "winreg",
    # SFTP destination: --collect-all picks up Paramiko's transport/auth modules
    # and its compiled crypto dependencies automatically.
    "--collect-all", "paramiko",
    "--hidden-import", "paramiko.transport",
    "--hidden-import", "paramiko.auth_handler",
    "--hidden-import", "paramiko.ecdsakey",
    "--hidden-import", "paramiko.ed25519key",
    # SMB destination support
    "--hidden-import", "smbprotocol",
    "--hidden-import", "smbprotocol.connection",
    "--hidden-import", "smbprotocol.session",
    "--hidden-import", "smbprotocol.tree",
    "--hidden-import", "smbprotocol.open",
    str(MAIN_PATH),
]

# Add icon if it exists
if ICON_PATH.exists():
    args += ["--icon", str(ICON_PATH)]

print(f"Building .exe from: {SCRIPT_DIR}")
print(f"Entry point:        {MAIN_PATH.name}")
print(f"Bundling config:    {CONFIG_PATH.name}")
print()

result = subprocess.run(args)

# Clean up the temporary blank config we wrote for bundling
try:
    _bundle_config.unlink(missing_ok=True)
except Exception:
    pass

if result.returncode == 0:
    print(f"\n✅ Build complete!  {SCRIPT_DIR / 'dist' / APP_NAME / (APP_NAME + '.exe')}")
    print("\nTo distribute:")
    print(f"  1. Copy the entire  dist/{APP_NAME}/  folder to the target PC")
    print(f"  2. Run  {APP_NAME}.exe")
    print(f"  3. A tray icon will appear — right-click to open settings")
    print()
    print("🔒 Security reminder:")
    print("   The .exe contains a BLANK config — no passwords or tokens.")
    print("   Users configure their own credentials on first run.")
    print("   Never add .env or config.json to the dist/ folder.")
else:
    print("\n❌ Build failed. Check the output above.")
    # Still clean up the temp file on failure
    sys.exit(1)