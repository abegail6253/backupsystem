"""
setup_wizard.py — One-click setup for BackupSys
================================================
Run this once on any PC to get BackupSys ready:

    python setup_wizard.py

What it does:
  1. Checks Python version (needs 3.8+)
  2. Installs all required packages from requirements_desktop.txt
  3. Creates a blank .env file (for secure credential storage)
  4. Creates a starter config.json if none exists
  5. Offers to add BackupSys to Windows startup
  6. Verifies the install by importing core modules
  7. Prints a summary of what to do next

No internet access needed after pip install.
No admin rights needed (installs to user's local pip cache).
"""

import sys
import os
import subprocess
import json
import shutil
from pathlib import Path

HERE = Path(__file__).resolve().parent

# ── Colour helpers (works without colorama) ──────────────────────────────────
def _c(text, code):
    if sys.platform == "win32":
        try:
            import ctypes
            ctypes.windll.kernel32.SetConsoleMode(
                ctypes.windll.kernel32.GetStdHandle(-11), 7
            )
        except Exception:
            pass
    return f"\033[{code}m{text}\033[0m"

OK  = lambda t: print(_c(f"  ✅  {t}", "32"))
ERR = lambda t: print(_c(f"  ❌  {t}", "31"))
WRN = lambda t: print(_c(f"  ⚠   {t}", "33"))
INF = lambda t: print(f"  ℹ   {t}")
HDR = lambda t: print(f"\n{'─'*60}\n  {t}\n{'─'*60}")

# ── Step 1: Python version ────────────────────────────────────────────────────
def check_python():
    HDR("Step 1 — Python version")
    if sys.version_info < (3, 8):
        ERR(f"Python 3.8+ required. You have {sys.version.split()[0]}.")
        ERR("Download: https://www.python.org/downloads/")
        sys.exit(1)
    OK(f"Python {sys.version.split()[0]}")

# ── Step 2: Install packages ──────────────────────────────────────────────────
def install_packages():
    HDR("Step 2 — Installing packages")

    req_file = HERE / "requirements_desktop.txt"
    if not req_file.exists():
        ERR(f"requirements_desktop.txt not found in {HERE}")
        ERR("Make sure setup_wizard.py is in the same folder as the project files.")
        sys.exit(1)

    INF("Running: pip install -r requirements_desktop.txt --user")
    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", "-r", str(req_file), "--user"],
        capture_output=False,   # show live pip output so user can see progress
    )
    if result.returncode != 0:
        ERR("pip install failed. Check the output above for details.")
        ERR("Common fix: run  python -m pip install --upgrade pip  first.")
        sys.exit(1)
    OK("All packages installed")

# ── Step 3: Create .env file ──────────────────────────────────────────────────
def create_env():
    HDR("Step 3 — Secure credentials (.env)")

    env_path = HERE / ".env"
    if env_path.exists():
        WRN(".env already exists — not overwriting.")
        INF("Edit it manually to set your credentials.")
        return

    env_content = """\
# BackupSys Secure Credentials
# ─────────────────────────────────────────────────────────────────────────────
# This file is loaded at startup.  NEVER commit it to Git (.gitignore already
# excludes it).  Move credentials out of config.json and into here instead.
#
# IMPORTANT after reading this file:
#   If you have real credentials in config.json right now, move them here
#   and clear the corresponding fields in config.json immediately.

# SMTP password for email notifications (avoids storing it in config.json)
# BACKUPSYS_EMAIL_PASSWORD=your_smtp_app_password_here

# Per-watch encryption key (replace <WATCH_ID> with the actual watch ID)
# BACKUPSYS_ENCRYPT_KEY_<WATCH_ID>=your_44char_fernet_key_here

# Global fallback encryption key (used for watches with no explicit key)
# BACKUPSYS_ENCRYPT_KEY_DEFAULT=your_44char_fernet_key_here

# Override the data directory (config.json, snapshots, logs)
# BACKUPSYS_DATA_DIR=C:\\BackupSysData

# Google Drive OAuth (optional)
# GDRIVE_CLIENT_ID=
# GDRIVE_CLIENT_SECRET=

"""
    env_path.write_text(env_content, encoding="utf-8")
    OK(".env created")
    WRN("Open .env and add your credentials — keep this file private!")

# ── Step 4: Create starter config.json ───────────────────────────────────────
def create_config():
    HDR("Step 4 — Default configuration")

    # Always use the same path as config_manager so we don't create a blank
    # config in the wrong location when BACKUPSYS_DATA_DIR is set.
    try:
        import config_manager as _cm
        cfg_path = _cm.CONFIG_PATH
    except Exception:
        cfg_path = HERE / "config.json"

    if cfg_path.exists():
        WRN("config.json already exists — not overwriting.")
        return

    default = {
        "destination": str(HERE / "backups"),
        "dest_type": "local",
        "auto_backup": False,
        "interval_min": 30,
        "interval_unit": "minutes",
        "retention_days": 30,
        "compression_enabled": False,
        "auto_retry": False,
        "retry_delay_min": 5,
        "max_backup_mbps": 0.0,
        "webhook_url": "",
        "webhook_on_success": False,
        "backup_schedule_times": [],
        "email_config": {
            "enabled": False,
            "smtp_host": "",
            "smtp_port": 587,
            "smtp_use_ssl": False,
            "username": "",
            "password": "",    # use BACKUPSYS_EMAIL_PASSWORD in .env instead
            "from_addr": "",
            "to_addr": "",
            "notify_on_success": False,
            "notify_on_failure": True,
        },
        "dest_sftp": {
            "host": "", "port": 22, "username": "",
            "remote_path": "", "key_path": "", "key_passphrase": "",
        },
        "dest_ftp": {"host": "", "port": 21, "username": "", "use_tls": True},
        "dest_https": {"url": "", "token": "", "verify_ssl": True},
        "dest_webdav": {"url": "", "username": "", "webdav_root": "", "remote_path": "", "verify_ssl": True},
        "dest_rclone": {"remote": "", "path": "/backups"},
        "default_exclude_patterns": [
            ".git", ".gitignore", "__pycache__", "node_modules",
            "*.pyc", "*.tmp", ".DS_Store", "Thumbs.db",
            "*.lock", "*.db", "*.sqlite*", ".env",
        ],
        "watches": [],
        "storage_type": "local",
    }
    (HERE / "backups").mkdir(exist_ok=True)
    (HERE / "snapshots").mkdir(exist_ok=True)
    cfg_path.write_text(json.dumps(default, indent=2), encoding="utf-8")
    OK("config.json created with safe defaults")
    OK("backups/ and snapshots/ directories created")

# ── Step 5: Windows startup (optional) ───────────────────────────────────────
def offer_startup():
    if sys.platform != "win32":
        return
    HDR("Step 5 — Windows startup (optional)")

    INF("BackupSys can be added to your Windows startup so it launches automatically")
    INF("when you log in.  This is done by writing a value to the Windows Registry:")
    INF("  HKCU\\Software\\Microsoft\\Windows\\CurrentVersion\\Run")
    INF("BackupSys will start minimised to the system tray — no window will appear.")
    INF("You can remove this at any time from Settings → General → 'Start on login'")
    INF("or via Windows Settings → Apps → Startup.")
    print()

    ans = input("  [ Opt-in ] Add BackupSys to Windows startup? [y/N] ").strip().lower()
    if ans != "y":
        INF("Skipped. You can enable this later from the app's Settings → General → 'Start on login'.")
        return

    try:
        import winreg
        key = winreg.OpenKey(
            winreg.HKEY_CURRENT_USER,
            r"Software\Microsoft\Windows\CurrentVersion\Run",
            0, winreg.KEY_SET_VALUE,
        )
        # Launch minimised to tray (pythonw = no console window)
        pythonw = Path(sys.executable).parent / "pythonw.exe"
        if not pythonw.exists():
            pythonw = sys.executable
        cmd = f'"{pythonw}" "{HERE / "desktop_app.py"}"'
        winreg.SetValueEx(key, "BackupSys", 0, winreg.REG_SZ, cmd)
        winreg.CloseKey(key)
        OK("Added to Windows startup")
    except Exception as e:
        WRN(f"Could not set startup key: {e}")
        INF("You can do this manually: Settings → Apps → Startup  or  regedit")

# ── Step 6: Verify install ────────────────────────────────────────────────────
def verify_install():
    HDR("Step 6 — Verifying install")

    checks = [
        ("PyQt5",         "PyQt5"),
        ("watchdog",      "watchdog"),
        ("cryptography",  "cryptography"),
        ("paramiko",      "paramiko"),
        ("psutil",        "psutil"),
    ]
    all_ok = True
    for label, mod in checks:
        try:
            __import__(mod)
            OK(label)
        except ImportError:
            ERR(f"{label} — not found (check pip output above)")
            all_ok = False

    optional = [
        ("google.oauth2",        "google-auth (optional — for Google Drive backup)"),
    ]
    for mod, label in optional:
        try:
            __import__(mod)
            OK(label)
        except ImportError:
            INF(f"{label} — not installed (install if needed)")

    return all_ok

# ── Step 7: Summary ───────────────────────────────────────────────────────────
def print_summary(all_ok: bool):
    HDR("Setup complete!")

    if all_ok:
        print(_c("""
  BackupSys is ready to run.

  To start the app:
      python desktop_app.py
  Or double-click desktop_app.py in File Explorer.

  A tray icon will appear in the system notification area.
  Right-click it to open the dashboard or settings.

  Next steps:
    1. Open Settings and add a Watch (the folder you want to back up)
    2. Set your backup destination (local folder, SFTP, FTP, or cloud)
    3. Enable Auto-Backup and set your preferred interval
    4. (Recommended) Add credentials to .env instead of config.json

  To build a standalone .exe (no Python required on target PC):
      pip install pyinstaller>=6.0.0
      python build_exe.py
  Then copy dist/BackupSystem/ to any Windows PC.
""", "32"))
    else:
        WRN("Some packages are missing.  Check the pip output above and re-run setup_wizard.py.")


# ── Programmatic entry point (called from desktop_app.py on first launch) ────
def run_for_app() -> bool:
    """
    Non-interactive setup called by desktop_app.py when no config exists yet.

    Performs only the file-creation steps safe to run from a GUI context
    (no stdin, no sys.exit, no package installs):
      - Creates a blank .env file if one does not exist
      - Creates a starter config.json if one does not exist

    Returns True on success, False if an exception occurred.
    The caller (desktop_app.py) is responsible for showing any GUI feedback.
    """
    try:
        create_env()
        create_config()
        return True
    except Exception:
        return False


def offer_startup_gui() -> bool:
    """
    Write the platform startup entry without any stdin prompt.
    Called by desktop_app.py after the user accepts a QMessageBox prompt.
    Returns True if the entry was written, False if an error occurred.
    """
    try:
        if sys.platform == "win32":
            import winreg
            pythonw = Path(sys.executable).parent / "pythonw.exe"
            if not pythonw.exists():
                pythonw = Path(sys.executable)
            cmd = f'"{pythonw}" "{HERE / "desktop_app.py"}"'
            key = winreg.OpenKey(
                winreg.HKEY_CURRENT_USER,
                r"Software\Microsoft\Windows\CurrentVersion\Run",
                0, winreg.KEY_SET_VALUE,
            )
            winreg.SetValueEx(key, "BackupSys", 0, winreg.REG_SZ, cmd)
            winreg.CloseKey(key)
        elif sys.platform == "darwin":
            plist_dir = Path.home() / "Library" / "LaunchAgents"
            plist_dir.mkdir(parents=True, exist_ok=True)
            plist_lines = [
                '<?xml version="1.0" encoding="UTF-8"?>',
                '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"'
                ' "http://www.apple.com/DTDs/PropertyList-1.0.dtd">',
                '<plist version="1.0"><dict>',
                '  <key>Label</key><string>com.backupsys.app</string>',
                '  <key>ProgramArguments</key><array>',
                f'    <string>{sys.executable}</string>',
                f'    <string>{HERE / "desktop_app.py"}</string>',
                '  </array>',
                '  <key>RunAtLoad</key><true/>',
                '  <key>KeepAlive</key><false/>',
                '</dict></plist>',
            ]
            (plist_dir / "com.backupsys.app.plist").write_text(
                "\n".join(plist_lines) + "\n", encoding="utf-8"
            )
        else:  # Linux / XDG
            autostart = Path.home() / ".config" / "autostart"
            autostart.mkdir(parents=True, exist_ok=True)
            lines = [
                "[Desktop Entry]",
                "Type=Application",
                "Name=BackupSys",
                f"Exec={sys.executable} {HERE / 'desktop_app.py'}",
                "Hidden=false",
                "NoDisplay=false",
                "X-GNOME-Autostart-enabled=true",
            ]
            (autostart / "backupsys.desktop").write_text(
                "\n".join(lines) + "\n", encoding="utf-8"
            )
        return True
    except Exception:
        return False



# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(_c("\n  BackupSys Setup Wizard", "1"))
    print(f"  Project folder: {HERE}\n")

    check_python()
    install_packages()
    create_env()
    create_config()
    offer_startup()
    all_ok = verify_install()
    print_summary(all_ok)