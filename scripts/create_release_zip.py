"""
create_release_zip.py — Build a clean, secret-free source release zip.

Run from the project root:
    python scripts/create_release_zip.py

Output: dist/BackupSys_<version>_source.zip

This script ONLY includes files that are safe to share publicly.
It explicitly blocks every file that could contain credentials, tokens,
or runtime state — even if those files are present in the project folder.
"""

import zipfile
import hashlib
import os
import sys
import json
import re
from pathlib import Path
from datetime import datetime

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT     = Path(__file__).resolve().parent.parent  # go up from scripts/ to project root

# ── Version ───────────────────────────────────────────────────────────────────
def _get_app_version():
    app_py = ROOT / "desktop_app.py"
    if app_py.exists():
        try:
            with open(app_py, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip().startswith("APP_VERSION"):
                        match = re.search(r'APP_VERSION\s*=\s*["\']([^"\']+)["\']', line)
                        if match:
                            return match.group(1)
        except UnicodeDecodeError:
            with open(app_py, 'r', encoding='latin-1') as f:
                for line in f:
                    if line.strip().startswith("APP_VERSION"):
                        match = re.search(r'APP_VERSION\s*=\s*["\']([^"\']+)["\']', line)
                        if match:
                            return match.group(1)
    return "1.0.0"

VERSION  = _get_app_version()
DIST_DIR = ROOT / "dist"

# ── Explicit allowlist ────────────────────────────────────────────────────────
SAFE_SOURCE_FILES = [
    "desktop_app.py",
    "backup_engine.py",
    "config_manager.py",
    "transport_utils.py",
    "notification_utils.py",
    "watcher.py",
    "integrity_scheduler.py",
    "setup_wizard.py",
    "backupsys_cli.py",
    "backupsys_api.py",
    "requirements_api.txt",
    "Procfile",
    "build_exe.py",
    "scripts/create_release_zip.py",
    ".github/workflows/ci.yml",
    "credential_store.py",
    "templates/dashboard.html",
    "templates/dashboard_login.html",
    "README.md",
    "CHANGELOG.md",
    "requirements_desktop.txt",
    "privacy.html",
    ".gitignore",
    "LICENSE",
    "pyproject.toml",
    "icon_256.png",
    "icon_64.png",
]

SAFE_TEST_FILES = [
    "tests/__init__.py",
    "tests/conftest.py",
    "tests/test_backup_engine.py",
    "tests/test_backup_engine_integration.py",
    "tests/test_backupsys_api.py",
    "tests/test_backupsys_cli.py",
    "tests/test_config_manager.py",
    "tests/test_credential_store.py",
    "tests/test_desktop_app.py",
    "tests/test_desktop_app_theme.py",
    "tests/test_build_exe.py",
    "tests/test_setup_wizard.py",
    "tests/test_encryption.py",
    "tests/test_integrity_scheduler.py",
    "tests/test_notification_utils.py",
    "tests/test_transport_utils.py",
    "tests/test_watcher.py",
]

# ── Template files (generated from memory — never copied from disk) ───────────
_SEP = "═" * 78
TEMPLATE_FILES = {
    ".env.example": (
        "# Copy this file to .env and fill in your values.\n"
        "# NEVER commit the real .env to git or include it in a release zip.\n"
        "#\n"
        "# This file covers BOTH the desktop app and the API server.\n"
        "# You only need the section(s) relevant to how you are running BackupSys.\n"
        "\n"
        f"# {_SEP}\n"
        "# DESKTOP APP  (desktop_app.py / backupsys_cli.py)\n"
        f"# {_SEP}\n"
        "\n"
        "# Google Drive OAuth credentials (from Google Cloud Console)\n"
        "GDRIVE_CLIENT_ID=your-client-id.apps.googleusercontent.com\n"
        "GDRIVE_CLIENT_SECRET=your-client-secret\n"
        "\n"
        "# Optional: override the data directory for config/snapshots/logs\n"
        "# BACKUPSYS_DATA_DIR=C:\\Users\\you\\AppData\\Local\\BackupSys\n"
        "\n"
        "# Optional: SMTP password (avoids storing it in config.json)\n"
        "# BACKUPSYS_EMAIL_PASSWORD=your-smtp-app-password\n"
        "\n"
        "# Optional: per-watch Fernet encryption keys\n"
        "# BACKUPSYS_ENCRYPT_KEY_DEFAULT=your-fernet-key\n"
        "# BACKUPSYS_ENCRYPT_KEY_<WATCH_ID>=per-watch-key\n"
        "\n"
        "# Optional: webhook URL for success/failure notifications (overrides config.json)\n"
        "# BACKUPSYS_WEBHOOK_URL=https://your-webhook-endpoint.example.com/notify\n"
        "\n"
        "# Optional: pause auto-backups on metered network connections (Windows only)\n"
        "# BACKUPSYS_PAUSE_ON_METERED=true\n"
        "\n"
        "# Optional: run in portable mode — store config/logs next to the .exe instead of AppData\n"
        "# BACKUPSYS_PORTABLE=true\n"
        "\n"
        f"# {_SEP}\n"
        "# API SERVER  (backupsys_api.py / Railway / Render / Fly.io)\n"
        f"# {_SEP}\n"
        "#\n"
        "# NOTE: backupsys_api.py calls load_dotenv() automatically at startup\n"
        "# (via python-dotenv, which is listed in requirements_api.txt).  Values placed\n"
        "# in this file are picked up without any manual \"source .env\" step — just copy\n"
        "# .env.example to .env, fill in your values, and start the server normally.\n"
        "# On hosting platforms (Railway, Render, Fly.io) prefer the platform's own\n"
        "# environment variable dashboard over a .env file so secrets are never on disk.\n"
        "\n"
        "# REQUIRED — all API requests are rejected if unset.\n"
        "# Generate with: python -c \"import secrets; print(secrets.token_hex(32))\"\n"
        "BACKUPSYS_API_KEY=your-api-key-min-32-chars\n"
        "\n"
        "# REQUIRED — signs Flask session cookies. If unset, a random key is generated\n"
        "# at startup, which logs everyone out on every restart/redeploy.\n"
        "# Generate with: python -c \"import secrets; print(secrets.token_hex(32))\"\n"
        "BACKUPSYS_SESSION_SECRET=generate-with-python-secrets-token-hex-32\n"
        "\n"
        "# Recommended — dedicated dashboard password. If unset, BACKUPSYS_API_KEY is\n"
        "# used as the fallback, which means rotating the API key also logs you out.\n"
        "BACKUPSYS_DASHBOARD_PASSWORD=your-dashboard-password\n"
        "\n"
        "# Set to true when running behind a reverse proxy (Railway, Render, Fly.io,\n"
        "# nginx, etc.) so the rate limiter reads the real client IP from\n"
        "# X-Forwarded-For. Leave unset for direct deployments to prevent IP spoofing.\n"
        "# BACKUPSYS_TRUSTED_PROXY=true\n"
        "\n"
        "# Path for the SQLite database. Default: ./backupsys.db\n"
        "# ⚠️  WARNING: the default is a RELATIVE path inside the container filesystem.\n"
        "# On Railway/Render/Fly.io a redeploy WIPES this file and loses all backup events.\n"
        "# Mount a persistent volume and point this at it:\n"
        "BACKUPSYS_DB_PATH=/data/backupsys.db\n"
        "\n"
        "# Directory where uploaded backup files are stored. Default: ./backupsys_files\n"
        "# ⚠️  WARNING: same as above — relative path = ephemeral storage = data loss on redeploy.\n"
        "# Use the same persistent volume mount:\n"
        "BACKUPSYS_FILES_DIR=/data/backupsys_files\n"
        "\n"
        "# Log level: DEBUG, INFO, WARNING, ERROR, CRITICAL. Default: INFO\n"
        "# LOG_LEVEL=INFO\n"
        "\n"
        "# CORS — which browser origins may call the API. Leave unset to disable CORS\n"
        "# headers entirely (safest for server-to-server use).\n"
        "# ALLOWED_ORIGINS=https://your-dashboard.example.com\n"
        "\n"
        "# ── Upload rate-limiting (optional overrides) ──────────────────────────────────\n"
        "# Max upload requests per IP per sliding window.  Defaults: 30 req / 60 s.\n"
        "# BACKUPSYS_UPLOAD_RATE_LIMIT=30\n"
        "# BACKUPSYS_UPLOAD_RATE_WINDOW_SEC=60\n"
        "\n"
        "# ── Total storage quota for uploaded backup files (optional) ──────────────────\n"
        "# Set to a byte count to cap total disk usage under BACKUPSYS_FILES_DIR.\n"
        "# New uploads are rejected with HTTP 507 once the quota is reached.\n"
        "# 0 (the default) means unlimited — set up external disk monitoring instead.\n"
        "# Example — 50 GB:\n"
        "# BACKUPSYS_STORAGE_QUOTA_BYTES=53687091200\n"
    ),
    "config.template.json": json.dumps({
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
        "backup_schedule_times": [],
        "backup_window_start": "",
        "backup_window_end": "",
        "idle_threshold_cpu": 0,
        "watches": [],
        "default_exclude_patterns": [
            "*.tmp", "*.log", "~$*", ".DS_Store", "Thumbs.db",
            "__pycache__", "*.pyc", ".git"
        ],
        "email_config": {
            "enabled": False,
            "smtp_host": "",
            "smtp_port": 587,
            "smtp_use_ssl": False,
            "username": "",
            "__password_note": "Store via Settings UI (OS keyring) or use an environment variable — never paste passwords here",
            "from_addr": "",
            "to_addr": "",
            "notify_on_success": False,
            "notify_on_failure": True
        },
        "dest_sftp": {"host": "", "port": 22, "username": "", "__password_note": "Store via Settings UI (OS keyring) or use an environment variable — never paste passwords here", "remote_path": ""},
        "dest_ftp":  {"host": "", "port": 21, "username": "", "__password_note": "Store via Settings UI (OS keyring) or use an environment variable — never paste passwords here", "use_tls": True},
        "dest_smb":  {"server": "", "share": "", "username": "", "__password_note": "Store via Settings UI (OS keyring) or use an environment variable — never paste passwords here", "remote_path": ""},
        "pause_on_metered": False,
        "force_full_interval_days": 0,
        "ntfy_config": {
            "enabled": False,
            "server": "https://ntfy.sh",
            "topic": "",
            "token": "",
            "priority": "default",
            "notify_on_success": False,
            "notify_on_failure": True,
        },
        "telegram_config": {
            "enabled": False,
            "bot_token": "",
            "chat_id": "",
            "parse_mode": "HTML",
            "notify_on_success": False,
            "notify_on_failure": True,
        },
        "pushover_config": {
            "enabled": False,
            "user_key": "",
            "api_token": "",
            "device": "",
            "priority": 0,
            "sound": "",
            "notify_on_success": False,
            "notify_on_failure": True,
        },
        "dest_https": {"url": "", "token": "", "verify_ssl": True},
        "dest_webdav": {"url": "", "username": "", "__password_note": "Store via Settings UI (OS keyring) or use an environment variable — never paste passwords here", "webdav_root": "", "remote_path": "", "verify_ssl": True},
        "dest_rclone": {
            "__note": "Set dest_type to 'rclone'. Run 'rclone config' to create a named remote, then set 'remote' to the name shown by 'rclone listremotes'.",
            "remote": "",
            "path": "/backups"
        },
        "dest_cloud": {
            "__note": "Set dest_type to 'cloud'. Connect via Settings → Cloud → Connect Google Drive. folder_id and folder_name are written automatically — do not edit them manually.",
            "provider": "gdrive",
            "folder_id": "",
            "folder_name": "My Drive (root)"
        },
    }, indent=2),
}

# ── Blocklist ──────────────────────────────────────────────────────────────────
BLOCKED_NAMES = {
    ".env", "_env", "env", "env.developer",
    ".secret_key", "secret_key",
    "credentials.json",
    ".user_cloud_tokens.json", "user_cloud_tokens.json",
    "token.json",
    "config.json",
    "history.json",
    "backup_queue.json",
    "connect_cloud.py",
    "sftp_repro.py",
    "tmp_patch_add_watch.py",
    "regenerate_manifests.py",
    "clear_admin.py",
    "live_dest_tests.py",
    "setup_cloud_dev.py",
    "env.developer",
    ".env.developer",
}

BLOCKED_EXTENSIONS = {
    ".log", ".bak", ".tmp", ".swp", ".token",
    ".pyc", ".pyo", ".pyd",
}

BLOCKED_DIRS = {
    "backups", "snapshots", "logs", "build", "dist",
    "__pycache__", ".git", ".idea", ".vscode", "venv", ".venv",
}

# ── Secret scanner ─────────────────────────────────────────────────────────────
_SECRET_PATTERNS = [
    re.compile(r'(?i)(password|secret|token|api[_-]?key)\s*=\s*["\'](?!your-|replace-|example-|<)[^"\']{8,}["\']\s*$'),
    re.compile(r'AIza[0-9A-Za-z_-]{35}'),
    re.compile(r'GOCSPX-[0-9A-Za-z_-]{28}'),
    re.compile(r'ya29\.[0-9A-Za-z_-]{100,}'),
    re.compile(r'(?i)ghp_[0-9A-Za-z]{36}'),
    re.compile(r'(?i)xox[baprs]-[0-9A-Za-z-]{10,}'),
    re.compile(r'(?i)sk-[A-Za-z0-9]{32,}'),
    re.compile(r'\d{15,}-[A-Za-z0-9_-]{30,}\.apps\.googleusercontent\.com'),
]

def _scan_for_secrets(path: Path) -> list[str]:
    hits = []
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
        for i, line in enumerate(text.splitlines(), 1):
            for pat in _SECRET_PATTERNS:
                if pat.search(line):
                    safe = re.sub(r'(?<==).+', ' <REDACTED>', line.strip())
                    hits.append(f"  line {i}: {safe}")
                    break
    except Exception:
        pass
    return hits


def _should_block(rel: Path) -> str | None:
    name = rel.name
    if name in BLOCKED_NAMES:
        return f"blocked filename: {name}"
    if rel.suffix.lower() in BLOCKED_EXTENSIONS:
        return f"blocked extension: {rel.suffix}"
    for part in rel.parts:
        if part in BLOCKED_DIRS:
            return f"blocked directory: {part}"
    return None


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*60}")
    print(f"  BackupSys {VERSION} — Release Zip Builder")
    print(f"{'='*60}\n")

    changelog = ROOT / "CHANGELOG.md"
    if not changelog.exists():
        print("  ❌  ABORT: CHANGELOG.md not found.")
        print("     Document your changes before releasing — update CHANGELOG.md first.")
        sys.exit(1)
    print("  ✅  CHANGELOG.md present")

    DIST_DIR.mkdir(parents=True, exist_ok=True)
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_name = f"BackupSys_{VERSION}_{ts}_source.zip"
    zip_path = DIST_DIR / zip_name

    included = []
    skipped  = []
    secret_warnings = []

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:

        for fname in SAFE_SOURCE_FILES:
            src = ROOT / fname
            if not src.exists():
                skipped.append((fname, "not found"))
                continue

            rel = src.relative_to(ROOT)
            reason = _should_block(rel)
            if reason:
                skipped.append((fname, f"BLOCKED — {reason}"))
                print(f"  ⛔  BLOCKED (allowlist override): {fname}  [{reason}]")
                continue

            if src.suffix in {".py", ".json", ".txt", ".md", ".html", ".env", ".cfg", ".ini", ".toml"}:
                hits = _scan_for_secrets(src)
                if hits:
                    secret_warnings.append((fname, hits))
                    skipped.append((fname, "SKIPPED — possible secrets detected"))
                    print(f"  ⚠️  SECRETS DETECTED — skipping: {fname}")
                    for h in hits:
                        print(f"       {h}")
                    continue

            zf.write(src, rel)
            included.append(fname)
            print(f"  ✅  {fname}")

        for fname in SAFE_TEST_FILES:
            src = ROOT / fname
            if src.exists():
                zf.write(src, fname)
                included.append(fname)
            else:
                skipped.append((fname, "not found"))

        for tname, content in TEMPLATE_FILES.items():
            zf.writestr(tname, content)
            included.append(tname)
            print(f"  ✅  {tname}  (generated template)")

    sha256 = hashlib.sha256(zip_path.read_bytes()).hexdigest()
    checksum_path = zip_path.with_suffix(".sha256")
    checksum_path.write_text(f"{sha256}  {zip_name}\n")

    print(f"\n{'─'*60}")
    print(f"  ✅  Included : {len(included)} files")
    print(f"  ⏭   Skipped  : {len(skipped)} files")
    if secret_warnings:
        print(f"\n  ⚠️  {len(secret_warnings)} file(s) were SKIPPED due to suspected secrets:")
        for fname, hits in secret_warnings:
            print(f"       • {fname}")
    print(f"\n  📦  Output   : dist/{zip_name}")
    print(f"  🔑  SHA-256  : {sha256}")
    print(f"{'─'*60}\n")

    if skipped:
        print("Skipped files:")
        for fname, reason in skipped:
            print(f"  ⏭  {fname}  [{reason}]")
        print()

    if secret_warnings:
        print("\n🛑  ACTION REQUIRED:")
        print("    One or more files were skipped because they appear to contain")
        print("    real credentials. Check the warnings above and ensure secrets")
        print("    are loaded from environment variables — never hardcoded.\n")
        sys.exit(1)

    print("Release zip is clean and ready to share. ✅\n")


if __name__ == "__main__":
    main()