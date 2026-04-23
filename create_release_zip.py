"""
create_release_zip.py — Build a clean, secret-free source release zip.

Run from the project root:
    python create_release_zip.py

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
ROOT     = Path(__file__).resolve().parent

# ── Version ───────────────────────────────────────────────────────────────────
# Read APP_VERSION from desktop_app.py to keep it as single source of truth
def _get_app_version():
    app_py = ROOT / "desktop_app.py"
    if app_py.exists():
        try:
            with open(app_py, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip().startswith("APP_VERSION"):
                        # Extract "1.1.0" from APP_VERSION = "1.1.0"
                        match = re.search(r'APP_VERSION\s*=\s*["\']([^"\']+)["\']', line)
                        if match:
                            return match.group(1)
        except UnicodeDecodeError:
            # Fallback to latin-1 or something
            with open(app_py, 'r', encoding='latin-1') as f:
                for line in f:
                    if line.strip().startswith("APP_VERSION"):
                        match = re.search(r'APP_VERSION\s*=\s*["\']([^"\']+)["\']', line)
                        if match:
                            return match.group(1)
    return "1.0.0"  # fallback

VERSION = _get_app_version()

# ── Paths ─────────────────────────────────────────────────────────────────────
# ROOT     = Path(__file__).resolve().parent  # moved up
DIST_DIR = ROOT / "dist"

# ── Explicit allowlist: ONLY these files are included ─────────────────────────
# Add new source files here when you create them.
SAFE_SOURCE_FILES = [
    # Core source
    "desktop_app.py",
    "backup_engine.py",
    "config_manager.py",
    "transport_utils.py",
    "notification_utils.py",
    "watcher.py",
    "integrity_scheduler.py",   # weekly background validation
    "connect_cloud.py",
    "setup_wizard.py",          # user-facing first-run helper
    "backupsys_cli.py",         # headless CLI / Task Scheduler mode

    # Build & packaging
    "build_exe.py",  # included for packaging
    "create_release_zip.py",  # included for packaging

    # Backend API (no secrets — reads from env vars at runtime)
    "backupsys_api.py",

    # Credential store (keyring wrapper)
    "credential_store.py",

    # Documentation & config templates
    "README.md",
    "CHANGELOG.md",
    "requirements_desktop.txt",
    "requirements_api.txt",
    "privacy.html",
    ".gitignore",
    "LICENSE",

    # Assets
    "icon_256.png",
    "icon_64.png",
]

# Test files included in the release so users can validate their install
SAFE_TEST_FILES = [
    "tests/__init__.py",
    "tests/test_backup_engine.py",
    "tests/test_backupsys_api.py",
    "tests/test_config_manager.py",
    "tests/test_credential_store.py",
    "tests/test_transport_utils.py",
    "tests/test_notification_utils.py",
    "tests/test_watcher.py",
]

# Template files: these are safe ONLY after secrets are stripped.
# We generate clean versions from scratch — never copy the live files.
TEMPLATE_FILES = {
    ".env.example": (
        "# Copy this file to .env and fill in your values.\n"
        "# NEVER commit the real .env to git or include it in a release zip.\n"
        "\n"
        "# URL of your deployed BackupSys API (Railway or similar)\n"
        "BACKUPSYS_API_URL=https://your-api.up.railway.app\n"
        "\n"
        "# Shared secret between the desktop app and your API server\n"
        "BACKUPSYS_API_KEY=replace-with-a-random-secret\n"
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
        "dest_https": {"url": "", "token": "", "verify_ssl": True},
        "dest_webdav": {"url": "", "username": "", "__password_note": "Store via Settings UI (OS keyring) or use an environment variable — never paste passwords here", "webdav_root": "", "remote_path": "", "verify_ssl": True},
    }, indent=2),
}

# ── Hardcoded blocklist (belt-and-suspenders) ─────────────────────────────────
# These are NEVER included regardless of any other logic.
BLOCKED_NAMES = {
    # Secrets
    ".env", "_env", "env", "env.developer",
    ".secret_key", "secret_key",
    "credentials.json",
    ".user_cloud_tokens.json", "user_cloud_tokens.json",
    "token.json",
    # Runtime state (contain real paths / tokens / history)
    "config.json",
    "history.json",
    "backup_queue.json",
    # Dev / scratch scripts — never user-facing
    "sftp_repro.py",
    "tmp_patch_add_watch.py",
    "regenerate_manifests.py",
    "clear_admin.py",
    "live_dest_tests.py",
    "setup_cloud_dev.py",
    # Build scripts — not needed by end users
    # "build_exe.py",  # included for packaging
    # "create_release_zip.py",  # included for packaging
    # Environment files — may contain real credentials
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

# ── Secret pattern scanner ────────────────────────────────────────────────────
# Refuse to include any file whose content looks like it contains a real secret.
_SECRET_PATTERNS = [
    # Only flag actual hardcoded string literals — not variable assignments that
    # read from cfg/env (e.g. password = cfg.get("password") is legitimate code).
    # The value must be a quoted string whose content is 8+ chars and doesn't look
    # like a placeholder.  The \s*$ anchor prevents matching mid-expression quotes.
    re.compile(r'(?i)(password|secret|token|api[_-]?key)\s*=\s*["\'](?!your-|replace-|example-|<)[^"\']{8,}["\']\s*$'),
    re.compile(r'AIza[0-9A-Za-z_-]{35}'),                   # Google API key
    re.compile(r'GOCSPX-[0-9A-Za-z_-]{28}'),               # Google OAuth secret
    re.compile(r'ya29\.[0-9A-Za-z_-]{100,}'),              # Google access token
    re.compile(r'(?i)ghp_[0-9A-Za-z]{36}'),                # GitHub PAT
    re.compile(r'(?i)xox[baprs]-[0-9A-Za-z-]{10,}'),      # Slack token
    re.compile(r'(?i)sk-[A-Za-z0-9]{32,}'),               # OpenAI / generic sk-
    re.compile(r'\d{15,}-[A-Za-z0-9_-]{30,}\.apps\.googleusercontent\.com'),  # GDrive client ID
]

def _scan_for_secrets(path: Path) -> list[str]:
    """Return a list of suspicious lines found in a text file."""
    hits = []
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
        for i, line in enumerate(text.splitlines(), 1):
            for pat in _SECRET_PATTERNS:
                if pat.search(line):
                    # Redact the match before printing
                    safe = re.sub(r'(?<==).+', ' <REDACTED>', line.strip())
                    hits.append(f"  line {i}: {safe}")
                    break
    except Exception:
        pass
    return hits


def _should_block(rel: Path) -> str | None:
    """Return a human-readable reason if this path must be excluded, else None."""
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

    # ── Pre-flight checks ─────────────────────────────────────────────────────
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

        # 1. Allowlisted source files
        for fname in SAFE_SOURCE_FILES:
            src = ROOT / fname
            if not src.exists():
                skipped.append((fname, "not found"))
                continue

            rel = src.relative_to(ROOT)

            # Belt-and-suspenders: block check even on allowlisted files
            reason = _should_block(rel)
            if reason:
                skipped.append((fname, f"BLOCKED — {reason}"))
                print(f"  ⛔  BLOCKED (allowlist override): {fname}  [{reason}]")
                continue

            # Secret scan for text files
            if src.suffix in {".py", ".json", ".txt", ".md", ".html", ".env", ".cfg", ".ini"}:
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

        # 1b. Test files
        for fname in SAFE_TEST_FILES:
            src = ROOT / fname
            if src.exists():
                zf.write(src, fname)
                included.append(fname)
            else:
                skipped.append((fname, "not found"))

        # 2. Generated template files (written from in-memory strings — never from disk)
        for tname, content in TEMPLATE_FILES.items():
            zf.writestr(tname, content)
            included.append(tname)
            print(f"  ✅  {tname}  (generated template)")

    # ── Checksum ──────────────────────────────────────────────────────────────
    sha256 = hashlib.sha256(zip_path.read_bytes()).hexdigest()
    checksum_path = zip_path.with_suffix(".sha256")
    checksum_path.write_text(f"{sha256}  {zip_name}\n")

    # ── Summary ───────────────────────────────────────────────────────────────
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