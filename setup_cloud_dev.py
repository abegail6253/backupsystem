"""
setup_cloud_dev.py — RUN THIS ONCE (you, the developer)
=========================================================
You run this ONCE on your own PC to register your Dropbox app
and Google Cloud project. The App Key/Secret/Client ID are then
bundled into the .env that ships with your app.

Each END USER will use their own account via connect_cloud.py
(or the desktop app's Connect button) — they never see these secrets.

HOW TO RUN:
    python setup_cloud_dev.py

BEFORE RUNNING:

  Dropbox:
    1. Go to https://www.dropbox.com/developers/apps
    2. Create App → Scoped access → Full Dropbox → any name
    3. Settings tab → copy App Key + App Secret
    4. Permissions tab → enable:
         files.content.write
         files.content.read
         account_info.read

  Google Drive:
    1. Go to https://console.cloud.google.com
    2. New Project → give it any name
    3. APIs & Services → Enable APIs → search "Google Drive API" → Enable
    4. Credentials → Create Credentials → OAuth 2.0 Client ID → Desktop app
    5. Download JSON → rename to credentials.json → place in this folder
    6. OAuth consent screen → set to "External" → add your test email
       (for internal company use you can keep it in "Testing" mode)
"""

import sys
import json
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent
SEP  = "=" * 60

def header(t): print(f"\n{SEP}\n  {t}\n{SEP}")
def ok(t):     print(f"  [OK]    {t}")
def fail(t):   print(f"  [FAIL]  {t}")
def info(t):   print(f"  [INFO]  {t}")
def ask(t):    return input(f"\n  >> {t}: ").strip()


def install_packages():
    header("Step 1 — Install cloud packages")
    pkgs = [
        ("dropbox",             "dropbox>=11.0.0"),
        ("google.oauth2",       "google-auth>=2.0.0"),
        ("google_auth_oauthlib","google-auth-oauthlib>=1.0.0"),
        ("googleapiclient",     "google-api-python-client>=2.0.0"),
    ]
    for mod, pkg in pkgs:
        try:
            __import__(mod); ok(f"{pkg} already installed")
        except ImportError:
            info(f"Installing {pkg}...")
            r = subprocess.run(
                [sys.executable, "-m", "pip", "install", pkg, "--user"],
                capture_output=True, text=True
            )
            ok(f"Installed: {pkg}") if r.returncode == 0 else fail(f"Failed: {pkg}\n{r.stderr[-300:]}")


def save_dropbox_app_credentials():
    header("Step 2 — Save Dropbox App Key + Secret")
    info("These identify YOUR app — NOT tied to any user account.")
    info("Every user who clicks 'Connect Dropbox' in BackupSys will")
    info("authenticate with THEIR OWN Dropbox account using these keys.")

    app_key    = ask("Paste your Dropbox App Key")
    app_secret = ask("Paste your Dropbox App Secret")

    if not app_key or not app_secret:
        fail("Skipping Dropbox — no credentials entered.")
        return

    _save_to_env("DROPBOX_APP_KEY",    app_key)
    _save_to_env("DROPBOX_APP_SECRET", app_secret)
    ok("DROPBOX_APP_KEY and DROPBOX_APP_SECRET saved to .env")
    info("Users will be redirected to Dropbox to log in with their own account.")


def save_gdrive_app_credentials():
    header("Step 3 — Save Google Drive Client ID + Secret")
    info("credentials.json should be in this folder (downloaded from Google Cloud Console).")

    creds_file = HERE / "credentials.json"
    if not creds_file.exists():
        fail(f"credentials.json not found at: {creds_file}")
        info("Download from Google Cloud Console → Credentials → OAuth 2.0 → Download JSON")
        info("Rename it credentials.json and place it in this folder, then re-run.")
        return

    try:
        data       = json.loads(creds_file.read_text(encoding="utf-8"))
        client_cfg = data.get("installed") or data.get("web", {})
        client_id  = client_cfg.get("client_id", "")
        secret     = client_cfg.get("client_secret", "")
        if not client_id or not secret:
            fail("credentials.json missing client_id or client_secret"); return
        ok(f"Client ID: {client_id[:40]}...")
    except Exception as e:
        fail(f"Could not read credentials.json: {e}"); return

    _save_to_env("GDRIVE_CLIENT_ID",     client_id)
    _save_to_env("GDRIVE_CLIENT_SECRET", secret)
    ok("GDRIVE_CLIENT_ID and GDRIVE_CLIENT_SECRET saved to .env")
    info("Users will be redirected to Google to log in with their own Gmail account.")

    # Keep credentials.json for connect_cloud.py to use during user OAuth flow
    ok("credentials.json will be bundled with the app for user login flow")


def print_summary():
    header("Done — Developer setup complete")
    print("""
  What happens next:
  ─────────────────────────────────────────────────────────
  1. Your .env now has the app-level credentials (App Key,
     Client ID, etc.) — these identify your app, not any user.

  2. When you build the .exe with build_exe.py, these are
     bundled inside the app automatically via the .env file.

  3. When a USER installs BackupSys on their PC, they run:
         python connect_cloud.py
     OR click "Connect" in the desktop app.
     They log in with THEIR OWN Dropbox or Gmail account.
     Their personal tokens are saved on THEIR OWN PC only.

  4. You NEVER see their passwords or tokens.
  ─────────────────────────────────────────────────────────
  Next step: run connect_cloud.py on a test PC to verify
  the full user login flow works end-to-end.
""")


def _save_to_env(key: str, value: str):
    env_path = HERE / ".env"
    lines    = env_path.read_text(encoding="utf-8").splitlines() if env_path.exists() else []
    lines    = [l for l in lines if not l.lstrip("#").strip().startswith(key + "=")]
    lines.append(f"{key}={value}")
    env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    print(f"\n{SEP}")
    print("  BackupSys — Developer App Registration (run once)")
    print(f"  Folder: {HERE}")
    print(f"{SEP}")

    install_packages()
    save_dropbox_app_credentials()
    save_gdrive_app_credentials()
    print_summary()
