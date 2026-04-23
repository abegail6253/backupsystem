"""
connect_cloud.py — Each USER runs this on their own PC
========================================================
This is what your end users run (or the desktop app triggers
automatically) to connect their own Google Drive.

Each user authenticates with THEIR OWN account.
Their tokens are saved only on THEIR PC — nobody else sees them.

HOW TO RUN:
    python connect_cloud.py

No setup needed — just run it and follow the browser prompts.
The desktop app can also call connect_gdrive()
directly when the user clicks the "Connect" button.
"""

import sys
import os
import json
import webbrowser
import urllib.parse
import urllib.request
import urllib.error
from pathlib import Path
from datetime import datetime, timedelta

HERE = Path(__file__).resolve().parent
SEP  = "=" * 60

def header(t): print(f"\n{SEP}\n  {t}\n{SEP}")
def ok(t):     print(f"  [OK]    {t}")
def fail(t):   print(f"  [FAIL]  {t}")
def info(t):   print(f"  [INFO]  {t}")
def ask(t):    return input(f"\n  >> {t}: ").strip()


# ─────────────────────────────────────────────────────────────────────────────
# Load app credentials (set by developer in .env file)
# ─────────────────────────────────────────────────────────────────────────────
def _load_env():
    """Load .env into os.environ if not already there."""
    env_path = HERE / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        if k.strip() and k.strip() not in os.environ:
            os.environ[k.strip()] = v.strip()

_load_env()


# ─────────────────────────────────────────────────────────────────────────────
# Token storage — saved per-user on their own PC
# ─────────────────────────────────────────────────────────────────────────────
USER_TOKENS_PATH = HERE / ".user_cloud_tokens.json"

def _load_user_tokens() -> dict:
    if USER_TOKENS_PATH.exists():
        try:
            return json.loads(USER_TOKENS_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}

def _save_user_tokens(tokens: dict):
    """Save user tokens to disk. Never committed to Git (.gitignore excludes it)."""
    existing = _load_user_tokens()
    existing.update(tokens)
    USER_TOKENS_PATH.write_text(json.dumps(existing, indent=2), encoding="utf-8")
    _ensure_gitignore(".user_cloud_tokens.json")

def _ensure_gitignore(entry: str):
    gi = HERE / ".gitignore"
    if gi.exists():
        content = gi.read_text(encoding="utf-8")
        if entry not in content:
            gi.write_text(content.rstrip() + f"\n{entry}\n", encoding="utf-8")


# ─────────────────────────────────────────────────────────────────────────────
# GOOGLE DRIVE — user login
# ─────────────────────────────────────────────────────────────────────────────
def connect_gdrive() -> dict:
    """
    Runs the Google Drive OAuth flow for the current user.
    Returns token dict on success, empty dict on failure.
    Can be called from desktop_app.py when user clicks 'Connect Google Drive'.
    """
    header("Connect your Google Drive account")

    client_id     = os.environ.get("GDRIVE_CLIENT_ID", "").strip()
    client_secret = os.environ.get("GDRIVE_CLIENT_SECRET", "").strip()
    creds_file    = HERE / "credentials.json"

    if not client_id or not client_secret:
        if not creds_file.exists():
            fail("Google Drive app credentials not found.")
            fail("Set GDRIVE_CLIENT_ID and GDRIVE_CLIENT_SECRET in your .env file. Get them from https://console.cloud.google.com/apis/credentials — create an OAuth 2.0 Client ID for a Desktop app.")
            return {}

    info("You will be redirected to Google to log in with YOUR Gmail account.")
    info("BackupSys will only access Drive files it creates — nothing else.")

    try:
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build
    except ImportError:
        fail("google-auth-oauthlib not installed.")
        info("Run: pip install google-auth-oauthlib google-api-python-client")
        return {}

    try:
        SCOPES = ["https://www.googleapis.com/auth/drive.file"]
        # drive.file scope = only files BackupSys creates/opens — safer than full drive

        if creds_file.exists():
            flow = InstalledAppFlow.from_client_secrets_file(str(creds_file), SCOPES)
        else:
            # Reconstruct from env vars if credentials.json was not bundled
            client_config = {
                "installed": {
                    "client_id":      client_id,
                    "client_secret":  client_secret,
                    "auth_uri":       "https://accounts.google.com/o/oauth2/auth",
                    "token_uri":      "https://oauth2.googleapis.com/token",
                    "redirect_uris":  ["urn:ietf:wg:oauth:2.0:oob", "http://localhost"],
                }
            }
            flow = InstalledAppFlow.from_client_config(client_config, SCOPES)

        info("Your browser will open for Google sign-in...")
        creds = flow.run_local_server(port=0, open_browser=True)

    except Exception as e:
        fail(f"OAuth flow failed: {e}")
        info("Make sure you are on a PC with a browser.")
        return {}

    # Verify connection
    info("Verifying Google Drive access...")
    try:
        service = build("drive", "v3", credentials=creds)

        about   = service.about().get(fields="user").execute()
        email   = about["user"]["emailAddress"]
        name    = about["user"]["displayName"]

        from googleapiclient.http import MediaInMemoryUpload
        media  = MediaInMemoryUpload(b"backupsys_ok", mimetype="text/plain")
        f      = service.files().create(
            body={"name": "backupsys_connection_test.txt"},
            media_body=media, fields="id"
        ).execute()
        service.files().delete(fileId=f["id"]).execute()
        ok(f"Connected as: {name} ({email})")
        ok("Upload test passed")

    except Exception as e:
        fail(f"Verification failed: {e}"); return {}

    # Save tokens
    user_tokens = {
        "gdrive": {
            "client_id":     client_id or creds.client_id,
            "client_secret": client_secret or creds.client_secret,
            "access_token":  creds.token,
            "refresh_token": creds.refresh_token,
            "token_uri":     creds.token_uri,
            "email":         email,
            "display_name":  name,
            "connected_at":  datetime.now().isoformat(),
        }
    }
    _save_user_tokens(user_tokens)
    ok(f"Tokens saved to: {USER_TOKENS_PATH.name}")

    # ── Folder picker — let the user choose which Drive folder to use ─────────
    chosen_folder_id   = ""
    chosen_folder_name = "My Drive (root)"
    try:
        info("Listing your Google Drive folders…")
        folders_result = service.files().list(
            q="mimeType='application/vnd.google-apps.folder' and trashed=false",
            fields="files(id, name)",
            orderBy="name",
            pageSize=50,
        ).execute()
        folders = folders_result.get("files", [])

        if folders:
            print("\n  Available Google Drive folders:")
            print("     0)  My Drive (root)")
            for i, folder in enumerate(folders, start=1):
                print(f"    {i:>2})  {folder['name']}")
            print()
            choice_str = ask(f"Enter folder number (0–{len(folders)}) or press Enter for root")
            if choice_str.isdigit():
                choice = int(choice_str)
                if 1 <= choice <= len(folders):
                    chosen_folder_id   = folders[choice - 1]["id"]
                    chosen_folder_name = folders[choice - 1]["name"]
                    ok(f"Backup folder set to: {chosen_folder_name}")
                else:
                    ok("Using My Drive root.")
            else:
                ok("Using My Drive root.")
        else:
            info("No folders found — uploads will go to My Drive root.")
    except Exception as _fp_err:
        info(f"Could not list Drive folders (non-fatal): {_fp_err}")
        info("Uploads will go to root. You can change this in Settings → Cloud.")

    _update_config_cloud("gdrive", {
        "provider":      "gdrive",
        "client_id":     user_tokens["gdrive"]["client_id"],
        "client_secret": user_tokens["gdrive"]["client_secret"],
        "access_token":  creds.token,
        "refresh_token": creds.refresh_token,
        "folder_id":     chosen_folder_id,
        "folder_name":   chosen_folder_name,
    })
    if chosen_folder_id:
        ok(f"Backups will upload to Drive folder: {chosen_folder_name}")
    else:
        ok("Backups will upload to My Drive root.")

    return user_tokens["gdrive"]


# ─────────────────────────────────────────────────────────────────────────────
# Status check
# ─────────────────────────────────────────────────────────────────────────────
def check_connections():
    header("Current cloud connections on this PC")
    tokens = _load_user_tokens()

    if not tokens:
        info("No cloud accounts connected yet on this PC.")
        return

    for provider, data in tokens.items():
        email = data.get("email", "unknown")
        name  = data.get("display_name", "")
        ts    = data.get("connected_at", "")[:19].replace("T", " ")
        ok(f"{provider.capitalize()}: {name} ({email}) — connected {ts}")


# ─────────────────────────────────────────────────────────────────────────────
# Disconnect (revoke tokens on this PC)
# ─────────────────────────────────────────────────────────────────────────────
def disconnect(provider: str):
    """Remove a provider's tokens from this PC."""
    tokens = _load_user_tokens()
    if provider not in tokens:
        info(f"{provider} is not connected on this PC."); return
    del tokens[provider]
    USER_TOKENS_PATH.write_text(json.dumps(tokens, indent=2), encoding="utf-8")

    _update_config_cloud(provider, {})
    ok(f"{provider.capitalize()} disconnected from this PC.")
    if provider == "gdrive":
        info("To fully revoke access, also visit:")
        info("  https://myaccount.google.com/permissions")


# ─────────────────────────────────────────────────────────────────────────────
# Helper: update config.json watches that use this provider
# ─────────────────────────────────────────────────────────────────────────────
def _update_config_cloud(provider: str, cloud_cfg: dict):
    """
    Update cloud_config in all watches that use this provider,
    and set it as the default for new watches.
    """
    cfg_path = HERE / "config.json"
    if not cfg_path.exists():
        return
    try:
        cfg = json.loads(cfg_path.read_text(encoding="utf-8"))

        for w in cfg.get("watches", []):
            existing = w.get("cloud_config", {})
            if existing.get("provider") == provider or not existing:
                w["cloud_config"] = cloud_cfg
                if cloud_cfg:
                    w["type"] = "cloud"

        cfg[f"_cloud_default_{provider}"] = cloud_cfg

        import tempfile, os as _os
        fd, tmp = tempfile.mkstemp(dir=cfg_path.parent, suffix=".tmp")
        with _os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2)
        _os.replace(tmp, cfg_path)

    except Exception as e:
        info(f"Could not update config.json: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
# NOTE: Connect / disconnect Google Drive through the desktop app UI
# (Settings → Cloud → Connect Google Drive).  This file is used as a
# module only — running it directly is not needed in normal operation.
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"\n{SEP}")
    print("  BackupSys — connect_cloud.py (developer / debug use only)")
    print("  For normal use, connect Google Drive via the desktop app UI.")
    print(f"  This PC: {HERE}")
    print(f"{SEP}\n")
    check_connections()
