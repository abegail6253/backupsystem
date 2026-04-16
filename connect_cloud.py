"""
connect_cloud.py — Each USER runs this on their own PC
========================================================
This is what your end users run (or the desktop app triggers
automatically) to connect their own Dropbox or Google Drive.

Each user authenticates with THEIR OWN account.
Their tokens are saved only on THEIR PC — nobody else sees them.

HOW TO RUN:
    python connect_cloud.py

No setup needed — just run it and follow the browser prompts.
The desktop app can also call connect_dropbox() / connect_gdrive()
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
# Load app credentials (set by developer via setup_cloud_dev.py)
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
    # Make sure .gitignore excludes this file
    _ensure_gitignore(".user_cloud_tokens.json")

def _ensure_gitignore(entry: str):
    gi = HERE / ".gitignore"
    if gi.exists():
        content = gi.read_text(encoding="utf-8")
        if entry not in content:
            gi.write_text(content.rstrip() + f"\n{entry}\n", encoding="utf-8")


# ─────────────────────────────────────────────────────────────────────────────
# DROPBOX — user login
# ─────────────────────────────────────────────────────────────────────────────
def connect_dropbox() -> dict:
    """
    Runs the Dropbox OAuth flow for the current user.
    Returns token dict on success, empty dict on failure.
    Can be called from desktop_app.py when user clicks 'Connect Dropbox'.
    """
    header("Connect your Dropbox account")

    app_key    = os.environ.get("DROPBOX_APP_KEY", "").strip()
    app_secret = os.environ.get("DROPBOX_APP_SECRET", "").strip()

    if not app_key or not app_secret:
        fail("App credentials not found.")
        fail("The developer must run setup_cloud_dev.py first to register the app.")
        return {}

    info("You will be redirected to Dropbox to log in with YOUR account.")
    info("BackupSys will only access your backup folder — nothing else.")

    auth_url = (
        "https://www.dropbox.com/oauth2/authorize"
        f"?client_id={app_key}"
        "&response_type=code"
        "&token_access_type=offline"
    )

    print(f"\n  Opening: {auth_url}\n")
    try:
        webbrowser.open(auth_url)
    except Exception:
        info("Could not open browser automatically. Please open the URL above manually.")

    auth_code = ask("After logging in, paste the code Dropbox gives you")
    if not auth_code:
        fail("No code entered. Dropbox connection cancelled."); return {}

    # Exchange code for tokens
    info("Connecting to Dropbox...")
    try:
        data = urllib.parse.urlencode({
            "code":          auth_code,
            "grant_type":    "authorization_code",
            "client_id":     app_key,
            "client_secret": app_secret,
        }).encode()
        req = urllib.request.Request(
            "https://api.dropboxapi.com/oauth2/token",
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            tokens = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        fail(f"Dropbox error: HTTP {e.code} — {e.read().decode()}"); return {}
    except Exception as e:
        fail(f"Connection failed: {e}"); return {}

    access_token  = tokens.get("access_token", "")
    refresh_token = tokens.get("refresh_token", "")

    if not access_token or not refresh_token:
        fail("Dropbox did not return tokens. Try again."); return {}

    # Verify + get account info
    try:
        import dropbox as dbx_mod
        dbx = dbx_mod.Dropbox(
            oauth2_access_token=access_token,
            oauth2_refresh_token=refresh_token,
            app_key=app_key,
            app_secret=app_secret,
        )
        account = dbx.users_get_current_account()
        display_name = account.name.display_name
        email        = account.email

        # Quick upload test
        dbx.files_upload(b"backupsys_ok", "/backupsys_connection_test.txt",
                         mode=dbx_mod.files.WriteMode.overwrite)
        dbx.files_delete_v2("/backupsys_connection_test.txt")
        ok(f"Connected as: {display_name} ({email})")
        ok("Upload test passed")

    except Exception as e:
        fail(f"Verification failed: {e}"); return {}

    # Save to user's local token store
    user_tokens = {
        "dropbox": {
            "access_token":  access_token,
            "refresh_token": refresh_token,
            "app_key":       app_key,
            "app_secret":    app_secret,
            "email":         email,
            "display_name":  display_name,
            "connected_at":  datetime.now().isoformat(),
        }
    }
    _save_user_tokens(user_tokens)
    ok(f"Tokens saved to: {USER_TOKENS_PATH.name}")

    # Also update config.json cloud_config for all watches set to dropbox
    _update_config_cloud("dropbox", {
        "provider":      "dropbox",
        "app_key":       app_key,
        "app_secret":    app_secret,
        "access_token":  access_token,
        "refresh_token": refresh_token,
        "remote_path":   "/backupsys",
    })

    return user_tokens["dropbox"]


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
            fail("The developer must run setup_cloud_dev.py first.")
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
                    "client_id":                  client_id,
                    "client_secret":              client_secret,
                    "auth_uri":                   "https://accounts.google.com/o/oauth2/auth",
                    "token_uri":                  "https://oauth2.googleapis.com/token",
                    "redirect_uris":              ["urn:ietf:wg:oauth:2.0:oob", "http://localhost"],
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

        # Get user info
        about   = service.about().get(fields="user").execute()
        email   = about["user"]["emailAddress"]
        name    = about["user"]["displayName"]

        # Quick upload test using drive.file scope
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

    _update_config_cloud("gdrive", {
        "provider":      "gdrive",
        "client_id":     user_tokens["gdrive"]["client_id"],
        "client_secret": user_tokens["gdrive"]["client_secret"],
        "access_token":  creds.token,
        "refresh_token": creds.refresh_token,
        "folder_id":     "",
    })

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

    # Also clear from config.json
    _update_config_cloud(provider, {})
    ok(f"{provider.capitalize()} disconnected from this PC.")
    info(f"To fully revoke access, also visit:")
    if provider == "dropbox":
        info("  https://www.dropbox.com/account/connected_apps")
    elif provider == "gdrive":
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

        # Update watches already using this provider
        for w in cfg.get("watches", []):
            existing = w.get("cloud_config", {})
            if existing.get("provider") == provider or not existing:
                w["cloud_config"] = cloud_cfg
                if cloud_cfg:
                    w["type"] = "cloud"

        # Save a top-level default for new watches
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
if __name__ == "__main__":
    print(f"\n{SEP}")
    print("  BackupSys — Connect Your Cloud Account")
    print(f"  This PC: {HERE}")
    print(f"{SEP}")

    check_connections()

    do_dropbox = input("\n  Connect Dropbox?      [y/N] ").strip().lower() == "y"
    do_gdrive  = input("  Connect Google Drive? [y/N] ").strip().lower() == "y"
    disconnect_something = input("  Disconnect a provider? [y/N] ").strip().lower() == "y"

    if do_dropbox:
        connect_dropbox()

    if do_gdrive:
        connect_gdrive()

    if disconnect_something:
        provider = input("\n  Which provider to disconnect? (dropbox/gdrive): ").strip().lower()
        if provider in ("dropbox", "gdrive"):
            disconnect(provider)

    header("Done")
    check_connections()
    print()
