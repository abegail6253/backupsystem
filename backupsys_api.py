"""
backupsys_api.py — BackupSys Backend API
=========================================
Host this on Railway (free) — https://railway.app
Your developer secrets NEVER leave this server.

Endpoints:
  POST /send-otp          → send 6-digit OTP to admin email
  POST /verify-otp        → verify OTP, returns true/false
  POST /dropbox/exchange  → exchange auth code for tokens
  POST /gdrive/exchange   → exchange auth code for tokens
  GET  /health            → health check

HOW TO DEPLOY ON RAILWAY (free):
  1. Create account at https://railway.app
  2. New Project → Deploy from GitHub repo
     OR: New Project → Empty Project → Add Service → Python
  3. Upload this file + requirements_api.txt
  4. Set environment variables in Railway dashboard:
       BACKUPSYS_API_KEY      = (generate a random string, e.g. python -c "import secrets; print(secrets.token_hex(32))")
       BACKUPSYS_SMTP_HOST    = smtp-mail.outlook.com
       BACKUPSYS_SMTP_PORT    = 587
       BACKUPSYS_SMTP_USER    = backupsys.alerts@outlook.com
       BACKUPSYS_SMTP_PASS    = your-smtp-password
       BACKUPSYS_SMTP_FROM    = backupsys.alerts@outlook.com
       DROPBOX_APP_KEY        = your-dropbox-app-key
       DROPBOX_APP_SECRET     = your-dropbox-app-secret
       GDRIVE_CLIENT_ID       = your-client-id.apps.googleusercontent.com
       GDRIVE_CLIENT_SECRET   = your-client-secret
  5. Railway gives you a URL like: https://backupsys-api.up.railway.app
  6. Put that URL in users' .env as BACKUPSYS_API_URL

The BACKUPSYS_API_KEY is a shared secret between the app and this server.
It prevents random people from calling your API endpoints.
"""

import os
import time
import random
import smtplib
import ssl
import urllib.request
import urllib.parse
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from flask import Flask, request, jsonify

app = Flask(__name__)

# ── Config from environment variables ────────────────────────────────────────
API_KEY        = os.environ.get("BACKUPSYS_API_KEY", "")
SMTP_HOST      = os.environ.get("BACKUPSYS_SMTP_HOST", "")
SMTP_PORT      = int(os.environ.get("BACKUPSYS_SMTP_PORT", "587"))
SMTP_USER      = os.environ.get("BACKUPSYS_SMTP_USER", "")
SMTP_PASS      = os.environ.get("BACKUPSYS_SMTP_PASS", "")
SMTP_FROM      = os.environ.get("BACKUPSYS_SMTP_FROM", SMTP_USER)
DROPBOX_KEY    = os.environ.get("DROPBOX_APP_KEY", "")
DROPBOX_SECRET = os.environ.get("DROPBOX_APP_SECRET", "")
GDRIVE_ID      = os.environ.get("GDRIVE_CLIENT_ID", "")
GDRIVE_SECRET  = os.environ.get("GDRIVE_CLIENT_SECRET", "")

# ── In-memory OTP store: { email: { otp, expires_at, attempts } } ─────────────
_otp_store: dict = {}
OTP_EXPIRY_SECS  = 300   # 5 minutes


# ── Auth middleware ───────────────────────────────────────────────────────────

def _check_api_key() -> bool:
    """Verify the X-API-Key header matches our secret."""
    if not API_KEY:
        return True   # no key configured — allow (dev mode only)
    return request.headers.get("X-API-Key", "") == API_KEY


# ── Health check ──────────────────────────────────────────────────────────────

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True, "service": "BackupSys API"})


# ── OTP: Send ─────────────────────────────────────────────────────────────────

@app.route("/send-otp", methods=["POST"])
def send_otp():
    """
    Body: { "email": "admin@example.com" }
    Generates a 6-digit OTP, sends it to the email, stores it server-side.
    """
    if not _check_api_key():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    data  = request.get_json(silent=True) or {}
    email = data.get("email", "").strip()

    if not email or "@" not in email:
        return jsonify({"ok": False, "error": "Invalid email address"}), 400

    if not SMTP_HOST or not SMTP_USER:
        return jsonify({"ok": False, "error": "SMTP not configured on server"}), 500

    # Rate limit: max 1 OTP per 60 seconds per email
    existing = _otp_store.get(email)
    if existing and time.time() - (existing.get("created_at", 0)) < 60:
        return jsonify({
            "ok": False,
            "error": "Please wait 60 seconds before requesting another code"
        }), 429

    # Generate OTP
    otp = str(random.randint(100000, 999999))

    # Send email
    msg = MIMEMultipart()
    msg["Subject"] = "BackupSys — Password Reset Code"
    msg["From"]    = f"BackupSys <{SMTP_FROM}>"
    msg["To"]      = email
    msg.attach(MIMEText(
        f"BackupSys Admin Password Reset\n"
        f"{'=' * 40}\n\n"
        f"Your reset code is:\n\n"
        f"        {otp}\n\n"
        f"This code expires in 5 minutes.\n"
        f"Enter it in the BackupSys app to set a new password.\n\n"
        f"If you did not request this, ignore this email.\n"
        f"Your password has NOT been changed.",
        "plain"
    ))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as s:
            s.ehlo()
            if s.has_extn("STARTTLS"):
                s.starttls(context=ssl.create_default_context())
                s.ehlo()
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(SMTP_FROM, [email], msg.as_string())
    except Exception as e:
        return jsonify({"ok": False, "error": f"Failed to send email: {e}"}), 500

    # Store OTP
    _otp_store[email] = {
        "otp":        otp,
        "expires_at": time.time() + OTP_EXPIRY_SECS,
        "created_at": time.time(),
        "attempts":   0,
    }

    return jsonify({"ok": True, "message": f"Reset code sent to {email}"})


# ── OTP: Verify ───────────────────────────────────────────────────────────────

@app.route("/verify-otp", methods=["POST"])
def verify_otp():
    """
    Body: { "email": "admin@example.com", "code": "123456" }
    Returns { "ok": true } if correct, { "ok": false } if wrong/expired.
    """
    if not _check_api_key():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    data  = request.get_json(silent=True) or {}
    email = data.get("email", "").strip()
    code  = data.get("code", "").strip()

    entry = _otp_store.get(email)

    if not entry:
        return jsonify({"ok": False, "error": "No reset code found. Request a new one."})

    if time.time() > entry["expires_at"]:
        del _otp_store[email]
        return jsonify({"ok": False, "error": "Code has expired. Request a new one."})

    entry["attempts"] += 1

    if entry["attempts"] > 5:
        del _otp_store[email]
        return jsonify({"ok": False, "error": "Too many attempts. Request a new code."})

    if code != entry["otp"]:
        remaining = 5 - entry["attempts"]
        return jsonify({
            "ok":      False,
            "error":   f"Incorrect code. {remaining} attempt(s) remaining."
        })

    # Correct — delete OTP (single use)
    del _otp_store[email]
    return jsonify({"ok": True})


# ── Dropbox OAuth: Exchange code for tokens ───────────────────────────────────

@app.route("/dropbox/exchange", methods=["POST"])
def dropbox_exchange():
    """
    Body: { "code": "auth_code", "redirect_uri": "http://localhost:8766/..." }
    Returns Dropbox access_token + refresh_token.
    App Secret never leaves this server.
    """
    if not _check_api_key():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    data         = request.get_json(silent=True) or {}
    code         = data.get("code", "")
    redirect_uri = data.get("redirect_uri", "")
    verifier     = data.get("code_verifier", "")

    if not code:
        return jsonify({"ok": False, "error": "Missing code"}), 400

    payload = {
        "code":          code,
        "grant_type":    "authorization_code",
        "client_id":     DROPBOX_KEY,
        "client_secret": DROPBOX_SECRET,
        "redirect_uri":  redirect_uri,
    }
    if verifier:
        payload["code_verifier"] = verifier

    try:
        req_data = urllib.parse.urlencode(payload).encode()
        req = urllib.request.Request(
            "https://api.dropboxapi.com/oauth2/token",
            data=req_data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            tokens = json.loads(resp.read())

        return jsonify({
            "ok":            True,
            "access_token":  tokens.get("access_token", ""),
            "refresh_token": tokens.get("refresh_token", ""),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ── Dropbox OAuth: Refresh token ──────────────────────────────────────────────

@app.route("/dropbox/refresh", methods=["POST"])
def dropbox_refresh():
    """
    Body: { "refresh_token": "..." }
    Returns new access_token. App Secret stays on server.
    """
    if not _check_api_key():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    data          = request.get_json(silent=True) or {}
    refresh_token = data.get("refresh_token", "")

    if not refresh_token:
        return jsonify({"ok": False, "error": "Missing refresh_token"}), 400

    try:
        import base64
        auth    = base64.b64encode(f"{DROPBOX_KEY}:{DROPBOX_SECRET}".encode()).decode()
        payload = urllib.parse.urlencode({
            "grant_type":    "refresh_token",
            "refresh_token": refresh_token,
        }).encode()
        req = urllib.request.Request(
            "https://api.dropboxapi.com/oauth2/token",
            data=payload,
            headers={
                "Authorization":  f"Basic {auth}",
                "Content-Type":   "application/x-www-form-urlencoded",
            },
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            tokens = json.loads(resp.read())

        return jsonify({
            "ok":           True,
            "access_token": tokens.get("access_token", ""),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ── Google Drive OAuth: Exchange code for tokens ──────────────────────────────

@app.route("/gdrive/exchange", methods=["POST"])
def gdrive_exchange():
    """
    Body: { "code": "auth_code", "redirect_uri": "http://localhost:8765/..." }
    Returns GDrive access_token + refresh_token.
    Client Secret never leaves this server.
    """
    if not _check_api_key():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    data         = request.get_json(silent=True) or {}
    code         = data.get("code", "")
    redirect_uri = data.get("redirect_uri", "")

    if not code:
        return jsonify({"ok": False, "error": "Missing code"}), 400

    try:
        payload = urllib.parse.urlencode({
            "code":          code,
            "client_id":     GDRIVE_ID,
            "client_secret": GDRIVE_SECRET,
            "redirect_uri":  redirect_uri,
            "grant_type":    "authorization_code",
        }).encode()
        req = urllib.request.Request(
            "https://oauth2.googleapis.com/token",
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            tokens = json.loads(resp.read())

        return jsonify({
            "ok":            True,
            "access_token":  tokens.get("access_token", ""),
            "refresh_token": tokens.get("refresh_token", ""),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ── Google Drive OAuth: Refresh token ────────────────────────────────────────

@app.route("/gdrive/refresh", methods=["POST"])
def gdrive_refresh():
    """
    Body: { "refresh_token": "..." }
    Returns new access_token. Client Secret stays on server.
    """
    if not _check_api_key():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    data          = request.get_json(silent=True) or {}
    refresh_token = data.get("refresh_token", "")

    if not refresh_token:
        return jsonify({"ok": False, "error": "Missing refresh_token"}), 400

    try:
        payload = urllib.parse.urlencode({
            "client_id":     GDRIVE_ID,
            "client_secret": GDRIVE_SECRET,
            "refresh_token": refresh_token,
            "grant_type":    "refresh_token",
        }).encode()
        req = urllib.request.Request(
            "https://oauth2.googleapis.com/token",
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            tokens = json.loads(resp.read())

        return jsonify({
            "ok":           True,
            "access_token": tokens.get("access_token", ""),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ── Run ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
