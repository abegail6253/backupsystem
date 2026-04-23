"""
backupsys_api.py — BackupSys Backend API
=========================================
Host this on Railway (free) — https://railway.app
Your developer secrets NEVER leave this server.

Endpoints:
  POST /send-otp          → send 6-digit OTP to admin email
  POST /verify-otp        → verify OTP, returns true/false
  POST /gdrive/exchange   → exchange auth code for tokens
  POST /gdrive/refresh    → refresh an access token
  GET  /health            → health check

HOW TO DEPLOY ON RAILWAY (free):
  1. Create account at https://railway.app
  2. New Project → Deploy from GitHub repo
     OR: New Project → Empty Project → Add Service → Python
  3. Upload this file + requirements_api.txt
  4. Set environment variables in Railway dashboard:
       BACKUPSYS_API_KEY      = (generate: python -c "import secrets; print(secrets.token_hex(32))")
       BACKUPSYS_OTP_HMAC_KEY = (generate: python -c "import secrets; print(secrets.token_hex(32))")
       BACKUPSYS_SMTP_HOST    = smtp-mail.outlook.com
       BACKUPSYS_SMTP_PORT    = 587
       BACKUPSYS_SMTP_USER    = backupsys.alerts@outlook.com
       BACKUPSYS_SMTP_PASS    = your-smtp-password
       BACKUPSYS_SMTP_FROM    = backupsys.alerts@outlook.com
       GDRIVE_CLIENT_ID       = your-client-id.apps.googleusercontent.com
       GDRIVE_CLIENT_SECRET   = your-client-secret
  5. Add a persistent volume in Railway → Mount path: /data
  6. Railway gives you a URL like: https://backupsys-api.up.railway.app
  7. Put that URL in users' .env as BACKUPSYS_API_URL

The BACKUPSYS_API_KEY is a shared secret between the app and this server.
It prevents random people from calling your API endpoints.

OTP storage:
  OTPs are persisted to a SQLite database at $BACKUPSYS_DB_PATH
  (default: /data/backupsys.db on Railway with a mounted volume,
  or ./backupsys.db locally).  Expired rows are cleaned up automatically.

  OTPs are stored as HMAC-SHA256 digests — even if someone reads the DB
  they cannot use the hash to verify their own guess without the
  BACKUPSYS_OTP_HMAC_KEY secret.
"""

import os
import time
import hmac
import hashlib
import secrets
import smtplib
import sqlite3
import ssl
import threading
import urllib.request
import urllib.parse
import json
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from flask import Flask, request, jsonify

# ── In-memory rate limiter ─────────────────────────────────────────────────────
# Tracks per-IP request counts with a sliding window.
# Uses a simple token-bucket style dict — no Redis required for single-process.

_rate_lock  = threading.Lock()
_rate_store: dict[str, list] = {}   # ip → [timestamp, ...]

# Limits (configurable via env)
_RATE_WINDOW_S   = int(os.environ.get("BACKUPSYS_RATE_WINDOW",  "60"))   # window length (s)
_RATE_MAX_OTP    = int(os.environ.get("BACKUPSYS_RATE_MAX_OTP", "5"))    # send-otp calls / window / IP
_RATE_MAX_VERIFY = int(os.environ.get("BACKUPSYS_RATE_MAX_VERIFY", "10")) # verify-otp calls / window / IP
_RATE_MAX_GDRIVE = int(os.environ.get("BACKUPSYS_RATE_MAX_GDRIVE", "20")) # gdrive calls / window / IP


def _client_ip() -> str:
    """Return the real client IP, honouring X-Forwarded-For (set by Railway's proxy)."""
    xff = request.headers.get("X-Forwarded-For", "")
    if xff:
        return xff.split(",")[0].strip()
    return request.remote_addr or "unknown"


def _rate_check(bucket: str, limit: int) -> bool:
    """
    Return True if the request should be allowed.
    Bucket key is  '<ip>:<endpoint>'.
    Slides the window and prunes old timestamps on every call.
    """
    now = time.time()
    with _rate_lock:
        timestamps = _rate_store.get(bucket, [])
        # Remove timestamps outside the current window
        timestamps = [t for t in timestamps if now - t < _RATE_WINDOW_S]
        if len(timestamps) >= limit:
            _rate_store[bucket] = timestamps
            return False   # rate-limited
        timestamps.append(now)
        _rate_store[bucket] = timestamps
        return True


def _rate_limit_response() -> tuple:
    """Standard 429 response returned when a client is rate-limited."""
    return jsonify({
        "ok":    False,
        "error": f"Rate limit exceeded. Maximum {_RATE_MAX_OTP} requests per "
                 f"{_RATE_WINDOW_S}s window.",
    }), 429

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger("backupsys_api")

app = Flask(__name__)

# ── Config from environment variables ─────────────────────────────────────────
API_KEY       = os.environ.get("BACKUPSYS_API_KEY", "").strip()
OTP_HMAC_KEY  = os.environ.get("BACKUPSYS_OTP_HMAC_KEY", "").strip()
SMTP_HOST     = os.environ.get("BACKUPSYS_SMTP_HOST", "")
SMTP_PORT     = int(os.environ.get("BACKUPSYS_SMTP_PORT", "587"))
SMTP_USER     = os.environ.get("BACKUPSYS_SMTP_USER", "")
SMTP_PASS     = os.environ.get("BACKUPSYS_SMTP_PASS", "")
SMTP_FROM     = os.environ.get("BACKUPSYS_SMTP_FROM", SMTP_USER)
GDRIVE_ID     = os.environ.get("GDRIVE_CLIENT_ID", "")
GDRIVE_SECRET = os.environ.get("GDRIVE_CLIENT_SECRET", "")

# SQLite database path — use /data/ on Railway (persistent volume), ./ locally.
_DB_DEFAULT = "/data/backupsys.db" if os.path.isdir("/data") else "./backupsys.db"
DB_PATH = os.environ.get("BACKUPSYS_DB_PATH", _DB_DEFAULT)

OTP_EXPIRY_SECS   = 300   # 5 minutes
OTP_RATE_LIMIT_S  = 60    # minimum seconds between send-otp requests per email
OTP_MAX_ATTEMPTS  = 5

# ── Startup guards ─────────────────────────────────────────────────────────────
# Fail loudly at startup rather than silently accepting unauthenticated
# requests in production. Both keys are required.

_STARTUP_ERRORS: list[str] = []

if not API_KEY:
    _STARTUP_ERRORS.append(
        "BACKUPSYS_API_KEY is not set. "
        "Generate one with: python -c \"import secrets; print(secrets.token_hex(32))\""
    )

if not OTP_HMAC_KEY:
    _STARTUP_ERRORS.append(
        "BACKUPSYS_OTP_HMAC_KEY is not set. "
        "Generate one with: python -c \"import secrets; print(secrets.token_hex(32))\""
    )

if _STARTUP_ERRORS:
    for err in _STARTUP_ERRORS:
        log.critical("MISSING REQUIRED ENV VAR: %s", err)
    # In production (gunicorn/Railway) we raise so the process exits non-zero
    # and Railway will alert you rather than silently serving broken endpoints.
    raise RuntimeError(
        "Server cannot start — required environment variables are missing. "
        "See the log above."
    )

log.info("API key configured ✓")
log.info("OTP HMAC key configured ✓")
log.info("Database path: %s", DB_PATH)


# ── SQLite OTP store ───────────────────────────────────────────────────────────

_db_lock = threading.Lock()  # SQLite in WAL mode is reader-concurrent but
                              # we serialize writes for simplicity.

def _get_conn() -> sqlite3.Connection:
    """Open a new connection to the SQLite DB (called per-request)."""
    conn = sqlite3.connect(DB_PATH, timeout=10, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")   # allow concurrent reads
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def _init_db():
    """Create the OTP table if it doesn't exist yet."""
    with _db_lock, _get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS otp_store (
                email       TEXT    PRIMARY KEY,
                otp_hash    TEXT    NOT NULL,   -- HMAC-SHA256 of the OTP
                expires_at  REAL    NOT NULL,   -- Unix timestamp
                created_at  REAL    NOT NULL,
                attempts    INTEGER NOT NULL DEFAULT 0
            )
        """)
        conn.commit()
    log.info("OTP database initialised at %s", DB_PATH)


def _hmac_otp(otp: str) -> str:
    """Return HMAC-SHA256(otp, OTP_HMAC_KEY) as a hex string.

    Storing the HMAC instead of the plaintext OTP means that even if an
    attacker reads the database they cannot use the stored value to pass
    verification — they would need both the raw OTP *and* the HMAC key.
    """
    return hmac.new(
        OTP_HMAC_KEY.encode(),
        otp.encode(),
        hashlib.sha256,
    ).hexdigest()


def _otp_store_get(email: str) -> sqlite3.Row | None:
    """Fetch the OTP row for *email*, or None if absent."""
    with _get_conn() as conn:
        return conn.execute(
            "SELECT * FROM otp_store WHERE email = ?", (email,)
        ).fetchone()


def _otp_store_set(email: str, otp_hash: str, expires_at: float, created_at: float):
    """Insert or replace the OTP row for *email*."""
    with _db_lock, _get_conn() as conn:
        conn.execute(
            """
            INSERT INTO otp_store (email, otp_hash, expires_at, created_at, attempts)
            VALUES (?, ?, ?, ?, 0)
            ON CONFLICT(email) DO UPDATE SET
                otp_hash   = excluded.otp_hash,
                expires_at = excluded.expires_at,
                created_at = excluded.created_at,
                attempts   = 0
            """,
            (email, otp_hash, expires_at, created_at),
        )
        conn.commit()


def _otp_store_increment_attempts(email: str) -> int:
    """Increment the attempt counter and return the new value."""
    with _db_lock, _get_conn() as conn:
        conn.execute(
            "UPDATE otp_store SET attempts = attempts + 1 WHERE email = ?",
            (email,),
        )
        conn.commit()
        row = conn.execute(
            "SELECT attempts FROM otp_store WHERE email = ?", (email,)
        ).fetchone()
        return row["attempts"] if row else OTP_MAX_ATTEMPTS + 1


def _otp_store_delete(email: str):
    """Delete the OTP row for *email* (used after success or lockout)."""
    with _db_lock, _get_conn() as conn:
        conn.execute("DELETE FROM otp_store WHERE email = ?", (email,))
        conn.commit()


def _otp_store_purge_expired():
    """Delete all expired OTP rows — called on each send-otp request."""
    with _db_lock, _get_conn() as conn:
        deleted = conn.execute(
            "DELETE FROM otp_store WHERE expires_at < ?", (time.time(),)
        ).rowcount
        conn.commit()
    if deleted:
        log.info("Purged %d expired OTP row(s)", deleted)


# ── Auth middleware ────────────────────────────────────────────────────────────

def _check_api_key() -> bool:
    """Verify the X-API-Key header matches our secret.

    API_KEY is guaranteed non-empty at startup (startup guard above),
    so there is no dev-mode bypass here.
    """
    return hmac.compare_digest(
        request.headers.get("X-API-Key", ""),
        API_KEY,
    )


# ── Health check ───────────────────────────────────────────────────────────────

@app.route("/health", methods=["GET"])
def health():
    # Include a basic DB reachability check
    try:
        with _get_conn() as conn:
            conn.execute("SELECT 1")
        db_ok = True
    except Exception as exc:
        log.error("DB health check failed: %s", exc)
        db_ok = False

    return jsonify({
        "ok":      db_ok,
        "service": "BackupSys API",
        "db":      "ok" if db_ok else "error",
    }), (200 if db_ok else 503)


# ── OTP: Send ──────────────────────────────────────────────────────────────────

@app.route("/send-otp", methods=["POST"])
def send_otp():
    """
    Body: { "email": "admin@example.com" }
    Generates a cryptographically secure 6-digit OTP, sends it to the email,
    and persists an HMAC of it to SQLite.
    """
    if not _check_api_key():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    data  = request.get_json(silent=True) or {}
    email = data.get("email", "").strip().lower()

    if not email or "@" not in email or len(email) > 254:
        return jsonify({"ok": False, "error": "Invalid email address"}), 400

    if not SMTP_HOST or not SMTP_USER:
        return jsonify({"ok": False, "error": "SMTP not configured on server"}), 500

    # Purge stale rows opportunistically (keeps the table small).
    _otp_store_purge_expired()

    # Rate limit: max 1 OTP per OTP_RATE_LIMIT_S seconds per email.
    existing = _otp_store_get(email)
    if existing and (time.time() - existing["created_at"]) < OTP_RATE_LIMIT_S:
        wait = int(OTP_RATE_LIMIT_S - (time.time() - existing["created_at"])) + 1
        return jsonify({
            "ok":    False,
            "error": f"Please wait {wait} second(s) before requesting another code.",
        }), 429

    # Generate a cryptographically secure 6-digit OTP.
    # secrets.randbelow gives uniform distribution; random.randint does not.
    otp = f"{secrets.randbelow(900000) + 100000}"

    # Send the email BEFORE writing to DB — if SMTP fails we don't persist.
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
        "plain",
    ))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as s:
            s.ehlo()
            if s.has_extn("STARTTLS"):
                s.starttls(context=ssl.create_default_context())
                s.ehlo()
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(SMTP_FROM, [email], msg.as_string())
    except Exception as exc:
        log.error("SMTP error for %s: %s", email, exc)
        return jsonify({"ok": False, "error": f"Failed to send email: {exc}"}), 500

    # Persist HMAC of the OTP — never the raw digit string.
    now = time.time()
    _otp_store_set(
        email      = email,
        otp_hash   = _hmac_otp(otp),
        expires_at = now + OTP_EXPIRY_SECS,
        created_at = now,
    )
    log.info("OTP issued for %s", email)

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

    ip = _client_ip()
    if not _rate_check(f"{ip}:verify-otp", _RATE_MAX_VERIFY):
        log.warning("Rate limit hit on /verify-otp from %s", ip)
        return _rate_limit_response()

    data  = request.get_json(silent=True) or {}
    email = data.get("email", "").strip().lower()
    code  = data.get("code",  "").strip()

    entry = _otp_store_get(email)

    if not entry:
        return jsonify({"ok": False, "error": "No reset code found. Request a new one."})

    # Check expiry first — don't increment the attempt counter on an expired code.
    if time.time() > entry["expires_at"]:
        _otp_store_delete(email)
        return jsonify({"ok": False, "error": "Code has expired. Request a new one."})

    # Increment attempt counter before the comparison so a race between two
    # concurrent requests can't both get the "1 attempt remaining" message.
    attempts = _otp_store_increment_attempts(email)

    if attempts > OTP_MAX_ATTEMPTS:
        _otp_store_delete(email)
        log.warning("OTP locked out for %s after %d attempts", email, attempts)
        return jsonify({"ok": False, "error": "Too many attempts. Request a new code."})

    # Constant-time comparison of HMACs prevents timing attacks.
    if not hmac.compare_digest(_hmac_otp(code), entry["otp_hash"]):
        remaining = OTP_MAX_ATTEMPTS - attempts
        return jsonify({
            "ok":    False,
            "error": f"Incorrect code. {remaining} attempt(s) remaining.",
        })

    # Correct — single-use: delete immediately.
    _otp_store_delete(email)
    log.info("OTP verified successfully for %s", email)
    return jsonify({"ok": True})


# ── Google Drive OAuth: Exchange code for tokens ───────────────────────────────

@app.route("/gdrive/exchange", methods=["POST"])
def gdrive_exchange():
    """
    Body: { "code": "auth_code", "redirect_uri": "http://localhost:8765/..." }
    Returns GDrive access_token + refresh_token.
    Client Secret never leaves this server.
    """
    if not _check_api_key():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    ip = _client_ip()
    if not _rate_check(f"{ip}:gdrive", _RATE_MAX_GDRIVE):
        log.warning("Rate limit hit on /gdrive/exchange from %s", ip)
        return _rate_limit_response()

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
            data    = payload,
            headers = {"Content-Type": "application/x-www-form-urlencoded"},
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            tokens = json.loads(resp.read())

        return jsonify({
            "ok":            True,
            "access_token":  tokens.get("access_token", ""),
            "refresh_token": tokens.get("refresh_token", ""),
        })
    except Exception as exc:
        log.error("GDrive exchange error: %s", exc)
        return jsonify({"ok": False, "error": str(exc)}), 500


# ── Google Drive OAuth: Refresh token ─────────────────────────────────────────

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
            data    = payload,
            headers = {"Content-Type": "application/x-www-form-urlencoded"},
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            tokens = json.loads(resp.read())

        return jsonify({
            "ok":           True,
            "access_token": tokens.get("access_token", ""),
        })
    except Exception as exc:
        log.error("GDrive refresh error: %s", exc)
        return jsonify({"ok": False, "error": str(exc)}), 500


# ── Initialise DB and run ──────────────────────────────────────────────────────

_init_db()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)