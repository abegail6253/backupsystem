"""
backupsys_api.py — Lightweight Flask API for BackupSys remote notifications
===========================================================================
Deployable to Railway, Render, Fly.io, or any host that runs a Python
WSGI app.  Not required for local-only or self-hosted backups — this is
an optional server component that receives backup status POSTs from the
desktop app and exposes a minimal admin webhook endpoint.

Features
--------
- HMAC-SHA256 request authentication  (shared secret, never sent in the clear)
- OTP (one-time ping) flow  — 1 OTP per 60 s rate limit, 5-attempt lockout
- SQLite persistence  — backup events and OTP state survive restarts
- Rotating event log  — keeps the 1 000 most-recent backup events
- Admin stats endpoint  — returns summary counts (authenticated)
- File store  — POST /backup/upload receives multipart uploads from the desktop
                GET  /manifest           returns the file list for a backup dir
                GET  /files/<path>       streams a stored file back for restore

Quick start (local)
-------------------
    pip install -r requirements_api.txt
    BACKUPSYS_API_KEY=changeme python backupsys_api.py

    *** NEVER use "changeme" or any short/guessable key on a public server. ***
    The API key is your only line of defence against unauthenticated writes.
    Use a random 32+ character secret (e.g. `python -c "import secrets; print(secrets.token_hex(32))"`)
    and set it as an environment variable in your hosting dashboard.

Quick start (Railway / Render / Fly.io)
----------------------------------------
    1. Push this file, requirements_api.txt, and Procfile to your project.
    2. Set the BACKUPSYS_API_KEY environment variable in the dashboard.
    3. The platform will start the app via gunicorn (see Procfile).
    4. Set the assigned public URL as webhook_url in your desktop config.json.

Environment variables
---------------------
BACKUPSYS_API_KEY   Shared secret used in HMAC-SHA256 request signing.
                    REQUIRED — the server refuses all requests without it.
                    Minimum 32 characters recommended.
PORT                TCP port to listen on (default 5000; Railway sets this).
BACKUPSYS_DB_PATH   Path for the SQLite database (default ./backupsys.db).
BACKUPSYS_FILES_DIR Directory where uploaded backup files are stored
                    (default ./backupsys_files).  The desktop app's
                    "POST /backup/upload" multipart uploads land here;
                    GET /manifest and GET /files/<path> serve them back.

Security notes
--------------
- The API key is never transmitted.  Every request must include an
  X-BackupSys-Signature header:  HMAC-SHA256(key, body_bytes).hex()
- OTP rate limiting (1 per 60 s) and attempt lockout (5 bad guesses)
  are enforced per IP address.
- OTPs are NOT returned in the /otp/request response body.  Deliver them
  via a secondary channel (email, SMS, push notification).
- All timestamps are stored and returned as UTC ISO-8601 strings.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import sqlite3
import secrets
import string
import sys
import time
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path

# ── .env auto-load ────────────────────────────────────────────────────────────
# Calls load_dotenv() so operators can put values in a .env file without having
# to source it manually before starting gunicorn.  If python-dotenv is not
# installed the server still works — env vars must then be set externally
# (platform dashboard, shell export, etc.).  python-dotenv is listed in
# requirements_api.txt so it is always present in normal deployments.
try:
    from dotenv import load_dotenv as _load_dotenv
    _load_dotenv()
except ImportError:
    pass  # python-dotenv absent — environment variables must be set externally

# ── Optional Flask import with a friendly error ───────────────────────────────
try:
    from flask import Flask, request, jsonify, g, send_file, session, redirect, url_for, make_response, render_template
    from werkzeug.exceptions import RequestEntityTooLarge
except ImportError:
    raise SystemExit(
        "Flask is not installed.  Run:  pip install flask\n"
        "Or add it to requirements_api.txt and redeploy."
    )

# ── Optional flask-cors (graceful fallback to manual after_request hook) ──────────────
try:
    from flask_cors import CORS as _CORS
    _FLASK_CORS_AVAILABLE = True
except ImportError:
    _FLASK_CORS_AVAILABLE = False

# ─── Configuration ─────────────────────────────────────────────────────────────

API_KEY:            str  = os.environ.get("BACKUPSYS_API_KEY", "").strip()
DASHBOARD_PASSWORD:  str  = os.environ.get("BACKUPSYS_DASHBOARD_PASSWORD", "").strip()
SESSION_SECRET:      str  = os.environ.get("BACKUPSYS_SESSION_SECRET", "").strip()
DB_PATH:    Path = Path(os.environ.get("BACKUPSYS_DB_PATH", "backupsys.db"))
FILES_DIR:  Path = Path(os.environ.get("BACKUPSYS_FILES_DIR", "backupsys_files"))
PORT:       int  = int(os.environ.get("PORT", 5000))
LOG_LEVEL:  str  = os.environ.get("LOG_LEVEL", "INFO").upper()
# Only trust X-Forwarded-For when sitting behind a known reverse proxy.
# Set BACKUPSYS_TRUSTED_PROXY=true on Railway/Render/Fly.io/nginx deployments.
TRUSTED_PROXY: bool = os.environ.get("BACKUPSYS_TRUSTED_PROXY", "").strip().lower() in {"1", "true", "yes"}

# GDrive / cloud credentials (optional — only needed if the server will trigger
# server-side GDrive uploads or validate cloud config at the API layer).
GDRIVE_CLIENT_ID:     str = os.environ.get("GDRIVE_CLIENT_ID", "").strip()
GDRIVE_CLIENT_SECRET: str = os.environ.get("GDRIVE_CLIENT_SECRET", "").strip()

# OTP policy
OTP_RATE_LIMIT_SEC: int = 60    # minimum seconds between OTP requests per IP
OTP_LENGTH:         int = 8     # characters
OTP_TTL_SEC:        int = 300   # OTP expires after 5 minutes
OTP_MAX_ATTEMPTS:   int = 5     # failed guesses before lockout
OTP_LOCKOUT_SEC:    int = 900   # lockout duration (15 min) after too many failures

# Event ingestion rate-limit policy
EVENT_RATE_LIMIT:      int = 60    # max requests per IP per window
EVENT_RATE_WINDOW_SEC: int = 60    # sliding window duration in seconds
EVENT_MAX_BODY_BYTES:  int = 8_192 # reject payloads larger than 8 KB

# Event log cap
MAX_EVENTS: int = 1_000

# File store upload limit (250 MB per file — raise via env if needed)
MAX_UPLOAD_BYTES: int = int(os.environ.get("BACKUPSYS_MAX_UPLOAD_BYTES", 250 * 1024 * 1024))

# Upload endpoint rate-limit policy (separate from the event endpoint)
UPLOAD_RATE_LIMIT:      int = int(os.environ.get("BACKUPSYS_UPLOAD_RATE_LIMIT",      "30"))
UPLOAD_RATE_WINDOW_SEC: int = int(os.environ.get("BACKUPSYS_UPLOAD_RATE_WINDOW_SEC", "60"))

# Total server-side storage quota for FILES_DIR (0 = unlimited).
# Set BACKUPSYS_STORAGE_QUOTA_BYTES to cap the total bytes that all clients
# can upload combined.  Once the quota is reached, new uploads are rejected
# with HTTP 507 until old backup folders are removed.
# Example: 10 GB → BACKUPSYS_STORAGE_QUOTA_BYTES=10737418240
UPLOAD_STORAGE_QUOTA_BYTES: int = int(os.environ.get("BACKUPSYS_STORAGE_QUOTA_BYTES", "0"))

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("backupsys_api")

# ─── Flask app ────────────────────────────────────────────────────────────────

app = Flask(__name__)
app.json.sort_keys = False
# ── Stream-level upload size cap ──────────────────────────────────────────────
# The per-file Content-Length guard in backup_upload() can be bypassed by
# chunked transfer encoding (no Content-Length header).  Setting
# MAX_CONTENT_LENGTH makes Flask/Werkzeug enforce the limit at the stream
# level before the view function runs, catching both cases.
app.config['MAX_CONTENT_LENGTH'] = MAX_UPLOAD_BYTES

# ── JSON error handler for stream-level 413 ───────────────────────────────────
# When Flask/Werkzeug enforces MAX_CONTENT_LENGTH at the stream level (e.g. a
# chunked upload with no Content-Length header) it raises RequestEntityTooLarge
# before the view function runs and would normally return an HTML error page.
# This handler intercepts that exception and returns a consistent JSON 413 so
# API clients are never surprised by an HTML response.
@app.errorhandler(RequestEntityTooLarge)
def _handle_too_large(e):
    _log_request(413)
    return jsonify({"error": f"File too large (max {MAX_UPLOAD_BYTES // 1024 // 1024} MB)."}), 413

# ── Session secret for dashboard cookie auth ──────────────────────────────────
# BACKUPSYS_SESSION_SECRET must be a long random string set in the environment.
# If it is missing a per-process random key is generated — sessions will be
# invalidated on every restart.  Set the env var for persistent logins.
if SESSION_SECRET:
    app.secret_key = SESSION_SECRET
else:
    app.secret_key = secrets.token_hex(32)
    logger.critical(
        "BACKUPSYS_SESSION_SECRET is not set.  A random key has been generated "
        "for this process — all dashboard sessions WILL be invalidated on every "
        "server restart or worker recycle.  "
        "Generate a stable secret with: python -c \"import secrets; print(secrets.token_hex(32))\" "
        "and set it as BACKUPSYS_SESSION_SECRET in your environment before deploying."
    )

# ─── API versioning ───────────────────────────────────────────────────────────────────────────────
# All API routes are registered on this blueprint and exposed under /v1/.
# The unversioned paths (/backup/event, /admin/stats, …) are kept as
# compatibility aliases so existing deployed desktop clients keep working
# without any config change.  New integrations should use /v1/… paths.
from flask import Blueprint as _Blueprint
v1 = _Blueprint("v1", __name__, url_prefix="/v1")


# ─── CORS ────────────────────────────────────────────────────────────────────────────────────────
# Allow cross-origin requests on all routes.  Uses flask-cors when available;
# falls back to a minimal after_request hook so the server still works if the
# package isn't installed yet.
#
# Security note: CORS is DISABLED by default (no CORS headers sent). You must
# explicitly opt in by setting ALLOWED_ORIGINS.  HMAC-SHA256 signing is the
# real enforcement layer — every mutating request must carry a valid
# X-BackupSys-Signature — but defaulting to an open wildcard is a footgun for
# users who expose the API on a public IP without realising it.
#
# ALLOWED_ORIGINS=*                                         # open (opt-in)
# ALLOWED_ORIGINS=https://dash.example.com,http://localhost:3000  # restricted
# (unset)                                                   # no CORS headers + warning log
#
# See README → Security → API CORS for a full discussion.
_raw_origins = os.environ.get("ALLOWED_ORIGINS", "").strip()
if _raw_origins:
    _CORS_ORIGINS: "list[str] | str" = (
        [o.strip() for o in _raw_origins.split(",") if o.strip()]
        if _raw_origins != "*" else "*"
    )
else:
    _CORS_ORIGINS = None  # no CORS headers — explicit opt-in required
    logger.warning(
        "ALLOWED_ORIGINS is not set — no Access-Control-Allow-Origin headers will be sent. "
        "Set ALLOWED_ORIGINS=* to allow all origins, or restrict to specific origins "
        "(e.g. ALLOWED_ORIGINS=https://dashboard.example.com). "
        "See README → Security → API CORS for details."
    )

if _FLASK_CORS_AVAILABLE:
    if _CORS_ORIGINS is not None:
        _CORS(app, resources={r"/*": {"origins": _CORS_ORIGINS}})
else:
    @app.after_request
    def _add_cors_headers(response):
        if _CORS_ORIGINS is None:
            return response
        if _CORS_ORIGINS == "*":
            response.headers["Access-Control-Allow-Origin"] = "*"
        else:
            origin = request.headers.get("Origin", "")
            if origin in _CORS_ORIGINS:
                response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, DELETE, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = (
            "Content-Type, X-BackupSys-Signature, Authorization"
        )
        return response

    @app.route("/<path:_path>", methods=["OPTIONS"])
    def _cors_preflight(_path=""):
        """Handle CORS preflight for all paths."""
        from flask import Response
        origin = request.headers.get("Origin", "")
        if _CORS_ORIGINS is None:
            allowed = ""
        elif _CORS_ORIGINS == "*":
            allowed = "*"
        elif origin in _CORS_ORIGINS:
            allowed = origin
        else:
            allowed = ""
        return Response(
            status=204,
            headers={
                "Access-Control-Allow-Origin": allowed,
                "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type, X-BackupSys-Signature, Authorization",
                "Access-Control-Max-Age": "86400",
            },
        )


# ─── Database helpers ─────────────────────────────────────────────────────────

def _get_db() -> sqlite3.Connection:
    """Return a per-request SQLite connection stored on Flask's g object."""
    if "db" not in g:
        conn = sqlite3.connect(str(DB_PATH), detect_types=sqlite3.PARSE_DECLTYPES)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        g.db = conn
    return g.db


@app.teardown_appcontext
def _close_db(exc=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def _init_db():
    """Create tables if they do not exist (idempotent) and run safe migrations."""
    db = sqlite3.connect(str(DB_PATH))
    db.executescript("""
        CREATE TABLE IF NOT EXISTS backup_events (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            received_at  TEXT    NOT NULL,
            watch_name   TEXT,
            watch_id     TEXT,
            status       TEXT,
            files_copied INTEGER,
            bytes_copied INTEGER,
            error        TEXT,
            payload      TEXT,
            machine_id   TEXT
        );

        CREATE TABLE IF NOT EXISTS otp_state (
            ip              TEXT    PRIMARY KEY,
            otp_hash        TEXT,
            issued_at       REAL,
            attempts        INTEGER DEFAULT 0,
            locked_until    REAL    DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS api_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          TEXT    NOT NULL,
            ip          TEXT,
            method      TEXT,
            path        TEXT,
            status_code INTEGER
        );

        -- Management: command queue (API → desktop)
        CREATE TABLE IF NOT EXISTS commands (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at   TEXT    NOT NULL,
            machine_id   TEXT,
            command      TEXT    NOT NULL,
            payload      TEXT,
            status       TEXT    NOT NULL DEFAULT 'pending',
            acked_at     TEXT
        );

        -- Management: watch registry (desktop → API, updated on connect/startup)
        CREATE TABLE IF NOT EXISTS watches (
            id           TEXT    PRIMARY KEY,
            machine_id   TEXT    NOT NULL,
            name         TEXT    NOT NULL,
            source       TEXT,
            dest_type    TEXT,
            active       INTEGER DEFAULT 1,
            registered_at TEXT   NOT NULL,
            updated_at   TEXT    NOT NULL
        );
    """)

    # ── Safe migration: add machine_id to existing databases that predate v1.1.7 ──
    # ALTER TABLE ADD COLUMN is idempotent-safe — we catch the OperationalError
    # raised when the column already exists rather than checking the schema first,
    # because the check-then-alter pattern has a TOCTOU race in WAL mode.
    try:
        db.execute("ALTER TABLE backup_events ADD COLUMN machine_id TEXT")
        db.commit()
        logger.info("Migration applied: backup_events.machine_id column added")
    except Exception:
        pass  # column already exists — nothing to do

    db.commit()
    db.close()
    logger.info(f"Database initialised at {DB_PATH.resolve()}")

    # Ensure the file-store root exists
    FILES_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"File store directory: {FILES_DIR.resolve()}")


# ─── HMAC authentication ──────────────────────────────────────────────────────

def _compute_sig(body: bytes) -> str:
    """HMAC-SHA256(API_KEY, body).hex()"""
    return hmac.new(API_KEY.encode(), body, hashlib.sha256).hexdigest()


def _verify_sig(body: bytes, header_sig: str) -> bool:
    """Constant-time comparison to prevent timing attacks."""
    if not header_sig:
        return False
    expected = _compute_sig(body)
    return hmac.compare_digest(expected, header_sig.strip().lower())


TIMESTAMP_TOLERANCE_SEC: int = 60  # ±60 s replay-protection window


def _check_timestamp() -> tuple[bool, str]:
    """Validate X-BackupSys-Timestamp is present and within ±TIMESTAMP_TOLERANCE_SEC of now.

    Returns (ok, error_message).  ok=True means the timestamp is acceptable.
    Clients must send a Unix-epoch float (or integer) in this header; e.g.
        X-BackupSys-Timestamp: 1715000000.123
    """
    raw = request.headers.get("X-BackupSys-Timestamp", "")
    if not raw:
        return False, "Missing X-BackupSys-Timestamp header."
    try:
        req_ts = float(raw)
    except ValueError:
        return False, "Invalid X-BackupSys-Timestamp — must be a Unix epoch number."
    delta = abs(time.time() - req_ts)
    if delta > TIMESTAMP_TOLERANCE_SEC:
        return False, f"Request timestamp out of ±{TIMESTAMP_TOLERANCE_SEC}s window (skew={delta:.1f}s)."
    return True, ""


def require_auth(f):
    """Decorator — reject requests whose HMAC signature is missing or wrong,
    or whose timestamp falls outside the ±60 s replay-protection window."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not API_KEY:
            logger.critical("BACKUPSYS_API_KEY is not set — all requests are rejected.")
            return jsonify({"error": "Server misconfiguration: API key not set."}), 500
        # ── Replay protection: timestamp must be within ±60 s ────────────────
        ts_ok, ts_err = _check_timestamp()
        if not ts_ok:
            _log_request(401)
            return jsonify({"error": f"Unauthorized — {ts_err}"}), 401
        # ── HMAC signature ───────────────────────────────────────────────────
        sig = request.headers.get("X-BackupSys-Signature", "")
        if not _verify_sig(request.get_data(), sig):
            _log_request(401)
            return jsonify({"error": "Unauthorized — invalid or missing signature."}), 401
        return f(*args, **kwargs)
    return wrapper


# ─── Request logging ──────────────────────────────────────────────────────────

MAX_API_LOG_ROWS: int = 10_000  # keep the 10 000 most-recent request log rows

def _log_request(status_code: int):
    """Write one row to api_log (best-effort) and prune old rows."""
    try:
        db = _get_db()
        db.execute(
            "INSERT INTO api_log (ts, ip, method, path, status_code) VALUES (?,?,?,?,?)",
            (_utcnow(), _client_ip(), request.method, request.path, status_code),
        )
        # ── Enforce rolling cap so the table never grows unboundedly ─────────
        db.execute(
            """DELETE FROM api_log WHERE id NOT IN (
                   SELECT id FROM api_log ORDER BY id DESC LIMIT ?
               )""",
            (MAX_API_LOG_ROWS,),
        )
        db.commit()
    except Exception as _log_err:
        logger.warning("[_log_request] Failed to write request log row: %s", _log_err)


def _client_ip() -> str:
    """Return the real client IP.

    X-Forwarded-For is only trusted when BACKUPSYS_TRUSTED_PROXY=true is set,
    which should only be enabled when the API sits behind a known reverse proxy
    (Railway, Render, Fly.io, nginx, etc.).  Without that flag, trusting
    X-Forwarded-For unconditionally lets any caller spoof their IP and bypass
    the per-IP rate limiter.
    """
    if TRUSTED_PROXY:
        forwarded = request.headers.get("X-Forwarded-For", "")
        if forwarded:
            return forwarded.split(",")[0].strip()
    return request.remote_addr or ""


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ─── OTP helpers ──────────────────────────────────────────────────────────────

def _generate_otp() -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(OTP_LENGTH))


def _hash_otp(otp: str) -> str:
    return hashlib.sha256(otp.encode()).hexdigest()


def _otp_state(db: sqlite3.Connection, ip: str) -> sqlite3.Row | None:
    return db.execute("SELECT * FROM otp_state WHERE ip = ?", (ip,)).fetchone()


def _upsert_otp_state(db: sqlite3.Connection, ip: str, **fields):
    existing = _otp_state(db, ip)
    if existing:
        sets = ", ".join(f"{k} = ?" for k in fields)
        db.execute(f"UPDATE otp_state SET {sets} WHERE ip = ?", (*fields.values(), ip))
    else:
        fields.setdefault("attempts", 0)
        fields.setdefault("locked_until", 0.0)
        cols = ", ".join(["ip"] + list(fields.keys()))
        vals = ", ".join(["?"] * (1 + len(fields)))
        db.execute(f"INSERT INTO otp_state ({cols}) VALUES ({vals})", (ip, *fields.values()))
    db.commit()


# ─── Event endpoint rate-limiter ─────────────────────────────────────────────
# Lightweight in-process sliding-window counter (survives gunicorn single-worker
# or threaded Flask dev server; for multi-worker deployments replace with Redis).
#
# Two parallel windows are maintained:
#   • per-IP       — guards against anonymous floods / unauthenticated probes
#   • per-machine  — guards against a single desktop client (identified by the
#                    machine_id field in the JSON body) starving others when
#                    many machines share one egress IP (NAT, VPN, proxy).
#
# A request is throttled if *either* window is exhausted.  When machine_id is
# absent the per-machine check is skipped (IP-only enforcement still applies).
#
# ── Multi-worker / Redis upgrade path ────────────────────────────────────────
# The Procfile defaults to --workers 1 to keep the in-process store coherent.
# To scale beyond one worker, swap this section for flask-limiter with a Redis
# backend — it's a near-drop-in replacement:
#
#   pip install flask-limiter[redis] redis
#
#   from flask_limiter import Limiter
#   from flask_limiter.util import get_remote_address
#
#   limiter = Limiter(
#       key_func=get_remote_address,
#       app=app,
#       storage_uri=os.environ["REDIS_URL"],   # set REDIS_URL in your .env
#       default_limits=[f"{EVENT_RATE_LIMIT} per {EVENT_RATE_WINDOW_SEC} second"],
#   )
#
# Then decorate each route with @limiter.limit(...) instead of calling
# _event_rate_limit_check() manually, and remove the _event_rl_* globals below.
# See https://flask-limiter.readthedocs.io/ for full docs.
# ─────────────────────────────────────────────────────────────────────────────

import threading as _threading
_event_rl_lock         = _threading.Lock()
_event_rl_store:         dict[str, list[float]] = {}   # ip       -> [ts, ...]
_event_rl_machine_store: dict[str, list[float]] = {}   # machine  -> [ts, ...]


def _sliding_window_allow(store: dict, key: str, limit: int, window_sec: int, now: float) -> bool:
    """Shared sliding-window helper.  Must be called while *_event_rl_lock* is held."""
    cutoff = now - window_sec
    timestamps = [t for t in store.get(key, []) if t > cutoff]
    if not timestamps:
        store.pop(key, None)
    if len(timestamps) >= limit:
        store[key] = timestamps
        return False
    timestamps.append(now)
    store[key] = timestamps
    return True


def _event_rate_limit_check(ip: str, machine_id: str | None = None) -> bool:
    """Return True if the request should be allowed, False if throttled.

    Checks both the per-IP and (when *machine_id* is supplied) per-machine
    sliding windows.  A request is rejected if *either* window is full, so
    machines behind a shared NAT/VPN cannot crowd out each other.
    """
    now = time.time()
    with _event_rl_lock:
        if not _sliding_window_allow(_event_rl_store, ip, EVENT_RATE_LIMIT, EVENT_RATE_WINDOW_SEC, now):
            return False
        if machine_id and not _sliding_window_allow(
            _event_rl_machine_store, machine_id, EVENT_RATE_LIMIT, EVENT_RATE_WINDOW_SEC, now
        ):
            return False
    return True


# ─── Upload endpoint rate-limiter ─────────────────────────────────────────────
# Separate from the event rate limiter: upload requests are heavier (disk I/O,
# large payloads) so they use a lower default limit (30 req/60 s per IP).
# Tune via BACKUPSYS_UPLOAD_RATE_LIMIT / BACKUPSYS_UPLOAD_RATE_WINDOW_SEC.
_upload_rl_lock  = _threading.Lock()
_upload_rl_store: dict[str, list[float]] = {}


def _upload_rate_limit_check(ip: str) -> bool:
    """Return True if the upload request should be allowed, False if throttled."""
    now = time.time()
    with _upload_rl_lock:
        return _sliding_window_allow(
            _upload_rl_store, ip, UPLOAD_RATE_LIMIT, UPLOAD_RATE_WINDOW_SEC, now
        )


# ─── Routes ───────────────────────────────────────────────────────────────────

@app.route("/health", methods=["GET"])
def health():
    """Unauthenticated health probe — used by Railway / load-balancers.

    Rate-limited to the same per-IP sliding window as other endpoints so that
    this open route cannot be used as a free DoS amplifier.
    """
    client_ip = _client_ip()
    if not _event_rate_limit_check(client_ip):
        return jsonify({"error": "Too many requests."}), 429
    return jsonify({"status": "ok", "ts": _utcnow()}), 200


# ── Backup event ingestion ────────────────────────────────────────────────────

@app.route("/backup/event", methods=["POST"])
@require_auth
def backup_event():
    """
    Receive a backup status event from the desktop app.

    Expected JSON body (all fields optional except 'status'):
        {
            "watch_name":   "My Documents",
            "watch_id":     "w_abc123",
            "machine_id":   "DESKTOP-ABC123",   ← hostname; identifies which machine sent this
            "status":       "success" | "failure" | "cancelled",
            "files_copied": 42,
            "bytes_copied": 10485760,
            "error":        ""
        }
    """
    # ── Payload size guard ─────────────────────────────────────────────────
    if request.content_length and request.content_length > EVENT_MAX_BODY_BYTES:
        _log_request(413)
        return jsonify({"error": "Payload too large."}), 413

    payload = request.get_json(silent=True) or {}
    if not payload:
        _log_request(400)
        return jsonify({"error": "Request body must be JSON."}), 400

    status     = payload.get("status", "unknown")
    machine_id = (payload.get("machine_id") or "").strip() or None

    # ── Rate limit (per-IP and, when present, per-machine) ─────────────────
    # Checked after JSON parse so machine_id is available for the finer-
    # grained machine window.  IP is still checked independently so
    # unauthenticated/pre-parse traffic is also covered by require_auth above.
    client_ip = request.remote_addr or "unknown"
    if not _event_rate_limit_check(client_ip, machine_id):
        logger.warning(f"[event] rate-limited ip={client_ip} machine={machine_id!r}")
        _log_request(429)
        return jsonify({"error": "Too many requests. Retry after 60 s."}), 429
    db = _get_db()

    db.execute(
        """INSERT INTO backup_events
               (received_at, watch_name, watch_id, status,
                files_copied, bytes_copied, error, payload, machine_id)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (
            _utcnow(),
            payload.get("watch_name"),
            payload.get("watch_id"),
            status,
            payload.get("files_copied"),
            payload.get("bytes_copied"),
            payload.get("error") or None,
            json.dumps(payload),
            machine_id,
        ),
    )

    # ── Enforce rolling cap ────────────────────────────────────────────────
    db.execute(
        """DELETE FROM backup_events WHERE id NOT IN (
               SELECT id FROM backup_events ORDER BY id DESC LIMIT ?
           )""",
        (MAX_EVENTS,),
    )
    db.commit()

    logger.info(f"[event] machine={machine_id!r} watch={payload.get('watch_name')!r} status={status}")
    _log_request(200)
    return jsonify({"ok": True, "received_at": _utcnow()}), 200


# ── Event history ─────────────────────────────────────────────────────────────

@app.route("/backup/events", methods=["GET"])
@require_auth
def list_events():
    """Return the most-recent backup events (newest first).

    Query params:
        limit       int  max rows to return (default 50, max 500)
        status      str  filter by status (success / failure / cancelled)
        machine_id  str  filter by machine hostname (exact match)
    """
    try:
        limit = min(int(request.args.get("limit", 50)), 500)
    except ValueError:
        limit = 50

    status_filter  = request.args.get("status",     "").strip()
    machine_filter = request.args.get("machine_id", "").strip()
    db = _get_db()

    # Build query dynamically based on which filters are active
    conditions = []
    params: list = []
    if status_filter:
        conditions.append("status = ?")
        params.append(status_filter)
    if machine_filter:
        conditions.append("machine_id = ?")
        params.append(machine_filter)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    params.append(limit)

    rows = db.execute(
        f"SELECT * FROM backup_events {where} ORDER BY id DESC LIMIT ?",
        params,
    ).fetchall()

    events = [dict(r) for r in rows]
    _log_request(200)
    return jsonify({"events": events, "count": len(events)}), 200


# ── Admin stats ───────────────────────────────────────────────────────────────

@app.route("/admin/stats", methods=["GET"])
@require_auth
def admin_stats():
    """Return summary counts for the admin panel / health dashboard."""
    db = _get_db()
    total     = db.execute("SELECT COUNT(*) FROM backup_events").fetchone()[0]
    successes = db.execute("SELECT COUNT(*) FROM backup_events WHERE status='success'").fetchone()[0]
    failures  = db.execute("SELECT COUNT(*) FROM backup_events WHERE status='failure'").fetchone()[0]
    last_row  = db.execute(
        "SELECT watch_name, status, received_at, machine_id FROM backup_events ORDER BY id DESC LIMIT 1"
    ).fetchone()

    # Per-machine breakdown — one entry per distinct machine_id seen in the DB
    machine_rows = db.execute(
        """SELECT
               COALESCE(machine_id, '(unknown)') AS machine,
               COUNT(*)                           AS total,
               SUM(status = 'success')            AS successes,
               SUM(status = 'failure')            AS failures,
               MAX(received_at)                   AS last_seen
           FROM backup_events
           GROUP BY machine
           ORDER BY last_seen DESC"""
    ).fetchall()

    stats = {
        "total_events":  total,
        "successes":     successes,
        "failures":      failures,
        "last_event":    dict(last_row) if last_row else None,
        "machines":      [dict(r) for r in machine_rows],
        "ts":            _utcnow(),
    }
    _log_request(200)
    return jsonify(stats), 200


# ── OTP — request ─────────────────────────────────────────────────────────────

@app.route("/otp/request", methods=["POST"])
@require_auth
def otp_request():
    """
    Issue a fresh OTP for the requesting IP.

    Rate limited: one OTP per OTP_RATE_LIMIT_SEC seconds per IP.
    The OTP is NOT returned in the response body.  Deliver it via a
    secondary channel (email, SMS, push notification) from your own
    notification layer.  The response only confirms that an OTP was
    issued and how long it is valid for.
    """
    ip  = _client_ip()
    now = time.time()
    db  = _get_db()
    row = _otp_state(db, ip)

    # ── Lockout check ─────────────────────────────────────────────────────
    if row and row["locked_until"] and now < row["locked_until"]:
        remaining = int(row["locked_until"] - now)
        _log_request(429)
        return jsonify({
            "error":    f"Too many failed attempts — locked out for {remaining}s.",
            "locked":   True,
            "retry_in": remaining,
        }), 429

    # ── Rate limit ────────────────────────────────────────────────────────
    if row and row["issued_at"] and (now - row["issued_at"]) < OTP_RATE_LIMIT_SEC:
        retry_in = int(OTP_RATE_LIMIT_SEC - (now - row["issued_at"]))
        _log_request(429)
        return jsonify({
            "error":    f"Rate limited — retry in {retry_in}s.",
            "retry_in": retry_in,
        }), 429

    otp     = _generate_otp()
    otp_h   = _hash_otp(otp)
    _upsert_otp_state(db, ip, otp_hash=otp_h, issued_at=now, attempts=0, locked_until=0.0)

    # OTP is intentionally NOT returned in the response body — deliver it via
    # email, SMS, or a push notification from your own notification layer.
    # Log it at DEBUG level only so it appears in local dev logs but not in
    # production log-aggregators that typically ship INFO+ only.
    logger.debug(f"[otp] issued for ip={ip}")
    logger.info(f"[otp] issued for ip={ip}")
    _log_request(200)
    return jsonify({
        "ok":         True,
        "expires_in": OTP_TTL_SEC,
    }), 200


# ── OTP — verify ──────────────────────────────────────────────────────────────

@app.route("/otp/verify", methods=["POST"])
@require_auth
def otp_verify():
    """
    Verify an OTP submitted by the user.

    Expected JSON body:
        { "otp": "ABCD1234" }

    Returns:
        200  { "ok": true }                 — OTP accepted
        400  { "error": "...", "ok": false } — wrong or expired OTP
        429  { "error": "...", "locked": true } — too many failures
    """
    ip  = _client_ip()
    now = time.time()
    db  = _get_db()

    body = request.get_json(silent=True) or {}
    candidate = (body.get("otp") or "").strip().upper()
    if not candidate:
        _log_request(400)
        return jsonify({"ok": False, "error": "Missing 'otp' field."}), 400

    row = _otp_state(db, ip)

    # ── Lockout ───────────────────────────────────────────────────────────
    if row and row["locked_until"] and now < row["locked_until"]:
        remaining = int(row["locked_until"] - now)
        _log_request(429)
        return jsonify({
            "ok":       False,
            "error":    f"Account locked — retry in {remaining}s.",
            "locked":   True,
            "retry_in": remaining,
        }), 429

    # ── No OTP on record ──────────────────────────────────────────────────
    if not row or not row["otp_hash"]:
        _log_request(400)
        return jsonify({"ok": False, "error": "No OTP has been issued for this IP."}), 400

    # ── Expiry ────────────────────────────────────────────────────────────
    if now - row["issued_at"] > OTP_TTL_SEC:
        _upsert_otp_state(db, ip, otp_hash=None, issued_at=None, attempts=0, locked_until=0.0)
        _log_request(400)
        return jsonify({"ok": False, "error": "OTP has expired — request a new one."}), 400

    # ── Constant-time comparison ──────────────────────────────────────────
    expected_hash = row["otp_hash"]
    candidate_hash = _hash_otp(candidate)
    match = hmac.compare_digest(expected_hash, candidate_hash)

    if match:
        # Success — clear state
        _upsert_otp_state(db, ip, otp_hash=None, issued_at=None, attempts=0, locked_until=0.0)
        logger.info(f"[otp] verified ok for ip={ip}")
        _log_request(200)
        return jsonify({"ok": True}), 200

    # ── Wrong guess ───────────────────────────────────────────────────────
    attempts = (row["attempts"] or 0) + 1
    if attempts >= OTP_MAX_ATTEMPTS:
        locked_until = now + OTP_LOCKOUT_SEC
        _upsert_otp_state(db, ip, otp_hash=None, issued_at=None, attempts=0, locked_until=locked_until)
        logger.warning(f"[otp] lockout triggered for ip={ip} after {attempts} failures")
        _log_request(429)
        return jsonify({
            "ok":       False,
            "error":    f"Too many failed attempts — locked out for {OTP_LOCKOUT_SEC // 60} min.",
            "locked":   True,
            "retry_in": OTP_LOCKOUT_SEC,
        }), 429

    _upsert_otp_state(db, ip, attempts=attempts)
    remaining_tries = OTP_MAX_ATTEMPTS - attempts
    _log_request(400)
    return jsonify({
        "ok":             False,
        "error":          f"Incorrect OTP — {remaining_tries} attempt(s) remaining.",
        "attempts_left":  remaining_tries,
    }), 400


# ── Ping / webhook test ───────────────────────────────────────────────────────

@app.route("/ping", methods=["POST"])
@require_auth
def ping():
    """Authenticated connectivity test used by the desktop app's 'Test Webhook' button."""
    _log_request(200)
    return jsonify({"ok": True, "pong": _utcnow()}), 200


# ─── File store — upload / manifest / download ────────────────────────────────
#
# The desktop app's upload_to_https() sends every file in a backup folder as a
# separate multipart POST.  Each request carries three form fields:
#
#   file       — binary file content  (file field)
#   filename   — relative path inside the backup folder  (e.g. "data/foo.bin")
#   backup_dir — top-level backup folder name  (e.g. "mywatch_20260504_120000")
#
# Files are stored at:  FILES_DIR / <backup_dir> / <filename>
#
# download_from_https() in transport_utils.py expects:
#   GET /manifest?backup_dir=<backup_dir>
#       → { "backup_dir": "...", "files": [{"path": "rel/path", "size": N}, ...] }
#   GET /files/<backup_dir>/<rel/path/to/file>
#       → raw file bytes (octet-stream)


def _safe_path(base: Path, *parts: str) -> Path | None:
    """
    Resolve *parts* relative to *base* and return the result only if it stays
    inside *base* (prevents path-traversal attacks).  Returns None on violation.
    """
    try:
        candidate = (base / Path(*parts)).resolve()
        base_resolved = base.resolve()
        candidate.relative_to(base_resolved)  # raises ValueError if outside
        return candidate
    except (ValueError, TypeError):
        return None


@app.route("/backup/upload", methods=["POST"])
@require_auth
def backup_upload():
    """
    Receive one backup file from the desktop app's upload_to_https().

    Expects multipart/form-data with:
        file       — the file content
        filename   — relative path inside the backup folder
        backup_dir — top-level backup folder name

    The file is written to:  FILES_DIR / backup_dir / filename
    """
    # Content-length guard (before reading the stream)
    cl = request.content_length
    if cl and cl > MAX_UPLOAD_BYTES:
        _log_request(413)
        return jsonify({"error": f"File too large (max {MAX_UPLOAD_BYTES // 1024 // 1024} MB)."}), 413

    # ── Upload rate limit ──────────────────────────────────────────────────
    client_ip = _client_ip()
    if not _upload_rate_limit_check(client_ip):
        logger.warning("[upload] rate-limited ip=%s", client_ip)
        _log_request(429)
        return jsonify({"error": f"Too many upload requests. Retry after {UPLOAD_RATE_WINDOW_SEC} s."}), 429

    # ── Total storage quota ────────────────────────────────────────────────
    # Guards against filling the disk with many small files even though each
    # individual file passes the per-file size check.  Disabled when
    # BACKUPSYS_STORAGE_QUOTA_BYTES is 0 (the default).
    if UPLOAD_STORAGE_QUOTA_BYTES > 0:
        try:
            used_bytes = sum(f.stat().st_size for f in FILES_DIR.rglob("*") if f.is_file())
            incoming   = cl or 0
            if used_bytes + incoming > UPLOAD_STORAGE_QUOTA_BYTES:
                logger.warning(
                    "[upload] storage quota exceeded: used=%d incoming=%d quota=%d",
                    used_bytes, incoming, UPLOAD_STORAGE_QUOTA_BYTES,
                )
                _log_request(507)
                return jsonify({
                    "error": (
                        f"Storage quota exceeded "
                        f"(used {used_bytes // 1_048_576} MB of "
                        f"{UPLOAD_STORAGE_QUOTA_BYTES // 1_048_576} MB)."
                    )
                }), 507
        except OSError as _quota_err:
            logger.warning("[upload] Could not check disk usage for quota: %s", _quota_err)

    uploaded_file = request.files.get("file")
    filename      = (request.form.get("filename") or "").strip().replace("\\", "/")
    backup_dir    = (request.form.get("backup_dir") or "").strip()

    if not uploaded_file or not filename or not backup_dir:
        _log_request(400)
        return jsonify({"error": "Missing required fields: file, filename, backup_dir."}), 400

    dest_path = _safe_path(FILES_DIR, backup_dir, filename)
    if dest_path is None:
        _log_request(400)
        return jsonify({"error": "Invalid path — possible traversal attempt."}), 400

    try:
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        uploaded_file.save(str(dest_path))
    except OSError as exc:
        logger.error(f"[upload] Failed to save {dest_path}: {exc}")
        _log_request(500)
        return jsonify({"error": f"Server error saving file: {exc}"}), 500

    logger.info(f"[upload] Stored {backup_dir}/{filename} ({dest_path.stat().st_size} bytes)")
    _log_request(201)
    return jsonify({"ok": True, "stored": f"{backup_dir}/{filename}"}), 201


@app.route("/manifest", methods=["GET"])
@require_auth
def manifest():
    """
    Return the file manifest for a backup folder.

    Query params:
        backup_dir  — the top-level backup folder name (required)

    Response:
        {
            "backup_dir": "mywatch_20260504_120000",
            "files": [
                {"path": "data/foo.bin", "size": 102400},
                ...
            ]
        }

    This is the endpoint called by download_from_https() in transport_utils.py.
    """
    backup_dir = (request.args.get("backup_dir") or "").strip()
    if not backup_dir:
        _log_request(400)
        return jsonify({"error": "Missing required query param: backup_dir."}), 400

    backup_path = _safe_path(FILES_DIR, backup_dir)
    if backup_path is None or not backup_path.is_dir():
        _log_request(404)
        return jsonify({"error": f"Backup folder not found: {backup_dir}"}), 404

    files = []
    for fp in sorted(backup_path.rglob("*")):
        if fp.is_file():
            rel = str(fp.relative_to(backup_path)).replace("\\", "/")
            files.append({"path": rel, "size": fp.stat().st_size})

    logger.info(f"[manifest] {backup_dir}: {len(files)} file(s)")
    _log_request(200)
    return jsonify({"backup_dir": backup_dir, "files": files}), 200


@app.route("/files/<path:filepath>", methods=["GET"])
@require_auth
def serve_file(filepath: str):
    """
    Stream a single stored file back to the client.

    URL pattern:  GET /files/<backup_dir>/<relative/path/to/file>

    This is the endpoint called per-file by download_from_https() in
    transport_utils.py when reconstructing a backup locally for restore.
    """
    # filepath arrives as "backup_dir/rel/path/to/file"
    dest_path = _safe_path(FILES_DIR, filepath)
    if dest_path is None or not dest_path.is_file():
        _log_request(404)
        return jsonify({"error": f"File not found: {filepath}"}), 404

    logger.info(f"[files] Serving {filepath} ({dest_path.stat().st_size} bytes)")
    _log_request(200)
    return send_file(str(dest_path), mimetype="application/octet-stream")


# ─── Restore endpoint (stub — desktop-only in v1) ─────────────────────────────
# Full restore is currently desktop-only: the desktop app calls GET /manifest
# to enumerate files, then GET /files/<path> to download each one, and finally
# reconstructs the backup locally via transport_utils.download_from_https().
#
# A future POST /restore endpoint would allow headless / remote-triggered
# restores without launching the desktop app.  The intended contract:
#
#   POST /restore
#   {
#     "backup_dir": "<backup_dir>",          # required — which backup to restore
#     "destination": "/path/on/server",      # optional — where to write files
#     "machine_id": "<machine_id>"           # optional — target machine filter
#   }
#
# The server would queue a restore command (similar to /commands/backup) and
# the desktop agent would pick it up on the next /commands/pending poll.
# Alternatively, a fully headless path would stream all files from FILES_DIR
# to the destination directly — but that requires the server to have write
# access to the restore target, which is only safe in controlled environments.
#
# To implement: add a "restore" command type to the commands table, handle it
# in the desktop app's _poll_commands() loop, and wire this endpoint to insert
# the command row.

@app.route("/restore", methods=["POST"])
@require_auth
def restore():
    """Placeholder — server-side restore is not implemented in v1.

    Restore is desktop-only: enumerate files via GET /manifest, download each
    via GET /files/<path>, and reconstruct locally with the desktop app or CLI.
    See the source comment above this route for the planned v2 contract.
    """
    _log_request(501)
    return jsonify({
        "error": "not_implemented",
        "detail": (
            "Server-side restore is not available in this version. "
            "Use the desktop app or CLI: enumerate files with GET /manifest, "
            "then download each with GET /files/<path>."
        ),
    }), 501


# ─── Management endpoints ─────────────────────────────────────────────────────
#
# These endpoints turn the API from a read-only logging sink into a two-way
# management channel.  The desktop app polls /commands/pending on a timer and
# executes any queued commands; it also POSTs to /watches/register on startup
# so operators can discover which watches exist via GET /watches.
#
# Authentication: same HMAC-SHA256 as every other endpoint (@require_auth).
#
# Desktop → API  (push state)
#   POST /watches/register     — upsert the full watch list for a machine
#   GET  /watches              — list all registered watches
#
# API → Desktop  (issue commands)
#   POST /commands/backup      — queue a "run backup" trigger for a watch
#   POST /commands/config      — queue a config-key update
#   GET  /commands/pending     — desktop polls for unacknowledged commands
#   POST /commands/<id>/ack    — desktop marks a command done (or failed)
#   GET  /commands             — operator view of all commands (filterable)
#   DELETE /commands/<id>      — cancel a pending command before it is acked


@app.route("/watches/register", methods=["POST"])
@require_auth
def watches_register():
    """Register (or refresh) the watch list for a desktop machine.

    The desktop app should call this on startup and after any config change.

    Expected JSON body:
        {
            "machine_id": "DESKTOP-ABC123",
            "watches": [
                {
                    "id":        "w_abc123",
                    "name":      "My Documents",
                    "source":    "C:/Users/alice/Documents",
                    "dest_type": "local",
                    "active":    true
                },
                ...
            ]
        }

    Returns: { "ok": true, "registered": N }
    """
    payload    = request.get_json(silent=True) or {}
    machine_id = (payload.get("machine_id") or "").strip()
    watches    = payload.get("watches", [])

    if not machine_id:
        _log_request(400)
        return jsonify({"error": "Missing 'machine_id'."}), 400
    if not isinstance(watches, list):
        _log_request(400)
        return jsonify({"error": "'watches' must be a list."}), 400

    db  = _get_db()
    now = _utcnow()
    for w in watches:
        wid = (w.get("id") or "").strip()
        if not wid:
            continue
        db.execute(
            """INSERT INTO watches (id, machine_id, name, source, dest_type, active,
                                    registered_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?)
               ON CONFLICT(id) DO UPDATE SET
                   machine_id    = excluded.machine_id,
                   name          = excluded.name,
                   source        = excluded.source,
                   dest_type     = excluded.dest_type,
                   active        = excluded.active,
                   updated_at    = excluded.updated_at""",
            (
                wid,
                machine_id,
                (w.get("name") or "").strip(),
                w.get("source", ""),
                w.get("dest_type", "local"),
                1 if w.get("active", True) else 0,
                now,
                now,
            ),
        )
    db.commit()
    logger.info("[watches] machine=%r registered %d watch(es)", machine_id, len(watches))
    _log_request(200)
    return jsonify({"ok": True, "registered": len(watches)}), 200


@app.route("/watches", methods=["GET"])
@require_auth
def list_watches():
    """Return all registered watches.

    Query params:
        machine_id  str   filter by machine (optional)
        active      bool  filter by active state: 1 / 0  (optional)
    """
    machine_filter = request.args.get("machine_id", "").strip()
    active_filter  = request.args.get("active", "").strip()

    conditions: list[str] = []
    params: list = []
    if machine_filter:
        conditions.append("machine_id = ?")
        params.append(machine_filter)
    if active_filter in ("0", "1"):
        conditions.append("active = ?")
        params.append(int(active_filter))

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    db    = _get_db()
    rows  = db.execute(
        f"SELECT * FROM watches {where} ORDER BY machine_id, name", params
    ).fetchall()

    _log_request(200)
    return jsonify({"watches": [dict(r) for r in rows], "count": len(rows)}), 200


@app.route("/commands/backup", methods=["POST"])
@require_auth
def queue_backup():
    """Queue a 'run backup now' command for a specific watch.

    The desktop app polls /commands/pending and will execute this command
    the next time it checks in (typically within 30 seconds).

    Expected JSON body:
        {
            "machine_id": "DESKTOP-ABC123",   ← which machine to target
            "watch_id":   "w_abc123",          ← which watch to back up
            "watch_name": "My Documents"       ← human label (for logging)
        }

    Returns: { "ok": true, "command_id": N, "dest_type": "<type or null>" }

    GDrive awareness
    ----------------
    If the watch's dest_type is "cloud", the queued command payload includes
    ``"dest_type": "cloud"`` so the desktop app knows to invoke its GDrive
    upload path.  If the watch is not yet registered (no row in the watches
    table), dest_type is omitted and the desktop falls back to its own config.
    """
    payload    = request.get_json(silent=True) or {}
    machine_id = (payload.get("machine_id") or "").strip()
    watch_id   = (payload.get("watch_id")   or "").strip()

    if not machine_id or not watch_id:
        _log_request(400)
        return jsonify({"error": "Both 'machine_id' and 'watch_id' are required."}), 400

    db  = _get_db()

    # ── GDrive awareness: pull dest_type from the watch registry ─────────────
    # If dest_type == "cloud", the desktop must use its GDrive upload path.
    # We surface this in the command payload so the desktop doesn't have to
    # re-derive it from its local config — and so future server-side GDrive
    # triggering has all the context it needs.
    watch_row = db.execute(
        "SELECT dest_type FROM watches WHERE id = ? AND machine_id = ?",
        (watch_id, machine_id),
    ).fetchone()
    dest_type = watch_row["dest_type"] if watch_row else None

    cmd_payload = {**payload}
    if dest_type:
        cmd_payload["dest_type"] = dest_type

    if dest_type == "cloud":
        logger.info(
            "[commands] backup queued for cloud/GDrive watch — "
            "desktop will use GDrive upload path.  "
            "Ensure GDRIVE_CLIENT_ID / GDRIVE_CLIENT_SECRET are set in .env."
        )
        if not GDRIVE_CLIENT_ID or not GDRIVE_CLIENT_SECRET:
            logger.warning(
                "[commands] GDrive credentials missing from server env — "
                "the desktop OAuth flow may fail.  "
                "Set GDRIVE_CLIENT_ID and GDRIVE_CLIENT_SECRET in your .env."
            )

    cur = db.execute(
        "INSERT INTO commands (created_at, machine_id, command, payload, status) VALUES (?,?,?,?,?)",
        (_utcnow(), machine_id, "backup", json.dumps(cmd_payload), "pending"),
    )
    db.commit()
    command_id = cur.lastrowid
    logger.info(
        "[commands] backup queued id=%d machine=%r watch=%r dest_type=%r",
        command_id, machine_id, watch_id, dest_type,
    )
    _log_request(201)
    return jsonify({"ok": True, "command_id": command_id, "dest_type": dest_type}), 201


@app.route("/commands/config", methods=["POST"])
@require_auth
def queue_config():
    """Queue a config-key update to be applied by the desktop app.

    The desktop app applies the keys on its next /commands/pending poll.
    Only top-level scalar config keys are accepted (no nested objects) to
    prevent accidental destructive changes via the API.

    Expected JSON body:
        {
            "machine_id": "DESKTOP-ABC123",
            "updates": {
                "pause_on_metered":        true,
                "max_backup_mbps":         10,
                "integrity_check_enabled": true
            }
        }

    Returns: { "ok": true, "command_id": N }
    """
    payload    = request.get_json(silent=True) or {}
    machine_id = (payload.get("machine_id") or "").strip()
    updates    = payload.get("updates", {})

    if not machine_id:
        _log_request(400)
        return jsonify({"error": "Missing 'machine_id'."}), 400
    if not isinstance(updates, dict) or not updates:
        _log_request(400)
        return jsonify({"error": "'updates' must be a non-empty dict of scalar key→value pairs."}), 400

    # Whitelist: only scalar (non-dict, non-list) top-level keys for safety
    _SCALAR_ONLY_KEYS = {
        k for k, v in updates.items()
        if not isinstance(v, (dict, list))
    }
    if len(_SCALAR_ONLY_KEYS) != len(updates):
        _log_request(400)
        return jsonify({"error": "Only scalar (non-nested) config keys may be updated via the API."}), 400

    db  = _get_db()
    cur = db.execute(
        "INSERT INTO commands (created_at, machine_id, command, payload, status) VALUES (?,?,?,?,?)",
        (_utcnow(), machine_id, "config_update", json.dumps({"updates": updates}), "pending"),
    )
    db.commit()
    command_id = cur.lastrowid
    logger.info("[commands] config_update queued id=%d machine=%r keys=%s",
                command_id, machine_id, list(updates))
    _log_request(201)
    return jsonify({"ok": True, "command_id": command_id}), 201


@app.route("/commands/pending", methods=["GET"])
@require_auth
def pending_commands():
    """Return all unacknowledged commands for a machine.

    The desktop app calls this endpoint on a polling timer (e.g. every 30 s)
    and executes each command in order before POSTing /commands/<id>/ack.

    Query params:
        machine_id  str  REQUIRED — only return commands for this machine

    Response:
        {
            "commands": [
                {
                    "id":        42,
                    "command":   "backup",
                    "payload":   { "watch_id": "w_abc123", ... },
                    "created_at": "2026-05-04T12:00:00+00:00"
                },
                ...
            ]
        }
    """
    machine_id = request.args.get("machine_id", "").strip()
    if not machine_id:
        _log_request(400)
        return jsonify({"error": "Missing required query param: machine_id."}), 400

    db   = _get_db()
    rows = db.execute(
        "SELECT id, command, payload, created_at FROM commands "
        "WHERE machine_id = ? AND status = 'pending' ORDER BY id",
        (machine_id,),
    ).fetchall()

    cmds = []
    for r in rows:
        try:
            payload = json.loads(r["payload"] or "{}")
        except (json.JSONDecodeError, TypeError):
            payload = {}
        cmds.append({"id": r["id"], "command": r["command"],
                     "payload": payload, "created_at": r["created_at"]})

    _log_request(200)
    return jsonify({"commands": cmds, "count": len(cmds)}), 200


@app.route("/commands/<int:command_id>/ack", methods=["POST"])
@require_auth
def ack_command(command_id: int):
    """Acknowledge (mark completed or failed) a command.

    Called by the desktop app after it has processed the command.

    Expected JSON body (all optional):
        {
            "status":  "done" | "failed",   ← default "done"
            "detail":  "error message if failed"
        }

    Returns: { "ok": true }
    """
    payload = request.get_json(silent=True) or {}
    status  = payload.get("status", "done").strip()
    if status not in ("done", "failed"):
        status = "done"

    db  = _get_db()
    row = db.execute("SELECT id FROM commands WHERE id = ?", (command_id,)).fetchone()
    if not row:
        _log_request(404)
        return jsonify({"error": f"Command {command_id} not found."}), 404

    db.execute(
        "UPDATE commands SET status = ?, acked_at = ? WHERE id = ?",
        (status, _utcnow(), command_id),
    )
    db.commit()
    logger.info("[commands] ack id=%d status=%s", command_id, status)
    _log_request(200)
    return jsonify({"ok": True, "command_id": command_id, "status": status}), 200


@app.route("/commands/<int:command_id>", methods=["DELETE"])
@require_auth
def cancel_command(command_id: int):
    """Cancel a pending command before the desktop picks it up.

    Returns 409 if the command has already been acknowledged.
    """
    db  = _get_db()
    row = db.execute("SELECT id, status FROM commands WHERE id = ?", (command_id,)).fetchone()
    if not row:
        _log_request(404)
        return jsonify({"error": f"Command {command_id} not found."}), 404
    if row["status"] != "pending":
        _log_request(409)
        return jsonify({"error": f"Command {command_id} is already '{row['status']}' — cannot cancel."}), 409

    db.execute("UPDATE commands SET status = 'cancelled', acked_at = ? WHERE id = ?",
               (_utcnow(), command_id))
    db.commit()
    logger.info("[commands] cancelled id=%d", command_id)
    _log_request(200)
    return jsonify({"ok": True, "command_id": command_id}), 200


@app.route("/commands", methods=["GET"])
@require_auth
def list_commands():
    """Return the command history (operator view).

    Query params:
        machine_id  str  filter by machine
        status      str  filter: pending / done / failed / cancelled
        limit       int  max rows (default 100, max 500)
    """
    try:
        limit = min(int(request.args.get("limit", 100)), 500)
    except ValueError:
        limit = 100

    machine_filter = request.args.get("machine_id", "").strip()
    status_filter  = request.args.get("status",     "").strip()

    conditions: list[str] = []
    params: list = []
    if machine_filter:
        conditions.append("machine_id = ?")
        params.append(machine_filter)
    if status_filter:
        conditions.append("status = ?")
        params.append(status_filter)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    params.append(limit)

    db   = _get_db()
    rows = db.execute(
        f"SELECT * FROM commands {where} ORDER BY id DESC LIMIT ?", params
    ).fetchall()

    _log_request(200)
    return jsonify({"commands": [dict(r) for r in rows], "count": len(rows)}), 200


# ─── Startup checks ───────────────────────────────────────────────────────────

def _check_startup_env():
    """Validate required and recommended environment variables at launch.

    Logs CRITICAL/WARNING for anything that will silently degrade functionality
    so operators get actionable feedback before the first request arrives.
    """
    # ── BACKUPSYS_API_KEY ────────────────────────────────────────────────────
    # Hard-exit on missing or obviously-weak keys so a mis-configured deploy
    # never silently accepts connections.  Generate a safe key with:
    #   python -c "import secrets; print(secrets.token_hex(32))"
    _known_weak = {"changeme", "change_me", "password", "secret", "test", "dev"}
    if not API_KEY:
        logger.critical(
            "BACKUPSYS_API_KEY is not set. "
            "Set it as an environment variable before starting the server. "
            "Generate one with: python -c \"import secrets; print(secrets.token_hex(32))\""
        )
        sys.exit(1)
    elif API_KEY.lower() in _known_weak:
        logger.critical(
            f"BACKUPSYS_API_KEY is set to a well-known placeholder ({API_KEY!r}). "
            "This key is trivially guessable and must not be used in any deployment. "
            "Generate a safe key with: python -c \"import secrets; print(secrets.token_hex(32))\""
        )
        sys.exit(1)
    elif len(API_KEY) < 32:
        logger.critical(
            f"BACKUPSYS_API_KEY is only {len(API_KEY)} characters — "
            "a minimum of 32 random characters is required for production. "
            "Generate one with: python -c \"import secrets; print(secrets.token_hex(32))\""
        )
        sys.exit(1)
    else:
        logger.info("BACKUPSYS_API_KEY loaded — OK (%d chars)", len(API_KEY))

    # ── Multi-worker / WEB_CONCURRENCY guard ─────────────────────────────────
    # The in-process sliding-window rate limiter is NOT safe when more than one
    # worker is running — each worker keeps its own counter, so the effective
    # limit becomes (workers × rate-limit) per client.  The Procfile hard-codes
    # --workers 1, but platforms like Railway and Render set WEB_CONCURRENCY in
    # the environment, which an operator might wire up without realising the risk.
    # Detect and shout loudly rather than silently multiply the effective limit.
    _wc_raw = os.environ.get("WEB_CONCURRENCY", "").strip()
    if _wc_raw:
        try:
            _wc = int(_wc_raw)
            if _wc > 1:
                logger.warning(
                    "⚠️  WEB_CONCURRENCY=%d detected.  The in-process sliding-window rate "
                    "limiter stores state per-worker, so the effective rate limit is currently "
                    "%d× EVENT_RATE_LIMIT (%d req/min per IP) and %d× UPLOAD_RATE_LIMIT "
                    "(%d req/min per IP).  Either keep --workers 1 (hardcoded in the Procfile) "
                    "or replace the in-process store with a Redis backend before scaling.  "
                    "See the Redis upgrade path comment in _sliding_window_allow() for the "
                    "drop-in snippet.",
                    _wc, _wc, EVENT_RATE_LIMIT * _wc,
                    _wc, UPLOAD_RATE_LIMIT * _wc,
                )
        except ValueError:
            pass  # non-integer WEB_CONCURRENCY — ignore

    # ── GDrive / cloud credentials ───────────────────────────────────────────
    # The API queues cloud-backup commands that the desktop executes, but the
    # credentials must be present in the .env for OAuth flows to succeed.
    # Warn early so operators don't discover a missing credential at click time.
    _gdrive_issues: list[str] = []
    if not GDRIVE_CLIENT_ID:
        _gdrive_issues.append("GDRIVE_CLIENT_ID")
    if not GDRIVE_CLIENT_SECRET:
        _gdrive_issues.append("GDRIVE_CLIENT_SECRET")

    if _gdrive_issues:
        logger.warning(
            "GDrive credentials not set: %s.  "
            "Cloud/GDrive backup commands can still be queued, but the "
            "desktop app's OAuth flow will fail when executed.  "
            "Add these to your .env file (see .env.example).",
            ", ".join(_gdrive_issues),
        )
    else:
        logger.info("GDrive credentials loaded — OK")

    # ── Session secret ────────────────────────────────────────────────────────
    if not SESSION_SECRET:
        logger.critical(
            "BACKUPSYS_SESSION_SECRET is not set.  Dashboard logins will not "
            "survive a server restart or gunicorn worker recycle.  "
            "Generate one with: python -c \"import secrets; print(secrets.token_hex(32))\""
        )
    else:
        logger.info("BACKUPSYS_SESSION_SECRET loaded — OK")

    # ── Dashboard password ────────────────────────────────────────────────────
    if not DASHBOARD_PASSWORD:
        logger.warning(
            "BACKUPSYS_DASHBOARD_PASSWORD is not set.  "
            "The dashboard falls back to a POST-form session login using "
            "BACKUPSYS_API_KEY.  Set BACKUPSYS_DASHBOARD_PASSWORD for "
            "browser-native HTTP Basic Auth (recommended for production)."
        )

    # ── Persistent storage paths ──────────────────────────────────────────────
    # On Railway, Render, Fly.io, and most container platforms the working
    # directory is part of the *ephemeral* container filesystem — it is wiped
    # on every redeploy or restart.  If DB_PATH or FILES_DIR still points at a
    # relative path (i.e. inside the container), all backup events and uploaded
    # files will be lost on the next deploy.
    #
    # Fix: mount a persistent volume in your hosting dashboard and set:
    #   BACKUPSYS_DB_PATH=/data/backupsys.db
    #   BACKUPSYS_FILES_DIR=/data/backupsys_files
    #
    # Railway docs: https://docs.railway.app/reference/volumes
    # Render  docs: https://render.com/docs/disks
    # Fly.io  docs: https://fly.io/docs/reference/volumes/
    _ephemeral_paths: list[str] = []
    if not DB_PATH.is_absolute():
        _ephemeral_paths.append(f"BACKUPSYS_DB_PATH={DB_PATH} (relative path — ephemeral on container platforms)")
    if not FILES_DIR.is_absolute():
        _ephemeral_paths.append(f"BACKUPSYS_FILES_DIR={FILES_DIR} (relative path — ephemeral on container platforms)")
    if _ephemeral_paths:
        logger.warning(
            "Data paths are relative and will be wiped on redeploy: %s.  "
            "Set BACKUPSYS_DB_PATH and BACKUPSYS_FILES_DIR to absolute paths "
            "on a persistent volume (e.g. /data/backupsys.db, /data/backupsys_files).",
            "; ".join(_ephemeral_paths),
        )
    else:
        logger.info(
            "Persistent paths OK — DB: %s  Files: %s",
            DB_PATH.resolve(), FILES_DIR.resolve(),
        )


# Keep the old name as an alias so any external callers aren't broken.
_check_api_key = _check_startup_env


# ─── Entry point ──────────────────────────────────────────────────────────────


# ─── Web Dashboard ────────────────────────────────────────────────────────────
# The dashboard HTML lives in templates/dashboard.html and
# templates/dashboard_login.html so it can be edited with syntax highlighting
# and diffed independently of the Python source.  Flask's render_template()
# loads them from that directory automatically.


def _fmt_bytes(n) -> str:
    """Human-readable byte size for the dashboard."""
    try:
        n = int(n or 0)
    except (TypeError, ValueError):
        return "—"
    if n == 0:
        return "—"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024
    return f"{n:.1f} PB"



@app.route("/dashboard/login", methods=["GET"])
def dashboard_login_form():
    """Show the dashboard login page."""
    return render_template("dashboard_login.html", error=None), 200


@app.route("/dashboard/login", methods=["POST"])
def dashboard_login_submit():
    """
    Verify the submitted password and set an HTTP-only session cookie.

    The password is accepted via a POST body (never a query string), so it
    does not appear in server access logs, browser history, or Referer headers.

    Two credentials are accepted (checked in order):
      1. BACKUPSYS_DASHBOARD_PASSWORD — dedicated dashboard password (recommended)
      2. BACKUPSYS_API_KEY            — fallback when dashboard password is not set

    On success a signed, HTTP-only session cookie is issued and the user is
    redirected to /dashboard.  On failure, the login form is re-rendered with
    a generic error message (constant-time comparison prevents timing attacks).
    """
    submitted = (request.form.get("password") or "").strip()
    authed = False

    if DASHBOARD_PASSWORD:
        authed = bool(submitted) and hmac.compare_digest(submitted, DASHBOARD_PASSWORD)
    elif API_KEY:
        authed = bool(submitted) and hmac.compare_digest(submitted, API_KEY)

    if authed:
        session["dashboard_authed"] = True
        _log_request(302)
        return redirect(url_for("dashboard"))

    _log_request(401)
    return render_template("dashboard_login.html", error="Incorrect password."), 401


@app.route("/dashboard/logout", methods=["GET"])
def dashboard_logout():
    """Clear the dashboard session cookie and redirect to the login page."""
    session.pop("dashboard_authed", None)
    return redirect(url_for("dashboard_login_form"))


@app.route("/dashboard", methods=["GET"])
def dashboard():
    """
    HTML dashboard — shows summary stats and a recent-events table.

    Auth (choose one):

    1. BACKUPSYS_DASHBOARD_PASSWORD (recommended)
       Set this env var to get HTTP Basic Auth — the browser shows a native
       login prompt.  Username: "admin".

    2. BACKUPSYS_API_KEY fallback
       If DASHBOARD_PASSWORD is not set, the login form at /dashboard/login
       accepts the API key as the password.  Credentials are submitted via
       POST body so they never appear in logs, history, or Referer headers.

    For either path a signed HTTP-only session cookie is issued on success,
    so subsequent page loads don't re-prompt.  Visit /dashboard/logout to
    clear the session.
    """
    # ── Auth layer 1: HTTP Basic Auth via BACKUPSYS_DASHBOARD_PASSWORD ──────
    if DASHBOARD_PASSWORD:
        # Allow cookie-based session (set by /dashboard/login) as well as
        # browser-native Basic Auth so both paths work consistently.
        if not session.get("dashboard_authed"):
            auth = request.authorization
            ok = (
                auth is not None
                and auth.username == "admin"
                and hmac.compare_digest(auth.password or "", DASHBOARD_PASSWORD)
            )
            if ok:
                # Promote to a session so subsequent requests don't re-prompt.
                session["dashboard_authed"] = True
            else:
                _log_request(401)
                return (
                    "",
                    401,
                    {
                        "WWW-Authenticate": 'Basic realm="BackupSys Dashboard"',
                        "Content-Type": "text/html; charset=utf-8",
                    },
                )

    # ── Auth layer 2: cookie session set by /dashboard/login ────────────────
    elif API_KEY:
        # Accept three equivalent auth paths (no DASHBOARD_PASSWORD configured):
        #   a) Signed session cookie issued by the POST login form
        #   b) ?key=<API_KEY> query-string (for scripts / direct links)
        #   c) X-BackupSys-Key header
        key_param = request.args.get("key", "").strip()
        key_header = request.headers.get("X-BackupSys-Key", "").strip()
        if not session.get("dashboard_authed"):
            provided = key_param or key_header
            if provided and hmac.compare_digest(provided, API_KEY):
                session["dashboard_authed"] = True
            else:
                _log_request(401)
                # Return 401 whether or not a key was provided — never redirect
                # when API_KEY auth is the active mechanism.
                return (
                    "",
                    401,
                    {"WWW-Authenticate": 'Bearer realm="BackupSys Dashboard"'},
                )

    else:
        # Neither auth mechanism is configured — refuse entirely.
        _log_request(503)
        return (
            "<h2>Dashboard Unavailable</h2>"
            "<p>The dashboard requires authentication.  "
            "Set <code>BACKUPSYS_DASHBOARD_PASSWORD</code> (recommended) or "
            "<code>BACKUPSYS_API_KEY</code> and restart the server.</p>",
            503,
            {"Content-Type": "text/html; charset=utf-8"},
        )

    db = _get_db()

    # ── Summary counts ──────────────────────────────────────────────────────
    total     = db.execute("SELECT COUNT(*) FROM backup_events").fetchone()[0]
    successes = db.execute("SELECT COUNT(*) FROM backup_events WHERE status='success'").fetchone()[0]
    failures  = db.execute("SELECT COUNT(*) FROM backup_events WHERE status='failure'").fetchone()[0]

    # ── Per-machine summary ─────────────────────────────────────────────────
    machine_rows_data = db.execute(
        """SELECT COALESCE(machine_id,'(unknown)') AS machine,
                  COUNT(*)                           AS total,
                  SUM(status='success')              AS ok,
                  SUM(status='failure')              AS err,
                  MAX(received_at)                   AS last_seen
           FROM backup_events
           GROUP BY machine ORDER BY last_seen DESC"""
    ).fetchall()

    machine_html_parts = []
    for r in machine_rows_data:
        machine_html_parts.append(
            f'<div class="machine">'
            f'<div class="name">💻 {r["machine"]}</div>'
            f'<div class="detail">'
            f'✅ {r["ok"] or 0} &nbsp; ❌ {r["err"] or 0} &nbsp; total {r["total"]}'
            f'</div>'
            f'<div class="detail ts">Last: {(r["last_seen"] or "")[:19]}</div>'
            f'</div>'
        )
    machine_rows_html = "\n".join(machine_html_parts) or '<div class="machine"><div class="detail">No events yet.</div></div>'

    # ── Recent events ───────────────────────────────────────────────────────
    limit = min(int(request.args.get("limit", 200)), 1000)
    events = db.execute(
        "SELECT * FROM backup_events ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()

    def _pill(status: str) -> str:
        cls = {"success": "ok", "failure": "err"}.get(status, "can")
        icon = {"success": "✅", "failure": "❌", "cancelled": "⏹"}.get(status, "•")
        return f'<span class="pill {cls}">{icon} {status}</span>'

    event_rows_parts = []
    for e in events:
        ts       = (e["received_at"] or "")[:19]
        machine  = e["machine_id"] or "—"
        watch    = e["watch_name"] or "—"
        status   = e["status"] or "unknown"
        files    = e["files_copied"] if e["files_copied"] is not None else "—"
        size     = _fmt_bytes(e["bytes_copied"])
        err_txt  = e["error"] or ""
        err_cell = f'<td class="error-cell" title="{err_txt}">{err_txt[:80]}</td>' if err_txt else "<td>—</td>"
        event_rows_parts.append(
            f"<tr>"
            f'<td class="ts">{ts}</td>'
            f"<td>{machine}</td>"
            f"<td>{watch}</td>"
            f"<td>{_pill(status)}</td>"
            f"<td>{files}</td>"
            f"<td>{size}</td>"
            f"{err_cell}"
            f"</tr>"
        )
    event_rows_html = "\n".join(event_rows_parts) or '<tr><td colspan="7" style="text-align:center;color:#94a3b8;padding:20px">No events recorded yet.</td></tr>'

    _log_request(200)
    return render_template(
        "dashboard.html",
        ts=_utcnow(),
        event_count=limit,
        total=total,
        successes=successes,
        failures=failures,
        machine_count=len(machine_rows_data),
        machine_rows=machine_rows_html,
        event_rows=event_rows_html,
    ), 200


# ─── Register the /v1 blueprint ──────────────────────────────────────────────────────────────
# Mirror every app route (except /health, /ping, /dashboard, and the OPTIONS
# catch-all) onto the v1 blueprint so they are accessible at /v1/<path>.
# The original flat paths remain as-is for backward compatibility.
_V1_SKIP = {"/health", "/ping", "/dashboard", "/dashboard/login", "/dashboard/logout"}

def _make_v1_proxy(rule, view_func, methods):
    """Return a thin wrapper that delegates to the original view function."""
    from functools import wraps

    @wraps(view_func)
    def _proxy(*args, **kwargs):
        return view_func(*args, **kwargs)

    return _proxy


for _rule in list(app.url_map.iter_rules()):
    if (
        _rule.rule in _V1_SKIP
        or _rule.rule.startswith("/v1/")
        or "<_path>" in _rule.rule   # the OPTIONS catch-all
    ):
        continue
    _methods = (_rule.methods or set()) - {"HEAD", "OPTIONS"}
    if not _methods:
        continue
    _v1_rule = _rule.rule  # e.g. "/backup/event"
    _endpoint = f"v1_{_rule.endpoint}"
    _view = app.view_functions[_rule.endpoint]
    v1.add_url_rule(
        _v1_rule,
        endpoint=_endpoint,
        view_func=_make_v1_proxy(_v1_rule, _view, _methods),
        methods=list(_methods),
    )

app.register_blueprint(v1)

# ── Module-level startup ───────────────────────────────────────────────────────
# These must run at import time, not just inside __main__, so that gunicorn
# workers execute them when the module is loaded via the Procfile:
#   gunicorn backupsys_api:app …
# Under gunicorn __name__ is "backupsys_api", never "__main__", so anything
# gated on `if __name__ == "__main__":` is silently skipped in production.
#
# _check_startup_env() — validates API key, session secret, and storage paths;
#                        calls sys.exit(1) on a missing/weak/short API key.
# _init_db()           — creates SQLite tables (CREATE TABLE IF NOT EXISTS) and
#                        runs safe migrations; idempotent on every restart.
_check_startup_env()
_init_db()


if __name__ == "__main__":
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    logger.info(f"Starting BackupSys API on port {PORT}  (debug={debug})")
    app.run(host="0.0.0.0", port=PORT, debug=debug)