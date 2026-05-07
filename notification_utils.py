"""
notification_utils.py — Email and webhook notification helpers for BackupSys.

Provides:
    send_email_notification(email_config, subject, body)  → dict
    send_webhook_notification(url, payload)               → dict
    build_backup_email(result)                            → (subject, body)

Integration — call from desktop_app.py (or wherever run_backup() results are
handled) after each backup completes, for example:

    from notification_utils import (
        send_email_notification, send_webhook_notification, build_backup_email
    )

    # ── Email notification ─────────────────────────────────────────────────
    ec = cfg.get("email_config", {})
    if ec.get("enabled"):
        success = result["status"] == "success"
        should_notify = (
            (success     and ec.get("notify_on_success", False)) or
            (not success and ec.get("notify_on_failure", True))
        )
        if should_notify:
            subj, body = build_backup_email(result)
            email_result = send_email_notification(ec, subj, body)
            if not email_result["ok"]:
                logger.warning(f"Email notification failed: {email_result['error']}")

    # ── Webhook notification ───────────────────────────────────────────────
    webhook_url = cfg.get("webhook_url", "")
    if webhook_url:
        success = result["status"] == "success"
        if success and not cfg.get("webhook_on_success", False):
            pass  # success webhooks are opt-in
        else:
            send_webhook_notification(webhook_url, {
                "event":      "backup_complete",
                "status":     result["status"],
                "watch_name": result.get("watch_name"),
                "watch_id":   result.get("watch_id"),
                "backup_id":  result.get("backup_id"),
                "timestamp":  result.get("timestamp"),
                "files_copied": result.get("files_copied", 0),
                "total_size": result.get("total_size", "0 B"),
                "duration_s": result.get("duration_s", 0),
                "error":      result.get("error"),
            })

email_config shape (from config.json):
    {
        "enabled":           true/false,
        "smtp_host":         "smtp.gmail.com",
        "smtp_port":         587,
        "smtp_use_ssl":      false,          # true = SMTP over SSL (port 465); false = STARTTLS
        "username":          "you@gmail.com",
        "password":          "app_password",
        "from_addr":         "you@gmail.com",
        "to_addr":           "alerts@example.com",
        "notify_on_success": false,
        "notify_on_failure": true
    }

The password may also be supplied via the BACKUPSYS_EMAIL_PASSWORD environment
variable (config_manager.py already injects this override on load).
"""

import logging
import os
import ssl
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

logger = logging.getLogger(__name__)


# ─── Email ────────────────────────────────────────────────────────────────────

def send_email_notification(
    email_config: dict,
    subject: str,
    body: str,
    body_html: Optional[str] = None,
) -> dict:
    """
    Send an email notification via SMTP/SMTPS.

    Supports:
      - STARTTLS  (smtp_use_ssl=False, typical port 587)
      - SSL/TLS   (smtp_use_ssl=True,  typical port 465)
      - Plain SMTP (smtp_port=25, no TLS — not recommended)

    Returns { ok: bool, error: str | None }.
    """
    host     = email_config.get("smtp_host", "").strip()
    port     = int(email_config.get("smtp_port", 587))
    use_ssl  = bool(email_config.get("smtp_use_ssl", False))
    username = email_config.get("username", "").strip()
    password = email_config.get("password", "")
    from_addr = email_config.get("from_addr", username).strip() or username
    to_addr   = email_config.get("to_addr", "").strip()

    if not host:
        return {"ok": False, "error": "SMTP host not configured"}
    if not to_addr:
        return {"ok": False, "error": "Recipient email (to_addr) not configured"}
    if not from_addr:
        return {"ok": False, "error": "Sender email (from_addr / username) not configured"}

    # Build the MIME message
    if body_html:
        msg = MIMEMultipart("alternative")
        msg.attach(MIMEText(body, "plain", "utf-8"))
        msg.attach(MIMEText(body_html, "html", "utf-8"))
    else:
        msg = MIMEMultipart()
        msg.attach(MIMEText(body, "plain", "utf-8"))

    msg["Subject"] = subject
    msg["From"]    = from_addr
    msg["To"]      = to_addr

    try:
        if use_ssl:
            # Direct SSL connection (port 465)
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(host, port, context=context, timeout=30) as server:
                if username and password:
                    server.login(username, password)
                server.sendmail(from_addr, [to_addr], msg.as_string())
        else:
            # STARTTLS (port 587) or plain (port 25)
            with smtplib.SMTP(host, port, timeout=30) as server:
                server.ehlo()
                # Upgrade to TLS if server supports it
                if server.has_extn("STARTTLS"):
                    context = ssl.create_default_context()
                    server.starttls(context=context)
                    server.ehlo()
                if username and password:
                    server.login(username, password)
                server.sendmail(from_addr, [to_addr], msg.as_string())

        logger.info(f"[email] Notification sent to {to_addr}: {subject}")
        return {"ok": True, "error": None}

    except smtplib.SMTPAuthenticationError:
        err = "SMTP authentication failed — check username/password or use an App Password"
        logger.warning(f"[email] {err}")
        return {"ok": False, "error": err}
    except smtplib.SMTPConnectError as e:
        err = f"Could not connect to {host}:{port} — {e}"
        logger.warning(f"[email] {err}")
        return {"ok": False, "error": err}
    except smtplib.SMTPException as e:
        err = f"SMTP error: {e}"
        logger.warning(f"[email] {err}")
        return {"ok": False, "error": err}
    except OSError as e:
        err = f"Network error connecting to {host}:{port}: {e}"
        logger.warning(f"[email] {err}")
        return {"ok": False, "error": err}
    except Exception as e:
        logger.warning(f"[email] Unexpected error: {e}")
        return {"ok": False, "error": str(e)}


def build_backup_email(result: dict) -> tuple:
    """
    Build a (subject, plain_text_body) tuple from a backup result dict.
    Returns concise, human-readable content suitable for any email client.
    """
    status     = result.get("status", "unknown").upper()
    watch_name = result.get("watch_name", "Unknown Watch")
    timestamp  = result.get("timestamp", "")[:19].replace("T", " ")
    error      = result.get("error", "")

    if status == "SUCCESS":
        icon    = "✅"
        summary = (
            f"Backup completed successfully.\n\n"
            f"  Watch:         {watch_name}\n"
            f"  Time:          {timestamp}\n"
            f"  Files copied:  {result.get('files_copied', 0)}\n"
            f"  Size:          {result.get('total_size', '0 B')}\n"
            f"  Duration:      {result.get('duration_s', 0):.1f}s\n"
            f"  Backup ID:     {result.get('backup_id', 'N/A')}\n"
        )
        if result.get("compression_ratio", 0) > 0:
            summary += f"  Compression:   {result['compression_ratio']}% saved\n"
        if result.get("cloud_upload"):
            cu = result["cloud_upload"]
            summary += f"  Cloud upload:  {'✅ OK' if cu.get('ok') else '⚠ ' + cu.get('error', 'failed')}\n"
        failed = result.get("failed_files", [])
        if failed:
            summary += f"\n⚠ {len(failed)} file(s) could not be copied:\n"
            for ff in failed[:10]:
                summary += f"  - {ff.get('path', '?')}: {ff.get('reason', '?')}\n"
            if len(failed) > 10:
                summary += f"  … and {len(failed) - 10} more\n"
    elif status == "CANCELLED":
        icon    = "⏹"
        summary = (
            f"Backup was cancelled by the user.\n\n"
            f"  Watch:    {watch_name}\n"
            f"  Time:     {timestamp}\n"
            f"  Backup ID: {result.get('backup_id', 'N/A')}\n"
        )
    else:
        icon    = "❌"
        summary = (
            f"Backup FAILED.\n\n"
            f"  Watch:     {watch_name}\n"
            f"  Time:      {timestamp}\n"
            f"  Error:     {error or 'Unknown error'}\n"
            f"  Backup ID: {result.get('backup_id', 'N/A')}\n"
        )

    subject = f"{icon} BackupSys — {status}: {watch_name} ({timestamp})"
    body    = f"BackupSys Notification\n{'=' * 50}\n\n{summary}\n"
    return subject, body


# ─── Webhook ──────────────────────────────────────────────────────────────────

def send_webhook_notification(url: str, payload: dict) -> dict:
    """
    POST a JSON payload to a webhook URL.

    Uses only the standard library (urllib). Compatible with Slack incoming
    webhooks, Discord webhooks, n8n/Zapier/Make HTTP triggers, and any custom
    REST endpoint that accepts application/json.

    Returns { ok: bool, status: int | None, error: str | None }.
    """
    import json as _json
    import urllib.request
    import urllib.error

    if not url or not url.strip():
        return {"ok": False, "status": None, "error": "Webhook URL is empty"}

    try:
        data = _json.dumps(payload, default=str).encode("utf-8")
        req  = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json", "User-Agent": "BackupSys/2.0"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            status = resp.status
            if 200 <= status < 300:
                logger.info(f"[webhook] POST → {url}  ({status})")
                return {"ok": True, "status": status, "error": None}
            else:
                err = f"HTTP {status}"
                logger.warning(f"[webhook] POST → {url} returned {err}")
                return {"ok": False, "status": status, "error": err}

    except urllib.error.HTTPError as e:
        err = f"HTTP {e.code}: {e.reason}"
        logger.warning(f"[webhook] {err} for {url}")
        return {"ok": False, "status": e.code, "error": err}
    except urllib.error.URLError as e:
        err = f"URL error: {e.reason}"
        logger.warning(f"[webhook] {err} for {url}")
        return {"ok": False, "status": None, "error": err}
    except Exception as e:
        logger.warning(f"[webhook] Unexpected error for {url}: {e}")
        return {"ok": False, "status": None, "error": str(e)}


def test_webhook(url: str) -> dict:
    """
    Send a test ping to a webhook URL to verify connectivity before saving.
    Returns { ok, status, error }.
    """
    return send_webhook_notification(url, {
        "event":   "test",
        "message": "BackupSys webhook test — if you see this it works!",
        "source":  "BackupSys",
    })


def test_email(email_config: dict) -> dict:
    """
    Send a test email using the provided config to verify SMTP connectivity.
    Mirrors test_webhook() — returns { ok: bool, error: str | None }.

    Useful for validating settings before saving, without waiting for a real backup.
    The caller should set email_config["enabled"] = True before calling this,
    since send_email_notification() returns early when enabled is False.
    """
    to_addr = email_config.get("to_addr", "").strip()
    if not to_addr:
        return {"ok": False, "error": "Recipient email (to_addr) not configured"}

    test_cfg = dict(email_config)
    test_cfg["enabled"] = True  # force enabled so the send isn't skipped

    return send_email_notification(
        test_cfg,
        subject="✅ BackupSys — Test Email",
        body=(
            "BackupSys Email Test\n"
            "====================\n\n"
            "If you received this, your SMTP settings are configured correctly.\n\n"
            "You can now save your email notification settings.\n"
        ),
    )

# ─── ntfy.sh Push Notifications ───────────────────────────────────────────────
#
# ntfy_config shape (from config.json):
#     {
#         "enabled":          true/false,
#         "server":           "https://ntfy.sh",       # or self-hosted URL
#         "topic":            "my-backupsys-alerts",   # REQUIRED
#         "token":            "",                      # optional Bearer token
#         "notify_on_success": false,
#         "notify_on_failure": true,
#         "priority":         "default"               # min/low/default/high/urgent
#     }
#
# Call send_ntfy_notification(cfg, result) after each backup completes.

def send_ntfy_notification(ntfy_config: dict, title: str, message: str,
                            priority: str = "default", tags: list | None = None) -> dict:
    """
    Send a push notification via ntfy.sh (or any self-hosted ntfy server).

    Returns { ok: bool, status: int | None, error: str | None }.
    """
    import urllib.request
    import urllib.error

    server = ntfy_config.get("server", "https://ntfy.sh").rstrip("/")
    topic  = (ntfy_config.get("topic") or "").strip()
    token  = (ntfy_config.get("token") or "").strip()

    if not topic:
        return {"ok": False, "status": None, "error": "ntfy topic not configured"}

    url = f"{server}/{topic}"

    headers = {
        "Title":        title.encode("utf-8"),
        "Priority":     priority,
        "Content-Type": "text/plain; charset=utf-8",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if tags:
        headers["Tags"] = ",".join(tags)

    try:
        data = message.encode("utf-8")
        req  = urllib.request.Request(url, data=data, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=15) as resp:
            status = resp.status
            if 200 <= status < 300:
                logger.info(f"[ntfy] Notification sent → {url}  ({status})")
                return {"ok": True, "status": status, "error": None}
            err = f"HTTP {status}"
            logger.warning(f"[ntfy] POST → {url} returned {err}")
            return {"ok": False, "status": status, "error": err}
    except urllib.error.HTTPError as e:
        err = f"HTTP {e.code}: {e.reason}"
        logger.warning(f"[ntfy] {err} for {url}")
        return {"ok": False, "status": e.code, "error": err}
    except urllib.error.URLError as e:
        err = f"URL error: {e.reason}"
        logger.warning(f"[ntfy] {err} for {url}")
        return {"ok": False, "status": None, "error": err}
    except Exception as e:
        logger.warning(f"[ntfy] Unexpected error for {url}: {e}")
        return {"ok": False, "status": None, "error": str(e)}


def build_ntfy_notification(result: dict) -> tuple:
    """
    Build (title, message, priority, tags) for an ntfy push from a backup result.
    Returns values suitable for send_ntfy_notification().
    """
    status     = result.get("status", "unknown")
    watch_name = result.get("watch_name", "Unknown Watch")
    timestamp  = result.get("timestamp", "")[:19].replace("T", " ")

    if status == "success":
        title    = f"✅ Backup OK: {watch_name}"
        message  = (
            f"Files: {result.get('files_copied', 0)}  ·  "
            f"Size: {result.get('total_size', '0 B')}  ·  "
            f"Duration: {result.get('duration_s', 0):.1f}s\n"
            f"Time: {timestamp}"
        )
        priority = "default"
        tags     = ["white_check_mark", "floppy_disk"]
    elif status == "cancelled":
        title    = f"⏹ Backup cancelled: {watch_name}"
        message  = f"Backup was cancelled by the user.\nTime: {timestamp}"
        priority = "low"
        tags     = ["octagonal_sign"]
    else:
        error = result.get("error") or "Unknown error"
        title    = f"❌ Backup FAILED: {watch_name}"
        message  = f"Error: {error}\nTime: {timestamp}"
        priority = "high"
        tags     = ["x", "rotating_light"]

    return title, message, priority, tags


def dispatch_ntfy(cfg: dict, result: dict):
    """
    Convenience wrapper — reads ntfy_config from the global config dict,
    checks enabled/notify_on_* flags, and sends the notification.
    Mirrors the pattern used by _send_email_notification() in desktop_app.py.
    """
    nc = cfg.get("ntfy_config", {})
    if not nc.get("enabled", False):
        return

    success = result.get("status") == "success"
    should_notify = (
        (success     and nc.get("notify_on_success", False)) or
        (not success and nc.get("notify_on_failure", True))
    )
    if not should_notify:
        return

    title, message, priority, tags = build_ntfy_notification(result)
    # Allow config to override priority
    priority = nc.get("priority", priority)
    result_r = send_ntfy_notification(nc, title, message, priority=priority, tags=tags)
    if not result_r["ok"]:
        logger.warning(f"[ntfy] Notification failed: {result_r['error']}")


def test_ntfy(ntfy_config: dict) -> dict:
    """
    Send a test push notification to verify ntfy connectivity.
    Returns { ok, status, error }.
    """
    return send_ntfy_notification(
        ntfy_config,
        title    = "✅ BackupSys — ntfy test",
        message  = "If you see this, your ntfy push notifications are working!",
        priority = "default",
        tags     = ["white_check_mark"],
    )


# ─── Telegram Bot Notifications ───────────────────────────────────────────────
#
# telegram_config shape (from config.json):
#     {
#         "enabled":           true/false,
#         "bot_token":         "123456:ABC-DEF...",   # from @BotFather
#         "chat_id":           "-1001234567890",       # user ID or group/channel chat_id
#         "notify_on_success": false,
#         "notify_on_failure": true,
#         "parse_mode":        "HTML"                 # HTML or MarkdownV2 (optional)
#     }
#
# To obtain a bot token: message @BotFather on Telegram → /newbot.
# To find your chat_id: message @userinfobot on Telegram.

def send_telegram_notification(telegram_config: dict, text: str) -> dict:
    """
    Send a message via the Telegram Bot API (sendMessage).

    Uses only the standard library (urllib).  Supports HTML and MarkdownV2
    parse modes for rich formatting.

    Returns { ok: bool, status: int | None, error: str | None }.
    """
    import json as _json
    import urllib.request
    import urllib.error

    token     = (telegram_config.get("bot_token") or "").strip()
    chat_id   = str(telegram_config.get("chat_id") or "").strip()
    parse_mode = telegram_config.get("parse_mode", "HTML")

    if not token:
        return {"ok": False, "status": None, "error": "Telegram bot_token not configured"}
    if not chat_id:
        return {"ok": False, "status": None, "error": "Telegram chat_id not configured"}

    url     = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id":    chat_id,
        "text":       text,
        "parse_mode": parse_mode,
    }

    try:
        data = _json.dumps(payload).encode("utf-8")
        req  = urllib.request.Request(
            url, data=data,
            headers={"Content-Type": "application/json", "User-Agent": "BackupSys/2.0"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            status = resp.status
            body   = _json.loads(resp.read().decode("utf-8"))
            if body.get("ok"):
                logger.info(f"[telegram] Message sent to chat_id={chat_id}")
                return {"ok": True, "status": status, "error": None}
            err = body.get("description", f"Telegram API error (HTTP {status})")
            logger.warning(f"[telegram] API error: {err}")
            return {"ok": False, "status": status, "error": err}

    except urllib.error.HTTPError as e:
        err = f"HTTP {e.code}: {e.reason}"
        logger.warning(f"[telegram] {err}")
        return {"ok": False, "status": e.code, "error": err}
    except urllib.error.URLError as e:
        err = f"URL error: {e.reason}"
        logger.warning(f"[telegram] {err}")
        return {"ok": False, "status": None, "error": err}
    except Exception as e:
        logger.warning(f"[telegram] Unexpected error: {e}")
        return {"ok": False, "status": None, "error": str(e)}


def build_telegram_message(result: dict) -> str:
    """
    Build an HTML-formatted Telegram message from a backup result dict.
    Returns a single string suitable for send_telegram_notification().
    """
    status     = result.get("status", "unknown")
    watch_name = result.get("watch_name", "Unknown Watch")
    timestamp  = result.get("timestamp", "")[:19].replace("T", " ")

    # Escape HTML special chars in user-supplied strings
    def _esc(s: str) -> str:
        return (str(s)
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;"))

    if status == "success":
        icon  = "✅"
        lines = [
            f"{icon} <b>Backup OK</b> — {_esc(watch_name)}",
            "",
            f"📁 Files: <code>{result.get('files_copied', 0)}</code>",
            f"💾 Size: <code>{_esc(result.get('total_size', '0 B'))}</code>",
            f"⏱ Duration: <code>{result.get('duration_s', 0):.1f}s</code>",
            f"🕐 Time: <code>{_esc(timestamp)}</code>",
        ]
        failed = result.get("failed_files", [])
        if failed:
            lines.append(f"\n⚠ <b>{len(failed)} file(s) could not be copied</b>")
    elif status == "cancelled":
        icon  = "⏹"
        lines = [
            f"{icon} <b>Backup Cancelled</b> — {_esc(watch_name)}",
            f"🕐 Time: <code>{_esc(timestamp)}</code>",
        ]
    else:
        icon  = "❌"
        error = _esc(result.get("error") or "Unknown error")
        lines = [
            f"{icon} <b>Backup FAILED</b> — {_esc(watch_name)}",
            "",
            f"🔴 Error: <code>{error}</code>",
            f"🕐 Time: <code>{_esc(timestamp)}</code>",
        ]

    return "\n".join(lines)


def dispatch_telegram(cfg: dict, result: dict):
    """
    Convenience wrapper — reads telegram_config from the global config dict,
    checks enabled/notify_on_* flags, and sends the notification.
    """
    tc = cfg.get("telegram_config", {})
    if not tc.get("enabled", False):
        return

    success = result.get("status") == "success"
    should_notify = (
        (success     and tc.get("notify_on_success", False)) or
        (not success and tc.get("notify_on_failure", True))
    )
    if not should_notify:
        return

    text   = build_telegram_message(result)
    res    = send_telegram_notification(tc, text)
    if not res["ok"]:
        logger.warning(f"[telegram] Notification failed: {res['error']}")


def test_telegram(telegram_config: dict) -> dict:
    """
    Send a test message via Telegram to verify bot_token and chat_id.
    Returns { ok, status, error }.
    """
    return send_telegram_notification(
        telegram_config,
        text="✅ <b>BackupSys — Telegram test</b>\n\nIf you see this, your Telegram bot notifications are working!",
    )


# ─── Pushover Notifications ───────────────────────────────────────────────────
#
# pushover_config shape (from config.json):
#     {
#         "enabled":           true/false,
#         "user_key":          "uXXXX...",    # your Pushover user key
#         "api_token":         "aXXXX...",    # your application API token
#         "device":            "",            # optional: restrict to one device name
#         "priority":          0,             # -2 lowest … 2 emergency (int)
#         "sound":             "",            # optional: cashregister, magic, etc.
#         "notify_on_success": false,
#         "notify_on_failure": true
#     }
#
# Priority semantics (Pushover docs):
#   -2 = no notification, -1 = quiet, 0 = normal, 1 = high, 2 = emergency
# Priority 2 (emergency) requires retry + expire params — not supported here;
# use priority 1 (high) for urgent backup failure alerts instead.

def send_pushover_notification(pushover_config: dict, title: str, message: str,
                                priority: int = 0) -> dict:
    """
    POST a notification to the Pushover API (/messages.json).

    Uses only the standard library (urllib).
    Returns { ok: bool, status: int | None, error: str | None }.
    """
    import json as _json
    import urllib.request
    import urllib.error
    import urllib.parse

    user_key  = (pushover_config.get("user_key")  or "").strip()
    api_token = (pushover_config.get("api_token") or "").strip()
    device    = (pushover_config.get("device")    or "").strip()
    sound     = (pushover_config.get("sound")     or "").strip()

    # Clamp priority to valid range; avoid emergency (2) which needs extra params
    priority = max(-2, min(1, int(priority)))

    if not user_key:
        return {"ok": False, "status": None, "error": "Pushover user_key not configured"}
    if not api_token:
        return {"ok": False, "status": None, "error": "Pushover api_token not configured"}

    fields = {
        "token":    api_token,
        "user":     user_key,
        "title":    title,
        "message":  message,
        "priority": str(priority),
    }
    if device:
        fields["device"] = device
    if sound:
        fields["sound"] = sound

    url = "https://api.pushover.net/1/messages.json"

    try:
        data = urllib.parse.urlencode(fields).encode("utf-8")
        req  = urllib.request.Request(
            url, data=data,
            headers={"User-Agent": "BackupSys/2.0"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            status = resp.status
            body   = _json.loads(resp.read().decode("utf-8"))
            if body.get("status") == 1:
                logger.info(f"[pushover] Notification sent to user_key=***")
                return {"ok": True, "status": status, "error": None}
            errors = "; ".join(body.get("errors", [f"Pushover error (HTTP {status})"]))
            logger.warning(f"[pushover] API error: {errors}")
            return {"ok": False, "status": status, "error": errors}

    except urllib.error.HTTPError as e:
        err = f"HTTP {e.code}: {e.reason}"
        logger.warning(f"[pushover] {err}")
        return {"ok": False, "status": e.code, "error": err}
    except urllib.error.URLError as e:
        err = f"URL error: {e.reason}"
        logger.warning(f"[pushover] {err}")
        return {"ok": False, "status": None, "error": err}
    except Exception as e:
        logger.warning(f"[pushover] Unexpected error: {e}")
        return {"ok": False, "status": None, "error": str(e)}


def build_pushover_notification(result: dict) -> tuple:
    """
    Build (title, message, priority) for a Pushover push from a backup result dict.
    """
    status     = result.get("status", "unknown")
    watch_name = result.get("watch_name", "Unknown Watch")
    timestamp  = result.get("timestamp", "")[:19].replace("T", " ")

    if status == "success":
        title    = f"✅ Backup OK: {watch_name}"
        message  = (
            f"Files: {result.get('files_copied', 0)}  ·  "
            f"Size: {result.get('total_size', '0 B')}  ·  "
            f"Duration: {result.get('duration_s', 0):.1f}s\n"
            f"Time: {timestamp}"
        )
        priority = 0
    elif status == "cancelled":
        title    = f"⏹ Backup Cancelled: {watch_name}"
        message  = f"Backup was cancelled by the user.\nTime: {timestamp}"
        priority = -1
    else:
        error    = result.get("error") or "Unknown error"
        title    = f"❌ Backup FAILED: {watch_name}"
        message  = f"Error: {error}\nTime: {timestamp}"
        priority = 1   # high — shows above the fold on iOS/Android

    return title, message, priority


def dispatch_pushover(cfg: dict, result: dict):
    """
    Convenience wrapper — reads pushover_config from the global config dict,
    checks enabled/notify_on_* flags, and sends the notification.
    """
    pc = cfg.get("pushover_config", {})
    if not pc.get("enabled", False):
        return

    success = result.get("status") == "success"
    should_notify = (
        (success     and pc.get("notify_on_success", False)) or
        (not success and pc.get("notify_on_failure", True))
    )
    if not should_notify:
        return

    title, message, priority = build_pushover_notification(result)
    # Allow config to override priority
    priority = int(pc.get("priority", priority))
    res = send_pushover_notification(pc, title, message, priority=priority)
    if not res["ok"]:
        logger.warning(f"[pushover] Notification failed: {res['error']}")


def test_pushover(pushover_config: dict) -> dict:
    """
    Send a test notification via Pushover to verify user_key and api_token.
    Returns { ok, status, error }.
    """
    return send_pushover_notification(
        pushover_config,
        title   = "✅ BackupSys — Pushover test",
        message = "If you see this, your Pushover notifications are working!",
        priority = 0,
    )
