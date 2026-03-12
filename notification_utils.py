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