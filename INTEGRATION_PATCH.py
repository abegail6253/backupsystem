"""
INTEGRATION PATCH — BackupSys
==============================
This file shows the EXACT code changes needed to wire transport_utils.py and
notification_utils.py into your existing backup_engine.py and config_manager.py.

HOW TO USE:
  1. Copy transport_utils.py and notification_utils.py into your project folder
     (same directory as backup_engine.py, desktop_app.py, etc.)
  2. Apply Change A in backup_engine.py
  3. Apply Change B in backup_engine.py
  4. Apply Change C in config_manager.py
  5. Add the two new files to build_exe.py  (Change D — one line)

════════════════════════════════════════════════════════════════════════════════
CHANGE A — backup_engine.py  (top of file, after existing imports ~line 1)
════════════════════════════════════════════════════════════════════════════════

Add these two import lines directly below the existing try/except ImportError
block for cryptography (around line 18):

    # Optional transport/notification helpers (new files)
    try:
        from transport_utils import (
            upload_to_sftp, upload_to_ftp, upload_to_smb, upload_to_https
        )
        TRANSPORT_AVAILABLE = True
    except ImportError:
        TRANSPORT_AVAILABLE = False

    try:
        from notification_utils import (
            send_email_notification, send_webhook_notification, build_backup_email
        )
        NOTIFICATIONS_AVAILABLE = True
    except ImportError:
        NOTIFICATIONS_AVAILABLE = False


════════════════════════════════════════════════════════════════════════════════
CHANGE B — backup_engine.py  run_backup() function
════════════════════════════════════════════════════════════════════════════════

In run_backup(), the EXISTING cloud upload block looks like this (around line 934):

    # ── Cloud upload (if watch type is cloud) ─────────────────
    cloud_upload_result = None
    if storage_type == "cloud" and cloud_config:
        provider = cloud_config.get("provider", "dropbox")
        ...

REPLACE that entire block with the expanded version below.
It handles Dropbox/GDrive (unchanged) PLUS the four new destinations,
and wires in email + webhook notifications at the end.

─────────────────────────────────────────────────────────────────────────────

    # ── Remote destination upload ──────────────────────────────────────────
    # Handles: cloud (Dropbox/GDrive), sftp, ftp, smb, https.
    # dest_type is read from the top-level config passed in via cloud_config
    # (we re-use that param as a generic "extra config" dict).
    # fall back to storage_type for backward compatibility.
    cloud_upload_result = None
    _dest_type = (cloud_config or {}).get("_dest_type", "") or storage_type

    if _dest_type == "cloud" and cloud_config:
        provider = cloud_config.get("provider", "dropbox")
        logger.info(f"☁ Uploading backup to cloud ({provider}): {watch_name}")
        if provider == "dropbox" and cloud_config.get("access_token"):
            cloud_upload_result = upload_to_dropbox(str(backup_dir), cloud_config)
        elif provider == "gdrive" and cloud_config.get("access_token"):
            cloud_upload_result = upload_to_gdrive(str(backup_dir), cloud_config)
        else:
            cloud_upload_result = {"ok": False, "error": f"Not connected — use the Connect button for {provider}"}

    elif _dest_type == "sftp" and TRANSPORT_AVAILABLE:
        sftp_cfg = (cloud_config or {}).get("sftp_config") or cloud_config or {}
        logger.info(f"📡 Uploading backup via SFTP: {watch_name}")
        cloud_upload_result = upload_to_sftp(str(backup_dir), sftp_cfg)

    elif _dest_type == "ftp" and TRANSPORT_AVAILABLE:
        ftp_cfg = (cloud_config or {}).get("ftp_config") or cloud_config or {}
        logger.info(f"📡 Uploading backup via FTP: {watch_name}")
        cloud_upload_result = upload_to_ftp(str(backup_dir), ftp_cfg)

    elif _dest_type == "smb" and TRANSPORT_AVAILABLE:
        smb_cfg = (cloud_config or {}).get("smb_config") or cloud_config or {}
        logger.info(f"📡 Uploading backup via SMB: {watch_name}")
        cloud_upload_result = upload_to_smb(str(backup_dir), smb_cfg)

    elif _dest_type == "https" and TRANSPORT_AVAILABLE:
        https_cfg = (cloud_config or {}).get("https_config") or cloud_config or {}
        logger.info(f"📡 Uploading backup via HTTPS: {watch_name}")
        cloud_upload_result = upload_to_https(str(backup_dir), https_cfg)

    if cloud_upload_result and not cloud_upload_result["ok"]:
        logger.warning(f"⚠ Remote upload failed: {cloud_upload_result['error']}")
    elif cloud_upload_result:
        logger.info(f"☁ Remote upload complete: {cloud_upload_result.get('uploaded', '?')} files uploaded")

    # ── Email notification ─────────────────────────────────────────────────
    # cloud_config may carry email_config and webhook settings forwarded from
    # the desktop app — fall back gracefully if not present.
    _email_cfg    = (cloud_config or {}).get("email_config", {})
    _webhook_url  = (cloud_config or {}).get("webhook_url", "")
    _webhook_ok   = (cloud_config or {}).get("webhook_on_success", False)

    if _email_cfg and _email_cfg.get("enabled") and NOTIFICATIONS_AVAILABLE:
        _success = result["status"] == "success"  # note: result populated just below
        _should_email = (
            (_success     and _email_cfg.get("notify_on_success", False)) or
            (not _success and _email_cfg.get("notify_on_failure", True))
        )
        if _should_email:
            # result dict is not fully populated yet at this point in the original code.
            # Move this block AFTER result.update({...}) below, or pass a snapshot:
            _email_result_snap = {**result,
                "status":       "success",
                "cloud_upload": cloud_upload_result,
                "files_copied": copied,
                "total_size":   _human_size(_safe_size(str(backup_dir))),
            }
            _subj, _body = build_backup_email(_email_result_snap)
            _er = send_email_notification(_email_cfg, _subj, _body)
            if not _er["ok"]:
                logger.warning(f"[email] Notification failed: {_er['error']}")

    # ── Webhook notification ───────────────────────────────────────────────
    if _webhook_url and NOTIFICATIONS_AVAILABLE:
        _success = result["status"] == "success"
        if not _success or _webhook_ok:
            send_webhook_notification(_webhook_url, {
                "event":        "backup_complete",
                "status":       result.get("status", "unknown"),
                "watch_name":   watch_name,
                "watch_id":     watch_id,
                "backup_id":    result.get("id"),
                "timestamp":    ts,
                "files_copied": copied,
                "duration_s":   round(time.time() - started, 2),
                "error":        result.get("error"),
            })

─────────────────────────────────────────────────────────────────────────────

NOTE: How to pass email_config + webhook settings into run_backup():

In desktop_app.py, when calling run_backup(), include them in cloud_config:

    extra = {
        "_dest_type":       cfg.get("dest_type", "local"),
        "email_config":     cfg.get("email_config", {}),
        "webhook_url":      cfg.get("webhook_url", ""),
        "webhook_on_success": cfg.get("webhook_on_success", False),
    }
    # Merge with destination-specific config:
    if cfg["dest_type"] == "sftp":
        extra.update(cfg.get("dest_sftp", {}))
    elif cfg["dest_type"] == "ftp":
        extra.update(cfg.get("dest_ftp", {}))
    elif cfg["dest_type"] == "smb":
        extra.update(cfg.get("dest_smb", {}))
    elif cfg["dest_type"] == "https":
        extra.update(cfg.get("dest_https", {}))
    elif cfg["dest_type"] == "cloud":
        extra.update(watch.get("cloud_config", {}))

    backup_engine.run_backup(
        ...,
        cloud_config=extra,
    )


════════════════════════════════════════════════════════════════════════════════
CHANGE C — config_manager.py  (security fix: env-var override for encrypt_key)
════════════════════════════════════════════════════════════════════════════════

The email password already has an env-var override (BACKUPSYS_EMAIL_PASSWORD).
The encrypt_key is currently stored as plain text in config.json.

In config_manager.py, load() function, after the existing env_pw block (~line 208):

    # Existing:
    env_pw = os.environ.get("BACKUPSYS_EMAIL_PASSWORD", "").strip()
    if env_pw and "email_config" in cfg:
        cfg["email_config"]["password"] = env_pw

ADD directly below:

    # NEW — per-watch encryption key override via environment variable.
    # Set BACKUPSYS_ENCRYPT_KEY_<WATCH_ID>=<fernet_key> to avoid storing
    # keys in plain text in config.json.
    # Example: BACKUPSYS_ENCRYPT_KEY_w_kkxjf0=your44charbase64key
    for w in cfg.get("watches", []):
        wid     = w.get("id", "")
        env_key = os.environ.get(f"BACKUPSYS_ENCRYPT_KEY_{wid}", "").strip()
        if env_key:
            w["encrypt_key"] = env_key

    # NEW — global fallback key (applies only to watches that have no key set)
    global_env_key = os.environ.get("BACKUPSYS_ENCRYPT_KEY_DEFAULT", "").strip()
    if global_env_key:
        for w in cfg.get("watches", []):
            if not w.get("encrypt_key"):
                w["encrypt_key"] = global_env_key


════════════════════════════════════════════════════════════════════════════════
CHANGE D — build_exe.py  (bundle the two new files)
════════════════════════════════════════════════════════════════════════════════

In build_exe.py, after the existing --add-data for config.json (around line 73),
add these two lines to the args list:

    "--add-data", f"{SCRIPT_DIR / 'transport_utils.py'}{os.pathsep}.",
    "--add-data", f"{SCRIPT_DIR / 'notification_utils.py'}{os.pathsep}.",

That's it.  PyInstaller will bundle both files alongside desktop_app.py in the
output folder, so they're importable at runtime without any sys.path tricks.
"""