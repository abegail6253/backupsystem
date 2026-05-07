"""
backupsys_cli.py — Headless / scriptable CLI for BackupSys
===========================================================

Run backups, list watches, show history, and validate backup integrity
without a display, GUI, or running Qt application.

Designed for:
  • Headless servers / WSL / SSH sessions
  • Windows Task Scheduler ("at 02:00 backup everything")
  • CI pipelines that want to verify a backup finished correctly
  • Power users who prefer the terminal

Usage examples
--------------
List all configured watches:
    python backupsys_cli.py list

Back up a specific watch by name or ID:
    python backupsys_cli.py backup --watch "My Documents"
    python backupsys_cli.py backup --watch w_abc123

Back up ALL active watches (same as auto-backup):
    python backupsys_cli.py backup --all

Back up and exit with code 1 if any watch fails:
    python backupsys_cli.py backup --all --strict

Show the last N backup results:
    python backupsys_cli.py history --limit 20

Validate the most-recent backup for a watch:
    python backupsys_cli.py validate --watch "My Documents"

Validate ALL watches' latest backups:
    python backupsys_cli.py validate --all

Generate a fresh encryption key:
    python backupsys_cli.py keygen

Print the active config (without secrets):
    python backupsys_cli.py config

Dry-run — preview what would change without copying anything:
    python backupsys_cli.py dry-run --watch "My Documents"
    python backupsys_cli.py dry-run --all --verbose

Environment variables
---------------------
All the usual BackupSys env vars apply:
    BACKUPSYS_DATA_DIR, BACKUPSYS_PORTABLE, BACKUPSYS_EMAIL_PASSWORD,
    BACKUPSYS_ENCRYPT_KEY_<WATCH_ID>, BACKUPSYS_ENCRYPT_KEY_DEFAULT
"""

import argparse
import json
import os
import sys
import time
import threading
from datetime import datetime
from pathlib import Path

# ── Bootstrap: find project root ──────────────────────────────────────────────
_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

# Load .env the same way desktop_app.py does
def _load_dotenv():
    for _p in [_HERE / ".env", _HERE / "_env"]:
        if _p.exists():
            try:
                from dotenv import load_dotenv
                load_dotenv(dotenv_path=_p, override=False)
            except ImportError:
                for line in _p.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, _, v = line.partition("=")
                    k = k.strip(); v = v.strip().strip("\"'")
                    if k and k not in os.environ:
                        os.environ[k] = v
            break
_load_dotenv()

import config_manager
import backup_engine

# ── Colour helpers ────────────────────────────────────────────────────────────
_COLOUR = sys.stdout.isatty()

def _c(text, code):
    return f"\033[{code}m{text}\033[0m" if _COLOUR else text

def ok(msg):    print(_c(f"  ✅  {msg}", "32"))
def err(msg):   print(_c(f"  ❌  {msg}", "31"), file=sys.stderr)
def warn(msg):  print(_c(f"  ⚠   {msg}", "33"))
def info(msg):  print(f"  {msg}")
def head(msg):  print(_c(f"\n{'─'*60}\n  {msg}\n{'─'*60}", "1"))


# ── Helpers ───────────────────────────────────────────────────────────────────

def _human(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"

def _resolve_watch(cfg: dict, spec: str):
    """Return a single watch dict matching name or ID, or None."""
    spec_l = spec.strip().lower()
    for w in cfg.get("watches", []):
        if w["id"].lower() == spec_l or w["name"].lower() == spec_l:
            return w
    return None

def _latest_backup_dir(destination: str, watch_name: str) -> str | None:
    """Return the path of the most-recent versioned backup dir for a watch."""
    dest = Path(destination)
    safe = "".join(c if c.isalnum() else "_" for c in watch_name)
    candidates = sorted(dest.glob(f"*__{safe}"), reverse=True)
    if candidates:
        return str(candidates[0])
    # Fallback: any dir matching YYYYMMDD_HHMMSS pattern
    candidates = sorted(dest.glob("20[0-9][0-9][0-9][0-9][0-9][0-9]_*"), reverse=True)
    return str(candidates[0]) if candidates else None


# ── Sub-commands ──────────────────────────────────────────────────────────────

def cmd_list(args, cfg):
    head("Configured watches")
    watches = cfg.get("watches", [])
    if not watches:
        warn("No watches configured. Open BackupSys or edit config.json to add one.")
        return 0
    fmt = "  {:<12}  {:<25}  {:<10}  {:<20}  {}"
    print(_c(fmt.format("ID", "Name", "Status", "Last backup", "Path"), "1"))
    print("  " + "─" * 90)
    for w in watches:
        status = "paused" if w.get("paused") else ("active" if w.get("active", True) else "inactive")
        lb = (w.get("last_backup") or "never")[:19].replace("T", " ")
        print(fmt.format(w["id"], w["name"][:24], status, lb, w["path"]))
    info(f"\n  {len(watches)} watch(es) total.")
    return 0


def cmd_config(args, cfg):
    head("Active configuration (secrets redacted)")
    safe = dict(cfg)
    for key in ("dest_sftp", "dest_ftp", "dest_smb", "dest_https", "dest_webdav"):
        if key in safe and isinstance(safe[key], dict):
            safe[key] = {k: ("***" if "pass" in k.lower() or "token" in k.lower() else v)
                         for k, v in safe[key].items()}
    if "email_config" in safe:
        ec = dict(safe["email_config"])
        ec["password"] = "***" if ec.get("password") else ""
        safe["email_config"] = ec
    for w in safe.get("watches", []):
        if w.get("encrypt_key"):
            w["encrypt_key"] = "***"
    print(json.dumps(safe, indent=2, default=str))
    return 0


def cmd_keygen(args, cfg):
    import backup_engine
    head("Generate encryption key")
    try:
        key = backup_engine.generate_encryption_key()
        ok(f"New key (copy this to your .env or Settings → Edit Watch):")
        print(f"\n    {key}\n")
        info("Store it in .env as: BACKUPSYS_ENCRYPT_KEY_<WATCH_ID>=<key>")
        info("Or set it per-watch in Settings → Edit Watch → Encryption Key.")
    except RuntimeError as e:
        err(str(e))
        return 1
    return 0


def cmd_rotate_key(args, cfg):
    """Rotate the encryption key for all .enc files in a backup directory."""
    import getpass as _getpass

    head("Rotate encryption key")

    backup_dir = args.backup_dir
    if not backup_dir:
        err("--backup-dir is required.")
        return 1

    from pathlib import Path as _Path
    bd = _Path(backup_dir)
    if not bd.is_dir():
        err(f"Directory not found: {backup_dir}")
        return 1

    if args.old_key:
        old_key = args.old_key
    else:
        old_key = _getpass.getpass("  Old encryption key: ")

    if args.new_key:
        new_key = args.new_key
    else:
        new_key = _getpass.getpass("  New encryption key: ")
        new_key2 = _getpass.getpass("  Confirm new key:    ")
        if new_key != new_key2:
            err("New keys do not match — aborting.")
            return 1

    if not old_key.strip() or not new_key.strip():
        err("Keys must not be blank — aborting.")
        return 1

    info(f"Rotating keys in: {backup_dir}")
    warn("This will re-encrypt every .enc file in place.  Make sure you have a backup before proceeding.")
    if not args.yes:
        ans = input("  Continue? [y/N] ").strip().lower()
        if ans != "y":
            info("Aborted.")
            return 0

    def _progress(rel_path, idx, total):
        print(f"  [{idx}/{total}] {rel_path}", end="\r", flush=True)

    result = backup_engine.rotate_encryption_key(
        backup_dir, old_key, new_key, progress_cb=_progress
    )
    print()  # newline after \r progress

    if result["ok"]:
        ok(f"Key rotation complete — {result['files_rotated']} file(s) re-encrypted.")
        if result.get("errors"):
            warn(f"{len(result['errors'])} file(s) could not be re-encrypted:")
            for e_msg in result["errors"]:
                err(f"  {e_msg}")
    else:
        err("Key rotation failed:")
        for e_msg in result.get("errors", ["Unknown error"]):
            err(f"  {e_msg}")
        return 1

    return 0


def cmd_history(args, cfg):
    head("Backup history")
    entries = config_manager.load_history()
    if not entries:
        warn("No history entries found.")
        return 0
    limit = args.limit or 20
    shown = entries[-limit:][::-1]
    fmt = "  {:<20}  {:<25}  {:<10}  {:<8}  {}"
    print(_c(fmt.format("Timestamp", "Watch", "Status", "Files", "Size"), "1"))
    print("  " + "─" * 80)
    for e in shown:
        ts  = (e.get("timestamp") or "")[:19].replace("T", " ")
        wn  = (e.get("watch_name") or "")[:24]
        st  = e.get("status", "?")
        fc  = str(e.get("files_copied", "?"))
        sz  = e.get("total_size", "?")
        colour = "32" if st == "success" else ("33" if st == "cancelled" else "31")
        print(_c(fmt.format(ts, wn, st, fc, sz), colour))
    info(f"\n  Showing {len(shown)} of {len(entries)} entries. Use --limit N for more.")
    return 0


def cmd_validate(args, cfg):
    import backup_engine
    head("Validate backup integrity")
    dest = cfg.get("destination", "")
    watches = cfg.get("watches", [])

    if args.all:
        targets = [w for w in watches if w.get("active", True) and not w.get("paused")]
    elif args.watch:
        w = _resolve_watch(cfg, args.watch)
        if not w:
            err(f"Watch not found: {args.watch}")
            return 1
        targets = [w]
    else:
        err("Specify --watch <name|id> or --all")
        return 1

    any_failed = False
    for w in targets:
        w_dest = w.get("destination", "").strip() or dest
        bd = _latest_backup_dir(w_dest, w["name"])
        if not bd:
            warn(f"{w['name']}: no backup found at {w_dest}")
            any_failed = True
            continue
        info(f"Validating {w['name']}  →  {Path(bd).name} …")
        result = backup_engine.validate_backup(bd)
        if result.get("valid") and result.get("manifest_ok", True):
            ok(f"{w['name']}: integrity OK  ({result.get('files_checked', 0)} files checked)")
        else:
            err(f"{w['name']}: integrity FAILED")
            for f in result.get("missing_files", [])[:5]:
                err(f"  Missing: {f}")
            for f in result.get("corrupted_files", [])[:5]:
                err(f"  Corrupted: {f}")
            if result.get("error"):
                err(f"  Error: {result['error']}")
            any_failed = True

    return 1 if any_failed else 0


def cmd_restore(args, cfg):
    """Restore a backup from the CLI — latest or a specific backup ID."""
    import backup_engine

    watch = args.watch
    target = args.target.strip() if args.target else None
    backup_id = args.backup_id.strip() if getattr(args, "backup_id", None) else None
    full_chain = getattr(args, "full_chain", False)
    overwrite = not getattr(args, "no_overwrite", False)

    if not target:
        err("--target <destination-folder> is required.")
        return 1

    watches = cfg.get("watches", [])
    w = _resolve_watch(cfg, watch)
    if not w:
        err(f"Watch not found: '{watch}'")
        info("Available watches:")
        for ww in watches:
            info(f"  {ww['id']}  {ww['name']}")
        return 1

    dest_global = cfg.get("destination", "")
    w_dest = w.get("destination", "").strip() or dest_global
    encrypt_key = w.get("encrypt_key") or None
    dest_type   = cfg.get("dest_type", "local")

    head(f"Restore — {w['name']}")

    # ── Remote destinations: download backup index to a temp dir first ────────
    # (mirrors the GUI's download-then-restore flow; backup_engine.list_backups
    # and restore_backup only work on local paths)
    _REMOTE_TYPES = {"sftp", "ftps", "ftp", "smb", "webdav", "https", "rclone"}
    temp_dir = None

    if dest_type in _REMOTE_TYPES:
        import tempfile as _tmpmod
        import shutil   as _shutil
        try:
            import transport_utils as _tu
        except ImportError:
            err("transport_utils module not found — cannot restore from a remote destination.")
            return 1

        temp_dir = _tmpmod.mkdtemp(prefix="backupsys_restore_")
        info(f"Downloading backups from {dest_type.upper()} \u2192 {temp_dir} \u2026")
        try:
            if dest_type in ("sftp", "ftps"):
                dl = _tu.download_from_sftp(
                    w_dest, temp_dir, cfg.get("dest_sftp", {}),
                    progress_cb=lambda n, fname: print(
                        f"\r    \u2193 {n} file(s)  {fname[:50]:<50}", end="", flush=True),
                )
            elif dest_type == "ftp":
                dl = _tu.download_from_ftp(
                    w_dest, temp_dir, cfg.get("dest_ftp", {}),
                    progress_cb=lambda n, fname: print(
                        f"\r    \u2193 {n} file(s)  {fname[:50]:<50}", end="", flush=True),
                )
            elif dest_type == "smb":
                dl = _tu.download_from_smb(
                    w_dest, temp_dir, cfg.get("dest_smb", {}),
                    progress_cb=lambda n, fname: print(
                        f"\r    \u2193 {n} file(s)  {fname[:50]:<50}", end="", flush=True),
                )
            elif dest_type == "webdav":
                dl = _tu.download_from_webdav(
                    w_dest, temp_dir, cfg.get("dest_webdav", {}),
                    progress_cb=lambda n, fname: print(
                        f"\r    \u2193 {n} file(s)  {fname[:50]:<50}", end="", flush=True),
                )
            elif dest_type == "https":
                dl = _tu.download_from_https(
                    w_dest, temp_dir, cfg.get("dest_https", {}),
                    progress_cb=lambda n, fname: print(
                        f"\r    \u2193 {n} file(s)  {fname[:50]:<50}", end="", flush=True),
                )
            elif dest_type == "rclone":
                dl = _tu.download_from_rclone(
                    w_dest, temp_dir, cfg.get("dest_rclone", {}))
            else:
                dl = {"status": "error", "error": f"Unsupported remote type: {dest_type}"}

            print()  # newline after inline progress
            if dl.get("status") != "ok":
                err(f"Download from {dest_type.upper()} failed: "
                    f"{dl.get('error', 'unknown error')}")
                _shutil.rmtree(temp_dir, ignore_errors=True)
                return 1

            info(f"Download complete ({dl.get('downloaded', '?')} file(s)).")
            w_dest = temp_dir   # point list_backups / restore at the local copy

        except Exception as exc:
            print()
            err(f"Download from {dest_type.upper()} failed: {exc}")
            _shutil.rmtree(temp_dir, ignore_errors=True)
            return 1

    # ── List backups (local path, or temp dir for remote) ─────────────────────
    try:
        backup_list = backup_engine.list_backups(w_dest, watch_id=w["id"])
    except Exception as e:
        err(f"Could not list backups: {e}")
        if temp_dir:
            import shutil; shutil.rmtree(temp_dir, ignore_errors=True)
        return 1

    if not backup_list:
        err(f"No backups found for watch '{w['name']}' at: {w_dest}")
        if temp_dir:
            import shutil; shutil.rmtree(temp_dir, ignore_errors=True)
        return 1

    # Resolve which backup to restore
    if backup_id:
        match = next((b for b in backup_list if b.get("id") == backup_id or
                      b.get("dir", "").endswith(backup_id)), None)
        if not match:
            err(f"Backup ID '{backup_id}' not found. Available backups:")
            for b in backup_list[-10:]:
                info(f"  {b.get('id', '?')}  {b.get('timestamp', '?')}  "
                     f"{b.get('size_human', '?')}  {b.get('status', '?')}")
            return 1
        chosen = match
    else:
        # Use most recent successful backup
        chosen = next((b for b in reversed(backup_list)
                       if b.get("status") == "success"), backup_list[-1])

    backup_dir = chosen.get("dir") or chosen.get("path", "")
    if not backup_dir:
        err("Could not determine backup directory path.")
        if temp_dir:
            import shutil; shutil.rmtree(temp_dir, ignore_errors=True)
        return 1

    info(f"Backup:  {chosen.get('timestamp', '?')}  "
         f"({chosen.get('size_human', '?')})  [{chosen.get('id', '?')}]")
    info(f"Restoring to: {target}")
    if full_chain:
        info("Mode: full incremental chain")
    else:
        info("Mode: single snapshot")
    info("(overwrite existing files)" if overwrite else "(skip existing files)")

    def _progress(copied, total, fname, **_):
        if total > 0 and copied % max(1, total // 10) == 0:
            pct = int(copied / total * 100)
            bar = ("\u2588" * (pct // 5)).ljust(20)
            print(f"\r    [{bar}] {pct:3d}%  {fname[:40]:<40}", end="", flush=True)

    try:
        try:
            if full_chain:
                result = backup_engine.restore_full_chain(
                    destination=w_dest,
                    watch_id=w["id"],
                    target_path=target,
                    up_to_backup_id=chosen.get("id"),
                    encrypt_key=encrypt_key,
                    overwrite=overwrite,
                    progress_cb=_progress,
                )
            else:
                result = backup_engine.restore_backup(
                    backup_dir=backup_dir,
                    target_path=target,
                    encrypt_key=encrypt_key,
                    overwrite=overwrite,
                    progress_cb=_progress,
                )
        except Exception as e:
            print()
            err(f"Restore failed: {e}")
            return 1

        print()  # newline after progress bar

        if result.get("ok") or result.get("status") == "success":
            ok(f"Restored {result.get('files_restored', result.get('restored', '?'))} "
               f"file(s) to: {target}")
            if result.get("skipped"):
                info(f"Skipped: {result['skipped']} file(s) (already up to date)")
            return 0
        else:
            err(f"Restore failed: {result.get('error', 'unknown error')}")
            return 1
    finally:
        # Always remove the temp download dir, whether we succeeded or not
        if temp_dir:
            import shutil as _sh
            _sh.rmtree(temp_dir, ignore_errors=True)


def cmd_dryrun(args, cfg):
    head("Dry run — preview changes without copying anything")

    watches = cfg.get("watches", [])
    if args.all:
        targets = [w for w in watches
                   if w.get("active", True)
                   and not w.get("paused")
                   and not w.get("skip_auto_backup")]
        if not targets:
            warn("No active watches to preview.")
            return 0
    elif args.watch:
        w = _resolve_watch(cfg, args.watch)
        if not w:
            err(f"Watch not found: '{args.watch}'")
            return 1
        targets = [w]
    else:
        err("Specify --watch <name|id> or --all")
        return 1

    dest_global = cfg.get("destination", "")
    dest_type   = cfg.get("dest_type", "local")

    for w in targets:
        w_dest   = w.get("destination", "").strip() or dest_global
        snapshot = config_manager.load_snapshot(w["id"], dest_type)

        info(f"\nPreviewing: {w['name']}  ({w['path']})")
        result = backup_engine.run_backup(
            source            = w["path"],
            destination       = w_dest,
            watch_id          = w["id"],
            watch_name        = w["name"],
            storage_type      = dest_type,
            previous_snapshot = snapshot or None,
            incremental       = bool(snapshot),
            exclude_patterns  = w.get("exclude_patterns", []),
            max_file_size_mb  = w.get("max_file_size_mb", 0),
            dry_run           = True,
        )

        changes = result.get("changes", [])
        added    = [c for c in changes if c["type"] == "added"]
        modified = [c for c in changes if c["type"] == "modified"]
        deleted  = [c for c in changes if c["type"] == "deleted"]

        ok(f"{w['name']}: {result.get('files_to_copy', 0)} file(s) would be copied  "
           f"({result.get('total_size', '0 B')})")
        if added:
            info(f"  + {len(added)} new file(s)")
        if modified:
            info(f"  ~ {len(modified)} modified file(s)")
        if deleted:
            info(f"  - {len(deleted)} deleted file(s) (marker only)")

        if args.verbose:
            for c in sorted(changes, key=lambda x: x.get("path", "")):
                sym = {"added": "+", "modified": "~", "deleted": "-"}.get(c["type"], "?")
                sz  = _human(c.get("size", 0))
                print(_c(f"    {sym} {c['path']:<60}  {sz}", "36"))

    return 0


def _dispatch_notifications(cfg: dict, w: dict, result: dict) -> None:
    """Send all configured notifications after a backup run.

    Mirrors the notification block inside BackupWorker (desktop_app.py) so
    that headless / scheduled CLI backups produce the same alerts as the GUI.
    Per-watch ``notify_overrides`` (webhook_url, ntfy_topic) are honoured.
    """
    # Build effective config with per-watch notification overrides applied.
    _notify_ov = w.get("notify_overrides", {})
    eff_cfg = dict(cfg)
    if _notify_ov.get("webhook_url"):
        eff_cfg = {**eff_cfg, "webhook_url": _notify_ov["webhook_url"]}
    if _notify_ov.get("ntfy_topic"):
        _nc = dict(eff_cfg.get("ntfy_config", {}))
        _nc["topic"] = _notify_ov["ntfy_topic"]
        eff_cfg = {**eff_cfg, "ntfy_config": _nc}

    status = result.get("status", "failed")
    is_success = status == "success"

    try:
        from notification_utils import (
            build_backup_email,
            send_email_notification,
            send_webhook_notification,
            dispatch_ntfy,
            dispatch_telegram,
            dispatch_pushover,
        )
    except ImportError:
        return  # notification_utils not installed — skip silently

    r_with_name = {**result, "watch_name": w["name"]}

    # ── Email ─────────────────────────────────────────────────────────────────
    ec = eff_cfg.get("email_config", {})
    _email_flag = "notify_on_success" if is_success else "notify_on_failure"
    if ec.get("enabled") and ec.get(_email_flag, not is_success):
        try:
            subject, body = build_backup_email(r_with_name)
        except Exception:
            emoji = "✅" if is_success else "⚠"
            subject = f"{emoji} Backup {'complete' if is_success else 'failed'}: {w['name']}"
            body = (
                f"Watch:    {w['name']}\n"
                f"Source:   {w['path']}\n"
                f"Status:   {status}\n"
                + (f"Error:    {result.get('error', '')}\n" if not is_success else
                   f"Files:    {result.get('files_copied', 0)}\n"
                   f"Size:     {result.get('total_size', '?')}\n")
                + f"Triggered by: cli\n"
            )
        res = send_email_notification(ec, subject, body)
        if res and not res.get("ok"):
            warn(f"  Email notification failed: {res.get('error')}")

    # ── Webhook ───────────────────────────────────────────────────────────────
    url = eff_cfg.get("webhook_url", "").strip()
    if url:
        if is_success and not eff_cfg.get("webhook_on_success", False):
            pass  # webhook_on_success not set — skip success pings
        else:
            try:
                send_webhook_notification(url, {
                    "status": status,
                    "watch": w["name"],
                    "files_copied": result.get("files_copied", 0),
                    "total_size": result.get("total_size", "?"),
                    "error": result.get("error", ""),
                    "timestamp": result.get("timestamp", ""),
                    "triggered_by": "cli",
                })
            except Exception as exc:
                warn(f"  Webhook notification failed: {exc}")

    # ── ntfy ──────────────────────────────────────────────────────────────────
    try:
        dispatch_ntfy(eff_cfg, r_with_name)
    except Exception as exc:
        warn(f"  ntfy notification failed: {exc}")

    # ── Telegram + Pushover ───────────────────────────────────────────────────
    try:
        dispatch_telegram(eff_cfg, r_with_name)
    except Exception as exc:
        warn(f"  Telegram notification failed: {exc}")
    try:
        dispatch_pushover(eff_cfg, r_with_name)
    except Exception as exc:
        warn(f"  Pushover notification failed: {exc}")


def cmd_backup(args, cfg):
    head("Running backup")

    watches = cfg.get("watches", [])
    if args.all:
        targets = [w for w in watches
                   if w.get("active", True)
                   and not w.get("paused")
                   and not w.get("skip_auto_backup")]
        if not targets:
            warn("No active watches to back up.")
            return 0
    elif args.watch:
        w = _resolve_watch(cfg, args.watch)
        if not w:
            err(f"Watch not found: '{args.watch}'")
            info("Available watches:")
            for ww in watches:
                info(f"  {ww['id']}  {ww['name']}")
            return 1
        targets = [w]
    else:
        err("Specify --watch <name|id> or --all")
        return 1

    dest_global  = cfg.get("destination", "")
    dest_type    = cfg.get("dest_type", "local")
    any_failed   = False
    cancel_event = threading.Event()

    # Handle Ctrl-C gracefully
    import signal
    def _sigint(sig, frame):
        warn("\nInterrupt received — cancelling …")
        cancel_event.set()
    signal.signal(signal.SIGINT, _sigint)

    for w in targets:
        if cancel_event.is_set():
            break

        w_dest      = w.get("destination", "").strip() or dest_global
        snapshot    = config_manager.load_snapshot(w["id"], dest_type)
        encrypt_key = w.get("encrypt_key") or None
        compress    = w.get("compression", False)

        # Build gdrive_config for non-local destinations
        _cloud_cfg = None
        if dest_type == "sftp":
            _cloud_cfg = {**cfg.get("dest_sftp", {}), "_dest_type": "sftp"}
        elif dest_type in ("ftp", "ftps"):
            _cloud_cfg = {**cfg.get("dest_ftp", {}), "_dest_type": dest_type}
        elif dest_type == "https":
            _cloud_cfg = {**cfg.get("dest_https", {}), "_dest_type": "https"}
        elif dest_type == "webdav":
            _cloud_cfg = {**cfg.get("dest_webdav", {}), "_dest_type": "webdav"}
        elif dest_type == "smb":
            # FIX #3: SMB was missing from this branch  uploads were silently skipped.
            _cloud_cfg = {**cfg.get("dest_smb", {}), "_dest_type": "smb"}
        elif dest_type == "gdrive":
            # FIX #4: accept "gdrive" as a legacy alias for "cloud" so configs
            # saved with dest_type:"gdrive" are not silently ignored by the CLI.
            _wcc = w.get("cloud_config") or {}
            if _wcc:
                _cloud_cfg = {**_wcc, "_dest_type": "gdrive"}
        elif dest_type == "rclone":
            _cloud_cfg = {**cfg.get("dest_rclone", {}), "_dest_type": "rclone"}

        # FIX #1: Mirror the desktop_app.py per-watch cloud_config fallback.
        # When the global dest_type is "local" but a watch has GDrive assigned
        # (cloud_config with an access_token), the backup must still upload to
        # GDrive.  Previously the CLI never applied this fallback, so GDrive
        # assignments were silently ignored unless the user also changed the
        # global dest_type to "gdrive".
        if _cloud_cfg is None:
            _w_cloud = w.get("cloud_config") or {}
            if _w_cloud and _w_cloud.get("access_token"):
                _cloud_cfg = {**_w_cloud, "_dest_type": "gdrive"}

        # Progress display
        _last_pct = [-1]
        def _progress(copied, total, fname, bytes_done=0, total_bytes=0):
            if total_bytes > 0:
                pct = int(bytes_done / total_bytes * 100)
            else:
                pct = int(copied / max(total, 1) * 100)
            if pct != _last_pct[0] and pct % 10 == 0:
                _last_pct[0] = pct
                bar = ("█" * (pct // 5)).ljust(20)
                print(f"\r    [{bar}] {pct:3d}%  {fname[:40]:<40}", end="", flush=True)

        def _scan(path):
            print(f"\r    Scanning … {Path(path).name[:60]:<60}", end="", flush=True)

        t0 = time.time()
        info(f"Backing up: {w['name']}  ({w['path']})")
        try:
            result = backup_engine.run_backup(
                source            = w["path"],
                destination       = w_dest,
                watch_id          = w["id"],
                watch_name        = w["name"],
                storage_type      = dest_type,
                previous_snapshot = snapshot or None,
                incremental       = bool(snapshot),
                progress_cb       = _progress,
                scan_cb           = _scan,
                exclude_patterns  = w.get("exclude_patterns", []),
                compress          = compress,
                encrypt_key       = encrypt_key,
                cloud_config      = _cloud_cfg,
                triggered_by      = "cli",
                cancel_event      = cancel_event,
                sync_mode         = w.get("sync_mode", False),
            )
        except Exception as e:
            result = {"status": "failed", "error": str(e)}

        print()  # newline after progress bar

        status = result.get("status", "failed")
        dur    = round(time.time() - t0, 1)

        if status == "success":
            ok(
                f"{w['name']}: {result.get('files_copied', 0)} file(s) · "
                f"{result.get('total_size', '?')} · {dur}s"
            )
            config_manager.update_watch_snapshot(
                cfg, w["id"],
                result.get("snapshot", {}),
                result.get("timestamp", datetime.now().isoformat()),
                result.get("total_size_bytes", 0),
                dest_type=dest_type,
            )
            if result.get("cloud_upload") and not result["cloud_upload"].get("ok"):
                warn(f"  Remote upload failed: {result['cloud_upload'].get('error')}")
        elif status == "cancelled":
            warn(f"{w['name']}: cancelled after {dur}s")
        else:
            err(f"{w['name']}: FAILED — {result.get('error', 'unknown error')}")
            any_failed = True

        if result.get("failed_files"):
            warn(f"  {len(result['failed_files'])} file(s) could not be copied:")
            for ff in result["failed_files"][:5]:
                warn(f"    {ff.get('path')}: {ff.get('reason')}")

        # ── Send notifications (mirrors BackupWorker in the GUI) ──────────────
        _dispatch_notifications(cfg, w, result)

    if any_failed and args.strict:
        return 1
    return 0


# ── Feature 3: Backup catalog / cross-watch file search ──────────────────────

def cmd_search(args, cfg):
    """
    Search for files across all backup manifests (all watches, all snapshots).

    Supports fnmatch glob patterns:
        backupsys_cli.py search --file "*.docx"
        backupsys_cli.py search --file "report*" --watch "My Documents"
        backupsys_cli.py search --file "budget.xlsx" --limit 50 --verbose
    """
    import fnmatch

    pattern   = (args.file or "").strip()
    filter_id = None
    limit     = args.limit or 200
    verbose   = getattr(args, "verbose", False)

    if not pattern:
        err("--file PATTERN is required  (e.g. --file '*.docx')")
        return 1

    # Resolve optional --watch filter to a watch id
    if args.watch:
        w = _resolve_watch(cfg, args.watch)
        if not w:
            err(f"Watch not found: '{args.watch}'")
            return 1
        filter_id = w["id"]

    global_dest = cfg.get("destination", "")
    dest_set: set = set()
    if global_dest:
        dest_set.add(global_dest)
    watch_names: dict = {}
    for w in cfg.get("watches", []):
        watch_names[w["id"]] = w.get("name", w["id"])
        wd = w.get("destination", "").strip()
        if wd:
            dest_set.add(wd)

    if not dest_set:
        err("No backup destination configured. Run a backup first.")
        return 1

    head(f"File search: {pattern!r}")

    matches   = 0
    scanned   = 0
    errors    = 0
    results   = []   # list of dicts for final display

    for dest in sorted(dest_set):
        dest_path = Path(dest)
        if not dest_path.exists():
            warn(f"Destination not accessible: {dest}")
            continue

        try:
            for backup_dir in sorted(dest_path.iterdir(), reverse=True):
                if not backup_dir.is_dir():
                    continue
                manifest_p = backup_dir / "MANIFEST.json"
                if not manifest_p.exists():
                    continue
                scanned += 1

                try:
                    with open(manifest_p, "r", encoding="utf-8") as f:
                        manifest = json.load(f)
                except Exception as exc:
                    errors += 1
                    logger.debug(f"Could not read manifest {manifest_p}: {exc}")
                    continue

                wid = manifest.get("watch_id", "")
                if filter_id and wid != filter_id:
                    continue

                watch_label = watch_names.get(wid, wid or backup_dir.name)
                ts_raw = manifest.get("timestamp", "")
                try:
                    ts_display = datetime.fromisoformat(ts_raw).strftime("%Y-%m-%d %H:%M")
                except Exception:
                    ts_display = ts_raw or "?"

                for entry in manifest.get("changes", []):
                    if entry.get("type") not in ("added", "modified"):
                        continue
                    rel_path = entry.get("path", "")
                    fname    = Path(rel_path).name
                    # Match against filename alone OR full relative path
                    if not (fnmatch.fnmatch(fname.lower(), pattern.lower())
                            or fnmatch.fnmatch(rel_path.lower(), pattern.lower())):
                        continue

                    size_b = entry.get("size", 0)
                    results.append({
                        "watch":    watch_label,
                        "ts":       ts_display,
                        "path":     rel_path,
                        "size":     size_b,
                        "size_h":   _human(size_b),
                        "bak_dir":  str(backup_dir),
                    })
                    matches += 1
                    if matches >= limit:
                        break

                if matches >= limit:
                    break
        except Exception as exc:
            errors += 1
            warn(f"Error scanning {dest}: {exc}")

    # ── Print results ─────────────────────────────────────────────────────────
    if not results:
        warn(f"No files matching {pattern!r} found.")
        info(f"  ({scanned} snapshot(s) scanned)")
        return 0

    fmt = "  {:<25}  {:<17}  {:<8}  {}"
    print(_c(fmt.format("Watch", "Backup Date", "Size", "File Path"), "1"))
    print("  " + "─" * 80)
    for r in results:
        print(fmt.format(r["watch"][:24], r["ts"], r["size_h"], r["path"]))
        if verbose:
            info(f"    └─ {r['bak_dir']}")

    noun = "match" if matches == 1 else "matches"
    info(f"\n  {matches} {noun} across {scanned} snapshot(s)."
         + (f"  ({errors} error(s))" if errors else "")
         + (f"  (limit reached — use --limit N to see more)" if matches >= limit else ""))
    return 0


# ── Feature 2: CLI helpers for config auto-backup ─────────────────────────────

def cmd_config_backups(args, cfg):
    """List or restore config.json auto-backups."""
    if getattr(args, "restore_n", None):
        n = args.restore_n
        head(f"Restoring config backup #{n}")
        result = config_manager.restore_config_backup(n)
        if result["ok"]:
            ok(f"config.json restored from backup #{n}. Restart BackupSys to apply.")
        else:
            err(f"Restore failed: {result['error']}")
        return 0 if result["ok"] else 1

    head("Config auto-backups")
    backups = config_manager.list_config_backups()
    if not backups:
        warn("No config backups found. They are written automatically on every save.")
        return 0
    fmt = "  #{:<3}  {:<24}  {:>8}  {}"
    print(_c(fmt.format("N", "Saved at", "Size", "Path"), "1"))
    print("  " + "─" * 72)
    for b in backups:
        print(fmt.format(
            b["n"],
            b["mtime_iso"][:19].replace("T", " "),
            f"{b['size'] // 1024} KB" if b["size"] >= 1024 else f"{b['size']} B",
            b["path"],
        ))
    info(f"\n  To restore:  backupsys_cli config-backups --restore 1")
    return 0


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        prog="backupsys_cli",
        description="BackupSys — headless CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # list
    sub.add_parser("list", help="List all configured watches")

    # config
    sub.add_parser("config", help="Print active configuration (secrets redacted)")

    # keygen
    sub.add_parser("keygen", help="Generate a new Fernet/AES-GCM encryption key")

    p_rk = sub.add_parser(
        "rotate-key",
        help="Re-encrypt all backup files in a directory with a new key",
        description=(
            "Rotate the encryption key for every .enc file inside a backup directory.\n"
            "Decrypts each file with the old key and immediately re-encrypts it with\n"
            "the new key in place.  The manifest hashes are updated accordingly.\n\n"
            "IMPORTANT: keep a safe copy of both keys until you have verified that the\n"
            "rotated backups restore correctly.  The old key cannot be recovered.\n\n"
            "Examples:\n"
            "  backupsys_cli rotate-key --backup-dir C:\\\\backups\\\\MyDocs_bk_abc123\n"
            "  backupsys_cli rotate-key --backup-dir /backups/MyDocs_bk_abc123 --yes"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p_rk.add_argument("--backup-dir", required=True, metavar="PATH",
                      help="Path to the backup directory containing .enc files")
    p_rk.add_argument("--old-key", default=None, metavar="KEY",
                      help="Current encryption key (prompted securely if omitted)")
    p_rk.add_argument("--new-key", default=None, metavar="KEY",
                      help="Replacement encryption key (prompted securely if omitted)")
    p_rk.add_argument("--yes", "-y", action="store_true",
                      help="Skip the confirmation prompt")

    # history
    p_hist = sub.add_parser("history", help="Show recent backup history")
    p_hist.add_argument("--limit", type=int, default=20,
                        metavar="N", help="Number of entries to show (default: 20)")

    # backup
    p_bak = sub.add_parser("backup", help="Run one or more backups")
    _grp = p_bak.add_mutually_exclusive_group(required=True)
    _grp.add_argument("--watch", metavar="NAME_OR_ID",
                      help="Name or ID of the watch to back up")
    _grp.add_argument("--all", action="store_true",
                      help="Back up all active, non-paused watches")
    p_bak.add_argument("--strict", action="store_true",
                       help="Exit with code 1 if any watch fails")

    # dry-run
    p_dry = sub.add_parser("dry-run", help="Preview what would be backed up without copying")
    _dgrp = p_dry.add_mutually_exclusive_group(required=True)
    _dgrp.add_argument("--watch", metavar="NAME_OR_ID",
                       help="Watch to preview")
    _dgrp.add_argument("--all", action="store_true",
                       help="Preview all active, non-paused watches")
    p_dry.add_argument("--verbose", "-v", action="store_true",
                       help="List every file that would be copied/deleted")

    # validate
    p_val = sub.add_parser("validate", help="Validate backup integrity")
    _vgrp = p_val.add_mutually_exclusive_group(required=True)
    _vgrp.add_argument("--watch", metavar="NAME_OR_ID",
                       help="Watch whose latest backup to validate")
    _vgrp.add_argument("--all", action="store_true",
                       help="Validate latest backup for every active watch")

    # restore
    p_rst = sub.add_parser("restore", help="Restore a backup to a folder")
    p_rst.add_argument("--watch", required=True, metavar="NAME_OR_ID",
                       help="Watch name or ID to restore from")
    p_rst.add_argument("--target", required=True, metavar="PATH",
                       help="Destination folder to restore files into")
    p_rst.add_argument("--backup-id", dest="backup_id", default=None,
                       metavar="ID",
                       help="Specific backup ID to restore (default: most recent)")
    p_rst.add_argument("--full-chain", dest="full_chain", action="store_true",
                       help="Replay full incremental chain up to the chosen backup")
    p_rst.add_argument("--no-overwrite", dest="no_overwrite", action="store_true",
                       help="Skip files that already exist in --target")

    # search — Feature 3: cross-watch backup catalog search
    p_srch = sub.add_parser(
        "search",
        help="Search for files across all backup snapshots",
        description=(
            "Scan every MANIFEST.json in the backup destination(s) and print "
            "entries whose filename or path matches PATTERN (fnmatch globs ok).\n\n"
            "Examples:\n"
            "  backupsys_cli search --file '*.docx'\n"
            "  backupsys_cli search --file 'budget*' --watch \"My Documents\"\n"
            "  backupsys_cli search --file 'report.xlsx' --limit 50 --verbose"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p_srch.add_argument("--file", metavar="PATTERN", required=True,
                        help="Filename or path pattern (fnmatch globs: *.docx, report*, etc.)")
    p_srch.add_argument("--watch", metavar="NAME_OR_ID", default=None,
                        help="Limit search to a specific watch (name or ID)")
    p_srch.add_argument("--limit", type=int, default=200, metavar="N",
                        help="Maximum number of results to show (default: 200)")
    p_srch.add_argument("--verbose", "-v", action="store_true",
                        help="Also print the backup directory path for each hit")

    # config-backups — Feature 2: list/restore auto config backups
    p_cbak = sub.add_parser(
        "config-backups",
        help="List or restore config.json auto-backups",
        description=(
            "BackupSys writes a rotating set of config backups (config.backup.1.json … .5.json)\n"
            "every time settings are saved.  Use this command to inspect or recover them.\n\n"
            "Examples:\n"
            "  backupsys_cli config-backups           # list available backups\n"
            "  backupsys_cli config-backups --restore 1  # restore most recent backup"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p_cbak.add_argument("--restore", dest="restore_n", type=int, default=None,
                        metavar="N",
                        help="Restore backup #N over config.json (1 = most recent)")

    args = parser.parse_args()

    try:
        cfg = config_manager.load()
    except Exception as e:
        err(f"Could not load config: {e}")
        sys.exit(1)

    dispatch = {
        "list":           cmd_list,
        "config":         cmd_config,
        "keygen":         cmd_keygen,
        "history":        cmd_history,
        "backup":         cmd_backup,
        "dry-run":        cmd_dryrun,
        "validate":       cmd_validate,
        "restore":        cmd_restore,
        "search":         cmd_search,         # Feature 3
        "config-backups": cmd_config_backups, # Feature 2
        "rotate-key":     cmd_rotate_key,
    }
    fn = dispatch.get(args.command)
    if fn is None:
        err(f"Unknown command: {args.command}")
        sys.exit(1)

    sys.exit(fn(args, cfg))


if __name__ == "__main__":
    main()