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

        # Build cloud_config for non-local destinations
        _cloud_cfg = None
        if dest_type == "sftp":
            _cloud_cfg = {**cfg.get("dest_sftp", {}), "_dest_type": "sftp"}
        elif dest_type in ("ftp", "ftps"):
            _cloud_cfg = {**cfg.get("dest_ftp", {}), "_dest_type": dest_type}
        elif dest_type == "https":
            _cloud_cfg = {**cfg.get("dest_https", {}), "_dest_type": "https"}
        elif dest_type == "webdav":
            _cloud_cfg = {**cfg.get("dest_webdav", {}), "_dest_type": "webdav"}
        elif dest_type == "cloud":
            _wcc = w.get("cloud_config") or {}
            if _wcc:
                _cloud_cfg = {**_wcc, "_dest_type": "cloud"}

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

    if any_failed and args.strict:
        return 1
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

    args = parser.parse_args()

    try:
        cfg = config_manager.load()
    except Exception as e:
        err(f"Could not load config: {e}")
        sys.exit(1)

    dispatch = {
        "list":     cmd_list,
        "config":   cmd_config,
        "keygen":   cmd_keygen,
        "history":  cmd_history,
        "backup":   cmd_backup,
        "dry-run":  cmd_dryrun,
        "validate": cmd_validate,
    }
    fn = dispatch.get(args.command)
    if fn is None:
        err(f"Unknown command: {args.command}")
        sys.exit(1)

    sys.exit(fn(args, cfg))


if __name__ == "__main__":
    main()
