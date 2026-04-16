#!/usr/bin/env python3
"""
Regenerate MANIFEST.json files for existing backup directories that are missing them.
This fixes the issue where local backups had their MANIFEST.json deleted, preventing
the "Open Backup" button from working.
"""

import json
import os
from pathlib import Path
from datetime import datetime

def regenerate_manifests(backup_root: str):
    """Regenerate MANIFEST.json for backup directories that don't have them."""
    root = Path(backup_root)
    if not root.exists():
        print(f"Backup root {backup_root} does not exist")
        return

    count = 0
    for backup_dir in root.iterdir():
        if not backup_dir.is_dir():
            continue

        manifest_path = backup_dir / "MANIFEST.json"
        if manifest_path.exists():
            continue  # Already has manifest

        # Parse backup directory name: YYYYMMDD_HHMMSS_fffff__watch_name
        name_parts = backup_dir.name.split('__')
        if len(name_parts) != 2:
            continue

        timestamp_str, watch_name = name_parts
        try:
            # Parse timestamp
            dt = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S_%f')
            timestamp = dt.isoformat()
        except ValueError:
            continue

        # Calculate backup size
        total_size = 0
        files_copied = 0
        for f in backup_dir.rglob('*'):
            if f.is_file() and f.name not in ('MANIFEST.json', 'BACKUP.sha256'):
                total_size += f.stat().st_size
                files_copied += 1

        # Create minimal manifest
        manifest = {
            "backup_id": f"{timestamp_str}_{watch_name}",
            "watch_id": watch_name,  # This is approximate
            "watch_name": watch_name,
            "source": "unknown",  # Can't determine from directory name
            "timestamp": timestamp,
            "status": "success",
            "incremental": True,  # Assume incremental
            "compressed": False,
            "compression_ratio": 0.0,
            "files_copied": files_copied,
            "changes": [],  # Can't reconstruct
            "snapshot": {},  # Can't reconstruct
            "duration_s": 0.0,
            "throughput_mbs": 0.0,
            "total_size_bytes": total_size,
            "failed_files": [],
            "cloud_upload": None,
            "triggered_by": "unknown"
        }

        try:
            with open(manifest_path, 'w') as f:
                json.dump(manifest, f, indent=2)
            print(f"Created MANIFEST.json for {backup_dir.name}")
            count += 1
        except Exception as e:
            print(f"Failed to create MANIFEST.json for {backup_dir.name}: {e}")

    print(f"Regenerated {count} MANIFEST.json files")

if __name__ == "__main__":
    # Assume backups are in ./backups relative to this script
    backup_root = Path(__file__).parent / "backups"
    regenerate_manifests(str(backup_root))