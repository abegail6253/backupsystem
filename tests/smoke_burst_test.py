"""Smoke test for burst-cache overwrite behavior.

This script imports `desktop_app` and simulates a confirmed remote attribution
followed by a server-local (local_actor) attribution attempt. The expected
behavior (after the recent patch) is that the logon-confirmed remote entry
is NOT overwritten by the later local_actor guess.
"""
import time
import logging
import sys
from pathlib import Path

# Ensure repository root is on sys.path so imports like `desktop_app` resolve
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import desktop_app


def reset_cache():
    with desktop_app._burst_cache_lock:
        desktop_app._burst_cache.clear()


def print_cache():
    with desktop_app._burst_cache_lock:
        for k, v in desktop_app._burst_cache.items():
            print(f"KEY={k} -> machine={v[0]}, ip={v[1]}, user={v[2]}, local_actor={v[4]}, event_type={v[5]}, logon_id_confirmed={v[6]}")


def run_test():
    logging.basicConfig(level=logging.INFO)
    reset_cache()

    # Step 1: confirmed remote attribution (logon_id_confirmed=True)
    desktop_app._burst_cache_put(
        host="fileserver",
        server_local_user="Administrator",
        machine="DESKTOP-0EDUBAP",
        ip="192.168.254.106",
        user="UserA",
        local_actor=False,
        event_type="deleted",
        logon_id_confirmed=True,
    )

    print("After remote confirmed put:")
    print_cache()

    # Wait briefly then attempt a local_actor overwrite (should be rejected)
    time.sleep(0.5)

    desktop_app._burst_cache_put(
        host="fileserver",
        server_local_user="Administrator",
        machine="DESKTOP-KGG55PU",
        ip="192.168.254.105",
        user="UserB",
        local_actor=True,
        event_type="deleted",
        logon_id_confirmed=False,
    )

    print("After local_actor put attempt:")
    print_cache()

    # Verify
    with desktop_app._burst_cache_lock:
        key = ("fileserver", "administrator")
        entry = desktop_app._burst_cache.get(key)
        if entry is None:
            print("FAIL: entry missing")
            raise SystemExit(2)
        if entry[0].lower() == "desktop-0edubap":
            print("PASS: confirmed remote attribution preserved (no overwrite)")
            raise SystemExit(0)
        else:
            print(f"FAIL: entry was overwritten -> machine={entry[0]}")
            raise SystemExit(3)


if __name__ == "__main__":
    run_test()
