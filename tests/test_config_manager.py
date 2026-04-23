"""
tests/test_config_manager.py — Unit tests for config_manager.
Run:  pytest tests/test_config_manager.py
"""
import os, sys
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config_manager as cm


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    """Redirect config_manager to a fresh directory for every test."""
    cfg_file = tmp_path / "config.json"
    monkeypatch.setattr(cm, "CONFIG_PATH",   cfg_file,                       raising=False)
    monkeypatch.setattr(cm, "QUEUE_PATH",    tmp_path / "backup_queue.json", raising=False)
    monkeypatch.setattr(cm, "HISTORY_PATH",  tmp_path / "history.json",      raising=False)
    monkeypatch.setattr(cm, "SNAPSHOTS_DIR", tmp_path / "snapshots",         raising=False)
    # Force clear of in-memory cache every test
    cm._config_cache["cfg"]   = None
    cm._config_cache["mtime"] = 0.0
    # Write a guaranteed-empty starting config to disk
    import json
    cfg_file.write_text(json.dumps({
        "destination": "./backups", "dest_type": "local",
        "auto_backup": False, "interval_min": 30, "interval_unit": "minutes",
        "retention_days": 30, "compression_enabled": False,
        "auto_retry": True, "retry_delay_min": 5, "max_backup_mbps": 0,
        "webhook_url": "", "webhook_on_success": False, "watches": [],
        "default_exclude_patterns": ["*.tmp", "*.log", ".git"],
        "email_config": {"enabled": False, "smtp_host": "", "smtp_port": 587,
                         "smtp_use_ssl": False, "username": "", "from_addr": "",
                         "to_addr": "", "notify_on_success": False, "notify_on_failure": True},
    }))
    yield tmp_path
    cm._config_cache["cfg"]   = None
    cm._config_cache["mtime"] = 0.0


def _fresh():
    """Load config with cache busted."""
    cm._config_cache["cfg"] = None
    return cm.load()


# ─── load / save ──────────────────────────────────────────────────────────────
class TestLoadSave:
    def test_load_returns_dict_with_watches(self):
        cfg = cm.load()
        assert isinstance(cfg, dict) and "watches" in cfg

    def test_save_and_reload(self):
        cfg = cm.load(); cfg["destination"] = "/custom"; cm.save(cfg)
        assert _fresh()["destination"] == "/custom"

    def test_two_successive_loads_are_consistent(self):
        # Both calls use the cache; value must not change between them
        assert cm.load()["auto_backup"] == cm.load()["auto_backup"]

    def test_corrupt_json_falls_back_to_defaults(self, tmp_path):
        (tmp_path / "config.json").write_text("{ BAD JSON !!!")
        cm._config_cache["cfg"] = None
        cfg = cm.load()
        assert isinstance(cfg, dict) and "watches" in cfg


# ─── Watch CRUD ───────────────────────────────────────────────────────────────
class TestWatchCrud:
    def test_add_watch_persists(self, tmp_path):
        # tmp_path itself exists, so no skip needed
        cm.add_watch(_fresh(), "W", str(tmp_path))
        assert len(_fresh()["watches"]) == 1

    def test_add_watch_returns_dict_with_id(self, tmp_path):
        w = cm.add_watch(_fresh(), "W", str(tmp_path))
        assert isinstance(w, dict) and "id" in w and w["id"]

    def test_add_assigns_unique_ids(self, tmp_path):
        src1 = tmp_path / "s1"; src1.mkdir()
        src2 = tmp_path / "s2"; src2.mkdir()
        w1 = cm.add_watch(_fresh(), "A", str(src1))
        w2 = cm.add_watch(_fresh(), "B", str(src2))
        assert w1["id"] != w2["id"]

    def test_remove_watch_clears_it(self, tmp_path):
        w = cm.add_watch(_fresh(), "ToRemove", str(tmp_path))
        cm.remove_watch(_fresh(), w["id"])
        assert len(_fresh()["watches"]) == 0

    def test_remove_nonexistent_returns_false(self):
        assert cm.remove_watch(_fresh(), "no-such-id") is False

    def test_update_watch_meta_changes_name(self, tmp_path):
        w = cm.add_watch(_fresh(), "Original", str(tmp_path))
        cm.update_watch_meta(_fresh(), w["id"], name="Updated")
        updated = next(x for x in _fresh()["watches"] if x["id"] == w["id"])
        assert updated["name"] == "Updated"

    def test_update_watch_meta_changes_active(self, tmp_path):
        w = cm.add_watch(_fresh(), "W", str(tmp_path))
        cm.update_watch_meta(_fresh(), w["id"], active=False)
        updated = next(x for x in _fresh()["watches"] if x["id"] == w["id"])
        assert updated["active"] is False

    def test_encrypt_key_preserved_on_update(self, tmp_path):
        w = cm.add_watch(_fresh(), "Enc", str(tmp_path), encrypt_key="mykey")
        cm.update_watch_meta(_fresh(), w["id"], name="Renamed")
        updated = next(x for x in _fresh()["watches"] if x["id"] == w["id"])
        assert updated.get("encrypt_key") == "mykey"

    def test_get_watch_by_id(self, tmp_path):
        w = cm.add_watch(_fresh(), "Find Me", str(tmp_path))
        found = cm.get_watch(_fresh(), w["id"])
        assert found is not None and found["name"] == "Find Me"


# ─── Snapshot persistence ─────────────────────────────────────────────────────
class TestSnapshots:
    def test_save_and_load(self):
        snap = {"f.txt": {"hash": "abc", "size": 10}}
        cm.save_snapshot("snap-001", snap)
        assert cm.load_snapshot("snap-001") == snap

    def test_missing_returns_empty_or_none(self):
        result = cm.load_snapshot("nonexistent-id")
        assert result == {} or result is None

    def test_overwrite(self):
        cm.save_snapshot("snap-002", {"a.txt": {"hash": "1", "size": 1}})
        cm.save_snapshot("snap-002", {"b.txt": {"hash": "2", "size": 2}})
        loaded = cm.load_snapshot("snap-002")
        assert "b.txt" in loaded and "a.txt" not in loaded


# ─── Defaults ─────────────────────────────────────────────────────────────────
class TestDefaults:
    def test_auto_backup_off_by_default(self):
        assert cm.load().get("auto_backup") is False

    def test_default_exclude_patterns_is_list(self):
        patterns = cm.load().get("default_exclude_patterns", [])
        assert isinstance(patterns, list) and len(patterns) > 0

    def test_email_config_has_enabled_key(self):
        assert "enabled" in cm.load().get("email_config", {})

    def test_watches_is_list(self):
        assert isinstance(cm.load().get("watches"), list)
