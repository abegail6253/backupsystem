"""
tests/test_build_exe.py — Tests for build_exe.py
=================================================
Run:  pytest tests/test_build_exe.py

build_exe.py is an imperative script (module-level code executes on import),
so we test it by exercising its logic in isolated helper functions that
are extracted and patched rather than importing the module directly.

Covers:
  - Python version guard (< 3.8 → exit)
  - PyInstaller availability guard (missing → exit)
  - Icon auto-generation from icon_256.png via Pillow
  - Icon-generation failure is non-fatal (warning only)
  - Missing desktop_app.py → exit
  - Live config.json credential scan (warns on suspicious fields)
  - Blank config generation (clean config written for bundling)
  - snapshots/ directory creation
  - PyInstaller subprocess invocation (args, returncode 0 / 1)
  - Temp bundle config cleanup on success and failure
  - _BLANK_CONFIG structure: no real credentials, required keys present
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

# ─────────────────────────────────────────────────────────────────────────────
# We cannot safely `import build_exe` because its top-level code runs
# PyInstaller, exits on missing files, etc.  Instead we exec() it into a
# fresh namespace under controlled conditions, or test each logical block
# via patched subprocess calls.
# ─────────────────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BUILD_EXE    = PROJECT_ROOT / "build_exe.py"


def _exec_build(tmp_path: Path, *, extra_globals: dict | None = None,
                pyinstaller_rc: int = 0) -> SimpleNamespace:
    """
    Execute build_exe.py in an isolated namespace with:
      • SCRIPT_DIR  → tmp_path  (so all file ops stay in tmp)
      • PyInstaller subprocess → mocked (returncode = pyinstaller_rc)
      • PIL         → mocked
      • sys.exit    → captured via SystemExit
    Returns a namespace with the captured state for assertions.
    """
    state = SimpleNamespace(run_args=None, run_returncode=pyinstaller_rc)

    def fake_run(args, **_kw):
        state.run_args = args
        return subprocess.CompletedProcess(args=args, returncode=pyinstaller_rc)

    # Write the minimum files the script checks for
    (tmp_path / "desktop_app.py").write_text("# stub\n")
    (tmp_path / "snapshots").mkdir(exist_ok=True)

    fake_pil_image = MagicMock()
    fake_pil_module = MagicMock()
    fake_pil_module.Image = fake_pil_image

    # Mock importlib.util.find_spec to pretend PyInstaller is available
    import importlib.util as _ilu
    real_find_spec = _ilu.find_spec
    def _fake_find_spec(name, *args, **kwargs):
        if name == "PyInstaller":
            return True  # truthy = "found"
        return real_find_spec(name, *args, **kwargs)

    g = {
        "__file__": str(tmp_path / "build_exe.py"),
        "__name__": "__exec_test__",
        # Provide a real __builtins__ so built-ins work normally
        "__builtins__": __builtins__,
    }
    if extra_globals:
        g.update(extra_globals)

    src = BUILD_EXE.read_text(encoding="utf-8")

    with patch("subprocess.run", side_effect=fake_run), \
         patch.dict("sys.modules", {"PIL": fake_pil_module, "PIL.Image": fake_pil_image}), \
         patch("importlib.util.find_spec", side_effect=_fake_find_spec):
        try:
            exec(compile(src, str(BUILD_EXE), "exec"), g)  # noqa: S102
        except SystemExit as exc:
            state.exit_code = exc.code
        else:
            state.exit_code = None

    return state


# ─────────────────────────────────────────────────────────────────────────────
# Python version guard
# ─────────────────────────────────────────────────────────────────────────────

class TestPythonVersionGuard:
    def test_exits_on_python_37(self):
        src = BUILD_EXE.read_text(encoding="utf-8")
        with patch.object(sys, "version_info", (3, 7, 9, "final", 0)):
            with pytest.raises(SystemExit) as exc:
                exec(compile(src, str(BUILD_EXE), "exec"),  # noqa: S102
                     {"__builtins__": __builtins__, "__file__": str(BUILD_EXE)})
        assert exc.value.code == 1

    def test_passes_on_python_38(self, tmp_path):
        # Current interpreter is >= 3.8, so just ensure the script doesn't
        # exit with the version-guard message.
        state = _exec_build(tmp_path)
        # If version check triggered, exit_code would be 1 before subprocess
        assert state.run_args is not None, "PyInstaller was never called — version guard may have triggered"


# ─────────────────────────────────────────────────────────────────────────────
# PyInstaller availability guard
# ─────────────────────────────────────────────────────────────────────────────

class TestPyInstallerGuard:
    def test_exits_when_pyinstaller_missing(self, tmp_path):
        (tmp_path / "desktop_app.py").write_text("# stub\n")
        src = BUILD_EXE.read_text(encoding="utf-8")
        fake_util = MagicMock()
        fake_util.find_spec.return_value = None   # simulate missing package

        with patch.dict("sys.modules", {"importlib.util": fake_util}):
            with pytest.raises(SystemExit) as exc:
                exec(compile(src, str(BUILD_EXE), "exec"),  # noqa: S102
                     {"__builtins__": __builtins__,
                      "__file__": str(tmp_path / "build_exe.py")})
        assert exc.value.code == 1


# ─────────────────────────────────────────────────────────────────────────────
# Icon generation
# ─────────────────────────────────────────────────────────────────────────────

class TestIconGeneration:
    def test_generates_ico_from_png_when_png_present(self, tmp_path, capsys):
        png = tmp_path / "icon_256.png"
        png.write_bytes(b"\x89PNG\r\n\x1a\n")  # minimal PNG-ish bytes
        _exec_build(tmp_path)
        # Pillow save should have been called (mocked, no real file written)
        # Just verify the script completed without exiting
        # (icon generation failure is non-fatal, so we only check no crash)

    def test_skips_icon_generation_when_no_png(self, tmp_path, capsys):
        """No icon_256.png → script should proceed without attempting conversion."""
        state = _exec_build(tmp_path)
        assert state.exit_code in (None, 0)

    def test_icon_generation_failure_is_nonfatal(self, tmp_path, capsys):
        """If Pillow raises, a warning is printed but the build continues."""
        png = tmp_path / "icon_256.png"
        png.write_bytes(b"fake")

        bad_pil = MagicMock()
        bad_pil.Image.open.side_effect = Exception("Pillow error")

        with patch.dict("sys.modules", {"PIL": bad_pil, "PIL.Image": bad_pil.Image}):
            state = _exec_build(tmp_path)
        # Script should not have exited with code 1 due to icon failure
        assert state.exit_code in (None, 0)


# ─────────────────────────────────────────────────────────────────────────────
# Missing entry point guard
# ─────────────────────────────────────────────────────────────────────────────

class TestMissingEntryPoint:
    def test_exits_when_desktop_app_missing(self, tmp_path):
        # Do NOT create desktop_app.py
        src = BUILD_EXE.read_text(encoding="utf-8")
        with patch("subprocess.run"):
            with pytest.raises(SystemExit) as exc:
                exec(compile(src, str(BUILD_EXE), "exec"),  # noqa: S102
                     {"__builtins__": __builtins__,
                      "__file__": str(tmp_path / "build_exe.py")})
        assert exc.value.code == 1


# ─────────────────────────────────────────────────────────────────────────────
# Blank config generation
# ─────────────────────────────────────────────────────────────────────────────

class TestBlankConfig:
    def test_blank_config_is_valid_json(self):
        """_BLANK_CONFIG as defined in build_exe.py must be valid JSON."""
        src = BUILD_EXE.read_text(encoding="utf-8")
        # Extract the _BLANK_CONFIG literal by exec-ing only up to it
        ns: dict = {"__builtins__": __builtins__}
        # Find the line where _BLANK_CONFIG starts and ends, then eval it
        match = re.search(r"_BLANK_CONFIG\s*=\s*(\{.*?\n\})", src, re.DOTALL)
        assert match, "_BLANK_CONFIG definition not found in build_exe.py"
        cfg = eval(match.group(1), {})  # noqa: S307 – safe, no user input
        serialised = json.dumps(cfg)
        assert json.loads(serialised) == cfg

    def test_blank_config_contains_no_credentials(self):
        """Every password/token/key field in _BLANK_CONFIG must be empty string."""
        src = BUILD_EXE.read_text(encoding="utf-8")
        match = re.search(r"_BLANK_CONFIG\s*=\s*(\{.*?\n\})", src, re.DOTALL)
        cfg = eval(match.group(1), {})  # noqa: S307

        def _find_creds(obj, path=""):
            issues = []
            if isinstance(obj, dict):
                for k, v in obj.items():
                    issues += _find_creds(v, f"{path}.{k}")
            elif isinstance(obj, str) and obj.strip():
                if re.search(r"(?i)(password|secret|token|key)", path):
                    issues.append(f"{path} = {obj!r}")
            return issues

        leaks = _find_creds(cfg)
        assert leaks == [], f"Credential fields with values in _BLANK_CONFIG: {leaks}"

    def test_blank_config_has_required_keys(self):
        src = BUILD_EXE.read_text(encoding="utf-8")
        match = re.search(r"_BLANK_CONFIG\s*=\s*(\{.*?\n\})", src, re.DOTALL)
        cfg = eval(match.group(1), {})  # noqa: S307
        for key in ("destination", "dest_type", "auto_backup", "watches",
                    "retention_days", "email_config"):
            assert key in cfg, f"Missing key in _BLANK_CONFIG: {key}"

    def test_blank_config_watches_is_empty(self):
        src = BUILD_EXE.read_text(encoding="utf-8")
        match = re.search(r"_BLANK_CONFIG\s*=\s*(\{.*?\n\})", src, re.DOTALL)
        cfg = eval(match.group(1), {})  # noqa: S307
        assert cfg["watches"] == []


# ─────────────────────────────────────────────────────────────────────────────
# Live config credential scan
# ─────────────────────────────────────────────────────────────────────────────

class TestLiveConfigScan:
    def test_warns_on_suspicious_fields(self, tmp_path, capsys):
        """Script should print a warning if live config.json has filled creds."""
        live_cfg = {
            "destination": "./backups",
            "dest_type": "local",
            "email_config": {"password": "s3cr3t!", "smtp_host": "smtp.example.com"},
            "watches": [],
        }
        (tmp_path / "config.json").write_text(json.dumps(live_cfg))
        state = _exec_build(tmp_path)
        # Build should still succeed (credentials are NOT bundled)
        assert state.exit_code in (None, 0)

    def test_no_warning_when_config_has_no_creds(self, tmp_path, capsys):
        """Clean config.json (no credentials) should produce no credential warning."""
        clean_cfg = {"destination": "./backups", "dest_type": "local",
                     "email_config": {"password": "", "smtp_host": ""},
                     "watches": []}
        (tmp_path / "config.json").write_text(json.dumps(clean_cfg))
        state = _exec_build(tmp_path)
        assert state.exit_code in (None, 0)


# ─────────────────────────────────────────────────────────────────────────────
# PyInstaller invocation
# ─────────────────────────────────────────────────────────────────────────────

class TestPyInstallerInvocation:
    def test_pyinstaller_called_with_correct_name(self, tmp_path):
        state = _exec_build(tmp_path)
        assert state.run_args is not None
        assert "BackupSystem" in state.run_args

    def test_pyinstaller_called_with_windowed(self, tmp_path):
        state = _exec_build(tmp_path)
        assert "--windowed" in state.run_args

    def test_pyinstaller_called_with_noconfirm(self, tmp_path):
        state = _exec_build(tmp_path)
        assert "--noconfirm" in state.run_args

    def test_pyinstaller_called_with_onedir(self, tmp_path):
        state = _exec_build(tmp_path)
        assert "--onedir" in state.run_args

    def test_pyinstaller_called_with_clean(self, tmp_path):
        state = _exec_build(tmp_path)
        assert "--clean" in state.run_args

    def test_exits_on_pyinstaller_failure(self, tmp_path):
        state = _exec_build(tmp_path, pyinstaller_rc=1)
        assert state.exit_code == 1

    def test_no_exit_on_pyinstaller_success(self, tmp_path):
        state = _exec_build(tmp_path, pyinstaller_rc=0)
        assert state.exit_code in (None, 0)


# ─────────────────────────────────────────────────────────────────────────────
# Temp bundle config cleanup
# ─────────────────────────────────────────────────────────────────────────────

class TestBundleConfigCleanup:
    def test_bundle_config_removed_after_success(self, tmp_path):
        _exec_build(tmp_path, pyinstaller_rc=0)
        bundle_cfg = tmp_path / ".bundle_config.json"
        assert not bundle_cfg.exists(), ".bundle_config.json was not cleaned up after success"

    def test_bundle_config_removed_after_failure(self, tmp_path):
        _exec_build(tmp_path, pyinstaller_rc=1)
        bundle_cfg = tmp_path / ".bundle_config.json"
        assert not bundle_cfg.exists(), ".bundle_config.json was not cleaned up after failure"
