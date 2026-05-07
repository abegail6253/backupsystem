"""
tests/test_setup_wizard.py — Unit tests for setup_wizard.py

All file-system side-effects are performed inside a temporary directory;
no packages are installed, no Registry keys are written, and no stdin
reads are triggered.
"""

import json
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch, call

# Make sure the project root is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import setup_wizard


# ─── helper ──────────────────────────────────────────────────────────────────

def _wizard_in(tmp_path: Path):
    """
    Monkey-patch setup_wizard.HERE so that file-creation functions write
    inside *tmp_path* rather than the real project folder.
    Returns the patcher so the caller can stop it if needed.
    """
    return patch.object(setup_wizard, "HERE", tmp_path)


# ─── check_python ─────────────────────────────────────────────────────────────

class TestCheckPython(unittest.TestCase):

    def test_current_python_passes(self):
        """The wizard must not exit when running under a supported interpreter."""
        # sys.version_info is always >= 3.8 in any supported test env
        with patch("setup_wizard.OK"), patch("setup_wizard.HDR"):
            try:
                setup_wizard.check_python()
            except SystemExit:
                self.fail("check_python() called sys.exit() on a supported Python version")

    def test_old_python_exits(self):
        """A Python < 3.8 tuple must cause a sys.exit(1) call."""
        old_version = (3, 7, 0, "final", 0)
        with patch("sys.version_info", old_version), \
             patch("setup_wizard.ERR"), \
             patch("setup_wizard.HDR"):
            with self.assertRaises(SystemExit) as ctx:
                setup_wizard.check_python()
            self.assertEqual(ctx.exception.code, 1)


# ─── install_packages ─────────────────────────────────────────────────────────

class TestInstallPackages(unittest.TestCase):

    def test_missing_requirements_file_exits(self, tmp_path=None):
        """If requirements_desktop.txt is absent, setup must exit with code 1."""
        import tempfile, os
        with tempfile.TemporaryDirectory() as td:
            with _wizard_in(Path(td)), \
                 patch("setup_wizard.ERR"), \
                 patch("setup_wizard.HDR"), \
                 patch("setup_wizard.INF"):
                with self.assertRaises(SystemExit) as ctx:
                    setup_wizard.install_packages()
                self.assertEqual(ctx.exception.code, 1)

    def test_pip_failure_exits(self):
        """A non-zero pip return code must cause sys.exit(1)."""
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            (td / "requirements_desktop.txt").write_text("PyQt6\n")
            failed = MagicMock()
            failed.returncode = 1
            with _wizard_in(td), \
                 patch("subprocess.run", return_value=failed), \
                 patch("setup_wizard.ERR"), \
                 patch("setup_wizard.HDR"), \
                 patch("setup_wizard.INF"):
                with self.assertRaises(SystemExit) as ctx:
                    setup_wizard.install_packages()
                self.assertEqual(ctx.exception.code, 1)

    def test_pip_success_does_not_exit(self):
        """A zero pip return code must not raise SystemExit."""
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            (td / "requirements_desktop.txt").write_text("PyQt6\n")
            success = MagicMock()
            success.returncode = 0
            with _wizard_in(td), \
                 patch("subprocess.run", return_value=success), \
                 patch("setup_wizard.OK"), \
                 patch("setup_wizard.HDR"), \
                 patch("setup_wizard.INF"):
                try:
                    setup_wizard.install_packages()
                except SystemExit:
                    self.fail("install_packages() exited on a successful pip run")


# ─── create_env ───────────────────────────────────────────────────────────────

class TestCreateEnv(unittest.TestCase):

    def test_creates_env_file_when_absent(self):
        """create_env() must write a .env file if one does not exist."""
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            with _wizard_in(td), \
                 patch("setup_wizard.OK"), \
                 patch("setup_wizard.WRN"), \
                 patch("setup_wizard.HDR"):
                setup_wizard.create_env()
            env_path = td / ".env"
            self.assertTrue(env_path.exists(), ".env was not created")
            content = env_path.read_text(encoding="utf-8")
            self.assertIn("BACKUPSYS_EMAIL_PASSWORD", content)

    def test_does_not_overwrite_existing_env(self):
        """If .env already exists, create_env() must leave it untouched."""
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            sentinel = "# original content\n"
            (td / ".env").write_text(sentinel, encoding="utf-8")
            with _wizard_in(td), \
                 patch("setup_wizard.WRN"), \
                 patch("setup_wizard.INF"), \
                 patch("setup_wizard.HDR"):
                setup_wizard.create_env()
            self.assertEqual((td / ".env").read_text(encoding="utf-8"), sentinel)


# ─── create_config ────────────────────────────────────────────────────────────

class TestCreateConfig(unittest.TestCase):

    def test_creates_valid_json_config(self):
        """create_config() must write a parseable config.json with expected keys."""
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            with _wizard_in(td), \
                 patch("setup_wizard.OK"), \
                 patch("setup_wizard.WRN"), \
                 patch("setup_wizard.HDR"):
                setup_wizard.create_config()
            cfg_path = td / "config.json"
            self.assertTrue(cfg_path.exists(), "config.json was not created")
            with open(cfg_path, encoding="utf-8") as f:
                data = json.load(f)
            self.assertIn("destination", data)
            self.assertIn("watches", data)
            self.assertIn("retention_days", data)
            self.assertIsInstance(data["watches"], list)

    def test_creates_required_directories(self):
        """create_config() must also create backups/ and snapshots/ directories."""
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            with _wizard_in(td), \
                 patch("setup_wizard.OK"), \
                 patch("setup_wizard.WRN"), \
                 patch("setup_wizard.HDR"):
                setup_wizard.create_config()
            self.assertTrue((td / "backups").is_dir())
            self.assertTrue((td / "snapshots").is_dir())

    def test_does_not_overwrite_existing_config(self):
        """If config.json exists, create_config() must leave it alone."""
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            original = {"custom": True}
            (td / "config.json").write_text(json.dumps(original), encoding="utf-8")
            with _wizard_in(td), \
                 patch("setup_wizard.WRN"), \
                 patch("setup_wizard.HDR"):
                setup_wizard.create_config()
            with open(td / "config.json", encoding="utf-8") as f:
                data = json.load(f)
            self.assertEqual(data, original)

    def test_default_config_has_safe_values(self):
        """Sensitive fields must default to empty/disabled."""
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            with _wizard_in(td), \
                 patch("setup_wizard.OK"), \
                 patch("setup_wizard.WRN"), \
                 patch("setup_wizard.HDR"):
                setup_wizard.create_config()
            with open(td / "config.json", encoding="utf-8") as f:
                data = json.load(f)
            email_cfg = data.get("email_config", {})
            self.assertFalse(email_cfg.get("enabled", True),
                             "Email must default to disabled")
            self.assertFalse(data.get("auto_backup", True),
                             "Auto-backup must default to disabled")


# ─── verify_install ───────────────────────────────────────────────────────────

class TestVerifyInstall(unittest.TestCase):

    def test_returns_true_when_all_imports_succeed(self):
        """verify_install() must return True when every required module imports."""
        dummy = MagicMock()
        with patch("builtins.__import__", return_value=dummy), \
             patch("setup_wizard.OK"), \
             patch("setup_wizard.ERR"), \
             patch("setup_wizard.INF"), \
             patch("setup_wizard.HDR"):
            result = setup_wizard.verify_install()
        self.assertTrue(result)

    def test_returns_false_when_a_required_import_fails(self):
        """verify_install() must return False if any required package is absent."""
        original_import = __builtins__.__import__ if hasattr(__builtins__, "__import__") else __import__

        def _fail_pyqt(name, *args, **kwargs):
            if name == "PyQt5":
                raise ImportError("No module named PyQt5")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=_fail_pyqt), \
             patch("setup_wizard.OK"), \
             patch("setup_wizard.ERR"), \
             patch("setup_wizard.INF"), \
             patch("setup_wizard.HDR"):
            result = setup_wizard.verify_install()
        self.assertFalse(result)


# ─── run_for_app ──────────────────────────────────────────────────────────────

class TestRunForApp(unittest.TestCase):
    """run_for_app() is the non-interactive entry point called by desktop_app.py."""

    def test_returns_true_on_success(self):
        """Must return True when both create_env and create_config succeed."""
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            with _wizard_in(td), \
                 patch("setup_wizard.OK"), \
                 patch("setup_wizard.WRN"), \
                 patch("setup_wizard.HDR"), \
                 patch("setup_wizard.INF"):
                result = setup_wizard.run_for_app()
        self.assertTrue(result)

    def test_returns_false_on_exception(self):
        """Must return False (not raise) if an unexpected error occurs."""
        with patch("setup_wizard.create_env", side_effect=PermissionError("denied")), \
             patch("setup_wizard.create_config"):
            result = setup_wizard.run_for_app()
        self.assertFalse(result)

    def test_does_not_call_sys_exit(self):
        """Must never call sys.exit() — it runs inside the GUI process."""
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            with _wizard_in(td), \
                 patch("setup_wizard.OK"), \
                 patch("setup_wizard.WRN"), \
                 patch("setup_wizard.HDR"), \
                 patch("setup_wizard.INF"), \
                 patch("sys.exit", side_effect=AssertionError("sys.exit must not be called")) as mock_exit:
                setup_wizard.run_for_app()
            mock_exit.assert_not_called()


# ─── offer_startup_gui ────────────────────────────────────────────────────────

class TestOfferStartupGui(unittest.TestCase):
    """offer_startup_gui() writes a startup entry without any stdin prompt."""

    @unittest.skipUnless(sys.platform == "win32", "Windows-only")
    def test_windows_writes_registry_key(self):
        mock_key = MagicMock()
        with patch("winreg.OpenKey", return_value=mock_key), \
             patch("winreg.SetValueEx") as mock_set, \
             patch("winreg.CloseKey"):
            result = setup_wizard.offer_startup_gui()
        self.assertTrue(result)
        mock_set.assert_called_once()

    @unittest.skipIf(sys.platform == "win32", "Non-Windows only")
    def test_non_windows_returns_true(self):
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            if sys.platform == "darwin":
                agents = td / "Library" / "LaunchAgents"
                agents.mkdir(parents=True)
                with patch("pathlib.Path.home", return_value=td):
                    result = setup_wizard.offer_startup_gui()
            else:  # Linux / XDG
                autostart = td / ".config" / "autostart"
                autostart.mkdir(parents=True)
                with patch("pathlib.Path.home", return_value=td):
                    result = setup_wizard.offer_startup_gui()
        self.assertTrue(result)

    def test_returns_false_on_exception(self):
        """Must return False (not raise) if the OS call fails."""
        with patch("sys.platform", "win32"), \
             patch("builtins.__import__", side_effect=ImportError("winreg")):
            result = setup_wizard.offer_startup_gui()
        self.assertFalse(result)


# ─── _c colour helper ─────────────────────────────────────────────────────────

class TestColourHelper(unittest.TestCase):

    def test_wraps_text_in_ansi_codes(self):
        result = setup_wizard._c("hello", "32")
        self.assertIn("hello", result)
        self.assertIn("\033[32m", result)
        self.assertIn("\033[0m", result)

    def test_empty_text_still_returns_string(self):
        result = setup_wizard._c("", "31")
        self.assertIsInstance(result, str)


if __name__ == "__main__":
    unittest.main()
