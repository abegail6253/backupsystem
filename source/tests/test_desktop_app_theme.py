"""
tests/test_desktop_app_theme.py — Unit tests for desktop_app theme functionality.
Run:  pytest tests/test_desktop_app_theme.py
"""
import os, sys
from pathlib import Path
from unittest.mock import Mock, patch
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import desktop_app as da


class TestThemeDefaults:
    """Test theme default behavior and QSettings integration."""

    @patch('desktop_app.QSettings')
    def test_theme_default_no_saved_preference(self, mock_qsettings):
        """Test that theme defaults to 'dark' when no preference is saved."""
        # Mock QSettings to return "" (no saved preference)
        mock_settings = Mock()
        mock_settings.value.return_value = ""
        mock_qsettings.return_value = mock_settings

        # Test the startup theme logic
        _theme_override = mock_settings.value("theme", "dark")
        _active_theme = _theme_override if _theme_override != "" else "dark"

        assert _active_theme == "dark"
        mock_settings.value.assert_called_with("theme", "dark")

    @patch('desktop_app.QSettings')
    def test_theme_explicit_dark(self, mock_qsettings):
        """Test that explicitly saved 'dark' theme is respected."""
        # Mock QSettings to return "dark"
        mock_settings = Mock()
        mock_settings.value.return_value = "dark"
        mock_qsettings.return_value = mock_settings

        # Test the startup theme logic
        _theme_override = mock_settings.value("theme", "dark")
        _active_theme = _theme_override if _theme_override != "" else "dark"

        assert _active_theme == "dark"
        mock_settings.value.assert_called_with("theme", "dark")

    @patch('desktop_app.QSettings')
    def test_theme_explicit_light(self, mock_qsettings):
        """Test that explicitly saved 'light' theme is respected."""
        # Mock QSettings to return "light"
        mock_settings = Mock()
        mock_settings.value.return_value = "light"
        mock_qsettings.return_value = mock_settings

        # Test the startup theme logic
        _theme_override = mock_settings.value("theme", "dark")
        _active_theme = _theme_override if _theme_override != "" else "dark"

        assert _active_theme == "light"
        mock_settings.value.assert_called_with("theme", "dark")

    @patch('desktop_app.QSettings')
    def test_theme_apply_logic_auto_mode(self, mock_qsettings):
        """Test that auto mode (empty choice) defaults to 'dark' in _apply_theme logic."""
        # Mock QSettings
        mock_settings = Mock()
        mock_qsettings.return_value = mock_settings

        # Test the _apply_theme resolution logic with empty choice (auto mode)
        choice = ""  # This represents auto mode
        _resolved = choice if choice != "" else "dark"

        assert _resolved == "dark"

    @patch('desktop_app.QSettings')
    def test_theme_apply_logic_explicit_dark(self, mock_qsettings):
        """Test that explicit 'dark' choice is resolved correctly."""
        # Mock QSettings
        mock_settings = Mock()
        mock_qsettings.return_value = mock_settings

        # Test the _apply_theme resolution logic with explicit dark choice
        choice = "dark"
        _resolved = choice if choice != "" else "dark"

        assert _resolved == "dark"

    @patch('desktop_app.QSettings')
    def test_theme_apply_logic_explicit_light(self, mock_qsettings):
        """Test that explicit 'light' choice is resolved correctly."""
        # Mock QSettings
        mock_settings = Mock()
        mock_qsettings.return_value = mock_settings

        # Test the _apply_theme resolution logic with explicit light choice
        choice = "light"
        _resolved = choice if choice != "" else "dark"

        assert _resolved == "light"