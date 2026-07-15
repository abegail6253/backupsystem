"""Lightweight internationalisation (i18n) for BackupSys.

Design goals (deliberately NOT the full Qt Linguist / .ts / .qm toolchain):
  * The ENGLISH SOURCE STRING is the lookup key — e.g. tr("Save Settings").
    Nothing to keep in sync, and any un-translated string simply falls back to
    English, so the UI is never blank.
  * Translations live in plain JSON (translations/<lang>.json) shaped as
        { "English source": "translated string", ... }
    A native speaker (e.g. the customer's team) can proofread/edit ja.json
    directly in any text editor — no special tooling required.
  * Parameterised messages use str.format placeholders so word order can differ
    per language:  tr("Backed up {n} files", n=count)

Language is chosen once at startup (persisted in QSettings by the app) and
applied on restart — see set_language() / get_language().
"""
from __future__ import annotations

import json
import logging
import os
import sys

logger = logging.getLogger("backupsys.i18n")

# Japanese is the default because the primary audience is Japanese customers;
# English remains available via the in-app switcher.
DEFAULT_LANGUAGE = "ja"

# code -> native display name (shown in the language switcher)
_AVAILABLE = {
    "en": "English",
    "ja": "日本語",   # 日本語
}

_lang: str = DEFAULT_LANGUAGE
_maps: dict[str, dict] = {}        # lang code -> { source: translation }


def _candidate_dirs() -> list:
    """All directories that might hold translations/, most-likely first.

    PyInstaller's bundle layout varies (onefile -> _MEIPASS; onedir -> the
    exe dir AND its _internal subfolder), and a build script may regenerate
    the .spec and drop the bundled data.  So we search several locations and
    also accept a translations/ folder dropped next to the .exe by hand.
    """
    dirs = []
    if getattr(sys, "frozen", False):
        meipass = getattr(sys, "_MEIPASS", "")
        exedir = os.path.dirname(sys.executable)
        if meipass:
            dirs.append(meipass)                        # onefile / onedir _internal
        dirs.append(exedir)                             # next to the .exe
        dirs.append(os.path.join(exedir, "_internal"))  # PyInstaller 6 onedir layout
        dirs.append(os.path.dirname(exedir))            # parent, just in case
    # source-tree / dev location (also a final fallback for frozen builds)
    dirs.append(os.path.dirname(os.path.abspath(__file__)))
    # de-dup, preserve order
    seen, out = set(), []
    for d in dirs:
        if d and d not in seen:
            seen.add(d); out.append(d)
    return out


def _find_translation_file(lang: str) -> str:
    """First existing translations/<lang>.json across candidate dirs, or ''."""
    for d in _candidate_dirs():
        p = os.path.join(d, "translations", f"{lang}.json")
        if os.path.isfile(p):
            return p
    return ""


def _load_map(lang: str) -> dict:
    """Load and cache translations/<lang>.json (UTF-8). Missing file -> {}."""
    if lang in _maps:
        return _maps[lang]
    data: dict = {}
    path = _find_translation_file(lang)
    if path:
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            logger.warning("i18n: failed to load %s: %s", path, e)
    elif lang != "en":   # en is expected to be a no-op; don't warn for it
        logger.warning(
            "i18n: translations/%s.json not found in any of: %s",
            lang, ", ".join(_candidate_dirs()),
        )
    _maps[lang] = data
    return data


def available_languages() -> dict:
    """Return {code: native_name} for the language switcher."""
    return dict(_AVAILABLE)


def set_language(lang) -> None:
    """Set the active language (falls back to the default if unknown).

    Defensive: QSettings may hand back a non-str / padded value in a frozen
    build, so coerce and strip before validating.
    """
    global _lang
    try:
        lang = str(lang).strip()
    except Exception:
        lang = DEFAULT_LANGUAGE
    if lang not in _AVAILABLE:
        lang = DEFAULT_LANGUAGE
    _lang = lang
    m = _load_map(lang)
    logger.info("i18n: active language=%s (%d translations loaded)", lang, len(m))


def get_language() -> str:
    """Return the active language code (e.g. 'ja' or 'en')."""
    return _lang


def loaded_count(lang: str = "") -> int:
    """How many translations are loaded for a language (diagnostic)."""
    return len(_maps.get(lang or _lang, {}))


def tr(text: str, **kwargs) -> str:
    """Translate an English source string to the active language.

    Unknown strings fall back to the English source, so the UI never breaks.
    Pass keyword args for {placeholder} substitution.
    """
    if _lang == "en":
        s = text
    else:
        s = _load_map(_lang).get(text, text)
    if kwargs:
        try:
            s = s.format(**kwargs)
        except Exception:
            # Malformed placeholder in a translation must never crash the UI.
            pass
    return s
