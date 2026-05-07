# Contributing to BackupSys

Thanks for your interest in contributing!  This document explains how to get
set up, what the project conventions are, and how to submit changes.

---

## Table of contents

1. [Getting started](#getting-started)
2. [Running the tests](#running-the-tests)
3. [Code style](#code-style)
4. [Submitting a pull request](#submitting-a-pull-request)
5. [Reporting bugs](#reporting-bugs)

---

## Getting started

```bash
# 1. Fork and clone
git clone https://github.com/your-fork/backupsys.git
cd backupsys

# 2. Create a virtual environment
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

# 3. Install all dependencies (desktop + API + dev tools)
pip install -r requirements_desktop.txt
pip install -r requirements_api.txt
pip install pytest pytest-cov

# 4. Copy the example env and fill in values
cp .env.example .env
```

> **WebDAV note:** `webdavclient3` is optional.  The test suite and the core
> backup engine work without it.  Install it only if you need full Nextcloud /
> ownCloud compatibility (`pip install "webdavclient3>=3.14.6"`).

---

## Running the tests

```bash
# All tests with coverage
pytest --cov=. --cov-report=term-missing

# A single module
pytest tests/test_backup_engine.py -v

# Fast smoke-run (skip slow integration tests)
pytest -m "not integration" -v
```

The CI pipeline (`.github/workflows/ci.yml`) runs the full suite automatically
on every push and pull request.  Check that it passes locally before opening a
PR.

---

## Code style

- **Python 3.11+** — use modern type hints (`str | None`, `list[str]`, etc.).
- **PEP 8** — 100-character line limit.  Run `ruff check .` or `flake8` before
  committing.
- **Docstrings** — public functions and classes must have a one-line summary.
  Multi-paragraph docstrings follow the Google style.
- **Type hints** — all new public functions must be fully annotated.
- **No bare `except`** — always catch specific exception types.

---

## Submitting a pull request

1. Create a feature branch off `main`:  `git checkout -b feat/my-change`
2. Make your changes with focused commits.
3. Add or update tests so coverage does not drop.
4. Update `CHANGELOG.md` under the `[Unreleased]` section.
5. Open a PR against `main` with a clear description of *what* and *why*.

### PR checklist

- [ ] Tests pass (`pytest --cov`)
- [ ] No new linting errors
- [ ] `CHANGELOG.md` updated
- [ ] Sensitive changes (crypto, auth, key storage) noted in the PR description

---

## Reporting bugs

Open a [GitHub Issue](../../issues/new) with:

- BackupSys version (`python backupsys_cli.py --version`)
- OS and Python version
- Steps to reproduce
- Expected vs. actual behaviour
- Relevant log output (redact any API keys or passwords)

For security-sensitive issues please follow the process in
[SECURITY.md](SECURITY.md) instead of opening a public issue.
