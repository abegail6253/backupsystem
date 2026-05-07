# Security Policy

## Supported versions

| Version | Supported |
|---------|-----------|
| 1.1.x   | ✅ Yes     |
| < 1.1   | ❌ No      |

Only the latest minor release receives security fixes.

---

## Reporting a vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Please report security issues by emailing **security@example.com** (replace
with your actual address before publishing).  Include:

- A description of the vulnerability and its potential impact
- Steps to reproduce or a proof-of-concept (redact any real credentials)
- BackupSys version, OS, and Python version
- Whether you believe a fix is straightforward

You should receive an acknowledgement within **48 hours** and a resolution
timeline within **7 days**.  We will credit you in the release notes unless
you prefer to remain anonymous.

---

## Security model

### API authentication

Every request to `backupsys_api.py` must carry an `X-BackupSys-Signature`
header containing `HMAC-SHA256(API_KEY, request_body).hex()`.  The API key is
**never transmitted**; only its HMAC output travels over the wire.

**Minimum key length:** 32 random characters.
Generate one with:

```bash
python -c "import secrets; print(secrets.token_hex(32))"
```

### Encryption keys

Backup archives are encrypted with AES-256-GCM (via the `cryptography` library).
Encryption keys are derived from a passphrase using Argon2id and stored in the
OS keyring (`keyring` library) wherever possible — never in `config.json` in
plaintext.

### Credential storage

SFTP, FTP, SMB, and SMTP passwords are stored using the OS keyring backend:

| Platform | Backend                    |
|----------|----------------------------|
| Windows  | Windows Credential Manager |
| macOS    | macOS Keychain             |
| Linux    | SecretService (GNOME / KWallet) |

If the keyring is unavailable, credentials fall back to `config.json` with a
warning logged.  **Never commit `config.json` to version control.**

### Dashboard session security

The web dashboard uses an HTTP-only signed session cookie (Flask `secret_key`).
Set `BACKUPSYS_SESSION_SECRET` to a long random value in production so sessions
survive server restarts.  Without it, a new random key is generated on each
start and all active sessions are invalidated.

### Rate limiting

The `/backup/event` endpoint enforces a sliding-window rate limit on both the
client IP and the `machine_id` field in the request body.  The OTP flow adds a
5-attempt lockout to defend against brute-force guessing.

### Known limitations / out of scope

- The in-process rate limiter is per-worker; multi-worker deployments (multiple
  gunicorn workers) should replace it with a shared Redis counter.
- CORS defaults to `*` because HMAC signing is the enforcement layer.
  Set `ALLOWED_ORIGINS` to tighten browser-level enforcement if desired.
- The desktop app does not (yet) verify the server's TLS certificate by default
  when `verify_ssl` is `false` in the config.  Enable certificate verification
  for production deployments.

---

## Disclosure policy

We follow [responsible disclosure](https://en.wikipedia.org/wiki/Responsible_disclosure).
Once a fix is released we will publish a security advisory on GitHub describing
the vulnerability, its impact, and the fix.  Embargoed details will not be
shared before the fix is available.
