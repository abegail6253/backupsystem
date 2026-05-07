# ─── BackupSys API Server — Dockerfile ───────────────────────────────────────
#
# Builds a self-contained image for backupsys_api.py.
# Suitable for local dev, VPS deployment, or any OCI-compatible registry.
#
# Quick start (local):
#   docker build -t backupsys-api .
#   docker run -p 5000:5000 \
#     -e BACKUPSYS_API_KEY=<your-key> \
#     -e BACKUPSYS_DASHBOARD_PASSWORD=<your-pw> \
#     -e BACKUPSYS_SESSION_SECRET=<random-secret> \
#     backupsys-api
#
# For persistent storage mount /data:
#   docker run -p 5000:5000 -v $(pwd)/data:/data \
#     -e BACKUPSYS_DB_PATH=/data/backupsys.db \
#     -e BACKUPSYS_FILES_DIR=/data/files \
#     ... backupsys-api

# ── Build stage: install deps into a clean layer ──────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

COPY requirements_api.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements_api.txt


# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM python:3.12-slim

LABEL org.opencontainers.image.title="BackupSys API"
LABEL org.opencontainers.image.description="Lightweight Flask API for BackupSys remote notifications"

# Copy pre-built packages from builder
COPY --from=builder /install /usr/local

# Create a non-root user for the process
RUN useradd --no-create-home --shell /bin/false backupsys

WORKDIR /app

# Copy only the files the server needs at runtime
COPY backupsys_api.py .

# Data directory: DB and uploaded files land here.
# Mount a volume over /data to persist across container restarts.
RUN mkdir /data && chown backupsys:backupsys /data

USER backupsys

# ── Environment ───────────────────────────────────────────────────────────────
# All secrets must be injected at runtime — never bake them into the image.
ENV PORT=5000 \
    BACKUPSYS_DB_PATH=/data/backupsys.db \
    BACKUPSYS_FILES_DIR=/data/files \
    LOG_LEVEL=INFO

EXPOSE 5000

# ── Health check ──────────────────────────────────────────────────────────────
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python - <<'EOF'
import urllib.request, sys
try:
    urllib.request.urlopen("http://localhost:5000/health", timeout=4)
    sys.exit(0)
except Exception:
    sys.exit(1)
EOF

# ── Entrypoint ────────────────────────────────────────────────────────────────
# Mirrors the Procfile exactly; --workers 2 is safe for a single-tenant API.
CMD ["gunicorn", "backupsys_api:app", \
     "--bind", "0.0.0.0:5000", \
     "--workers", "2", \
     "--timeout", "30", \
     "--access-logfile", "-", \
     "--error-logfile", "-"]