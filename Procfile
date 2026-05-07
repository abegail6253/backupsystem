# ⚠️  RATE-LIMITER WARNING: the in-process sliding-window rate limiter keeps its
# state per-worker.  Running more than one worker means each worker enforces the
# limit independently, so the effective limit is (workers × EVENT_RATE_LIMIT).
# For single-machine deployments set --workers 1 (safe default below).
# For multi-worker or multi-instance deployments (Railway autoscale, etc.) you
# MUST replace the in-process store with a shared backend such as Redis — see
# _sliding_window_allow() in backupsys_api.py for the drop-in flask-limiter snippet.
#
# WEB_CONCURRENCY is intentionally hard-coded to 1 below.  Do NOT override it
# with a platform environment variable (e.g. Railway's WEB_CONCURRENCY setting)
# without first wiring up the Redis backend — doing so silently multiplies the
# effective rate limit and allows proportionally more traffic per client.
web: gunicorn backupsys_api:app --bind 0.0.0.0:$PORT --workers 1 --timeout 30 --access-logfile - --error-logfile -