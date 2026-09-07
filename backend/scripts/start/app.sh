#!/bin/bash
# Container start: apply migrations, then serve.
#
# Seeding and the legacy data migrations moved to start/init_data.sh, run by the
# ow-migrate Cloud Run job. They took 36.4s of a measured 42.9s cold start
# (2026-09-07) while FastAPI itself needed only 4.9s, and prevent-api's
# /api/health/connect has a 20s read timeout — so at minScale=0 every cold start
# timed out and reached the app as a 503 with no OW sign-in.
#
# `alembic upgrade head` deliberately stays here: it is 2.3s and it guarantees a
# new revision can never serve against an old schema. Do not move it into the
# job without also making the job run before the service deploy.
set -e -x

# Local compose has no migrate job to run init_data.sh, so keep the full setup
# here. Cold-start latency does not matter locally.
if [ "$ENVIRONMENT" = "local" ]; then
    bash scripts/start/init_data.sh
else
    echo 'Applying migrations...'
    uv run alembic upgrade head
fi

# Init app
echo "Starting the FastAPI application..."
if [ "$ENVIRONMENT" = "local" ]; then
    uv run fastapi dev app/main.py --host 0.0.0.0 --port "${API_PORT:-8000}"
else
    uv run fastapi run app/main.py --host 0.0.0.0 --port "${API_PORT:-8000}"
fi
