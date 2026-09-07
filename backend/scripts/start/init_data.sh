#!/bin/bash
# One-shot database setup: migrations, seeding, and legacy data migrations.
#
# Split out of start/app.sh, which used to run all of this on every container
# start. Measured on Cloud Run at minScale=0 (2026-09-07): these steps took
# 36.4s of a 42.9s cold start, and FastAPI itself only 4.9s. The caller
# (prevent-api /api/health/connect) has a 20s read timeout, so every cold start
# deterministically timed out and surfaced to the app as a 503 — no OW sign-in.
#
# app.sh still applies migrations, so the schema is always current before the
# container serves. Everything here is idempotent and safe to re-run.
set -e -x

# Ensure svix database exists (idempotent)
echo 'Ensuring svix database...'
uv run python scripts/init/create_svix_db.py

# Init database
echo 'Applying migrations...'
uv run alembic upgrade head

# Initialize provider settings
echo 'Initializing provider settings...'
uv run python scripts/init_provider_settings.py

# Initialize device priority table
echo 'Initializing priorities...'
uv run python scripts/init_device_priorities.py

# Seed admin account (uses ADMIN_EMAIL/ADMIN_PASSWORD env vars, or defaults)
echo 'Seeding admin account...'
uv run python scripts/init/seed_admin.py

# Initialize series type definitions
echo 'Initializing series type definitions...'
uv run python scripts/init/seed_series_types.py

# TODO: Remove this after ~2026-06-01 once all deployments have migrated.
# Drops legacy recovery_score timeseries data; no-op if already cleaned up.
echo 'Running recovery_score series type cleanup...'
uv run python scripts/data_migrations/drop_recovery_score_series_type.py \
    || echo "Warning: recovery_score cleanup failed — will retry on next run."

# TODO: Remove this after ~2026-06-01 once all deployments have migrated.
# Divides body_fat_percentage values stored 100x too large (Samsung/Google bug, PR #917); no-op if already corrected.
echo 'Running body_fat_percentage normalization...'
uv run python scripts/data_migrations/normalize_body_fat_percentage.py \
    || echo "Warning: body_fat_percentage normalization failed — will retry on next run."

# TODO: Remove this after ~2026-09-01 once all deployments have migrated.
# Relabels Oura HRV stored as SDNN (id=3) to RMSSD (id=7); scoped to provider='oura', no-op once corrected.
echo 'Running Oura HRV SDNN->RMSSD relabel...'
uv run python scripts/data_migrations/relabel_oura_hrv_sdnn_to_rmssd.py \
    || echo "Warning: Oura HRV relabel failed — will retry on next run."

# TODO: Remove this after ~2026-08-01 once all deployments have migrated.
# Nulls workout heart_rate_min values copied from average HR (Garmin pre-#1121, Polar pre-#1041); no-op once cleaned.
echo 'Running workout heart_rate_min cleanup...'
uv run python scripts/data_migrations/null_bogus_workout_heart_rate_min.py \
    || echo "Warning: workout heart_rate_min cleanup failed - will retry on next run."

# TODO: Remove this after ~2026-08-01 once all deployments have migrated.
# Multiplies Apple HealthKit walking metrics stored as fractions/meters (issues #1105, #1106); no-op if already corrected.
echo 'Running Apple walking metrics normalization...'
uv run python scripts/data_migrations/normalize_apple_walking_metrics.py \
    || echo "Warning: Apple walking metrics normalization failed — will retry on next run."

# TODO: Remove this after ~2026-09-01 once all deployments have migrated.
# Labels is_daily_total on archival data_point_series (daily totals → TRUE); idempotent,
# only flips NULL rows, batched. After the first full pass, re-runs are no-ops.
echo 'Running is_daily_total backfill...'
uv run python scripts/data_migrations/backfill_is_daily_total.py \
    || echo "Warning: is_daily_total backfill failed — will retry on next run."

# Initialize archival settings
echo 'Initializing archival settings...'
uv run python scripts/init/seed_archival_settings.py

# Register webhook event types with Svix (with retry, non-fatal)
echo 'Registering webhook event types...'
for i in 1 2 3; do
    uv run python scripts/init/seed_webhook_event_types.py && break
    echo "Svix not ready yet, retrying in 5s... (attempt ${i}/3)"
    sleep 5
done || echo "Warning: Could not register webhook event types with Svix. Will retry on next run."

echo 'Database setup complete.'
