# PreventCare: Redis-free Open Wearables

This fork (`miagarbaccio-preventcare/open-wearables`) adds an optional
**Postgres ephemeral store** so production can run without Memorystore Redis
(~largest always-on cost for PreventCare’s OW stack).

## What changed

| Concern | Upstream (Redis) | `EPHEMERAL_BACKEND=sql` |
|--------|------------------|-------------------------|
| Sleep session state / locks | Redis keys | `ephemeral_kv` table |
| OAuth CSRF state | Redis | `ephemeral_kv` |
| Sync coordination locks | Redis | `ephemeral_kv` |
| Garmin backfill state | Redis | `ephemeral_kv` |
| Sync-status SSE | Redis lists + pub/sub | `ephemeral_kv` + `ephemeral_pubsub` |
| Celery broker / results | Redis | SQLAlchemy broker + DB result backend |

## Enable

1. Deploy this fork (not `the-momentum/open-wearables`).
2. Run Alembic migrations (creates `ephemeral_kv`, `ephemeral_pubsub`).
3. Set on **API and worker** Cloud Run services:

```bash
EPHEMERAL_BACKEND=sql
# REDIS_* can remain unset / dummy — unused when backend=sql
```

4. Smoke-test HealthKit sleep ingest + daily `syncOwHealthDaily`.
5. Only after green for several days: delete Memorystore `open-wearables-redis`
   and shrink/remove the VPC connector if nothing else needs it.

**Do not delete Redis until step 4 is proven.** Code backup alone does not protect live wearables data paths.

## Deploy from PreventCare iOSApp repo

`gcp/open-wearables/cloudbuild.yaml` should clone:

```text
https://github.com/miagarbaccio-preventcare/open-wearables.git
```

branch/tag: `preventcare/redis-free-ephemeral-store` (or `main` after merge).

## Rollback

Set `EPHEMERAL_BACKEND=redis`, restore Redis host/port secrets, redeploy.
Ephemeral SQL rows are disposable; durable health data remains in Cloud SQL
domain tables and Firestore (PreventCare).

## Limits

- `SqlKvClient` implements only the Redis command subset OW uses.
- Celery over SQLAlchemy is fine for ~small user counts; revisit Cloud Tasks
  if task volume grows.
- SSE pub/sub is poll-based via Postgres (slightly higher latency than Redis).
