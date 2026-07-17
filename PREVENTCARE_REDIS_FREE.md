# PreventCare: Redis-free Open Wearables

This fork (`miagarbaccio-preventcare/open-wearables`) adds an optional
**Postgres ephemeral store** so production can run without Memorystore Redis
(~largest always-on cost for PreventCare’s OW stack).

## What changed

| Concern | Upstream (Redis) | Hybrid (current) | Full SQL (future) |
|--------|------------------|------------------|-------------------|
| Sleep / OAuth / locks / sync-status | Redis | `EPHEMERAL_BACKEND=sql` → `ephemeral_kv` | same |
| Celery broker / results | Redis | `CELERY_BROKER_BACKEND=redis` | `CELERY_BROKER_BACKEND=sql` |
| Delete Memorystore | — | **not yet** | after Celery SQL proven |

## Enable (hybrid — safe cutover)

1. Deploy this fork (not `the-momentum/open-wearables`).
2. Run Alembic migrations (creates `ephemeral_kv`, `ephemeral_pubsub`).
3. Set on **API and worker** Cloud Run services:

```bash
EPHEMERAL_BACKEND=sql
CELERY_BROKER_BACKEND=redis
```

4. Smoke-test HealthKit sleep ingest + daily `syncOwHealthDaily`.
5. Later: set `CELERY_BROKER_BACKEND=sql`, confirm Celery beat/worker logs, then
   delete Memorystore `open-wearables-redis`.

**Do not delete Redis until Celery is on SQL and proven.**

## Deploy from PreventCare iOSApp repo

`gcp/open-wearables/cloudbuild.yaml` should clone:

```text
https://github.com/miagarbaccio-preventcare/open-wearables.git
```

branch/tag: `preventcare/redis-free-ephemeral-store` (or `main` after merge).

## Rollback

Set `EPHEMERAL_BACKEND=redis` (and keep `CELERY_BROKER_BACKEND=redis`), redeploy.
Ephemeral SQL rows are disposable; durable health data remains in Cloud SQL
domain tables and Firestore (PreventCare).

## Limits

- `SqlKvClient` implements only the Redis command subset OW uses.
- Celery over SQLAlchemy still needs production validation before Memorystore removal.
- SSE pub/sub is poll-based via Postgres when ephemeral is SQL.
