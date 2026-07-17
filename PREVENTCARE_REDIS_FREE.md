# PreventCare: Redis-free Open Wearables

This fork (`miagarbaccio-preventcare/open-wearables`) adds an optional
**Postgres ephemeral store** so production can run without Memorystore Redis
(~largest always-on cost for PreventCare’s OW stack).

## Production cutover (2026-07-17)

| Concern | Setting | Backend |
|--------|---------|---------|
| Sleep / OAuth / locks / sync-status | `EPHEMERAL_BACKEND=sql` | `ephemeral_kv` / `ephemeral_pubsub` |
| Celery broker / results | `CELERY_BROKER_BACKEND=sql` | `sqla+postgresql` / `db+postgresql` |
| Worker CPU | `cpu-throttling: false` | Required — throttling freezes Celery |
| Memorystore | still provisioned | Delete only after soak test |

Verified: Celery connected to Cloud SQL; `process_sdk_upload` succeeded.

## Enable

```bash
EPHEMERAL_BACKEND=sql
CELERY_BROKER_BACKEND=sql
# Worker must NOT use CPU throttling
```

## Rollback

```bash
EPHEMERAL_BACKEND=redis
CELERY_BROKER_BACKEND=redis
```

Then redeploy. Ephemeral SQL rows are disposable.

## Delete Memorystore (after soak)

Only after several days of healthy sync:

1. Confirm no Redis traffic / no regressions.
2. Explicit approval to delete `open-wearables-redis`.
3. Optionally shrink/remove VPC connector if unused.

## Deploy from PreventCare iOSApp repo

`gcp/open-wearables/cloudbuild.yaml` clones:

```text
https://github.com/miagarbaccio-preventcare/open-wearables.git
```

branch: `preventcare/redis-free-ephemeral-store`
