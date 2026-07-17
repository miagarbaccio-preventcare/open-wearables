"""Centralized ephemeral store client (Redis or Postgres).

PreventCare: set ``EPHEMERAL_BACKEND=sql`` to run without Memorystore Redis
for application state. Celery broker/result also switch to SQLAlchemy/DB
when sql backend is selected (see ``create_celery``).
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any

import redis

from app.config import settings


@lru_cache()
def get_redis_client() -> Any:
    """Return the process-wide ephemeral client (Redis or SqlKvClient).

    Named ``get_redis_client`` for compatibility with upstream call sites.
    """
    if settings.ephemeral_backend.strip().lower() == "sql":
        from app.database import engine
        from app.integrations.sql_kv_client import SqlKvClient

        return SqlKvClient(engine)

    return redis.from_url(
        settings.redis_url,
        decode_responses=True,
    )


def clear_ephemeral_client_cache() -> None:
    """Test helper — reset the singleton after settings patches."""
    get_redis_client.cache_clear()
