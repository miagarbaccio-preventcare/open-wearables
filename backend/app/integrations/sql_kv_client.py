"""Postgres-backed ephemeral store with a Redis-compatible surface.

PreventCare fork: allows running Open Wearables without Memorystore Redis for
application state (sleep sessions, OAuth CSRF, sync locks, Garmin backfill,
sync-status history). Celery still needs a broker — see ``celery_broker_url``.

Not a full Redis replacement. Implements only the command subset used by this
codebase. Pub/sub uses a pollable ``ephemeral_pubsub`` table (no LISTEN/NOTIFY
dependency) so Cloud Run multi-instance SSE still works.
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

_KIND_STRING = "string"
_KIND_SET = "set"
_KIND_LIST = "list"

# Known compare-and-delete Lua used by sync_coordination / garmin locks.
_RELEASE_LUA_SNIPPET = 'redis.call("get", KEYS[1]) == ARGV[1]'


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _redis_glob_to_like(pattern: str) -> str:
    """Convert a Redis SCAN MATCH glob to SQL LIKE (approx)."""
    out: list[str] = []
    for ch in pattern:
        if ch == "*":
            out.append("%")
        elif ch == "?":
            out.append("_")
        elif ch in ("%","_"):
            out.append("\\" + ch)
        else:
            out.append(ch)
    return "".join(out)


class _SqlLock:
    """Minimal redis.lock()-compatible lock using SET NX + TTL."""

    def __init__(
        self,
        client: "SqlKvClient",
        name: str,
        timeout: float | int = 30,
        blocking_timeout: float | int | None = None,
    ) -> None:
        self._client = client
        self.name = name
        self.timeout = float(timeout)
        self.blocking_timeout = (
            None if blocking_timeout is None else float(blocking_timeout)
        )
        self.local = threading.local()
        self.local.token = None

    def acquire(self, blocking: bool | None = None, blocking_timeout: float | None = None) -> bool:
        if blocking is None:
            blocking = True
        wait = self.blocking_timeout if blocking_timeout is None else blocking_timeout
        token = uuid.uuid4().hex
        deadline = None if wait is None else time.monotonic() + max(0.0, wait)

        while True:
            ok = self._client.set(self.name, token, nx=True, ex=int(self.timeout) or 1)
            if ok:
                self.local.token = token
                return True
            if not blocking:
                return False
            if deadline is not None and time.monotonic() >= deadline:
                return False
            time.sleep(0.05)

    def release(self) -> bool:
        token = getattr(self.local, "token", None)
        if not token:
            return False
        released = self._client.compare_delete(self.name, token)
        self.local.token = None
        return released

    def __enter__(self) -> "_SqlLock":
        if not self.acquire():
            raise TimeoutError(f"Could not acquire lock {self.name}")
        return self

    def __exit__(self, *args: Any) -> None:
        self.release()


class _Pipeline:
    def __init__(self, client: "SqlKvClient") -> None:
        self._client = client
        self._ops: list[tuple[str, tuple, dict]] = []

    def lpush(self, key: str, *values: str) -> "_Pipeline":
        self._ops.append(("lpush", (key, *values), {}))
        return self

    def ltrim(self, key: str, start: int, end: int) -> "_Pipeline":
        self._ops.append(("ltrim", (key, start, end), {}))
        return self

    def expire(self, key: str, seconds: int) -> "_Pipeline":
        self._ops.append(("expire", (key, seconds), {}))
        return self

    def sadd(self, key: str, *members: str) -> "_Pipeline":
        self._ops.append(("sadd", (key, *members), {}))
        return self

    def set(self, key: str, value: str, ex: int | None = None) -> "_Pipeline":
        self._ops.append(("set", (key, value), {"ex": ex}))
        return self

    def publish(self, channel: str, message: str) -> "_Pipeline":
        self._ops.append(("publish", (channel, message), {}))
        return self

    def execute(self) -> list[Any]:
        results = []
        for name, args, kwargs in self._ops:
            results.append(getattr(self._client, name)(*args, **kwargs))
        self._ops.clear()
        return results


class _PubSub:
    def __init__(self, client: "SqlKvClient", ignore_subscribe_messages: bool = False) -> None:
        self._client = client
        self.ignore_subscribe_messages = ignore_subscribe_messages
        self._channels: set[str] = set()
        self._last_id = 0
        self._pending: list[dict[str, Any]] = []

    def subscribe(self, *channels: str) -> None:
        for ch in channels:
            self._channels.add(ch)
            self._pending.append({"type": "subscribe", "channel": ch, "data": 1})
        # Start after current max so we only see new publishes.
        self._last_id = self._client._pubsub_max_id()

    def get_message(
        self,
        ignore_subscribe_messages: bool | None = None,
        timeout: float = 0.0,
    ) -> dict[str, Any] | None:
        ignore = (
            self.ignore_subscribe_messages
            if ignore_subscribe_messages is None
            else ignore_subscribe_messages
        )
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            while self._pending:
                msg = self._pending.pop(0)
                if ignore and msg.get("type") == "subscribe":
                    continue
                return msg
            rows = self._client._pubsub_fetch(self._channels, after_id=self._last_id, limit=50)
            if rows:
                for row_id, channel, payload in rows:
                    self._last_id = max(self._last_id, row_id)
                    self._pending.append(
                        {"type": "message", "channel": channel, "data": payload}
                    )
                continue
            if time.monotonic() >= deadline:
                return None
            time.sleep(min(0.1, max(0.01, deadline - time.monotonic())))

    def close(self) -> None:
        self._channels.clear()
        self._pending.clear()

    def unsubscribe(self, *channels: str) -> None:
        if not channels:
            self._channels.clear()
        else:
            for ch in channels:
                self._channels.discard(ch)


class SqlKvClient:
    """Redis-like client persisted in Postgres ``ephemeral_kv`` / ``ephemeral_pubsub``."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _purge_expired(self, conn: Any, key: str | None = None) -> None:
        if key is None:
            conn.execute(
                text("DELETE FROM ephemeral_kv WHERE expires_at IS NOT NULL AND expires_at < :now"),
                {"now": _utcnow()},
            )
        else:
            conn.execute(
                text(
                    "DELETE FROM ephemeral_kv WHERE key = :key "
                    "AND expires_at IS NOT NULL AND expires_at < :now"
                ),
                {"key": key, "now": _utcnow()},
            )

    def _read_row(self, conn: Any, key: str) -> tuple[str, Any] | None:
        self._purge_expired(conn, key)
        row = conn.execute(
            text("SELECT kind, value_json FROM ephemeral_kv WHERE key = :key"),
            {"key": key},
        ).fetchone()
        if not row:
            return None
        return row[0], row[1]

    def _upsert(
        self,
        conn: Any,
        key: str,
        kind: str,
        value: Any,
        expires_at: datetime | None,
    ) -> None:
        conn.execute(
            text(
                """
                INSERT INTO ephemeral_kv (key, kind, value_json, expires_at, updated_at)
                VALUES (:key, :kind, CAST(:value AS jsonb), :expires_at, :updated_at)
                ON CONFLICT (key) DO UPDATE SET
                    kind = EXCLUDED.kind,
                    value_json = EXCLUDED.value_json,
                    expires_at = EXCLUDED.expires_at,
                    updated_at = EXCLUDED.updated_at
                """
            ),
            {
                "key": key,
                "kind": kind,
                "value": json.dumps(value),
                "expires_at": expires_at,
                "updated_at": _utcnow(),
            },
        )

    def _expires_at(self, seconds: int | None) -> datetime | None:
        if seconds is None:
            return None
        return _utcnow() + timedelta(seconds=int(seconds))

    # ------------------------------------------------------------------
    # String commands
    # ------------------------------------------------------------------

    def get(self, key: str) -> str | None:
        with self._engine.begin() as conn:
            row = self._read_row(conn, key)
            if not row or row[0] != _KIND_STRING:
                return None
            return str(row[1]) if not isinstance(row[1], str) else row[1]

    def set(
        self,
        key: str,
        value: str,
        ex: int | None = None,
        px: int | None = None,
        nx: bool = False,
        xx: bool = False,
        keepttl: bool = False,
    ) -> bool | None:
        if px is not None and ex is None:
            ex = max(1, int(px / 1000))
        with self._engine.begin() as conn:
            self._purge_expired(conn, key)
            existing = conn.execute(
                text("SELECT expires_at FROM ephemeral_kv WHERE key = :key"),
                {"key": key},
            ).fetchone()
            if nx and existing is not None:
                return False
            if xx and existing is None:
                return False
            expires_at = None
            if keepttl and existing is not None:
                expires_at = existing[0]
            elif ex is not None:
                expires_at = self._expires_at(ex)
            self._upsert(conn, key, _KIND_STRING, str(value), expires_at)
            return True

    def setex(self, key: str, time: int, value: str) -> bool:
        return bool(self.set(key, value, ex=int(time)))

    def delete(self, *keys: str) -> int:
        if not keys:
            return 0
        with self._engine.begin() as conn:
            result = conn.execute(
                text("DELETE FROM ephemeral_kv WHERE key = ANY(:keys)"),
                {"keys": list(keys)},
            )
            return result.rowcount or 0

    def expire(self, key: str, seconds: int) -> bool:
        with self._engine.begin() as conn:
            self._purge_expired(conn, key)
            result = conn.execute(
                text(
                    "UPDATE ephemeral_kv SET expires_at = :expires_at, updated_at = :updated_at "
                    "WHERE key = :key"
                ),
                {"key": key, "expires_at": self._expires_at(seconds), "updated_at": _utcnow()},
            )
            return bool(result.rowcount)

    def incr(self, key: str) -> int:
        with self._engine.begin() as conn:
            self._purge_expired(conn, key)
            row = self._read_row(conn, key)
            current = 0
            expires_at = None
            if row and row[0] == _KIND_STRING:
                try:
                    current = int(row[1])
                except (TypeError, ValueError):
                    current = 0
                exp = conn.execute(
                    text("SELECT expires_at FROM ephemeral_kv WHERE key = :key"),
                    {"key": key},
                ).fetchone()
                expires_at = exp[0] if exp else None
            new_val = current + 1
            self._upsert(conn, key, _KIND_STRING, str(new_val), expires_at)
            return new_val

    def mget(self, keys: Iterable[str]) -> list[str | None]:
        key_list = list(keys)
        if not key_list:
            return []
        with self._engine.begin() as conn:
            for k in key_list:
                self._purge_expired(conn, k)
            rows = conn.execute(
                text(
                    "SELECT key, kind, value_json FROM ephemeral_kv WHERE key = ANY(:keys)"
                ),
                {"keys": key_list},
            ).fetchall()
            by_key = {
                r[0]: (str(r[2]) if r[1] == _KIND_STRING else None) for r in rows
            }
            return [by_key.get(k) for k in key_list]

    def compare_delete(self, key: str, expected: str) -> bool:
        """Atomic delete-if-value-matches (Redis Lua release pattern)."""
        with self._engine.begin() as conn:
            self._purge_expired(conn, key)
            row = self._read_row(conn, key)
            if not row or row[0] != _KIND_STRING:
                return False
            current = str(row[1])
            if current != expected:
                return False
            conn.execute(text("DELETE FROM ephemeral_kv WHERE key = :key"), {"key": key})
            return True

    def eval(self, script: str, numkeys: int, *keys_and_args: str) -> int:
        """Support the compare-and-delete Lua used for lock release."""
        compact = re.sub(r"\s+", "", script)
        supported = (
            'redis.call("get",KEYS[1])==ARGV[1]' in compact
            or "redis.call('get',KEYS[1])==ARGV[1]" in compact
            or _RELEASE_LUA_SNIPPET.replace(" ", "") in compact
        )
        if not supported:
            raise NotImplementedError(
                "SqlKvClient.eval only supports compare-and-delete lock release scripts"
            )
        keys = list(keys_and_args[:numkeys])
        args = list(keys_and_args[numkeys:])
        if len(keys) != 1 or len(args) != 1:
            raise ValueError("compare-and-delete eval expects 1 key and 1 arg")
        return 1 if self.compare_delete(keys[0], args[0]) else 0

    # ------------------------------------------------------------------
    # Set commands
    # ------------------------------------------------------------------

    def sadd(self, key: str, *members: str) -> int:
        with self._engine.begin() as conn:
            self._purge_expired(conn, key)
            row = self._read_row(conn, key)
            current: set[str] = set()
            expires_at = None
            if row and row[0] == _KIND_SET:
                current = set(row[1] or [])
                exp = conn.execute(
                    text("SELECT expires_at FROM ephemeral_kv WHERE key = :key"),
                    {"key": key},
                ).fetchone()
                expires_at = exp[0] if exp else None
            before = len(current)
            current.update(str(m) for m in members)
            self._upsert(conn, key, _KIND_SET, sorted(current), expires_at)
            return len(current) - before

    def srem(self, key: str, *members: str) -> int:
        with self._engine.begin() as conn:
            self._purge_expired(conn, key)
            row = self._read_row(conn, key)
            if not row or row[0] != _KIND_SET:
                return 0
            current = set(row[1] or [])
            before = len(current)
            current.difference_update(str(m) for m in members)
            exp = conn.execute(
                text("SELECT expires_at FROM ephemeral_kv WHERE key = :key"),
                {"key": key},
            ).fetchone()
            expires_at = exp[0] if exp else None
            if current:
                self._upsert(conn, key, _KIND_SET, sorted(current), expires_at)
            else:
                conn.execute(text("DELETE FROM ephemeral_kv WHERE key = :key"), {"key": key})
            return before - len(current)

    def smembers(self, key: str) -> set[str]:
        with self._engine.begin() as conn:
            row = self._read_row(conn, key)
            if not row or row[0] != _KIND_SET:
                return set()
            return {str(m) for m in (row[1] or [])}

    # ------------------------------------------------------------------
    # List commands
    # ------------------------------------------------------------------

    def lpush(self, key: str, *values: str) -> int:
        with self._engine.begin() as conn:
            self._purge_expired(conn, key)
            row = self._read_row(conn, key)
            items: list[str] = []
            expires_at = None
            if row and row[0] == _KIND_LIST:
                items = list(row[1] or [])
                exp = conn.execute(
                    text("SELECT expires_at FROM ephemeral_kv WHERE key = :key"),
                    {"key": key},
                ).fetchone()
                expires_at = exp[0] if exp else None
            for v in values:
                items.insert(0, str(v))
            self._upsert(conn, key, _KIND_LIST, items, expires_at)
            return len(items)

    def ltrim(self, key: str, start: int, end: int) -> bool:
        with self._engine.begin() as conn:
            self._purge_expired(conn, key)
            row = self._read_row(conn, key)
            if not row or row[0] != _KIND_LIST:
                return True
            items = list(row[1] or [])
            # Redis end is inclusive; -1 means last element.
            if end == -1:
                end = len(items) - 1
            trimmed = items[start : end + 1] if items else []
            exp = conn.execute(
                text("SELECT expires_at FROM ephemeral_kv WHERE key = :key"),
                {"key": key},
            ).fetchone()
            expires_at = exp[0] if exp else None
            self._upsert(conn, key, _KIND_LIST, trimmed, expires_at)
            return True

    def lrange(self, key: str, start: int, end: int) -> list[str]:
        with self._engine.begin() as conn:
            row = self._read_row(conn, key)
            if not row or row[0] != _KIND_LIST:
                return []
            items = [str(x) for x in (row[1] or [])]
            if end == -1:
                end = len(items) - 1
            return items[start : end + 1]

    # ------------------------------------------------------------------
    # Scan / pubsub / lock / pipeline
    # ------------------------------------------------------------------

    def scan(
        self,
        cursor: int = 0,
        match: str | None = None,
        count: int = 100,
    ) -> tuple[int, list[str]]:
        like = _redis_glob_to_like(match) if match else "%"
        offset = int(cursor)
        with self._engine.begin() as conn:
            self._purge_expired(conn)
            rows = conn.execute(
                text(
                    """
                    SELECT key FROM ephemeral_kv
                    WHERE key LIKE :like ESCAPE '\\'
                    ORDER BY key
                    LIMIT :limit OFFSET :offset
                    """
                ),
                {"like": like, "limit": int(count), "offset": offset},
            ).fetchall()
            keys = [r[0] for r in rows]
            next_cursor = 0 if len(keys) < count else offset + len(keys)
            return next_cursor, keys

    def publish(self, channel: str, message: str) -> int:
        with self._engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO ephemeral_pubsub (channel, payload, created_at)
                    VALUES (:channel, :payload, :created_at)
                    """
                ),
                {"channel": channel, "payload": message, "created_at": _utcnow()},
            )
            # Opportunistic GC: keep last ~24h
            conn.execute(
                text(
                    "DELETE FROM ephemeral_pubsub WHERE created_at < :cutoff"
                ),
                {"cutoff": _utcnow() - timedelta(hours=24)},
            )
        return 1

    def _pubsub_max_id(self) -> int:
        with self._engine.begin() as conn:
            row = conn.execute(text("SELECT COALESCE(MAX(id), 0) FROM ephemeral_pubsub")).fetchone()
            return int(row[0]) if row else 0

    def _pubsub_fetch(
        self, channels: set[str], after_id: int, limit: int = 50
    ) -> list[tuple[int, str, str]]:
        if not channels:
            return []
        with self._engine.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT id, channel, payload FROM ephemeral_pubsub
                    WHERE id > :after_id AND channel = ANY(:channels)
                    ORDER BY id ASC
                    LIMIT :limit
                    """
                ),
                {
                    "after_id": after_id,
                    "channels": list(channels),
                    "limit": limit,
                },
            ).fetchall()
            return [(int(r[0]), str(r[1]), str(r[2])) for r in rows]

    def pubsub(self, ignore_subscribe_messages: bool = False) -> _PubSub:
        return _PubSub(self, ignore_subscribe_messages=ignore_subscribe_messages)

    def lock(
        self,
        name: str,
        timeout: float | int = 30,
        blocking_timeout: float | int | None = None,
        **_kwargs: Any,
    ) -> _SqlLock:
        return _SqlLock(self, name, timeout=timeout, blocking_timeout=blocking_timeout)

    def pipeline(self, transaction: bool = True) -> _Pipeline:
        return _Pipeline(self)
