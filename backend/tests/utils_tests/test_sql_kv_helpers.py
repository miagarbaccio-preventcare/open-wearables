"""Unit tests for SqlKvClient helpers and lock-release eval support."""

from __future__ import annotations

import re

from app.integrations.sql_kv_client import _RELEASE_LUA_SNIPPET, _redis_glob_to_like


class TestRedisGlobToLike:
    def test_star_and_question(self) -> None:
        assert _redis_glob_to_like("sync:status:user:*:runs") == "sync:status:user:%:runs"
        assert _redis_glob_to_like("sleep:?") == "sleep:_"

    def test_escapes_like_metacharacters(self) -> None:
        assert _redis_glob_to_like("a%b_c") == r"a\%b\_c"


class TestReleaseLuaDetection:
    def test_upstream_script_matches(self) -> None:
        script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        compact = re.sub(r"\s+", "", script)
        assert 'redis.call("get",KEYS[1])==ARGV[1]' in compact
        assert _RELEASE_LUA_SNIPPET.replace(" ", "") in compact
