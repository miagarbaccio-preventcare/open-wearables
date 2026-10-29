"""The upload trigger starts one drain job per lock window, and never raises."""

from __future__ import annotations

from app.services.sdk_drain_trigger import LOCK_KEY, start_sdk_drain


class _Kv:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}

    def set(self, key: str, value: str, nx: bool = False, ex: int | None = None) -> bool:
        if nx and key in self.store:
            return False
        self.store[key] = value
        return True

    def delete(self, key: str) -> int:
        self.store.pop(key, None)
        return 1


def test_two_uploads_inside_the_lock_start_one_job() -> None:
    kv = _Kv()
    starts: list[int] = []

    assert start_sdk_drain(kv, lambda: starts.append(1)) is True
    assert start_sdk_drain(kv, lambda: starts.append(1)) is False

    assert starts == [1]
    assert LOCK_KEY in kv.store


def test_a_failed_start_drops_the_lock_so_the_next_upload_can_retry() -> None:
    kv = _Kv()

    def boom() -> None:
        raise RuntimeError("down")

    assert start_sdk_drain(kv, boom) is False
    assert LOCK_KEY not in kv.store
    assert start_sdk_drain(kv, lambda: None) is True
