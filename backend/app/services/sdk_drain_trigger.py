"""Start the HealthKit drain job when an upload arrives (ADR 0010).

A short lock keeps a burst of batches from starting a burst of jobs. The
trigger never raises: a Google outage must not fail the upload. The hourly
scheduler is the backstop when a start is missed.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from typing import Any

import httpx

from app.integrations.redis_client import get_redis_client

logger = logging.getLogger(__name__)

LOCK_KEY = "ow:sdk-drain-trigger"
LOCK_SECONDS = 60
DEFAULT_JOB = "open-wearables-sdk-drain"
DEFAULT_REGION = "us-central1"


def start_sdk_drain(kv: Any, runner: Callable[[], None]) -> bool:
    """Claim the lock and start one job. False means skipped or the start failed."""
    try:
        claimed = bool(kv.set(LOCK_KEY, "1", nx=True, ex=LOCK_SECONDS))
    except Exception:
        logger.warning("event=sdk_drain_lock_failed")
        return False
    if not claimed:
        return False
    try:
        runner()
    except Exception:
        logger.warning("event=sdk_drain_trigger_failed")
        try:
            kv.delete(LOCK_KEY)
        except Exception:
            logger.warning("event=sdk_drain_lock_release_failed")
        return False
    return True


def run_cloud_run_job() -> None:
    project = os.environ.get("GOOGLE_CLOUD_PROJECT", "").strip()
    if not project:
        raise RuntimeError("missing project")
    region = os.environ.get("CLOUD_RUN_REGION", DEFAULT_REGION).strip() or DEFAULT_REGION
    job = os.environ.get("SDK_DRAIN_JOB", DEFAULT_JOB).strip() or DEFAULT_JOB
    token = _metadata_token()
    url = f"https://run.googleapis.com/v2/projects/{project}/locations/{region}/jobs/{job}:run"
    response = httpx.post(
        url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=5.0,
    )
    if response.status_code >= 300:
        raise RuntimeError("job run failed")


def _metadata_token() -> str:
    response = httpx.get(
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
        headers={"Metadata-Flavor": "Google"},
        timeout=2.0,
    )
    response.raise_for_status()
    token = response.json().get("access_token")
    if not isinstance(token, str) or not token:
        raise RuntimeError("missing token")
    return token


def trigger_sdk_drain() -> None:
    try:
        start_sdk_drain(get_redis_client(), run_cloud_run_job)
    except Exception:
        logger.warning("event=sdk_drain_trigger_failed")
