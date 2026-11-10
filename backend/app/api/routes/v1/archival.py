"""API endpoints for data lifecycle / archival settings.

Admin-only endpoints to configure when old data is permanently deleted. Archival
itself is refused — see ``ARCHIVAL_UNSUPPORTED_DETAIL`` in ``archival_service``.
"""

from logging import getLogger

from fastapi import APIRouter, status

from app.database import DbSession
from app.integrations.celery.tasks.archival_task import run_daily_archival
from app.schemas.utils import ArchivalSettingUpdate, ArchivalSettingWithEstimate
from app.services import DeveloperDep
from app.services.archival_service import archival_service

router = APIRouter()
logger = getLogger(__name__)


@router.get(
    "/settings/archival",
    status_code=status.HTTP_200_OK,
    summary="Get data lifecycle settings",
    description="Returns current archival/retention configuration and storage size estimates.",
)
def get_archival_settings(
    db: DbSession,
    _developer: DeveloperDep,
) -> ArchivalSettingWithEstimate:
    return archival_service.get_settings(db)


@router.put(
    "/settings/archival",
    status_code=status.HTTP_200_OK,
    summary="Update data lifecycle settings",
    description=(
        "Configure delete_after_days (when old data is permanently removed); "
        "null keeps data indefinitely. archive_after_days is rejected with 400 — "
        "aggregating samples into daily rows discards the intraday data the read "
        "paths depend on."
    ),
)
def update_archival_settings(
    db: DbSession,
    _developer: DeveloperDep,
    update: ArchivalSettingUpdate,
) -> ArchivalSettingWithEstimate:
    return archival_service.update_settings(db, update)


@router.post(
    "/settings/archival/run",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Trigger the lifecycle job manually",
    description=(
        "Dispatches the daily retention job via Celery. Returns immediately with the task ID. "
        "Archival is refused, so this only applies delete_after_days."
    ),
)
def trigger_archival(
    _developer: DeveloperDep,
) -> dict[str, str]:
    result = run_daily_archival.delay()
    return {"task_id": result.id, "status": "dispatched"}
