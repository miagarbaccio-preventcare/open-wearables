"""Daily data retention Celery task.

Runs once per day (via beat schedule) to delete data older than
delete_after_days, configured by the admin via archival_settings. If that is
NULL the task is a no-op. Archival is refused — see
``ARCHIVAL_UNSUPPORTED_DETAIL`` in ``archival_service``.
"""

from logging import getLogger

from celery import shared_task

from app.database import SessionLocal
from app.services.archival_service import archival_service

logger = getLogger(__name__)


@shared_task(
    name="app.integrations.celery.tasks.archival_task.run_daily_archival",
    soft_time_limit=300,  # 5 min soft limit — raises SoftTimeLimitExceeded
    time_limit=360,  # 6 min hard limit — kills the worker
    acks_late=True,  # Re-queue if worker dies mid-task
    max_retries=3,
    default_retry_delay=60,
)
def run_daily_archival() -> dict:
    """Execute the daily retention job.

    The service internally limits batch size and wall time so this task
    is safe for first-run scenarios with millions of rows. If not all rows
    are processed, the next scheduled invocation continues automatically.
    """
    with SessionLocal() as db:
        try:
            summary = archival_service.run_daily_archival(db)
            logger.info("Daily retention completed: %s", summary)
            return summary
        except Exception:
            logger.exception("Daily archival task failed")
            raise
