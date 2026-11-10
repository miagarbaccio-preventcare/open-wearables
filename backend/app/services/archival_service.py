"""Service for data lifecycle management (archival + retention)."""

from __future__ import annotations

from datetime import date, timedelta
from logging import Logger, getLogger

from fastapi import HTTPException

from app.database import DbSession
from app.models.archival_setting import ArchivalSetting
from app.repositories.archival_repository import (
    ArchivalSettingRepository,
    DataPointSeriesArchiveRepository,
)
from app.schemas.utils import (
    ArchivalSettingRead,
    ArchivalSettingUpdate,
    ArchivalSettingWithEstimate,
    StorageEstimate,
)
from app.utils.exceptions import handle_exceptions
from app.utils.structured_logging import log_structured

# Archival is refused rather than merely left unset.
#
# It collapses each (source, type, UTC date) into one row and deletes the intraday
# rows it summarised. The reads are built on those rows: hourly breakdowns and
# active/intensity minutes are derived from them, and a consumer that bins by the
# user's local day cannot place a UTC-dated daily aggregate. An archived day comes
# back on the wrong local day with one value where there were twenty-four, and a
# consumer tracking per-day coverage then settles it as complete, so nothing
# re-fetches it. Sparing the types that matter would spare heart rate and steps,
# which is where the volume is, leaving archival nothing to compact.
#
# Retention (delete_after_days) still runs: dropping data past a stated horizon is
# a policy decision, not a silent lossy rewrite.
ARCHIVAL_UNSUPPORTED_DETAIL = (
    "Archival is not supported: collapsing samples into daily rows discards the "
    "intraday data the read paths depend on. Use delete_after_days for retention."
)


class ArchivalService:
    """Orchestrates lifecycle settings CRUD, storage estimates, and the daily retention job."""

    def __init__(self, log: Logger):
        self.logger = log
        self.settings_repo = ArchivalSettingRepository()
        self.archive_repo = DataPointSeriesArchiveRepository()

    # ── Settings CRUD ─────────────────────────────────────────────

    @handle_exceptions
    def get_settings(self, db: DbSession) -> ArchivalSettingWithEstimate:
        """Return current archival configuration together with table-size estimates."""
        setting = self.settings_repo.get(db)
        storage = self._get_storage(db, setting)
        return ArchivalSettingWithEstimate(
            settings=ArchivalSettingRead.model_validate(setting),
            storage=storage,
        )

    @handle_exceptions
    def update_settings(self, db: DbSession, update: ArchivalSettingUpdate) -> ArchivalSettingWithEstimate:
        """Persist new archival settings and return updated estimates.

        Rejects any ``archive_after_days`` instead of storing a value the job will
        refuse to act on, so the caller is told now rather than discovering later
        that the setting reads back as enabled while nothing archives.
        """
        if update.archive_after_days is not None:
            raise HTTPException(status_code=400, detail=ARCHIVAL_UNSUPPORTED_DETAIL)

        setting = self.settings_repo.update(db, update.archive_after_days, update.delete_after_days)
        storage = self._get_storage(db, setting)
        return ArchivalSettingWithEstimate(
            settings=ArchivalSettingRead.model_validate(setting),
            storage=storage,
        )

    # ── Daily job (called by Celery) ──────────────────────────────

    def run_daily_archival(self, db: DbSession) -> dict:
        """Apply the retention half of the data lifecycle.

        Archival is refused (see ``ARCHIVAL_UNSUPPORTED_DETAIL``), so the only
        policy acted on is ``delete_after_days``: live rows past the horizon are
        deleted outright, along with any archive rows an earlier configuration
        left behind. ``archived_rows`` stays in the summary so the shape callers
        and log queries already match does not change.

        Returns a summary dict for logging/monitoring.
        """
        setting = self.settings_repo.get(db)
        summary: dict = {"archived_rows": 0, "deleted_rows": 0, "deleted_live_rows": 0}

        # Reachable only for a row written before the refusal or straight to the
        # table. Report it and carry on — the rows it would have collapsed stay
        # live, so retention below still reaches them.
        if setting.archive_after_days is not None:
            log_structured(
                self.logger,
                "warning",
                "Archival: refused — archive_after_days is set but archival is unsupported",
                archive_after_days=setting.archive_after_days,
            )

        delete_days = setting.delete_after_days
        if delete_days is None:
            return summary

        cutoff = date.today() - timedelta(days=delete_days)
        log_structured(
            self.logger,
            "info",
            f"Retention: deleting live rows before {cutoff}",
            delete_after_days=delete_days,
            cutoff_date=str(cutoff),
        )
        deleted = self.archive_repo.delete_live_before(db, cutoff)
        summary["deleted_live_rows"] = deleted
        log_structured(
            self.logger,
            "info",
            f"Retention: deleted {deleted} live rows",
            deleted_live_rows=deleted,
        )

        deleted_archive = self.archive_repo.delete_archive_before(db, cutoff)
        if deleted_archive:
            summary["deleted_rows"] = deleted_archive

        return summary

    # ── Private helpers ───────────────────────────────────────────

    def _get_storage(self, db: DbSession, setting: ArchivalSetting) -> StorageEstimate:
        raw = self.archive_repo.get_storage_estimate(db)

        if setting.delete_after_days is not None:
            growth_class = "bounded"
        elif setting.archive_after_days is not None:
            growth_class = "linear_efficient"
        else:
            growth_class = "linear"

        raw["growth_class"] = growth_class
        return StorageEstimate(**raw)


archival_service = ArchivalService(log=getLogger(__name__))
