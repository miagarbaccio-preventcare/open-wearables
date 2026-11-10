"""
Tests for ArchivalService.

Tests cover:
- Refusing to enable or perform archival
- The retention policy that still applies when archival is refused
"""

from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.models import DataPointSeries, DataPointSeriesArchive
from app.models.archival_setting import ArchivalSetting
from app.schemas.utils import ArchivalSettingUpdate
from app.services.archival_service import archival_service
from tests.factories import (
    DataPointSeriesFactory,
    DataSourceFactory,
    SeriesTypeDefinitionFactory,
    UserFactory,
)


def _set_lifecycle(db: Session, *, archive_after_days: int | None, delete_after_days: int | None) -> None:
    """Write the singleton row directly, which is the only way past the refusal."""
    setting = db.query(ArchivalSetting).filter(ArchivalSetting.id == 1).first()
    if setting is None:
        setting = ArchivalSetting(id=1)
        db.add(setting)
    setting.archive_after_days = archive_after_days
    setting.delete_after_days = delete_after_days
    db.commit()


def _days_ago(days: int) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=days)


def _live_row_ages(db: Session, data_source_id: object) -> set[int]:
    """Ages in whole days of the live rows still attached to *data_source_id*."""
    today = datetime.now(timezone.utc).date()
    rows = db.query(DataPointSeries).filter(DataPointSeries.data_source_id == data_source_id).all()
    return {(today - row.recorded_at.date()).days for row in rows}


class TestUpdateSettings:
    """Enabling archival through the API."""

    def test_rejects_an_archive_horizon(self, db: Session) -> None:
        """Archival cannot be turned on: it discards the intraday rows the reads need."""
        # Arrange
        _set_lifecycle(db, archive_after_days=None, delete_after_days=None)

        # Act
        with pytest.raises(HTTPException) as exc_info:
            archival_service.update_settings(db, ArchivalSettingUpdate(archive_after_days=90))

        # Assert — rejected, and nothing was stored
        assert exc_info.value.status_code == 400
        assert "Archival is not supported" in str(exc_info.value.detail)
        assert db.query(ArchivalSetting).filter(ArchivalSetting.id == 1).one().archive_after_days is None

    def test_rejects_an_archive_horizon_alongside_retention(self, db: Session) -> None:
        """A valid retention value does not carry an archive value in with it."""
        # Arrange
        _set_lifecycle(db, archive_after_days=None, delete_after_days=None)

        # Act
        with pytest.raises(HTTPException) as exc_info:
            archival_service.update_settings(db, ArchivalSettingUpdate(archive_after_days=30, delete_after_days=365))

        # Assert — the whole update is refused, retention included
        assert exc_info.value.status_code == 400
        setting = db.query(ArchivalSetting).filter(ArchivalSetting.id == 1).one()
        assert (setting.archive_after_days, setting.delete_after_days) == (None, None)

    def test_accepts_a_retention_horizon(self, db: Session) -> None:
        """Retention is a policy choice and stays configurable."""
        # Arrange
        _set_lifecycle(db, archive_after_days=None, delete_after_days=None)

        # Act
        result = archival_service.update_settings(db, ArchivalSettingUpdate(delete_after_days=365))

        # Assert
        assert result.settings.delete_after_days == 365
        assert result.settings.archive_after_days is None


class TestRunDailyArchival:
    """The daily lifecycle job."""

    @staticmethod
    def _seed_row(days_ago: int) -> tuple[object, object]:
        user = UserFactory()
        data_source = DataSourceFactory(user=user, source="apple", device_model="device_1")
        DataPointSeriesFactory(
            data_source=data_source,
            series_type=SeriesTypeDefinitionFactory.get_or_create_heart_rate(),
            recorded_at=_days_ago(days_ago),
            value=70,
        )
        return user, data_source

    def test_refuses_to_archive_a_configured_horizon(self, db: Session) -> None:
        """A row written straight to the table still does not get archived."""
        # Arrange
        _, data_source = self._seed_row(days_ago=100)
        _set_lifecycle(db, archive_after_days=30, delete_after_days=None)

        # Act
        summary = archival_service.run_daily_archival(db)

        # Assert — the samples stay where the read paths can still see them
        assert summary["archived_rows"] == 0
        assert _live_row_ages(db, data_source.id) == {100}
        assert db.query(DataPointSeriesArchive).count() == 0

    def test_deletes_live_rows_past_the_retention_horizon(self, db: Session) -> None:
        """Retention still applies, and it reads the live table."""
        # Arrange
        _, data_source = self._seed_row(days_ago=100)
        DataPointSeriesFactory(
            data_source=data_source,
            series_type=SeriesTypeDefinitionFactory.get_or_create_heart_rate(),
            recorded_at=_days_ago(1),
            value=65,
        )
        _set_lifecycle(db, archive_after_days=None, delete_after_days=30)

        # Act
        summary = archival_service.run_daily_archival(db)

        # Assert
        assert summary["deleted_live_rows"] == 1
        assert _live_row_ages(db, data_source.id) == {1}

    def test_retention_reaches_rows_an_archive_horizon_would_have_taken(self, db: Session) -> None:
        """With both set, retention deletes from the live table and the shorter horizon is inert.

        The row between the two horizons is the one archival would have collapsed;
        refusing archival is what leaves it whole.
        """
        # Arrange
        _, data_source = self._seed_row(days_ago=100)
        for days_ago in (45, 1):
            DataPointSeriesFactory(
                data_source=data_source,
                series_type=SeriesTypeDefinitionFactory.get_or_create_heart_rate(),
                recorded_at=_days_ago(days_ago),
                value=70,
            )
        _set_lifecycle(db, archive_after_days=30, delete_after_days=60)

        # Act
        summary = archival_service.run_daily_archival(db)

        # Assert
        assert summary["archived_rows"] == 0
        assert summary["deleted_live_rows"] == 1
        assert _live_row_ages(db, data_source.id) == {45, 1}
        assert db.query(DataPointSeriesArchive).count() == 0

    def test_is_a_no_op_without_a_retention_horizon(self, db: Session) -> None:
        """Neither policy set means nothing is touched."""
        # Arrange
        _, data_source = self._seed_row(days_ago=100)
        _set_lifecycle(db, archive_after_days=None, delete_after_days=None)

        # Act
        summary = archival_service.run_daily_archival(db)

        # Assert
        assert summary == {"archived_rows": 0, "deleted_rows": 0, "deleted_live_rows": 0}
        assert _live_row_ages(db, data_source.id) == {100}
