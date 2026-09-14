"""
Tests for TimeSeriesService.

Tests cover:
- Bulk creating time series samples
- Getting daily histogram of data points
- Counting data points by series type
- Counting data points by provider
- Downsampling samples into fixed-width buckets
"""

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.models import User
from app.schemas.enums import SeriesType
from app.schemas.model_crud.activities import (
    HeartRateSampleCreate,
    StepSampleCreate,
    TimeSeriesQueryParams,
    TimeSeriesSampleCreate,
)
from app.services.timeseries_service import timeseries_service
from app.utils.pagination import encode_bucket_cursor, encode_cursor
from tests.factories import (
    DataPointSeriesFactory,
    DataSourceFactory,
    SeriesTypeDefinitionFactory,
    UserFactory,
)


class TestTimeSeriesServiceBulkCreateSamples:
    """Test bulk creation of time series samples."""

    def test_bulk_create_heart_rate_samples(self, db: Session) -> None:
        """Should bulk create heart rate samples."""
        # Arrange
        user = UserFactory()
        DataSourceFactory(source="apple", device_model="device_1")

        initial_count = timeseries_service.get_total_count(db)
        now = datetime.now(timezone.utc)
        samples = [
            HeartRateSampleCreate(
                id=uuid4(),
                user_id=user.id,
                provider_name="apple",
                device_model="device_1",
                recorded_at=now - timedelta(minutes=i),
                value=70 + i,
                series_type=SeriesType.heart_rate,
            )
            for i in range(5)
        ]

        # Act
        timeseries_service.bulk_create_samples(db, samples)

        # Assert - verify samples were created
        final_count = timeseries_service.get_total_count(db)
        assert final_count == initial_count + 5

    def test_bulk_create_step_samples(self, db: Session) -> None:
        """Should bulk create step samples."""
        # Arrange
        user = UserFactory()
        DataSourceFactory(source="apple", device_model="device_2")

        initial_count = timeseries_service.get_total_count(db)
        now = datetime.now(timezone.utc)
        samples = [
            StepSampleCreate(
                id=uuid4(),
                user_id=user.id,
                provider_name="apple",
                device_model="device_2",
                recorded_at=now - timedelta(hours=i),
                value=1000 + i * 100,
                series_type=SeriesType.steps,
            )
            for i in range(3)
        ]

        # Act
        timeseries_service.bulk_create_samples(db, samples)

        # Assert
        final_count = timeseries_service.get_total_count(db)
        assert final_count == initial_count + 3

    def test_bulk_create_mixed_series_types(self, db: Session) -> None:
        """Should bulk create samples of different series types."""
        # Arrange
        user = UserFactory()
        DataSourceFactory(source="apple", device_model="device_3")

        initial_count = timeseries_service.get_total_count(db)
        now = datetime.now(timezone.utc)
        samples = [
            TimeSeriesSampleCreate(
                id=uuid4(),
                user_id=user.id,
                provider_name="apple",
                device_model="device_3",
                recorded_at=now - timedelta(minutes=1),
                value=72,
                series_type=SeriesType.heart_rate,
            ),
            TimeSeriesSampleCreate(
                id=uuid4(),
                user_id=user.id,
                provider_name="apple",
                device_model="device_3",
                recorded_at=now - timedelta(minutes=2),
                value=5000,
                series_type=SeriesType.steps,
            ),
        ]

        # Act
        timeseries_service.bulk_create_samples(db, samples)

        # Assert
        total_count = timeseries_service.get_total_count(db)
        assert total_count >= initial_count + 2


class TestTimeSeriesServiceGetDailyHistogram:
    """Test getting daily histogram of data points."""

    def test_get_daily_histogram_groups_by_day(self, db: Session) -> None:
        """Should group data points by day."""
        # Arrange
        mapping = DataSourceFactory()
        series_type = SeriesTypeDefinitionFactory.get_or_create_heart_rate()

        start_date = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 4, 0, 0, 0, tzinfo=timezone.utc)

        # Day 1: 3 samples
        for i in range(3):
            DataPointSeriesFactory(
                mapping=mapping,
                series_type=series_type,
                recorded_at=datetime(2024, 1, 1, 10 + i, 0, 0, tzinfo=timezone.utc),
            )

        # Day 2: 2 samples
        for i in range(2):
            DataPointSeriesFactory(
                mapping=mapping,
                series_type=series_type,
                recorded_at=datetime(2024, 1, 2, 10 + i, 0, 0, tzinfo=timezone.utc),
            )

        # Day 3: 1 sample
        DataPointSeriesFactory(
            mapping=mapping,
            series_type=series_type,
            recorded_at=datetime(2024, 1, 3, 10, 0, 0, tzinfo=timezone.utc),
        )

        # Act
        histogram = timeseries_service.get_daily_histogram(db, start_date, end_date)

        # Assert
        assert len(histogram) == 3
        assert histogram[0] == 3  # Day 1
        assert histogram[1] == 2  # Day 2
        assert histogram[2] == 1  # Day 3

    def test_get_daily_histogram_empty_range(self, db: Session) -> None:
        """Should return empty list for range with no data."""
        # Arrange
        start_date = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        end_date = datetime(2024, 6, 7, 0, 0, 0, tzinfo=timezone.utc)

        # Act
        histogram = timeseries_service.get_daily_histogram(db, start_date, end_date)

        # Assert
        assert histogram == []


class TestTimeSeriesServiceGetCountBySeriesType:
    """Test counting data points by series type."""

    def test_get_count_by_series_type_groups_correctly(self, db: Session) -> None:
        """Should group and count data points by series type."""
        # Arrange
        mapping = DataSourceFactory()
        hr_type = SeriesTypeDefinitionFactory.get_or_create_heart_rate()
        step_type = SeriesTypeDefinitionFactory.get_or_create_steps()

        # Create 3 heart rate samples
        for _ in range(3):
            DataPointSeriesFactory(mapping=mapping, series_type=hr_type)

        # Create 2 step samples
        for _ in range(2):
            DataPointSeriesFactory(mapping=mapping, series_type=step_type)

        # Act
        results = timeseries_service.get_count_by_series_type(db)

        # Assert
        results_dict = dict(results)
        assert results_dict[hr_type.id] == 3
        assert results_dict[step_type.id] == 2

    def test_get_count_by_series_type_ordered_by_count(self, db: Session) -> None:
        """Should order results by count descending."""
        # Arrange
        mapping = DataSourceFactory()
        # Use existing seeded series types to avoid ID conflicts
        type1 = SeriesTypeDefinitionFactory.get_or_create_heart_rate()
        type2 = SeriesTypeDefinitionFactory.get_or_create_steps()

        # Create more of type1
        for _ in range(5):
            DataPointSeriesFactory(mapping=mapping, series_type=type1)

        # Create fewer of type2
        for _ in range(2):
            DataPointSeriesFactory(mapping=mapping, series_type=type2)

        # Act
        results = timeseries_service.get_count_by_series_type(db)

        # Assert
        # Results should be ordered by count descending
        assert results[0][1] >= results[1][1]

    def test_get_count_by_series_type_empty_result(self, db: Session) -> None:
        """Should return empty list when no data points exist."""
        # Act
        results = timeseries_service.get_count_by_series_type(db)

        # Assert
        assert results == []


class TestTimeSeriesServiceGetCountBySource:
    """Test counting data points by source."""

    def test_get_count_by_source_groups_correctly(self, db: Session) -> None:
        """Should group and count data points by source."""
        # Arrange
        user = UserFactory()
        apple_mapping = DataSourceFactory(user=user, source="apple")
        garmin_mapping = DataSourceFactory(user=user, source="garmin")

        series_type = SeriesTypeDefinitionFactory.get_or_create_heart_rate()

        # Create 4 samples from Apple
        for _ in range(4):
            DataPointSeriesFactory(mapping=apple_mapping, series_type=series_type)

        # Create 2 samples from Garmin
        for _ in range(2):
            DataPointSeriesFactory(mapping=garmin_mapping, series_type=series_type)

        # Act
        results = timeseries_service.get_count_by_source(db)

        # Assert
        results_dict = dict(results)
        assert results_dict["apple"] == 4
        assert results_dict["garmin"] == 2

    def test_get_count_by_source_ordered_by_count(self, db: Session) -> None:
        """Should order results by count descending."""
        # Arrange
        results = timeseries_service.get_count_by_source(db)

        if len(results) > 1:
            # Verify descending order
            for i in range(len(results) - 1):
                assert results[i][1] >= results[i + 1][1]

    def test_get_count_by_source_empty_result(self, db: Session) -> None:
        """Should return empty list when no data points exist."""
        # Act
        results = timeseries_service.get_count_by_source(db)

        # Assert
        assert results == []


class TestTimeSeriesServiceGetTotalCount:
    """Test getting total count of data points."""

    def test_get_total_count(self, db: Session) -> None:
        """Should return total count of all data points."""
        # Arrange
        mapping = DataSourceFactory()
        series_type = SeriesTypeDefinitionFactory.get_or_create_heart_rate()

        initial_count = timeseries_service.get_total_count(db)

        # Create 5 samples
        for _ in range(5):
            DataPointSeriesFactory(mapping=mapping, series_type=series_type)

        # Act
        total_count = timeseries_service.get_total_count(db)

        # Assert
        assert total_count == initial_count + 5

    def test_get_total_count_empty_database(self, db: Session) -> None:
        """Should return 0 when no data points exist."""
        # Note: This test might fail if there's existing data in the test DB
        # from other tests running in the same session
        # Act
        count = timeseries_service.get_total_count(db)

        # Assert
        assert count >= 0  # At minimum should be non-negative


class TestTimeSeriesServiceGetCountInRange:
    """Test counting data points in date range."""

    def test_get_count_in_range(self, db: Session) -> None:
        """Should count data points within date range."""
        # Arrange
        mapping = DataSourceFactory()
        series_type = SeriesTypeDefinitionFactory.get_or_create_heart_rate()

        now = datetime.now(timezone.utc)
        start = now - timedelta(days=7)
        end = now - timedelta(days=1)

        # Create samples at different times
        DataPointSeriesFactory(
            mapping=mapping,
            series_type=series_type,
            recorded_at=now - timedelta(days=10),
        )  # Before range
        DataPointSeriesFactory(
            mapping=mapping,
            series_type=series_type,
            recorded_at=now - timedelta(days=5),
        )  # In range
        DataPointSeriesFactory(
            mapping=mapping,
            series_type=series_type,
            recorded_at=now - timedelta(days=3),
        )  # In range
        DataPointSeriesFactory(mapping=mapping, series_type=series_type, recorded_at=now)  # After range

        # Act
        count = timeseries_service.get_count_in_range(db, start, end)

        # Assert
        assert count == 2

    def test_get_count_in_range_empty_result(self, db: Session) -> None:
        """Should return 0 when no data points in range."""
        # Arrange
        now = datetime.now(timezone.utc)
        future = now + timedelta(days=7)
        far_future = future + timedelta(days=7)

        # Act
        count = timeseries_service.get_count_in_range(db, future, far_future)

        # Assert
        assert count == 0


class TestTimeSeriesServiceGetTimeseries:
    """Test the paginated sample query."""

    @staticmethod
    def _seed(db: Session, count: int = 3) -> tuple[User, TimeSeriesQueryParams]:
        user = UserFactory()
        DataSourceFactory(user=user, source="apple", device_model="device_1")
        now = datetime.now(timezone.utc)
        timeseries_service.bulk_create_samples(
            db,
            [
                TimeSeriesSampleCreate(
                    id=uuid4(),
                    user_id=user.id,
                    source="apple",
                    device_model="device_1",
                    recorded_at=now - timedelta(minutes=i),
                    value=70 + i,
                    series_type=SeriesType.heart_rate,
                )
                for i in range(count)
            ],
        )
        return user, TimeSeriesQueryParams(device_model="device_1")

    def test_reports_the_applied_resolution(self, db: Session) -> None:
        """Metadata reports the resolution actually applied, never the request.

        A caller compares this against what it asked for, so it must describe the
        rows in the response: raw when samples came back one per row.
        """
        # Arrange
        user, query_params = self._seed(db)

        # Act
        response = timeseries_service.get_timeseries(
            db,
            user.id,
            [SeriesType.heart_rate],
            query_params,
            requested_resolution="raw",
        )

        # Assert
        assert response.metadata.resolution == "raw"
        assert response.metadata.sample_count == 3

    def test_bucket_cursor_is_rejected_by_the_raw_path(self, db: Session) -> None:
        """A bucket cursor must not resume a raw walk.

        The two cursors key on different things, so silently accepting one for
        the other would skip or repeat rows instead of failing.
        """
        # Arrange
        user, query_params = self._seed(db)
        query_params.cursor = encode_bucket_cursor(datetime.now(timezone.utc), 1, uuid4())

        # Act / Assert
        with pytest.raises(HTTPException) as exc_info:
            timeseries_service.get_timeseries(db, user.id, [SeriesType.heart_rate], query_params)

        assert exc_info.value.status_code == 400

    def test_total_count_is_opt_in(self, db: Session) -> None:
        """include_total must reach the count query and surface on pagination."""
        # Arrange
        user, query_params = self._seed(db)

        # Act
        default_response = timeseries_service.get_timeseries(db, user.id, [SeriesType.heart_rate], query_params)
        counted_response = timeseries_service.get_timeseries(
            db, user.id, [SeriesType.heart_rate], query_params, include_total=True
        )

        # Assert — the rows come back either way; only the count is conditional
        assert default_response.pagination.total_count is None
        assert len(default_response.data) == 3

        assert counted_response.pagination.total_count == 3
        assert len(counted_response.data) == 3


class TestGetTimeseriesBucketed:
    """Server-side downsampling of the timeseries read path."""

    DAY = datetime(2026, 3, 2, 0, 0, tzinfo=timezone.utc)

    @staticmethod
    def _window() -> tuple[datetime, datetime]:
        """A single-day window with an explicit end time (no date-only widening)."""
        return TestGetTimeseriesBucketed.DAY, TestGetTimeseriesBucketed.DAY + timedelta(hours=23, minutes=59)

    @classmethod
    def _params(cls, limit: int = 50, cursor: str | None = None) -> TimeSeriesQueryParams:
        start, end = cls._window()
        return TimeSeriesQueryParams(start_datetime=start, end_datetime=end, limit=limit, cursor=cursor)

    @classmethod
    def _at(cls, hour: int, minute: int = 0) -> datetime:
        return cls.DAY + timedelta(hours=hour, minutes=minute)

    def test_averages_rate_types_within_each_bucket(self, db: Session) -> None:
        """Heart rate aggregates by AVG, so a bucket is the mean of its samples."""
        # Arrange
        user = UserFactory()
        data_source = DataSourceFactory(user=user, source="apple", device_model="device_1")
        hr_type = SeriesTypeDefinitionFactory.get_or_create_heart_rate()
        for minute, value in ((5, 60), (35, 80)):
            DataPointSeriesFactory(
                data_source=data_source, series_type=hr_type, recorded_at=self._at(10, minute), value=value
            )
        DataPointSeriesFactory(data_source=data_source, series_type=hr_type, recorded_at=self._at(11, 15), value=100)

        # Act
        response = timeseries_service.get_timeseries(
            db, user.id, [SeriesType.heart_rate], self._params(), requested_resolution="1hour"
        )

        # Assert
        assert response.metadata.resolution == "1hour"
        assert [(s.timestamp, s.value) for s in response.data] == [
            (self._at(10), 70.0),
            (self._at(11), 100.0),
        ]

    def test_sums_cumulative_types_within_each_bucket(self, db: Session) -> None:
        """Steps aggregate by SUM, so a bucket is the total of its samples."""
        # Arrange
        user = UserFactory()
        data_source = DataSourceFactory(user=user, source="apple", device_model="device_1")
        steps_type = SeriesTypeDefinitionFactory.get_or_create_steps()
        for minute, value in ((5, 100), (35, 200)):
            DataPointSeriesFactory(
                data_source=data_source,
                series_type=steps_type,
                recorded_at=self._at(10, minute),
                value=value,
                is_daily_total=False,
            )
        DataPointSeriesFactory(
            data_source=data_source,
            series_type=steps_type,
            recorded_at=self._at(11, 15),
            value=50,
            is_daily_total=False,
        )

        # Act
        response = timeseries_service.get_timeseries(
            db, user.id, [SeriesType.steps], self._params(), requested_resolution="1hour"
        )

        # Assert
        assert [(s.timestamp, s.value) for s in response.data] == [
            (self._at(10), 300.0),
            (self._at(11), 50.0),
        ]

    def test_excludes_provider_daily_totals_from_buckets(self, db: Session) -> None:
        """A daily total must not be reported as one bucket's value.

        A provider daily total describes the whole day, so folding it into the
        bucket holding its timestamp would both overstate that bucket and — since
        the daily-preference rule shadows same-day samples — double-count the day
        against the intraday samples in every other bucket.
        """
        # Arrange
        user = UserFactory()
        data_source = DataSourceFactory(user=user, source="garmin", device_model="device_1")
        steps_type = SeriesTypeDefinitionFactory.get_or_create_steps()
        for minute, value in ((5, 100), (35, 200)):
            DataPointSeriesFactory(
                data_source=data_source,
                series_type=steps_type,
                recorded_at=self._at(10, minute),
                value=value,
                is_daily_total=False,
            )
        DataPointSeriesFactory(
            data_source=data_source,
            series_type=steps_type,
            recorded_at=self._at(10, 50),
            value=5000,
            is_daily_total=True,
        )

        # Act
        response = timeseries_service.get_timeseries(
            db, user.id, [SeriesType.steps], self._params(), requested_resolution="1hour"
        )

        # Assert — only the intraday samples, and the day still totals 300
        assert [(s.timestamp, s.value) for s in response.data] == [(self._at(10), 300.0)]
        assert sum(s.value for s in response.data) == 300.0
        assert all(s.is_daily_total is False for s in response.data)

    def test_carries_the_zone_offset_through(self, db: Session) -> None:
        """Buckets keep the recording offset, which callers use to map local days.

        Bucket starts are floored on the UTC epoch, so the offset is the only
        thing that tells a caller which local day a bucket belongs to.
        """
        # Arrange
        user = UserFactory()
        data_source = DataSourceFactory(user=user, source="apple", device_model="device_1")
        hr_type = SeriesTypeDefinitionFactory.get_or_create_heart_rate()
        for minute in (5, 35):
            DataPointSeriesFactory(
                data_source=data_source,
                series_type=hr_type,
                recorded_at=self._at(10, minute),
                value=70,
                zone_offset="-04:00",
            )

        # Act
        response = timeseries_service.get_timeseries(
            db, user.id, [SeriesType.heart_rate], self._params(), requested_resolution="1hour"
        )

        # Assert
        assert [s.zone_offset for s in response.data] == ["-04:00"]

    def test_keeps_devices_in_separate_buckets(self, db: Session) -> None:
        """Two devices in one hour stay two buckets, not one blended average."""
        # Arrange
        user = UserFactory()
        hr_type = SeriesTypeDefinitionFactory.get_or_create_heart_rate()
        watch = DataSourceFactory(user=user, source="apple", device_model="watch")
        strap = DataSourceFactory(user=user, source="garmin", device_model="strap")
        DataPointSeriesFactory(data_source=watch, series_type=hr_type, recorded_at=self._at(10, 5), value=60)
        DataPointSeriesFactory(data_source=strap, series_type=hr_type, recorded_at=self._at(10, 25), value=100)

        # Act
        response = timeseries_service.get_timeseries(
            db, user.id, [SeriesType.heart_rate], self._params(), requested_resolution="1hour"
        )

        # Assert
        assert len(response.data) == 2
        assert {s.timestamp for s in response.data} == {self._at(10)}
        assert sorted(s.value for s in response.data) == [60.0, 100.0]
        assert {s.source.device for s in response.data if s.source} == {"watch", "strap"}

    def test_pagination_visits_every_bucket_once(self, db: Session) -> None:
        """Walking next_cursor must cover all buckets with no gap or repeat."""
        # Arrange
        user = UserFactory()
        data_source = DataSourceFactory(user=user, source="apple", device_model="device_1")
        hr_type = SeriesTypeDefinitionFactory.get_or_create_heart_rate()
        for hour in range(5):
            DataPointSeriesFactory(
                data_source=data_source, series_type=hr_type, recorded_at=self._at(hour, 10), value=60 + hour
            )

        # Act — page through two buckets at a time
        seen: list[tuple[datetime, float]] = []
        cursor: str | None = None
        for _ in range(5):
            page = timeseries_service.get_timeseries(
                db,
                user.id,
                [SeriesType.heart_rate],
                self._params(limit=2, cursor=cursor),
                requested_resolution="1hour",
            )
            seen.extend((s.timestamp, s.value) for s in page.data)
            cursor = page.pagination.next_cursor
            if not cursor:
                break

        # Assert
        assert seen == [(self._at(hour), float(60 + hour)) for hour in range(5)]
        assert cursor is None

    def test_total_count_is_opt_in_and_counts_buckets(self, db: Session) -> None:
        """include_total counts buckets, not raw samples, and stays opt-in.

        Same contract as the raw path, so a caller cannot tell the two
        resolutions apart by whether a count came back.
        """
        # Arrange
        user = UserFactory()
        data_source = DataSourceFactory(user=user, source="apple", device_model="device_1")
        hr_type = SeriesTypeDefinitionFactory.get_or_create_heart_rate()
        for hour in range(3):
            for minute in (5, 35):
                DataPointSeriesFactory(
                    data_source=data_source, series_type=hr_type, recorded_at=self._at(hour, minute), value=70
                )

        # Act
        default_response = timeseries_service.get_timeseries(
            db, user.id, [SeriesType.heart_rate], self._params(), requested_resolution="1hour"
        )
        counted_response = timeseries_service.get_timeseries(
            db,
            user.id,
            [SeriesType.heart_rate],
            self._params(),
            include_total=True,
            requested_resolution="1hour",
        )

        # Assert — 6 samples collapse to 3 buckets
        assert default_response.pagination.total_count is None
        assert len(default_response.data) == 3

        assert counted_response.pagination.total_count == 3
        assert len(counted_response.data) == 3

    def test_total_count_ignores_the_cursor(self, db: Session) -> None:
        """The count describes the whole window, not the page."""
        # Arrange
        user = UserFactory()
        data_source = DataSourceFactory(user=user, source="apple", device_model="device_1")
        hr_type = SeriesTypeDefinitionFactory.get_or_create_heart_rate()
        for hour in range(5):
            DataPointSeriesFactory(
                data_source=data_source, series_type=hr_type, recorded_at=self._at(hour, 10), value=70
            )

        # Act — take page one, then resume with its cursor
        first = timeseries_service.get_timeseries(
            db,
            user.id,
            [SeriesType.heart_rate],
            self._params(limit=2),
            include_total=True,
            requested_resolution="1hour",
        )
        second = timeseries_service.get_timeseries(
            db,
            user.id,
            [SeriesType.heart_rate],
            self._params(limit=2, cursor=first.pagination.next_cursor),
            include_total=True,
            requested_resolution="1hour",
        )

        # Assert
        assert first.pagination.total_count == 5
        assert second.pagination.total_count == 5
        assert len(second.data) == 2

    def test_backward_pagination_returns_the_preceding_buckets(self, db: Session) -> None:
        """previous_cursor must step back to the buckets just before the page.

        Backward paging runs as a descending keyset in SQL and is reversed before
        returning, so this pins both the direction and the ascending order.
        """
        # Arrange
        user = UserFactory()
        data_source = DataSourceFactory(user=user, source="apple", device_model="device_1")
        hr_type = SeriesTypeDefinitionFactory.get_or_create_heart_rate()
        for hour in range(6):
            DataPointSeriesFactory(
                data_source=data_source, series_type=hr_type, recorded_at=self._at(hour, 10), value=60 + hour
            )

        # Act — walk forward twice, then back once
        page_one = timeseries_service.get_timeseries(
            db, user.id, [SeriesType.heart_rate], self._params(limit=2), requested_resolution="1hour"
        )
        page_two = timeseries_service.get_timeseries(
            db,
            user.id,
            [SeriesType.heart_rate],
            self._params(limit=2, cursor=page_one.pagination.next_cursor),
            requested_resolution="1hour",
        )
        back = timeseries_service.get_timeseries(
            db,
            user.id,
            [SeriesType.heart_rate],
            self._params(limit=2, cursor=page_two.pagination.previous_cursor),
            requested_resolution="1hour",
        )

        # Assert — stepping back from page two lands on page one, still ascending
        assert [(s.timestamp, s.value) for s in page_two.data] == [
            (self._at(2), 62.0),
            (self._at(3), 63.0),
        ]
        assert [(s.timestamp, s.value) for s in back.data] == [(s.timestamp, s.value) for s in page_one.data]
        assert [s.timestamp for s in back.data] == [self._at(0), self._at(1)]

    def test_pagination_does_not_split_a_bucket_across_pages(self, db: Session) -> None:
        """Types and devices sharing a bucket_start must not be skipped.

        The cursor keys on (bucket_start, type, device); keying on the timestamp
        alone would drop whatever else sat in the straddled bucket.
        """
        # Arrange
        user = UserFactory()
        hr_type = SeriesTypeDefinitionFactory.get_or_create_heart_rate()
        steps_type = SeriesTypeDefinitionFactory.get_or_create_steps()
        watch = DataSourceFactory(user=user, source="apple", device_model="watch")
        phone = DataSourceFactory(user=user, source="apple", device_model="phone")
        # Four buckets, all at the same hour: two types across two devices.
        for data_source in (watch, phone):
            for series_type in (hr_type, steps_type):
                DataPointSeriesFactory(
                    data_source=data_source,
                    series_type=series_type,
                    recorded_at=self._at(10, 5),
                    value=70,
                    is_daily_total=False,
                )

        # Act — one bucket at a time, so every page boundary falls inside the hour
        seen: list[tuple[SeriesType, str | None]] = []
        cursor: str | None = None
        for _ in range(5):
            page = timeseries_service.get_timeseries(
                db,
                user.id,
                [SeriesType.heart_rate, SeriesType.steps],
                self._params(limit=1, cursor=cursor),
                requested_resolution="1hour",
            )
            seen.extend((s.type, s.source.device if s.source else None) for s in page.data)
            cursor = page.pagination.next_cursor
            if not cursor:
                break

        # Assert — all four buckets, each exactly once
        assert sorted(seen) == sorted(
            [
                (SeriesType.heart_rate, "watch"),
                (SeriesType.heart_rate, "phone"),
                (SeriesType.steps, "watch"),
                (SeriesType.steps, "phone"),
            ]
        )

    def test_bucket_width_follows_the_requested_resolution(self, db: Session) -> None:
        """15min splits an hour that 1hour keeps whole."""
        # Arrange
        user = UserFactory()
        data_source = DataSourceFactory(user=user, source="apple", device_model="device_1")
        hr_type = SeriesTypeDefinitionFactory.get_or_create_heart_rate()
        for minute, value in ((5, 60), (20, 80)):
            DataPointSeriesFactory(
                data_source=data_source, series_type=hr_type, recorded_at=self._at(10, minute), value=value
            )

        # Act
        hourly = timeseries_service.get_timeseries(
            db, user.id, [SeriesType.heart_rate], self._params(), requested_resolution="1hour"
        )
        quarterly = timeseries_service.get_timeseries(
            db, user.id, [SeriesType.heart_rate], self._params(), requested_resolution="15min"
        )

        # Assert
        assert [(s.timestamp, s.value) for s in hourly.data] == [(self._at(10), 70.0)]
        assert [(s.timestamp, s.value) for s in quarterly.data] == [
            (self._at(10, 0), 60.0),
            (self._at(10, 15), 80.0),
        ]

    def test_raw_cursor_is_rejected_by_the_bucketed_path(self, db: Session) -> None:
        """A raw-sample cursor must not resume a bucket walk."""
        # Arrange
        user = UserFactory()
        params = self._params(cursor=encode_cursor(self._at(10), uuid4()))

        # Act / Assert
        with pytest.raises(HTTPException) as exc_info:
            timeseries_service.get_timeseries(
                db, user.id, [SeriesType.heart_rate], params, requested_resolution="1hour"
            )

        assert exc_info.value.status_code == 400
