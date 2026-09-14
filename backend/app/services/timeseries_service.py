import threading
from collections import defaultdict
from collections.abc import Callable
from datetime import datetime
from logging import Logger, getLogger
from typing import Any
from uuid import UUID

from sqlalchemy import event as sa_event

from app.database import DbSession
from app.models import DataPointSeries
from app.repositories import DataPointSeriesRepository
from app.repositories.data_point_series_repository import WriteCounts
from app.schemas.enums import (
    AggregationMethod,
    SeriesType,
    get_aggregation_method,
    get_series_type_from_id,
    get_series_type_unit,
)
from app.schemas.model_crud.activities import (
    HeartRateSampleCreate,
    StepSampleCreate,
    TimeSeriesQueryParams,
    TimeSeriesSampleCreate,
    TimeSeriesSampleUpdate,
)
from app.schemas.responses.activity import BucketedSampleResult, TimeSeriesSample
from app.schemas.utils import (
    PaginatedResponse,
    Pagination,
    SourceMetadata,
    TimeseriesMetadata,
)
from app.services.outgoing_webhooks import svix as svix_service
from app.services.outgoing_webhooks.events import on_timeseries_batch_saved
from app.services.services import AppService
from app.utils.exceptions import handle_exceptions
from app.utils.pagination import encode_bucket_cursor, encode_cursor

# Bucket width per requested resolution. "raw" is absent on purpose: a missing
# entry is what selects the unaggregated path.
BUCKET_SECONDS_BY_RESOLUTION: dict[str, int] = {
    "1min": 60,
    "5min": 300,
    "15min": 900,
    "1hour": 3600,
}

# Which aggregate column represents a bucket, per the series type's archiving
# method: cumulative types sum, rate/level types average, peak types take the max.
_BUCKET_VALUE_BY_METHOD: dict[AggregationMethod, Callable[[BucketedSampleResult], float]] = {
    AggregationMethod.SUM: lambda bucket: bucket["sum_value"],
    AggregationMethod.AVG: lambda bucket: bucket["avg_value"],
    AggregationMethod.MAX: lambda bucket: bucket["max_value"],
}


def _bucket_key(bucket: BucketedSampleResult) -> tuple[datetime, int, UUID]:
    """Sort/cursor key for a bucket, matching the repository's ORDER BY."""
    return (
        bucket["bucket_start"],
        bucket["series_type_definition_id"],
        bucket["data_source_id"],
    )


class TimeSeriesService(
    AppService[
        DataPointSeriesRepository,
        DataPointSeries,
        TimeSeriesSampleCreate,
        TimeSeriesSampleUpdate,
    ],
):
    """Coordinated access to unified device time series samples."""

    def __init__(self, log: Logger):
        super().__init__(crud_model=DataPointSeriesRepository, model=DataPointSeries, log=log)

    def bulk_create_samples(
        self,
        db_session: DbSession,
        samples: (list[TimeSeriesSampleCreate] | list[HeartRateSampleCreate] | list[StepSampleCreate]),
    ) -> WriteCounts:
        counts = self.crud.bulk_create(db_session, samples)  # ty:ignore[invalid-argument-type]
        samples_copy = list(samples)

        @sa_event.listens_for(db_session, "after_commit", once=True)
        def _start_webhook_thread(session: DbSession) -> None:  # noqa: ARG001
            if not svix_service.is_enabled():
                return
            threading.Thread(
                target=self._emit_timeseries_webhooks,
                args=(samples_copy,),
                daemon=True,
            ).start()

        return counts

    @staticmethod
    def _emit_timeseries_webhooks(
        samples: list[TimeSeriesSampleCreate] | list[HeartRateSampleCreate] | list[StepSampleCreate],
    ) -> None:
        """Emit one webhook event per (user, provider, series_type) batch."""
        if not samples:
            return
        try:
            groups: dict[tuple[UUID, str, str], list[Any]] = defaultdict(list)
            for s in samples:
                key = (s.user_id, s.provider or s.source or "unknown", s.series_type.value)
                groups[key].append(s)
            for (user_id, provider, series_type_value), group_samples in groups.items():
                sorted_samples = sorted(group_samples, key=lambda s: s.recorded_at)
                series_type_enum = SeriesType(series_type_value)
                unit = get_series_type_unit(series_type_enum)
                webhook_samples = [
                    {
                        "timestamp": s.recorded_at.isoformat(),
                        "zone_offset": s.zone_offset,
                        "type": series_type_value,
                        "value": float(s.value),
                        "unit": unit,
                        "source": {"provider": provider, "device": s.device_model},
                        "is_daily_total": s.is_daily_total,
                    }
                    for s in sorted_samples
                ]
                on_timeseries_batch_saved(
                    user_id=user_id,
                    provider=provider,
                    series_type=series_type_value,
                    sample_count=len(sorted_samples),
                    start_time=sorted_samples[0].recorded_at.isoformat(),
                    end_time=sorted_samples[-1].recorded_at.isoformat(),
                    samples=webhook_samples,
                )
        except Exception:
            getLogger(__name__).warning("Failed to emit timeseries webhooks", exc_info=True)

    def get_total_count(self, db_session: DbSession) -> int:
        """Get total count of all data points."""
        return self.crud.get_total_count(db_session)

    def get_count_in_range(self, db_session: DbSession, start_datetime: datetime, end_datetime: datetime) -> int:
        """Get count of data points within a datetime range."""
        return self.crud.get_count_in_range(db_session, start_datetime, end_datetime)

    def get_daily_histogram(
        self,
        db_session: DbSession,
        start_datetime: datetime,
        end_datetime: datetime,
    ) -> list[int]:
        """Get daily histogram of data points for the given date range."""
        return self.crud.get_daily_histogram(db_session, start_datetime, end_datetime)

    def get_count_by_series_type(self, db_session: DbSession) -> list[tuple[int, int]]:
        """Get count of data points grouped by series type ID."""
        return self.crud.get_count_by_series_type(db_session)

    def get_count_by_source(self, db_session: DbSession) -> list[tuple[str | None, int]]:
        """Get count of data points grouped by source."""
        return self.crud.get_count_by_source(db_session)

    @handle_exceptions
    def get_timeseries(
        self,
        db_session: DbSession,
        user_id: UUID,
        types: list[SeriesType],
        params: TimeSeriesQueryParams,
        include_total: bool = False,
        requested_resolution: str = "raw",
    ) -> PaginatedResponse[TimeSeriesSample]:
        """Return samples for ``types`` in the ``params`` window.

        ``requested_resolution`` selects raw samples or server-side buckets;
        ``metadata.resolution`` always reports what was actually applied.
        """
        bucket_seconds = BUCKET_SECONDS_BY_RESOLUTION.get(requested_resolution)
        if bucket_seconds is not None:
            return self._get_bucketed_timeseries(
                db_session,
                user_id,
                types,
                params,
                resolution=requested_resolution,
                bucket_seconds=bucket_seconds,
                include_total=include_total,
            )

        samples, total_count = self.crud.get_samples(
            db_session,
            params,
            types,
            user_id,
            include_total=include_total,
        )

        limit = params.limit or 50
        has_more = len(samples) > limit

        # Check if this is backward pagination
        is_backward = params.cursor and params.cursor.startswith("prev_")

        # Trim to limit
        if has_more:
            samples = samples[-limit:] if is_backward else samples[:limit]

        # Generate cursors
        next_cursor = None
        previous_cursor = None

        if samples:
            # Always generate next_cursor if has_more
            if has_more:
                last_sample = samples[-1][0]
                next_cursor = encode_cursor(last_sample.recorded_at, last_sample.id, "next")

            # Generate previous_cursor only if:
            # 1. We used a cursor to get here (not the first page)
            # 2. There are more items before (for backward) OR we're doing forward navigation
            if params.cursor:
                # For backward navigation: only set previous_cursor if has_more
                # For forward navigation: always set previous_cursor
                if is_backward:
                    if has_more:
                        first_sample = samples[0][0]
                        previous_cursor = encode_cursor(first_sample.recorded_at, first_sample.id, "prev")
                else:
                    first_sample = samples[0][0]
                    previous_cursor = encode_cursor(first_sample.recorded_at, first_sample.id, "prev")

        # Map to response format
        data = []
        for sample, data_source in samples:
            series_type = get_series_type_from_id(sample.series_type_definition_id)
            unit = get_series_type_unit(series_type)

            # Build source from data source info if available
            source = None
            if data_source:
                source = SourceMetadata(
                    provider=data_source.source or "unknown",
                    device=data_source.device_model,
                )

            item = TimeSeriesSample(
                timestamp=sample.recorded_at,
                zone_offset=sample.zone_offset,
                type=series_type,
                value=float(sample.value),
                unit=unit,
                source=source,
                is_daily_total=sample.is_daily_total,
            )
            data.append(item)

        return PaginatedResponse(
            data=data,
            pagination=Pagination(
                has_more=has_more,
                next_cursor=next_cursor,
                previous_cursor=previous_cursor,
                total_count=total_count,
            ),
            metadata=TimeseriesMetadata(
                resolution="raw",
                sample_count=len(data),
                start_time=params.start_datetime,
                end_time=params.end_datetime,
            ),
        )

    def _get_bucketed_timeseries(
        self,
        db_session: DbSession,
        user_id: UUID,
        types: list[SeriesType],
        params: TimeSeriesQueryParams,
        resolution: str,
        bucket_seconds: int,
        include_total: bool,
    ) -> PaginatedResponse[TimeSeriesSample]:
        """Return one sample per (bucket, series type, data source).

        Aggregation is what makes a wide window affordable: a caller that needs
        six months of hourly shape gets a few thousand buckets instead of the
        hundreds of paged round-trips the raw path would take to cover the same
        span. The repository pages the buckets themselves, so response size and
        memory stay tied to the page rather than to the window.
        """
        buckets, total_count = self.crud.get_bucketed_samples(
            db_session,
            params,
            types,
            user_id,
            bucket_seconds=bucket_seconds,
            include_total=include_total,
        )

        limit = params.limit or 50
        is_backward = bool(params.cursor and params.cursor.startswith("prev_"))

        has_more = len(buckets) > limit
        if has_more:
            # The repository returns buckets ascending either way, so the extra
            # row marking a further page is at the front when paging backward.
            buckets = buckets[-limit:] if is_backward else buckets[:limit]

        # Cursor rules mirror the raw path so a caller's paging loop behaves the
        # same at either resolution.
        next_cursor = None
        previous_cursor = None
        if buckets:
            if has_more:
                next_cursor = encode_bucket_cursor(*_bucket_key(buckets[-1]))
            if params.cursor and (has_more or not is_backward):
                previous_cursor = encode_bucket_cursor(*_bucket_key(buckets[0]), direction="prev")

        data = []
        for bucket in buckets:
            series_type = get_series_type_from_id(bucket["series_type_definition_id"])
            method = get_aggregation_method(series_type)

            source = None
            if bucket["source"] or bucket["device_model"]:
                source = SourceMetadata(
                    provider=bucket["source"] or "unknown",
                    device=bucket["device_model"],
                )

            data.append(
                TimeSeriesSample(
                    timestamp=bucket["bucket_start"],
                    zone_offset=bucket["zone_offset"],
                    type=series_type,
                    value=_BUCKET_VALUE_BY_METHOD[method](bucket),
                    unit=get_series_type_unit(series_type),
                    source=source,
                    # A bucket aggregates intraday samples only; daily totals are
                    # excluded upstream, so no bucket is ever a daily total.
                    is_daily_total=False,
                )
            )

        return PaginatedResponse(
            data=data,
            pagination=Pagination(
                has_more=has_more,
                next_cursor=next_cursor,
                previous_cursor=previous_cursor,
                total_count=total_count,
            ),
            metadata=TimeseriesMetadata(
                resolution=resolution,  # ty:ignore[invalid-argument-type]
                sample_count=len(data),
                start_time=params.start_datetime,
                end_time=params.end_datetime,
            ),
        )


timeseries_service = TimeSeriesService(log=getLogger(__name__))
