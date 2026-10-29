from datetime import date, datetime
from typing import NotRequired, TypedDict
from uuid import UUID

from pydantic import BaseModel

from app.schemas.enums import SeriesType
from app.schemas.utils import SourceMetadata
from app.utils.dates import ZoneOffset


class TimeSeriesSample(BaseModel):
    timestamp: datetime
    zone_offset: ZoneOffset = None
    type: SeriesType
    value: float | int
    unit: str
    source: SourceMetadata | None = None
    # True = daily total. False/None = not a daily total (summable sample); None is a
    # legacy row and is treated as False by the aggregation.
    is_daily_total: bool | None = None


class BucketedSampleResult(TypedDict):
    """One downsampled bucket, keyed by (bucket_start, series type, data source).

    The repository returns every aggregate column and the service picks the one
    that matches the series type's ``AggregationMethod`` — grouping by series
    type means each row belongs to exactly one type, so a single query can serve
    SUM, AVG and MAX types without a per-type CASE.

    The aggregates are non-optional: ``value`` is NOT NULL and a group exists
    only because it has at least one row.
    """

    bucket_start: datetime
    series_type_definition_id: int
    data_source_id: UUID
    source: str | None
    device_model: str | None
    zone_offset: str | None
    sum_value: float
    avg_value: float
    max_value: float


class ActivityAggregateResult(TypedDict):
    """Result from daily activity aggregation query."""

    activity_date: date
    provider: str | None
    source: str | None
    device_model: str | None
    steps_sum: int
    active_energy_sum: float
    basal_energy_sum: float
    hr_avg: int | None
    hr_max: int | None
    hr_min: int | None
    distance_sum: float | None
    flights_climbed_sum: int | None
    active_time_minutes: int | None  # Provider-reported daily active time; None when not reported
    steps_from_daily_total: NotRequired[bool]


class ActiveMinutesResult(TypedDict):
    """Result from daily active/sedentary minutes query."""

    activity_date: date
    source: str | None
    device_model: str | None
    active_minutes: int
    tracked_minutes: int
    sedentary_minutes: int


class IntensityMinutesResult(TypedDict):
    """Result from daily intensity minutes query."""

    activity_date: date
    source: str | None
    device_model: str | None
    light_minutes: int
    moderate_minutes: int
    vigorous_minutes: int
