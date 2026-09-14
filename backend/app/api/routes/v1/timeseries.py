from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Query

from app.database import DbSession
from app.schemas.enums import SeriesType
from app.schemas.model_crud.activities import TimeSeriesQueryParams
from app.schemas.responses.activity import TimeSeriesSample
from app.schemas.utils import PaginatedResponse
from app.services import ApiKeyDep, timeseries_service
from app.utils.dates import DateTimeQueryParam, parse_query_datetime

router = APIRouter()


@router.get("/users/{user_id}/timeseries")
def get_timeseries(
    user_id: UUID,
    start_time: DateTimeQueryParam,
    end_time: DateTimeQueryParam,
    db: DbSession,
    _api_key: ApiKeyDep,
    types: Annotated[list[SeriesType], Query()] = [],
    resolution: Annotated[
        Literal["raw", "1min", "5min", "15min", "1hour"],
        Query(
            description=(
                "Bucket size. `raw` returns individual samples; any other value "
                "aggregates each (bucket, type, device) with that type's method "
                "(sum for cumulative types, average for rates, max for peaks) and "
                "excludes provider daily totals, which belong to a whole day "
                "rather than one bucket. `metadata.resolution` reports the "
                "resolution actually applied. Bucket cursors are not "
                "interchangeable with raw cursors."
            ),
        ),
    ] = "raw",
    cursor: str | None = None,
    limit: Annotated[
        int,
        Query(
            ge=1,
            le=1000,
            description=(
                "Page size. Matches the 1000 the query schema already allowed; the "
                "route was the tighter of the two, which made a wide window cost "
                "ten times the round trips it needed."
            ),
        ),
    ] = 50,
    include_total: Annotated[
        bool,
        Query(description="Include total_count. Costs a COUNT(*) over the whole window."),
    ] = False,
) -> PaginatedResponse[TimeSeriesSample]:
    """Returns granular time series data (biometrics or activity)."""
    params = TimeSeriesQueryParams(
        start_datetime=parse_query_datetime(start_time),
        end_datetime=parse_query_datetime(end_time),
        limit=limit,
        cursor=cursor,
    )
    return timeseries_service.get_timeseries(
        db,
        user_id,
        types,
        params,
        include_total=include_total,
        requested_resolution=resolution,
    )
