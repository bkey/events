from datetime import datetime, timedelta, timezone
import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import ValidationError
from pymongo.errors import PyMongoError

from config.limiter import limiter
from config.settings import Settings, get_settings, settings
from models.stats import EventStatsBucket, RealtimeStatsBucket, StatsPeriod
from services.stats import (
    build_realtime_pipeline,
    build_stats_pipeline,
    sign_cache,
    verify_cache,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/stats", tags=["stats"])


@router.get(
    "/",
    summary="Get Event Stats",
    description="Returns event counts grouped by type and truncated timestamp for the requested period.",
    response_model=list[EventStatsBucket],
    responses={
        422: {"description": "Invalid or missing period parameter"},
        429: {"description": "Rate limit exceeded (30 requests/minute)"},
        503: {"description": "MongoDB is unavailable"},
    },
)
@limiter.limit("30/minute")  # type: ignore[untyped-decorator]
async def get_event_stats(
    request: Request,
    settings: Annotated[Settings, Depends(get_settings)],
    period: StatsPeriod = Query(
        ..., description="Aggregation period: hourly, daily, or weekly"
    ),
    type: str | None = Query(None, description="Filter by event type"),
) -> list[EventStatsBucket]:
    """Return event counts grouped by type and truncated timestamp."""
    collection = request.app.state.mongodb_client[settings.db_name][
        settings.events_collection
    ]
    try:
        results = await (
            await collection.aggregate(
                build_stats_pipeline(period, type),
                allowDiskUse=True,
                hint={"type": 1, "timestamp": -1},
            )
        ).to_list(None)
    except PyMongoError as exc:
        raise HTTPException(
            status_code=503, detail="Database is unavailable — try again later"
        ) from exc
    return [EventStatsBucket.model_validate(doc) for doc in results]


@router.get(
    "/realtime",
    summary="Get Realtime Event Stats",
    description=(
        f"Returns event counts by type over the last {settings.realtime_window_seconds // 60} minutes. "
        f"Results are cached for {settings.realtime_cache_ttl}s and served from Redis when available."
    ),
    response_model=list[RealtimeStatsBucket],
    responses={
        429: {"description": "Rate limit exceeded (60 requests/minute)"},
        503: {"description": "MongoDB is unavailable"},
    },
)
@limiter.limit("60/minute")  # type: ignore[untyped-decorator]
async def get_event_stats_realtime(
    request: Request,
    settings: Annotated[Settings, Depends(get_settings)],
) -> list[RealtimeStatsBucket]:
    """Return per-type event counts for the last 5 minutes, served from cache when available."""
    redis = request.app.state.redis_client
    if redis is not None:
        cached = await redis.get(settings.realtime_cache_key)
        if cached:
            try:
                buckets_raw = verify_cache(
                    cached, settings.cache_hmac_secret.get_secret_value()
                )
                if buckets_raw is None:
                    logger.warning(
                        "Discarding realtime stats cache entry with invalid or missing signature"
                    )
                else:
                    return [RealtimeStatsBucket(**b) for b in buckets_raw]
            except (ValueError, ValidationError):
                logger.warning("Discarding invalid realtime stats cache entry")

    since = datetime.now(timezone.utc) - timedelta(
        seconds=settings.realtime_window_seconds
    )
    collection = request.app.state.mongodb_client[settings.db_name][
        settings.events_collection
    ]
    try:
        results = await (
            await collection.aggregate(
                build_realtime_pipeline(since), allowDiskUse=True
            )
        ).to_list(None)
    except PyMongoError as exc:
        raise HTTPException(
            status_code=503, detail="Database is unavailable — try again later"
        ) from exc

    buckets = [RealtimeStatsBucket(type=r["_id"], count=r["count"]) for r in results]

    if redis is not None:
        await redis.set(
            settings.realtime_cache_key,
            sign_cache(
                [b.model_dump() for b in buckets],
                settings.cache_hmac_secret.get_secret_value(),
            ),
            ex=settings.realtime_cache_ttl,
        )

    return buckets
