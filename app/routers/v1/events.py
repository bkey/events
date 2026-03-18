import asyncio
from datetime import datetime
from typing import Annotated
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from kombu.exceptions import OperationalError
from pydantic import Field, HttpUrl
from pymongo.errors import PyMongoError

from config.limiter import limiter
from config.settings import Settings, get_settings, settings
from models.events import Event, PaginatedEvents, QueuedResponse
from services.events import build_query, deserialize_events
from tasks.event_queue import enqueue_events

router = APIRouter(tags=["events"])


@router.post(
    "/",
    summary="Create Events",
    description="Enqueue a batch of events for async processing. Returns the number of events accepted.",
    status_code=202,
    response_model=QueuedResponse,
    responses={
        422: {"description": "Request body failed validation"},
        429: {"description": "Rate limit exceeded (100 requests/minute)"},
        503: {"description": "Event queue (Redis/Celery) is unavailable"},
    },
)
@limiter.limit("100/minute")  # type: ignore[untyped-decorator]
async def create_event(
    request: Request,
    events: Annotated[
        list[Event], Field(min_length=1, max_length=settings.max_batch_size)
    ],
) -> QueuedResponse:
    """Enqueue a batch of events for async persistence via Celery."""
    docs = [
        {
            "_id": str(uuid.uuid4()),
            **event.model_dump(exclude={"event_id"}),
            "event_id": event.event_id or str(uuid.uuid4()),
            "source_url": str(event.source_url),
        }
        for event in events
    ]
    try:
        enqueue_events(docs)
    except OperationalError as exc:
        raise HTTPException(
            status_code=503, detail="Event queue is unavailable — try again later"
        ) from exc
    return QueuedResponse(queued=len(docs))


@router.get(
    "/",
    summary="Gets Events",
    description="Returns a paginated list of events from MongoDB, optionally filtered by type, user, URL, or date range.",
    response_model=PaginatedEvents,
    response_model_exclude_none=True,
    responses={
        422: {"description": "Query parameter failed validation"},
        429: {"description": "Rate limit exceeded (60 requests/minute)"},
        503: {"description": "MongoDB is unavailable"},
    },
)
@limiter.limit("60/minute")  # type: ignore[untyped-decorator]
async def get_events(
    request: Request,
    settings: Annotated[Settings, Depends(get_settings)],
    skip: int = Query(0, ge=0, description="Number of items to skip"),
    limit: int = Query(10, ge=1, le=100, description="Maximum items to return"),
    type: str | None = Query(None, description="Filter by event type"),
    user_id: str | None = Query(None, description="Filter by user ID"),
    source_url: HttpUrl | None = Query(None, description="Filter by source URL"),
    date_from: datetime | None = Query(
        None, description="Filter events on or after this timestamp (ISO 8601)"
    ),
    date_to: datetime | None = Query(
        None, description="Filter events on or before this timestamp (ISO 8601)"
    ),
) -> PaginatedEvents:
    """Fetch a paginated, optionally filtered list of events from MongoDB."""
    query = build_query(type, user_id, source_url, date_from, date_to)
    collection = request.app.state.mongodb_client[settings.db_name][
        settings.events_collection
    ]
    try:
        total, results = await asyncio.gather(
            collection.count_documents(query),
            collection.find(query, {"_id": 0}).skip(skip).limit(limit).to_list(limit),
        )
    except PyMongoError as exc:
        raise HTTPException(
            status_code=503, detail="Database is unavailable — try again later"
        ) from exc
    return PaginatedEvents(
        total=total, skip=skip, limit=limit, results=deserialize_events(results)
    )
