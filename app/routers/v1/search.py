import asyncio
import logging
import re
from typing import Annotated, cast

from elasticsearch import AsyncElasticsearch, TransportError
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from state import AppState

from config.limiter import limiter
from config.settings import Settings, get_settings
from db.elastic import connect_elasticsearch, ensure_index
from models.events import Event

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/search", tags=["search"])

SEARCH_FIELDS = ["type", "user_id", "source_url", "metadata.*"]

# Allowed characters in a metadata key passed as a query parameter.
# Prevents field-name injection into Elasticsearch queries.
_METADATA_KEY_RE = re.compile(r"^[a-zA-Z0-9_.-]+$")

_reconnect_lock = asyncio.Lock()


async def _get_es_client(request: Request) -> AsyncElasticsearch:
    """Return the ES client, attempting to reconnect if it was unavailable at startup."""
    state: AppState = request.app.state
    if state.elasticsearch_client is not None:
        return cast(AsyncElasticsearch, state.elasticsearch_client)
    async with _reconnect_lock:
        # Re-check after acquiring the lock — another coroutine may have reconnected.
        if state.elasticsearch_client is not None:
            return state.elasticsearch_client
        try:
            state.elasticsearch_client = await connect_elasticsearch()
            await ensure_index(state.elasticsearch_client)
            logger.info("Reconnected to Elasticsearch")
        except RuntimeError as exc:
            raise HTTPException(
                status_code=503,
                detail="Search is unavailable — Elasticsearch is unreachable",
            ) from exc
    return cast(AsyncElasticsearch, state.elasticsearch_client)


@router.get(
    "/",
    summary="Search events",
    description=(
        "Full-text search over events using Elasticsearch simple_query_string syntax. "
        "Searches across `type`, `user_id`, `source_url`, and all `metadata` fields by default. "
        'Supports: `"exact phrase"`, `term*` (prefix), `+required`, `term1 | term2` (OR).'
    ),
    response_model=list[Event],
    response_model_exclude_none=True,
    responses={
        422: {"description": "Query parameter failed validation"},
        429: {"description": "Rate limit exceeded (30 requests/minute)"},
        503: {"description": "Elasticsearch is unavailable"},
    },
)
@limiter.limit("30/minute")  # type: ignore[untyped-decorator]
async def get_event_search(
    request: Request,
    settings: Annotated[Settings, Depends(get_settings)],
    q: str = Query(
        ...,
        min_length=1,
        description=(
            "Search query. Supports simple_query_string syntax: "
            '"exact phrase", term*, +required, term1 | term2.'
        ),
    ),
    limit: int = Query(10, ge=1, le=100, description="Maximum items to return"),
    metadata_key: str | None = Query(
        None,
        description=(
            "Restrict search to a single metadata key (e.g. 'campaign'). "
            "When omitted, all fields are searched. "
            "Only letters, digits, underscores, hyphens, and dots are allowed."
        ),
    ),
) -> list[Event]:
    """Search for events in Elasticsearch using a simple_query_string query."""
    if metadata_key is not None:
        if not _METADATA_KEY_RE.match(metadata_key):
            raise HTTPException(
                status_code=422,
                detail="metadata_key may only contain letters, digits, underscores, hyphens, and dots",
            )
        fields = [f"metadata.{metadata_key}"]
    else:
        fields = SEARCH_FIELDS

    es = await _get_es_client(request)
    try:
        response = await es.search(
            index=settings.events_index,
            query={
                "simple_query_string": {
                    "query": q,
                    "fields": fields,
                    "default_operator": "and",
                    "flags": "PHRASE|PREFIX|AND|OR|WHITESPACE",
                }
            },
            size=limit,
        )
    except TransportError as e:
        raise HTTPException(
            status_code=503,
            detail="Search is unavailable — Elasticsearch is unreachable",
        ) from e
    return [
        Event(**hit["_source"])
        for hit in response.get("hits", {}).get("hits", [])
        if "_source" in hit
    ]
