from datetime import datetime
import logging

from pydantic import HttpUrl, ValidationError

from models.events import Event

logger = logging.getLogger(__name__)


def build_query(
    type: str | None,
    user_id: str | None,
    source_url: HttpUrl | None,
    date_from: datetime | None,
    date_to: datetime | None,
) -> dict[str, object]:
    """Build a MongoDB filter document from optional query parameters."""
    query: dict[str, object] = {}
    if type is not None:
        query["type"] = type
    if user_id is not None:
        query["user_id"] = user_id
    if source_url is not None:
        query["source_url"] = str(source_url)
    if date_from is not None or date_to is not None:
        ts: dict[str, datetime] = {}
        if date_from is not None:
            ts["$gte"] = date_from
        if date_to is not None:
            ts["$lte"] = date_to
        query["timestamp"] = ts
    return query


def deserialize_events(docs: list[dict[str, object]]) -> list[Event]:
    """Convert raw MongoDB documents to Event models, skipping malformed entries."""
    valid = []
    for doc in docs:
        try:
            valid.append(Event(**doc))
        except ValidationError:
            logger.warning(
                "Skipping malformed event document _id=%s from MongoDB", doc.get("_id")
            )
    return valid
