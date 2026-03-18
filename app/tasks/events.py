from datetime import datetime
import logging
from typing import Any

from celery.exceptions import MaxRetriesExceededError, Reject
from elasticsearch import Elasticsearch
from pymongo import MongoClient
from pymongo.errors import BulkWriteError, PyMongoError

from config.settings import settings
from db.elastic import connect_elasticsearch_sync
from db.mongo import connect_db_sync
from tasks.dlq import write_to_dlq
from tasks.worker import celery_app

logger = logging.getLogger(__name__)

_MONGO_DUPLICATE_KEY_ERROR = 11000


class EventsTask(celery_app.Task):  # type: ignore[misc]
    """Base task that lazily initializes per-process DB clients on first use."""

    _mongo_client: MongoClient[dict[str, object]] | None = None
    _es_client: Elasticsearch | None = None
    _es_initialized: bool = False

    @property
    def mongo_client(self) -> MongoClient[dict[str, object]]:
        if self._mongo_client is None:
            self._mongo_client = connect_db_sync()
        return self._mongo_client

    @property
    def es_client(self) -> Elasticsearch | None:
        if not self._es_initialized:
            try:
                self._es_client = connect_elasticsearch_sync()
            except RuntimeError:
                logger.warning(
                    "Elasticsearch unavailable — events will not be indexed until worker restarts"
                )
            self._es_initialized = True
        return self._es_client


def _parse_timestamps(events: list[dict[str, object]]) -> list[dict[str, object]]:
    """Convert ISO timestamp strings to datetime objects.

    Celery's JSON serializer converts datetime objects to ISO strings in transit.
    MongoDB requires native datetime objects for date comparisons and aggregations.
    """
    parsed = []
    for event in events:
        ts = event.get("timestamp")
        if isinstance(ts, str):
            event = {**event, "timestamp": datetime.fromisoformat(ts)}
        parsed.append(event)
    return parsed


def _persist_to_mongo(task: EventsTask, events: list[dict[str, object]]) -> int:
    """Insert events into MongoDB. Returns the number of documents inserted.

    Swallows duplicate key errors (idempotent retries), retries transient errors
    up to max_retries, and writes to the DLQ on exhaustion.
    """
    collection = task.mongo_client[settings.db_name][settings.events_collection]
    events = _parse_timestamps(events)
    try:
        result = collection.insert_many(events, ordered=False)
        return len(result.inserted_ids)
    except BulkWriteError as e:
        non_duplicate_errors = [
            err
            for err in e.details["writeErrors"]
            if err["code"] != _MONGO_DUPLICATE_KEY_ERROR
        ]
        if non_duplicate_errors:
            try:
                raise task.retry(exc=e, countdown=2**task.request.retries)
            except MaxRetriesExceededError as mre:
                write_to_dlq(task.mongo_client, events, str(mre), task.request.retries)
                raise Reject(f"Max retries exceeded: {mre}", requeue=False) from mre
        inserted = int(e.details["nInserted"])
        duplicates = len(e.details["writeErrors"])
        logger.warning(
            "Bulk insert: %d inserted, %d duplicate(s) skipped", inserted, duplicates
        )
        return inserted
    except PyMongoError as exc:
        try:
            raise task.retry(exc=exc, countdown=2**task.request.retries)
        except MaxRetriesExceededError as e:
            write_to_dlq(task.mongo_client, events, str(e), task.request.retries)
            raise Reject(f"Max retries exceeded: {e}", requeue=False) from e


def _build_bulk_body(events: list[dict[str, object]]) -> list[Any]:
    """Build the ES bulk operations list from a batch of event documents."""
    return [
        op
        for event in events
        for op in (
            {"index": {"_index": settings.events_index, "_id": event["_id"]}},
            {k: v for k, v in event.items() if k != "_id"},
        )
    ]


def _handle_bulk_errors(
    task: EventsTask,
    response: Any,
    events: list[dict[str, object]],
) -> None:
    """Log and DLQ any per-item failures from an ES bulk response."""
    failed_items = [
        item["index"]
        for item in response["items"]
        if item.get("index", {}).get("error")
    ]
    failed_ids = {f["_id"] for f in failed_items}
    failed_events = [e for e in events if e["_id"] in failed_ids]
    logger.warning(
        "Elasticsearch bulk index: %d/%d item(s) failed",
        len(failed_events),
        len(events),
        extra={"failed_ids": list(failed_ids)},
    )
    write_to_dlq(
        task.mongo_client, failed_events, "Elasticsearch bulk index failure", 0
    )


def _index_in_elasticsearch(task: EventsTask, events: list[dict[str, object]]) -> None:
    """Bulk-index events in Elasticsearch. Partial failures are written to the DLQ."""
    if task.es_client is None:
        logger.warning(
            "Elasticsearch client unavailable — skipping index for %d events",
            len(events),
        )
        return

    try:
        response = task.es_client.bulk(operations=_build_bulk_body(events))
        if response.get("errors"):
            _handle_bulk_errors(task, response, events)
    except Exception:
        logger.warning(
            "Failed to index %d events in Elasticsearch", len(events), exc_info=True
        )
        write_to_dlq(task.mongo_client, events, "Elasticsearch bulk request failed", 0)


@celery_app.task(  # type: ignore[untyped-decorator]
    name="tasks.events.process_events",
    bind=True,
    base=EventsTask,
    max_retries=3,
    acks_late=True,
)
def process_events(self: EventsTask, events: list[dict[str, object]]) -> dict[str, int]:
    """Persist a batch of events to MongoDB and index them in Elasticsearch."""
    inserted = _persist_to_mongo(self, events)
    _index_in_elasticsearch(self, events)
    return {"inserted": inserted}
