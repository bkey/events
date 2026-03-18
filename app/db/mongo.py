import logging
from typing import Any

from pymongo import ASCENDING, DESCENDING, AsyncMongoClient, MongoClient
from pymongo.errors import PyMongoError
from pymongo.operations import IndexModel

from config.settings import settings
from db._utils import redact_url

logger = logging.getLogger(__name__)

# Indexes are designed around the GET /events filter patterns:
#   - unique event_id index provides client-side deduplication
#   - compound (field, timestamp) covers both single-field and combined filters
#   - bare timestamp index serves stats aggregation and realtime range queries
_INDEXES = [
    IndexModel([("event_id", ASCENDING)], unique=True),
    IndexModel([("type", ASCENDING), ("timestamp", DESCENDING)]),
    IndexModel([("user_id", ASCENDING), ("timestamp", DESCENDING)]),
    IndexModel([("source_url", ASCENDING), ("timestamp", DESCENDING)]),
    IndexModel([("timestamp", DESCENDING)]),
]

# Schema validator rejects documents missing required fields or with wrong types.
# validationAction="error" causes MongoDB to reject the write rather than just log it,
# catching worker serialization bugs at insert time rather than silently at query time.
# validationLevel="moderate" only validates new inserts and updates, not existing docs.
_EVENTS_SCHEMA: dict[str, Any] = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["_id", "event_id", "type", "timestamp", "user_id", "source_url"],
        "properties": {
            "_id": {"bsonType": "string"},
            "event_id": {"bsonType": "string"},
            "type": {"bsonType": "string"},
            "timestamp": {"bsonType": "date"},
            "user_id": {"bsonType": "string"},
            "source_url": {"bsonType": "string"},
            "metadata": {"bsonType": "object"},
        },
    }
}


async def ensure_indexes(client: AsyncMongoClient[dict[str, object]]) -> None:
    """Create collection indexes and schema validator if they do not already exist. Idempotent."""
    db = client[settings.db_name]
    collection = db[settings.events_collection]
    await collection.create_indexes(_INDEXES)
    await db.command(
        "collMod",
        settings.events_collection,
        validator=_EVENTS_SCHEMA,
        validationLevel="moderate",
        validationAction="error",
    )
    logger.info(
        "MongoDB indexes and schema validator ensured on %s.%s",
        settings.db_name,
        settings.events_collection,
    )

    dlq_ttl_seconds = settings.dlq_ttl_days * 24 * 60 * 60
    await db[settings.dlq_collection].create_index(
        "failed_at", expireAfterSeconds=dlq_ttl_seconds
    )
    logger.info(
        "DLQ TTL index ensured on %s.%s (expiry: %d days)",
        settings.db_name,
        settings.dlq_collection,
        settings.dlq_ttl_days,
    )


async def connect_db() -> AsyncMongoClient[dict[str, object]]:
    """Connect to MongoDB and verify the connection with a ping."""
    client: AsyncMongoClient[dict[str, object]] = AsyncMongoClient(
        settings.mongodb_url, maxPoolSize=50
    )
    try:
        await client.admin.command("ping")
    except PyMongoError as e:
        await client.close()
        raise RuntimeError(
            f"Could not connect to MongoDB at {redact_url(settings.mongodb_url)}: {e}"
        ) from e
    return client


def connect_db_sync() -> MongoClient[dict[str, object]]:
    """Connect to MongoDB synchronously and verify the connection with a ping."""
    client: MongoClient[dict[str, object]] = MongoClient(
        settings.mongodb_url, maxPoolSize=50
    )
    try:
        client.admin.command("ping")
    except PyMongoError as e:
        client.close()
        raise RuntimeError(
            f"Could not connect to MongoDB at {redact_url(settings.mongodb_url)}: {e}"
        ) from e
    return client
