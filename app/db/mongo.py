import logging

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


async def ensure_indexes(client: AsyncMongoClient[dict[str, object]]) -> None:
    """Create collection indexes if they do not already exist. Idempotent."""
    collection = client[settings.db_name][settings.events_collection]
    await collection.create_indexes(_INDEXES)
    logger.info(
        "MongoDB indexes ensured on %s.%s", settings.db_name, settings.events_collection
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
