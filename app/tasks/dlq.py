from datetime import datetime, timezone
import logging

from pymongo import MongoClient

from config.settings import settings

logger = logging.getLogger(__name__)


def write_to_dlq(
    mongo_client: MongoClient[dict[str, object]],
    events: list[dict[str, object]],
    reason: str,
    retries: int,
) -> None:
    """Write a failed event batch to the dead-letter collection for later inspection."""
    try:
        mongo_client[settings.db_name][settings.dlq_collection].insert_one(
            {
                "events": events,
                "reason": reason,
                "retries": retries,
                "failed_at": datetime.now(timezone.utc),
            }
        )
        logger.error(
            "Wrote %d events to DLQ after %d retries: %s",
            len(events),
            retries,
            reason,
        )
    except Exception:
        logger.exception(
            "Failed to write %d events to DLQ — events are lost", len(events)
        )
