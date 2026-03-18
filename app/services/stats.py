from datetime import datetime
import hashlib
import hmac
import json
import logging
from typing import Any

from models.stats import StatsPeriod

logger = logging.getLogger(__name__)

PERIOD_TO_UNIT = {
    StatsPeriod.HOURLY: "hour",
    StatsPeriod.DAILY: "day",
    StatsPeriod.WEEKLY: "week",
}


def sign_cache(buckets: list[Any], secret: str) -> str:
    """Serialize buckets and append an HMAC-SHA256 signature."""
    payload = json.dumps(buckets)
    sig = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return json.dumps({"d": payload, "s": sig})


def verify_cache(raw: str, secret: str) -> list[Any] | None:
    """Return the deserialized bucket list if the signature is valid, else None."""
    try:
        outer = json.loads(raw)
        payload: str = outer["d"]
        sig: str = outer["s"]
    except (ValueError, KeyError, TypeError):
        return None
    expected = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig, expected):
        logger.warning("Realtime stats cache entry failed HMAC verification")
        return None
    result: list[Any] = json.loads(payload)
    return result


def build_stats_pipeline(
    period: StatsPeriod, type: str | None = None
) -> list[dict[str, Any]]:
    """Build a MongoDB aggregation pipeline for periodic event counts."""
    pipeline: list[dict[str, Any]] = []
    if type is not None:
        pipeline.append({"$match": {"type": type}})
    pipeline += [
        {"$project": {"_id": 0, "type": 1, "timestamp": 1}},
        {
            "$group": {
                "_id": {
                    "type": "$type",
                    "period": {
                        "$dateTrunc": {
                            "date": "$timestamp",
                            "unit": PERIOD_TO_UNIT[period],
                        }
                    },
                },
                "count": {"$sum": 1},
            }
        },
        {"$sort": {"_id.period": -1, "_id.type": 1}},
    ]
    return pipeline


def build_realtime_pipeline(since: datetime) -> list[dict[str, Any]]:
    """Build a MongoDB aggregation pipeline for realtime event counts."""
    return [
        {"$match": {"timestamp": {"$gte": since}}},
        {"$project": {"_id": 0, "type": 1}},
        {"$group": {"_id": "$type", "count": {"$sum": 1}}},
    ]
