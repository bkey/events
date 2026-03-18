from datetime import datetime, timedelta, timezone
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
    period: StatsPeriod, type: str | None = None, lookback_days: int = 90
) -> list[dict[str, Any]]:
    """Build a MongoDB aggregation pipeline for periodic event counts.

    Scans at most `lookback_days` of data (default 90) to keep aggregations
    bounded as the collection grows.
    """
    since = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    match: dict[str, Any] = {"timestamp": {"$gte": since}}
    if type is not None:
        match["type"] = type
    return [
        {"$match": match},
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


def build_realtime_pipeline(since: datetime) -> list[dict[str, Any]]:
    """Build a MongoDB aggregation pipeline for realtime event counts."""
    return [
        {"$match": {"timestamp": {"$gte": since}}},
        {"$project": {"_id": 0, "type": 1}},
        {"$group": {"_id": "$type", "count": {"$sum": 1}}},
    ]
