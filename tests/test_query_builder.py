"""Unit tests for _build_query (events router) and _sign_cache/_verify_cache (stats router)."""

from datetime import datetime, timezone
import json

from pydantic import HttpUrl
import pytest

from services.events import build_query as _build_query
from services.stats import build_stats_pipeline as _build_stats_pipeline
from services.stats import sign_cache as _sign_cache
from services.stats import verify_cache as _verify_cache

SECRET = "test-secret"  # noqa: S105
T1 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
T2 = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# _build_query
# ---------------------------------------------------------------------------


def test_build_query_no_filters() -> None:
    assert _build_query(None, None, None, None, None) == {}


def test_build_query_type() -> None:
    q = _build_query("click", None, None, None, None)
    assert q == {"type": "click"}


def test_build_query_user_id() -> None:
    q = _build_query(None, "user-1", None, None, None)
    assert q == {"user_id": "user-1"}


def test_build_query_source_url_normalized() -> None:
    url = HttpUrl("https://example.com")
    q = _build_query(None, None, url, None, None)
    # HttpUrl normalizes to include trailing slash
    assert q == {"source_url": "https://example.com/"}


def test_build_query_source_url_already_normalized() -> None:
    url = HttpUrl("https://example.com/path")
    q = _build_query(None, None, url, None, None)
    assert q == {"source_url": "https://example.com/path"}


def test_build_query_date_from_only() -> None:
    q = _build_query(None, None, None, T1, None)
    assert q == {"timestamp": {"$gte": T1}}


def test_build_query_date_to_only() -> None:
    q = _build_query(None, None, None, None, T2)
    assert q == {"timestamp": {"$lte": T2}}


def test_build_query_date_range() -> None:
    q = _build_query(None, None, None, T1, T2)
    assert q == {"timestamp": {"$gte": T1, "$lte": T2}}


def test_build_query_all_filters() -> None:
    url = HttpUrl("https://example.com/")
    q = _build_query("view", "user-1", url, T1, T2)
    assert q["type"] == "view"
    assert q["user_id"] == "user-1"
    assert q["source_url"] == "https://example.com/"
    assert q["timestamp"] == {"$gte": T1, "$lte": T2}


def test_build_query_date_from_equal_to_date_to() -> None:
    q = _build_query(None, None, None, T1, T1)
    assert q == {"timestamp": {"$gte": T1, "$lte": T1}}


def test_build_query_no_timestamp_key_when_no_dates() -> None:
    q = _build_query("click", "user-1", None, None, None)
    assert "timestamp" not in q


# ---------------------------------------------------------------------------
# build_stats_pipeline
# ---------------------------------------------------------------------------


def test_build_stats_pipeline_always_starts_with_timestamp_match() -> None:
    from models.stats import StatsPeriod

    pipeline = _build_stats_pipeline(StatsPeriod.DAILY)
    match = pipeline[0]["$match"]
    assert "timestamp" in match
    assert "type" not in match
    assert pipeline[1] == {"$project": {"_id": 0, "type": 1, "timestamp": 1}}


def test_build_stats_pipeline_with_type_includes_type_in_match() -> None:
    from models.stats import StatsPeriod

    pipeline = _build_stats_pipeline(StatsPeriod.DAILY, type="pageview")
    match = pipeline[0]["$match"]
    assert match["type"] == "pageview"
    assert "timestamp" in match
    assert pipeline[1] == {"$project": {"_id": 0, "type": 1, "timestamp": 1}}


# ---------------------------------------------------------------------------
# _sign_cache
# ---------------------------------------------------------------------------


def test_sign_cache_returns_valid_json() -> None:
    result = _sign_cache([{"type": "click", "count": 5}], SECRET)
    outer = json.loads(result)
    assert "d" in outer
    assert "s" in outer


def test_sign_cache_payload_is_serialized_buckets() -> None:
    buckets = [{"type": "click", "count": 3}]
    outer = json.loads(_sign_cache(buckets, SECRET))
    assert json.loads(outer["d"]) == buckets


def test_sign_cache_signature_is_hex_string() -> None:
    outer = json.loads(_sign_cache([], SECRET))
    assert all(c in "0123456789abcdef" for c in outer["s"])
    assert len(outer["s"]) == 64  # SHA-256 hex digest


def test_sign_cache_different_secrets_produce_different_signatures() -> None:
    buckets = [{"type": "click", "count": 1}]
    sig1 = json.loads(_sign_cache(buckets, "secret-a"))["s"]
    sig2 = json.loads(_sign_cache(buckets, "secret-b"))["s"]
    assert sig1 != sig2


def test_sign_cache_empty_buckets() -> None:
    outer = json.loads(_sign_cache([], SECRET))
    assert json.loads(outer["d"]) == []


# ---------------------------------------------------------------------------
# _verify_cache
# ---------------------------------------------------------------------------


def test_verify_cache_roundtrip() -> None:
    buckets = [{"type": "click", "count": 7}]
    raw = _sign_cache(buckets, SECRET)
    assert _verify_cache(raw, SECRET) == buckets


def test_verify_cache_wrong_secret_returns_none() -> None:
    raw = _sign_cache([{"type": "view", "count": 1}], SECRET)
    assert _verify_cache(raw, "wrong-secret") is None


def test_verify_cache_tampered_payload_returns_none() -> None:
    outer = json.loads(_sign_cache([{"type": "click", "count": 1}], SECRET))
    outer["d"] = json.dumps([{"type": "click", "count": 999}])
    assert _verify_cache(json.dumps(outer), SECRET) is None


def test_verify_cache_tampered_signature_returns_none() -> None:
    outer = json.loads(_sign_cache([{"type": "click", "count": 1}], SECRET))
    outer["s"] = "a" * 64
    assert _verify_cache(json.dumps(outer), SECRET) is None


def test_verify_cache_missing_signature_key_returns_none() -> None:
    outer = json.loads(_sign_cache([{"type": "click", "count": 1}], SECRET))
    del outer["s"]
    assert _verify_cache(json.dumps(outer), SECRET) is None


def test_verify_cache_missing_payload_key_returns_none() -> None:
    outer = json.loads(_sign_cache([{"type": "click", "count": 1}], SECRET))
    del outer["d"]
    assert _verify_cache(json.dumps(outer), SECRET) is None


def test_verify_cache_not_json_returns_none() -> None:
    assert _verify_cache("not json at all", SECRET) is None


def test_verify_cache_empty_string_returns_none() -> None:
    assert _verify_cache("", SECRET) is None


def test_verify_cache_json_array_returns_none() -> None:
    # Valid JSON but wrong shape (array instead of object)
    assert _verify_cache("[]", SECRET) is None


def test_verify_cache_empty_buckets_roundtrip() -> None:
    raw = _sign_cache([], SECRET)
    assert _verify_cache(raw, SECRET) == []
