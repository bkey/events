from datetime import datetime, timezone
import json
from unittest.mock import AsyncMock, MagicMock

from conftest import TEST_SETTINGS, build_collection_mock, build_mongo_mock
from fastapi.testclient import TestClient
from main import create_app
from pymongo.errors import PyMongoError
import pytest

from config.settings import get_settings
from services.stats import sign_cache as _sign_cache

STATS_URL = "/v1/events/stats/"
REALTIME_URL = "/v1/events/stats/realtime"

STATS_DOC = {
    "_id": {"type": "pageview", "period": datetime(2024, 1, 1, tzinfo=timezone.utc)},
    "count": 10,
}

REALTIME_DOC = {"_id": "pageview", "count": 5}


# --- GET /v1/events/stats/ ---


@pytest.mark.parametrize("period", ["hourly", "daily", "weekly"])
def test_get_stats_valid_periods(client: TestClient, period: str) -> None:
    response = client.get(STATS_URL, params={"period": period})
    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_get_stats_returns_buckets(
    client: TestClient, mock_collection: MagicMock
) -> None:
    mock_collection.aggregate.return_value.to_list.return_value = [STATS_DOC]
    response = client.get(STATS_URL, params={"period": "daily"})
    assert response.status_code == 200
    results = response.json()
    assert len(results) == 1
    assert results[0]["type"] == "pageview"
    assert results[0]["count"] == 10


def test_get_stats_type_filter_returns_matching_buckets(
    client: TestClient, mock_collection: MagicMock
) -> None:
    mock_collection.aggregate.return_value.to_list.return_value = [STATS_DOC]
    response = client.get(STATS_URL, params={"period": "daily", "type": "pageview"})
    assert response.status_code == 200
    results = response.json()
    assert len(results) == 1
    assert results[0]["type"] == "pageview"


def test_get_stats_type_filter_passes_to_pipeline(
    client: TestClient, mock_collection: MagicMock
) -> None:
    mock_collection.aggregate.return_value.to_list.return_value = []
    client.get(STATS_URL, params={"period": "daily", "type": "click"})
    pipeline = mock_collection.aggregate.call_args[0][0]
    assert pipeline[0] == {"$match": {"type": "click"}}


def test_get_stats_no_type_filter_omits_match_stage(
    client: TestClient, mock_collection: MagicMock
) -> None:
    mock_collection.aggregate.return_value.to_list.return_value = []
    client.get(STATS_URL, params={"period": "daily"})
    pipeline = mock_collection.aggregate.call_args[0][0]
    assert pipeline[0] == {"$project": {"_id": 0, "type": 1, "timestamp": 1}}


def test_get_stats_invalid_period_rejected(client: TestClient) -> None:
    response = client.get(STATS_URL, params={"period": "yearly"})
    assert response.status_code == 422


def test_get_stats_missing_period_rejected(client: TestClient) -> None:
    response = client.get(STATS_URL)
    assert response.status_code == 422


def test_get_stats_mongo_unavailable_returns_503(
    client: TestClient, mock_collection: MagicMock
) -> None:
    mock_collection.aggregate.return_value.to_list.side_effect = PyMongoError()
    response = client.get(STATS_URL, params={"period": "daily"})
    assert response.status_code == 503


# --- GET /v1/events/stats/realtime ---


def test_get_realtime_returns_200(client: TestClient) -> None:
    response = client.get(REALTIME_URL)
    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_get_realtime_cache_miss_queries_mongo(
    client: TestClient, mock_collection: MagicMock, mock_redis: MagicMock
) -> None:
    mock_redis.get.return_value = None
    mock_collection.aggregate.return_value.to_list.return_value = [REALTIME_DOC]

    response = client.get(REALTIME_URL)
    assert response.status_code == 200
    results = response.json()
    assert len(results) == 1
    assert results[0]["type"] == "pageview"
    assert results[0]["count"] == 5


def test_get_realtime_cache_miss_writes_to_cache(
    client: TestClient, mock_collection: MagicMock, mock_redis: MagicMock
) -> None:
    mock_redis.get.return_value = None
    mock_collection.aggregate.return_value.to_list.return_value = [REALTIME_DOC]

    client.get(REALTIME_URL)

    mock_redis.set.assert_called_once()
    key, payload = mock_redis.set.call_args[0]
    assert key == "stats:realtime"
    outer = json.loads(payload)
    assert json.loads(outer["d"]) == [{"type": "pageview", "count": 5}]
    assert "s" in outer


def test_get_realtime_cache_hit_returns_cached(
    client: TestClient, mock_collection: MagicMock, mock_redis: MagicMock
) -> None:
    cached = _sign_cache(
        [{"type": "click", "count": 3}],
        TEST_SETTINGS.cache_hmac_secret.get_secret_value(),
    )
    mock_redis.get.return_value = cached

    response = client.get(REALTIME_URL)
    assert response.status_code == 200
    results = response.json()
    assert results == [{"type": "click", "count": 3}]
    mock_collection.aggregate.assert_not_called()


def test_get_realtime_cache_hit_skips_mongo(
    client: TestClient, mock_collection: MagicMock, mock_redis: MagicMock
) -> None:
    mock_redis.get.return_value = _sign_cache(
        [{"type": "pageview", "count": 1}],
        TEST_SETTINGS.cache_hmac_secret.get_secret_value(),
    )
    client.get(REALTIME_URL)
    mock_collection.aggregate.assert_not_called()


def test_get_realtime_no_redis_queries_mongo(
    mock_collection: MagicMock, mock_task: MagicMock
) -> None:
    mock_collection.aggregate.return_value.to_list.return_value = [REALTIME_DOC]
    app = create_app()
    app.dependency_overrides[get_settings] = lambda: TEST_SETTINGS
    app.state.mongodb_client = build_mongo_mock(mock_collection)
    app.state.elasticsearch_client = None
    app.state.redis_client = None

    response = TestClient(app).get(REALTIME_URL)
    assert response.status_code == 200
    assert response.json() == [{"type": "pageview", "count": 5}]


def test_get_realtime_mongo_unavailable_returns_503(
    client: TestClient, mock_collection: MagicMock
) -> None:
    mock_collection.aggregate.return_value.to_list.side_effect = PyMongoError()
    response = client.get(REALTIME_URL)
    assert response.status_code == 503


def test_get_realtime_corrupt_cache_falls_through_to_mongo(
    client: TestClient, mock_collection: MagicMock, mock_redis: MagicMock
) -> None:
    mock_redis.get.return_value = "not valid json"
    mock_collection.aggregate.return_value.to_list.return_value = [REALTIME_DOC]

    response = client.get(REALTIME_URL)
    assert response.status_code == 200
    assert response.json() == [{"type": "pageview", "count": 5}]
    mock_collection.aggregate.assert_called_once()


def test_get_realtime_tampered_cache_falls_through_to_mongo(
    client: TestClient, mock_collection: MagicMock, mock_redis: MagicMock
) -> None:
    signed = json.loads(
        _sign_cache(
            [{"type": "pageview", "count": 1}],
            TEST_SETTINGS.cache_hmac_secret.get_secret_value(),
        )
    )
    signed["s"] = "badsignature"
    mock_redis.get.return_value = json.dumps(signed)
    mock_collection.aggregate.return_value.to_list.return_value = [REALTIME_DOC]

    response = client.get(REALTIME_URL)
    assert response.status_code == 200
    assert response.json() == [{"type": "pageview", "count": 5}]
    mock_collection.aggregate.assert_called_once()


def test_get_realtime_invalid_cache_schema_falls_through_to_mongo(
    client: TestClient, mock_collection: MagicMock, mock_redis: MagicMock
) -> None:
    mock_redis.get.return_value = json.dumps([{"wrong_field": "value"}])
    mock_collection.aggregate.return_value.to_list.return_value = [REALTIME_DOC]

    response = client.get(REALTIME_URL)
    assert response.status_code == 200
    assert response.json() == [{"type": "pageview", "count": 5}]
    mock_collection.aggregate.assert_called_once()
