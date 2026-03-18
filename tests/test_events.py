from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from conftest import EVENT_DOC, VALID_EVENT
from fastapi.testclient import TestClient
from pymongo.errors import PyMongoError
import pytest

EVENTS_URL = "/v1/events/"


# --- POST /v1/events/ ---


def test_post_valid_event(client: TestClient) -> None:
    response = client.post(EVENTS_URL, json=[VALID_EVENT])
    assert response.status_code == 202
    assert response.json() == {"queued": 1}


def test_post_valid_batch(client: TestClient) -> None:
    response = client.post(EVENTS_URL, json=[VALID_EVENT] * 5)
    assert response.status_code == 202
    assert response.json() == {"queued": 5}


def test_post_empty_list_rejected(client: TestClient) -> None:
    response = client.post(EVENTS_URL, json=[])
    assert response.status_code == 422


def test_post_single_event_accepted(client: TestClient) -> None:
    response = client.post(EVENTS_URL, json=[VALID_EVENT])
    assert response.status_code == 202
    assert response.json() == {"queued": 1}


def test_post_batch_at_limit_accepted(client: TestClient) -> None:
    response = client.post(EVENTS_URL, json=[VALID_EVENT] * 500)
    assert response.status_code == 202
    assert response.json() == {"queued": 500}


def test_post_batch_over_limit_rejected(client: TestClient) -> None:
    response = client.post(EVENTS_URL, json=[VALID_EVENT] * 501)
    assert response.status_code == 422


def test_post_future_timestamp_rejected(client: TestClient) -> None:
    future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    response = client.post(EVENTS_URL, json=[{**VALID_EVENT, "timestamp": future}])
    assert response.status_code == 422


def test_post_custom_event_type_accepted(client: TestClient) -> None:
    response = client.post(EVENTS_URL, json=[{**VALID_EVENT, "type": "custom_event"}])
    assert response.status_code == 202


def test_post_invalid_url_rejected(client: TestClient) -> None:
    response = client.post(
        EVENTS_URL, json=[{**VALID_EVENT, "source_url": "not-a-url"}]
    )
    assert response.status_code == 422


def test_post_empty_user_id_rejected(client: TestClient) -> None:
    response = client.post(EVENTS_URL, json=[{**VALID_EVENT, "user_id": ""}])
    assert response.status_code == 422


def test_post_user_id_at_max_length_accepted(client: TestClient) -> None:
    response = client.post(EVENTS_URL, json=[{**VALID_EVENT, "user_id": "a" * 256}])
    assert response.status_code == 202


def test_post_user_id_too_long_rejected(client: TestClient) -> None:
    response = client.post(EVENTS_URL, json=[{**VALID_EVENT, "user_id": "a" * 257}])
    assert response.status_code == 422


def test_post_event_id_stored_as_own_field(
    client: TestClient, mock_task: MagicMock
) -> None:
    response = client.post(
        EVENTS_URL, json=[{**VALID_EVENT, "event_id": "my-stable-id"}]
    )
    assert response.status_code == 202
    doc = mock_task.call_args[0][0][0]
    assert doc["event_id"] == "my-stable-id"
    import uuid

    uuid.UUID(doc["_id"])  # _id is always a fresh UUID, independent of event_id


def test_post_without_event_id_generates_uuid(
    client: TestClient, mock_task: MagicMock
) -> None:
    response = client.post(EVENTS_URL, json=[VALID_EVENT])
    assert response.status_code == 202
    doc = mock_task.call_args[0][0][0]
    import uuid

    uuid.UUID(doc["_id"])
    uuid.UUID(doc["event_id"])  # both _id and event_id are valid UUIDs


def test_post_empty_event_id_rejected(client: TestClient) -> None:
    response = client.post(EVENTS_URL, json=[{**VALID_EVENT, "event_id": ""}])
    assert response.status_code == 422


def test_post_broker_unavailable_returns_503(
    client: TestClient, mock_task: MagicMock
) -> None:
    from kombu.exceptions import OperationalError

    mock_task.side_effect = OperationalError("broker down")
    response = client.post(EVENTS_URL, json=[VALID_EVENT])
    assert response.status_code == 503


# --- GET /v1/events/ ---


def test_get_events_returns_paginated_shape(client: TestClient) -> None:
    response = client.get(EVENTS_URL)
    assert response.status_code == 200
    body = response.json()
    assert "total" in body
    assert "skip" in body
    assert "limit" in body
    assert "results" in body


def test_get_events_empty(client: TestClient) -> None:
    response = client.get(EVENTS_URL)
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 0
    assert body["results"] == []


def test_get_events_returns_results(
    client: TestClient, mock_collection: MagicMock
) -> None:
    mock_collection.count_documents.return_value = 1
    mock_collection.find.return_value.skip.return_value.limit.return_value.to_list.return_value = [
        EVENT_DOC
    ]

    response = client.get(EVENTS_URL)
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert len(body["results"]) == 1
    assert body["results"][0]["type"] == "pageview"
    assert body["results"][0]["user_id"] == "user-123"


def test_get_events_pagination_defaults(client: TestClient) -> None:
    response = client.get(EVENTS_URL)
    body = response.json()
    assert body["skip"] == 0
    assert body["limit"] == 10


def test_get_events_custom_pagination(client: TestClient) -> None:
    response = client.get(EVENTS_URL, params={"skip": 20, "limit": 50})
    body = response.json()
    assert body["skip"] == 20
    assert body["limit"] == 50


def test_get_events_limit_min_accepted(client: TestClient) -> None:
    response = client.get(EVENTS_URL, params={"limit": 1})
    assert response.status_code == 200
    assert response.json()["limit"] == 1


def test_get_events_limit_max_accepted(client: TestClient) -> None:
    response = client.get(EVENTS_URL, params={"limit": 100})
    assert response.status_code == 200
    assert response.json()["limit"] == 100


def test_get_events_invalid_limit_rejected(client: TestClient) -> None:
    response = client.get(EVENTS_URL, params={"limit": 101})
    assert response.status_code == 422


def test_get_events_negative_skip_rejected(client: TestClient) -> None:
    response = client.get(EVENTS_URL, params={"skip": -1})
    assert response.status_code == 422


def test_get_events_mongo_unavailable_returns_503(
    client: TestClient, mock_collection: MagicMock
) -> None:
    mock_collection.count_documents.side_effect = PyMongoError()
    response = client.get(EVENTS_URL)
    assert response.status_code == 503


# --- GET /v1/events/ filters ---


def test_get_events_filter_by_type(
    client: TestClient, mock_collection: MagicMock
) -> None:
    client.get(EVENTS_URL, params={"type": "pageview"})
    query = mock_collection.find.call_args[0][0]
    assert query["type"] == "pageview"


def test_get_events_filter_by_user_id(
    client: TestClient, mock_collection: MagicMock
) -> None:
    client.get(EVENTS_URL, params={"user_id": "user-123"})
    query = mock_collection.find.call_args[0][0]
    assert query["user_id"] == "user-123"


def test_get_events_filter_by_source_url(
    client: TestClient, mock_collection: MagicMock
) -> None:
    client.get(EVENTS_URL, params={"source_url": "https://example.com"})
    query = mock_collection.find.call_args[0][0]
    # Pydantic normalizes the URL the same way it does on write (trailing slash added)
    assert query["source_url"] == "https://example.com/"


def test_get_events_source_url_normalized_to_match_stored_form(
    client: TestClient, mock_collection: MagicMock
) -> None:
    """Query URL must be normalized identically to how source_url is stored on write."""
    # Both "https://example.com" and "https://example.com/" should resolve to the
    # same stored value so that filter results are consistent.
    client.get(EVENTS_URL, params={"source_url": "https://example.com"})
    query_without_slash = mock_collection.find.call_args[0][0]["source_url"]

    client.get(EVENTS_URL, params={"source_url": "https://example.com/"})
    query_with_slash = mock_collection.find.call_args[0][0]["source_url"]

    assert query_without_slash == query_with_slash


def test_get_events_filter_by_date_from(
    client: TestClient, mock_collection: MagicMock
) -> None:
    client.get(EVENTS_URL, params={"date_from": "2024-01-01T00:00:00Z"})
    query = mock_collection.find.call_args[0][0]
    assert isinstance(query["timestamp"]["$gte"], datetime)


def test_get_events_filter_by_date_to(
    client: TestClient, mock_collection: MagicMock
) -> None:
    client.get(EVENTS_URL, params={"date_to": "2024-01-31T23:59:59Z"})
    query = mock_collection.find.call_args[0][0]
    assert isinstance(query["timestamp"]["$lte"], datetime)


def test_get_events_filter_by_date_range(
    client: TestClient, mock_collection: MagicMock
) -> None:
    client.get(
        EVENTS_URL,
        params={"date_from": "2024-01-01T00:00:00Z", "date_to": "2024-01-31T23:59:59Z"},
    )
    query = mock_collection.find.call_args[0][0]
    assert isinstance(query["timestamp"]["$gte"], datetime)
    assert isinstance(query["timestamp"]["$lte"], datetime)


def test_get_events_combined_filters(
    client: TestClient, mock_collection: MagicMock
) -> None:
    client.get(
        EVENTS_URL,
        params={
            "type": "click",
            "user_id": "user-456",
            "date_from": "2024-01-01T00:00:00Z",
        },
    )
    query = mock_collection.find.call_args[0][0]
    assert query["type"] == "click"
    assert query["user_id"] == "user-456"
    assert isinstance(query["timestamp"]["$gte"], datetime)


def test_get_events_no_filters_empty_query(
    client: TestClient, mock_collection: MagicMock
) -> None:
    client.get(EVENTS_URL)
    query = mock_collection.find.call_args[0][0]
    assert query == {}


def test_get_events_filter_count_uses_same_query(
    client: TestClient, mock_collection: MagicMock
) -> None:
    client.get(EVENTS_URL, params={"type": "conversion"})
    count_query = mock_collection.count_documents.call_args[0][0]
    find_query = mock_collection.find.call_args[0][0]
    assert count_query == find_query


def test_get_events_skips_malformed_docs(
    client: TestClient, mock_collection: MagicMock
) -> None:
    valid_doc = EVENT_DOC
    malformed_doc = {"type": "pageview"}  # missing required fields
    mock_collection.count_documents.return_value = 2
    mock_collection.find.return_value.skip.return_value.limit.return_value.to_list.return_value = [
        valid_doc,
        malformed_doc,
    ]

    response = client.get(EVENTS_URL)
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 2
    assert len(body["results"]) == 1
    assert body["results"][0]["type"] == "pageview"
