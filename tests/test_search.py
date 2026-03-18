from unittest.mock import AsyncMock, MagicMock, patch

from conftest import EVENT_DOC, VALID_EVENT, build_collection_mock, build_mongo_mock
from elasticsearch import TransportError
from fastapi.testclient import TestClient
from main import create_app
import pytest

SEARCH_URL = "/v1/events/search"

ES_HIT = {"_source": EVENT_DOC}


# --- GET /v1/events/search ---


def test_search_returns_200(client: TestClient) -> None:
    response = client.get(SEARCH_URL, params={"q": "pageview"})
    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_search_returns_matching_events(client: TestClient, mock_es: MagicMock) -> None:
    mock_es.search.return_value = {"hits": {"hits": [ES_HIT]}}
    response = client.get(SEARCH_URL, params={"q": "pageview"})
    assert response.status_code == 200
    results = response.json()
    assert len(results) == 1
    assert results[0]["type"] == "pageview"
    assert results[0]["user_id"] == "user-123"
    assert results[0]["event_id"] == "evt-123"


def test_search_empty_results(client: TestClient) -> None:
    response = client.get(SEARCH_URL, params={"q": "nomatch"})
    assert response.status_code == 200
    assert response.json() == []


def test_search_missing_query_rejected(client: TestClient) -> None:
    response = client.get(SEARCH_URL)
    assert response.status_code == 422


def test_search_empty_query_rejected(client: TestClient) -> None:
    response = client.get(SEARCH_URL, params={"q": ""})
    assert response.status_code == 422


def test_search_limit_param(client: TestClient, mock_es: MagicMock) -> None:
    client.get(SEARCH_URL, params={"q": "test", "limit": 25})
    _, kwargs = mock_es.search.call_args
    assert kwargs["size"] == 25


def test_search_uses_simple_query_string(
    client: TestClient, mock_es: MagicMock
) -> None:
    client.get(SEARCH_URL, params={"q": "chrome"})
    _, kwargs = mock_es.search.call_args
    query = kwargs["query"]
    assert "simple_query_string" in query
    assert "multi_match" not in query


def test_search_default_operator_is_and(client: TestClient, mock_es: MagicMock) -> None:
    client.get(SEARCH_URL, params={"q": "chrome mobile"})
    _, kwargs = mock_es.search.call_args
    sqs = kwargs["query"]["simple_query_string"]
    assert sqs["default_operator"] == "and"


def test_search_includes_metadata_fields(
    client: TestClient, mock_es: MagicMock
) -> None:
    client.get(SEARCH_URL, params={"q": "chrome"})
    _, kwargs = mock_es.search.call_args
    fields = kwargs["query"]["simple_query_string"]["fields"]
    assert "metadata.*" in fields


def test_search_invalid_limit_rejected(client: TestClient) -> None:
    response = client.get(SEARCH_URL, params={"q": "test", "limit": 101})
    assert response.status_code == 422


def test_search_es_unavailable_returns_503(
    mock_collection: MagicMock, mock_redis: MagicMock, mock_task: MagicMock
) -> None:
    app = create_app()
    app.state.mongodb_client = build_mongo_mock(mock_collection)
    app.state.elasticsearch_client = None
    app.state.redis_client = mock_redis
    with patch(
        "routers.v1.search.connect_elasticsearch", side_effect=RuntimeError("ES down")
    ):
        response = TestClient(app).get(SEARCH_URL, params={"q": "test"})
    assert response.status_code == 503


def test_search_es_connection_error_returns_503(
    client: TestClient, mock_es: MagicMock
) -> None:
    mock_es.search.side_effect = TransportError("connection failed")
    response = client.get(SEARCH_URL, params={"q": "test"})
    assert response.status_code == 503


# --- metadata_key parameter ---


def test_search_metadata_key_targets_specific_field(
    client: TestClient, mock_es: MagicMock
) -> None:
    client.get(SEARCH_URL, params={"q": "spring_sale", "metadata_key": "campaign"})
    _, kwargs = mock_es.search.call_args
    fields = kwargs["query"]["simple_query_string"]["fields"]
    assert fields == ["metadata.campaign"]


def test_search_metadata_key_excludes_other_fields(
    client: TestClient, mock_es: MagicMock
) -> None:
    client.get(SEARCH_URL, params={"q": "foo", "metadata_key": "campaign"})
    _, kwargs = mock_es.search.call_args
    fields = kwargs["query"]["simple_query_string"]["fields"]
    assert "type" not in fields
    assert "user_id" not in fields
    assert "metadata.*" not in fields


def test_search_without_metadata_key_uses_all_fields(
    client: TestClient, mock_es: MagicMock
) -> None:
    client.get(SEARCH_URL, params={"q": "foo"})
    _, kwargs = mock_es.search.call_args
    fields = kwargs["query"]["simple_query_string"]["fields"]
    assert fields == ["type", "user_id", "source_url", "metadata.*"]


def test_search_metadata_key_with_dots_allowed(
    client: TestClient, mock_es: MagicMock
) -> None:
    client.get(SEARCH_URL, params={"q": "foo", "metadata_key": "nested.key"})
    _, kwargs = mock_es.search.call_args
    fields = kwargs["query"]["simple_query_string"]["fields"]
    assert fields == ["metadata.nested.key"]


def test_search_metadata_key_invalid_characters_rejected(
    client: TestClient,
) -> None:
    response = client.get(SEARCH_URL, params={"q": "foo", "metadata_key": "bad key!"})
    assert response.status_code == 422


def test_search_metadata_key_injection_attempt_rejected(
    client: TestClient,
) -> None:
    response = client.get(
        SEARCH_URL, params={"q": "foo", "metadata_key": 'key"; drop table events--'}
    )
    assert response.status_code == 422
