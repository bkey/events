from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from conftest import build_collection_mock, build_mongo_mock
from pymongo.errors import BulkWriteError, PyMongoError
import pytest

from tasks.events import _parse_timestamps, process_events

TASK_EVENT = {
    "_id": "test-id-123",
    "type": "pageview",
    "timestamp": "2024-01-01T00:00:00+00:00",
    "user_id": "user-123",
    "source_url": "https://example.com/",
    "metadata": {},
}


@pytest.fixture(autouse=True)
def reset_task_clients():
    """Reset lazily-initialized clients on the task singleton between tests."""
    process_events._mongo_client = None
    process_events._es_client = None
    process_events._es_initialized = False
    yield
    process_events._mongo_client = None
    process_events._es_client = None
    process_events._es_initialized = False


@pytest.fixture
def mock_collection() -> MagicMock:
    collection = build_collection_mock()
    result = MagicMock()
    result.inserted_ids = ["test-id-123"]
    collection.insert_many.return_value = result
    return collection


@pytest.fixture
def mock_es_client() -> MagicMock:
    es = MagicMock()
    es.bulk = MagicMock(return_value={"errors": False, "items": []})
    return es


@pytest.fixture
def with_mongo(mock_collection: MagicMock) -> MagicMock:
    """Inject a mock MongoDB client directly into the task singleton."""
    process_events._mongo_client = build_mongo_mock(mock_collection)
    return process_events._mongo_client


@pytest.fixture
def with_es(mock_es_client: MagicMock) -> MagicMock:
    """Inject a mock Elasticsearch client directly into the task singleton."""
    process_events._es_client = mock_es_client
    process_events._es_initialized = True
    return mock_es_client


# --- lazy initialization ---


def test_mongo_client_initialized_on_first_use(mock_collection: MagicMock) -> None:
    mongo_mock = build_mongo_mock(mock_collection)
    with patch("tasks.events.connect_db_sync", return_value=mongo_mock) as mock_connect:
        _ = process_events.mongo_client
        mock_connect.assert_called_once()
        assert process_events._mongo_client is mongo_mock


def test_mongo_client_not_reinitialized_on_second_access(
    mock_collection: MagicMock,
) -> None:
    mongo_mock = build_mongo_mock(mock_collection)
    with patch("tasks.events.connect_db_sync", return_value=mongo_mock) as mock_connect:
        _ = process_events.mongo_client
        _ = process_events.mongo_client
        mock_connect.assert_called_once()


def test_es_client_initialized_on_first_use(mock_es_client: MagicMock) -> None:
    with patch(
        "tasks.events.connect_elasticsearch_sync", return_value=mock_es_client
    ) as mock_connect:
        _ = process_events.es_client
        mock_connect.assert_called_once()
        assert process_events._es_client is mock_es_client


def test_es_client_unavailable_returns_none() -> None:
    with patch(
        "tasks.events.connect_elasticsearch_sync",
        side_effect=RuntimeError("ES down"),
    ):
        result = process_events.es_client
        assert result is None
        assert process_events._es_initialized is True


def test_es_client_not_reinitialized_after_failure() -> None:
    with patch(
        "tasks.events.connect_elasticsearch_sync",
        side_effect=RuntimeError("ES down"),
    ) as mock_connect:
        _ = process_events.es_client
        _ = process_events.es_client
        mock_connect.assert_called_once()


# --- _parse_timestamps ---


def test_parse_timestamps_converts_iso_string_to_datetime() -> None:
    events = [{"timestamp": "2024-01-01T00:00:00+00:00", "type": "pageview"}]
    result = _parse_timestamps(events)
    assert isinstance(result[0]["timestamp"], datetime)


def test_parse_timestamps_leaves_datetime_unchanged() -> None:
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    events = [{"timestamp": ts, "type": "pageview"}]
    result = _parse_timestamps(events)
    assert result[0]["timestamp"] is ts


def test_parse_timestamps_leaves_other_fields_unchanged() -> None:
    events = [
        {"timestamp": "2024-01-01T00:00:00+00:00", "type": "pageview", "_id": "abc"}
    ]
    result = _parse_timestamps(events)
    assert result[0]["type"] == "pageview"
    assert result[0]["_id"] == "abc"


# --- process_events ---


def test_process_events_inserts_to_mongo(
    with_mongo: MagicMock, mock_collection: MagicMock
) -> None:
    result = process_events.apply(args=[[TASK_EVENT]])
    assert result.get() == {"inserted": 1}
    inserted = mock_collection.insert_many.call_args[0][0]
    assert len(inserted) == 1
    assert isinstance(
        inserted[0]["timestamp"], datetime
    )  # string was parsed to datetime


def test_process_events_indexes_to_es(
    with_mongo: MagicMock, with_es: MagicMock, mock_es_client: MagicMock
) -> None:
    process_events.apply(args=[[TASK_EVENT]])
    mock_es_client.bulk.assert_called_once()
    body = mock_es_client.bulk.call_args[1]["operations"]
    assert body[0] == {"index": {"_index": "events", "_id": "test-id-123"}}
    assert "_id" not in body[1]


def test_process_events_bulk_body_has_correct_structure(
    with_mongo: MagicMock,
    with_es: MagicMock,
    mock_collection: MagicMock,
    mock_es_client: MagicMock,
) -> None:
    events = [
        {**TASK_EVENT, "_id": "id-1"},
        {**TASK_EVENT, "_id": "id-2", "type": "click"},
    ]
    mock_result = MagicMock()
    mock_result.inserted_ids = ["id-1", "id-2"]
    mock_collection.insert_many.return_value = mock_result

    process_events.apply(args=[events])

    body = mock_es_client.bulk.call_args[1]["operations"]
    assert len(body) == 4  # 2 action+doc pairs
    assert body[0]["index"]["_id"] == "id-1"
    assert body[2]["index"]["_id"] == "id-2"


def test_process_events_skips_es_when_unavailable(
    with_mongo: MagicMock, mock_es_client: MagicMock
) -> None:
    process_events._es_client = None
    process_events._es_initialized = True
    result = process_events.apply(args=[[TASK_EVENT]])
    assert result.get() == {"inserted": 1}
    mock_es_client.bulk.assert_not_called()


def test_process_events_es_error_does_not_fail_task(
    with_mongo: MagicMock, with_es: MagicMock, mock_es_client: MagicMock
) -> None:
    mock_es_client.bulk.side_effect = Exception("ES connection lost")
    result = process_events.apply(args=[[TASK_EVENT]])
    assert result.get() == {"inserted": 1}


def test_process_events_handles_duplicate_key_errors(
    with_mongo: MagicMock, mock_collection: MagicMock
) -> None:
    error = BulkWriteError(
        {"writeErrors": [{"code": 11000, "errmsg": "duplicate key"}], "nInserted": 0}
    )
    mock_collection.insert_many.side_effect = error
    result = process_events.apply(args=[[TASK_EVENT]])
    assert result.get() == {"inserted": 0}


def test_process_events_reraises_non_duplicate_mongo_errors(
    with_mongo: MagicMock, mock_collection: MagicMock
) -> None:
    error = BulkWriteError(
        {"writeErrors": [{"code": 99999, "errmsg": "other error"}], "nInserted": 0}
    )
    mock_collection.insert_many.side_effect = error
    result = process_events.apply(args=[[TASK_EVENT]])
    assert result.state == "FAILURE"


def test_process_events_retries_on_mongo_error(
    with_mongo: MagicMock, mock_collection: MagicMock
) -> None:
    mock_collection.insert_many.side_effect = PyMongoError("transient error")
    result = process_events.apply(args=[[TASK_EVENT]])
    assert result.state == "FAILURE"
    assert mock_collection.insert_many.call_count == 4  # 1 initial + 3 retries


def test_es_bulk_partial_failure_writes_only_failed_events_to_dlq(
    with_mongo: MagicMock,
    mock_collection: MagicMock,
    with_es: MagicMock,
    mock_es_client: MagicMock,
) -> None:
    events = [
        {**TASK_EVENT, "_id": "id-1"},
        {**TASK_EVENT, "_id": "id-2", "type": "click"},
    ]
    mock_result = MagicMock()
    mock_result.inserted_ids = ["id-1", "id-2"]
    mock_collection.insert_many.return_value = mock_result

    mock_es_client.bulk.return_value = {
        "errors": True,
        "items": [
            {"index": {"_id": "id-1", "result": "created"}},
            {
                "index": {
                    "_id": "id-2",
                    "error": {"type": "mapper_exception", "reason": "field too long"},
                }
            },
        ],
    }

    result = process_events.apply(args=[events])
    assert result.get() == {"inserted": 2}

    dlq_doc = mock_collection.insert_one.call_args[0][0]
    assert len(dlq_doc["events"]) == 1
    assert dlq_doc["events"][0]["_id"] == "id-2"
    assert "Elasticsearch" in dlq_doc["reason"]


def test_dlq_write_failure_is_logged_but_does_not_propagate(
    with_mongo: MagicMock,
    mock_collection: MagicMock,
    with_es: MagicMock,
    mock_es_client: MagicMock,
) -> None:
    mock_es_client.bulk.return_value = {
        "errors": True,
        "items": [
            {
                "index": {
                    "_id": "test-id-123",
                    "error": {"type": "mapper_exception", "reason": "failed"},
                }
            },
        ],
    }
    mock_collection.insert_one.side_effect = Exception("DLQ unavailable")

    result = process_events.apply(args=[[TASK_EVENT]])
    assert result.get() == {"inserted": 1}
