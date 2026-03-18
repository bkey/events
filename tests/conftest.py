from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.testclient import TestClient
from main import create_app
import pytest

from config.settings import Settings, get_settings

VALID_EVENT = {
    "type": "pageview",
    "timestamp": datetime(2024, 1, 1, tzinfo=timezone.utc).isoformat(),
    "user_id": "user-123",
    "source_url": "https://example.com/",
}

EVENT_DOC = {
    "event_id": "evt-123",
    "type": "pageview",
    "timestamp": datetime(2024, 1, 1, tzinfo=timezone.utc),
    "user_id": "user-123",
    "source_url": "https://example.com/",
    "metadata": {},
}


def build_collection_mock() -> MagicMock:
    """Build a mock MongoDB collection with async cursor chains."""
    collection = MagicMock()
    collection.count_documents = AsyncMock(return_value=0)

    cursor = MagicMock()
    cursor.skip.return_value = cursor
    cursor.limit.return_value = cursor
    cursor.to_list = AsyncMock(return_value=[])
    collection.find.return_value = cursor

    agg_cursor = MagicMock()
    agg_cursor.to_list = AsyncMock(return_value=[])
    collection.aggregate = AsyncMock(return_value=agg_cursor)

    return collection


def build_mongo_mock(collection: MagicMock) -> MagicMock:
    """Wrap a collection mock in a db/client mock."""
    db = MagicMock()
    db.__getitem__ = MagicMock(return_value=collection)
    mongo = MagicMock()
    mongo.__getitem__ = MagicMock(return_value=db)
    return mongo


@pytest.fixture
def mock_collection() -> MagicMock:
    return build_collection_mock()


@pytest.fixture
def mock_es() -> MagicMock:
    es = MagicMock()
    es.search = AsyncMock(return_value={"hits": {"hits": []}})
    return es


@pytest.fixture
def mock_redis() -> MagicMock:
    redis = MagicMock()
    redis.get = AsyncMock(return_value=None)
    redis.set = AsyncMock(return_value=True)
    return redis


@pytest.fixture
def mock_task() -> MagicMock:
    with patch("routers.v1.events.enqueue_events") as mock:
        yield mock


TEST_SETTINGS = Settings(
    mongodb_url="mongodb://localhost:27017",
    elasticsearch_url="http://localhost:9200",
    redis_url="redis://localhost:6379/0",
    HMAC_SECRET="test-hmac-secret",  # noqa: S106
    cors_allowed_origins=[],
)


@pytest.fixture
def client(
    mock_collection: MagicMock,
    mock_es: MagicMock,
    mock_redis: MagicMock,
    mock_task: MagicMock,
) -> TestClient:
    app = create_app()
    app.dependency_overrides[get_settings] = lambda: TEST_SETTINGS
    app.state.mongodb_client = build_mongo_mock(mock_collection)
    app.state.elasticsearch_client = mock_es
    app.state.redis_client = mock_redis
    return TestClient(app)
