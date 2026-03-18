from fastapi.testclient import TestClient
from main import create_app
from pymongo import MongoClient
import pytest

from config.settings import settings
from tasks.events import process_events
from tasks.worker import celery_app

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def sync_mongo() -> MongoClient:
    client = MongoClient(settings.mongodb_url)
    yield client
    client.close()


@pytest.fixture(autouse=True)
def clean_collection(sync_mongo: MongoClient):
    """Wipe the events collection before and after every test."""
    col = sync_mongo[settings.db_name][settings.events_collection]
    col.delete_many({})
    yield
    col.delete_many({})


@pytest.fixture(autouse=True)
def reset_task_clients():
    """Keep the task singleton clean between tests."""
    process_events._mongo_client = None
    process_events._es_client = None
    process_events._es_initialized = False
    yield
    process_events._mongo_client = None
    process_events._es_client = None
    process_events._es_initialized = False


@pytest.fixture()
def collection(sync_mongo: MongoClient):
    return sync_mongo[settings.db_name][settings.events_collection]


@pytest.fixture()
def mongo_api_client(sync_mongo: MongoClient) -> TestClient:
    """TestClient backed by real MongoDB. No Celery, no ES, no Redis."""
    app = create_app()
    with TestClient(app, raise_server_exceptions=True) as client:
        app.state.elasticsearch_client = None
        app.state.redis_client = None
        yield client


@pytest.fixture()
def eager_client(sync_mongo: MongoClient) -> TestClient:
    """
    TestClient with Celery in eager mode and real MongoDB.
    Lifespan creates the AsyncMongoClient for the API read path.
    sync_mongo is injected into the task singleton for writes.
    """
    celery_app.conf.task_always_eager = True
    process_events._mongo_client = sync_mongo
    process_events._es_initialized = True

    app = create_app()
    with TestClient(app, raise_server_exceptions=True) as client:
        app.state.elasticsearch_client = None
        app.state.redis_client = None
        yield client

    celery_app.conf.task_always_eager = False
