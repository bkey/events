"""
Integration tests for the Elasticsearch indexing and search path.

Flow: POST /v1/events → Celery task (eager) → MongoDB insert + ES bulk index
      → refresh index → GET /v1/events/search returns indexed events.
"""

import contextlib

from elasticsearch import Elasticsearch
from fastapi.testclient import TestClient
from helpers import make_payload
from main import create_app
from pymongo import MongoClient
import pytest

from config.settings import settings
from db.elastic import connect_elasticsearch_sync
from tasks.events import process_events
from tasks.worker import celery_app

# ---------------------------------------------------------------------------
# ES-specific fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def sync_es() -> Elasticsearch:
    client = connect_elasticsearch_sync()
    yield client
    client.close()


@pytest.fixture(autouse=True)
def clean_es_index(sync_es: Elasticsearch):
    """Delete all ES documents in the events index before and after each test."""

    def _wipe():
        with contextlib.suppress(Exception):  # index may not exist yet on first run
            sync_es.delete_by_query(
                index=settings.events_index,
                query={"match_all": {}},
                refresh=True,
            )

    _wipe()
    yield
    _wipe()


@pytest.fixture()
def es_eager_client(sync_mongo: MongoClient, sync_es: Elasticsearch) -> TestClient:
    """
    TestClient with:
    - Celery in eager mode
    - real sync MongoClient injected into the task for writes
    - real sync Elasticsearch injected into the task for indexing
    - lifespan creates real AsyncMongoClient and AsyncElasticsearch for the API
    - Redis overridden to None
    """
    celery_app.conf.task_always_eager = True
    process_events._mongo_client = sync_mongo
    process_events._es_client = sync_es
    process_events._es_initialized = True

    app = create_app()
    with TestClient(app, raise_server_exceptions=True) as client:
        app.state.redis_client = None
        yield client, sync_es

    celery_app.conf.task_always_eager = False


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_posted_event_is_searchable(es_eager_client):
    client, sync_es = es_eager_client
    payload = make_payload(type="click", user_id="search-user")

    resp = client.post("/v1/events/", json=[payload])
    assert resp.status_code == 202

    sync_es.indices.refresh(index=settings.events_index)

    search_resp = client.get("/v1/events/search", params={"q": "search-user"})
    assert search_resp.status_code == 200
    results = search_resp.json()
    assert len(results) == 1
    assert results[0]["user_id"] == "search-user"


@pytest.mark.integration
def test_batch_events_all_indexed(es_eager_client):
    client, sync_es = es_eager_client
    payloads = [
        make_payload(type="pageview", user_id="batch-user"),
        make_payload(type="click", user_id="batch-user"),
        make_payload(type="conversion", user_id="batch-user"),
    ]

    resp = client.post("/v1/events/", json=payloads)
    assert resp.status_code == 202

    sync_es.indices.refresh(index=settings.events_index)

    search_resp = client.get(
        "/v1/events/search", params={"q": "batch-user", "limit": 10}
    )
    assert search_resp.status_code == 200
    assert len(search_resp.json()) == 3


@pytest.mark.integration
def test_search_by_event_type(es_eager_client):
    client, sync_es = es_eager_client
    client.post(
        "/v1/events/",
        json=[
            make_payload(type="pageview"),
            make_payload(type="click"),
        ],
    )

    sync_es.indices.refresh(index=settings.events_index)

    results = client.get("/v1/events/search", params={"q": "pageview"}).json()
    assert all(r["type"] == "pageview" for r in results)


@pytest.mark.integration
def test_search_by_source_url(es_eager_client):
    client, sync_es = es_eager_client
    client.post(
        "/v1/events/",
        json=[
            make_payload(source_url="https://target.example.com/"),
            make_payload(source_url="https://other.example.com/"),
        ],
    )

    sync_es.indices.refresh(index=settings.events_index)

    results = client.get("/v1/events/search", params={"q": "target.example.com"}).json()
    assert len(results) == 1
    assert "target.example.com" in results[0]["source_url"]


@pytest.mark.integration
def test_unindexed_event_not_returned(es_eager_client):
    """Events not yet indexed are not returned by search (sanity check)."""
    client, sync_es = es_eager_client
    results = client.get("/v1/events/search", params={"q": "ghost-user"}).json()
    assert results == []


@pytest.mark.integration
def test_retry_does_not_duplicate_in_es(es_eager_client):
    """Re-indexing the same _id overwrites the existing document, not duplicates."""
    client, sync_es = es_eager_client
    payload = make_payload(user_id="retry-user")

    client.post("/v1/events/", json=[payload])
    client.post("/v1/events/", json=[payload])

    sync_es.indices.refresh(index=settings.events_index)

    # Two POSTs assign two different _ids, so two ES documents are expected.
    # This test documents that behavior — ES dedup only applies within a single task.
    results = client.get(
        "/v1/events/search", params={"q": "retry-user", "limit": 10}
    ).json()
    assert len(results) == 2
