"""
Integration tests for the Celery task + MongoDB write path.

These tests require a real MongoDB instance. Run via:
    docker compose -f docker-compose.test.yml up --build --abort-on-container-exit --exit-code-from test

Two layers are covered:
  1. Task layer — process_events.apply() with a real MongoClient injected directly.
  2. API layer  — POST /v1/events with Celery in eager mode, verified via GET /v1/events.
"""

from fastapi.testclient import TestClient
from helpers import make_doc, make_payload
from pymongo import MongoClient
import pytest

from tasks.events import process_events

# ---------------------------------------------------------------------------
# Task-layer tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_task_persists_single_event(sync_mongo, collection):
    process_events._mongo_client = sync_mongo
    process_events._es_initialized = True

    event = make_doc()
    result = process_events.apply(args=[[event]])

    assert result.get() == {"inserted": 1}
    doc = collection.find_one({"_id": event["_id"]})
    assert doc is not None
    assert doc["type"] == event["type"]
    assert doc["user_id"] == event["user_id"]


@pytest.mark.integration
def test_task_persists_batch(sync_mongo, collection):
    process_events._mongo_client = sync_mongo
    process_events._es_initialized = True

    events = [make_doc(type=t) for t in ("pageview", "click", "conversion")]
    result = process_events.apply(args=[events])

    assert result.get() == {"inserted": 3}
    assert collection.count_documents({}) == 3


@pytest.mark.integration
def test_task_idempotent_on_retry(sync_mongo, collection):
    """Reprocessing the same _id must not raise or double-insert."""
    process_events._mongo_client = sync_mongo
    process_events._es_initialized = True

    event = make_doc()
    process_events.apply(args=[[event]])
    result = process_events.apply(args=[[event]])

    assert result.get() == {"inserted": 0}
    assert collection.count_documents({"_id": event["_id"]}) == 1


@pytest.mark.integration
def test_task_partial_retry_inserts_only_new_events(sync_mongo, collection):
    """On retry, duplicates are swallowed; only new events are inserted."""
    process_events._mongo_client = sync_mongo
    process_events._es_initialized = True

    first = make_doc()
    process_events.apply(args=[[first]])

    second = make_doc()
    result = process_events.apply(args=[[first, second]])

    assert result.get() == {"inserted": 1}
    assert collection.count_documents({}) == 2


# ---------------------------------------------------------------------------
# End-to-end API tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_post_event_appears_in_get(eager_client: TestClient, collection):
    """POST /v1/events → Celery task → MongoDB → GET /v1/events returns the event."""
    payload = make_payload(type="click", user_id="integration-user")

    post_resp = eager_client.post("/v1/events/", json=[payload])
    assert post_resp.status_code == 202
    assert post_resp.json()["queued"] == 1

    get_resp = eager_client.get("/v1/events/", params={"user_id": "integration-user"})
    assert get_resp.status_code == 200
    body = get_resp.json()
    assert body["total"] == 1
    assert body["results"][0]["type"] == "click"
    assert body["results"][0]["user_id"] == "integration-user"


@pytest.mark.integration
def test_post_batch_all_appear_in_get(eager_client: TestClient, collection):
    """A batch of events is fully persisted and retrievable."""
    payloads = [make_payload(type="pageview") for _ in range(3)]

    post_resp = eager_client.post("/v1/events/", json=payloads)
    assert post_resp.status_code == 202
    assert post_resp.json()["queued"] == 3

    get_resp = eager_client.get("/v1/events/", params={"limit": 10})
    assert get_resp.status_code == 200
    assert get_resp.json()["total"] == 3


@pytest.mark.integration
def test_post_without_client_id_does_not_deduplicate(
    eager_client: TestClient, collection
):
    """The API assigns a fresh UUID per POST, so two POSTs = two documents."""
    payload = make_payload(user_id="dedup-user")
    payload.pop("event_id")  # let the API assign a unique event_id per POST

    eager_client.post("/v1/events/", json=[payload])
    eager_client.post("/v1/events/", json=[payload])

    get_resp = eager_client.get("/v1/events/", params={"user_id": "dedup-user"})
    assert get_resp.json()["total"] == 2
