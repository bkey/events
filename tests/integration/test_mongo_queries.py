"""
Integration tests verifying that _build_query() filter documents produce correct
results against a real MongoDB instance, and that compound indexes are exercised.
"""

from datetime import datetime, timedelta, timezone
import uuid

from fastapi.testclient import TestClient
from helpers import make_doc
from pymongo.collection import Collection
import pytest


def insert(*docs, collection: Collection):
    """Insert one or more docs, converting timestamp strings to datetime objects."""
    prepared = []
    for doc in docs:
        d = dict(doc)
        if isinstance(d.get("timestamp"), str):
            d["timestamp"] = datetime.fromisoformat(d["timestamp"])
        prepared.append(d)
    collection.insert_many(prepared)


T0 = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
T1 = T0 + timedelta(hours=1)
T2 = T0 + timedelta(hours=2)
T3 = T0 + timedelta(hours=3)


# ---------------------------------------------------------------------------
# Single-field filters
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_filter_by_type(mongo_api_client: TestClient, collection: Collection):
    insert(
        make_doc(type="pageview", timestamp=T0.isoformat()),
        make_doc(type="click", timestamp=T0.isoformat()),
        collection=collection,
    )
    resp = mongo_api_client.get("/v1/events/", params={"type": "click"})
    body = resp.json()
    assert body["total"] == 1
    assert body["results"][0]["type"] == "click"


@pytest.mark.integration
def test_filter_by_user_id(mongo_api_client: TestClient, collection: Collection):
    insert(
        make_doc(user_id="alice", timestamp=T0.isoformat()),
        make_doc(user_id="bob", timestamp=T0.isoformat()),
        collection=collection,
    )
    resp = mongo_api_client.get("/v1/events/", params={"user_id": "alice"})
    body = resp.json()
    assert body["total"] == 1
    assert body["results"][0]["user_id"] == "alice"


@pytest.mark.integration
def test_filter_by_source_url(mongo_api_client: TestClient, collection: Collection):
    insert(
        make_doc(source_url="https://example.com/a", timestamp=T0.isoformat()),
        make_doc(source_url="https://example.com/b", timestamp=T0.isoformat()),
        collection=collection,
    )
    resp = mongo_api_client.get(
        "/v1/events/", params={"source_url": "https://example.com/a"}
    )
    body = resp.json()
    assert body["total"] == 1
    assert body["results"][0]["source_url"] == "https://example.com/a"


# ---------------------------------------------------------------------------
# Date range filters
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_filter_date_from(mongo_api_client: TestClient, collection: Collection):
    insert(
        make_doc(timestamp=T0.isoformat()),
        make_doc(timestamp=T1.isoformat()),
        make_doc(timestamp=T2.isoformat()),
        collection=collection,
    )
    resp = mongo_api_client.get(
        "/v1/events/", params={"date_from": T1.isoformat(), "limit": 10}
    )
    assert resp.json()["total"] == 2


@pytest.mark.integration
def test_filter_date_to(mongo_api_client: TestClient, collection: Collection):
    insert(
        make_doc(timestamp=T0.isoformat()),
        make_doc(timestamp=T1.isoformat()),
        make_doc(timestamp=T2.isoformat()),
        collection=collection,
    )
    resp = mongo_api_client.get(
        "/v1/events/", params={"date_to": T1.isoformat(), "limit": 10}
    )
    assert resp.json()["total"] == 2


@pytest.mark.integration
def test_filter_date_range(mongo_api_client: TestClient, collection: Collection):
    insert(
        make_doc(timestamp=T0.isoformat()),
        make_doc(timestamp=T1.isoformat()),
        make_doc(timestamp=T2.isoformat()),
        make_doc(timestamp=T3.isoformat()),
        collection=collection,
    )
    resp = mongo_api_client.get(
        "/v1/events/",
        params={"date_from": T1.isoformat(), "date_to": T2.isoformat(), "limit": 10},
    )
    assert resp.json()["total"] == 2


# ---------------------------------------------------------------------------
# Compound filter (exercises compound index)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_filter_type_and_date_range(
    mongo_api_client: TestClient, collection: Collection
):
    insert(
        make_doc(type="pageview", timestamp=T0.isoformat()),
        make_doc(type="pageview", timestamp=T1.isoformat()),
        make_doc(type="click", timestamp=T1.isoformat()),
        make_doc(type="pageview", timestamp=T2.isoformat()),
        collection=collection,
    )
    resp = mongo_api_client.get(
        "/v1/events/",
        params={
            "type": "pageview",
            "date_from": T1.isoformat(),
            "date_to": T2.isoformat(),
            "limit": 10,
        },
    )
    body = resp.json()
    assert body["total"] == 2
    assert all(r["type"] == "pageview" for r in body["results"])


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_pagination_total_reflects_full_count(
    mongo_api_client: TestClient, collection: Collection
):
    insert(
        *[make_doc(timestamp=T0.isoformat()) for _ in range(5)], collection=collection
    )
    resp = mongo_api_client.get("/v1/events/", params={"limit": 2, "skip": 0})
    body = resp.json()
    assert body["total"] == 5
    assert len(body["results"]) == 2


@pytest.mark.integration
def test_pagination_skip_returns_correct_slice(
    mongo_api_client: TestClient, collection: Collection
):
    insert(
        *[make_doc(timestamp=T0.isoformat()) for _ in range(5)], collection=collection
    )
    resp_page1 = mongo_api_client.get("/v1/events/", params={"limit": 3, "skip": 0})
    resp_page2 = mongo_api_client.get("/v1/events/", params={"limit": 3, "skip": 3})
    assert len(resp_page1.json()["results"]) == 3
    assert len(resp_page2.json()["results"]) == 2


@pytest.mark.integration
def test_no_results_returns_empty(mongo_api_client: TestClient, collection: Collection):
    resp = mongo_api_client.get("/v1/events/", params={"user_id": "ghost"})
    body = resp.json()
    assert body["total"] == 0
    assert body["results"] == []
