from unittest.mock import MagicMock, patch

from conftest import build_collection_mock, build_mongo_mock, mock_task  # noqa: F401
from fastapi.testclient import TestClient
from main import create_app
import pytest

import config.settings as settings_mod

EVENTS_URL = "/v1/events/"
ALLOWED_ORIGIN = "https://app.example.com"
OTHER_ORIGIN = "https://attacker.example.com"


def _make_client(origins: list[str]) -> TestClient:
    with patch.object(settings_mod.settings, "cors_allowed_origins", origins):
        app = create_app()
    app.state.mongodb_client = build_mongo_mock(build_collection_mock())
    app.state.elasticsearch_client = None
    app.state.redis_client = None
    return TestClient(app)


@pytest.fixture
def client_with_cors(mock_task: MagicMock) -> TestClient:  # noqa: F811
    return _make_client([ALLOWED_ORIGIN])


def test_cors_allowed_origin_receives_header(client_with_cors: TestClient) -> None:
    response = client_with_cors.get(EVENTS_URL, headers={"Origin": ALLOWED_ORIGIN})
    assert response.headers.get("access-control-allow-origin") == ALLOWED_ORIGIN


def test_cors_disallowed_origin_receives_no_header(
    client_with_cors: TestClient,
) -> None:
    response = client_with_cors.get(EVENTS_URL, headers={"Origin": OTHER_ORIGIN})
    assert "access-control-allow-origin" not in response.headers


def test_cors_no_origins_configured_blocks_all(mock_task: MagicMock) -> None:  # noqa: F811
    client = _make_client([])
    response = client.get(EVENTS_URL, headers={"Origin": ALLOWED_ORIGIN})
    assert "access-control-allow-origin" not in response.headers


def test_cors_preflight_allowed_origin(client_with_cors: TestClient) -> None:
    response = client_with_cors.options(
        EVENTS_URL,
        headers={
            "Origin": ALLOWED_ORIGIN,
            "Access-Control-Request-Method": "GET",
        },
    )
    assert response.status_code == 200
    assert response.headers.get("access-control-allow-origin") == ALLOWED_ORIGIN


def test_cors_preflight_disallowed_method(client_with_cors: TestClient) -> None:
    response = client_with_cors.options(
        EVENTS_URL,
        headers={
            "Origin": ALLOWED_ORIGIN,
            "Access-Control-Request-Method": "DELETE",
        },
    )
    assert response.status_code == 400
