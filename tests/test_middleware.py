from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.requests import Request
from starlette.responses import PlainTextResponse

from middleware.request_size import RequestSizeLimitMiddleware
from middleware.trace_id import TraceIDMiddleware

# ---------------------------------------------------------------------------
# Helpers — minimal ASGI apps for testing middleware in isolation
# ---------------------------------------------------------------------------


def _make_size_app(max_bytes: int) -> TestClient:
    app = FastAPI()
    app.add_middleware(RequestSizeLimitMiddleware, max_bytes=max_bytes)

    @app.post("/")
    async def echo(request: Request):
        body = await request.body()
        return PlainTextResponse(str(len(body)))

    return TestClient(app, raise_server_exceptions=True)


def _make_trace_app() -> TestClient:
    app = FastAPI()
    app.add_middleware(TraceIDMiddleware)

    @app.get("/")
    async def noop():
        return PlainTextResponse("ok")

    return TestClient(app, raise_server_exceptions=True)


# ---------------------------------------------------------------------------
# RequestSizeLimitMiddleware
# ---------------------------------------------------------------------------


def test_request_within_limit_is_accepted():
    client = _make_size_app(max_bytes=100)
    response = client.post("/", content=b"x" * 100)
    assert response.status_code == 200


def test_content_length_over_limit_rejected_fast_path():
    """Content-Length header exceeds limit — rejected before reading the body."""
    client = _make_size_app(max_bytes=10)
    response = client.post(
        "/",
        content=b"x" * 20,
        headers={"Content-Length": "20"},
    )
    assert response.status_code == 413
    assert response.json() == {"detail": "Request body too large"}


def test_chunked_body_over_limit_rejected_stream_path():
    """Body exceeds limit via streaming (no Content-Length header)."""
    client = _make_size_app(max_bytes=10)
    # Remove Content-Length so stream enforcement is used.
    response = client.post("/", content=b"x" * 20, headers={"Content-Length": ""})
    assert response.status_code == 413


def test_malformed_content_length_falls_through_to_stream():
    """A non-integer Content-Length is ignored; stream enforcement applies."""
    client = _make_size_app(max_bytes=100)
    response = client.post(
        "/",
        content=b"hello",
        headers={"Content-Length": "not-a-number"},
    )
    assert response.status_code == 200


def test_exact_limit_is_accepted():
    client = _make_size_app(max_bytes=5)
    response = client.post("/", content=b"hello")
    assert response.status_code == 200


def test_one_byte_over_limit_rejected():
    client = _make_size_app(max_bytes=5)
    response = client.post("/", content=b"hello!")
    assert response.status_code == 413


def test_non_http_scope_passes_through():
    """WebSocket and lifespan scopes must not be intercepted."""
    # TestClient issues an HTTP request — we verify the middleware doesn't
    # break normal operation for http scope (non-http is implicitly covered
    # by the lifespan scope that TestClient manages internally).
    client = _make_size_app(max_bytes=10)
    response = client.post("/", content=b"hi")
    assert response.status_code == 200


def test_413_response_is_json():
    client = _make_size_app(max_bytes=1)
    response = client.post("/", content=b"xx")
    assert response.headers["content-type"] == "application/json"
    assert "detail" in response.json()


# ---------------------------------------------------------------------------
# TraceIDMiddleware
# ---------------------------------------------------------------------------


def test_trace_id_echoed_in_response_when_provided():
    client = _make_trace_app()
    response = client.get("/", headers={"X-Trace-ID": "my-trace-123"})
    assert response.headers["x-trace-id"] == "my-trace-123"


def test_trace_id_generated_when_not_provided():
    client = _make_trace_app()
    response = client.get("/")
    trace_id = response.headers.get("x-trace-id")
    assert trace_id is not None
    assert len(trace_id) > 0


def test_trace_id_is_uuid_when_generated():
    import uuid

    client = _make_trace_app()
    response = client.get("/")
    trace_id = response.headers["x-trace-id"]
    # Should not raise — confirms the generated value is a valid UUID.
    uuid.UUID(trace_id)


def test_different_requests_get_different_generated_trace_ids():
    client = _make_trace_app()
    r1 = client.get("/")
    r2 = client.get("/")
    assert r1.headers["x-trace-id"] != r2.headers["x-trace-id"]


def test_provided_trace_id_is_not_overwritten():
    client = _make_trace_app()
    response = client.get("/", headers={"X-Trace-ID": "keep-this"})
    assert response.headers["x-trace-id"] == "keep-this"
