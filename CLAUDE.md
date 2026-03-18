# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
uv sync --all-groups

# Run all tests
uv run pytest

# Run a single test file
uv run pytest tests/test_events.py

# Run a single test
uv run pytest tests/test_events.py::test_post_valid_event

# Run with coverage
uv run pytest --cov=app

# Start the API (requires MongoDB and Redis to be running)
uv run uvicorn app.main:create_app --factory --app-dir . --port 8000

# Start the Celery worker
uv run celery -A tasks.worker.celery_app worker --loglevel=info

# Start all services (API + worker + MongoDB + Redis + Elasticsearch)
docker compose up
```

## Package Management

Use `uv` exclusively — never `pip`.
- Add a dependency: `uv add package`
- Add a dev dependency: `uv add --dev package`
- Upgrade a package: `uv add --dev package --upgrade-package package`
- Forbidden: `uv pip install`, `@latest` syntax

## Architecture

**Request lifecycle for event ingestion:**
1. `POST /v1/events` validates the batch (Pydantic), enqueues it via `process_events.delay()` (Celery → Redis broker), returns 202 immediately
2. The Celery worker picks up the task, writes to MongoDB with `insert_many(ordered=False)`, and bulk-indexes to Elasticsearch
3. Duplicate key errors (E11000) on retry are swallowed — processing is idempotent

**Read paths:**
- `GET /v1/events` → MongoDB, filtered query built by `_build_query()` in `routers/v1/events.py`
- `GET /v1/events/search` → Elasticsearch `simple_query_string` across `type`, `user_id`, `source_url`, `metadata.*`
- `GET /v1/events/stats` → MongoDB `$dateTrunc` aggregation pipeline
- `GET /v1/events/stats/realtime` → Redis cache (10s TTL), falls back to MongoDB on miss or cache corruption

**Middleware stack** (outermost first): `RequestSizeLimitMiddleware` (pure ASGI, 5MB body limit) → `TraceIDMiddleware` (injects `X-Trace-ID`, populates `trace_id_var` for structured logging)

**Line length**: 88 characters maximum (enforced by ruff).

**Configuration** (`app/constants.py`): All config is via `pydantic_settings.BaseSettings`. Environment variables map directly to field names (e.g. `MONGODB_URL`, `REDIS_URL`, `ELASTICSEARCH_URL`). Module-level constants are backward-compat aliases for `settings.*` — always add new config to the `Settings` class, not as bare `os.getenv()` calls.

**Celery client injection** (`app/tasks/events.py`): `EventsTask` is a base task class with `mongo_client` and `es_client` as lazily-initialized properties. Clients are stored as instance attributes on the task singleton (one per worker process). Tests inject mocks by setting `process_events._mongo_client` and `process_events._es_client` directly.

**Logging**: `configure_logging()` in `main.py` installs a JSON formatter at the root logger. Every log line includes `trace_id` from `trace_id_var` (a `contextvars.ContextVar`). Call `configure_logging()` before any module-level loggers are created.

**ES index mapping** is defined in `db/elastic.py:_EVENTS_MAPPING` and applied at startup via `ensure_index()`. `metadata` uses `dynamic: True` — new keys are auto-mapped. The `metadata.*` wildcard in search requires this.

**MongoDB indexes** are defined in `db/mongo.py:_INDEXES` and applied idempotently at startup. Compound indexes are `(field, timestamp DESC)` to cover both single-field filters and combined field+date-range queries.

## Testing

Tests live in `tests/` with `app/` on `sys.path` (set in `pyproject.toml`). Unit tests use mocks. Integration tests (under `tests/integration/`) require real services and are run via `docker compose -f docker-compose.test.yml up`.

Key fixtures in `conftest.py`:
- `client` — `TestClient` with all external dependencies mocked
- `mock_collection` — MongoDB collection mock with async cursor chains pre-configured
- `mock_task` — patches `routers.v1.events.process_events`

For task tests (`test_tasks.py`), inject mocks directly onto the singleton: `process_events._mongo_client = ...`. The `reset_task_clients` autouse fixture cleans up between tests.

## PR Reviews

- Always add `bkey` as reviewer.
- PR descriptions should focus on what problem is solved and how, not code-level specifics.
