# Architecture

## System Diagram

```
                        ┌─────────────────────────────────────────┐
                        │               FastAPI                    │
                        │                                          │
  Client ──────────────▶│  POST /v1/events                        │
                        │    └── validate → enqueue ──────────────┼──▶ Redis (broker)
                        │                                          │         │
                        │  GET /v1/events                          │         ▼
                        │    └──────────────────────────────────── ┼──▶ MongoDB
                        │                                          │
                        │  GET /v1/events/search                   │
                        │    └──────────────────────────────────── ┼──▶ Elasticsearch
                        │                                          │
                        │  GET /v1/events/stats                    │
                        │    └──────────────────────────────────── ┼──▶ MongoDB
                        │                                          │
                        │  GET /v1/events/stats/realtime           │
                        │    ├── cache hit ──────────────────────── ┼──▶ Redis (cache)
                        │    └── cache miss ─────────────────────── ┼──▶ MongoDB
                        └─────────────────────────────────────────┘
                                          │
                                    Redis (broker)
                                          │
                        ┌─────────────────▼───────────────────────┐
                        │           Celery Worker                  │
                        │                                          │
                        │  process_events                          │
                        │    ├── insert_many ────────────────────── ┼──▶ MongoDB
                        │    └── bulk index ─────────────────────── ┼──▶ Elasticsearch
                        └─────────────────────────────────────────┘
```

## Component Responsibilities

| Component | Responsibility |
|-----------|----------------|
| **FastAPI** | Validates incoming events, enforces rate limiting, enqueues work, serves read queries |
| **Redis** | Celery broker (task queue for ingestion) and read cache (`/stats/realtime`) |
| **Celery Worker** | Persists event batches to MongoDB and indexes them in Elasticsearch |
| **MongoDB** | Primary store for all events — source of truth for pagination and stats aggregations |
| **Elasticsearch** | Search index for full-text queries across event fields |

## Storage Rationale

**MongoDB** is the source of truth. It handles writes well at volume, supports the `$dateTrunc` aggregation used by `/stats`, and its document model maps naturally to the event schema.

**Elasticsearch** handles search. MongoDB's text search is limited — Elasticsearch provides proper full-text analysis and relevance scoring with no impact on write throughput since indexing is async.

**Redis** serves two roles: Celery broker and read cache. As a broker it keeps ingestion non-blocking — the API returns 202 immediately and the worker handles persistence, preventing slow DB writes from affecting client-facing latency. As a cache it stores the `/stats/realtime` aggregation result with a 10-second TTL, avoiding a MongoDB aggregation scan on every poll.

## Failure Modes

**MongoDB unavailable**
- API startup fails fast — MongoDB is required
- `process_events` retries up to 3 times with exponential backoff (1, 2, 4s), then rejects the message
- Rejected messages are written to the `dead_letter_events` MongoDB collection for inspection and replay

**Elasticsearch unavailable**
- API starts successfully and all non-search endpoints continue to function
- `/search` returns 503
- The Celery worker starts successfully; `process_events` persists to MongoDB and logs a warning, skipping the ES index step
- Events ingested while ES is down will be missing from search results and must be re-indexed from MongoDB once ES recovers — no automated backfill is currently implemented

**Worker crashes mid-batch**
- The Celery task is re-queued by the broker (assuming `acks_late=True` or default visibility timeout)
- MongoDB `insert_many` uses `ordered=False` and ignores duplicate key errors (E11000) so re-delivery is safe
- Elasticsearch `bulk` uses the MongoDB `_id` as the document ID, making re-indexing idempotent

**Redis unavailable**
- `POST /v1/events` will fail — events cannot be enqueued
- Read endpoints (`GET /events`, `/search`, `/stats`) are unaffected

## Scaling Considerations

At 10x event volume, the likely bottlenecks would be:

1. **Single Celery worker** — add more worker replicas; Celery scales horizontally with no coordination needed
2. **MongoDB write throughput** — add a write-optimised index strategy; consider sharding on `timestamp` or `user_id` for even distribution
3. **Elasticsearch indexing lag** — bulk indexing is efficient, but a dedicated indexing queue (separate from the persistence queue) would allow independent scaling and prevent index backpressure from blocking MongoDB writes
4. **Redis broker** — a single Redis node becomes a bottleneck; move to Redis Cluster or replace with Kafka for higher fan-out
5. **Pagination at depth** — `skip(N)` scans and discards N documents before returning results; at high volume with large offsets this becomes expensive. Cursor-based (keyset) pagination — `WHERE timestamp < last_seen AND _id < last_id` — eliminates the scan entirely but requires clients to carry an opaque cursor token rather than a page number
6. **Elasticsearch normalizer cost** — the `lowercase_normalizer` applied to all keyword fields is negligible at current volume, but at very high ingest rates per-field transformations accumulate across the bulk pipeline. Worth benchmarking if indexing throughput becomes a bottleneck before adding further normalizers or token filters

## What I'd Do Differently

- **ES backfill job** — a periodic task (or triggered job) to re-index any events present in MongoDB but missing from Elasticsearch, to recover from ES outages automatically. Currently events ingested while ES is down are permanently absent from search results.
- **Separate persistence and indexing queues** — decouple MongoDB writes from ES indexing so an ES slowdown doesn't affect ingestion latency. Right now a slow ES bulk index blocks the worker from acknowledging the task.
- **DLQ replay endpoint** — the dead-letter collection exists and captures permanently-failed batches, but there's no API to inspect or replay them. A `GET /v1/events/dlq` and `POST /v1/events/dlq/{id}/replay` would close that loop.
- **Elasticsearch index lifecycle management** — for high-volume production use, time-based index rollover (e.g. one index per day) with ILM policies would keep index sizes manageable and allow old data to be moved to cheaper storage tiers automatically.