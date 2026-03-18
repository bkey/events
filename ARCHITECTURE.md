
# Event Ingestion & Query Service — Architecture

## Overview

This service ingests high-volume event data, persists it as the system of record, and exposes APIs for querying, search, and aggregations.

The system is designed to:

* Decouple ingestion from persistence for low-latency writes
* Provide strong consistency for primary data access
* Support full-text search via a secondary index
* Scale horizontally across ingestion and query paths

---

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

---

## Request Lifecycle

### Write Path (`POST /v1/events`)

1. Request is validated synchronously in FastAPI
2. Event batch is enqueued to Redis (Celery broker)
3. API returns `202 Accepted`
4. Celery worker asynchronously:

   * Persists events to MongoDB
   * Indexes events into Elasticsearch (best-effort)

**Durability guarantee:**
An event is considered durable only after a successful MongoDB write.

**Delivery semantics:**
At-least-once processing (duplicates possible, handled via idempotency).

---

### Read Paths

| Endpoint                    | Source of Truth         | Notes                 |
| --------------------------- | ----------------------- | --------------------- |
| `/v1/events`                | MongoDB                 | Strongly consistent   |
| `/v1/events/search`         | Elasticsearch           | Eventually consistent |
| `/v1/events/stats`          | MongoDB                 | Aggregation pipeline  |
| `/v1/events/stats/realtime` | Redis (cache) → MongoDB | Cache TTL 10s; covers last 1 hour of data |

---

## Consistency Model

* **MongoDB**

  * Primary system of record
  * Strong consistency for reads and writes

* **Elasticsearch**

  * Eventually consistent with MongoDB
  * Indexing occurs asynchronously via Celery

* **Redis cache (`/stats/realtime`)**

  * Time-based consistency (TTL = 10 seconds)

**Important:**

* No cross-system transactional guarantees exist between MongoDB and Elasticsearch
* Search results may lag behind primary data
* Cached stats may be stale within the TTL window

---

## Component Responsibilities

| Component         | Responsibility                                                                  |
| ----------------- | ------------------------------------------------------------------------------- |
| **FastAPI**       | Request validation, rate limiting, enqueueing ingestion jobs, serving read APIs |
| **Redis**         | Celery broker (ingestion queue) and cache for realtime stats                    |
| **Celery Worker** | Asynchronous processing of event batches                                        |
| **MongoDB**       | Source of truth for all events and aggregations                                 |
| **Elasticsearch** | Full-text search index                                                          |

---

## Storage Rationale

### MongoDB (Primary Store)

* Optimized for high write throughput
* Flexible document schema maps directly to event payloads
* Supports aggregation pipelines (e.g. `$dateTrunc`) used by `/stats`
* Serves as the authoritative data source for all non-search queries
* `$jsonSchema` collection validator enforces required fields and BSON types at write time, rejecting malformed documents before they reach the application
* Stats aggregations are bounded to a 90-day default lookback window to prevent unbounded collection scans as data grows

---

### Elasticsearch (Search Index)

* Provides full-text search, tokenization, and relevance scoring
* Avoids performance impact on primary write path via async indexing
* Uses MongoDB `_id` as document ID for idempotent indexing
* Search supports offset-based pagination (`offset` + `limit`) for navigating large result sets
* Queries are bounded by a 5s server-side timeout; ES returns partial results rather than blocking indefinitely

---

### Redis

**Broker:**

* Decouples ingestion from persistence
* Enables non-blocking API writes (low latency)

**Cache:**

* Stores `/stats/realtime` results
* TTL = 10 seconds to reduce repeated aggregation load

---

## Failure Modes

### MongoDB Unavailable

* **Impact:**

  * API fails to start
  * Worker cannot persist events

* **Behavior:**

  * `process_events` retries 3 times with exponential backoff (1s, 2s, 4s)
  * Failed batches are written to `dead_letter_events`

* **Recovery:**

  * Manual inspection and replay of DLQ required

---

### Elasticsearch Unavailable

* **Impact:**

  * `/v1/events/search` returns `503`
  * Newly ingested events are not searchable

* **Unaffected:**

  * Event ingestion (MongoDB writes succeed)
  * `/events`, `/stats`

* **Behavior:**

  * Worker skips indexing and logs warning

* **Recovery:**

  * Manual re-index from MongoDB required
  * No automated backfill currently implemented

---

### Worker Crash During Processing

* **Behavior:**

  * Celery configured with `acks_late=True`
  * Task is re-queued if worker crashes mid-processing

* **Idempotency guarantees:**

  * MongoDB `insert_many(ordered=false)` ignores duplicate key errors
  * Elasticsearch uses `_id` for idempotent indexing

* **Result:**

  * Safe at-least-once processing

---

### Redis Unavailable

* **Impact:**

  * `POST /v1/events` fails (cannot enqueue)
  * `/stats/realtime` cache unavailable

* **Unaffected:**

  * All MongoDB-backed read endpoints

---

## Scaling Characteristics

### Current Limits (≈10× Volume)

1. **Celery worker throughput**
2. **MongoDB write capacity**
3. **Elasticsearch indexing lag**
4. **Redis broker saturation**
5. **Deep pagination (`skip(N)`) inefficiency**
6. **Aggregation scan cost at high cardinality**
7. **Elasticsearch ingest pipeline overhead**

---

### Scaling Strategies

* **Horizontal worker scaling**

  * Add multiple Celery (or SQS) workers

* **MongoDB scaling**

  * Shard on `timestamp` or `user_id`
  * Optimize write-heavy indexes

* **Decouple indexing**

  * Separate queue for Elasticsearch indexing

* **Broker scaling**

  * Redis Cluster or Kafka for higher throughput

* **Pagination**

  * Replace offset pagination with cursor-based pagination:

* **Pre-aggregation**

  * Materialize stats (e.g. hourly rollups) into separate collection

* **Elasticsearch optimization**

  * Benchmark ingest pipeline (normalizers, token filters)

---

## Operational Considerations

### Idempotency

* Required due to at-least-once delivery
* Achieved via:

  * MongoDB unique index on `event_id` — duplicate events are silently skipped
  * Elasticsearch uses the MongoDB `_id` as its document ID, making re-indexing a no-op overwrite

---

### Observability (Recommended)

* Queue depth (Redis)
* Task latency (enqueue → completion)
* MongoDB write latency
* Elasticsearch indexing lag
* DLQ size

---

### Backpressure

* Currently implicit via Redis queue growth
* No explicit rate limiting based on downstream capacity

---

## Known Limitations

* No automatic Elasticsearch backfill
* Single queue couples persistence and indexing latency
* DLQ is not externally accessible
* Offset pagination does not scale for large datasets
* Aggregations are computed on raw data (no rollups)

---

## What I'd do With More Time

1. **Elasticsearch backfill job**

   * Automatically reconcile missing indexed events

2. **Separate persistence and indexing queues**

   * Prevent ES slowdown from affecting ingestion

3. **DLQ replay endpoint**

   * `dead_letter_events` collection exists and captures failed batches; a `GET /v1/events/dlq` and `POST /v1/events/dlq/{id}/replay` would close the loop

4. **Pre-aggregated stats**

   * Reduce aggregation cost at high volume

5. **Elasticsearch Index Lifecycle Management (scale-dependent)**

   * Time-based index rollover and tiered storage

---

## Summary

This architecture prioritizes:

* Low-latency ingestion via async processing
* Strong consistency for primary data access
* Eventual consistency for search
* Horizontal scalability across ingestion and querying

The primary tradeoff is **consistency vs performance**, with MongoDB as the source of truth and Elasticsearch as a best-effort secondary index.

