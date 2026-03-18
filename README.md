# Events — Distributed Event Processing Platform

![CI](https://github.com/bkey/feathr/actions/workflows/ci.yml/badge.svg)

A production-grade API for ingesting, processing, querying, and searching high-volume web events. Built with FastAPI, MongoDB, Elasticsearch, Redis, and Celery.

## Architecture

```
Client
  │
  ▼
FastAPI (port 8000)
  │
  ├── POST /v1/events             → Redis (Celery broker)
  │                                        │
  │                                        ▼
  │                                 Celery Worker
  │                                  ├── MongoDB (persistence)
  │                                  └── Elasticsearch (search index)
  │
  ├── GET /v1/events               → MongoDB
  ├── GET /v1/events/search        → Elasticsearch
  ├── GET /v1/events/stats         → MongoDB
  └── GET /v1/events/stats/realtime→ Redis cache → MongoDB (on miss)
```

## API

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/v1/events/` | Enqueue a batch of events |
| `GET` | `/v1/events/` | List events with pagination |
| `GET` | `/v1/events/search?q=` | Full-text search via Elasticsearch |
| `GET` | `/v1/events/stats?period=` | Event counts grouped by type and time period; optionally filtered by `?type=` |
| `GET` | `/v1/events/stats/realtime` | Per-type event counts over the last 5 minutes, served from Redis cache |

### Event schema

```json
{
  "event_id": "uuid",
  "type": "pageview | click | conversion",
  "timestamp": "2024-01-01T00:00:00Z",
  "user_id": "user-123",
  "source_url": "https://example.com",
  "metadata": {
    "field1": "test",
    "field2": "foo"
  }
}
```

`event_id` is optional. If omitted, a UUID is assigned at ingestion time. Supplying a stable client-side ID makes retried requests idempotent — duplicate submissions are ignored.

Batches of up to 500 events can be posted in a single request.

## Getting Started

### Prerequisites

- [uv](https://docs.astral.sh/uv/) — Python package manager
- Docker and Docker Compose

### Run locally

```bash
docker compose up
```

This starts the API on port 8000 along with MongoDB, Elasticsearch, and Redis.

Once running, interactive API docs are available at:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

### Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `MONGODB_URL` | Yes | MongoDB connection string |
| `ELASTICSEARCH_URL` | Yes | Elasticsearch connection string |
| `REDIS_URL` | Yes | Redis connection string (Celery broker) |
| `HMAC_SECRET` | Yes | Secret key used to sign the Redis stats cache payload |
| `DB_NAME` | No (default: `feathr`) | MongoDB database name |
| `EVENTS_COLLECTION` | No (default: `events`) | MongoDB collection name |
| `EVENTS_INDEX` | No (default: `events`) | Elasticsearch index name |
| `MAX_BATCH_SIZE` | No (default: `500`) | Maximum events per POST request |
| `CORS_ALLOWED_ORIGINS` | No (default: `[]`) | Comma-separated list of allowed CORS origins |
| `PORT` | No (default: `8000`) | API server port |
| `ES_NUMBER_OF_REPLICAS` | No (default: `0`) | Elasticsearch replica count — set to `1` or more for multi-node clusters |

Copy `example.env` to `.env` to configure the Docker Compose services.

## Testing

### Running the Tests

Run the full suite (unit + integration) together:                                                                                                        
                                                                                                                                                               
```bash
uv run pytest
```

The integration tests require MongoDB, Elasticsearch, and Redis. You can start them with:                                                                     

```
  docker compose up mongo elasticsearch redis -d   
```

### Testing Philosophy

I believe that if you write code, it should include tests. Tests not only help identify bugs, but they also serve as working examples of how the code should be used and how it behaves under different conditions—effectively acting as living documentation. Designing code with testing in mind from the start makes the testing process easier and often results in cleaner, better-structured code. Tests should also be run frequently, ideally through automated processes.

Unit tests are generally preferred because they are faster, more focused, and easier to debug. They test small pieces of code in isolation, so they run quickly and failures usually point directly to the problem. This makes them ideal for running frequently during development.

Integration tests verify that multiple components work together, which is important, but they are typically slower, more complex, and harder to diagnose when they fail.

I usually write many unit tests and fewer integration tests, using unit tests for logic and integration tests for key interactions between components.

### What I'd prioritize with more time

- **Contract tests for the Celery task** — the current unit tests inject mocks directly onto the task singleton, which works but is brittle. A contract test would verify the serialized task payload matches what the worker expects, catching serialization regressions before they reach production.
- **Load tests for the ingestion pipeline** — correctness tests don't tell you whether the pipeline holds up at volume. A locust or k6 script hitting `POST /v1/events` at sustained throughput would expose queue depth buildup, worker lag, and MongoDB write contention before they appear in production.
- **ES search accuracy tests** — the integration tests verify events are indexed, but don't assert that specific queries return the expected results.


## Queue

This app uses Celery with Redis as a message queue. The API enqueues via `process_events.delay`. The worker picks up the task and writes to MongoDB + Elasticsearch.                                                                            
                                                                                                                                                       
### Guarantees                                                                                                                                  
                                                                                                                                                              
* At-least-once delivery — Celery's default acknowledgement model: the task message is only ACKed after the handler returns successfully. If the worker       
  crashes mid-execution, Redis will redeliver the message to another worker. Combined with the ordered=False bulk insert and the duplicate key (E11000)
  swallowing logic, this makes the pipeline idempotent — retries won't create duplicate documents.

* Bounded retries with backoff — max_retries=3 with exponential countdown (2**self.request.retries → 1s, 2s, 4s). After exhaustion, the task is Rejected with
  requeue=False and written to the `dead_letter_events` MongoDB collection for inspection and replay.

  To inspect failed events:
  ```js
  db.dead_letter_events.find().sort({ failed_at: -1 })
  ```
  Each document contains the original `events` array, the `reason` string, the number of `retries`, and a `failed_at` timestamp.

* Serialization safety — task_serializer="json" and accept_content=["json"] prevent pickle deserialization attacks. The event documents are already plain
  dicts by the time they're enqueued.

* Process isolation for DB clients — EventsTask lazily initializes mongo_client and es_client as per-process singletons. Each worker process gets its own
  connection; they're not shared across processes or tasks.

### What would change for SQS

The `app/tasks/event_queue.py` is meant to provide a wrapper so that Celery could easily be replace with SQS.

* SQS would increase reliability and durability. Messages are stored redundantly across multiple servers, so they aren’t lost if a component fails.

* SQS scales automatically with traffic, handling anything from a few messages to very large workloads without manual infrastructure management.

* SQS uses visibility timeouts — if a worker doesn't delete the message within the window, it becomes visible again. You'd need to tune visibility_timeout to
  be longer than your worst-case processing time, or actively extend it for slow batches. Celery does support this via kombu's SQS transport, but it's easy to
   misconfigure.

* Redis queues are FIFO and immediate. SQS Standard offers no ordering guarantee and has a ~1M TPS limit with at-least-once delivery. SQS FIFO gives
  exactly-once within a 5-minute deduplication window — with that, you could remove the E11000 swallowing logic and let the queue handle deduplication
  instead.

* `process_events.delay()` will happily enqueue into Redis even if the queue depth is unbounded. SQS queues have a 120,000 in-flight message limit and you'd
  want CloudWatch alarms on ApproximateNumberOfMessagesNotVisible to detect worker lag before it becomes a problem.

* SQS has a 256KB per-message limit. With max_batch_size=500 events that each include metadata (up to 10KB each), a single task payload could easily exceed
  this. You'd need to either reduce batch sizes, store the payload in S3 and pass only the S3 key in the SQS message (the S3 event notification pattern), or
  split large batches into multiple messages at the API layer.

* Redis auth is a password in the URL. SQS uses IAM roles — you'd remove the broker URL credentials entirely and let the worker's instance profile or ECS task
   role provide access. No secrets to rotate.


## Caching strategy

### What's cached and why

`GET /v1/events/stats/realtime` is cached in Redis using an HMAC-signed payload to prevent cache poisoning. The TTL is 10 seconds.

`GET /v1/events/stats?period=` queries MongoDB directly on every request. Period aggregations are less latency-sensitive and better suited to query optimisation (early `$project`, `allowDiskUse`) than a cache layer.

### TTL rationale

  The realtime TTL is 10 seconds. The rationale is:

  - The endpoint is described as "realtime" so staleness should be minimal
  - The aggregation window is 5 minutes of data — at low-to-moderate write volume, 10s of staleness is imperceptible relative to that window
  - It's a read-heavy endpoint (rate limited to 60/minute per client) that could be hit by dashboards polling on an interval — without caching, every poll hits MongoDB with an aggregation scan

  10 seconds is essentially arbitrary. The right value depends on how often your dashboard polls and how much MongoDB aggregation load you're willing to accept. It's exposed as a setting precisely because there's no universal answer.

### Cache invalidation approach

  This uses a pure TTL-based expiry. There's no active invalidation when events are written. A fresh write won't appear in realtime stats for up to 10 seconds. Cache entries just expire naturally. Simplicity is the key benefit of this strategy. Cache invalidation is famously one of the two hard problems of Computer Science. However it makes the assumption that the data doesn't need to be immediately consistent.


### What would change under higher write volume

  With a higher write volume, short TTL becomes expensive. At high write rates, a 10s TTL means MongoDB runs a full aggregation every 10 seconds regardless of how many clients are
  hitting the endpoint. The aggregation scans 5 minutes of events — if you're ingesting thousands of events per second, that's a large scan repeated
  constantly. Options:

  - Push-based cache population: have the Celery worker/SQS update the Redis cache after each successful insert_many rather than letting it expire and rebuilding
  on demand. The cache is always warm; reads never hit MongoDB.
  - Increment counters instead of aggregating: maintain a Redis hash of {type: count} that the worker increments atomically (HINCRBY) on each insert. The
  realtime endpoint just reads the hash — no MongoDB involved at all. You'd use a sliding window with a sorted set (events bucketed by timestamp) or accept
  that the counter resets on Redis restart.
  - Longer TTL with write-through invalidation: extend TTL to 60s+ but explicitly delete or update the cache key when the worker processes a batch. Requires
  the worker to know the cache key, which it currently doesn't.

  Redis becomes a single point of failure. Currently Redis being down just means every realtime stats request falls back to MongoDB. Under high read volume
  that fallback could overwhelm MongoDB. You'd want Redis Sentinel or Cluster, and potentially a circuit breaker that returns a degraded response (last known
  value + a staleness indicator) rather than hammering the DB.

  HMAC verification on every cache hit has negligible cost now but if the payload grows (many event types, long type strings), you're deserializing JSON twice
   per hit (once for the envelope, once for the payload). At high read volume you'd want to benchmark whether in-memory caching at the application layer (e.g.
   a process-local cache with a shorter TTL than Redis) makes sense to avoid repeated Redis round-trips entirely.

## MongoDB index reasoning

`GET /v1/events` builds a filter from up to five optional parameters: `type`, `user_id`, `source_url`, `date_from`, `date_to`. Results are always paginated with `skip`/`limit`.

All five parameters are optional and can be combined freely. Each compound index is designed to cover the most common pattern for its leading field: a single string filter, optionally narrowed by a date range.

### Compound indexes: `(type, timestamp DESC)`, `(user_id, timestamp DESC)`, `(source_url, timestamp DESC)`

Each compound index covers two cases with one structure:

- **Single-field filter** (`?type=pageview`): MongoDB uses the first key of the index, scans only matching documents, returns them pre-sorted by `timestamp`. No in-memory sort needed.
- **Field + date range** (`?type=pageview&date_from=...&date_to=...`): MongoDB uses the first key for equality, then the second key to satisfy the range — the index covers the entire query. This is the most important case because unbounded date scans on large collections are expensive.

`timestamp` is `DESC` because most queries want recent events first, matching the natural sort order of the index and avoiding a blocking sort step.

A bare `(type)` index would handle equality filters but force a collection scan for any date range. A bare `(timestamp)` index wouldn't help `type`/`user_id`/`source_url` filters at all. The compound is strictly better for both cases.

Verified with `explain("executionStats")` on 10,000 documents:

| Query | Plan | docsExamined | keysExamined |
|-------|------|-------------|--------------|
| `?type=pageview` | `IXSCAN` → `type_1_timestamp_-1` | 3,334 | 3,334 |
| `?type=pageview&date_from=...` | `IXSCAN` → `type_1_timestamp_-1` | 3,334 | 3,334 |
| `?user_id=user-1` | `IXSCAN` → `user_id_1_timestamp_-1` | 2,000 | 2,000 |
| `?source_url=...` | `IXSCAN` → `source_url_1_timestamp_-1` | 3,334 | 3,334 |

Keys examined equals docs examined in every case — no wasted key scans, no in-memory sorts.

### Bare index: `(timestamp DESC)`

Serves three query patterns that don't involve the string filter fields:

- **Date-only queries** (`?date_from=...&date_to=...` with no other filters): produces `{"timestamp": {"$gte": ..., "$lte": ...}}` — the bare `timestamp` index handles this directly (`IXSCAN`, 10,000 keys examined for a full-range query on the test dataset).
- **Realtime stats** (`GET /v1/events/stats/realtime`): the `$match` stage filters `{"timestamp": {"$gte": since}}`. Without this index, the aggregation scans the entire collection on every request. With it, only documents within the 5-minute window are examined (2 docs on a 10k collection in testing).
- **Historical stats** (`GET /v1/events/stats`): without a `?type=` filter the pipeline has no `$match`, so the full index is scanned. The pipeline is hinted to use `type_1_timestamp_-1` as a covered query — MongoDB reads only index entries and fetches zero documents (`docsExamined: 0`, `keysExamined: 10,000`, plan: `PROJECTION_COVERED`). With `?type=pageview`, a `$match` stage is prepended and the same hint narrows the scan to only the matching type prefix — fewer keys examined, same zero document fetches.

### What's not indexed

- **`metadata`** — a freeform dict with dynamic keys, queried only through Elasticsearch. Indexing it in MongoDB would require knowing the key paths in advance or using a wildcard index, neither of which is warranted when ES already handles that search path.
- **`_id`** — MongoDB creates a unique index on `_id` automatically. `_id` is always a fresh UUID assigned at ingestion and is not exposed through the API.
- **`event_id`** — the unique index on `event_id` provides the deduplication guarantee that makes Celery retries idempotent. Supplying a stable client-side `event_id` ensures duplicate submissions are ignored.

### Gaps to address at higher scale

The compound indexes don't help if a caller filters by, say, `user_id` without a date range and expects results sorted by something other than `timestamp`. If sorted pagination on other fields becomes a requirement, you'd need to extend the index or add new ones — but adding indexes has a write-amplification cost, so they should be added in response to observed query patterns, not speculatively.

## Elasticsearch index mapping

The events index uses an explicit mapping defined in `app/db/elastic.py` and applied at startup via `ensure_index()`.

```json
{
  "mappings": {
    "properties": {
      "event_id":   { "type": "keyword" },
      "type":       { "type": "keyword" },
      "timestamp":  { "type": "date" },
      "user_id":    { "type": "keyword" },
      "source_url": { "type": "text" },
      "metadata":   { "type": "object", "dynamic": true }
    }
  }
}
```

### Field type choices

**`event_id` → `keyword`**
Stored as a top-level field separate from the ES document `_id`. Returned by `GET /v1/events` and `GET /v1/events/search`, and can be filtered directly in ES queries if needed.

**`type`, `user_id` → `keyword` with `lowercase_normalizer`**
Used for exact-match filtering and aggregations, not full-text search. `keyword` prevents tokenization — `"pageview"` is stored and matched as a single term. The `lowercase_normalizer` folds values to lowercase at both index and query time, making searches case-insensitive.

**`source_url` → `text`**
Mapped as `text` so the standard analyzer tokenizes URLs into terms (e.g. `https://target.example.com/` → `["https", "target", "example", "com"]`). This allows `simple_query_string` to match on domain components naturally. Keyword exact-match is not useful here because stored values include the full URL scheme and path, which users never search for verbatim.

**`timestamp` → `date`**
Enables range queries (`$gte`/`$lte`) and date histogram aggregations natively. ES parses ISO 8601 strings automatically.

**`metadata` → `object` with `dynamic: true` and `lowercase_normalizer`**
The metadata schema is intentionally open-ended — callers can send arbitrary key/value pairs. `dynamic: true` means ES automatically maps new keys as they appear. String values are mapped as `keyword` with `lowercase_normalizer` via a dynamic template, which suits structured data such as device type, browser name, and campaign IDs. The `metadata_key` query parameter (e.g. `?metadata_key=device`) narrows the search to a single known field.

### Known limitation

The `metadata.*` wildcard in `simple_query_string` relies on ES expanding the wildcard across all dynamically mapped sub-fields at query time. This works reliably once fields have been mapped (i.e. at least one document with that key has been indexed), but fields that have never been seen will not match. The `?metadata_key=` parameter is the recommended approach when targeting a specific known key.

## Rate Limiting

All endpoints are rate-limited per client IP. Exceeding a limit returns `429 Too Many Requests`.

| Endpoint | Limit |
|----------|-------|
| `POST /v1/events/` | 100 req/min |
| `GET /v1/events/` | 60 req/min |
| `GET /v1/events/search` | 30 req/min |
| `GET /v1/events/stats` | 30 req/min |
| `GET /v1/events/stats/realtime` | 60 req/min |

## AI in My Workflow

### Tools used

Claude (Sonnet 4.6) via Claude Code. I used it throughout the entire build, not just for isolated questions. The first thing I did was create an CLAUDE.md file based on one I had used previously. In general, I approach using AI like having another developer on my team, working with me. I favor an iterative approach to development regardless if AI is involved or not. I break a project like this into individual tasks. For example, the first step of this project which I gave to Claude was to create an API with POST /events and GET /events endpoints with FastAPI using a MongoDB database. From there, I made some changes to code structure and setup myself. After that I continued to iterate, adding new endpoints/databases and reviewing continuously.

### How it helped

**Code review as a forcing function.** I used a custom `/code-reviewer` skill repeatedly across multiple rounds. Each pass surfaced a different category of issue: the first round caught missing CORS config and hardcoded credentials in docker-compose; a later round caught that `bulk(body=)` had been deprecated in elasticsearch-py 9.x in favor of `bulk(operations=)`; another caught that a bare `Exception` in the connect functions would silently swallow programming errors alongside connection failures. Running structured reviews on a cadence kept quality from drifting as the codebase grew.

**Stress-testing architectural decisions.** I used Claude to pressure-test specific choices before committing to them — particularly around the caching strategy and the queue design. Asking "what breaks under higher write volume?" produced the push-based cache population and Redis counter increment alternatives documented in the README. Asking "what's missing compared to a real SQS implementation?" produced the visibility timeout, message size, and IAM sections in the documentation. These weren't answers I needed to look up — they were a way of making sure I hadn't missed anything obvious.

**Identifying tradeoffs I hadn't considered.** The `metadata.*` wildcard issue in Elasticsearch is a good example. I'd built the search endpoint and it appeared to work, but when I tested with a fresh index the wildcard was unreliable — fields that hadn't been seen yet weren't matched. Claude identified that `simple_query_string` with `metadata.*` expands at query time against already-mapped fields only, and suggested dynamic templates as the fix. This was a subtle correctness issue that wouldn't have surfaced in mocked unit tests.

**Refactoring structure.** As the project grew I used Claude to critique the file structure and then execute some moves. Each move was a deliberate decision, not automatic: Claude would propose where something belonged and why, I'd agree or redirect.

### Where I pushed back or corrected AI output

In addition to the structural changes I made mentioned above, there were multiple other instances where I disagreed with the AI about what to do. Here are a few examples:

**The `_build_query` location.** Claude initially suggested moving it to `db/mongo.py` on the grounds that it builds a MongoDB query document. I disagreed — it takes HTTP query parameters as inputs, which means it belongs closer to the request handling layer, not the database layer. We landed on `services/events.py` as a middle ground.

**`PaginatedEvents` in `models/stats.py`.** When splitting stats models out of `events.py`, Claude initially included `PaginatedEvents` in the new `stats.py`. I caught that it references the `Event` model, which would create a cross-module dependency. It stayed in `events.py`.

### How AI shaped my overall approach

The main effect was pace. The feedback loop that would normally take the form of a code review with a teammate happened inline and continuously. This meant I could move faster on implementation and still catch the class of issues — stale API usage, missing error paths, exception hierarchy mistakes — that tend to accumulate when working alone. The tradeoff is that AI review is better at spotting known patterns than at identifying novel design problems, so I still had to do the higher-level architectural thinking myself. The most useful sessions were the ones where I used Claude to interrogate a decision I'd already made, not to make the decision for me.

I also tried using Claude's `/voice` command for the first time which was fun for me personally.