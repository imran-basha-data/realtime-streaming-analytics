# Architecture

## Components

| Component | Code | Responsibility |
|---|---|---|
| Event model | `src/streaming/events.py` | Pydantic v2 schemas, parsing with reason codes, partition keys |
| Generator | `src/streaming/generator.py` | Deterministic (seeded) synthetic manufacturing and logistics events |
| Producer | `src/streaming/producer.py` | Idempotent Kafka producer with backpressure and delivery accounting |
| Topics | `src/streaming/topics.py` | Creates the events and DLQ topics with explicit partitions and retention |
| Consumer | `src/streaming/consumer.py` | Poll a batch, validate, persist, dead-letter, commit offsets |
| Store | `src/streaming/store.py`, `sql/schema.sql` | Idempotent, transactional writes of events and rollups |
| API | `src/streaming/api.py` | Read-only analytics over the rollups |
| Metrics | `src/streaming/metrics.py` | Prometheus metrics for the consumer (the API defines its own) |
| CLI | `src/streaming/cli.py` | `stream topics | produce | migrate | consume | api` |

One container image runs every role. Configuration comes only from `STREAM_*` environment variables (`src/streaming/config.py`).

## Data flow

```
                       key = plant_id | shipment_id
 generator ─► producer ───────────────────────────► manufacturing.events  (6 partitions, 7 d retention)
                                                          │
                                consume(max 500 msgs) ◄───┘
                                          │
                        parse_event() per message
                         ├─ valid ───────► accepted
                         └─ invalid ─────► dead letter (reason, detail, payload)
                                          │
              ┌───────────── one PostgreSQL transaction ─────────────┐
              │ INSERT processed_events ON CONFLICT DO NOTHING        │
              │   RETURNING event_id      → which ids are new         │
              │ UPSERT rollups for new ids only                       │
              │ INSERT dead_letters ON CONFLICT DO NOTHING            │
              └───────────────────────────────────────────────────────┘
                                          │ commit
                        publish dead letters ─► manufacturing.events.dlq (30 d retention)
                                          │ broker acknowledged
                        commit Kafka offsets (synchronous)
```

## Event schemas (version 1)

Every event shares this envelope:

| Field | Type | Rule |
|---|---|---|
| `event_id` | UUID | unique per event; the de-duplication key |
| `event_type` | string | discriminator, one of the four types below |
| `schema_version` | int | must be `1` |
| `occurred_at` | timestamp with timezone | naive timestamps are rejected; no more than `STREAM_MAX_CLOCK_SKEW_S` (default 300 s) in the future |
| `source` | string | 1–64 characters |
| `payload` | object | type-specific (below); unknown fields are rejected |

| `event_type` | Payload fields |
|---|---|
| `production.completed` | `plant_id` (`PLANT-XXX`), `line_id` (`L00`), `product_id` (`PRD-0000`), `units` (1–10 000), `scrap_units` (0–10 000) |
| `quality.inspected` | `plant_id`, `product_id`, `sample_size` (1–1 000), `defects` (0–sample size) |
| `shipment.dispatched` | `shipment_id` (`SHP-000000`), `origin_plant_id`, `destination`, `carrier`, `promised_at` |
| `shipment.delivered` | `shipment_id`, `delivered_at`, `condition` (`ok` or `damaged`) |

Rejected messages get one of these reason codes: `empty_message`, `invalid_json`, `schema_violation`, `unsupported_event_type` or `future_timestamp`.

**Schema evolution.** `schema_version` is part of the envelope, so a version 2 can be added as new union members while version 1 stays readable. Producers would then be upgraded only after consumers.

## Topics and partitioning

| Topic | Partitions | Retention | Key |
|---|---|---|---|
| `manufacturing.events` | 6 (`STREAM_TOPIC_PARTITIONS`) | 7 days | `plant_id` for production and quality, `shipment_id` for shipments |
| `manufacturing.events.dlq` | 1 | 30 days (longer, for inspection and replay) | `source-topic:partition:offset` |

- **Partition keys and ordering.** Kafka only orders messages within a partition.
  - Keying shipments by `shipment_id` keeps a shipment's dispatch and delivery in order during normal operation.
  - The store does not rely on that order (see [reliability](reliability.md)).
  - Keying plant events by `plant_id` keeps each plant's stream on one partition.
  - The trade-off: with only three plants in the demo data, plant events use at most three partitions.
- **Auto-creation is disabled** in the compose broker, so topics always have the intended partition count and retention.
- **Producer settings.**
  - `enable.idempotence=true` and `acks=all` prevent broker-side duplicates from producer retries.
  - `linger.ms=20` and lz4 compression batch messages efficiently.
  - When the local queue is full (`BufferError`), the producer polls and retries instead of dropping.

## Database model

| Table | Grain | Key | Purpose |
|---|---|---|---|
| `processed_events` | one row per accepted event | `event_id` | de-duplication point, ingestion audit, latency |
| `production_minute` | plant × product × minute (event time) | composite PK | units, scrap, event count |
| `quality_minute` | plant × product × minute (event time) | composite PK | samples, defects, inspections |
| `shipments` | one row per shipment | `shipment_id` | current state: dispatch columns and delivery columns |
| `dead_letters` | one row per poison message | unique `(topic, partition, offset)` | reason, detail, original payload |

- **Aggregation grain.** Rollups are per minute of **event time**, so a late or replayed event lands in the minute it happened, not the minute it arrived.
- **Additive upserts.** Rollup upserts add to the existing row (`units = units + EXCLUDED.units`). That is safe only because they run for newly inserted event ids.
- **Schema setup.** The schema is idempotent (`CREATE ... IF NOT EXISTS`) and applied under a transaction-scoped advisory lock, so several consumers can start at once.

## Analytics API

The API reads only the rollups and `processed_events`, never Kafka.
- **Windows.** Production and quality windows use event time; pipeline health uses ingestion time.
- **Startup.** The connection pool opens lazily, so the API starts while PostgreSQL is down and `/health/ready` returns 503 until the database is reachable.
- **Metrics labels.** Request metrics use the route template (`/api/v1/production/summary`), never the raw URL, so query strings cannot inflate metric cardinality.

## Design decisions and alternatives

| Decision | Alternative | Why this choice |
|---|---|---|
| Idempotent writes keyed by `event_id` | Kafka transactions (exactly-once) | The sink is PostgreSQL, not Kafka, so Kafka transactions cannot cover the database write. De-duplicating in the same database transaction as the rollups is simpler and holds across producer retries, replays and duplicate publishes. |
| One transaction per batch | One per message | Fewer round trips. A failure rolls back the whole batch, which is then redelivered and de-duplicated. |
| Synchronous offset commit after the database commit | Auto-commit | Auto-commit can commit offsets for messages that were never persisted (data loss). |
| DLQ topic plus table | Drop or log poison messages | Nothing is silently discarded. The table makes dead letters queryable, and the topic allows replay. |
| `cooperative-sticky` assignment | Eager rebalancing | Members keep unaffected partitions during a rebalance. |
| Per-minute rollups in PostgreSQL | A stream processor (Flink, Kafka Streams) | Enough for this volume and much simpler to operate. The [operations](operations.md#scaling-limits) page notes where this stops scaling. |
