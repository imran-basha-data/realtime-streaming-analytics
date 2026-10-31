# realtime-streaming-analytics

[![CI](https://github.com/imran-basha-data/realtime-streaming-analytics/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/imran-basha-data/realtime-streaming-analytics/actions/workflows/ci.yml)

Event-driven pipeline for manufacturing and logistics events. Python producers publish to Apache Kafka; a consumer validates, de-duplicates and aggregates the events into PostgreSQL; a read-only API serves near-real-time analytics; Prometheus and Grafana monitor the whole flow.

```
 producers ──► Kafka: manufacturing.events (6 partitions)
                 │
                 ▼
             consumer ──► PostgreSQL (one transaction per batch) ──► analytics API
                 │          processed_events · rollups · dead_letters
                 └──► Kafka: manufacturing.events.dlq
 Prometheus ◄── /metrics (consumer, API) ──► Grafana
```

## What it demonstrates

- **Schema-validated events.** Four event types are defined as a Pydantic v2 discriminated union. Invalid messages are dead-lettered with a reason code and never block a partition.
- **At-least-once delivery with idempotent writes.**
  - Offsets are committed only after the PostgreSQL transaction and the DLQ publish succeed.
  - Redelivered events are recognised by `event_id` and never counted twice.
  - This is tested by crashing the consumer between the database commit and the offset commit.
  - Kafka exactly-once semantics are not claimed (see [docs/reliability.md](docs/reliability.md)).
- **Order-independent state.** A shipment's delivery may arrive before its dispatch (for example after a replay), and the final row is the same.
- **Failure handling.** Transient database errors are retried with backoff, and persistent errors stop the consumer without committing. A `SIGKILL`ed consumer resumes from its last committed offset.
- **Operations.** A non-root container, a compose stack, Prometheus alerts with unit tests, a Grafana dashboard, and CI that fails if any test is skipped.

## Quick start

```bash
cp .env.example .env                       # set local passwords
docker compose up -d --build --wait        # Kafka, PostgreSQL, consumer, API, Prometheus, Grafana
docker compose --profile demo run --rm producer   # synthetic traffic with duplicates and malformed messages
curl -s localhost:8000/api/v1/pipeline/health | python3 -m json.tool
```

| Service | URL (loopback only) |
|---|---|
| Analytics API (OpenAPI docs) | http://localhost:8000/docs |
| Prometheus | http://localhost:9090 |
| Grafana (admin / `GRAFANA_ADMIN_PASSWORD`) | http://localhost:3000 |

`scripts/smoke_test.sh` runs the whole flow and checks PostgreSQL, the API and Prometheus against the producer's own report. CI runs it on every pull request.

## Events (schema version 1)

| `event_type` | Partition key | Payload |
|---|---|---|
| `production.completed` | `plant_id` | plant, line, product, units, scrap units |
| `quality.inspected` | `plant_id` | plant, product, sample size, defects |
| `shipment.dispatched` | `shipment_id` | origin plant, destination, carrier, promised time |
| `shipment.delivered` | `shipment_id` | delivered time, condition |

Full schemas, topic configuration and the database model are in [docs/architecture.md](docs/architecture.md).

## API

| Endpoint | Returns |
|---|---|
| `GET /api/v1/production/summary?window_minutes=60&plant_id=` | units, scrap, scrap rate per plant |
| `GET /api/v1/production/timeseries` | units and scrap per minute |
| `GET /api/v1/quality/summary` | defect rate per plant and product |
| `GET /api/v1/shipments/summary?window_hours=24` | in transit, on-time rate, damaged, average lateness |
| `GET /api/v1/pipeline/health?window_minutes=15` | throughput, dead letters by reason, p50/p95 latency, freshness |
| `/health/live`, `/health/ready`, `/metrics` | probes and Prometheus metrics |

## Development

```bash
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt && pip install --no-deps -e .
docker run -d --name kafka -p 9092:9092 apache/kafka:4.1.0
docker run -d --name pg -p 5432:5432 -e POSTGRES_USER=rsa -e POSTGRES_PASSWORD=rsa_local_dev -e POSTGRES_DB=rsa_test postgres:16-alpine

export STREAM_TEST_BOOTSTRAP_SERVERS=localhost:9092
export STREAM_TEST_DATABASE_URL=postgresql://rsa:rsa_local_dev@localhost:5432/rsa_test
export STREAM_REQUIRE_SERVICES=1           # fail rather than skip when services are missing
pytest
ruff check . && ruff format --check . && mypy && bandit -r src
```

## Documentation

- [Architecture](docs/architecture.md): components, event schemas, topics, partitioning, database model.
- [Reliability](docs/reliability.md): delivery semantics, failure modes and how each is tested.
- [Operations](docs/operations.md): configuration, deployment, monitoring, alert runbook.
- [Testing and evidence](docs/testing.md): what is tested, and observed results with their limits.

## Licence

[MIT](LICENSE)
