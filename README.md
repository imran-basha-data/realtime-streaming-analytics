# realtime-streaming-analytics

Event-driven pipeline for manufacturing and logistics events: Python producers publish to Apache Kafka, and a consumer validates, de-duplicates and aggregates them into PostgreSQL for near-real-time analytics.

> **Status:** event schemas, the synthetic event generator and the idempotent Kafka producer are in place. The consumer, PostgreSQL models, analytics API and full documentation follow in later pull requests.

## Events (schema version 1)

| `event_type` | Key (partition) | Payload |
|---|---|---|
| `production.completed` | `plant_id` | plant, line, product, units, scrap units |
| `quality.inspected` | `plant_id` | plant, product, sample size, defects |
| `shipment.dispatched` | `shipment_id` | origin plant, destination, carrier, promised time |
| `shipment.delivered` | `shipment_id` | delivered time, condition |

Every event carries `event_id` (UUID), `schema_version`, a timezone-aware `occurred_at` and a `source`. Shipment events are keyed by shipment, so a shipment's dispatch and delivery always land on the same partition and are read in order.

## Run locally

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:4.1.0
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt && pip install --no-deps -e .
stream topics
stream produce --count 1000 --rate 200 --duplicate-rate 0.02 --malformed-rate 0.01
```

## Tests

```bash
STREAM_TEST_BOOTSTRAP_SERVERS=localhost:9092 pytest
```

## Licence

[MIT](LICENSE)
