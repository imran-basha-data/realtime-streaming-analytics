# Operations

## Configuration

All settings are environment variables with the `STREAM_` prefix (`src/streaming/config.py`). Invalid values fail at start-up.

| Variable | Default | Notes |
|---|---|---|
| `STREAM_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka brokers |
| `STREAM_EVENTS_TOPIC` / `STREAM_DLQ_TOPIC` | `manufacturing.events` / `.dlq` | must differ |
| `STREAM_TOPIC_PARTITIONS` / `STREAM_TOPIC_REPLICATION` | `6` / `1` | used by `stream topics`; use replication 3 on a real cluster |
| `STREAM_CONSUMER_GROUP` | `analytics-consumer` | |
| `STREAM_MAX_BATCH_SIZE` | `500` | messages per database transaction |
| `STREAM_DB_MAX_RETRIES` / `STREAM_DB_RETRY_BASE_DELAY_S` | `5` / `0.5` | exponential backoff, capped at 30 s per wait |
| `STREAM_MAX_CLOCK_SKEW_S` | `300` | events further in the future are dead-lettered |
| `STREAM_DATABASE_URL` | local dev URL | `postgresql://` only; supply via secret, never commit |
| `STREAM_METRICS_PORT` | `9108` | consumer `/metrics` |
| `STREAM_API_HOST` / `STREAM_API_PORT` / `STREAM_API_POOL_SIZE` | `127.0.0.1` / `8000` / `10` | containers set the host to `0.0.0.0` |
| `STREAM_LOG_LEVEL` / `STREAM_LOG_FORMAT` | `INFO` / `json` | one JSON object per line |

Secrets: `.env` is git-ignored, and `.env.example` lists every variable with placeholder values. Compose refuses to start without `POSTGRES_PASSWORD` and `GRAFANA_ADMIN_PASSWORD`.

## Deployment (compose)

```bash
cp .env.example .env && $EDITOR .env
docker compose up -d --build --wait
```

Start-up order is enforced by compose dependencies:
1. Kafka and PostgreSQL become healthy.
2. The `topics` and `migrate` jobs run once.
3. The consumer and API start.
4. Prometheus and Grafana start alongside.

Container hardening:
- application containers run as uid 10001 with a read-only root filesystem;
- all Linux capabilities are dropped and `no-new-privileges` is set;
- every published port is bound to `127.0.0.1`.

**Scaling consumers.** Run more consumer replicas with the same `STREAM_CONSUMER_GROUP`, for example `docker compose up -d --scale consumer=3`.
- Prometheus's static target `consumer:9108` resolves to a single replica; switch to `dns_sd_configs` to scrape every replica.
- Useful parallelism is capped by the partition count (6).

## Monitoring

Prometheus scrapes the consumer (`consumer:9108`) and the API (`api:8000/metrics`) every 15 s.

| Metric | Type | Meaning |
|---|---|---|
| `stream_events_total{event_type,outcome}` | counter | events processed or detected as duplicates |
| `stream_dead_letters_total{reason}` | counter | messages sent to the DLQ |
| `stream_batch_duration_seconds` | histogram | validate + persist + commit time per batch |
| `stream_event_latency_seconds` | histogram | event `occurred_at` to database commit |
| `stream_consumer_lag_messages{partition}` | gauge | high watermark minus consumer position |
| `stream_db_retries_total` | counter | retried database transactions |
| `stream_batches_total` | counter | committed batches |
| `api_requests_total{route,method,status}` | counter | API requests by route template |
| `api_request_duration_seconds{route}` | histogram | API latency |

Grafana provisions the **Stream analytics pipeline** dashboard (folder *Streaming*) read-only from `monitoring/grafana/dashboards/`. Its panels are: consumer up, total lag, dead-letter ratio, p95 latency, events per second by type, duplicates, dead letters by reason, lag per partition, p50/p95 latency, batch duration, database retries, and API rate and latency.

## Alert runbook

Rules are in `monitoring/prometheus/alerts.yml` and unit-tested in `alerts.test.yml` (`promtool test rules`).

| Alert | Fires when | First checks |
|---|---|---|
| `StreamConsumerDown` (critical) | consumer not scrapeable for 2 m | `docker compose ps consumer`; logs for `db_retries_exhausted` or a fatal Kafka error. A crash-looping consumer is safe (offsets are not committed) but analytics are going stale. |
| `StreamConsumerStalled` (critical) | lag > 0 and no batch committed in 10 m, for 5 m | Consumer alive but stuck: database locks (`pg_stat_activity`), a long rebalance, or `max.poll.interval.ms` evictions in the logs. |
| `StreamConsumerLagHigh` (warning) | total lag > 10 000 for 10 m | Compare `stream_batch_duration_seconds` with the producer rate. Scale consumers (up to the partition count) or raise `STREAM_MAX_BATCH_SIZE`. |
| `StreamDeadLetterRatioHigh` (warning) | > 5 % of messages dead-lettered for 10 m | `stream_dead_letters_total` by reason; `SELECT reason, detail, payload FROM dead_letters ORDER BY received_at DESC LIMIT 20`. Usually a producer deploying a new schema or a clock problem (`future_timestamp`). |
| `StreamEventLatencyHigh` (warning) | p95 event-to-commit latency > 30 s for 10 m | Lag (consumer behind), or producers sending old events (backfill or replay). Check `stream:consumer_lag:sum` first. |
| `StreamDatabaseRetries` (warning) | > 5 retries in 10 m | PostgreSQL availability, connection limits, failover. |
| `AnalyticsApiErrors` (warning) | 5xx ratio > 5 % for 10 m | `/health/ready`; 503s mean the database is unreachable from the API. |

**Replaying dead letters.**
1. Fix the producer or schema.
2. Re-publish the DLQ payloads to the events topic.
3. Events that were invalid are processed normally; anything already stored is ignored by `event_id`.

## Scaling limits

These are design limits, not measured breaking points:
- **Partitions.** Parallelism is bounded by the partition count. Increasing it later changes key-to-partition mapping for new messages; that is safe here, because the store does not depend on ordering.
- **Single sink.** Every event is a primary-key insert into one PostgreSQL table. At sustained high volume `processed_events` needs time-based partitioning and a retention job, since today it grows without bound. The next step would be a stream processor (Kafka Streams or Flink) with a columnar store.
- **Hot rows.** Rollup upserts touch one row per plant × product × minute. Very hot keys would contend on that row.
- **Single broker.** The compose stack runs one broker (replication factor 1), so it has no broker fault tolerance. Production needs three or more brokers, `min.insync.replicas=2` and topic replication 3.
- **No authentication.** Kafka listeners are plaintext and unauthenticated, and the API has no auth. That is acceptable only on a loopback-bound demo; production needs TLS/SASL and an API gateway or auth.
