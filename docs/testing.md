# Testing and evidence

## Test suite

70 tests. CI runs them against real Kafka 4.1 (KRaft) and PostgreSQL 16 service containers, with `STREAM_REQUIRE_SERVICES=1`, so a missing service fails the run instead of skipping tests. A separate CI step also fails the build if the JUnit report shows any skipped test.

| File | Tests | Covers |
|---|---|---|
| `tests/unit/test_events.py` | 22 | schema validation, every reason code, clock skew, partition keys, round-trip serialisation |
| `tests/unit/test_producer.py` | 10 | deterministic generator, idempotent producer configuration, delivery accounting and keys, duplicate and malformed injection, backpressure on `BufferError`, rate limiting |
| `tests/unit/test_cli_and_logging.py` | 13 | CLI argument validation, JSON and text log formatting |
| `tests/integration/test_kafka_producer.py` | 2 | real broker: per-entity ordering on one partition, idempotent topic creation |
| `tests/integration/test_store.py` | 7 | rollups, redelivered batch, in-batch duplicates, shipment order independence, dead letters once, rollback |
| `tests/integration/test_consumer.py` | 7 | end to end against ground truth, crash before offset commit, restart, transient and persistent DB failure, consumer group, future timestamps |
| `tests/integration/test_api.py` | 9 | every endpoint with exact figures (pinned clock), validation, metrics labels, DB outage |

**How the consumer tests decide correctness.** Each test publishes a known set of events while recording the ground truth: unique ids, injected duplicates, injected poison messages, total production units and deliveries. The final PostgreSQL state must equal that ground truth exactly.

**How crashes are simulated.** The consumer exposes named checkpoints (`before_db_commit`, `after_db_commit`, `before_offset_commit`). Tests raise there to simulate a crash or a database error at a precise point, which is far more deterministic than killing a process at a random time.

Branch coverage from the full suite is **93 %**. The main uncovered code is the `consume` and `api` branches of `cli.py`, which run in the container smoke test rather than in pytest.

## Static and security checks (CI)

| Check | Tool |
|---|---|
| Lint and format | ruff |
| Types | mypy `--strict` on `src/streaming` |
| Python security patterns | bandit |
| Dependency vulnerabilities | pip-audit, `--strict`, against fully pinned `requirements.txt` |
| Secrets in Git history | gitleaks, full history |
| Container image vulnerabilities | Trivy (fixable HIGH and CRITICAL fail the build) |
| Alert and recording rules | `promtool check config` and `promtool test rules` |
| End to end | `scripts/smoke_test.sh` |

## Observed results

These come from one machine (a shared cloud sandbox) at modest, rate-limited load. They show the system behaving correctly, **not** its capacity. No throughput or latency benchmark has been run.

### Smoke test (compose stack)

```
producer: {"sent": 3090, "duplicates_injected": 62, "malformed_injected": 28, "delivered": 3090, "failed": 0, ...}
database: 3000 unique events, 28 dead letters (duplicates ignored)
api: events=3000 dead_letters=28 p50=513.5ms p95=960.0ms (observed, not a benchmark)
prometheus: all scrape targets up
SMOKE TEST PASSED
```

Event-to-database latency is dominated by batching. The consumer polls up to 500 messages or 1 s, so at 300–500 events/s most events wait about half a second. Committed batches of around 330 events took 30–50 ms each, according to the consumer's own `batch_committed` logs.

### Crash recovery in the compose stack

1. 6,167 messages published, made up of 6,000 unique events, 116 duplicates and 51 malformed messages. The database held exactly 6,000 events and 51 dead letters.
2. A second run of 4,118 messages was published: 4,000 unique events, 76 duplicates and 42 malformed messages.
3. While that run was in progress, the consumer was killed with `SIGKILL` (7,452 events stored at that point), then restarted.
4. Final state: **10,000 events and 93 dead letters**, which is exactly the ground truth. Nothing was lost and nothing was double-counted.

After restart, the consumer's logs showed batches containing duplicates. These were the redelivered, uncommitted messages; they were recognised and ignored.

Processing resumed only once Kafka's 45-second session timeout expired the killed member (see [reliability](reliability.md#graceful-shutdown-and-rebalancing)).

## Known limitations

- **Single-broker topology.** The compose stack and CI test against one broker (replication factor 1), so broker failover is not tested.
- **No load or soak test.** Memory behaviour over hours, and the throughput ceiling, are unknown.
- **Unbounded table growth.** `processed_events` grows without bound; there is no retention job yet.
- **No authentication.** Kafka uses plaintext listeners and the API has no auth, which is acceptable only for the loopback-bound demo.
- **DLQ topic duplicates.** A crash between the DLQ publish and the offset commit can publish a dead letter to the DLQ topic twice. The `dead_letters` table stays unique.
