"""End-to-end consumer behaviour against real Kafka and PostgreSQL.

Every test publishes a known set of events (plus duplicates and poison
messages), runs one or more consumers, and checks PostgreSQL against the
ground truth computed from what was published.
"""

from __future__ import annotations

import json
import random
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import psycopg
import pytest
from confluent_kafka import Consumer

from streaming.config import Settings
from streaming.consumer import ConsumerStopped, StreamConsumer
from streaming.events import ProductionCompletedEvent, ShipmentDeliveredEvent
from streaming.generator import EventGenerator, GeneratorConfig
from streaming.producer import EventProducer, corrupt

pytestmark = pytest.mark.integration


@dataclass
class Published:
    unique_ids: set[str]
    duplicates: int
    poison: int
    production_units: int
    deliveries: int


def publish(
    bootstrap: str,
    topic: str,
    count: int,
    *,
    dup_rate: float = 0.05,
    poison_rate: float = 0.03,
    seed: int = 21,
) -> Published:
    start = datetime.now(UTC) - timedelta(minutes=5)
    ticks = iter(range(10**6))
    gen = EventGenerator(
        GeneratorConfig(seed=seed), clock=lambda: start + timedelta(milliseconds=50 * next(ticks))
    )
    rng = random.Random(seed)
    producer = EventProducer.connect(bootstrap, topic)
    truth = Published(set(), 0, 0, 0, 0)
    for _ in range(count):
        event = gen.next_event()
        value = producer.send(event)
        truth.unique_ids.add(str(event.event_id))
        if isinstance(event, ProductionCompletedEvent):
            truth.production_units += event.payload.units
        if isinstance(event, ShipmentDeliveredEvent):
            truth.deliveries += 1
        if rng.random() < dup_rate:
            producer.send_raw("dup", value)
            truth.duplicates += 1
        if rng.random() < poison_rate:
            producer.send_raw("poison", corrupt(value, rng))
            truth.poison += 1
    assert producer.flush(30) == 0
    return truth


def settings_for(bootstrap: str, topic: str, db: str, group: str | None = None) -> Settings:
    return Settings(
        bootstrap_servers=bootstrap,
        events_topic=topic,
        dlq_topic=f"{topic}.dlq",
        consumer_group=group or f"group-{uuid.uuid4().hex[:8]}",
        database_url=db,
        max_batch_size=100,
        poll_timeout_s=0.5,
        db_retry_base_delay_s=0.0,
    )


def db_state(db: str) -> dict[str, object]:
    with psycopg.connect(db) as conn:

        def val(sql: str) -> object:
            row = conn.execute(sql).fetchone()  # type: ignore[arg-type]
            return row[0] if row else None

        return {
            "events": val("SELECT count(*) FROM processed_events"),
            "units": val("SELECT coalesce(sum(units), 0) FROM production_minute"),
            "deliveries": val("SELECT count(*) FROM shipments WHERE delivered_at IS NOT NULL"),
            "dead_letters": val("SELECT count(*) FROM dead_letters"),
        }


def expected(truth: Published) -> dict[str, object]:
    return {
        "events": len(truth.unique_ids),
        "units": truth.production_units,
        "deliveries": truth.deliveries,
        "dead_letters": truth.poison,
    }


def dlq_messages(bootstrap: str, dlq: str, expected_count: int) -> list[dict[str, str]]:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"dlq-{uuid.uuid4().hex}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([dlq])
    headers: list[dict[str, str]] = []
    try:
        for _ in range(30):
            for msg in consumer.consume(100, timeout=1.0):
                if msg.error() is None:
                    headers.append({k: v.decode() for k, v in (msg.headers() or [])})
            if len(headers) >= expected_count:
                break
    finally:
        consumer.close()
    return headers


def test_end_to_end_counts_match_ground_truth(bootstrap_servers: str, topic: str, db: str) -> None:
    truth = publish(bootstrap_servers, topic, 600)
    assert truth.duplicates > 0 and truth.poison > 0

    consumer = StreamConsumer(settings_for(bootstrap_servers, topic, db))
    stats = consumer.run(idle_timeout_s=4)

    assert db_state(db) == expected(truth)
    assert stats.processed == len(truth.unique_ids)
    assert stats.duplicates == truth.duplicates
    assert stats.dead_letters == truth.poison
    assert set(stats.by_reason) <= {"invalid_json", "schema_violation"}

    letters = dlq_messages(bootstrap_servers, f"{topic}.dlq", truth.poison)
    assert len(letters) == truth.poison
    assert all(h["dlq.source.topic"] == topic and h["dlq.reason"] for h in letters)

    text = consumer.metrics.registry.get_sample_value(
        "stream_events_total", {"event_type": "production.completed", "outcome": "processed"}
    )
    assert text and text > 0


def test_crash_before_offset_commit_redelivers_without_double_counting(
    bootstrap_servers: str, topic: str, db: str
) -> None:
    truth = publish(bootstrap_servers, topic, 400)
    settings = settings_for(bootstrap_servers, topic, db)

    def crash(stage: str) -> None:
        if stage == "before_offset_commit":
            raise ConsumerStopped("process killed after the DB commit, before the offset commit")

    with pytest.raises(ConsumerStopped):
        StreamConsumer(settings, checkpoint=crash).run(idle_timeout_s=4)
    committed_before = db_state(db)["events"]
    assert committed_before and committed_before > 0  # type: ignore[operator]

    recovered = StreamConsumer(settings).run(idle_timeout_s=4)

    assert db_state(db) == expected(truth)
    assert recovered.duplicates >= committed_before  # type: ignore[operator]


def test_restart_resumes_from_committed_offsets(
    bootstrap_servers: str, topic: str, db: str
) -> None:
    truth = publish(bootstrap_servers, topic, 500, dup_rate=0.0)
    settings = settings_for(bootstrap_servers, topic, db)

    first = StreamConsumer(settings).run(max_batches=2)
    assert first.batches == 2
    second = StreamConsumer(settings).run(idle_timeout_s=4)

    assert db_state(db) == expected(truth)
    assert first.processed + second.processed == len(truth.unique_ids)
    assert second.duplicates == 0  # committed batches are not redelivered


def test_transient_database_errors_are_retried(bootstrap_servers: str, topic: str, db: str) -> None:
    truth = publish(bootstrap_servers, topic, 200)
    failures = {"left": 2}

    def flaky(stage: str) -> None:
        if stage == "before_db_commit" and failures["left"]:
            failures["left"] -= 1
            raise psycopg.OperationalError("server closed the connection unexpectedly")

    stats = StreamConsumer(settings_for(bootstrap_servers, topic, db), checkpoint=flaky).run(
        idle_timeout_s=4
    )
    assert stats.db_retries == 2
    assert db_state(db) == expected(truth)


def test_database_outage_stops_consumer_without_losing_events(
    bootstrap_servers: str, topic: str, db: str
) -> None:
    truth = publish(bootstrap_servers, topic, 200)
    settings = settings_for(bootstrap_servers, topic, db)

    def down(stage: str) -> None:
        if stage == "before_db_commit":
            raise psycopg.OperationalError("connection refused")

    with pytest.raises(psycopg.OperationalError):
        StreamConsumer(settings, checkpoint=down).run(idle_timeout_s=4)
    assert db_state(db)["events"] == 0

    StreamConsumer(settings).run(idle_timeout_s=4)  # database is back
    assert db_state(db) == expected(truth)


def test_two_consumers_in_one_group_share_partitions(
    bootstrap_servers: str, topic: str, db: str
) -> None:
    group = f"group-{uuid.uuid4().hex[:8]}"
    consumers = [
        StreamConsumer(settings_for(bootstrap_servers, topic, db, group)) for _ in range(2)
    ]
    results: list[int] = [0, 0]
    errors: list[BaseException] = []

    def work(i: int) -> None:
        try:
            results[i] = consumers[i].run().processed  # runs until stop()
        except BaseException as exc:  # surface thread failures in the test
            errors.append(exc)

    threads = [threading.Thread(target=work, args=(i,), daemon=True) for i in range(2)]
    for t in threads:
        t.start()
    try:
        # Publish only once both members own partitions, so the work is really shared.
        deadline = time.monotonic() + 60
        while not all(c.assigned_partitions() for c in consumers):
            assert not errors, errors
            assert time.monotonic() < deadline, "consumer group did not settle"
            time.sleep(0.2)
        owned = [c.assigned_partitions() for c in consumers]
        truth = publish(bootstrap_servers, topic, 900, dup_rate=0.0, poison_rate=0.0)

        deadline = time.monotonic() + 60
        while db_state(db)["events"] != len(truth.unique_ids) and time.monotonic() < deadline:
            time.sleep(0.5)
    finally:
        for c in consumers:
            c.stop()
        for t in threads:
            t.join(timeout=60)
    assert not errors, errors
    assert not any(t.is_alive() for t in threads)

    assert db_state(db) == expected(truth)
    assert sorted(owned[0] + owned[1]) == [0, 1, 2]  # partitions split, none shared
    assert sum(results) >= len(truth.unique_ids)  # rebalances may redeliver, never lose
    assert all(r > 0 for r in results), results


def test_future_timestamp_is_dead_lettered(bootstrap_servers: str, topic: str, db: str) -> None:
    producer = EventProducer.connect(bootstrap_servers, topic)
    future = datetime.now(UTC) + timedelta(hours=2)
    event = EventGenerator(GeneratorConfig(seed=1), clock=lambda: future).next_event()
    producer.send(event)
    producer.flush(10)

    stats = StreamConsumer(settings_for(bootstrap_servers, topic, db)).run(idle_timeout_s=3)
    assert stats.by_reason == {"future_timestamp": 1}
    with psycopg.connect(db) as conn:
        row = conn.execute("SELECT reason, payload FROM dead_letters").fetchone()
    assert row is not None and row[0] == "future_timestamp"
    assert json.loads(row[1])["event_id"] == str(event.event_id)
