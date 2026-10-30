"""Producer against a real Kafka broker."""

from __future__ import annotations

import json
from collections import defaultdict

import pytest
from confluent_kafka import Consumer

from streaming.config import Settings
from streaming.generator import EventGenerator, GeneratorConfig
from streaming.producer import EventProducer, run_producer
from streaming.topics import ensure_topics

pytestmark = pytest.mark.integration


def drain(bootstrap: str, topic: str, expected: int) -> list[tuple[int, bytes, bytes]]:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"drain-{topic}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    out: list[tuple[int, bytes, bytes]] = []
    try:
        for _ in range(60):
            for msg in consumer.consume(500, timeout=1.0):
                if msg.error():
                    continue
                out.append((msg.partition(), msg.key(), msg.value()))
            if len(out) >= expected:
                break
    finally:
        consumer.close()
    return out


def test_events_reach_kafka_with_per_entity_ordering(bootstrap_servers: str, topic: str) -> None:
    producer = EventProducer.connect(bootstrap_servers, topic)
    stats = run_producer(
        producer, EventGenerator(GeneratorConfig(seed=9)), count=400, rate_per_s=10_000
    )
    assert stats.delivered == 400 and stats.failed == 0

    messages = drain(bootstrap_servers, topic, 400)
    assert len(messages) == 400
    partitions_by_key: dict[bytes, set[int]] = defaultdict(set)
    for partition, key, _value in messages:
        partitions_by_key[key].add(partition)
    # The same key always lands on the same partition, preserving per-entity order.
    assert all(len(parts) == 1 for parts in partitions_by_key.values())
    assert len({p for p, _k, _v in messages}) > 1  # load is spread over partitions

    # Within a partition, a shipment's delivery never precedes its dispatch.
    seen_dispatch: set[str] = set()
    for _partition, _key, value in messages:
        doc = json.loads(value)
        if doc["event_type"] == "shipment.dispatched":
            seen_dispatch.add(doc["payload"]["shipment_id"])
        elif doc["event_type"] == "shipment.delivered":
            assert doc["payload"]["shipment_id"] in seen_dispatch


def test_ensure_topics_is_idempotent(bootstrap_servers: str, topic: str) -> None:
    settings = Settings(
        bootstrap_servers=bootstrap_servers,
        events_topic=topic,
        dlq_topic=f"{topic}.dlq",
        topic_partitions=3,
    )
    assert ensure_topics(settings) == [f"{topic}.dlq"]  # events topic already exists
    assert ensure_topics(settings) == []
