"""Kafka producer for the event stream.

The producer is idempotent (``enable.idempotence=true``, ``acks=all``): broker
retries cannot create duplicates or reorder messages within a partition. It
still runs at-least-once end to end, because the application itself may
re-send after a timeout, so consumers must de-duplicate by ``event_id``.
"""

from __future__ import annotations

import json
import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from confluent_kafka import KafkaError, Message, Producer

from streaming.events import Event, partition_key, serialize
from streaming.generator import EventGenerator

log = logging.getLogger(__name__)


class KafkaProducerLike(Protocol):
    def produce(self, topic: str, *args: Any, **kwargs: Any) -> None: ...
    def poll(self, timeout: float) -> int: ...
    def flush(self, timeout: float) -> int: ...


def producer_config(bootstrap_servers: str) -> dict[str, Any]:
    return {
        "bootstrap.servers": bootstrap_servers,
        "client.id": "event-producer",
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 20,
        "compression.type": "lz4",
        # Bounded local buffer: when it fills, produce() raises BufferError and
        # the send loop waits for deliveries (producer-side backpressure).
        "queue.buffering.max.messages": 100_000,
    }


@dataclass
class ProducerStats:
    sent: int = 0
    duplicates_injected: int = 0
    malformed_injected: int = 0
    delivered: int = 0
    failed: int = 0
    backpressure_waits: int = 0

    def as_dict(self) -> dict[str, int]:
        return dict(self.__dict__)


class EventProducer:
    def __init__(
        self,
        topic: str,
        producer: KafkaProducerLike,
    ) -> None:
        self.topic = topic
        self._producer = producer
        self.stats = ProducerStats()

    @classmethod
    def connect(cls, bootstrap_servers: str, topic: str) -> EventProducer:
        return cls(topic, Producer(producer_config(bootstrap_servers)))

    def _on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            self.stats.failed += 1
            log.error("delivery_failed", extra={"fields": {"error": str(err)}})
        else:
            self.stats.delivered += 1

    def send_raw(self, key: str, value: bytes) -> None:
        while True:
            try:
                self._producer.produce(
                    self.topic, value=value, key=key.encode(), on_delivery=self._on_delivery
                )
                break
            except BufferError:
                self.stats.backpressure_waits += 1
                self._producer.poll(0.5)
        self.stats.sent += 1
        self._producer.poll(0)

    def send(self, event: Event) -> bytes:
        value = serialize(event)
        self.send_raw(partition_key(event), value)
        return value

    def flush(self, timeout: float = 30.0) -> int:
        return self._producer.flush(timeout)


def corrupt(value: bytes, rng: random.Random) -> bytes:
    """Make a poison message: invalid JSON, or valid JSON that breaks the schema."""
    if rng.random() < 0.5:
        return value[: max(1, len(value) // 2)]
    doc = json.loads(value)
    if "units" in doc["payload"]:
        doc["payload"]["units"] = -5
    elif "sample_size" in doc["payload"]:
        doc["payload"]["defects"] = doc["payload"]["sample_size"] + 1
    else:
        doc["payload"]["shipment_id"] = "not-a-shipment"
    return json.dumps(doc).encode()


def run_producer(
    producer: EventProducer,
    generator: EventGenerator,
    *,
    count: int,
    rate_per_s: float,
    duplicate_rate: float = 0.0,
    malformed_rate: float = 0.0,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
) -> ProducerStats:
    """Produce ``count`` generated events at roughly ``rate_per_s`` per second.

    Optionally re-sends some events (duplicates) and sends some corrupted
    copies (poison messages), so consumers can be shown to handle both.
    """
    if rate_per_s <= 0:
        raise ValueError("rate_per_s must be positive")
    rng = generator.rng
    interval = 1.0 / rate_per_s
    next_at = monotonic()
    for _ in range(count):
        event = generator.next_event()
        value = producer.send(event)
        key = partition_key(event)
        if malformed_rate and rng.random() < malformed_rate:
            producer.send_raw(key, corrupt(value, rng))
            producer.stats.malformed_injected += 1
        if duplicate_rate and rng.random() < duplicate_rate:
            producer.send_raw(key, value)
            producer.stats.duplicates_injected += 1
        next_at += interval
        delay = next_at - monotonic()
        if delay > 0:
            sleep(delay)
    remaining = producer.flush()
    if remaining:
        log.error("flush_incomplete", extra={"fields": {"remaining": remaining}})
    return producer.stats
