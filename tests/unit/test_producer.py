import json
import random
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from streaming.events import (
    EventType,
    InvalidEvent,
    ShipmentDeliveredEvent,
    ShipmentDispatchedEvent,
    parse_event,
)
from streaming.generator import EventGenerator, GeneratorConfig
from streaming.producer import EventProducer, corrupt, producer_config, run_producer

T0 = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)


class FakeClock:
    def __init__(self) -> None:
        self.now = T0

    def __call__(self) -> datetime:
        self.now += timedelta(milliseconds=100)
        return self.now


class FakeKafka:
    """Records produce() calls; can simulate a full local queue."""

    def __init__(self, buffer_full_times: int = 0) -> None:
        self.messages: list[tuple[str, bytes, bytes]] = []
        self.callbacks: list[Any] = []
        self.buffer_full_times = buffer_full_times
        self.polls = 0

    def produce(self, topic: str, value: bytes, key: bytes, on_delivery: Any) -> None:
        if self.buffer_full_times:
            self.buffer_full_times -= 1
            raise BufferError("Local: Queue full")
        self.messages.append((topic, key, value))
        self.callbacks.append(on_delivery)

    def poll(self, timeout: float) -> int:
        self.polls += 1
        served = len(self.callbacks)
        for callback in self.callbacks:
            callback(None, None)
        self.callbacks.clear()
        return served

    def flush(self, timeout: float) -> int:
        self.poll(timeout)
        return 0


def generator(seed: int = 3) -> EventGenerator:
    return EventGenerator(GeneratorConfig(seed=seed), clock=FakeClock())


def test_generator_is_deterministic_for_a_seed() -> None:
    a = [generator(5).next_event().model_dump_json() for _ in range(1)]
    first = generator(5)
    second = generator(5)
    seq_a = [first.next_event().model_dump_json() for _ in range(50)]
    seq_b = [second.next_event().model_dump_json() for _ in range(50)]
    assert seq_a == seq_b
    assert a[0] == seq_a[0]


def test_generator_emits_every_type_and_valid_events() -> None:
    gen = generator()
    events = [gen.next_event() for _ in range(400)]
    assert {e.event_type for e in events} == set(EventType)
    for event in events:  # every generated event passes validation
        parse_event(
            event.model_dump_json().encode(),
            now=T0 + timedelta(hours=1),
            max_clock_skew=timedelta(0),
        )


def test_deliveries_only_follow_their_dispatch() -> None:
    gen = generator()
    dispatched: set[str] = set()
    for _ in range(500):
        event = gen.next_event()
        if isinstance(event, ShipmentDispatchedEvent):
            dispatched.add(event.payload.shipment_id)
        elif isinstance(event, ShipmentDeliveredEvent):
            assert event.payload.shipment_id in dispatched
            dispatched.discard(event.payload.shipment_id)


def test_idempotent_producer_configuration() -> None:
    config = producer_config("broker:9092")
    assert config["enable.idempotence"] is True
    assert config["acks"] == "all"


def test_run_producer_counts_and_keys() -> None:
    kafka = FakeKafka()
    producer = EventProducer("events", kafka)
    stats = run_producer(producer, generator(), count=100, rate_per_s=1e9, sleep=lambda _s: None)
    assert stats.sent == stats.delivered == 100
    assert stats.failed == 0
    for _topic, key, value in kafka.messages:
        doc = json.loads(value)
        payload = doc["payload"]
        expected = payload.get("shipment_id") or payload.get("plant_id")
        assert key.decode() == expected


def test_duplicates_and_poison_messages_are_injected_on_request() -> None:
    kafka = FakeKafka()
    stats = run_producer(
        EventProducer("events", kafka),
        generator(),
        count=300,
        rate_per_s=1e9,
        duplicate_rate=0.1,
        malformed_rate=0.1,
        sleep=lambda _s: None,
    )
    assert stats.duplicates_injected > 0 and stats.malformed_injected > 0
    assert stats.sent == 300 + stats.duplicates_injected + stats.malformed_injected
    rejected = 0
    for _topic, _key, value in kafka.messages:
        try:
            parse_event(value, now=T0 + timedelta(hours=1), max_clock_skew=timedelta(0))
        except InvalidEvent:
            rejected += 1
    assert rejected == stats.malformed_injected


def test_corrupt_always_produces_an_invalid_message() -> None:
    gen = generator()
    rng = random.Random(1)
    for _ in range(200):
        value = gen.next_event().model_dump_json().encode()
        with pytest.raises(InvalidEvent):
            parse_event(
                corrupt(value, rng), now=T0 + timedelta(hours=1), max_clock_skew=timedelta(0)
            )


def test_backpressure_waits_when_local_queue_is_full() -> None:
    kafka = FakeKafka(buffer_full_times=3)
    producer = EventProducer("events", kafka)
    producer.send_raw("k", b"v")
    assert producer.stats.backpressure_waits == 3
    assert len(kafka.messages) == 1


def test_rate_limiting_paces_sends() -> None:
    sleeps: list[float] = []
    clock = {"t": 0.0}
    run_producer(
        EventProducer("events", FakeKafka()),
        generator(),
        count=10,
        rate_per_s=10,
        sleep=sleeps.append,
        monotonic=lambda: clock["t"],
    )
    # With a frozen clock every send is ahead of schedule, by 0.1 s increments.
    assert sleeps == pytest.approx([0.1 * n for n in range(1, 11)])


def test_invalid_rate_is_rejected() -> None:
    with pytest.raises(ValueError):
        run_producer(EventProducer("events", FakeKafka()), generator(), count=1, rate_per_s=0)
