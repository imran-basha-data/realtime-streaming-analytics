"""Kafka consumer: validate, persist idempotently, dead-letter, then commit.

Processing order for every batch:

1. Validate each message. Poison messages become dead letters; they never
   block the partition.
2. In **one** PostgreSQL transaction: insert event ids (de-duplicating),
   update rollups for new events only, record dead letters.
3. Publish dead letters to the DLQ topic and wait for the broker to confirm.
4. Commit Kafka offsets synchronously.

A crash anywhere before step 4 means Kafka redelivers the batch, and step 2
recognises every event already stored. That gives **at-least-once delivery
with idempotent writes**: each event affects the analytics exactly once, but
it is not Kafka exactly-once semantics.
"""

from __future__ import annotations

import logging
import signal
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
from confluent_kafka import Consumer, KafkaError, Message, Producer, TopicPartition

from streaming.config import Settings
from streaming.db import connect
from streaming.events import InvalidEvent, parse_event
from streaming.metrics import ConsumerMetrics, build_consumer_metrics
from streaming.store import AcceptedEvent, BatchOutcome, DeadLetter, apply_batch

log = logging.getLogger(__name__)

Checkpoint = Callable[[str], None]
Clock = Callable[[], datetime]


class ConsumerStopped(Exception):
    """Raised by tests through a checkpoint to simulate a crash."""


@dataclass
class RunStats:
    batches: int = 0
    processed: int = 0
    duplicates: int = 0
    dead_letters: int = 0
    db_retries: int = 0
    by_reason: dict[str, int] = field(default_factory=dict)


def consumer_config(settings: Settings) -> dict[str, Any]:
    return {
        "bootstrap.servers": settings.bootstrap_servers,
        "group.id": settings.consumer_group,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "partition.assignment.strategy": "cooperative-sticky",
        # Must exceed the worst-case batch time, including database retries.
        "max.poll.interval.ms": 300_000,
        "session.timeout.ms": 45_000,
    }


class StreamConsumer:
    def __init__(
        self,
        settings: Settings,
        *,
        metrics: ConsumerMetrics | None = None,
        clock: Clock = lambda: datetime.now(UTC),
        checkpoint: Checkpoint | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.settings = settings
        self.metrics = metrics or build_consumer_metrics()
        self.clock = clock
        self.checkpoint = checkpoint or (lambda _stage: None)
        self.sleep = sleep
        self.stats = RunStats()
        self._stop = threading.Event()
        self._consumer = Consumer(consumer_config(settings))
        self._dlq = Producer(
            {
                "bootstrap.servers": settings.bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
                "client.id": "dlq-producer",
            }
        )
        self._conn: psycopg.Connection | None = None
        self._owned: set[int] = set()
        self._owned_lock = threading.Lock()

    # ------------------------------------------------------------------ control
    def stop(self, *_args: object) -> None:
        self._stop.set()

    def install_signal_handlers(self) -> None:
        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGINT, self.stop)

    def run(
        self, *, max_batches: int | None = None, idle_timeout_s: float | None = None
    ) -> RunStats:
        """Consume until stopped, ``max_batches`` is reached, or idle for ``idle_timeout_s``."""
        self._consumer.subscribe(
            [self.settings.events_topic],
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
            on_lost=self._on_revoke,
        )
        idle_since = time.monotonic()
        try:
            while not self._stop.is_set():
                messages = self._consumer.consume(
                    self.settings.max_batch_size, timeout=self.settings.poll_timeout_s
                )
                messages = [m for m in messages if not self._is_ignorable_error(m)]
                if not messages:
                    if not self.assigned_partitions():
                        # Still joining the group: waiting for partitions is not idleness.
                        idle_since = time.monotonic()
                    elif (
                        idle_timeout_s is not None
                        and time.monotonic() - idle_since > idle_timeout_s
                    ):
                        break
                    continue
                idle_since = time.monotonic()
                self._process(messages)
                if max_batches is not None and self.stats.batches >= max_batches:
                    break
        finally:
            # Graceful exit: leave the group so partitions are reassigned promptly.
            self._consumer.close()
            self._dlq.flush(10)
            if self._conn is not None:
                self._conn.close()
        return self.stats

    def assigned_partitions(self) -> list[int]:
        """Partitions currently owned by this group member (safe from any thread)."""
        with self._owned_lock:
            return sorted(self._owned)

    def _on_assign(self, _consumer: Consumer, partitions: list[TopicPartition]) -> None:
        # Called during consume(); the client applies the incremental assignment itself.
        with self._owned_lock:
            self._owned.update(tp.partition for tp in partitions)
        log.info(
            "partitions_assigned", extra={"fields": {"partitions": self.assigned_partitions()}}
        )

    def _on_revoke(self, _consumer: Consumer, partitions: list[TopicPartition]) -> None:
        with self._owned_lock:
            self._owned.difference_update(tp.partition for tp in partitions)
        log.info(
            "partitions_revoked",
            extra={"fields": {"partitions": [tp.partition for tp in partitions]}},
        )

    # ---------------------------------------------------------------- internals
    @staticmethod
    def _is_ignorable_error(message: Message) -> bool:
        error = message.error()
        if error is None:
            return False
        if error.code() == KafkaError._PARTITION_EOF:
            return True
        if error.fatal():
            raise RuntimeError(f"fatal Kafka error: {error}")
        log.warning("kafka_error", extra={"fields": {"error": str(error)}})
        return True

    def _process(self, messages: list[Message]) -> None:
        started = time.perf_counter()
        now = self.clock()
        skew = timedelta(seconds=self.settings.max_clock_skew_s)
        accepted: list[AcceptedEvent] = []
        dead: list[DeadLetter] = []
        for msg in messages:
            topic, partition, offset = _coordinates(msg)
            try:
                event = parse_event(msg.value(), now=now, max_clock_skew=skew)
                accepted.append(AcceptedEvent(event, partition, offset))
            except InvalidEvent as exc:
                raw = msg.value()
                dead.append(
                    DeadLetter(
                        topic=topic,
                        partition=partition,
                        offset=offset,
                        reason=exc.reason,
                        detail=exc.detail[:500],
                        payload=raw.decode("utf-8", errors="replace")[:10_000] if raw else None,
                    )
                )

        outcome = self._persist_with_retry(accepted, dead, now)
        self.checkpoint("after_db_commit")

        for letter in dead:
            self._publish_dead_letter(letter)
        if dead:
            remaining = self._dlq.flush(30)
            if remaining:
                raise RuntimeError(f"{remaining} dead letters not acknowledged by Kafka")

        self.checkpoint("before_offset_commit")
        self._consumer.commit(asynchronous=False)
        self._record(outcome, dead, time.perf_counter() - started, now)

    def _persist_with_retry(
        self, accepted: list[AcceptedEvent], dead: list[DeadLetter], now: datetime
    ) -> BatchOutcome:
        attempts = self.settings.db_max_retries
        for attempt in range(1, attempts + 1):
            try:
                conn = self._connection()
                with conn.transaction():
                    self.checkpoint("before_db_commit")
                    return apply_batch(conn, accepted, dead, now)
            except psycopg.OperationalError as exc:
                self._drop_connection()
                self.stats.db_retries += 1
                self.metrics.db_retries.inc()
                if attempt == attempts:
                    log.error("db_retries_exhausted", extra={"fields": {"attempts": attempt}})
                    raise
                delay = min(30.0, self.settings.db_retry_base_delay_s * 2 ** (attempt - 1))
                log.warning(
                    "db_transient_error",
                    extra={"fields": {"attempt": attempt, "delay_s": delay, "error": str(exc)}},
                )
                self.sleep(delay)
        raise AssertionError("unreachable")  # pragma: no cover

    def _connection(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed:
            self._conn = connect(self.settings.database_url)
        return self._conn

    def _drop_connection(self) -> None:
        if self._conn is not None:
            self._conn.close()
        self._conn = None

    def _publish_dead_letter(self, letter: DeadLetter) -> None:
        headers: dict[str, str | bytes | None] = {
            "dlq.reason": letter.reason,
            "dlq.detail": letter.detail,
            "dlq.source.topic": letter.topic,
            "dlq.source.partition": str(letter.partition),
            "dlq.source.offset": str(letter.offset),
        }
        self._dlq.produce(
            self.settings.dlq_topic,
            value=(letter.payload or "").encode("utf-8"),
            key=f"{letter.topic}:{letter.partition}:{letter.offset}".encode(),
            headers=headers,
        )

    def _record(
        self, outcome: BatchOutcome, dead: list[DeadLetter], seconds: float, now: datetime
    ) -> None:
        self.stats.batches += 1
        self.stats.processed += len(outcome.processed)
        self.stats.duplicates += outcome.duplicates
        self.stats.dead_letters += len(dead)
        self.metrics.batches.inc()
        self.metrics.batch_seconds.observe(seconds)
        for item in outcome.processed:
            event = item.event
            self.metrics.events.labels(event.event_type.value, "processed").inc()
            self.metrics.event_latency.observe(max(0.0, (now - event.occurred_at).total_seconds()))
        if outcome.duplicates:
            self.metrics.events.labels("any", "duplicate").inc(outcome.duplicates)
        for letter in dead:
            self.stats.by_reason[letter.reason] = self.stats.by_reason.get(letter.reason, 0) + 1
            self.metrics.dead_letters.labels(letter.reason).inc()
        self._update_lag()
        log.info(
            "batch_committed",
            extra={
                "fields": {
                    "processed": len(outcome.processed),
                    "duplicates": outcome.duplicates,
                    "dead_letters": len(dead),
                    "duration_ms": round(seconds * 1000, 1),
                }
            },
        )

    def _update_lag(self) -> None:
        try:
            assignment = self._consumer.assignment()
            positions = self._consumer.position(assignment)
            for tp in positions:
                if tp.offset < 0:
                    continue
                _low, high = self._consumer.get_watermark_offsets(
                    TopicPartition(tp.topic, tp.partition), timeout=2, cached=False
                )
                self.metrics.lag.labels(str(tp.partition)).set(max(0, high - tp.offset))
        except Exception as exc:  # lag is best-effort observability
            log.debug("lag_update_failed", extra={"fields": {"error": str(exc)}})


def _coordinates(msg: Message) -> tuple[str, int, int]:
    """Topic, partition and offset of a consumed (non-error) message."""
    topic, partition, offset = msg.topic(), msg.partition(), msg.offset()
    if topic is None or partition is None or offset is None:
        raise RuntimeError("consumed message has no topic/partition/offset")
    return topic, partition, offset
