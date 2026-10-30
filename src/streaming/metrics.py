"""Prometheus metrics for the consumer."""

from __future__ import annotations

from dataclasses import dataclass

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram


@dataclass(frozen=True)
class ConsumerMetrics:
    registry: CollectorRegistry
    events: Counter
    dead_letters: Counter
    batch_seconds: Histogram
    event_latency: Histogram
    lag: Gauge
    db_retries: Counter
    batches: Counter


def build_consumer_metrics(registry: CollectorRegistry | None = None) -> ConsumerMetrics:
    registry = registry or CollectorRegistry()
    return ConsumerMetrics(
        registry=registry,
        events=Counter(
            "stream_events_total",
            "Valid events consumed, by type and outcome (processed or duplicate).",
            ["event_type", "outcome"],
            registry=registry,
        ),
        dead_letters=Counter(
            "stream_dead_letters_total",
            "Messages sent to the dead-letter topic, by reason.",
            ["reason"],
            registry=registry,
        ),
        batch_seconds=Histogram(
            "stream_batch_duration_seconds",
            "Time to validate, persist and commit one batch.",
            buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
            registry=registry,
        ),
        event_latency=Histogram(
            "stream_event_latency_seconds",
            "Delay from event occurrence to commit in PostgreSQL.",
            buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 300),
            registry=registry,
        ),
        lag=Gauge(
            "stream_consumer_lag_messages",
            "Messages between the committed position and the partition high watermark.",
            ["partition"],
            registry=registry,
        ),
        db_retries=Counter(
            "stream_db_retries_total",
            "Batch transactions retried after a transient database error.",
            registry=registry,
        ),
        batches=Counter("stream_batches_total", "Batches committed.", registry=registry),
    )
