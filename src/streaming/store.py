"""Transactional, idempotent persistence of a batch of events.

``apply_batch`` must run inside one transaction. It first inserts every event
id into ``processed_events`` with ``ON CONFLICT DO NOTHING``, then updates the
rollups **only for ids that were actually inserted**. A redelivered event is
therefore recorded as a duplicate and never counted twice, and because the
insert and the rollups commit together, a crash can never leave one without
the other.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import psycopg

from streaming.events import (
    Event,
    ProductionCompletedEvent,
    QualityInspectedEvent,
    ShipmentDeliveredEvent,
    ShipmentDispatchedEvent,
)


@dataclass(frozen=True)
class AcceptedEvent:
    event: Event
    partition: int
    offset: int


@dataclass(frozen=True)
class DeadLetter:
    topic: str
    partition: int
    offset: int
    reason: str
    detail: str
    payload: str | None


@dataclass
class BatchOutcome:
    processed: list[AcceptedEvent] = field(default_factory=list)
    duplicates: int = 0
    dead_letters: int = 0


def minute(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0)


def apply_batch(
    conn: psycopg.Connection,
    accepted: list[AcceptedEvent],
    dead_letters: list[DeadLetter],
    now: datetime,
) -> BatchOutcome:
    outcome = BatchOutcome()

    # De-duplicate within the batch first; the database handles earlier batches.
    unique: dict[str, AcceptedEvent] = {}
    for item in accepted:
        key = str(item.event.event_id)
        if key in unique:
            outcome.duplicates += 1
        else:
            unique[key] = item

    inserted_ids: set[str] = set()
    if unique:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO processed_events
                    (event_id, event_type, occurred_at, ingested_at, kafka_partition,
                     kafka_offset, latency_ms)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
                RETURNING event_id::text
                """,
                [
                    (
                        item.event.event_id,
                        item.event.event_type.value,
                        item.event.occurred_at,
                        now,
                        item.partition,
                        item.offset,
                        max(0.0, (now - item.event.occurred_at).total_seconds() * 1000),
                    )
                    for item in unique.values()
                ],
                returning=True,
            )
            while True:
                row = cur.fetchone()
                if row is not None:
                    inserted_ids.add(row[0])
                if not cur.nextset():
                    break

    new_events = [item for key, item in unique.items() if key in inserted_ids]
    outcome.duplicates += len(unique) - len(new_events)
    outcome.processed = new_events

    _apply_rollups(conn, [item.event for item in new_events])

    if dead_letters:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO dead_letters
                    (topic, kafka_partition, kafka_offset, reason, detail, payload)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (topic, kafka_partition, kafka_offset) DO NOTHING
                """,
                [
                    (d.topic, d.partition, d.offset, d.reason, d.detail, d.payload)
                    for d in dead_letters
                ],
            )
        outcome.dead_letters = len(dead_letters)
    return outcome


def _apply_rollups(conn: psycopg.Connection, events: list[Event]) -> None:
    production: dict[tuple[str, str, datetime], list[int]] = defaultdict(lambda: [0, 0, 0])
    quality: dict[tuple[str, str, datetime], list[int]] = defaultdict(lambda: [0, 0, 0])
    dispatches: list[tuple[Any, ...]] = []
    deliveries: list[tuple[Any, ...]] = []

    for event in events:
        if isinstance(event, ProductionCompletedEvent):
            p = event.payload
            agg = production[(p.plant_id, p.product_id, minute(event.occurred_at))]
            agg[0] += p.units
            agg[1] += p.scrap_units
            agg[2] += 1
        elif isinstance(event, QualityInspectedEvent):
            q = event.payload
            agg = quality[(q.plant_id, q.product_id, minute(event.occurred_at))]
            agg[0] += q.sample_size
            agg[1] += q.defects
            agg[2] += 1
        elif isinstance(event, ShipmentDispatchedEvent):
            d = event.payload
            dispatches.append(
                (
                    d.shipment_id,
                    d.origin_plant_id,
                    d.destination,
                    d.carrier,
                    event.occurred_at,
                    d.promised_at,
                )
            )
        elif isinstance(event, ShipmentDeliveredEvent):
            s = event.payload
            deliveries.append((s.shipment_id, s.delivered_at, s.condition))

    with conn.cursor() as cur:
        if production:
            cur.executemany(
                """
                INSERT INTO production_minute AS t
                    (plant_id, product_id, minute, units, scrap_units, events)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (plant_id, product_id, minute) DO UPDATE SET
                    units = t.units + EXCLUDED.units,
                    scrap_units = t.scrap_units + EXCLUDED.scrap_units,
                    events = t.events + EXCLUDED.events
                """,
                [(*key, *vals) for key, vals in production.items()],
            )
        if quality:
            cur.executemany(
                """
                INSERT INTO quality_minute AS t
                    (plant_id, product_id, minute, samples, defects, inspections)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (plant_id, product_id, minute) DO UPDATE SET
                    samples = t.samples + EXCLUDED.samples,
                    defects = t.defects + EXCLUDED.defects,
                    inspections = t.inspections + EXCLUDED.inspections
                """,
                [(*key, *vals) for key, vals in quality.items()],
            )
        if dispatches:
            cur.executemany(
                """
                INSERT INTO shipments AS s
                    (shipment_id, origin_plant_id, destination, carrier, dispatched_at,
                     promised_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (shipment_id) DO UPDATE SET
                    origin_plant_id = EXCLUDED.origin_plant_id,
                    destination = EXCLUDED.destination,
                    carrier = EXCLUDED.carrier,
                    dispatched_at = EXCLUDED.dispatched_at,
                    promised_at = EXCLUDED.promised_at,
                    updated_at = now()
                """,
                dispatches,
            )
        if deliveries:
            cur.executemany(
                """
                INSERT INTO shipments AS s (shipment_id, delivered_at, condition)
                VALUES (%s, %s, %s)
                ON CONFLICT (shipment_id) DO UPDATE SET
                    delivered_at = EXCLUDED.delivered_at,
                    condition = EXCLUDED.condition,
                    updated_at = now()
                """,
                deliveries,
            )
