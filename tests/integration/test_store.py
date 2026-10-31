"""Idempotent, transactional persistence against PostgreSQL."""

from __future__ import annotations

import json
from datetime import timedelta

import psycopg
import pytest

from streaming.events import parse_event
from streaming.store import AcceptedEvent, DeadLetter, apply_batch

from helpers import NOW, delivered, dispatched, production, quality

pytestmark = pytest.mark.integration


def accepted(*docs: dict[str, object]) -> list[AcceptedEvent]:
    out = []
    for offset, doc in enumerate(docs):
        event = parse_event(json.dumps(doc).encode(), now=NOW, max_clock_skew=timedelta(hours=1))
        out.append(AcceptedEvent(event, partition=0, offset=offset))
    return out


def run(db: str, events: list[AcceptedEvent], dead: list[DeadLetter] | None = None):  # type: ignore[no-untyped-def]
    with psycopg.connect(db) as conn, conn.transaction():
        return apply_batch(conn, events, dead or [], NOW + timedelta(seconds=2))


def one(db: str, sql: str) -> tuple[object, ...]:
    with psycopg.connect(db) as conn:
        row = conn.execute(sql).fetchone()  # type: ignore[arg-type]
    assert row is not None
    return tuple(row)


def test_rollups_are_computed_per_minute(db: str) -> None:
    run(
        db,
        accepted(
            production(units=10, scrap_units=1),
            production(units=5, scrap_units=0),
            quality(sample_size=20, defects=2),
        ),
    )
    assert one(db, "SELECT units, scrap_units, events FROM production_minute") == (15, 1, 2)
    assert one(db, "SELECT samples, defects, inspections FROM quality_minute") == (20, 2, 1)
    assert one(db, "SELECT count(*) FROM processed_events") == (3,)


def test_redelivered_batch_is_not_counted_twice(db: str) -> None:
    batch = accepted(production(units=10), quality(), dispatched())
    first = run(db, batch)
    second = run(db, batch)
    assert (len(first.processed), first.duplicates) == (3, 0)
    assert (len(second.processed), second.duplicates) == (0, 3)
    assert one(db, "SELECT units, events FROM production_minute") == (10, 1)
    assert one(db, "SELECT count(*) FROM processed_events") == (3,)


def test_duplicates_inside_one_batch(db: str) -> None:
    doc = production(units=7)
    outcome = run(db, accepted(doc, doc, doc))
    assert (len(outcome.processed), outcome.duplicates) == (1, 2)
    assert one(db, "SELECT units FROM production_minute") == (7,)


@pytest.mark.parametrize("order", ["in_order", "delivery_first"])
def test_shipment_state_is_correct_in_any_order(db: str, order: str) -> None:
    dispatch, delivery = dispatched("SHP-000009"), delivered("SHP-000009", delay_s=45)
    batches = [[dispatch], [delivery]] if order == "in_order" else [[delivery], [dispatch]]
    for docs in batches:
        run(db, accepted(*docs))
    row = one(
        db,
        "SELECT origin_plant_id, carrier, condition, "
        "extract(epoch FROM delivered_at - promised_at)::int FROM shipments",
    )
    assert row == ("PLANT-LDS", "DPD", "ok", 45)


def test_dead_letters_are_recorded_once_per_message(db: str) -> None:
    letter = DeadLetter("events", 2, 99, "invalid_json", "bad", "{oops")
    run(db, [], [letter])
    run(db, [], [letter])  # redelivered poison message
    assert one(db, "SELECT count(*), max(reason) FROM dead_letters") == (1, "invalid_json")


def test_failed_transaction_leaves_nothing_behind(db: str) -> None:
    with pytest.raises(RuntimeError), psycopg.connect(db) as conn, conn.transaction():
        apply_batch(conn, accepted(production(units=10)), [], NOW)
        raise RuntimeError("crash before commit")
    assert one(db, "SELECT count(*) FROM processed_events") == (0,)
    assert one(db, "SELECT count(*) FROM production_minute") == (0,)
