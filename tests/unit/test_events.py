import json
from datetime import timedelta

import pytest

from streaming.events import (
    InvalidEvent,
    ProductionCompletedEvent,
    ShipmentDeliveredEvent,
    parse_event,
    partition_key,
    serialize,
)

from helpers import NOW, delivered, dispatched, envelope, production, quality

SKEW = timedelta(minutes=5)


def parse(doc: object):  # type: ignore[no-untyped-def]
    return parse_event(json.dumps(doc).encode(), now=NOW, max_clock_skew=SKEW)


@pytest.mark.parametrize("builder", [production, quality, dispatched, delivered])
def test_every_event_type_round_trips(builder) -> None:  # type: ignore[no-untyped-def]
    event = parse(builder())
    again = parse_event(serialize(event), now=NOW, max_clock_skew=SKEW)
    assert again == event


def test_discriminated_union_returns_the_right_type() -> None:
    assert isinstance(parse(production()), ProductionCompletedEvent)
    assert isinstance(parse(delivered()), ShipmentDeliveredEvent)


def reason_for(raw: bytes | None) -> str:
    with pytest.raises(InvalidEvent) as info:
        parse_event(raw, now=NOW, max_clock_skew=SKEW)
    return info.value.reason


def test_empty_and_invalid_json() -> None:
    assert reason_for(None) == "empty_message"
    assert reason_for(b"") == "empty_message"
    assert reason_for(b'{"event_id": "abc"') == "invalid_json"
    assert reason_for(b"not json at all") == "invalid_json"


def test_unknown_event_type() -> None:
    doc = envelope("machine.exploded", {"anything": 1})
    assert reason_for(json.dumps(doc).encode()) == "unsupported_event_type"


@pytest.mark.parametrize(
    "doc",
    [
        production(units=0),
        production(units=-5),
        production(plant_id="Birmingham"),
        production(product_id="P1"),
        quality(defects=21),
        dispatched(shipment_id="not-a-shipment"),
        delivered(condition="lost"),
        production(unexpected="field"),
        envelope("production.completed", {"plant_id": "PLANT-BHM"}),
        {**production(), "schema_version": 2},
        {**production(), "event_id": "not-a-uuid"},
        {**production(), "occurred_at": "2026-06-01T12:00:00"},  # naive timestamp
    ],
)
def test_schema_violations_are_rejected(doc: dict[str, object]) -> None:
    assert reason_for(json.dumps(doc).encode()) == "schema_violation"


def test_schema_violation_detail_names_the_field() -> None:
    with pytest.raises(InvalidEvent) as info:
        parse(production(units=-5))
    assert "units" in info.value.detail


def test_future_events_are_rejected_beyond_clock_skew() -> None:
    ok = production()
    ok["occurred_at"] = (NOW + timedelta(minutes=4)).isoformat()
    parse(ok)
    late = production()
    late["occurred_at"] = (NOW + timedelta(minutes=6)).isoformat()
    assert reason_for(json.dumps(late).encode()) == "future_timestamp"


def test_partition_keys_keep_related_events_together() -> None:
    assert partition_key(parse(dispatched("SHP-000042"))) == "SHP-000042"
    assert partition_key(parse(delivered("SHP-000042"))) == "SHP-000042"
    assert partition_key(parse(production(plant_id="PLANT-LDS"))) == "PLANT-LDS"
    assert partition_key(parse(quality())) == "PLANT-MAN"
