"""Analytics API against a real PostgreSQL populated through the store."""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg_pool import ConnectionPool

from streaming.api import create_app
from streaming.config import Settings
from streaming.events import parse_event
from streaming.store import AcceptedEvent, DeadLetter, apply_batch

from helpers import NOW, delivered, dispatched, production, quality

pytestmark = pytest.mark.integration

AS_OF = NOW + timedelta(minutes=2)


def at(doc: dict[str, Any], **delta: float) -> dict[str, Any]:
    doc["occurred_at"] = (NOW + timedelta(**delta)).isoformat()
    return doc


def seed(db: str, docs: list[dict[str, Any]], dead: list[DeadLetter] | None = None) -> None:
    events = [
        AcceptedEvent(
            parse_event(json.dumps(d).encode(), now=AS_OF, max_clock_skew=timedelta(hours=1)),
            partition=0,
            offset=i,
        )
        for i, d in enumerate(docs)
    ]
    with psycopg.connect(db) as conn, conn.transaction():
        apply_batch(conn, events, dead or [], NOW + timedelta(seconds=2))


@pytest.fixture
def client(db: str) -> Iterator[TestClient]:
    pool = ConnectionPool(db, min_size=1, max_size=2, open=True)
    try:
        app = create_app(Settings(database_url=db), clock=lambda: AS_OF, pool=pool)
        with TestClient(app) as test_client:
            yield test_client
    finally:
        pool.close()


def test_empty_database_returns_empty_results(client: TestClient) -> None:
    assert client.get("/api/v1/production/summary").json() == []
    assert client.get("/api/v1/quality/summary").json() == []
    shipments = client.get("/api/v1/shipments/summary").json()
    assert shipments["dispatched"] == 0 and shipments["on_time_rate"] is None
    health = client.get("/api/v1/pipeline/health").json()
    assert health["events_ingested"] == 0 and health["last_ingested_at"] is None


def test_production_summary_per_plant_with_window_and_filter(db: str, client: TestClient) -> None:
    seed(
        db,
        [
            production(plant_id="PLANT-BHM", units=100, scrap_units=5),
            at(production(plant_id="PLANT-BHM", units=50, scrap_units=0), minutes=1),
            production(plant_id="PLANT-MAN", units=40, scrap_units=4),
            at(production(plant_id="PLANT-MAN", units=999, scrap_units=0), hours=-3),  # too old
        ],
    )
    body = client.get("/api/v1/production/summary", params={"window_minutes": 60}).json()
    assert body == [
        {
            "plant_id": "PLANT-BHM",
            "units": 150,
            "scrap_units": 5,
            "scrap_rate": 0.0333,
            "events": 2,
        },
        {"plant_id": "PLANT-MAN", "units": 40, "scrap_units": 4, "scrap_rate": 0.1, "events": 1},
    ]
    wide = client.get("/api/v1/production/summary", params={"window_minutes": 240}).json()
    assert {r["plant_id"]: r["units"] for r in wide} == {"PLANT-BHM": 150, "PLANT-MAN": 1039}

    only = client.get("/api/v1/production/summary", params={"plant_id": "PLANT-MAN"}).json()
    assert [r["plant_id"] for r in only] == ["PLANT-MAN"]


def test_production_timeseries_is_ordered_by_minute(db: str, client: TestClient) -> None:
    seed(
        db,
        [
            at(production(units=10), minutes=1),
            production(units=20, scrap_units=1),
            production(plant_id="PLANT-MAN", units=5, scrap_units=0),
        ],
    )
    points = client.get("/api/v1/production/timeseries").json()
    assert [(p["minute"][11:16], p["units"], p["scrap_units"]) for p in points] == [
        ("12:00", 25, 1),
        ("12:01", 10, 2),
    ]


def test_quality_defect_rate(db: str, client: TestClient) -> None:
    seed(db, [quality(sample_size=50, defects=2), quality(sample_size=50, defects=3)])
    assert client.get("/api/v1/quality/summary").json() == [
        {
            "plant_id": "PLANT-MAN",
            "product_id": "PRD-0002",
            "samples": 100,
            "defects": 5,
            "defect_rate": 0.05,
            "inspections": 2,
        },
    ]


def test_shipment_summary(db: str, client: TestClient) -> None:
    seed(
        db,
        [
            dispatched("SHP-000001"),
            delivered("SHP-000001", delay_s=0),
            dispatched("SHP-000002"),
            delivered("SHP-000002", delay_s=90),
            dispatched("SHP-000003"),
            delivered("SHP-000003", delay_s=0, condition="damaged"),
            dispatched("SHP-000004"),  # still in transit
            delivered("SHP-000005"),  # delivery seen before its dispatch
        ],
    )
    assert client.get("/api/v1/shipments/summary").json() == {
        "window_hours": 24,
        "dispatched": 4,
        "delivered": 3,
        "in_transit": 1,
        "on_time": 2,
        "late": 1,
        "on_time_rate": 0.6667,
        "damaged": 1,
        "avg_late_minutes": 1.5,
        "deliveries_without_dispatch": 1,
    }


def test_pipeline_health(db: str, client: TestClient) -> None:
    # Events occurred 10s..1s before ingestion (NOW + 2s), so latency is 3s..12s.
    seed(
        db,
        [at(production(units=1), seconds=-s) for s in range(1, 11)],
        [
            DeadLetter("events", 0, 900, "invalid_json", "x", "{"),
            DeadLetter("events", 0, 901, "schema_violation", "x", "{}"),
            DeadLetter("events", 1, 902, "invalid_json", "x", "{"),
        ],
    )
    with psycopg.connect(db) as conn:
        conn.execute("UPDATE dead_letters SET received_at = %s", (NOW + timedelta(seconds=2),))
    health = client.get("/api/v1/pipeline/health").json()
    assert health["events_ingested"] == 10
    assert health["dead_letters"] == 3
    assert health["dead_letters_by_reason"] == {"invalid_json": 2, "schema_violation": 1}
    assert health["latency_ms_p50"] == 7500.0
    assert health["latency_ms_max"] == 12000.0
    assert health["seconds_since_last_event"] == 118.0


def test_input_validation(client: TestClient) -> None:
    assert client.get("/api/v1/production/summary", params={"window_minutes": 0}).status_code == 422
    assert (
        client.get("/api/v1/production/summary", params={"window_minutes": 5000}).status_code == 422
    )
    assert (
        client.get("/api/v1/production/summary", params={"plant_id": "x' OR 1=1"}).status_code
        == 422
    )


def test_health_and_metrics(client: TestClient) -> None:
    assert client.get("/health/live").json() == {"status": "ok"}
    assert client.get("/health/ready").json() == {"status": "ready"}
    client.get("/api/v1/production/summary", params={"plant_id": "PLANT-BHM"})
    text = client.get("/metrics").text
    assert (
        'api_requests_total{method="GET",route="/api/v1/production/summary",status="200"} 1.0'
        in text
    )
    assert "PLANT-BHM" not in text  # query strings never become label values


def test_database_outage_returns_503(db: str) -> None:
    dead_pool = ConnectionPool(
        "postgresql://nobody@127.0.0.1:1/none", min_size=0, max_size=1, timeout=1, open=True
    )
    try:
        app = create_app(Settings(database_url=db), clock=lambda: AS_OF, pool=dead_pool)
        with TestClient(app) as client:
            assert client.get("/health/live").status_code == 200  # liveness ignores the DB
            assert client.get("/health/ready").status_code == 503
            response = client.get("/api/v1/production/summary")
            assert response.status_code == 503
            assert response.json() == {"detail": "database unavailable"}
    finally:
        dead_pool.close()
