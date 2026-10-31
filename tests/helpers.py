"""Shared builders for tests."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

NOW = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)


def envelope(event_type: str, payload: dict[str, Any], **overrides: Any) -> dict[str, Any]:
    doc: dict[str, Any] = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "schema_version": 1,
        "occurred_at": NOW.isoformat(),
        "source": "test",
        "payload": payload,
    }
    doc.update(overrides)
    return doc


def production(**payload: Any) -> dict[str, Any]:
    body = {
        "plant_id": "PLANT-BHM",
        "line_id": "L01",
        "product_id": "PRD-0001",
        "units": 50,
        "scrap_units": 2,
    }
    body.update(payload)
    return envelope("production.completed", body)


def quality(**payload: Any) -> dict[str, Any]:
    body = {"plant_id": "PLANT-MAN", "product_id": "PRD-0002", "sample_size": 20, "defects": 1}
    body.update(payload)
    return envelope("quality.inspected", body)


def dispatched(shipment_id: str = "SHP-000001", **payload: Any) -> dict[str, Any]:
    body = {
        "shipment_id": shipment_id,
        "origin_plant_id": "PLANT-LDS",
        "destination": "Glasgow DC",
        "carrier": "DPD",
        "promised_at": (NOW + timedelta(seconds=30)).isoformat(),
    }
    body.update(payload)
    return envelope("shipment.dispatched", body)


def delivered(shipment_id: str = "SHP-000001", delay_s: int = 0, **payload: Any) -> dict[str, Any]:
    body = {
        "shipment_id": shipment_id,
        "delivered_at": (NOW + timedelta(seconds=30 + delay_s)).isoformat(),
        "condition": "ok",
    }
    body.update(payload)
    return envelope("shipment.delivered", body)
