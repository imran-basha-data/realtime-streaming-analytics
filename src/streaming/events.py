"""Event schemas (version 1).

Every event shares an envelope (``event_id``, ``event_type``, ``schema_version``,
``occurred_at``, ``source``) and carries a typed payload. Parsing goes through a
discriminated union on ``event_type``, so an event is either fully valid for
its type or rejected with a precise error.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from enum import StrEnum
from typing import Annotated, Literal
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    ValidationError,
    model_validator,
)

PlantId = Annotated[str, Field(pattern=r"^PLANT-[A-Z]{3}$")]
ProductId = Annotated[str, Field(pattern=r"^PRD-\d{4}$")]
ShipmentId = Annotated[str, Field(pattern=r"^SHP-\d{6}$")]


class EventType(StrEnum):
    PRODUCTION_COMPLETED = "production.completed"
    QUALITY_INSPECTED = "quality.inspected"
    SHIPMENT_DISPATCHED = "shipment.dispatched"
    SHIPMENT_DELIVERED = "shipment.delivered"


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ProductionCompleted(_Strict):
    plant_id: PlantId
    line_id: Annotated[str, Field(pattern=r"^L\d{2}$")]
    product_id: ProductId
    units: Annotated[int, Field(ge=1, le=10_000)]
    scrap_units: Annotated[int, Field(ge=0, le=10_000)]


class QualityInspected(_Strict):
    plant_id: PlantId
    product_id: ProductId
    sample_size: Annotated[int, Field(ge=1, le=1_000)]
    defects: Annotated[int, Field(ge=0, le=1_000)]

    @model_validator(mode="after")
    def _defects_within_sample(self) -> QualityInspected:
        if self.defects > self.sample_size:
            raise ValueError("defects cannot exceed sample_size")
        return self


class ShipmentDispatched(_Strict):
    shipment_id: ShipmentId
    origin_plant_id: PlantId
    destination: Annotated[str, Field(min_length=2, max_length=64)]
    carrier: Annotated[str, Field(min_length=2, max_length=64)]
    promised_at: AwareDatetime


class ShipmentDelivered(_Strict):
    shipment_id: ShipmentId
    delivered_at: AwareDatetime
    condition: Literal["ok", "damaged"]


class _Envelope(_Strict):
    event_id: UUID
    schema_version: Literal[1]
    occurred_at: AwareDatetime
    source: Annotated[str, Field(min_length=1, max_length=64)]


class ProductionCompletedEvent(_Envelope):
    event_type: Literal[EventType.PRODUCTION_COMPLETED]
    payload: ProductionCompleted


class QualityInspectedEvent(_Envelope):
    event_type: Literal[EventType.QUALITY_INSPECTED]
    payload: QualityInspected


class ShipmentDispatchedEvent(_Envelope):
    event_type: Literal[EventType.SHIPMENT_DISPATCHED]
    payload: ShipmentDispatched


class ShipmentDeliveredEvent(_Envelope):
    event_type: Literal[EventType.SHIPMENT_DELIVERED]
    payload: ShipmentDelivered


Event = Annotated[
    ProductionCompletedEvent
    | QualityInspectedEvent
    | ShipmentDispatchedEvent
    | ShipmentDeliveredEvent,
    Field(discriminator="event_type"),
]
EVENT_ADAPTER: TypeAdapter[Event] = TypeAdapter(Event)


class InvalidEvent(ValueError):
    """Raised when a message is not a valid event. ``reason`` is a short code."""

    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail


def parse_event(raw: bytes | None, *, now: datetime, max_clock_skew: timedelta) -> Event:
    """Decode and validate one Kafka message value.

    Reason codes: ``empty_message``, ``invalid_json``, ``schema_violation``,
    ``unsupported_event_type``, ``future_timestamp``.
    """
    if not raw:
        raise InvalidEvent("empty_message", "message value is empty")
    try:
        event = EVENT_ADAPTER.validate_json(raw)
    except ValidationError as exc:
        first = exc.errors()[0]
        if first["type"] == "json_invalid":
            raise InvalidEvent("invalid_json", str(first["msg"])) from exc
        if first["type"] == "union_tag_invalid":
            raise InvalidEvent("unsupported_event_type", str(first["msg"])) from exc
        location = ".".join(str(p) for p in first["loc"])
        raise InvalidEvent("schema_violation", f"{location}: {first['msg']}") from exc
    if event.occurred_at > now + max_clock_skew:
        raise InvalidEvent(
            "future_timestamp", f"occurred_at {event.occurred_at.isoformat()} is ahead of now"
        )
    return event


def partition_key(event: Event) -> str:
    """Key that keeps related events in order on one partition.

    Shipment events are keyed by shipment so dispatch and delivery for the same
    shipment are consumed in order; plant events are keyed by plant.
    """
    payload = event.payload
    if isinstance(payload, ShipmentDispatched | ShipmentDelivered):
        return payload.shipment_id
    return payload.plant_id


def serialize(event: Event) -> bytes:
    return event.model_dump_json().encode("utf-8")
