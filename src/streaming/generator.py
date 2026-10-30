"""Synthetic manufacturing and logistics event stream.

Shipment transit is time-compressed: a promised delivery window is seconds to
a minute rather than hours, so a demo run shows on-time and late deliveries
within a few minutes. Every other field is realistic in shape and range.
"""

from __future__ import annotations

import random
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from streaming.events import (
    Event,
    EventType,
    ProductionCompleted,
    ProductionCompletedEvent,
    QualityInspected,
    QualityInspectedEvent,
    ShipmentDelivered,
    ShipmentDeliveredEvent,
    ShipmentDispatched,
    ShipmentDispatchedEvent,
)

Clock = Callable[[], datetime]

PLANTS = ("PLANT-BHM", "PLANT-MAN", "PLANT-LDS")
LINES = ("L01", "L02", "L03")
DESTINATIONS = ("Glasgow DC", "Bristol DC", "London DC", "Dublin DC", "Rotterdam Hub")
CARRIERS = ("DHL Freight", "DPD", "XPO", "Kuehne Logistics")


@dataclass(frozen=True)
class GeneratorConfig:
    seed: int | None = None
    products: int = 12
    # Relative frequency of each event type.
    production_weight: float = 0.5
    quality_weight: float = 0.15
    dispatch_weight: float = 0.2
    delivery_weight: float = 0.15
    min_transit_s: float = 5.0
    max_transit_s: float = 60.0


@dataclass
class _Shipment:
    shipment_id: str
    promised_at: datetime


@dataclass
class EventGenerator:
    config: GeneratorConfig = field(default_factory=GeneratorConfig)
    clock: Clock = field(default=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        # Seeded PRNG for reproducible synthetic data, not a security use.
        self._rng = random.Random(self.config.seed)  # noqa: S311  # nosec B311
        self._products = [f"PRD-{n:04d}" for n in range(1, self.config.products + 1)]
        self._pending: list[_Shipment] = []
        self._shipment_seq = 0

    def _uuid(self) -> uuid.UUID:
        return uuid.UUID(int=self._rng.getrandbits(128), version=4)

    def next_event(self) -> Event:
        cfg = self.config
        weights = [cfg.production_weight, cfg.quality_weight, cfg.dispatch_weight]
        kinds = [
            EventType.PRODUCTION_COMPLETED,
            EventType.QUALITY_INSPECTED,
            EventType.SHIPMENT_DISPATCHED,
        ]
        if self._pending:
            kinds.append(EventType.SHIPMENT_DELIVERED)
            weights.append(cfg.delivery_weight)
        kind = self._rng.choices(kinds, weights=weights)[0]
        now = self.clock()
        envelope = {"event_id": self._uuid(), "schema_version": 1, "occurred_at": now}

        if kind is EventType.PRODUCTION_COMPLETED:
            units = self._rng.randint(5, 120)
            return ProductionCompletedEvent(
                **envelope,
                event_type=kind,
                source="mes",
                payload=ProductionCompleted(
                    plant_id=self._rng.choice(PLANTS),
                    line_id=self._rng.choice(LINES),
                    product_id=self._rng.choice(self._products),
                    units=units,
                    scrap_units=self._rng.randint(0, max(1, units // 15)),
                ),
            )
        if kind is EventType.QUALITY_INSPECTED:
            sample = self._rng.choice([10, 20, 50])
            defect_rate = self._rng.choice([0.0, 0.0, 0.02, 0.05, 0.12])
            return QualityInspectedEvent(
                **envelope,
                event_type=kind,
                source="qms",
                payload=QualityInspected(
                    plant_id=self._rng.choice(PLANTS),
                    product_id=self._rng.choice(self._products),
                    sample_size=sample,
                    defects=min(sample, round(sample * defect_rate * self._rng.uniform(0.5, 1.5))),
                ),
            )
        if kind is EventType.SHIPMENT_DISPATCHED:
            self._shipment_seq += 1
            shipment_id = f"SHP-{self._shipment_seq:06d}"
            promised = now + timedelta(
                seconds=self._rng.uniform(cfg.min_transit_s, cfg.max_transit_s)
            )
            self._pending.append(_Shipment(shipment_id, promised))
            return ShipmentDispatchedEvent(
                **envelope,
                event_type=kind,
                source="tms",
                payload=ShipmentDispatched(
                    shipment_id=shipment_id,
                    origin_plant_id=self._rng.choice(PLANTS),
                    destination=self._rng.choice(DESTINATIONS),
                    carrier=self._rng.choice(CARRIERS),
                    promised_at=promised,
                ),
            )
        shipment = self._pending.pop(self._rng.randrange(len(self._pending)))
        return ShipmentDeliveredEvent(
            **envelope,
            event_type=EventType.SHIPMENT_DELIVERED,
            source="tms",
            payload=ShipmentDelivered(
                shipment_id=shipment.shipment_id,
                delivered_at=now,
                condition="damaged" if self._rng.random() < 0.03 else "ok",
            ),
        )

    @property
    def rng(self) -> random.Random:
        return self._rng
