"""Read-only analytics API over the rollups written by the consumer.

All windows are measured back from ``as_of`` (the API clock, injectable for
tests). Production and quality windows use **event time** (the minute the
event occurred); pipeline health uses **ingestion time**.
"""

# No ``from __future__ import annotations``: FastAPI must evaluate the
# dependency annotations (``AsOf``) defined inside ``create_app``.

import logging
import time
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

import psycopg
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Histogram,
    generate_latest,
)
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from pydantic import BaseModel

from streaming import __version__
from streaming.config import Settings

log = logging.getLogger(__name__)

Clock = Callable[[], datetime]
WindowMinutes = Annotated[int, Query(ge=1, le=1440, description="Look-back window in minutes.")]
WindowHours = Annotated[int, Query(ge=1, le=720, description="Look-back window in hours.")]
PlantFilter = Annotated[
    str | None, Query(pattern=r"^PLANT-[A-Z]{3}$", description="Restrict to one plant.")
]


# ------------------------------------------------------------------ responses
class ProductionSummary(BaseModel):
    plant_id: str
    units: int
    scrap_units: int
    scrap_rate: float
    events: int


class ProductionPoint(BaseModel):
    minute: datetime
    units: int
    scrap_units: int


class QualitySummary(BaseModel):
    plant_id: str
    product_id: str
    samples: int
    defects: int
    defect_rate: float
    inspections: int


class ShipmentSummary(BaseModel):
    window_hours: int
    dispatched: int
    delivered: int
    in_transit: int
    on_time: int
    late: int
    on_time_rate: float | None
    damaged: int
    avg_late_minutes: float | None
    deliveries_without_dispatch: int


class PipelineHealth(BaseModel):
    window_minutes: int
    events_ingested: int
    dead_letters: int
    dead_letters_by_reason: dict[str, int]
    latency_ms_p50: float | None
    latency_ms_p95: float | None
    latency_ms_max: float | None
    last_ingested_at: datetime | None
    seconds_since_last_event: float | None


# ------------------------------------------------------------------ metrics
class ApiMetrics:
    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        self.requests = Counter(
            "api_requests_total",
            "HTTP requests by route template, method and status.",
            ["route", "method", "status"],
            registry=self.registry,
        )
        self.latency = Histogram(
            "api_request_duration_seconds",
            "HTTP request duration by route template.",
            ["route"],
            buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5),
            registry=self.registry,
        )


# ------------------------------------------------------------------ app
def create_app(
    settings: Settings | None = None,
    *,
    clock: Clock = lambda: datetime.now(UTC),
    pool: ConnectionPool | None = None,
) -> FastAPI:
    settings = settings or Settings()
    owns_pool = pool is None
    db_pool = pool or ConnectionPool(
        settings.database_url,
        min_size=1,
        max_size=settings.api_pool_size,
        timeout=5,
        open=False,
        kwargs={"connect_timeout": 5},
    )
    metrics = ApiMetrics()

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        if owns_pool:
            # wait=False: the API starts even if PostgreSQL is down; readiness reports it.
            db_pool.open(wait=False)
        try:
            yield
        finally:
            if owns_pool:
                db_pool.close()

    app = FastAPI(
        title="Manufacturing stream analytics",
        version=__version__,
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url=None,
    )

    @contextmanager
    def cursor() -> Iterator[psycopg.Cursor[dict[str, Any]]]:
        try:
            with db_pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                yield cur
        except (psycopg.OperationalError, TimeoutError) as exc:
            log.warning("db_unavailable", extra={"fields": {"error": str(exc)}})
            raise HTTPException(status_code=503, detail="database unavailable") from exc

    def as_of() -> datetime:
        return clock()

    AsOf = Annotated[datetime, Depends(as_of)]

    @app.middleware("http")
    async def record_metrics(request: Request, call_next: Any) -> Response:
        started = time.perf_counter()
        response: Response = await call_next(request)
        route = request.scope.get("route")
        # Label by template ("/api/v1/...") rather than raw path to bound cardinality.
        template = getattr(route, "path", "unmatched")
        metrics.requests.labels(template, request.method, str(response.status_code)).inc()
        metrics.latency.labels(template).observe(time.perf_counter() - started)
        return response

    @app.get("/health/live", tags=["health"])
    def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready", tags=["health"])
    def ready() -> dict[str, str]:
        with cursor() as cur:
            cur.execute("SELECT 1 FROM processed_events LIMIT 1")
        return {"status": "ready"}

    @app.get("/metrics", include_in_schema=False)
    def metrics_endpoint() -> Response:
        return Response(generate_latest(metrics.registry), media_type=CONTENT_TYPE_LATEST)

    @app.get("/api/v1/production/summary", tags=["production"])
    def production_summary(
        now: AsOf, window_minutes: WindowMinutes = 60, plant_id: PlantFilter = None
    ) -> list[ProductionSummary]:
        with cursor() as cur:
            cur.execute(
                """
                SELECT plant_id,
                       sum(units)::bigint       AS units,
                       sum(scrap_units)::bigint AS scrap_units,
                       sum(events)::bigint      AS events
                FROM production_minute
                WHERE minute >= %(since)s AND minute <= %(now)s
                  AND (%(plant)s::text IS NULL OR plant_id = %(plant)s::text)
                GROUP BY plant_id
                ORDER BY plant_id
                """,
                {"since": _since(now, window_minutes), "now": now, "plant": plant_id},
            )
            rows = cur.fetchall()
        return [
            ProductionSummary(**row, scrap_rate=_ratio(row["scrap_units"], row["units"]))
            for row in rows
        ]

    @app.get("/api/v1/production/timeseries", tags=["production"])
    def production_timeseries(
        now: AsOf, window_minutes: WindowMinutes = 60, plant_id: PlantFilter = None
    ) -> list[ProductionPoint]:
        with cursor() as cur:
            cur.execute(
                """
                SELECT minute,
                       sum(units)::bigint       AS units,
                       sum(scrap_units)::bigint AS scrap_units
                FROM production_minute
                WHERE minute >= %(since)s AND minute <= %(now)s
                  AND (%(plant)s::text IS NULL OR plant_id = %(plant)s::text)
                GROUP BY minute
                ORDER BY minute
                """,
                {"since": _since(now, window_minutes), "now": now, "plant": plant_id},
            )
            return [ProductionPoint(**row) for row in cur.fetchall()]

    @app.get("/api/v1/quality/summary", tags=["quality"])
    def quality_summary(
        now: AsOf, window_minutes: WindowMinutes = 60, plant_id: PlantFilter = None
    ) -> list[QualitySummary]:
        with cursor() as cur:
            cur.execute(
                """
                SELECT plant_id, product_id,
                       sum(samples)::bigint     AS samples,
                       sum(defects)::bigint     AS defects,
                       sum(inspections)::bigint AS inspections
                FROM quality_minute
                WHERE minute >= %(since)s AND minute <= %(now)s
                  AND (%(plant)s::text IS NULL OR plant_id = %(plant)s::text)
                GROUP BY plant_id, product_id
                ORDER BY plant_id, product_id
                """,
                {"since": _since(now, window_minutes), "now": now, "plant": plant_id},
            )
            rows = cur.fetchall()
        return [
            QualitySummary(**row, defect_rate=_ratio(row["defects"], row["samples"]))
            for row in rows
        ]

    @app.get("/api/v1/shipments/summary", tags=["shipments"])
    def shipments_summary(now: AsOf, window_hours: WindowHours = 24) -> ShipmentSummary:
        since = now - timedelta(hours=window_hours)
        with cursor() as cur:
            cur.execute(
                """
                SELECT
                  count(*) FILTER (WHERE dispatched_at IS NOT NULL)                   AS dispatched,
                  count(*) FILTER (WHERE dispatched_at IS NOT NULL
                                     AND delivered_at IS NOT NULL)                    AS delivered,
                  count(*) FILTER (WHERE dispatched_at IS NOT NULL
                                     AND delivered_at IS NULL)                        AS in_transit,
                  count(*) FILTER (WHERE delivered_at <= promised_at)                 AS on_time,
                  count(*) FILTER (WHERE delivered_at >  promised_at)                 AS late,
                  count(*) FILTER (WHERE dispatched_at IS NOT NULL
                                     AND condition = 'damaged')                       AS damaged,
                  avg(extract(epoch FROM delivered_at - promised_at) / 60.0)
                      FILTER (WHERE delivered_at > promised_at)                       AS avg_late
                FROM shipments
                WHERE dispatched_at >= %(since)s AND dispatched_at <= %(now)s
                """,
                {"since": since, "now": now},
            )
            row = cur.fetchone() or {}
            cur.execute(
                """
                SELECT count(*) AS orphans FROM shipments
                WHERE dispatched_at IS NULL AND delivered_at >= %(since)s
                """,
                {"since": since},
            )
            orphans = (cur.fetchone() or {}).get("orphans", 0)
        assessed = row["on_time"] + row["late"]
        return ShipmentSummary(
            window_hours=window_hours,
            dispatched=row["dispatched"],
            delivered=row["delivered"],
            in_transit=row["in_transit"],
            on_time=row["on_time"],
            late=row["late"],
            on_time_rate=_ratio(row["on_time"], assessed) if assessed else None,
            damaged=row["damaged"],
            avg_late_minutes=_round(row["avg_late"]),
            deliveries_without_dispatch=orphans,
        )

    @app.get("/api/v1/pipeline/health", tags=["pipeline"])
    def pipeline_health(now: AsOf, window_minutes: WindowMinutes = 15) -> PipelineHealth:
        since = _since(now, window_minutes)
        with cursor() as cur:
            cur.execute(
                """
                SELECT count(*) AS events,
                       percentile_cont(0.5)  WITHIN GROUP (ORDER BY latency_ms) AS p50,
                       percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95,
                       max(latency_ms) AS max
                FROM processed_events
                WHERE ingested_at >= %(since)s AND ingested_at <= %(now)s
                """,
                {"since": since, "now": now},
            )
            stats = cur.fetchone() or {}
            cur.execute(
                """
                SELECT reason, count(*) AS n FROM dead_letters
                WHERE received_at >= %(since)s AND received_at <= %(now)s
                GROUP BY reason ORDER BY reason
                """,
                {"since": since, "now": now},
            )
            by_reason = {r["reason"]: r["n"] for r in cur.fetchall()}
            cur.execute("SELECT max(ingested_at) AS last FROM processed_events")
            last = (cur.fetchone() or {}).get("last")
        return PipelineHealth(
            window_minutes=window_minutes,
            events_ingested=stats["events"],
            dead_letters=sum(by_reason.values()),
            dead_letters_by_reason=by_reason,
            latency_ms_p50=_round(stats["p50"]),
            latency_ms_p95=_round(stats["p95"]),
            latency_ms_max=_round(stats["max"]),
            last_ingested_at=last,
            seconds_since_last_event=_round((now - last).total_seconds()) if last else None,
        )

    return app


def _since(now: datetime, window_minutes: int) -> datetime:
    return now - timedelta(minutes=window_minutes)


def _ratio(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 4) if denominator else 0.0


def _round(value: float | None) -> float | None:
    return None if value is None else round(float(value), 1)
