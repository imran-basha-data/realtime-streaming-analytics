from __future__ import annotations

import contextlib
import os
import sys
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))


def _require(var: str) -> str:
    value = os.environ.get(var)
    if not value:
        if os.environ.get("STREAM_REQUIRE_SERVICES") == "1":
            pytest.fail(f"STREAM_REQUIRE_SERVICES=1 but {var} is not set")
        pytest.skip(f"{var} not set")
    return value


@pytest.fixture(scope="session")
def bootstrap_servers() -> str:
    return _require("STREAM_TEST_BOOTSTRAP_SERVERS")


@pytest.fixture
def topic(bootstrap_servers: str) -> Iterator[str]:
    """A fresh 3-partition topic per test, deleted afterwards."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    name = f"test.events.{uuid.uuid4().hex[:10]}"
    for future in admin.create_topics(
        [NewTopic(name, num_partitions=3, replication_factor=1)]
    ).values():
        future.result()
    yield name
    for future in admin.delete_topics([name, f"{name}.dlq"]).values():
        with contextlib.suppress(Exception):  # best-effort cleanup
            future.result()


@pytest.fixture(scope="session")
def database_url() -> str:
    return _require("STREAM_TEST_DATABASE_URL")


@pytest.fixture
def db(database_url: str) -> str:
    """An empty, freshly migrated database for each test."""
    import psycopg

    from streaming.db import apply_schema

    with psycopg.connect(database_url, autocommit=True) as conn:
        for table in (
            "processed_events",
            "dead_letters",
            "production_minute",
            "quality_minute",
            "shipments",
        ):
            conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
    apply_schema(database_url)
    return database_url
