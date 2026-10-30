"""Database connection and schema setup."""

from __future__ import annotations

from importlib import resources

import psycopg


def connect(dsn: str) -> psycopg.Connection:
    return psycopg.connect(dsn, connect_timeout=10)


def apply_schema(dsn: str) -> None:
    sql = resources.files("streaming").joinpath("sql", "schema.sql").read_text("utf-8")
    with connect(dsn) as conn, conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(7311)")  # serialise concurrent starts
        conn.execute(sql)
