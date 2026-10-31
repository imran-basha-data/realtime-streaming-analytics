"""CLI argument handling and structured logging (no external services)."""

from __future__ import annotations

import json
import logging
import sys
from collections.abc import Iterator

import pytest

from streaming import cli
from streaming.logging_config import JsonFormatter, TextFormatter, configure_logging


@pytest.fixture(autouse=True)
def restore_root_logger() -> Iterator[None]:
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    yield
    root.handlers[:] = handlers
    root.setLevel(level)


def record(**fields: object) -> logging.LogRecord:
    rec = logging.LogRecord(
        "streaming.test", logging.WARNING, __file__, 1, "batch_committed", None, None
    )
    rec.fields = fields
    return rec


def test_parser_requires_a_command() -> None:
    with pytest.raises(SystemExit):
        cli.build_parser().parse_args([])


def test_parser_accepts_every_role() -> None:
    parser = cli.build_parser()
    for argv in (
        ["topics"],
        ["migrate"],
        ["api"],
        ["consume", "--max-batches", "3"],
        ["produce", "--count", "5", "--rate", "10"],
    ):
        assert parser.parse_args(argv).command == argv[0]


@pytest.mark.parametrize("flag", ["--duplicate-rate", "--malformed-rate"])
@pytest.mark.parametrize("value", ["-0.1", "1", "1.5"])
def test_produce_rejects_rates_outside_unit_interval(
    flag: str, value: str, capsys: pytest.CaptureFixture[str]
) -> None:
    assert cli.main(["produce", flag, value]) == 2
    assert "must be in [0, 1)" in capsys.readouterr().err


def test_json_formatter_merges_fields() -> None:
    line = json.loads(JsonFormatter().format(record(processed=10, duplicates=1)))
    assert line["event"] == "batch_committed"
    assert line["level"] == "WARNING"
    assert (line["processed"], line["duplicates"]) == (10, 1)
    assert line["ts"].endswith("+00:00")


def test_json_formatter_includes_exceptions() -> None:
    try:
        raise ValueError("boom")
    except ValueError:
        rec = record()
        rec.exc_info = sys.exc_info()
    assert "ValueError: boom" in json.loads(JsonFormatter().format(rec))["exception"]


def test_text_formatter() -> None:
    assert TextFormatter().format(record(processed=3)) == (
        "WARNING streaming.test: batch_committed processed=3"
    )


@pytest.mark.parametrize(("fmt", "formatter"), [("json", JsonFormatter), ("text", TextFormatter)])
def test_configure_logging_installs_one_handler(fmt: str, formatter: type) -> None:
    configure_logging("debug", fmt)
    root = logging.getLogger()
    assert len(root.handlers) == 1
    assert isinstance(root.handlers[0].formatter, formatter)
    assert root.level == logging.DEBUG
