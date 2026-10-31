"""Command-line interface: ``stream <command>``."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence

from streaming import __version__
from streaming.config import Settings
from streaming.generator import EventGenerator, GeneratorConfig
from streaming.logging_config import configure_logging
from streaming.producer import EventProducer, run_producer
from streaming.topics import ensure_topics


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="stream", description="Event streaming pipeline.")
    parser.add_argument("--version", action="version", version=f"stream {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("topics", help="Create the events and dead-letter topics if missing.")

    produce = sub.add_parser("produce", help="Publish synthetic events.")
    produce.add_argument("--count", type=int, default=1000)
    produce.add_argument("--rate", type=float, default=100.0, help="Events per second.")
    produce.add_argument("--seed", type=int, default=None)
    produce.add_argument("--duplicate-rate", type=float, default=0.0)
    produce.add_argument("--malformed-rate", type=float, default=0.0)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = Settings()
    configure_logging(settings.log_level, settings.log_format)

    if args.command == "topics":
        created = ensure_topics(settings)
        print(json.dumps({"created": created}))
        return 0

    if args.command == "produce":
        for name in ("duplicate_rate", "malformed_rate"):
            if not 0 <= getattr(args, name) < 1:
                print(f"--{name.replace('_', '-')} must be in [0, 1)", file=sys.stderr)
                return 2
        producer = EventProducer.connect(settings.bootstrap_servers, settings.events_topic)
        stats = run_producer(
            producer,
            EventGenerator(GeneratorConfig(seed=args.seed)),
            count=args.count,
            rate_per_s=args.rate,
            duplicate_rate=args.duplicate_rate,
            malformed_rate=args.malformed_rate,
        )
        print(json.dumps(stats.as_dict()))
        return 0 if stats.failed == 0 else 1

    return 1  # pragma: no cover


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
