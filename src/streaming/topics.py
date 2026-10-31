"""Topic administration."""

from __future__ import annotations

import logging
from typing import Any

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka import admin as kafka_admin
from confluent_kafka.admin import AdminClient

from streaming.config import Settings

# NewTopic is re-exported by confluent_kafka.admin but missing from its __all__.
NewTopic = kafka_admin.NewTopic  # type: ignore[attr-defined]

log = logging.getLogger(__name__)

SEVEN_DAYS_MS = 7 * 24 * 3600 * 1000
THIRTY_DAYS_MS = 30 * 24 * 3600 * 1000


def topic_specs(settings: Settings) -> list[Any]:
    return [
        NewTopic(
            settings.events_topic,
            num_partitions=settings.topic_partitions,
            replication_factor=settings.topic_replication,
            config={"retention.ms": str(SEVEN_DAYS_MS), "cleanup.policy": "delete"},
        ),
        # The DLQ keeps poison messages longer so they can be inspected and replayed.
        NewTopic(
            settings.dlq_topic,
            num_partitions=1,
            replication_factor=settings.topic_replication,
            config={"retention.ms": str(THIRTY_DAYS_MS), "cleanup.policy": "delete"},
        ),
    ]


def ensure_topics(settings: Settings, admin: AdminClient | None = None) -> list[str]:
    """Create missing topics; existing topics are left unchanged. Returns created names."""
    admin = admin or AdminClient({"bootstrap.servers": settings.bootstrap_servers})
    created = []
    for name, future in admin.create_topics(topic_specs(settings), request_timeout=15).items():
        try:
            future.result()
            created.append(name)
            log.info("topic_created", extra={"fields": {"topic": name}})
        except KafkaException as exc:
            error = exc.args[0]
            if isinstance(error, KafkaError) and error.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                continue
            raise
    return created
