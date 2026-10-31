"""Runtime configuration from ``STREAM_*`` environment variables."""

from __future__ import annotations

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="STREAM_", extra="ignore", frozen=True)

    bootstrap_servers: str = "localhost:9092"
    events_topic: str = "manufacturing.events"
    dlq_topic: str = "manufacturing.events.dlq"
    topic_partitions: int = Field(default=6, ge=1, le=100)
    topic_replication: int = Field(default=1, ge=1, le=5)

    consumer_group: str = "analytics-consumer"
    # Upper bound on messages processed per database transaction.
    max_batch_size: int = Field(default=500, ge=1, le=10_000)
    poll_timeout_s: float = Field(default=1.0, gt=0, le=30)
    # Retries for transient database errors before the consumer gives up and
    # exits without committing offsets (the batch is then redelivered).
    db_max_retries: int = Field(default=5, ge=1, le=20)
    db_retry_base_delay_s: float = Field(default=0.5, ge=0, le=30)
    # Events further in the future than this are rejected as clock-skewed.
    max_clock_skew_s: int = Field(default=300, ge=0, le=86_400)

    database_url: str = "postgresql://rsa:rsa_local_dev@localhost:5432/rsa"
    metrics_port: int = Field(default=9108, ge=1024, le=65_535)

    # Analytics API. Loopback by default; containers set STREAM_API_HOST=0.0.0.0.
    api_host: str = "127.0.0.1"
    api_port: int = Field(default=8000, ge=1024, le=65_535)
    api_pool_size: int = Field(default=10, ge=1, le=100)
    log_level: str = "INFO"
    log_format: str = "json"

    @model_validator(mode="after")
    def _distinct_topics(self) -> Settings:
        if self.events_topic == self.dlq_topic:
            raise ValueError("STREAM_DLQ_TOPIC must differ from STREAM_EVENTS_TOPIC")
        if not self.database_url.startswith(("postgresql://", "postgres://")):
            raise ValueError("STREAM_DATABASE_URL must be a postgresql:// URL")
        return self
