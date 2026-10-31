-- Idempotent schema: safe to apply on every start.

-- One row per event ever accepted. The primary key on event_id is the
-- de-duplication point that makes at-least-once delivery count each event once.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id        uuid PRIMARY KEY,
    event_type      text NOT NULL,
    occurred_at     timestamptz NOT NULL,
    ingested_at     timestamptz NOT NULL DEFAULT now(),
    kafka_partition integer NOT NULL,
    kafka_offset    bigint NOT NULL,
    latency_ms      double precision NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_processed_events_ingested ON processed_events (ingested_at);
CREATE INDEX IF NOT EXISTS ix_processed_events_type_time ON processed_events (event_type, occurred_at);

-- Messages that could not be processed. Unique on Kafka coordinates so a
-- redelivered poison message is recorded once.
CREATE TABLE IF NOT EXISTS dead_letters (
    dead_letter_id  bigserial PRIMARY KEY,
    topic           text NOT NULL,
    kafka_partition integer NOT NULL,
    kafka_offset    bigint NOT NULL,
    reason          text NOT NULL,
    detail          text NOT NULL,
    payload         text,
    received_at     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_dead_letters_coordinates UNIQUE (topic, kafka_partition, kafka_offset)
);
CREATE INDEX IF NOT EXISTS ix_dead_letters_received ON dead_letters (received_at);

-- Per-minute production rollup (event time).
CREATE TABLE IF NOT EXISTS production_minute (
    plant_id        text NOT NULL,
    product_id      text NOT NULL,
    minute          timestamptz NOT NULL,
    units           bigint NOT NULL,
    scrap_units     bigint NOT NULL,
    events          integer NOT NULL,
    PRIMARY KEY (plant_id, product_id, minute)
);

-- Per-minute quality rollup (event time).
CREATE TABLE IF NOT EXISTS quality_minute (
    plant_id        text NOT NULL,
    product_id      text NOT NULL,
    minute          timestamptz NOT NULL,
    samples         bigint NOT NULL,
    defects         bigint NOT NULL,
    inspections     integer NOT NULL,
    PRIMARY KEY (plant_id, product_id, minute)
);

-- Current state per shipment. Dispatch and delivery may arrive in either
-- order (e.g. after a replay), so each side only fills its own columns.
CREATE TABLE IF NOT EXISTS shipments (
    shipment_id     text PRIMARY KEY,
    origin_plant_id text,
    destination     text,
    carrier         text,
    dispatched_at   timestamptz,
    promised_at     timestamptz,
    delivered_at    timestamptz,
    condition       text,
    updated_at      timestamptz NOT NULL DEFAULT now()
);
