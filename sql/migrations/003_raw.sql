-- 003: the raw tables with payload_bytes added in raw.otel.
-- Insert only: raw rows are never updated or deleted.

-- One row per Kafka message: one OTLP export request as JSON
CREATE TABLE raw.otel (
    topic              text NOT NULL,
    kpartition         int NOT NULL,
    koffset            bigint NOT NULL,
    kafka_timestamp    timestamptz NOT NULL, -- record time, also the partition key
    arrived_at         timestamptz NOT NULL default now(),
    payload            jsonb NOT NULL,
    payload_sha256     bytea NOT NULL,
    payload_bytes      integer NOT NULL, -- size of the message as received, for measuring volume
    run_id uuid        NOT NULL,
    PRIMARY KEY (topic, kpartition, koffset, kafka_timestamp)
) PARTITION BY RANGE (kafka_timestamp);

-- one row per event from producers
CREATE TABLE raw.events (
    source           text NOT NULL REFERENCES control.source_register (source_id), -- gitlab.production, demo.flag-changes
    event_id         text NOT NULL, -- issue id updated_at; flip_id
    event_type       text NOT NULL,
    event_time       timestamptz, -- when it happened at the source
    arrived_at       timestamptz NOT NULL default now(),
    payload          jsonb NOT NULL, -- people already psuedomised
    payload_sha256   bytea NOT NULL,
    run_id           uuid NOT NULL,
    PRIMARY KEY (source, event_id)
);
