-- 003: the raw tables with payload_bytes added in raw.otel.
-- Insert only: raw rows are never updated or deleted.

-- One row per Kafka message: one OTLP export request as JSON
CREATE TABLE raw.otel (
    topic              text NOT NULL,
    kpartition         int NOT NULL,
    koffset            bigint NOT NULL,
    kafka_timestamp    timestamptz NOT NULL,
    arrived_at         timestamptz NOT NULL default now(),
    payload            jsonb NOT NULL,
    payload_sha256     bytea NOT NULL,
    payload_bytes      integer NOT NULL,
    run_id uuid        NOT NULL,
    PRIMARY KEY (topic, kpartition, koffset, kafka_timestamp)
) PARTITION BY RANGE(kafka_timestamp);

