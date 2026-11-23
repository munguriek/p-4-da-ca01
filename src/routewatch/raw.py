from dataclasses import dataclass

import psycopg
from psycopg import sql
from psycopg.pq import TransactionStatus

from .capture import CapturedMessage

RAW_SCHEMA = "routewatch_raw"


@dataclass(frozen=True)
class RawRecord:
    kafka_cluster_id: str
    topic: str
    partition: int
    offset: int
    captured: CapturedMessage
    kafka_timestamp_ms: int | None = None


class RawReplayConflict(RuntimeError):
    """A Kafka position already exists with different raw data."""


def initialize_raw_storage(
    connection: psycopg.Connection,
    *,
    schema: str = RAW_SCHEMA,
) -> None:
    with connection.transaction():
        connection.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
        )

        connection.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {} (
                    kafka_cluster_id TEXT NOT NULL
                        CHECK (kafka_cluster_id <> ''),
                    topic TEXT NOT NULL CHECK (topic <> ''),
                    kafka_partition INTEGER NOT NULL
                        CHECK (kafka_partition >= 0),
                    kafka_offset BIGINT NOT NULL
                        CHECK (kafka_offset >= 0),
                    payload BYTEA NOT NULL,
                    run_id UUID NOT NULL,
                    received_at TIMESTAMPTZ NOT NULL,
                    kafka_timestamp_ms BIGINT,
                    stored_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                    PRIMARY KEY (
                        kafka_cluster_id, topic,
                        kafka_partition, kafka_offset
                    )
                )
                """
            ).format(sql.Identifier(schema, "messages"))
        )


def store_raw_record(
    connection: psycopg.Connection,
    record: RawRecord,
    *,
    schema: str = RAW_SCHEMA,
) -> bool:
    """Insert within the caller's transaction; return False for an exact replay."""
    if connection.info.transaction_status != TransactionStatus.INTRANS:
        raise RuntimeError("store_raw_record requires an active transaction")

    identity = (
        record.kafka_cluster_id,
        record.topic,
        record.partition,
        record.offset,
    )

    content = (
        record.captured.payload,
        record.captured.run_id,
        record.captured.received_at,
        record.kafka_timestamp_ms,
    )

    table = sql.Identifier(schema, "messages")

    inserted = connection.execute(
        sql.SQL(
            """
            INSERT INTO {} (
                kafka_cluster_id, topic, kafka_partition, kafka_offset,
                payload, run_id, received_at, kafka_timestamp_ms
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (
                kafka_cluster_id, topic, kafka_partition, kafka_offset
            ) DO NOTHING
            RETURNING 1
            """
        ).format(table),
        (*identity, *content),
    ).fetchone()

    if inserted is not None:
        return True

    existing = connection.execute(
        sql.SQL(
            """
            SELECT payload, run_id, received_at, kafka_timestamp_ms
            FROM {}
            WHERE kafka_cluster_id = %s
              AND topic = %s
              AND kafka_partition = %s
              AND kafka_offset = %s
            """
        ).format(table),
        identity,
    ).fetchone()

    if existing != content:
        raise RawReplayConflict("Different data already exists at this Kafka position")

    return False
