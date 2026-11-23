from dataclasses import dataclass

import psycopg
from psycopg import sql
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row

from .clean_storage import store_clean_result, store_rejected_message
from .transform import InvalidRISMessage, transform_message


@dataclass(frozen=True)
class CleanBatchResult:
    selected: int
    processed: int
    events: int
    rejected: int


def clean_pending_batch(
    connection: psycopg.Connection,
    *,
    topic: str,
    batch_size: int = 1000,
    transform_version: int = 1,
    schema_name: str = "routewatch_clean",
    raw_schema_name: str = "routewatch_raw",
) -> CleanBatchResult:
    """Stage one bounded batch inside the caller's transaction."""
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if transform_version <= 0:
        raise ValueError("transform_version must be positive")
    if connection.info.transaction_status != TransactionStatus.INTRANS:
        raise RuntimeError("clean_pending_batch requires an active transaction")

    processed = events = rejected = 0

    with connection.transaction(), connection.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            sql.SQL(
                """
                SELECT r.kafka_cluster_id, r.topic, r.kafka_partition,
                       r.kafka_offset, r.payload
                FROM {raw_messages} AS r
                WHERE r.topic = %s
                  AND NOT EXISTS (
                      SELECT 1 FROM {processing} AS p
                      WHERE p.kafka_cluster_id = r.kafka_cluster_id
                        AND p.topic = r.topic
                        AND p.kafka_partition = r.kafka_partition
                        AND p.kafka_offset = r.kafka_offset
                        AND p.transform_version = %s
                  )
                ORDER BY r.kafka_cluster_id, r.topic,
                         r.kafka_partition, r.kafka_offset
                LIMIT %s
                """
            ).format(
                raw_messages=sql.Identifier(raw_schema_name, "messages"),
                processing=sql.Identifier(schema_name, "processing"),
            ),
            (topic, transform_version, batch_size),
        )
        rows = cursor.fetchall()

        for cluster_id, raw_topic, partition, offset, payload in rows:
            position = {
                "kafka_cluster_id": cluster_id,
                "topic": raw_topic,
                "partition": partition,
                "offset": offset,
                "transform_version": transform_version,
                "schema_name": schema_name,
            }
            try:
                result = transform_message(bytes(payload))
            except InvalidRISMessage as error:
                inserted = store_rejected_message(
                    connection,
                    f"{type(error).__name__}: {error}",
                    **position,
                )
                if inserted:
                    processed += 1
                    rejected += 1
            else:
                inserted = store_clean_result(connection, result, **position)
                if inserted:
                    processed += 1
                    events += len(result.events)

    return CleanBatchResult(
        selected=len(rows),
        processed=processed,
        events=events,
        rejected=rejected,
    )
