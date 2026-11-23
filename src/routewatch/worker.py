from collections.abc import Sequence

import psycopg
from confluent_kafka import Consumer, KafkaException, TopicPartition

from .raw import RAW_SCHEMA, RawRecord, store_raw_record


def persist_batch(
    connection: psycopg.Connection,
    consumer: Consumer,
    records: Sequence[RawRecord],
    *,
    schema: str = RAW_SCHEMA,
) -> int:
    """Commit the database batch before committing its Kafka offsets."""
    if not records:
        return 0

    if len({record.kafka_cluster_id for record in records}) != 1:
        raise ValueError("A batch must come from one Kafka cluster")

    inserted = 0
    next_offsets: dict[tuple[str, int], int] = {}

    with connection.transaction():
        for record in records:
            inserted += int(store_raw_record(connection, record, schema=schema))

            position = (record.topic, record.partition)
            next_offsets[position] = max(
                next_offsets.get(position, 0),
                record.offset + 1,
            )

    offsets = [
        TopicPartition(topic, partition, next_offset)
        for (topic, partition), next_offset in sorted(next_offsets.items())
    ]

    committed = consumer.commit(
        offsets=offsets,
        asynchronous=False,
    )

    if not committed:
        raise RuntimeError("Kafka returned no commit results")

    for partition in committed:
        if partition.error is not None:
            raise KafkaException(partition.error)

    return inserted
