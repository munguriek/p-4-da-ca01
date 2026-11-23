import logging
from datetime import datetime
from time import monotonic
from uuid import UUID

from confluent_kafka import (
    TIMESTAMP_NOT_AVAILABLE,
    Consumer,
    KafkaException,
    Message,
)

from .capture import capture_message
from .config import KafkaSettings, PostgresSettings
from .db import connect_postgres
from .raw import RAW_SCHEMA, RawRecord, initialize_raw_storage
from .worker import persist_batch

logger = logging.getLogger(__name__)


def create_consumer(settings: KafkaSettings, *, group_id: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "client.id": "airdeall-routewatch-raw",
            "group.id": group_id,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "allow.auto.create.topics": False,
            "auto.offset.reset": "earliest",
            "max.partition.fetch.bytes": 8_388_608,
            "socket.timeout.ms": 10_000,
        }
    )


def to_raw_record(message: Message, *, kafka_cluster_id: str) -> RawRecord:
    error = message.error()

    if error is not None:
        raise KafkaException(error)

    payload = message.value()

    if not isinstance(payload, bytes):
        raise TypeError("Raw consumer requires a bytes payload")

    topic = message.topic()

    if not topic:
        raise ValueError("Kafka message has no topic")

    headers = message.headers() or []

    def header_text(name: str) -> str:
        values = [value for key, value in headers if key == name]

        if len(values) != 1 or not isinstance(values[0], bytes):
            raise ValueError(f"Expected one {name} header")

        return values[0].decode("ascii")

    captured = capture_message(
        payload,
        run_id=UUID(header_text("routewatch_run_id")),
        received_at=datetime.fromisoformat(header_text("routewatch_received_at")),
    )

    timestamp_type, timestamp_ms = message.timestamp()

    return RawRecord(
        kafka_cluster_id=kafka_cluster_id,
        topic=topic,
        partition=message.partition(),
        offset=message.offset(),
        captured=captured,
        kafka_timestamp_ms=(
            None if timestamp_type == TIMESTAMP_NOT_AVAILABLE else timestamp_ms
        ),
    )


def run_raw_worker(
    kafka: KafkaSettings,
    postgres: PostgresSettings,
    *,
    group_id: str,
    max_messages: int = 0,
    idle_timeout: float | None = None,
    schema: str = RAW_SCHEMA,
) -> tuple[int, int]:
    """Return (processed, inserted); zero max_messages runs continuously."""
    if max_messages < 0:
        raise ValueError("max_messages must be zero or positive")

    if idle_timeout is not None and idle_timeout <= 0:
        raise ValueError("idle_timeout must be positive")

    processed = 0
    inserted = 0

    with (
        connect_postgres(postgres) as connection,
        create_consumer(kafka, group_id=group_id) as consumer,
    ):
        initialize_raw_storage(connection, schema=schema)

        metadata = consumer.list_topics(timeout=10)

        if not metadata.cluster_id:
            raise RuntimeError("Kafka returned no cluster ID")

        if kafka.topic not in metadata.topics:
            raise RuntimeError(f"Missing Kafka topic: {kafka.topic}")

        consumer.subscribe([kafka.topic])
        last_activity = monotonic()

        while max_messages == 0 or processed < max_messages:
            limit = min(100, max_messages - processed) if max_messages else 100
            messages = consumer.consume(num_messages=limit, timeout=1.0)

            if not messages:
                if (
                    idle_timeout is not None
                    and monotonic() - last_activity >= idle_timeout
                ):
                    raise TimeoutError("No Kafka messages before idle timeout")

                continue

            records = [
                to_raw_record(message, kafka_cluster_id=metadata.cluster_id)
                for message in messages
            ]

            inserted += persist_batch(
                connection,
                consumer,
                records,
                schema=schema,
            )

            processed += len(records)
            last_activity = monotonic()

            logger.info(
                "processed=%d inserted=%d replayed=%d",
                processed,
                inserted,
                processed - inserted,
            )

    return processed, inserted
