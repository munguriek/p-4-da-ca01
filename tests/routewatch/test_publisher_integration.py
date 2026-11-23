from collections.abc import Iterator
from datetime import UTC, datetime
from time import monotonic
from uuid import uuid4

import pytest
from confluent_kafka import TIMESTAMP_CREATE_TIME, Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from routewatch.capture import capture_message
from routewatch.config import KafkaSettings, load_kafka_settings
from routewatch.publisher import KafkaPublisher, create_producer

pytestmark = pytest.mark.integration


@pytest.fixture
def kafka_test_topic() -> Iterator[tuple[KafkaSettings, str]]:
    settings = load_kafka_settings()
    topic = f"routewatch.test.{uuid4().hex}"
    admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})

    specification = NewTopic(
        topic,
        num_partitions=1,
        replication_factor=1,
        config={
            "cleanup.policy": "delete",
            "retention.ms": "3600000",
            "segment.ms": "60000",
            "max.message.bytes": "8388608",
            "message.timestamp.type": "CreateTime",
        },
    )

    admin.create_topics(
        [specification],
        operation_timeout=10,
        request_timeout=15,
    )[topic].result(timeout=20)

    try:
        yield settings, topic
    finally:
        admin.delete_topics(
            [topic],
            operation_timeout=10,
            request_timeout=15,
        )[topic].result(timeout=20)


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param(b'{"id":"event-1"}  ', id="json-and-whitespace"),
        pytest.param(b"\xffnot-json", id="binary-payload"),
        pytest.param(b"x" * 8_000_000, id="maximum-capture-size"),
    ],
)
def test_publisher_round_trip(
    kafka_test_topic: tuple[KafkaSettings, str],
    payload: bytes,
) -> None:
    settings, topic = kafka_test_topic

    record = capture_message(
        payload,
        run_id=uuid4(),
        received_at=datetime.now(UTC),
    )

    with (
        create_producer(settings) as producer,
        Consumer(
            {
                "bootstrap.servers": settings.bootstrap_servers,
                "group.id": f"routewatch.test.{uuid4().hex}",
                "enable.auto.commit": False,
                "allow.auto.create.topics": False,
                "max.partition.fetch.bytes": 8_388_608,
            }
        ) as consumer,
    ):
        publisher = KafkaPublisher(producer, topic=topic)
        publisher.publish(record)
        publisher.flush(timeout=35)

        consumer.assign([TopicPartition(topic, 0, 0)])

        message = None
        deadline = monotonic() + 15

        while message is None and monotonic() < deadline:
            message = consumer.poll(1.0)

        assert message is not None, "Kafka returned no message within 15 seconds"
        assert message.error() is None, message.error()
        assert message.topic() == topic
        assert message.partition() == 0
        assert message.offset() >= 0
        assert message.value() == payload

        assert dict(message.headers() or []) == {
            "routewatch_run_id": str(record.run_id).encode("ascii"),
            "routewatch_received_at": record.received_at.isoformat().encode("ascii"),
        }

        assert message.timestamp() == (
            TIMESTAMP_CREATE_TIME,
            int(record.received_at.timestamp() * 1000),
        )
