from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import psycopg
import pytest
from confluent_kafka import KafkaError, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from psycopg import sql

from routewatch import consumer as raw_consumer
from routewatch import worker
from routewatch.capture import capture_message
from routewatch.config import (
    load_kafka_settings,
    load_postgres_settings,
)
from routewatch.db import connect_postgres
from routewatch.publisher import KafkaPublisher, create_producer
from routewatch.raw import initialize_raw_storage

pytestmark = pytest.mark.integration


@pytest.fixture
def pipeline(monkeypatch: pytest.MonkeyPatch):
    groups = set()
    real_factory = raw_consumer.create_consumer

    def tracked_consumer(settings, *, group_id):
        client = real_factory(settings, group_id=group_id)
        groups.add(group_id)
        return client

    monkeypatch.setattr(raw_consumer, "create_consumer", tracked_consumer)

    token = uuid4().hex
    topic = f"routewatch.test.{token}"
    schema = f"routewatch_test_{token}"
    group = f"routewatch.test.{token}"

    kafka = replace(load_kafka_settings(), topic=topic)
    postgres = load_postgres_settings()
    admin = AdminClient({"bootstrap.servers": kafka.bootstrap_servers})

    admin.create_topics(
        [NewTopic(topic, num_partitions=1, replication_factor=1)],
        operation_timeout=10,
        request_timeout=15,
    )[topic].result(timeout=20)

    try:
        with (
            connect_postgres(postgres) as connection,
            create_producer(kafka) as producer,
        ):
            initialize_raw_storage(connection, schema=schema)

            try:
                yield SimpleNamespace(
                    kafka=kafka,
                    postgres=postgres,
                    producer=producer,
                    connection=connection,
                    topic=topic,
                    schema=schema,
                    group=group,
                )
            finally:
                connection.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                        sql.Identifier(schema)
                    )
                )
    finally:
        for group_id in sorted(groups):
            try:
                admin.delete_consumer_groups([group_id], request_timeout=10)[
                    group_id
                ].result(timeout=15)
            except KafkaException as error:
                if error.args[0].code() != KafkaError.GROUP_ID_NOT_FOUND:
                    raise

        admin.delete_topics([topic], operation_timeout=10, request_timeout=15)[
            topic
        ].result(timeout=20)


def read_rows(pipeline):
    return pipeline.connection.execute(
        sql.SQL(
            "SELECT payload, run_id, received_at FROM {} ORDER BY kafka_offset"
        ).format(sql.Identifier(pipeline.schema, "messages"))
    ).fetchall()


def committed_offset(pipeline, group):
    with raw_consumer.create_consumer(pipeline.kafka, group_id=group) as reader:
        position = reader.committed([TopicPartition(pipeline.topic, 0)], timeout=10)[0]

        assert position.error is None, position.error
        return position.offset


def publish(pipeline, payload):
    captured = capture_message(
        payload,
        run_id=uuid4(),
        received_at=datetime.now(UTC),
    )

    publisher = KafkaPublisher(pipeline.producer, topic=pipeline.topic)
    publisher.publish(captured)
    publisher.flush(timeout=35)

    return captured


def run(pipeline, group=None):
    return raw_consumer.run_raw_worker(
        pipeline.kafka,
        pipeline.postgres,
        group_id=group or pipeline.group,
        max_messages=1,
        idle_timeout=20,
        schema=pipeline.schema,
    )


@pytest.mark.parametrize(
    "payload",
    [b'{"id":"test-event"}  ', b"\xffnot-json\x00"],
    ids=["json-and-whitespace", "binary-payload"],
)
def test_kafka_to_postgres_and_replay(pipeline, payload) -> None:
    captured = publish(pipeline, payload)

    assert run(pipeline) == (1, 1)
    assert read_rows(pipeline) == [(payload, captured.run_id, captured.received_at)]
    assert committed_offset(pipeline, pipeline.group) == 1

    replay_group = pipeline.group + ".replay"

    assert run(pipeline, replay_group) == (1, 0)
    assert len(read_rows(pipeline)) == 1
    assert committed_offset(pipeline, replay_group) == 1


def test_database_failure_leaves_offset_uncommitted(
    pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publish(pipeline, b'{"id":"test-event"}')
    real_writer = worker.store_raw_record

    def fail_after_insert(connection, record, *, schema):
        real_writer(connection, record, schema=schema)
        raise psycopg.OperationalError("simulated write failure")

    monkeypatch.setattr(worker, "store_raw_record", fail_after_insert)

    with pytest.raises(psycopg.OperationalError, match="simulated write failure"):
        run(pipeline)

    assert read_rows(pipeline) == []
    assert committed_offset(pipeline, pipeline.group) < 0


def test_missing_capture_metadata_leaves_offset_uncommitted(pipeline) -> None:
    pipeline.producer.produce(
        pipeline.topic,
        value=b'{"id":"test-event"}',
        headers=[("routewatch_run_id", str(uuid4()).encode("ascii"))],
    )
    assert pipeline.producer.flush(35) == 0

    with pytest.raises(ValueError, match="routewatch_received_at"):
        run(pipeline)

    assert read_rows(pipeline) == []
    assert committed_offset(pipeline, pipeline.group) < 0
