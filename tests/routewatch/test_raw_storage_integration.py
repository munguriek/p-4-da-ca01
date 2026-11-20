from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from airdeall.routewatch.capture import capture_message
from airdeall.routewatch.config import load_postgres_settings
from airdeall.routewatch.db import connect_postgres
from airdeall.routewatch.raw import (
    RawRecord,
    RawReplayConflict,
    initialize_raw_storage,
    store_raw_record,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def database() -> Iterator[tuple[psycopg.Connection, str]]:
    with connect_postgres(load_postgres_settings()) as connection:
        schema = f"routewatch_test_{uuid4().hex}"

        try:
            yield connection, schema
        finally:
            connection.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema)
                )
            )


@pytest.fixture
def record() -> RawRecord:
    captured = capture_message(
        b'{"id":"event-1"}  ',
        run_id=uuid4(),
        received_at=datetime.now(UTC),
    )

    return RawRecord(
        kafka_cluster_id="cluster-a",
        topic="routewatch.ris.raw",
        partition=0,
        offset=42,
        captured=captured,
        kafka_timestamp_ms=int(captured.received_at.timestamp() * 1000),
    )


def read_rows(connection: psycopg.Connection, schema: str) -> list[tuple]:
    return connection.execute(
        sql.SQL(
            """
            SELECT kafka_cluster_id, topic, kafka_partition, kafka_offset,
                   payload, run_id, received_at, kafka_timestamp_ms
            FROM {}
            """
        ).format(sql.Identifier(schema, "messages"))
    ).fetchall()


def test_initialization_can_run_twice(database) -> None:
    connection, schema = database

    initialize_raw_storage(connection, schema=schema)
    initialize_raw_storage(connection, schema=schema)

    assert read_rows(connection, schema) == []


@pytest.mark.parametrize(
    "payload",
    [b'{"id":"event-1"}  ', b"\xffnot-json\x00", b""],
    ids=["json-and-whitespace", "binary-and-null-byte", "empty-payload"],
)
def test_preserves_payload_and_metadata(database, record, payload) -> None:
    connection, schema = database
    initialize_raw_storage(connection, schema=schema)

    record = replace(
        record,
        captured=replace(record.captured, payload=payload),
    )

    with connection.transaction():
        assert store_raw_record(connection, record, schema=schema) is True

    assert read_rows(connection, schema) == [
        (
            record.kafka_cluster_id,
            record.topic,
            record.partition,
            record.offset,
            payload,
            record.captured.run_id,
            record.captured.received_at,
            record.kafka_timestamp_ms,
        )
    ]


def test_replay_after_commit_keeps_one_row(database, record) -> None:
    connection, schema = database
    initialize_raw_storage(connection, schema=schema)

    with connection.transaction():
        assert store_raw_record(connection, record, schema=schema) is True

    with connection.transaction():
        assert store_raw_record(connection, record, schema=schema) is False

    assert len(read_rows(connection, schema)) == 1


@pytest.mark.parametrize(
    "change",
    [
        {"offset": 43},
        {"partition": 1},
        {"topic": "routewatch.other"},
        {"kafka_cluster_id": "cluster-b"},
    ],
    ids=["offset", "partition", "topic", "cluster"],
)
def test_distinct_kafka_positions_remain_separate(
    database,
    record,
    change,
) -> None:
    connection, schema = database
    initialize_raw_storage(connection, schema=schema)

    with connection.transaction():
        assert store_raw_record(connection, record, schema=schema) is True

        assert (
            store_raw_record(
                connection,
                replace(record, **change),
                schema=schema,
            )
            is True
        )

    assert len(read_rows(connection, schema)) == 2


def test_conflicting_replay_is_reported(database, record) -> None:
    connection, schema = database
    initialize_raw_storage(connection, schema=schema)

    with connection.transaction():
        store_raw_record(connection, record, schema=schema)

    changed = replace(
        record,
        captured=replace(record.captured, payload=b"changed"),
    )

    with (
        pytest.raises(RawReplayConflict, match="Kafka position"),
        connection.transaction(),
    ):
        store_raw_record(connection, changed, schema=schema)

    assert len(read_rows(connection, schema)) == 1


def test_failed_batch_rolls_back_all_its_rows(database, record) -> None:
    connection, schema = database
    initialize_raw_storage(connection, schema=schema)

    invalid = replace(record, partition=-1, offset=43)

    with (
        pytest.raises(psycopg.errors.CheckViolation),
        connection.transaction(),
    ):
        store_raw_record(connection, record, schema=schema)
        store_raw_record(connection, invalid, schema=schema)

    assert read_rows(connection, schema) == []


def test_writer_requires_an_explicit_transaction(database, record) -> None:
    connection, schema = database
    initialize_raw_storage(connection, schema=schema)

    with pytest.raises(RuntimeError, match="active transaction"):
        store_raw_record(connection, record, schema=schema)

    assert read_rows(connection, schema) == []
