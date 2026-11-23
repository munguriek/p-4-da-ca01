from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock
from uuid import UUID

import psycopg
import pytest
from confluent_kafka import Consumer, KafkaError, KafkaException

from routewatch import worker
from routewatch.capture import capture_message
from routewatch.raw import RawRecord


@pytest.fixture
def records() -> list[RawRecord]:
    captured = capture_message(
        b'{"id":"event-1"}',
        run_id=UUID(int=1),
        received_at=datetime(2026, 11, 20, 17, 0, tzinfo=UTC),
    )

    first = RawRecord(
        kafka_cluster_id="cluster-a",
        topic="routewatch.ris.raw",
        partition=0,
        offset=42,
        captured=captured,
    )

    return [
        first,
        replace(first, partition=1, offset=7),
        replace(first, offset=41),
        replace(first, topic="routewatch.other", offset=3),
    ]


@pytest.fixture
def flow(monkeypatch: pytest.MonkeyPatch):
    events = []
    connection = Mock(spec=psycopg.Connection)
    consumer = Mock(spec=Consumer)
    transaction = MagicMock()

    transaction.__enter__.side_effect = lambda: events.append("db_begin")

    def leave_transaction(error_type, _error, _traceback):
        events.append("db_commit" if error_type is None else "db_rollback")
        return False

    transaction.__exit__.side_effect = leave_transaction
    connection.transaction.return_value = transaction

    def write_record(*_args, **_kwargs):
        events.append("write")
        return True

    writer = Mock(side_effect=write_record)
    monkeypatch.setattr(worker, "store_raw_record", writer)

    def commit_offsets(**kwargs):
        events.append("kafka_commit")
        return kwargs["offsets"]

    consumer.commit.side_effect = commit_offsets

    return connection, consumer, writer, events


def test_commits_database_before_kafka_and_tracks_each_partition(
    flow,
    records,
) -> None:
    connection, consumer, writer, events = flow

    inserted = worker.persist_batch(
        connection,
        consumer,
        records,
        schema="unit_raw",
    )

    assert inserted == 4
    assert events == [
        "db_begin",
        "write",
        "write",
        "write",
        "write",
        "db_commit",
        "kafka_commit",
    ]

    connection.transaction.assert_called_once()
    consumer.commit.assert_called_once()

    assert [call.args[1] for call in writer.call_args_list] == records
    assert all(
        call.args[0] is connection and call.kwargs["schema"] == "unit_raw"
        for call in writer.call_args_list
    )

    arguments = consumer.commit.call_args.kwargs

    assert arguments["asynchronous"] is False
    assert {
        (position.topic, position.partition, position.offset)
        for position in arguments["offsets"]
    } == {
        ("routewatch.ris.raw", 0, 43),
        ("routewatch.ris.raw", 1, 8),
        ("routewatch.other", 0, 4),
    }


def test_replayed_rows_still_advance_kafka_offsets(flow, records) -> None:
    connection, consumer, writer, _events = flow
    writer.side_effect = [False, True, False, False]

    assert worker.persist_batch(connection, consumer, records) == 1

    offsets = consumer.commit.call_args.kwargs["offsets"]

    assert {
        (position.topic, position.partition, position.offset) for position in offsets
    } == {
        ("routewatch.ris.raw", 0, 43),
        ("routewatch.ris.raw", 1, 8),
        ("routewatch.other", 0, 4),
    }


def test_database_write_failure_prevents_kafka_commit(flow, records) -> None:
    connection, consumer, writer, events = flow
    writer.side_effect = [True, psycopg.OperationalError("write failed")]

    with pytest.raises(psycopg.OperationalError, match="write failed"):
        worker.persist_batch(connection, consumer, records)

    assert events == ["db_begin", "db_rollback"]
    consumer.commit.assert_not_called()


def test_database_commit_failure_prevents_kafka_commit(flow, records) -> None:
    connection, consumer, _writer, events = flow

    def fail_commit(*_args):
        events.append("db_commit_failed")
        raise psycopg.OperationalError("commit failed")

    connection.transaction.return_value.__exit__.side_effect = fail_commit

    with pytest.raises(psycopg.OperationalError, match="commit failed"):
        worker.persist_batch(connection, consumer, records)

    assert events[-1] == "db_commit_failed"
    consumer.commit.assert_not_called()


def test_kafka_commit_failure_is_reported_after_database_commit(
    flow,
    records,
) -> None:
    connection, consumer, _writer, events = flow
    consumer.commit.side_effect = KafkaException(KafkaError(KafkaError._TIMED_OUT))

    with pytest.raises(KafkaException):
        worker.persist_batch(connection, consumer, records)

    assert events[-1] == "db_commit"
    consumer.commit.assert_called_once()


def test_partition_commit_error_is_reported(flow, records) -> None:
    connection, consumer, _writer, events = flow
    error = KafkaError(KafkaError.GROUP_AUTHORIZATION_FAILED)

    consumer.commit.side_effect = None
    consumer.commit.return_value = [SimpleNamespace(error=error)]

    with pytest.raises(KafkaException):
        worker.persist_batch(connection, consumer, records)

    assert events[-1] == "db_commit"


@pytest.mark.parametrize("result", [None, []])
def test_missing_commit_results_are_reported(flow, records, result) -> None:
    connection, consumer, _writer, _events = flow

    consumer.commit.side_effect = None
    consumer.commit.return_value = result

    with pytest.raises(RuntimeError, match="no commit results"):
        worker.persist_batch(connection, consumer, records)


def test_empty_batch_does_no_work(flow) -> None:
    connection, consumer, writer, events = flow

    assert worker.persist_batch(connection, consumer, []) == 0

    assert events == []
    connection.transaction.assert_not_called()
    writer.assert_not_called()
    consumer.commit.assert_not_called()


def test_batch_cannot_mix_kafka_clusters(flow, records) -> None:
    connection, consumer, writer, events = flow
    mixed = [
        records[0],
        replace(records[1], kafka_cluster_id="cluster-b"),
    ]

    with pytest.raises(ValueError, match="one Kafka cluster"):
        worker.persist_batch(connection, consumer, mixed)

    assert events == []
    writer.assert_not_called()
    consumer.commit.assert_not_called()
