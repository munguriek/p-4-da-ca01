import json
import os
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from routewatch.clean_storage import (
    CleanReplayConflict,
    initialize_clean_schema,
    store_clean_result,
    store_rejected_message,
)
from routewatch.cleaner import CleanBatchResult, clean_pending_batch
from routewatch.config import load_postgres_settings
from routewatch.transform import transform_message

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("ROUTEWATCH_RUN_INTEGRATION") != "1",
        reason="Set ROUTEWATCH_RUN_INTEGRATION=1 to run database tests",
    ),
]


@pytest.fixture
def clean_db():
    settings = load_postgres_settings()
    token = uuid4().hex
    raw_schema = f"rw_raw_it_{token}"
    clean_schema = f"rw_clean_it_{token}"

    with (
        psycopg.connect(
            host=settings.host,
            port=settings.port,
            dbname=settings.dbname,
            user=settings.user,
            password=settings.password,
            autocommit=True,
            connect_timeout=5,
        ) as connection,
        connection.transaction(force_rollback=True),
    ):
        connection.execute(
            sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(raw_schema))
        )
        connection.execute(
            sql.SQL(
                "CREATE TABLE {} (LIKE routewatch_raw.messages INCLUDING ALL)"
            ).format(sql.Identifier(raw_schema, "messages"))
        )
        connection.execute(
            sql.SQL(
                """
                INSERT INTO {} (
                    kafka_cluster_id, topic, kafka_partition, kafka_offset,
                    payload, run_id, received_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
            ).format(sql.Identifier(raw_schema, "messages")),
            (
                "schema-test",
                "routewatch.ris.raw",
                0,
                0,
                b"{}",
                uuid4(),
                datetime(2026, 1, 1, tzinfo=UTC),
            ),
        )
        yield connection, raw_schema, clean_schema


def _initialize(db) -> None:
    connection, raw_schema, clean_schema = db
    initialize_clean_schema(
        connection,
        schema_name=clean_schema,
        raw_schema_name=raw_schema,
    )


def _insert_receipt(
    db, *, version=1, offset=0, status="ignored", count=0, error=None
) -> None:
    connection, _, clean_schema = db
    connection.execute(
        sql.SQL(
            """
            INSERT INTO {} (
                kafka_cluster_id, topic, kafka_partition, kafka_offset,
                transform_version, status, event_count, error_message
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
        ).format(sql.Identifier(clean_schema, "processing")),
        (
            "schema-test",
            "routewatch.ris.raw",
            0,
            offset,
            version,
            status,
            count,
            error,
        ),
    )


def test_creates_both_tables_and_can_be_rerun(clean_db) -> None:
    _initialize(clean_db)
    _initialize(clean_db)
    connection, _, clean_schema = clean_db

    rows = connection.execute(
        "SELECT tablename FROM pg_tables WHERE schemaname = %s ORDER BY tablename",
        (clean_schema,),
    ).fetchall()

    assert rows == [("prefix_events",), ("processing",)]


def test_processing_receipt_requires_an_existing_raw_message(clean_db) -> None:
    _initialize(clean_db)
    connection, _, _ = clean_db

    with (
        pytest.raises(psycopg.errors.ForeignKeyViolation),
        connection.transaction(),
    ):
        _insert_receipt(clean_db, offset=999)


def test_raw_position_and_version_define_receipt_identity(clean_db) -> None:
    _initialize(clean_db)
    connection, _, clean_schema = clean_db
    _insert_receipt(clean_db, version=1)

    with pytest.raises(psycopg.errors.UniqueViolation), connection.transaction():
        _insert_receipt(clean_db, version=1)

    _insert_receipt(clean_db, version=2)

    rows = connection.execute(
        sql.SQL("SELECT transform_version FROM {} ORDER BY transform_version").format(
            sql.Identifier(clean_schema, "processing")
        )
    ).fetchall()

    assert rows == [(1,), (2,)]


@pytest.mark.parametrize(
    ("status", "count", "error"),
    [
        ("cleaned", 0, None),
        ("ignored", 1, None),
        ("rejected", 0, None),
        ("empty", 0, "unexpected error"),
        ("rejected", 0, ""),
    ],
)
def test_rejects_inconsistent_processing_outcomes(
    clean_db, status, count, error
) -> None:
    _initialize(clean_db)
    connection, _, _ = clean_db

    with pytest.raises(psycopg.errors.CheckViolation), connection.transaction():
        _insert_receipt(clean_db, status=status, count=count, error=error)


def test_schema_creation_rolls_back_when_the_raw_table_is_missing(
    clean_db,
) -> None:
    connection, _, clean_schema = clean_db
    missing_raw_schema = f"rw_missing_it_{uuid4().hex}"
    connection.execute(
        sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(missing_raw_schema))
    )

    with pytest.raises(psycopg.errors.UndefinedTable), connection.transaction():
        initialize_clean_schema(
            connection,
            schema_name=clean_schema,
            raw_schema_name=missing_raw_schema,
        )

    assert connection.execute(
        "SELECT to_regnamespace(%s)", (clean_schema,)
    ).fetchone() == (None,)


def test_withdrawal_rows_reject_announcement_attributes(clean_db) -> None:
    _initialize(clean_db)
    connection, _, clean_schema = clean_db
    _insert_receipt(clean_db, status="cleaned", count=1)

    query = sql.SQL(
        """
        INSERT INTO {} (
            kafka_cluster_id, topic, kafka_partition, kafka_offset,
            transform_version, action_index, source_id, event_ts,
            collector_host, peer_ip, peer_asn, action_type,
            prefix, ip_version, prefix_length, next_hops,
            prefix_was_normalized, origin
        ) VALUES (
            'schema-test', 'routewatch.ris.raw', 0, 0, 1, 0,
            'source-1', TIMESTAMPTZ '2026-01-01 00:00:00+00',
            'rrc21.ripe.net', '192.0.2.1', 64496, 'WITHDRAW',
            '192.0.2.0/24', 4, 24, ARRAY[]::INET[], FALSE, %s
        )
        """
    ).format(sql.Identifier(clean_schema, "prefix_events"))

    with pytest.raises(psycopg.errors.CheckViolation), connection.transaction():
        connection.execute(query, ("IGP",))

    connection.execute(query, (None,))

    assert connection.execute(
        sql.SQL("SELECT action_type, origin FROM {}").format(
            sql.Identifier(clean_schema, "prefix_events")
        )
    ).fetchall() == [("WITHDRAW", None)]


def _prepare_result(db, message):
    connection, raw_schema, _ = db
    payload = json.dumps(message).encode("utf-8")
    connection.execute(
        sql.SQL(
            """
            INSERT INTO {} (
                kafka_cluster_id, topic, kafka_partition, kafka_offset,
                payload, run_id, received_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
        ).format(sql.Identifier(raw_schema, "messages")),
        (
            "schema-test",
            "routewatch.ris.raw",
            0,
            1,
            payload,
            uuid4(),
            datetime(2026, 11, 21, tzinfo=UTC),
        ),
    )
    return transform_message(payload)


def _write(db, result, *, version=1):
    connection, _, clean_schema = db
    return store_clean_result(
        connection,
        result,
        kafka_cluster_id="schema-test",
        topic="routewatch.ris.raw",
        partition=0,
        offset=1,
        transform_version=version,
        schema_name=clean_schema,
    )


def _counts(db):
    connection, _, clean_schema = db
    return tuple(
        connection.execute(
            sql.SQL("SELECT count(*) FROM {}").format(
                sql.Identifier(clean_schema, table)
            )
        ).fetchone()[0]
        for table in ("processing", "prefix_events")
    )


def test_stores_mixed_events_and_their_features(clean_db, announcement_message) -> None:
    _initialize(clean_db)
    data = announcement_message["data"]
    data["withdrawals"] = ["41.220.128.0/20"]
    data["announcements"][0]["prefixes"].append("41.220.144.0/20")
    result = _prepare_result(clean_db, announcement_message)

    assert _write(clean_db, result) is True
    assert _counts(clean_db) == (1, 3)

    connection, _, clean_schema = clean_db
    rows = connection.execute(
        sql.SQL(
            """
            SELECT action_index, action_type, prefix::text,
                   origin_asn, prepending_count, community_count
            FROM {} ORDER BY action_index
            """
        ).format(sql.Identifier(clean_schema, "prefix_events"))
    ).fetchall()

    assert rows == [
        (0, "WITHDRAW", "41.220.128.0/20", None, None, None),
        (1, "ANNOUNCE", "41.220.128.0/20", 36909, 2, 3),
        (2, "ANNOUNCE", "41.220.144.0/20", 36909, 2, 3),
    ]


def test_exact_replay_handles_nested_paths_and_ipv6_next_hops(
    clean_db, announcement_message
) -> None:
    _initialize(clean_db)
    data = announcement_message["data"]
    data["path"] = [8218, [327708, 37100]]
    data["community"].append([8218, 102])
    data["announcements"][0]["next_hop"] = "2001:db8::1,fe80::1"
    result = _prepare_result(clean_db, announcement_message)

    assert _write(clean_db, result) is True
    connection, _, clean_schema = clean_db
    query = sql.SQL("SELECT processed_at FROM {}").format(
        sql.Identifier(clean_schema, "processing")
    )
    before = connection.execute(query).fetchone()

    assert _write(clean_db, result) is False
    assert _counts(clean_db) == (1, 1)
    assert connection.execute(query).fetchone() == before


def test_changed_output_at_the_same_identity_raises_a_conflict(
    clean_db, announcement_message
) -> None:
    _initialize(clean_db)
    result = _prepare_result(clean_db, announcement_message)
    assert _write(clean_db, result) is True
    changed = replace(result, events=(replace(result.events[0], origin="EGP"),))

    with pytest.raises(CleanReplayConflict):
        _write(clean_db, changed)

    assert _counts(clean_db) == (1, 1)
    connection, _, clean_schema = clean_db
    assert connection.execute(
        sql.SQL("SELECT origin FROM {}").format(
            sql.Identifier(clean_schema, "prefix_events")
        )
    ).fetchall() == [("IGP",)]


def test_a_receipt_cannot_hide_missing_stored_events(
    clean_db, announcement_message
) -> None:
    _initialize(clean_db)
    result = _prepare_result(clean_db, announcement_message)
    assert _write(clean_db, result) is True
    connection, _, clean_schema = clean_db
    connection.execute(
        sql.SQL("DELETE FROM {}").format(sql.Identifier(clean_schema, "prefix_events"))
    )

    with pytest.raises(CleanReplayConflict):
        _write(clean_db, result)

    assert _counts(clean_db) == (1, 0)


def test_bad_later_event_rolls_back_the_whole_write(
    clean_db, announcement_message
) -> None:
    _initialize(clean_db)
    announcement_message["data"]["announcements"][0]["prefixes"].append(
        "41.220.144.0/20"
    )
    result = _prepare_result(clean_db, announcement_message)
    first, second = result.events
    bad_second = replace(second, action=replace(second.action, prefix_length=21))
    bad_result = replace(result, events=(first, bad_second))

    with pytest.raises(psycopg.errors.CheckViolation):
        _write(clean_db, bad_result)

    assert _counts(clean_db) == (0, 0)
    assert _write(clean_db, result) is True
    assert _counts(clean_db) == (1, 2)


@pytest.mark.parametrize("status", ["empty", "ignored"])
def test_no_event_outcomes_are_stored_and_replayable(
    clean_db, announcement_message, status
) -> None:
    _initialize(clean_db)
    if status == "empty":
        announcement_message["data"]["announcements"] = []
        announcement_message["data"]["withdrawals"] = []
        message = announcement_message
    else:
        message = {"type": "pong"}
    result = _prepare_result(clean_db, message)

    assert result.status == status
    assert _write(clean_db, result) is True
    assert _write(clean_db, result) is False
    assert _counts(clean_db) == (1, 0)

    other_status = "ignored" if status == "empty" else "empty"
    with pytest.raises(CleanReplayConflict):
        _write(clean_db, replace(result, status=other_status))


@pytest.mark.parametrize("attributes_present", [False, True])
def test_storage_preserves_missing_versus_empty_attributes(
    clean_db, announcement_message, attributes_present
) -> None:
    _initialize(clean_db)
    data = announcement_message["data"]
    if attributes_present:
        data["path"] = []
        data["community"] = []
    else:
        data.pop("path")
        data.pop("community")
    result = _prepare_result(clean_db, announcement_message)

    assert _write(clean_db, result) is True
    connection, _, clean_schema = clean_db
    row = connection.execute(
        sql.SQL(
            "SELECT as_path, as_path_length, communities, community_count FROM {}"
        ).format(sql.Identifier(clean_schema, "prefix_events"))
    ).fetchone()

    expected = ([], 0, [], 0) if attributes_present else (None, None, None, None)
    assert row == expected


def test_different_versions_can_store_the_same_raw_position(
    clean_db, announcement_message
) -> None:
    _initialize(clean_db)
    result = _prepare_result(clean_db, announcement_message)

    assert _write(clean_db, result, version=1) is True
    assert _write(clean_db, result, version=2) is True
    assert _write(clean_db, result, version=1) is False
    assert _write(clean_db, result, version=2) is False
    assert _counts(clean_db) == (2, 2)


_BATCH_TOPIC = "routewatch.batch.raw"


def _seed_batch_message(db, payload, *, offset, partition=0):
    connection, raw_schema, _ = db
    connection.execute(
        sql.SQL(
            """
            INSERT INTO {} (
                kafka_cluster_id, topic, kafka_partition, kafka_offset,
                payload, run_id, received_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
        ).format(sql.Identifier(raw_schema, "messages")),
        (
            "batch-cluster",
            _BATCH_TOPIC,
            partition,
            offset,
            payload,
            uuid4(),
            datetime(2026, 11, 21, tzinfo=UTC),
        ),
    )


def _run_batch(db, *, size=1000, version=1):
    connection, raw_schema, clean_schema = db
    return clean_pending_batch(
        connection,
        topic=_BATCH_TOPIC,
        batch_size=size,
        transform_version=version,
        schema_name=clean_schema,
        raw_schema_name=raw_schema,
    )


def test_rejection_receipts_are_replay_safe(clean_db, announcement_message) -> None:
    _initialize(clean_db)
    result = _prepare_result(clean_db, announcement_message)
    connection, _, clean_schema = clean_db
    position = {
        "kafka_cluster_id": "schema-test",
        "topic": "routewatch.ris.raw",
        "partition": 0,
        "offset": 1,
        "schema_name": clean_schema,
    }
    reason = "InvalidRISMessage: test rejection"

    assert store_rejected_message(connection, reason, **position) is True
    assert store_rejected_message(connection, reason, **position) is False

    with pytest.raises(CleanReplayConflict):
        store_rejected_message(connection, "different reason", **position)
    with pytest.raises(CleanReplayConflict):
        _write(clean_db, result)

    assert _counts(clean_db) == (1, 0)


def test_batches_are_bounded_and_skip_processed_messages(
    clean_db, announcement_message
) -> None:
    _initialize(clean_db)
    payload = json.dumps(announcement_message).encode("utf-8")
    for offset in range(3):
        _seed_batch_message(clean_db, payload, offset=offset)

    assert _run_batch(clean_db, size=2) == CleanBatchResult(2, 2, 2, 0)
    assert _run_batch(clean_db, size=2) == CleanBatchResult(1, 1, 1, 0)
    assert _run_batch(clean_db, size=2) == CleanBatchResult(0, 0, 0, 0)
    assert _counts(clean_db) == (3, 3)


def test_bad_payload_is_recorded_and_does_not_block_later_messages(
    clean_db, announcement_message
) -> None:
    _initialize(clean_db)
    payload = json.dumps(announcement_message).encode("utf-8")
    _seed_batch_message(clean_db, payload, offset=0)
    _seed_batch_message(clean_db, b"not-json", offset=1)
    _seed_batch_message(clean_db, payload, offset=2)

    assert _run_batch(clean_db) == CleanBatchResult(3, 3, 2, 1)
    assert _run_batch(clean_db) == CleanBatchResult(0, 0, 0, 0)

    connection, raw_schema, clean_schema = clean_db
    rows = connection.execute(
        sql.SQL(
            "SELECT kafka_offset, status, error_message FROM {} ORDER BY kafka_offset"
        ).format(sql.Identifier(clean_schema, "processing"))
    ).fetchall()

    assert [(offset, status) for offset, status, _ in rows] == [
        (0, "cleaned"),
        (1, "rejected"),
        (2, "cleaned"),
    ]
    assert rows[1][2].startswith("InvalidRISMessage:")
    assert connection.execute(
        sql.SQL("SELECT payload FROM {} WHERE topic = %s AND kafka_offset = 1").format(
            sql.Identifier(raw_schema, "messages")
        ),
        (_BATCH_TOPIC,),
    ).fetchone() == (b"not-json",)


def test_empty_and_ignored_messages_are_marked_processed(
    clean_db, announcement_message
) -> None:
    _initialize(clean_db)
    announcement_message["data"]["announcements"] = []
    announcement_message["data"]["withdrawals"] = []
    _seed_batch_message(
        clean_db, json.dumps(announcement_message).encode("utf-8"), offset=0
    )
    _seed_batch_message(clean_db, b'{"type":"pong"}', offset=1)

    assert _run_batch(clean_db) == CleanBatchResult(2, 2, 0, 0)
    assert _run_batch(clean_db) == CleanBatchResult(0, 0, 0, 0)
    assert _counts(clean_db) == (2, 0)


def test_new_partitions_with_lower_offsets_are_discovered(
    clean_db, announcement_message
) -> None:
    _initialize(clean_db)
    payload = json.dumps(announcement_message).encode("utf-8")
    _seed_batch_message(clean_db, payload, partition=1, offset=100)
    assert _run_batch(clean_db) == CleanBatchResult(1, 1, 1, 0)

    _seed_batch_message(clean_db, payload, partition=0, offset=0)
    assert _run_batch(clean_db) == CleanBatchResult(1, 1, 1, 0)
    assert _counts(clean_db) == (2, 2)


def test_each_transformation_version_has_its_own_pending_messages(
    clean_db, announcement_message
) -> None:
    _initialize(clean_db)
    _seed_batch_message(
        clean_db, json.dumps(announcement_message).encode("utf-8"), offset=0
    )

    assert _run_batch(clean_db, version=1) == CleanBatchResult(1, 1, 1, 0)
    assert _run_batch(clean_db, version=2) == CleanBatchResult(1, 1, 1, 0)
    assert _run_batch(clean_db, version=1) == CleanBatchResult(0, 0, 0, 0)
    assert _counts(clean_db) == (2, 2)


def test_database_failure_rolls_back_the_whole_batch(
    clean_db, announcement_message
) -> None:
    _initialize(clean_db)
    connection, _, clean_schema = clean_db
    connection.execute(
        sql.SQL(
            "ALTER TABLE {} ADD CONSTRAINT fail_for_test "
            "CHECK (source_id <> 'db-blocked')"
        ).format(sql.Identifier(clean_schema, "prefix_events"))
    )
    _seed_batch_message(
        clean_db, json.dumps(announcement_message).encode("utf-8"), offset=0
    )
    bad = deepcopy(announcement_message)
    bad["data"]["id"] = "db-blocked"
    _seed_batch_message(clean_db, json.dumps(bad).encode("utf-8"), offset=1)

    with pytest.raises(psycopg.errors.CheckViolation):
        _run_batch(clean_db)

    assert _counts(clean_db) == (0, 0)

    connection.execute(
        sql.SQL("ALTER TABLE {} DROP CONSTRAINT fail_for_test").format(
            sql.Identifier(clean_schema, "prefix_events")
        )
    )
    assert _run_batch(clean_db) == CleanBatchResult(2, 2, 2, 0)
