import hashlib
import json
import time
from datetime import UTC, datetime

import pytest
from confluent_kafka import Consumer, Producer

from airdeall.raw import RawRow
from airdeall.settings import KAFKA_BOOTSTRAP_SERVERS, sink_consumer_config
from airdeall.sink import RawSink, Reject, write_batch

pytestmark = pytest.mark.integration

NOON_9_NOVEMBER = datetime(2026, 11, 9, 12, tzinfo=UTC)


def raw_row(offset, ts, run_id):
    body = json.dumps({"n": offset}).encode("utf-8")
    return RawRow(
        topic="otel.spans",
        kpartition=0,
        koffset=offset,
        kafka_timestamp=ts,
        payload=body.decode("utf-8"),
        payload_sha256=hashlib.sha256(body).digest(),
        payload_bytes=len(body),
        run_id=run_id,
    )


def produce_json(topic, count):
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    for n in range(count):
        producer.produce(topic, json.dumps({"n": n}).encode("utf-8"))
        producer.poll(0)
    assert producer.flush(30) == 0


def drain(sink, expected, seconds=60):
    """Run the sink until it has read `expected` messages or the time runs out"""
    seen = 0
    deadline = time.monotonic() + seconds
    while seen < expected and time.monotonic() < deadline:
        counts = sink.run_once()
        if counts is not None:
            seen += counts.rows_in
    return seen


def offsets_in_raw(conn, topic):
    rows = conn.execute(
        "SELECT koffset FROM raw.otel WHERE topic = %s ORDER BY koffset", (topic,)
    ).fetchall()
    return [offset for (offset,) in rows]


def test_inserting_the_same_batch_twice_adds_nothing(migrated_db, run_id):
    rows = [raw_row(n, NOON_9_NOVEMBER, run_id) for n in range(3)]

    first = write_batch(migrated_db, rows, [], run_id)
    second = write_batch(migrated_db, rows, [], run_id)

    assert (first.inserted, first.duplicate) == (3, 0)
    assert (second.inserted, second.duplicate) == (0, 3)
    assert migrated_db.execute("SELECT count(*) FROM raw.otel").fetchone()[0] == 3


def test_batch_spanning_midnight_creates_both_partitions(migrated_db, run_id):
    before = raw_row(0, datetime(2026, 11, 9, 23, 59, 59, 999000, tzinfo=UTC), run_id)
    after = raw_row(1, datetime(2026, 11, 10, tzinfo=UTC), run_id)

    write_batch(migrated_db, [before, after], [], run_id)

    placed = migrated_db.execute(
        "SELECT tableoid::regclass::text, koffset FROM raw.otel ORDER BY koffset"
    ).fetchall()
    assert placed == [("raw.otel_20261109", 0), ("raw.otel_20261110", 1)]


def test_reject_is_quarantined_once(migrated_db, run_id):
    reject = Reject("otel.spans", 0, 7, "not json", b"not json")

    first = write_batch(migrated_db, [], [reject], run_id)
    second = write_batch(migrated_db, [], [reject], run_id)

    assert (first.quarantined, first.duplicate) == (1, 0)
    assert (second.quarantined, second.duplicate) == (0, 1)
    stored = migrated_db.execute(
        "SELECT source, reason, payload FROM control.quarantine"
    ).fetchall()
    assert stored == [("otel.demo", "not json", b"not json")]


def test_consumer_writes_every_message_once(migrated_db, fresh_topic, run_id):
    produce_json(fresh_topic, 1000)
    consumer = Consumer(sink_consumer_config(group_id=f"{fresh_topic}-sink"))
    consumer.subscribe([fresh_topic])
    try:
        drain(RawSink(consumer, migrated_db, run_id), expected=1000)
    finally:
        consumer.close()

    assert offsets_in_raw(migrated_db, fresh_topic) == list(range(1000))
