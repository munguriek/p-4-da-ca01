"""The raw sink: copies Kafka messages into raw.otel, each exactly once.

Each batch is written in one database transaction, and only then are its Kafka
offsets committed. A crash between the two makes Kafka deliver the batch again,
and the primary key of raw.otel turns that second delivery into a no-op
"""

import logging
from collections.abc import Callable, Sequence
from dataclasses import astuple, dataclass
from uuid import UUID

import psycopg
from confluent_kafka import Consumer, KafkaException
from psycopg import sql

from airdeall.raw import InvalidPayload, RawRow, day_partition, to_raw_row

log = logging.getLogger(__name__)

SOURCE = "otel.demo"  # the source id for quarantine rows
BATCH_SIZE = 500
POLL_TIMEOUT_SECONDS = 1.0
RAW_COLUMNS = sql.SQL(", ").join(
    sql.Identifier(name)
    for name in (
        "topic",
        "kpartition",
        "koffset",
        "kafka_timestamp",
        "payload",
        "payload_sha256",
        "payload_bytes",
        "run_id",
    )
)


@dataclass(frozen=True)
class Reject:
    """A message that went to quarantine, and why"""

    topic: str
    kpartition: int
    koffset: int
    reason: str
    payload: bytes


@dataclass(frozen=True)
class BatchCounts:
    """What happened to one batch: rows_in = inserted + duplicate + quarantined"""

    rows_in: int
    inserted: int
    duplicate: int
    quarantined: int


def start_run(conn: psycopg.Connection, job: str) -> UUID:
    """Create this run's row in control.run_log and return its id"""
    row = conn.execute(
        "INSERT INTO control.run_log (job) VALUES (%s) RETURNING run_id", (job,)
    ).fetchone()
    return row[0]


def finish_run(
    conn: psycopg.Connection, run_id: UUID, status: str, note: str | None = None
) -> None:
    """Mark the run finished or failed."""
    conn.execute(
        "UPDATE control.run_log SET finished_at = now(), status = %s, note = %s"
        " WHERE run_id = %s",
        (status, note, run_id),
    )


def write_batch(
    conn: psycopg.Connection,
    rows: Sequence[RawRow],
    rejects: Sequence[Reject],
    run_id: UUID,
) -> BatchCounts:
    """Store one batch in a single transaction, and add its counts to the run log"""
    with conn.transaction():
        inserted = insert_rows(conn, rows) if rows else 0
        quarantined = quarantine(conn, rejects)
        rows_in = len(rows) + len(rejects)
        counts = BatchCounts(
            rows_in=rows_in,
            inserted=inserted,
            duplicate=rows_in - inserted - quarantined,
            quarantined=quarantined,
        )
        conn.execute(
            "UPDATE control.run_log SET rows_in = rows_in + %s,"
            " rows_inserted = rows_inserted + %s,"
            " rows_duplicate = rows_duplicate + %s,"
            " rows_quarantined = rows_quarantined + %s"
            " WHERE run_id = %s",
            (*astuple(counts), run_id),
        )
    return counts


def insert_rows(conn: psycopg.Connection, rows: Sequence[RawRow]) -> int:
    """Insert the rows not already in raw.otel; return how many were new"""
    for partition in sorted({day_partition(row.kafka_timestamp) for row in rows}):
        conn.execute(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {} PARTITION OF raw.otel"
                " FOR VALUES FROM ({}) TO ({})"
            ).format(
                sql.Identifier("raw", partition.name),
                sql.Literal(partition.start),
                sql.Literal(partition.end),
            )
        )
    conn.execute(
        "CREATE TEMP TABLE staging (LIKE raw.otel INCLUDING DEFAULTS) ON COMMIT DROP"
    )
    with conn.cursor() as cur:
        copy_sql = sql.SQL("COPY staging ({}) FROM STDIN").format(RAW_COLUMNS)
        with cur.copy(copy_sql) as copy:
            for row in rows:
                copy.write_row(astuple(row))
        cur.execute(
            sql.SQL(
                "INSERT INTO raw.otel ({0}) SELECT {0} FROM staging"
                " ON CONFLICT DO NOTHING"
            ).format(RAW_COLUMNS)
        )
        return cur.rowcount


def quarantine(conn: psycopg.Connection, rejects: Sequence[Reject]) -> int:
    """Quarantine the rejects not already there; return how many were new"""
    added = 0
    for reject in rejects:
        cur = conn.execute(
            "INSERT INTO control.quarantine"
            " (source, topic, kpartition, koffset, reason, payload)"
            " SELECT %s, %s, %s, %s, %s, %s WHERE NOT EXISTS ("
            " SELECT 1 FROM control.quarantine"
            " WHERE topic = %s AND kpartition = %s AND koffset = %s)",
            (
                SOURCE,
                *astuple(reject),
                reject.topic,
                reject.kpartition,
                reject.koffset,
            ),
        )
        added += cur.rowcount
    return added


class RawSink:
    """Reads batches from Kafka into raw.otel, committing offsets only afterwards"""

    def __init__(
        self,
        consumer: Consumer,
        conn: psycopg.Connection,
        run_id: UUID,
        batch_size: int = BATCH_SIZE,
        timeout: float = POLL_TIMEOUT_SECONDS,
        after_db_commit: Callable[[], None] | None = None,
    ) -> None:
        self.consumer = consumer
        self.conn = conn
        self.run_id = run_id
        self.batch_size = batch_size
        self.timeout = timeout
        self.after_db_commit = after_db_commit  # a test hook; None in production

    def run_once(self) -> BatchCounts | None:
        """Read, store and commit one batch; return its counts, or None if idle."""
        rows: list[RawRow] = []
        rejects: list[Reject] = []
        for msg in self.consumer.consume(self.batch_size, self.timeout):
            error = msg.error()
            if error is not None:
                if error.fatal():
                    raise KafkaException(error)
                log.warning("kafka: %s", error)
                continue
            try:
                rows.append(to_raw_row(msg, self.run_id))
            except InvalidPayload as reason:
                rejects.append(
                    Reject(
                        msg.topic(),
                        msg.partition(),
                        msg.offset(),
                        str(reason),
                        msg.value() or b"",
                    )
                )
        if not rows and not rejects:
            return None
        counts = write_batch(self.conn, rows, rejects, self.run_id)
        if self.after_db_commit is not None:
            self.after_db_commit()
        self.consumer.commit(asynchronous=False)
        log.info(
            "batch: %d in, %d inserted, %d duplicate, %d quarantined",
            *astuple(counts),
        )
        return counts

    def run(self, stop: Callable[[], bool]) -> None:
        """Run batches until stop() returns True; the current batch always finishes."""
        while not stop():
            self.run_once()
