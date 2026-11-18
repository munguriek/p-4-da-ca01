"""Kafka Messages as rows for raw.otel"""

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from typing import NamedTuple, Protocol
from uuid import UUID

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
LONE_SURROGATE = re.compile("[\ud800-\udfff]")


class KafkaMessage(Protocol):
    """
    The parts of a message that the raw sink reads.
    ... ellipsis. It acts as a placeholder for the method body
    https://typing.python.org/en/latest/spec/protocol.html
    """

    def topic(self) -> str: ...
    def partition(self) -> int: ...
    def offset(self) -> int: ...
    def timestamp(self) -> tuple[int, int]: ...
    def value(self) -> bytes | None: ...


@dataclass(frozen=True)
class RawRow:
    """One row of raw.otel, with its fields in column order"""

    topic: str
    kpartition: int
    koffset: int
    kafka_timestamp: datetime
    payload: str
    payload_sha256: bytes
    payload_bytes: int
    run_id: UUID


def to_raw_row(msg: KafkaMessage, run_id: UUID) -> RawRow:
    """The raw.otel row for one Kafka message. Raises InvalidPayload if it cannot be stored"""
    body = msg.value() or b""
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as error:
        raise InvalidPayload("not utf-8") from error
    try:
        document = json.loads(text, parse_constant=refuse_constant)
    except ValueError as error:
        raise InvalidPayload("not json") from error
    check_strings(document)
    _, milliseconds = msg.timestamp()
    return RawRow(
        topic=msg.topic(),
        kpartition=msg.partition(),
        koffset=msg.offset(),
        kafka_timestamp=EPOCH + timedelta(milliseconds=milliseconds),
        payload=text,
        payload_sha256=hashlib.sha256(body).digest(),
        payload_bytes=len(body),
        run_id=run_id,
    )


class DayPartition(NamedTuple):
    """A daily partition of raw.otel: name and UTC range it holds"""

    name: str
    start: datetime
    end: datetime


def day_partition(ts: datetime) -> DayPartition:
    """The raw.otel partition that holds ts: one per UTC day, named otel_YYYYMMDD"""
    if ts.tzinfo is None:
        raise ValueError("the timestamp must carry a time zone")
    start = datetime.combine(ts.astimezone(UTC).date(), time(), tzinfo=UTC)
    return DayPartition(f"otel_{start:%Y%m%d}", start, start + timedelta(days=1))


def refuse_constant(name: str) -> None:
    """NaN and Infinity are not JSON, though Python's json module accepts them"""
    raise ValueError(f"{name} is not a JSON value")


def check_strings(value: object) -> None:
    """Raise InvalidPayload if jsonb would refuse a string, key or value, in value."""
    if isinstance(value, str):
        if "\x00" in value:
            raise InvalidPayload("nul escape")
        if LONE_SURROGATE.search(value):
            raise InvalidPayload("lone surrogate")
    elif isinstance(value, dict):
        for key, item in value.items():
            check_strings(key)
            check_strings(item)
    elif isinstance(value, list):
        for item in value:
            check_strings(item)


class InvalidPayload(Exception):
    """A message that raw.otel cannot store which will go to the quarantine instead"""
