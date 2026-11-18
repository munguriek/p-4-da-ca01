"""Kafka Messages as rows for raw.otel"""

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol
from uuid import UUID

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

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
    """One row of raw.otel, with it's fields in column order"""
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
    text = body.decode("utf-8")
    try:
        json.loads(text)
    except json.JSONDecodeError as error:
        raise InvalidPayload("not json") from error
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

class InvalidPayload(Exception):
    """A message that raw.otel cannot store which will go to the quarantine instead"""
