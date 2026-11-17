"""Kafka Messages as rows for raw.otel"""

import hashlib
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
