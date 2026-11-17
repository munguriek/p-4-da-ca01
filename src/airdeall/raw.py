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
