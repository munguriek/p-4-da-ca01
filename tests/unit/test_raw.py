from uuid import UUID
from dataclasses import dataclass

from confluent_kafka import TIMESTAMP_CREATE_TIME

# A fixed run ID for unit tests to make them deterministic
RUN_ID = UUID("00000000-0000-7000-8000-000000000001")
# Boundary case 2026-11-09 23:59:59.999
LAST_MS_OF_9_NOVEMBER_UTC = 1794268799999

@dataclass
class FakeMessage:
    """Stands in for confluent_kafka.Message, with only methods the sink reads"""
    body: bytes
    timestamp_ms: int = LAST_MS_OF_9_NOVEMBER_UTC

    def topic(self) -> str:
        return "otels.spans"

    def partition(self) -> int:
        return 2

    def timestamp(self) -> tuple[int, int]:
        return TIMESTAMP_CREATE_TIME, self.timestamp_ms

    def value(self) -> bytes:
        return self.body
