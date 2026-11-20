from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID


@dataclass(frozen=True)
class CapturedMessage:
    payload: bytes
    run_id: UUID
    received_at: datetime


def capture_message(
    payload: bytes,
    *,
    run_id: UUID,
    received_at: datetime,
) -> CapturedMessage:
    """Attach acquisition metadata to an unchanged payload."""
    if received_at.tzinfo is None or received_at.utcoffset() is None:
        raise ValueError("received_at must be timezone-aware")

    return CapturedMessage(
        payload=payload,
        run_id=run_id,
        received_at=received_at.astimezone(UTC),
    )
