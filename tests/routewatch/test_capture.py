from datetime import UTC, datetime, timedelta, timezone
from uuid import UUID

import pytest

from routewatch.capture import capture_message


@pytest.mark.parametrize(
    "payload",
    [
        b'{"type":"ris_message","data":{"id":"event-1"}}  ',
        b"\xffnot-json",
    ],
)
def test_preserves_payload_and_attaches_acquisition_metadata(payload):
    run_id = UUID(int=1)
    received_at = datetime(2026, 11, 20, 5, 0, tzinfo=UTC)

    record = capture_message(
        payload,
        run_id=run_id,
        received_at=received_at,
    )

    assert record.payload == payload
    assert record.run_id == run_id
    assert record.received_at == received_at


def test_normalises_receipt_time_to_utc():
    received_at = datetime(
        2026,
        11,
        20,
        8,
        0,
        tzinfo=timezone(timedelta(hours=3)),
    )

    record = capture_message(
        b'{"id":"event-1"}',
        run_id=UUID(int=1),
        received_at=received_at,
    )

    assert record.received_at == datetime(2026, 11, 20, 5, 0, tzinfo=UTC)
    assert record.received_at.tzinfo is UTC


def test_rejects_receipt_time_without_a_timezone():
    received_at = datetime(2026, 11, 20, 5, 0, tzinfo=UTC).replace(tzinfo=None)

    with pytest.raises(ValueError, match="timezone-aware"):
        capture_message(
            b'{"id":"event-1"}',
            run_id=UUID(int=1),
            received_at=received_at,
        )
