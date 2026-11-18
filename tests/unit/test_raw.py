import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from uuid import UUID

import pytest
from confluent_kafka import TIMESTAMP_CREATE_TIME

from airdeall.raw import InvalidPayload, day_partition, to_raw_row

# A fixed run ID for unit tests to make them deterministic
RUN_ID = UUID("00000000-0000-7000-8000-000000000001")
# Boundary case 2026-11-09 23:59:59.999
LAST_MS_OF_9_NOVEMBER_UTC = 1794268799999


@dataclass
class FakeMessage:
    """Stands in for confluent_kafka.Message, with only methods the sink reads"""

    body: bytes | None
    timestamp_ms: int = LAST_MS_OF_9_NOVEMBER_UTC

    def topic(self) -> str:
        return "otel.spans"

    def partition(self) -> int:
        return 2

    def offset(self) -> int:
        return 41

    def timestamp(self) -> tuple[int, int]:
        return TIMESTAMP_CREATE_TIME, self.timestamp_ms

    def value(self) -> bytes | None:
        return self.body


def test_raw_row_maps_kafka_fields():
    body = b'{"resourceSpans": []}'

    row = to_raw_row(FakeMessage(body), RUN_ID)

    assert (row.topic, row.kpartition, row.koffset) == ("otel.spans", 2, 41)
    assert row.kafka_timestamp == datetime(2026, 11, 9, 23, 59, 59, 999000, tzinfo=UTC)
    assert row.kafka_timestamp.utcoffset() == timedelta(0)
    assert row.payload == body.decode("utf-8")
    assert row.payload_sha256 == hashlib.sha256(body).digest()
    assert row.payload_bytes == len(body)
    assert row.run_id == RUN_ID


def test_non_json_payload_is_rejected():
    with pytest.raises(InvalidPayload, match="not json"):
        to_raw_row(FakeMessage(b"not json"), RUN_ID)


def test_nul_escape_is_rejected():
    with pytest.raises(InvalidPayload, match="nul escape"):
        to_raw_row(FakeMessage(b'{"name": "a\\u0000b"}'), RUN_ID)


@pytest.mark.parametrize(
    ("body", "reason"),
    [
        (b"\xff\xfe", "not utf-8"),
        (b'{"value": NaN}', "not json"),
        (b'{"name": "\\ud800"}', "lone surrogate"),
    ],
    ids=["invalid-utf-8", "nan", "lone-surrogate"],
)
def test_payload_jsonb_cannot_store_is_rejected(body, reason):
    with pytest.raises(InvalidPayload, match=reason):
        to_raw_row(FakeMessage(body), RUN_ID)


@pytest.mark.parametrize(
    "body",
    [
        b'{"name": "a\\\\u0000b"}',  # an escaped backslash, then the text u0000
        b'{"name": "\\ud83d\\ude00"}',  # a surrogate pair: one emoji
    ],
    ids=["escaped-backslash", "surrogate-pair"],
)
def test_escapes_jsonb_accepts_are_kept(body):
    assert to_raw_row(FakeMessage(body), RUN_ID).payload == body.decode("utf-8")


def test_message_without_a_value_is_rejected():
    with pytest.raises(InvalidPayload, match="not json"):
        to_raw_row(FakeMessage(None), RUN_ID)


def test_day_partition_bounds_are_utc():
    last_ms = datetime(2026, 11, 9, 23, 59, 59, 999000, tzinfo=UTC)

    name, start, end = day_partition(last_ms)

    assert name == "otel_20261109"
    assert start == datetime(2026, 11, 9, tzinfo=UTC)
    assert end == datetime(2026, 11, 10, tzinfo=UTC)


def test_day_partition_uses_the_utc_date():
    half_past_midnight_at_utc_plus_one = datetime(
        2026, 11, 10, 0, 30, tzinfo=timezone(timedelta(hours=1))
    )

    assert day_partition(half_past_midnight_at_utc_plus_one).name == "otel_20261109"


def test_naive_timestamp_is_refused():
    # A datetime without a time zone, on purpose: day_partition can't know its UTC date.
    naive = datetime(2026, 11, 9, 12)  # noqa: DTZ001

    with pytest.raises(ValueError, match="time zone"):
        day_partition(naive)
