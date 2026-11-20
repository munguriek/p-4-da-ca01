from datetime import UTC
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import UUID

import httpx
import pytest

from airdeall.routewatch import collector
from airdeall.routewatch.publisher import KafkaPublisher
from airdeall.routewatch.stream import IncompleteMessageError


def stream(payloads=(), *, error=None, events=None):
    try:
        yield from payloads
        if error is not None:
            raise error
    finally:
        if events is not None:
            events.append("source_closed")


@pytest.fixture
def flow(monkeypatch: pytest.MonkeyPatch):
    events = []
    source = Mock()
    pause = Mock()
    publisher = Mock(spec=KafkaPublisher)
    publisher.flush.side_effect = lambda **_kwargs: events.append("flush")
    monkeypatch.setattr(collector, "iter_ris_messages", source)
    monkeypatch.setattr(collector, "sleep", pause)

    return SimpleNamespace(
        client=Mock(spec=httpx.Client),
        publisher=publisher,
        source=source,
        pause=pause,
        events=events,
    )


def test_captures_the_limit_and_closes_before_flushing(flow) -> None:
    payloads = [b'{"id":"event-1"}  ', b"\xffnot-json"]
    flow.source.return_value = stream(
        [*payloads, b"must-not-be-read"], events=flow.events
    )

    result = collector.collect_messages(
        flow.client,
        flow.publisher,
        collector="rrc00",
        max_messages=2,
    )

    assert result == 2
    records = [call.args[0] for call in flow.publisher.publish.call_args_list]
    assert [record.payload for record in records] == payloads
    assert isinstance(records[0].run_id, UUID)
    assert records[0].run_id == records[1].run_id
    assert all(record.received_at.tzinfo is UTC for record in records)
    assert flow.events == ["source_closed", "flush"]
    flow.source.assert_called_once_with(flow.client, collector="rrc00")
    flow.publisher.flush.assert_called_once_with(timeout=35)


# def test_zero_limit_reads_until_the_source_ends(flow) -> None:
#     flow.source.return_value = stream([b"one", b"two"])


#     assert collector.collect_messages(flow.client, flow.publisher) == 2
#     assert flow.publisher.publish.call_count == 2
def test_zero_limit_reconnects_after_the_source_ends(flow) -> None:
    flow.source.side_effect = [
        stream([b"one"]),
        stream([b"two"], error=KeyboardInterrupt()),
    ]

    with pytest.raises(KeyboardInterrupt):
        collector.collect_messages(flow.client, flow.publisher)

    assert flow.publisher.publish.call_count == 2
    flow.pause.assert_called_once_with(1.0)


# @pytest.mark.parametrize(
#     "error",
#     [httpx.ReadError("disconnected"), KeyboardInterrupt()],
#     ids=["source-error", "interrupt"],
# )
# def test_source_errors_and_interrupts_close_then_flush(flow, error) -> None:
#     flow.source.return_value = stream([b"one"], error=error, events=flow.events)

#     with pytest.raises(type(error)) as caught:
#         collector.collect_messages(flow.client, flow.publisher)


#     assert caught.value is error
#     flow.publisher.publish.assert_called_once()
#     assert flow.events == ["source_closed", "flush"]
def test_interrupt_closes_then_flushes(flow) -> None:
    error = KeyboardInterrupt()
    flow.source.return_value = stream([b"one"], error=error, events=flow.events)

    with pytest.raises(KeyboardInterrupt) as caught:
        collector.collect_messages(flow.client, flow.publisher)

    assert caught.value is error
    flow.publisher.publish.assert_called_once()
    assert flow.events == ["source_closed", "flush"]
    flow.pause.assert_not_called()


@pytest.mark.parametrize(
    "error",
    [RuntimeError("delivery failed"), TimeoutError("queue remained full")],
)
def test_publishing_errors_close_then_flush(flow, error) -> None:
    flow.source.return_value = stream([b"one"], events=flow.events)
    flow.publisher.publish.side_effect = error

    with pytest.raises(type(error)) as caught:
        collector.collect_messages(flow.client, flow.publisher)

    assert caught.value is error
    assert flow.events == ["source_closed", "flush"]


def test_flush_failure_is_reported(flow) -> None:
    flow.source.return_value = stream([b"one"])
    flow.publisher.flush.side_effect = RuntimeError("delivery failed")

    with pytest.raises(RuntimeError, match="delivery failed"):
        collector.collect_messages(flow.client, flow.publisher, max_messages=1)


def test_negative_limit_is_rejected_before_collecting(flow) -> None:
    with pytest.raises(ValueError, match="max_messages"):
        collector.collect_messages(flow.client, flow.publisher, max_messages=-1)

    flow.source.assert_not_called()
    flow.publisher.publish.assert_not_called()
    flow.publisher.flush.assert_not_called()


@pytest.mark.parametrize(
    "error",
    [
        httpx.ReadError("disconnected"),
        httpx.ReadTimeout("idle"),
        httpx.RemoteProtocolError("connection closed"),
        httpx.ProxyError("proxy unavailable"),
        IncompleteMessageError("incomplete message"),
    ],
    ids=["network", "timeout", "protocol", "proxy", "incomplete-frame"],
)
def test_reconnects_and_keeps_the_overall_message_limit(flow, error) -> None:
    flow.source.side_effect = [
        stream([b"one"], error=error, events=flow.events),
        stream([b"two", b"must-not-be-read"], events=flow.events),
    ]

    assert collector.collect_messages(flow.client, flow.publisher, max_messages=2) == 2

    records = [call.args[0] for call in flow.publisher.publish.call_args_list]
    assert [record.payload for record in records] == [b"one", b"two"]
    assert records[0].run_id != records[1].run_id
    assert flow.events == [
        "source_closed",
        "flush",
        "source_closed",
        "flush",
    ]
    flow.pause.assert_called_once_with(1.0)


def test_backoff_grows_is_capped_and_resets_after_data(flow) -> None:
    flow.source.side_effect = [
        *[stream() for _ in range(8)],
        stream([b"one"]),
        stream([b"two"]),
    ]

    assert collector.collect_messages(flow.client, flow.publisher, max_messages=2) == 2
    assert [call.args[0] for call in flow.pause.call_args_list] == [
        1,
        2,
        4,
        8,
        16,
        30,
        30,
        30,
        1,
    ]


def test_oversized_frames_stop_collection(flow) -> None:
    flow.source.return_value = stream(
        error=ValueError("Message exceeds maximum size"),
        events=flow.events,
    )

    with pytest.raises(ValueError, match="maximum size"):
        collector.collect_messages(flow.client, flow.publisher)

    flow.source.assert_called_once()
    flow.pause.assert_not_called()
    assert flow.events == ["source_closed", "flush"]


def test_failed_flush_before_reconnecting_stops_collection(flow) -> None:
    flow.source.side_effect = [
        stream(error=httpx.ReadError("disconnected")),
        stream([b"must-not-be-read"]),
    ]
    flow.publisher.flush.side_effect = RuntimeError("delivery failed")

    with pytest.raises(RuntimeError, match="delivery failed"):
        collector.collect_messages(flow.client, flow.publisher)

    flow.source.assert_called_once()
    flow.pause.assert_not_called()
