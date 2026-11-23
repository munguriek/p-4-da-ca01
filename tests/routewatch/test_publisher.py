from datetime import UTC, datetime
from unittest.mock import Mock
from uuid import UUID

import pytest
from confluent_kafka import KafkaError, Message, Producer

from routewatch.capture import CapturedMessage, capture_message
from routewatch.publisher import KafkaPublisher

TOPIC = "routewatch.ris.raw"
RUN_ID = UUID(int=1)
RECEIVED_AT = datetime(2026, 11, 20, 6, 0, 0, 123456, tzinfo=UTC)


@pytest.fixture
def producer() -> Mock:
    client = Mock(spec=Producer)
    client.flush.return_value = 0
    return client


@pytest.fixture
def record() -> CapturedMessage:
    return capture_message(
        b'{"id":"event-1"}',
        run_id=RUN_ID,
        received_at=RECEIVED_AT,
    )


@pytest.mark.parametrize(
    "payload",
    [b'{"id":"event-1"}  ', b"\xffnot-json"],
)
def test_preserves_payload_and_capture_metadata(
    producer: Mock,
    payload: bytes,
) -> None:
    record = capture_message(
        payload,
        run_id=RUN_ID,
        received_at=RECEIVED_AT,
    )
    publisher = KafkaPublisher(producer, topic=TOPIC)

    publisher.publish(record)

    producer.produce.assert_called_once()
    call = producer.produce.call_args

    assert call.args == (TOPIC,)
    assert call.kwargs["value"] == payload
    assert dict(call.kwargs["headers"]) == {
        "routewatch_run_id": b"00000000-0000-0000-0000-000000000001",
        "routewatch_received_at": b"2026-11-20T06:00:00.123456+00:00",
    }
    assert call.kwargs["timestamp"] == 1_795_154_400_123

    producer.poll.assert_called_once_with(0)
    producer.flush.assert_not_called()

    call.kwargs["on_delivery"](None, Mock(spec=Message))
    publisher.flush()

    producer.flush.assert_called_once_with(10.0)


def test_reports_failure_even_if_flush_returns_zero(
    producer: Mock,
    record: CapturedMessage,
) -> None:
    publisher = KafkaPublisher(producer, topic=TOPIC)
    publisher.publish(record)
    publisher.publish(record)

    callbacks = [call.kwargs["on_delivery"] for call in producer.produce.call_args_list]

    def report_results(_timeout: float) -> int:
        callbacks[0](
            KafkaError(KafkaError._MSG_TIMED_OUT),
            Mock(spec=Message),
        )
        callbacks[1](None, Mock(spec=Message))
        return 0

    producer.flush.side_effect = report_results

    with pytest.raises(RuntimeError, match="Kafka delivery failed"):
        publisher.flush()


def test_reports_messages_still_pending_after_flush(
    producer: Mock,
    record: CapturedMessage,
) -> None:
    publisher = KafkaPublisher(producer, topic=TOPIC)
    publisher.publish(record)
    producer.flush.return_value = 1

    with pytest.raises(TimeoutError, match="still pending"):
        publisher.flush(timeout=0.1)


def test_stops_publishing_after_a_delivery_failure(
    producer: Mock,
    record: CapturedMessage,
) -> None:
    publisher = KafkaPublisher(producer, topic=TOPIC)

    def report_failure(_timeout: float) -> None:
        callback = producer.produce.call_args.kwargs["on_delivery"]
        callback(
            KafkaError(KafkaError._MSG_TIMED_OUT),
            Mock(spec=Message),
        )

    producer.poll.side_effect = report_failure

    with pytest.raises(RuntimeError, match="Kafka delivery failed"):
        publisher.publish(record)

    with pytest.raises(RuntimeError, match="Kafka delivery failed"):
        publisher.publish(record)

    producer.produce.assert_called_once()


def test_retries_a_full_queue_without_changing_the_record(
    producer: Mock,
    record: CapturedMessage,
) -> None:
    producer.produce.side_effect = [BufferError("full"), None]
    publisher = KafkaPublisher(producer, topic=TOPIC)

    publisher.publish(record)

    first, retry = producer.produce.call_args_list
    assert first.args == retry.args
    assert first.kwargs == retry.kwargs
    assert [call.args[0] for call in producer.poll.call_args_list] == [0.1, 0]


def test_full_queue_has_a_bounded_wait(
    producer: Mock,
    record: CapturedMessage,
) -> None:
    producer.produce.side_effect = BufferError("full")
    publisher = KafkaPublisher(producer, topic=TOPIC)

    with pytest.raises(TimeoutError, match="queue remained full"):
        publisher.publish(record, queue_timeout=0)

    producer.produce.assert_called_once()


def test_delivery_failure_during_backpressure_stops_retries(
    producer: Mock,
    record: CapturedMessage,
) -> None:
    publisher = KafkaPublisher(producer, topic=TOPIC)
    publisher.publish(record)
    callback = producer.produce.call_args.kwargs["on_delivery"]

    producer.produce.side_effect = BufferError("full")

    def report_failure(_timeout):
        callback(
            KafkaError(KafkaError._MSG_TIMED_OUT),
            Mock(spec=Message),
        )

    producer.poll.side_effect = report_failure

    with pytest.raises(RuntimeError, match="Kafka delivery failed"):
        publisher.publish(record)

    assert producer.produce.call_count == 2


def test_negative_queue_timeout_is_rejected(
    producer: Mock,
    record: CapturedMessage,
) -> None:
    publisher = KafkaPublisher(producer, topic=TOPIC)

    with pytest.raises(ValueError, match="queue_timeout"):
        publisher.publish(record, queue_timeout=-1)

    producer.produce.assert_not_called()
