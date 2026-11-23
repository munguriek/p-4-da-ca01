from time import monotonic

from confluent_kafka import KafkaError, Message, Producer

from .capture import CapturedMessage
from .config import KafkaSettings


class KafkaPublisher:
    """Queue captured payloads and report delivery failures."""

    def __init__(self, producer: Producer, *, topic: str) -> None:
        self._producer = producer
        self._topic = topic
        self._delivery_error: KafkaError | None = None

    def publish(
        self,
        record: CapturedMessage,
        *,
        queue_timeout: float = 10.0,
    ) -> None:
        if queue_timeout < 0:
            raise ValueError("queue_timeout must be zero or positive")
        self._raise_if_delivery_failed()
        deadline = monotonic() + queue_timeout

        while True:
            try:
                self._producer.produce(
                    self._topic,
                    value=record.payload,
                    headers=[
                        ("routewatch_run_id", str(record.run_id).encode("ascii")),
                        (
                            "routewatch_received_at",
                            record.received_at.isoformat().encode("ascii"),
                        ),
                    ],
                    timestamp=int(record.received_at.timestamp() * 1000),
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError as error:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise TimeoutError("Kafka queue remained full") from error

                self._producer.poll(min(0.1, remaining))
                self._raise_if_delivery_failed()

        self._producer.poll(0)
        self._raise_if_delivery_failed()

    def flush(self, *, timeout: float = 10.0) -> None:
        remaining = self._producer.flush(timeout)
        self._raise_if_delivery_failed()

        if remaining:
            raise TimeoutError(
                f"Kafka flush timed out: {remaining} message(s) still pending"
            )

    def _on_delivery(
        self,
        error: KafkaError | None,
        _message: Message,
    ) -> None:
        if error is not None and self._delivery_error is None:
            self._delivery_error = error

    def _raise_if_delivery_failed(self) -> None:
        if self._delivery_error is not None:
            raise RuntimeError(f"Kafka delivery failed: {self._delivery_error}")


def create_producer(settings: KafkaSettings) -> Producer:
    """Create the Kafka client used by RouteWatch."""
    return Producer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "client.id": "routewatch",
            "enable.idempotence": True,
            "acks": "all",
            "message.max.bytes": 8_388_608,
            "delivery.timeout.ms": 30_000,
            "queue.buffering.max.kbytes": 65_536,
        }
    )
