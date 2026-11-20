from contextlib import closing
from datetime import UTC, datetime
from uuid import uuid4

import httpx

from airdeall.routewatch.capture import capture_message
from airdeall.routewatch.config import load_kafka_settings
from airdeall.routewatch.publisher import KafkaPublisher, create_producer
from airdeall.routewatch.source import iter_ris_messages


def main() -> None:
    settings = load_kafka_settings()
    run_id = uuid4()
    queued = 0

    with create_producer(settings) as producer:
        publisher = KafkaPublisher(producer, topic=settings.topic)

        try:
            with (
                httpx.Client(timeout=15.0, follow_redirects=True) as client,
                closing(iter_ris_messages(client)) as messages,
            ):
                for payload in messages:
                    record = capture_message(
                        payload,
                        run_id=run_id,
                        received_at=datetime.now(UTC),
                    )
                    publisher.publish(record)
                    queued += 1

                    print(
                        {
                            "number": queued,
                            "status": "queued",
                            "run_id": str(record.run_id),
                            "received_at": record.received_at.isoformat(),
                            "payload_bytes": len(record.payload),
                        }
                    )

                    if queued == 3:
                        break
        finally:
            publisher.flush(timeout=35)

    if queued != 3:
        raise RuntimeError(f"Expected 3 messages, received {queued}")

    print(
        {
            "status": "delivered",
            "messages": queued,
            "topic": settings.topic,
            "run_id": str(run_id),
        }
    )


if __name__ == "__main__":
    main()
