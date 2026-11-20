import json
from contextlib import closing
from datetime import UTC, datetime
from uuid import uuid4

import httpx

from airdeall.routewatch.capture import capture_message
from airdeall.routewatch.source import iter_ris_messages


def main() -> None:
    run_id = uuid4()

    with (
        httpx.Client(timeout=15.0, follow_redirects=True) as client,
        closing(iter_ris_messages(client)) as messages,
    ):
        for count, payload in enumerate(messages, start=1):
            record = capture_message(
                payload,
                run_id=run_id,
                received_at=datetime.now(UTC),
            )

            event = json.loads(record.payload)

            assert event["type"] == "ris_message", event
            data = event["data"]
            assert data["type"] == "UPDATE", event

            print(
                {
                    "number": count,
                    "run_id": str(record.run_id),
                    "received_at": record.received_at.isoformat(),
                    "source_id": data["id"],
                    "collector": data["host"],
                    "type": data["type"],
                    "payload_bytes": len(record.payload),
                }
            )

            if count == 3:
                break


if __name__ == "__main__":
    main()
