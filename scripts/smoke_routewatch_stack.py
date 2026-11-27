"""Check a running RouteWatch stack end to end with synthetic RIS updates.

CI runs this against the containers, with the collector stopped. It publishes
UPDATE messages built from documentation addresses (RFC 5737, RFC 3849) and AS
numbers (RFC 5398) to the Kafka topic, then waits until the web UI's summary
shows each one stored once and cleaned once. The raw table is append-only, so
these rows stay: never point this at a database you analyse.
"""

import argparse
import json
import sys
from datetime import UTC, datetime
from time import monotonic, sleep
from uuid import uuid4

import httpx

from routewatch.capture import capture_message
from routewatch.config import load_kafka_settings
from routewatch.publisher import KafkaPublisher, create_producer

SMOKE_HOST = "smoke.routewatch.invalid"


def synthetic_updates(
    count: int, *, tag: str, timestamp: float
) -> tuple[list[bytes], int]:
    """Return the payloads and the prefix events they should clean into.

    Even-numbered messages announce one IPv4 and one IPv6 prefix; odd-numbered
    messages withdraw one IPv4 prefix.
    """
    payloads = []
    events = 0
    for number in range(count):
        data = {
            "id": f"smoke-{tag}-{number:04d}",
            "timestamp": timestamp,
            "host": SMOKE_HOST,
            "peer": "192.0.2.1",
            "peer_asn": "64496",
            "type": "UPDATE",
        }
        if number % 2 == 0:
            data |= {
                "path": [64496, 64500, 64511],
                "origin": "IGP",
                "community": [[64496, 100]],
                "announcements": [
                    {"next_hop": "192.0.2.1", "prefixes": ["198.51.100.0/24"]},
                    {"next_hop": "2001:db8::1", "prefixes": ["2001:db8:100::/48"]},
                ],
                "withdrawals": [],
            }
            events += 2
        else:
            data["withdrawals"] = ["203.0.113.0/24"]
            events += 1
        message = {"type": "ris_message", "data": data}
        payloads.append(json.dumps(message).encode("utf-8"))
    return payloads, events


def counts(summary: dict) -> dict[str, int]:
    cleaned = summary["processing"]["by_status"]["cleaned"]
    return {
        "raw_messages": summary["raw"]["messages"],
        "receipts": summary["processing"]["receipts"],
        "cleaned_messages": cleaned["messages"],
        "cleaned_events": cleaned["events"],
        "clean_events": summary["clean"]["events"],
    }


def fetch_json(client: httpx.Client, path: str) -> dict:
    response = client.get(path)
    response.raise_for_status()
    return response.json()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--messages", type=int, default=20)
    parser.add_argument("--ui-url", default="http://127.0.0.1:8765")
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument(
        "--allow-synthetic-writes",
        action="store_true",
        help="Confirm that synthetic rows may be added to the raw and clean tables.",
    )
    arguments = parser.parse_args()

    if not arguments.allow_synthetic_writes:
        parser.error(
            "this adds synthetic rows to the append-only raw table; "
            "pass --allow-synthetic-writes to confirm"
        )
    if arguments.messages < 2:
        parser.error("--messages must be at least 2")

    settings = load_kafka_settings()
    run_id = uuid4()
    payloads, events = synthetic_updates(
        arguments.messages,
        tag=run_id.hex[:12],
        timestamp=datetime.now(UTC).timestamp(),
    )

    with httpx.Client(base_url=arguments.ui_url, timeout=10.0) as client:
        first = fetch_json(client, "/api/summary")
        if first.get("approximate"):
            print(
                "The UI reports estimated counts, so its tables are too large for "
                "this test: run it against a fresh stack.",
                file=sys.stderr,
            )
            return 1
        before = counts(first)
        messages = len(payloads)
        expected = {
            "raw_messages": before["raw_messages"] + messages,
            "receipts": before["receipts"] + messages,
            "cleaned_messages": before["cleaned_messages"] + messages,
            "cleaned_events": before["cleaned_events"] + events,
            "clean_events": before["clean_events"] + events,
        }

        with create_producer(settings) as producer:
            publisher = KafkaPublisher(producer, topic=settings.topic)
            for payload in payloads:
                record = capture_message(
                    payload, run_id=run_id, received_at=datetime.now(UTC)
                )
                publisher.publish(record)
            publisher.flush(timeout=30)

        deadline = monotonic() + arguments.timeout
        while True:
            summary = fetch_json(client, "/api/summary")
            seen = counts(summary)
            if all(seen[name] >= expected[name] for name in expected):
                break
            if monotonic() >= deadline:
                print(
                    json.dumps(
                        {"status": "timed_out", "expected": expected, "seen": seen}
                    ),
                    file=sys.stderr,
                )
                return 1
            sleep(2)

        problems = [
            f"{name}: expected {expected[name]}, found {seen[name]}"
            for name in expected
            if seen[name] != expected[name]
        ]
        if not summary["receipts_match_rows"]:
            problems.append("receipts_match_rows is false")
        for path in ("/api/raw?limit=1", "/api/clean?limit=1", "/api/kafka"):
            try:
                fetch_json(client, path)
            except httpx.HTTPError as error:
                problems.append(f"{path}: {error}")

    if problems:
        print(json.dumps({"status": "failed", "problems": problems}), file=sys.stderr)
        return 1

    print(
        json.dumps(
            {
                "status": "passed",
                "run_id": str(run_id),
                "messages": messages,
                "prefix_events": events,
                "totals": seen,
            }
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
