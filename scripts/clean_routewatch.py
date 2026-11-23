import argparse
import signal
from dataclasses import asdict
from time import sleep

import psycopg

from routewatch.clean_storage import initialize_clean_schema
from routewatch.cleaner import clean_pending_batch
from routewatch.config import load_kafka_settings, load_postgres_settings


def stop_cleaner(_signal, _frame) -> None:
    raise KeyboardInterrupt


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Clean captured RouteWatch messages from PostgreSQL."
    )
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Maximum raw messages to examine; 0 has no limit.",
    )
    parser.add_argument("--transform-version", type=int, default=1)
    parser.add_argument(
        "--follow",
        action="store_true",
        help="Wait for new raw messages after catching up.",
    )
    arguments = parser.parse_args()

    if arguments.batch_size <= 0:
        parser.error("--batch-size must be positive")
    if arguments.max_messages < 0:
        parser.error("--max-messages must be zero or positive")
    if arguments.transform_version <= 0:
        parser.error("--transform-version must be positive")

    postgres = load_postgres_settings()
    kafka = load_kafka_settings()
    signal.signal(signal.SIGTERM, stop_cleaner)
    totals = {"selected": 0, "processed": 0, "events": 0, "rejected": 0}
    status = "complete"

    try:
        with psycopg.connect(
            host=postgres.host,
            port=postgres.port,
            dbname=postgres.dbname,
            user=postgres.user,
            password=postgres.password,
            autocommit=True,
            connect_timeout=5,
        ) as connection:
            with connection.transaction():
                initialize_clean_schema(connection)

            print(
                {
                    "status": "ready",
                    "topic": kafka.topic,
                    "transform_version": arguments.transform_version,
                },
                flush=True,
            )

            while (
                arguments.max_messages == 0
                or totals["selected"] < arguments.max_messages
            ):
                size = arguments.batch_size
                if arguments.max_messages:
                    size = min(size, arguments.max_messages - totals["selected"])

                with connection.transaction():
                    batch = clean_pending_batch(
                        connection,
                        topic=kafka.topic,
                        batch_size=size,
                        transform_version=arguments.transform_version,
                    )

                if batch.selected == 0:
                    if arguments.follow:
                        sleep(2)
                        continue
                    break

                for name, count in asdict(batch).items():
                    totals[name] += count

                print({"batch": asdict(batch), "totals": totals}, flush=True)

            if arguments.max_messages and totals["selected"] >= arguments.max_messages:
                status = "limit"
    except KeyboardInterrupt:
        status = "stopped"

    print(
        {
            "status": status,
            "topic": kafka.topic,
            "transform_version": arguments.transform_version,
            **totals,
        },
        flush=True,
    )


if __name__ == "__main__":
    main()
