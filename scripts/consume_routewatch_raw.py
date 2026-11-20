import argparse
import logging

from airdeall.routewatch.config import (
    load_consumer_group_id,
    load_kafka_settings,
    load_postgres_settings,
)
from airdeall.routewatch.consumer import run_raw_worker


def main() -> None:
    parser = argparse.ArgumentParser(description="Store RouteWatch Kafka records.")
    parser.add_argument("--max-messages", type=int, default=0)
    parser.add_argument("--idle-timeout", type=float)
    arguments = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(message)s",
    )

    try:
        processed, inserted = run_raw_worker(
            load_kafka_settings(),
            load_postgres_settings(),
            group_id=load_consumer_group_id(),
            max_messages=arguments.max_messages,
            idle_timeout=arguments.idle_timeout,
        )
    except KeyboardInterrupt:
        print("Raw worker stopped.")
        return

    print(
        {
            "processed": processed,
            "inserted": inserted,
            "replayed": processed - inserted,
        }
    )


if __name__ == "__main__":
    main()
