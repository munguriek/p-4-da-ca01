import argparse
import logging
import signal
from pathlib import Path
from time import gmtime

import httpx

from routewatch.collector import collect_messages
from routewatch.config import load_kafka_settings
from routewatch.publisher import KafkaPublisher, create_producer

logger = logging.getLogger(__name__)


def stop_collector(_signal, _frame) -> None:
    raise KeyboardInterrupt


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect live RIS updates.")
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Stop after this many messages; 0 runs continuously.",
    )
    parser.add_argument("--collector", default="rrc21")
    parser.add_argument(
        "--log-file",
        type=Path,
        default=Path("logs/routewatch-collector.log"),
    )
    arguments = parser.parse_args()

    if arguments.max_messages < 0:
        parser.error("--max-messages must be zero or positive")

    arguments.log_file.parent.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter(
        "%(asctime)sZ %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    formatter.converter = gmtime
    handlers = [
        logging.StreamHandler(),
        logging.FileHandler(arguments.log_file, encoding="utf-8"),
    ]
    for handler in handlers:
        handler.setFormatter(formatter)
    logging.basicConfig(level=logging.INFO, handlers=handlers)
    signal.signal(signal.SIGTERM, stop_collector)

    settings = load_kafka_settings()

    with (
        create_producer(settings) as producer,
        httpx.Client(timeout=15.0, follow_redirects=True) as client,
    ):
        publisher = KafkaPublisher(producer, topic=settings.topic)

        try:
            delivered = collect_messages(
                client,
                publisher,
                collector=arguments.collector,
                max_messages=arguments.max_messages,
            )
        except KeyboardInterrupt:
            logger.info("collector_stopped_by_user")
            return

    print({"delivered": delivered})


if __name__ == "__main__":
    main()
