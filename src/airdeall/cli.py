"""The airdeall command: airdeall <command> [options]."""

import argparse
import logging
import signal
import threading
from pathlib import Path

import psycopg
from confluent_kafka import Consumer, KafkaException

from airdeall.migrations import MigrationError, migrate
from airdeall.settings import (
    MissingSetting,
    database_connection_info,
    kafka_topics,
    load_settings,
    sink_consumer_config,
)
from airdeall.sink import RawSink, finish_run, start_run

log = logging.getLogger("airdeall")


def run_sink(args: argparse.Namespace) -> int:
    settings = load_settings()
    conninfo = database_connection_info(settings)
    topics = kafka_topics(settings)

    stop = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())

    with psycopg.connect(conninfo, autocommit=True) as conn:
        run_id = start_run(conn, "raw-sink")
        log.info("run %s reading %s", run_id, ", ".join(topics))
        consumer = Consumer(sink_consumer_config())
        try:
            consumer.subscribe(topics)
            RawSink(consumer, conn, run_id).run(stop.is_set)
        except Exception as error:
            finish_run(conn, run_id, "failed", note=str(error))
            raise
        finally:
            consumer.close()
        finish_run(conn, run_id, "finished")
    log.info("run %s finished", run_id)
    return 0


def run_migrate(args: argparse.Namespace) -> int:
    conninfo = database_connection_info(load_settings())
    with psycopg.connect(conninfo, autocommit=True) as conn:
        migrate(conn, args.folder)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="airdeall",
        description="Stream infrastructure data through Kafka into PostgreSQL.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    migrate_parser = commands.add_parser(
        "migrate", help="apply new database migrations"
    )
    migrate_parser.add_argument(
        "--folder",
        type=Path,
        default=Path("sql/migrations"),
        help="folder of NNN_name.sql files (default: %(default)s)",
    )
    migrate_parser.set_defaults(handler=run_migrate)
    sink_parser = commands.add_parser(
        "sink", help="copy telemetry from Kafka into raw.otel until stopped"
    )
    sink_parser.set_defaults(handler=run_sink)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    try:
        return args.handler(args)
    except MissingSetting as error:
        log.error("%s", error)
        return 2
    except (MigrationError, OSError, psycopg.Error, KafkaException) as error:
        log.error("%s", error)
        return 1
