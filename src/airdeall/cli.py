"""The airdeall command: airdeall <command> [options]."""

import argparse
import logging
from pathlib import Path

import psycopg

from airdeall.migrations import MigrationError, migrate
from airdeall.settings import MissingSetting, database_connection_info, load_settings

log = logging.getLogger("airdeall")


def run_migrate(args: argparse.Namespace) -> int:
    conninfo = database_connection_info(load_settings())
    with psycopg.connect(conninfo, autocommit=True) as conn:
        migrate(conn, args.folder)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="airdeall",
        description="Stream Infrastructure data through kafka into PostgreSQL.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    migrate_parser = commands.add_parser(
        "migrate", help="apply new database migrations"
    )
    migrate_parser.add_argument(
        "--folder",
        type=Path,
        default=Path("sql/migrations"),
        help="folder of NNN_name.sql files (default: %(defaults)s)",
    )
    migrate_parser.set_defaults(handler=run_migrate)
    return parser


def main(argv: list[str] | None = None) -> str:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    try:
        return args.handler(args)
    except (MigrationError, MissingSetting, OSError, psycopg.Error) as error:
        log.error("%s", error)
        return 1
