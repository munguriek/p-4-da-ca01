"""The airdeall command: airdeall <command> [options]."""

import argparse
import logging

import psycopg

from airdeall.migrations import migrate
from airdeall.settings import database_connection_info, load_settings

log = logging.getLogger("airdeall")


def run_migrate(args: argparse.Namespace) -> int:
    conninfo = database_connection_info(load_settings())
    with psycopg.connect(conninfo, autocommit=True) as conn:
        migrate(conn, args.folder)
    return 0
