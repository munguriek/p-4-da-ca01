"""Apply SQL migrations in order, each once, recorded in control.schema_migration."""

import hashlib
import logging
import re
from dataclasses import dataclass
from pathlib import Path

import psycopg

log = logging.getLogger(__name__)

MIGRATION_FILE = re.compile(r"(\d{3})_\w+\.sql")

BOOTSTRAP_SQL = """
CREATE SCHEMA IF NOT EXISTS control;
CREATE TABLE IF NOT EXISTS control.schema_migration (
    version    integer      PRIMARY KEY,
    name       text         NOT NULL,
    checksum   text         NOT NULL,
    applied_at timestamptz  NOT NULL DEFAULT now()
);
"""


class MigrationError(Exception):
    """A migration can not be applied as it stands."""


class EditedMigration(MigrationError):
    """A migration file was changed since it was applied."""

    def __init__(self, name: str) -> None:
        super().__init__(
            f"{name} has changed since it was applied; \n Put the change in a new migration instead"
        )


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    sql: str
    checksum: str


def load_migrations(folder: Path) -> list[Migration]:
    """Read the NNN_name.sql files in folder, in order of NNN; ignore other files."""
    migrations = []
    for path in folder.iterdir():
        match = MIGRATION_FILE.fullmatch(path.name)
        if match is None:
            continue
        data = path.read_bytes()
        migrations.append(
            Migration(
                version=int(match[1]),
                name=path.name,
                sql=data.decode("utf-8"),
                checksum=hashlib.sha256(data).hexdigest(),
            )
        )
    return sorted(migrations, key=lambda m: m.version)


def migrate(connection: psycopg.Connection, folder: Path) -> list[str]:
    """
    Apply each migration in folder not yet applied and return their names.
    Open connection with autocommit=True: each transaction() block is then a real transaction,
    so every migration is committed as soon as it succeeds.
    """
    with connection.transaction():
        connection.execute(BOOTSTRAP_SQL)
        rows = connection.execute(
            "SELECT version, checksum FROM control.schema_migration"
        )
        applied = dict(rows.fetchall())

    applied_now = []
    for m in load_migrations(folder):
        if m.version in applied:
            if applied[m.version] != m.checksum:
                raise EditedMigration(m.name)
            continue
        with connection.transaction():
            connection.execute(m.sql)
            connection.execute(
                "INSERT INTO control.schema_migration (version, name, checksum)"
                " VALUES (%s, %s, %s)",
                (m.version, m.name, m.checksum),
            )
        log.info("applied %s", m.name)
        applied_now.append(m.name)
    log.info(
        "%d migrations applied, %d already applied", len(applied_now), len(applied)
    )
    return applied_now
