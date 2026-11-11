"""Apply SQL migrations in order, each once. recorded in control.schema_migration."""
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
            version = int(match[1]),
            name = path.name,
            sql = data.decode("utf-8"),
            checksum = hashlib.sha256(data).hexdigest(),
        )
    )
    return sorted(migrations, key=lambda m: m.version)
