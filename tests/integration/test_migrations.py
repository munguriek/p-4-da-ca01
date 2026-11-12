import pytest


from airdeall.settings import EditedMigration, migrate

pytestmark = pytest.mark.integration

SCHEMAS = {"raw", "clean", "ontology", "analysis", "control"}

TABLES = [
    "control.schema_migration",
    "control.source_register",
    "control.run_log",
    "control.quarantine",
    "raw.otel",
    "raw.events",
]


@pytest.fixture
def migrations_folder(pytestconfig):
    return pytestconfig.rootpath / "sql" / "migrations"


def test_migrate_creates_schemas_and_tables(fresh_db, migrations_folder):
    migrate(fresh_db, migrations_folder)

    schemas = {row[0] for row in fresh_db.execute("SELECT nspname FROM pg_namespace")}
    missing_tables = [
        table
        for table in TABLES
        if fresh_db.execute("SELECT to_regclass(%s)", (table,)).fetchone()[0] is None
    ]
    assert SCHEMAS <= schemas
    assert missing_tables == []
