import pytest

from airdeall.migrations import EditedMigration, migrate

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


def test_second_migrate_applies_nothing(fresh_db, migrations_folder):
    first = migrate(fresh_db, migrations_folder)
    second = migrate(fresh_db, migrations_folder)
    assert first == ["001_schemas.sql", "002_control.sql", "003_raw.sql"]
    assert second == []


def test_edited_migration_is_refused(fresh_db, tmp_path):
    migration = tmp_path / "001_a.sql"
    migration.write_text("CREATE SCHEMA a;\n", encoding="utf-8")
    migrate(fresh_db, tmp_path)

    migration.write_text("CREATE SCHEMA a;\nCREATE SCHEMA b;\n", encoding="utf-8")
    (tmp_path / "002_c.sql").write_text("CREATE SCHEMA c;\n", encoding="utf-8")

    with pytest.raises(EditedMigration, match="001_a.sql"):
        migrate(fresh_db, tmp_path)
    applied = fresh_db.execute("SELECT name FROM control.schema_migration").fetchall()
    assert applied == [("001_a.sql",)]
