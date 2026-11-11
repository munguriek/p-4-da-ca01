from airdeall.migrations import migrations

def test_migrations_are_loaded_in_order(tmp_path):
    (tmp_path / "002_b.sql").write_text("SELECT 2;\n", encoding="utf-8")
    (tmp_path / "001_a.sql").write_text("SELECT 1;\n", encoding="utf-8")
    (tmp_path / "003_c.sql").write_text("SELECT 3;\n", encoding="utf-8")
    (tmp_path / "notes.txt").write_text("not a migration\n", encoding="utf-8")

    migrations = load_migrations(tmp_path)

    assert [(m.version, m.name) for m in migrations] == [
        (1, "001_a.sql"),
        (2, "002_b.sql"),
        (3, "003_c.sql"),
    ]

def test_checksum_changes_when_the_file_changes(tmp_path):
    path = tmp_path / "001_a.sql"
    path.write_text("SELECT 1;\n", encoding="utf-8")
    before = load_migrations(tmp_path)[0].checksum
    unchanged = load_migrations(tmp_path)[0].checksum

    path.write_text("SELECT 1; -- edited\n", encoding="utf-8")
    after = load_migrations(tmp_path)[0].checksum

    assert unchanged == before
    assert after != before
