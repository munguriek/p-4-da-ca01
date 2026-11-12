import pytest

from airdeall.cli import main


def test_migrate_help_shows_the_default_folder(capsys):
    with pytest.raises(SystemExit) as exit_info:
        main(["migrate", "--help"])

    assert exit_info.value.code == 0
    assert "sql/migrations" in capsys.readouterr().out
