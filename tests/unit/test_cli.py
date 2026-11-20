import pytest

from airdeall.cli import build_parser, main, run_sink


def test_migrate_help_shows_the_default_folder(capsys):
    with pytest.raises(SystemExit) as exit_info:
        main(["migrate", "--help"])

    assert exit_info.value.code == 0
    assert "sql/migrations" in capsys.readouterr().out


def test_sink_is_a_command():
    args = build_parser().parse_args(["sink"])

    assert args.handler is run_sink


def test_a_missing_setting_exits_with_code_2(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # no .env here, so no real setting is read
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)

    assert main(["migrate"]) == 2
