import pytest

from airdeall.settings import MissingSetting, database_connection_info, read_env_file


def test_env_file_lines_become_settings(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "# a comment\n\nKAFKA_CLUSTER_ID=fake-cluster-id\nPOSTGRES_PASSWORD=fake=pw\n",
        encoding="utf-8",
    )
    assert read_env_file(env_file) == {
        "KAFKA_CLUSTER_ID": "fake-cluster-id",
        "POSTGRES_PASSWORD": "fake=pw",
    }


def test_missing_env_file_gives_no_settings(tmp_path):
    assert read_env_file(tmp_path / ".env") == {}


def test_missing_password_is_an_error():
    with pytest.raises(MissingSetting, match="POSTGRES_PASSWORD"):
        database_connection_info({})
