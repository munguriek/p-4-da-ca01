import pytest
from psycopg.conninfo import conninfo_to_dict

from airdeall.settings import (
    MissingSetting,
    database_connection_info,
    gitlab_settings,
    kafka_topics,
    read_env_file,
    sink_consumer_config,
)

FAKE_KEY = "00" * 32


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


def test_connection_info_points_at_the_pipeline_database():
    conninfo = database_connection_info({"POSTGRES_PASSWORD": "fake-password"})

    assert conninfo_to_dict(conninfo) == {
        "host": "127.0.0.1",
        "port": "5433",
        "dbname": "airdeall",
        "user": "pipeline",
        "password": "fake-password",
    }


def test_topics_come_from_a_comma_separated_setting():
    settings = {"KAFKA_TOPICS": "otel.spans,otel.metrics,otel.logs"}

    assert kafka_topics(settings) == ["otel.spans", "otel.metrics", "otel.logs"]


def test_spaces_around_topic_names_are_ignored():
    settings = {"KAFKA_TOPICS": "otel.spans, otel.metrics ,otel.logs"}

    assert kafka_topics(settings) == ["otel.spans", "otel.metrics", "otel.logs"]


@pytest.mark.parametrize(
    "settings",
    [{}, {"KAFKA_TOPICS": ""}, {"KAFKA_TOPICS": " , "}],
    ids=["missing", "empty", "only-commas"],
)
def test_missing_topics_are_an_error(settings):
    with pytest.raises(MissingSetting, match="KAFKA_TOPICS"):
        kafka_topics(settings)


def test_sink_commits_offsets_by_hand_and_starts_at_the_earliest():
    config = sink_consumer_config()

    assert config == {
        "bootstrap.servers": "127.0.0.1:29092",
        "group.id": "pg-raw-sink",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }


def test_gitlab_settings_need_a_token():
    with pytest.raises(MissingSetting, match="GITLAB_TOKEN"):
        gitlab_settings({"PSEUDONYM_KEY": FAKE_KEY})


@pytest.mark.parametrize(
    "key", ["", "not-hex", "00" * 31], ids=["missing", "not-hex", "31-bytes"]
)
def test_gitlab_settings_need_a_32_byte_hex_key(key):
    with pytest.raises(MissingSetting, match="PSEUDONYM_KEY"):
        gitlab_settings({"GITLAB_TOKEN": "fake-token", "PSEUDONYM_KEY": key})


def test_gitlab_settings_have_defaults():
    gitlab = gitlab_settings({"GITLAB_TOKEN": "fake-token", "PSEUDONYM_KEY": FAKE_KEY})

    assert gitlab.key == bytes(32)
    assert (gitlab.project, gitlab.poll_seconds, gitlab.min_interval) == (
        "gitlab-com/gl-infra/production",
        600,
        1.0,
    )


def test_gitlab_settings_never_show_the_token_or_key():
    gitlab = gitlab_settings({"GITLAB_TOKEN": "fake-token", "PSEUDONYM_KEY": "ab" * 32})

    assert "fake-token" not in repr(gitlab)
    assert repr(gitlab.key) not in repr(gitlab)
