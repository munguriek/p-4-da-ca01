import os
from pathlib import Path

import pytest

from routewatch.config import load_kafka_settings

BOOTSTRAP = "ROUTEWATCH_KAFKA_BOOTSTRAP_SERVERS"
TOPIC = "ROUTEWATCH_KAFKA_TOPIC"


@pytest.fixture(autouse=True)
def isolate_kafka_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(BOOTSTRAP, raising=False)
    monkeypatch.delenv(TOPIC, raising=False)


@pytest.fixture
def env_file(tmp_path: Path) -> Path:
    path = tmp_path / ".env"
    path.write_text(
        f"{BOOTSTRAP}=127.0.0.1:39092\n{TOPIC}=routewatch.ris.raw\n",
        encoding="utf-8",
    )
    return path


def test_reads_kafka_settings_from_env_file(env_file: Path) -> None:
    settings = load_kafka_settings(env_file)

    assert settings.bootstrap_servers == "127.0.0.1:39092"
    assert settings.topic == "routewatch.ris.raw"

    assert BOOTSTRAP not in os.environ
    assert TOPIC not in os.environ


def test_exported_variables_override_env_file(
    env_file: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(BOOTSTRAP, "broker:9092")
    monkeypatch.setenv(TOPIC, "alternate.topic")

    settings = load_kafka_settings(env_file)

    assert settings.bootstrap_servers == "broker:9092"
    assert settings.topic == "alternate.topic"


@pytest.mark.parametrize("name", [BOOTSTRAP, TOPIC])
@pytest.mark.parametrize("value", [None, "", "   "])
def test_rejects_missing_or_blank_settings(
    env_file: Path,
    name: str,
    value: str | None,
) -> None:
    values = {
        BOOTSTRAP: "127.0.0.1:39092",
        TOPIC: "routewatch.ris.raw",
    }

    if value is None:
        del values[name]
    else:
        values[name] = value

    env_file.write_text(
        "".join(f"{key}={item}\n" for key, item in values.items()),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=name):
        load_kafka_settings(env_file)
