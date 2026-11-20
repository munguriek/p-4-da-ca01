from pathlib import Path

import pytest

from airdeall.routewatch.config import load_postgres_settings

VALUES = {
    "ROUTEWATCH_POSTGRES_HOST": "127.0.0.1",
    "ROUTEWATCH_POSTGRES_PORT": "5434",
    "ROUTEWATCH_POSTGRES_DB": "routewatch",
    "ROUTEWATCH_POSTGRES_USER": "routewatch",
    "ROUTEWATCH_POSTGRES_PASSWORD": "test-only-password",
}


@pytest.fixture(autouse=True)
def isolate_postgres_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in VALUES:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def env_file(tmp_path: Path) -> Path:
    path = tmp_path / ".env"
    path.write_text(
        "".join(f"{name}={value}\n" for name, value in VALUES.items()),
        encoding="utf-8",
    )
    return path


def test_reads_postgres_settings(env_file: Path) -> None:
    settings = load_postgres_settings(env_file)

    assert settings.host == "127.0.0.1"
    assert settings.port == 5434
    assert settings.dbname == "routewatch"
    assert settings.user == "routewatch"
    assert settings.password == "test-only-password"
    assert "test-only-password" not in repr(settings)


def test_exported_postgres_settings_override_file(
    env_file: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ROUTEWATCH_POSTGRES_HOST", "routewatch-db")
    monkeypatch.setenv("ROUTEWATCH_POSTGRES_PORT", "5432")

    settings = load_postgres_settings(env_file)

    assert settings.host == "routewatch-db"
    assert settings.port == 5432


def test_preserves_quoted_password_whitespace(env_file: Path) -> None:
    values = {
        **VALUES,
        "ROUTEWATCH_POSTGRES_PASSWORD": "'  test-only-password  '",
    }
    env_file.write_text(
        "".join(f"{name}={value}\n" for name, value in values.items()),
        encoding="utf-8",
    )

    settings = load_postgres_settings(env_file)

    assert settings.password == "  test-only-password  "


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("ROUTEWATCH_POSTGRES_PASSWORD", None),
        ("ROUTEWATCH_POSTGRES_PASSWORD", ""),
        ("ROUTEWATCH_POSTGRES_PASSWORD", "   "),
        ("ROUTEWATCH_POSTGRES_PORT", "not-a-number"),
        ("ROUTEWATCH_POSTGRES_PORT", "0"),
        ("ROUTEWATCH_POSTGRES_PORT", "65536"),
    ],
)
def test_rejects_invalid_postgres_settings(
    env_file: Path,
    name: str,
    value: str | None,
) -> None:
    values = VALUES.copy()

    if value is None:
        del values[name]
    else:
        values[name] = value

    env_file.write_text(
        "".join(f"{key}={item}\n" for key, item in values.items()),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=name):
        load_postgres_settings(env_file)
