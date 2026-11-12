"""Settings for the pipeline, read from .env and the environment"""

import os
from collections.abc import Mapping
from pathlib import Path

from psycopg.conninfo import make_conninfo

# These match the pipeline-db service in compose.yaml
DB_HOST = "127.0.0.1"
DB_PORT = 5433
DB_NAME = "airdeall"
DB_USER = "pipeline"


class MissingSetting(Exception):
    """A required setting is neither in the environment nor .env."""


def read_env_file(path: Path) -> dict[str, str]:
    """
    RETURN the KEY=VALUE lines of an env as a dict.

    Skip blank lines and lines starting with # (comments).
    Settings can come from the environment
    """
    if not path.exists():
        return {}
    settings = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, _, value = line.partition("=")
        settings[key.strip()] = value.strip()
    return settings


def load_settings(env_file: Path = Path(".env")) -> dict[str, str]:
    """Settings from env file with process environment taking precedence"""
    return {**read_env_file(env_file), **os.environ}


def database_connection_info(settings: Mapping[str, str]) -> str:
    """The libpq connection string for the pipeline database."""
    password = settings.get("POSTGRES_PASSWORD")
    if not password:
        raise MissingSetting("POSTGRES_PASSWORD is not set: copy .env.example to .env and fill it in")
    return make_conninfo(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=password
    )
