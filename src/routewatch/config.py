import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import dotenv_values


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str
    topic: str


def load_kafka_settings(
    env_file: str | Path = ".env",
) -> KafkaSettings:
    """Read and validate RouteWatch's Kafka configuration."""
    values = {
        **dotenv_values(env_file),
        **os.environ,
    }

    def required(name: str) -> str:
        value = values.get(name)

        if value is None or not value.strip():
            raise ValueError(f"{name} must be set")

        return value.strip()

    return KafkaSettings(
        bootstrap_servers=required("ROUTEWATCH_KAFKA_BOOTSTRAP_SERVERS"),
        topic=required("ROUTEWATCH_KAFKA_TOPIC"),
    )


@dataclass(frozen=True)
class PostgresSettings:
    host: str
    port: int
    dbname: str
    user: str
    password: str = field(repr=False)


def load_postgres_settings(
    env_file: str | Path = ".env",
) -> PostgresSettings:
    values = {
        **dotenv_values(env_file),
        **os.environ,
    }

    def required(name: str, *, strip: bool = True) -> str:
        value = values.get(name)

        if value is None or not value.strip():
            raise ValueError(f"{name} must be set")

        return value.strip() if strip else value

    port_name = "ROUTEWATCH_POSTGRES_PORT"

    try:
        port = int(required(port_name))
    except ValueError:
        raise ValueError(f"{port_name} must be an integer from 1 to 65535") from None

    if not 1 <= port <= 65535:
        raise ValueError(f"{port_name} must be an integer from 1 to 65535")

    return PostgresSettings(
        host=required("ROUTEWATCH_POSTGRES_HOST"),
        port=port,
        dbname=required("ROUTEWATCH_POSTGRES_DB"),
        user=required("ROUTEWATCH_POSTGRES_USER"),
        password=required("ROUTEWATCH_POSTGRES_PASSWORD", strip=False),
    )


def load_consumer_group_id(env_file: str | Path = ".env") -> str:
    values = {
        **dotenv_values(env_file),
        **os.environ,
    }

    name = "ROUTEWATCH_KAFKA_GROUP_ID"
    value = values.get(name)

    if value is None or not value.strip():
        raise ValueError(f"{name} must be set")

    return value.strip()
