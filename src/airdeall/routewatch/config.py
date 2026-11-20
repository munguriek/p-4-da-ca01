import os
from dataclasses import dataclass
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
