import pytest

from airdeall.routewatch.config import load_postgres_settings
from airdeall.routewatch.db import connect_postgres

pytestmark = pytest.mark.integration


def test_connects_to_routewatch_database() -> None:
    settings = load_postgres_settings()

    with connect_postgres(settings) as connection:
        assert connection.autocommit is True

        row = connection.execute("SELECT current_database(), current_user").fetchone()

        assert row == (settings.dbname, settings.user)
