import psycopg

from .config import PostgresSettings


def connect_postgres(settings: PostgresSettings) -> psycopg.Connection:
    """Open a connection; writers will use explicit transaction blocks."""
    return psycopg.connect(
        host=settings.host,
        port=settings.port,
        dbname=settings.dbname,
        user=settings.user,
        password=settings.password,
        connect_timeout=5,
        application_name="routewatch",
        autocommit=True,
    )
