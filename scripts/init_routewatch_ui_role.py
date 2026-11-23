"""Create or update the read-only PostgreSQL role the web UI connects as.

Safe to rerun: it creates the role if missing, resets its password from .env,
and grants SELECT on the RouteWatch schemas, including tables created later.
"""

from psycopg import sql

from routewatch.config import load_postgres_settings
from routewatch.db import connect_postgres
from routewatch.web import load_ui_postgres_settings

SCHEMAS = ("routewatch_raw", "routewatch_clean")


def main() -> None:
    try:
        owner = load_postgres_settings()
        ui = load_ui_postgres_settings()
    except ValueError as error:
        raise SystemExit(f"{error} (see .env.example)") from None
    role = sql.Identifier(ui.user)

    with connect_postgres(owner) as connection, connection.transaction():
        exists = connection.execute(
            "SELECT 1 FROM pg_roles WHERE rolname = %s", (ui.user,)
        ).fetchone()
        if exists is None:
            connection.execute(
                sql.SQL(
                    "CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
                    "NOREPLICATION NOBYPASSRLS"
                ).format(role)
            )

        # Hash on this side, so the plain password never reaches the server.
        verifier = connection.pgconn.encrypt_password(
            ui.password.encode(), ui.user.encode(), b"scram-sha-256"
        ).decode()
        connection.execute(
            sql.SQL("ALTER ROLE {} WITH LOGIN PASSWORD {}").format(
                role, sql.Literal(verifier)
            )
        )
        for setting, value in (
            ("default_transaction_read_only", "on"),
            ("statement_timeout", "5s"),
            ("idle_in_transaction_session_timeout", "10s"),
        ):
            connection.execute(
                sql.SQL("ALTER ROLE {} SET {} = {}").format(
                    role, sql.Identifier(setting), sql.Literal(value)
                )
            )
        connection.execute(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                sql.Identifier(owner.dbname), role
            )
        )
        for name in SCHEMAS:
            schema = sql.Identifier(name)
            connection.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(schema))
            connection.execute(
                sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(schema, role)
            )
            connection.execute(
                sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(
                    schema, role
                )
            )
            connection.execute(
                sql.SQL(
                    "ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT SELECT ON TABLES TO {}"
                ).format(schema, role)
            )

    print(f"Read-only role ready: {ui.user} (SELECT on {', '.join(SCHEMAS)})")


if __name__ == "__main__":
    main()
