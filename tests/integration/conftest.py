import uuid


import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo


from airdeall.settings import database_connection_info, load_settings


@pytest.fixture
def fresh_db(pytestconfig):
    """A connection to an empty database"""
    conninfo = database_connection_info(load_settings(pytestconfig.rootpath / ".env"))
    name = f"airdeall_test_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(conninfo, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        try:
            test_conninfo = make_conninfo(conninfo, dbname=name)
            with psycopg.connect(test_conninfo, autocommit=True) as conn:
                yield conn
        finally:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
