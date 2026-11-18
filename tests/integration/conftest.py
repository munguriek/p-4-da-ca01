import uuid

import psycopg
import pytest
from confluent_kafka.admin import AdminClient, NewTopic
from psycopg import sql
from psycopg.conninfo import make_conninfo

from airdeall.migrations import migrate
from airdeall.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    database_connection_info,
    load_settings,
)
from airdeall.sink import start_run


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
            admin.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name))
            )


@pytest.fixture
def migrated_db(fresh_db, pytestconfig):
    """A fresh database with every migration applied"""
    migrate(fresh_db, pytestconfig.rootpath / "sql" / "migrations")
    return fresh_db


@pytest.fixture
def run_id(migrated_db):
    """A run row for the test to count against."""
    return start_run(migrated_db, "test")


@pytest.fixture
def fresh_topic():
    """A new single-partition topic of its own, deleted after the test."""
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    name = f"airdeall-test-{uuid.uuid4().hex[:12]}"
    created = admin.create_topics([NewTopic(name, num_partitions=1)])
    created[name].result(timeout=30)
    yield name
    admin.delete_topics([name])[name].result(timeout=30)
