import json
from dataclasses import asdict, fields
from ipaddress import ip_address, ip_network
from typing import Any

import psycopg
from psycopg import sql
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row
from psycopg.types.json import Jsonb

from .transform import ASPathFeatures, CleanEvent, CommunityFeatures, TransformResult


def initialize_clean_schema(
    connection: psycopg.Connection,
    *,
    schema_name: str = "routewatch_clean",
    raw_schema_name: str = "routewatch_raw",
) -> None:
    """Create the clean tables inside the caller's transaction."""
    if connection.info.transaction_status != TransactionStatus.INTRANS:
        raise RuntimeError("initialize_clean_schema requires an active transaction")

    processing = sql.Identifier(schema_name, "processing")
    events = sql.Identifier(schema_name, "prefix_events")
    raw_messages = sql.Identifier(raw_schema_name, "messages")

    connection.execute(
        sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name))
    )
    connection.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {processing} (
                kafka_cluster_id TEXT NOT NULL,
                topic TEXT NOT NULL,
                kafka_partition INTEGER NOT NULL,
                kafka_offset BIGINT NOT NULL,
                transform_version INTEGER NOT NULL CHECK (transform_version > 0),
                status TEXT NOT NULL
                    CHECK (status IN ('cleaned', 'empty', 'ignored', 'rejected')),
                event_count INTEGER NOT NULL CHECK (event_count >= 0),
                error_message TEXT,
                processed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                PRIMARY KEY (
                    kafka_cluster_id, topic, kafka_partition,
                    kafka_offset, transform_version
                ),
                FOREIGN KEY (
                    kafka_cluster_id, topic, kafka_partition, kafka_offset
                ) REFERENCES {raw_messages} (
                    kafka_cluster_id, topic, kafka_partition, kafka_offset
                ),
                CHECK ((status = 'cleaned') = (event_count > 0)),
                CHECK ((status = 'rejected') = (error_message IS NOT NULL)),
                CHECK (error_message IS NULL OR btrim(error_message) <> '')
            )
            """
        ).format(processing=processing, raw_messages=raw_messages)
    )
    connection.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {events} (
                kafka_cluster_id TEXT NOT NULL,
                topic TEXT NOT NULL,
                kafka_partition INTEGER NOT NULL,
                kafka_offset BIGINT NOT NULL,
                transform_version INTEGER NOT NULL,
                action_index INTEGER NOT NULL CHECK (action_index >= 0),
                source_id TEXT NOT NULL,
                event_ts TIMESTAMPTZ NOT NULL,
                collector_host TEXT NOT NULL,
                peer_ip INET NOT NULL,
                peer_asn BIGINT NOT NULL
                    CHECK (peer_asn BETWEEN 0 AND 4294967295),
                action_type TEXT NOT NULL
                    CHECK (action_type IN ('ANNOUNCE', 'WITHDRAW')),
                prefix CIDR NOT NULL,
                ip_version SMALLINT NOT NULL,
                prefix_length SMALLINT NOT NULL,
                next_hops INET[] NOT NULL,
                prefix_was_normalized BOOLEAN NOT NULL,
                as_path JSONB CHECK (jsonb_typeof(as_path) = 'array'),
                as_path_length INTEGER CHECK (as_path_length >= 0),
                unique_as_count INTEGER CHECK (unique_as_count >= 0),
                prepending_count INTEGER CHECK (prepending_count >= 0),
                origin_asn BIGINT
                    CHECK (origin_asn BETWEEN 0 AND 4294967295),
                has_as_set BOOLEAN,
                has_private_asn BOOLEAN,
                has_nonconsecutive_repeat BOOLEAN,
                origin TEXT CHECK (origin IN ('IGP', 'EGP', 'INCOMPLETE')),
                communities JSONB CHECK (jsonb_typeof(communities) = 'array'),
                community_count INTEGER CHECK (community_count >= 0),
                duplicate_community_count INTEGER
                    CHECK (duplicate_community_count >= 0),
                has_blackhole_community BOOLEAN,
                PRIMARY KEY (
                    kafka_cluster_id, topic, kafka_partition,
                    kafka_offset, transform_version, action_index
                ),
                FOREIGN KEY (
                    kafka_cluster_id, topic, kafka_partition,
                    kafka_offset, transform_version
                ) REFERENCES {processing} (
                    kafka_cluster_id, topic, kafka_partition,
                    kafka_offset, transform_version
                ),
                CHECK (
                    ip_version = family(prefix)
                    AND prefix_length = masklen(prefix)
                ),
                CHECK (
                    action_type = 'ANNOUNCE' OR (
                        cardinality(next_hops) = 0
                        AND num_nonnulls(
                            as_path, as_path_length, unique_as_count,
                            prepending_count, origin_asn, has_as_set,
                            has_private_asn, has_nonconsecutive_repeat,
                            origin, communities, community_count,
                            duplicate_community_count, has_blackhole_community
                        ) = 0
                    )
                )
            )
            """
        ).format(events=events, processing=processing)
    )


class CleanReplayConflict(RuntimeError):
    """Stored output differs at the same raw position and version."""


_EVENT_COLUMNS = (
    "action_index",
    "source_id",
    "event_ts",
    "collector_host",
    "peer_ip",
    "peer_asn",
    "action_type",
    "prefix",
    "ip_version",
    "prefix_length",
    "next_hops",
    "prefix_was_normalized",
    "as_path",
    "as_path_length",
    "unique_as_count",
    "prepending_count",
    "origin_asn",
    "has_as_set",
    "has_private_asn",
    "has_nonconsecutive_repeat",
    "origin",
    "communities",
    "community_count",
    "duplicate_community_count",
    "has_blackhole_community",
)


def _event_row(event: CleanEvent) -> tuple[Any, ...]:
    values = {
        **asdict(event.metadata),
        **asdict(event.action),
        **{
            field.name: getattr(event.path_features, field.name, None)
            for field in fields(ASPathFeatures)
        },
        "origin": event.origin,
        **{
            field.name: getattr(event.community_features, field.name, None)
            for field in fields(CommunityFeatures)
        },
    }
    values["peer_ip"] = ip_address(values["peer_ip"])
    values["prefix"] = ip_network(values["prefix"])
    values["next_hops"] = [ip_address(value) for value in values["next_hops"]]

    for name in ("as_path", "communities"):
        if values[name] is not None:
            values[name] = json.loads(json.dumps(values[name], allow_nan=False))

    return tuple(values[name] for name in _EVENT_COLUMNS)


def _adapt_event_row(row: tuple[Any, ...]) -> tuple[Any, ...]:
    return tuple(
        Jsonb(value)
        if name in {"as_path", "communities"} and value is not None
        else value
        for name, value in zip(_EVENT_COLUMNS, row, strict=True)
    )


def store_clean_result(
    connection: psycopg.Connection,
    result: TransformResult,
    *,
    kafka_cluster_id: str,
    topic: str,
    partition: int,
    offset: int,
    transform_version: int = 1,
    schema_name: str = "routewatch_clean",
) -> bool:
    """Stage one result atomically; return False for an identical replay."""
    if connection.info.transaction_status != TransactionStatus.INTRANS:
        raise RuntimeError("store_clean_result requires an active transaction")

    key = (kafka_cluster_id, topic, partition, offset, transform_version)
    rows = [
        _event_row(event)
        for event in sorted(result.events, key=lambda event: event.action.action_index)
    ]
    processing = sql.Identifier(schema_name, "processing")
    events = sql.Identifier(schema_name, "prefix_events")
    columns = sql.SQL(", ").join(map(sql.Identifier, _EVENT_COLUMNS))
    condition = sql.SQL(
        "(kafka_cluster_id, topic, kafka_partition, kafka_offset, transform_version)"
        " = (%s, %s, %s, %s, %s)"
    )

    with connection.transaction(), connection.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            sql.SQL(
                """
                INSERT INTO {processing} (
                    kafka_cluster_id, topic, kafka_partition, kafka_offset,
                    transform_version, status, event_count
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (
                    kafka_cluster_id, topic, kafka_partition,
                    kafka_offset, transform_version
                ) DO NOTHING
                RETURNING 1
                """
            ).format(processing=processing),
            key + (result.status, len(rows)),
        )

        if cursor.fetchone() is None:
            cursor.execute(
                sql.SQL(
                    "SELECT status, event_count, error_message "
                    "FROM {processing} WHERE {condition} FOR UPDATE"
                ).format(processing=processing, condition=condition),
                key,
            )
            if cursor.fetchone() != (result.status, len(rows), None):
                raise CleanReplayConflict("Stored processing outcome differs")

            cursor.execute(
                sql.SQL(
                    "SELECT {columns} FROM {events} WHERE {condition} "
                    "ORDER BY action_index"
                ).format(columns=columns, events=events, condition=condition),
                key,
            )
            if cursor.fetchall() != rows:
                raise CleanReplayConflict("Stored prefix events differ")
            return False

        cursor.executemany(
            sql.SQL(
                """
                INSERT INTO {events} (
                    kafka_cluster_id, topic, kafka_partition, kafka_offset,
                    transform_version, {columns}
                ) VALUES (%s, %s, %s, %s, %s, {placeholders})
                """
            ).format(
                events=events,
                columns=columns,
                placeholders=sql.SQL(", ").join(
                    sql.Placeholder() for _ in _EVENT_COLUMNS
                ),
            ),
            [key + _adapt_event_row(row) for row in rows],
        )
        return True


def store_rejected_message(
    connection: psycopg.Connection,
    reason: str,
    *,
    kafka_cluster_id: str,
    topic: str,
    partition: int,
    offset: int,
    transform_version: int = 1,
    schema_name: str = "routewatch_clean",
) -> bool:
    """Stage a rejection receipt; return False for an identical replay."""
    if connection.info.transaction_status != TransactionStatus.INTRANS:
        raise RuntimeError("store_rejected_message requires an active transaction")

    key = (kafka_cluster_id, topic, partition, offset, transform_version)
    processing = sql.Identifier(schema_name, "processing")
    events = sql.Identifier(schema_name, "prefix_events")
    condition = sql.SQL(
        "(kafka_cluster_id, topic, kafka_partition, kafka_offset, transform_version)"
        " = (%s, %s, %s, %s, %s)"
    )

    with connection.transaction(), connection.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            sql.SQL(
                """
                INSERT INTO {processing} (
                    kafka_cluster_id, topic, kafka_partition, kafka_offset,
                    transform_version, status, event_count, error_message
                ) VALUES (%s, %s, %s, %s, %s, 'rejected', 0, %s)
                ON CONFLICT (
                    kafka_cluster_id, topic, kafka_partition,
                    kafka_offset, transform_version
                ) DO NOTHING
                RETURNING 1
                """
            ).format(processing=processing),
            key + (reason,),
        )
        inserted = cursor.fetchone() is not None

        if not inserted:
            cursor.execute(
                sql.SQL(
                    "SELECT status, event_count, error_message "
                    "FROM {processing} WHERE {condition} FOR UPDATE"
                ).format(processing=processing, condition=condition),
                key,
            )
            if cursor.fetchone() != ("rejected", 0, reason):
                raise CleanReplayConflict("Stored rejection outcome differs")

            cursor.execute(
                sql.SQL("SELECT 1 FROM {events} WHERE {condition} LIMIT 1").format(
                    events=events, condition=condition
                ),
                key,
            )
            if cursor.fetchone() is not None:
                raise CleanReplayConflict("Rejected message has stored events")

        return inserted
