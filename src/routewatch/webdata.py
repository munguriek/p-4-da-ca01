"""Read-only queries behind the web UI's data page.

Every function takes an open connection from the UI's read-only role and
returns plain dictionaries that the web server turns into JSON. Pages are
newest first and use keyset pagination on the tables' primary keys, so a page
never skips or repeats rows while new ones arrive.
"""

import base64
import binascii
import json
from datetime import UTC, date, datetime
from decimal import Decimal
from ipaddress import ip_network
from typing import Any
from uuid import UUID

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

RAW_MESSAGES = sql.Identifier("routewatch_raw", "messages")
PROCESSING = sql.Identifier("routewatch_clean", "processing")
PREFIX_EVENTS = sql.Identifier("routewatch_clean", "prefix_events")

DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200
PREVIEW_CHARS = 160
MAX_PAYLOAD_BYTES = 262_144
MAX_EVENTS_PER_MESSAGE = 1_000


class BadRequest(ValueError):
    """A query parameter the data page cannot use."""


# --- JSON -----------------------------------------------------------------


def utc_text(value: datetime) -> str:
    """ISO 8601 in UTC with a Z suffix, to the millisecond."""
    return (
        value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    )


def json_default(value: Any) -> Any:
    """Convert database values that json.dumps cannot encode."""
    if isinstance(value, datetime):
        return utc_text(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, bytes | bytearray | memoryview):
        return bytes(value).hex()
    if isinstance(value, set | frozenset | tuple):
        return list(value)
    # inet, cidr and their arrays arrive as ipaddress objects
    return str(value)


# --- Parameters -----------------------------------------------------------


def first(params: dict[str, list[str]], name: str) -> str | None:
    values = params.get(name)
    if not values:
        return None
    value = values[0].strip()
    return value or None


def page_size(params: dict[str, list[str]]) -> int:
    text = first(params, "limit")
    if text is None:
        return DEFAULT_PAGE_SIZE
    try:
        size = int(text)
    except ValueError:
        raise BadRequest("limit must be a whole number") from None
    if not 1 <= size <= MAX_PAGE_SIZE:
        raise BadRequest(f"limit must be from 1 to {MAX_PAGE_SIZE}")
    return size


def whole_number(
    params: dict[str, list[str]],
    name: str,
    *,
    low: int = 0,
    high: int = 2**63 - 1,
) -> int | None:
    text = first(params, name)
    if text is None:
        return None
    try:
        number = int(text)
    except ValueError:
        raise BadRequest(f"{name} must be a whole number") from None
    if not low <= number <= high:
        raise BadRequest(f"{name} must be from {low} to {high}")
    return number


def encode_cursor(key: tuple[Any, ...]) -> str:
    text = json.dumps(list(key), separators=(",", ":"))
    return base64.urlsafe_b64encode(text.encode()).decode().rstrip("=")


def decode_cursor(text: str, *, kinds: tuple[type, ...]) -> tuple[Any, ...]:
    """Decode a cursor this module issued, checking its shape."""
    try:
        padded = text + "=" * (-len(text) % 4)
        values = json.loads(base64.urlsafe_b64decode(padded.encode()))
    except (binascii.Error, ValueError, UnicodeDecodeError):
        raise BadRequest("cursor is not valid") from None
    if not isinstance(values, list) or len(values) != len(kinds):
        raise BadRequest("cursor is not valid")
    for value, kind in zip(values, kinds, strict=True):
        if isinstance(value, bool) or not isinstance(value, kind):
            raise BadRequest("cursor is not valid")
    return tuple(values)


def page_direction(
    params: dict[str, list[str]],
    *,
    kinds: tuple[type, ...],
) -> tuple[str | None, tuple[Any, ...] | None]:
    before = first(params, "before")
    after = first(params, "after")
    if before and after:
        raise BadRequest("use before or after, not both")
    if before:
        return "older", decode_cursor(before, kinds=kinds)
    if after:
        return "newer", decode_cursor(after, kinds=kinds)
    return None, None


# --- Payloads -------------------------------------------------------------


def preview_text(head: bytes) -> str:
    text = head.decode("utf-8", errors="replace")
    text = " ".join(text.split())
    if len(text) > PREVIEW_CHARS:
        return text[: PREVIEW_CHARS - 1] + "…"
    return text


INLINE_WIDTH = 88


def _inline(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(", ", ": "))


def _is_flat(value: list) -> bool:
    """Scalars, or pairs of scalars such as BGP communities."""

    def scalar(item: Any) -> bool:
        return not isinstance(item, dict | list)

    return all(
        scalar(item) or (isinstance(item, list) and all(map(scalar, item)))
        for item in value
    )


def pretty_json(value: Any, indent: int = 0) -> str:
    """Indented JSON that keeps short lists, like AS paths, on one line."""
    pad = "  " * (indent + 1)
    end = "  " * indent
    if isinstance(value, dict):
        if not value:
            return "{}"
        items = [
            f"{pad}{json.dumps(key, ensure_ascii=False)}: {pretty_json(item, indent + 1)}"
            for key, item in value.items()
        ]
        return "{\n" + ",\n".join(items) + "\n" + end + "}"
    if isinstance(value, list):
        if not value:
            return "[]"
        if _is_flat(value):
            text = _inline(value)
            if len(pad) + len(text) <= INLINE_WIDTH:
                return text
            lines: list[str] = []
            line = ""
            for item in value:
                piece = _inline(item)
                if line and len(pad) + len(line) + len(piece) + 2 > INLINE_WIDTH:
                    lines.append(line + ",")
                    line = piece
                else:
                    line = f"{line}, {piece}" if line else piece
            lines.append(line)
            return "[\n" + "\n".join(pad + part for part in lines) + "\n" + end + "]"
        items = [f"{pad}{pretty_json(item, indent + 1)}" for item in value]
        return "[\n" + ",\n".join(items) + "\n" + end + "]"
    return json.dumps(value, ensure_ascii=False)


def describe_payload(payload: bytes, *, size: int) -> dict[str, Any]:
    """Show stored bytes exactly, and pretty JSON when they decode cleanly."""
    truncated = size > len(payload)
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError:
        return {
            "encoding": "hex",
            "text": payload[:4096].hex(" "),
            "pretty": None,
            "truncated": truncated or len(payload) > 4096,
        }

    pretty = None
    if not truncated:
        try:
            pretty = pretty_json(json.loads(text))
        except ValueError:
            pretty = None

    return {
        "encoding": "utf-8",
        "text": text,
        "pretty": pretty,
        "truncated": truncated,
    }


# --- Raw messages ---------------------------------------------------------

RAW_KEY = (str, str, int, int)
RAW_ORDER = (
    "r.kafka_cluster_id",
    "r.topic",
    "r.kafka_partition",
    "r.kafka_offset",
)


def _order_by(columns: tuple[str, ...], direction: str) -> sql.Composable:
    keyword = sql.SQL(direction)
    return sql.SQL(", ").join(
        sql.SQL("{} {}").format(sql.SQL(column), keyword) for column in columns
    )


def _row_compare(columns: tuple[str, ...], operator: str) -> sql.Composable:
    return sql.SQL("({}) {} ({})").format(
        sql.SQL(", ").join(sql.SQL(column) for column in columns),
        sql.SQL(operator),
        sql.SQL(", ").join(sql.Placeholder() for _ in columns),
    )


def _raw_row(row: dict[str, Any]) -> dict[str, Any]:
    timestamp_ms = row["kafka_timestamp_ms"]
    return {
        "key": encode_cursor(
            (
                row["kafka_cluster_id"],
                row["topic"],
                row["kafka_partition"],
                row["kafka_offset"],
            )
        ),
        "cluster_id": row["kafka_cluster_id"],
        "topic": row["topic"],
        "partition": row["kafka_partition"],
        "offset": row["kafka_offset"],
        "received_at": row["received_at"],
        "stored_at": row["stored_at"],
        "kafka_timestamp": (
            None
            if timestamp_ms is None
            else utc_text(datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC))
        ),
        "run_id": row["run_id"],
        "size_bytes": row["size_bytes"],
        "preview": preview_text(bytes(row["head"])),
        "status": row["status"],
        "event_count": row["event_count"],
        "error_message": row["error_message"],
    }


def fetch_raw_page(
    connection: psycopg.Connection,
    params: dict[str, list[str]],
    *,
    topic: str,
    transform_version: int,
) -> dict[str, Any]:
    limit = page_size(params)
    direction, cursor = page_direction(params, kinds=RAW_KEY)

    conditions = [sql.SQL("r.topic = %s")]
    values: list[Any] = [topic]
    order = "DESC"
    if direction == "older":
        conditions.append(_row_compare(RAW_ORDER, "<"))
        values.extend(cursor or ())
    elif direction == "newer":
        conditions.append(_row_compare(RAW_ORDER, ">"))
        values.extend(cursor or ())
        order = "ASC"

    query = sql.SQL(
        """
        SELECT r.kafka_cluster_id, r.topic, r.kafka_partition, r.kafka_offset,
               r.received_at, r.stored_at, r.kafka_timestamp_ms, r.run_id,
               octet_length(r.payload) AS size_bytes,
               substring(r.payload FROM 1 FOR {head_bytes}) AS head,
               p.status, p.event_count, p.error_message
        FROM {raw} AS r
        LEFT JOIN {processing} AS p
          ON p.kafka_cluster_id = r.kafka_cluster_id
         AND p.topic = r.topic
         AND p.kafka_partition = r.kafka_partition
         AND p.kafka_offset = r.kafka_offset
         AND p.transform_version = {version}
        WHERE {conditions}
        ORDER BY {order}
        LIMIT {limit}
        """
    ).format(
        head_bytes=sql.Literal(PREVIEW_CHARS * 4),
        raw=RAW_MESSAGES,
        processing=PROCESSING,
        version=sql.Literal(transform_version),
        conditions=sql.SQL(" AND ").join(conditions),
        order=_order_by(RAW_ORDER, order),
        limit=sql.Literal(limit + 1),
    )

    with connection.cursor(row_factory=dict_row) as cur:
        rows = cur.execute(query, values).fetchall()

    return _page(rows, limit=limit, direction=direction, convert=_raw_row)


def _page(rows, *, limit, direction, convert) -> dict[str, Any]:
    """Trim the probe row and report whether older or newer pages exist."""
    more = len(rows) > limit
    rows = rows[:limit]
    if direction == "newer":
        rows.reverse()
    converted = [convert(row) for row in rows]

    if direction == "newer":
        has_newer, has_older = more, True
    elif direction == "older":
        has_newer, has_older = True, more
    else:
        has_newer, has_older = False, more

    return {
        "rows": converted,
        "has_newer": has_newer and bool(converted),
        "has_older": has_older and bool(converted),
        "newer_cursor": converted[0]["key"] if converted else None,
        "older_cursor": converted[-1]["key"] if converted else None,
    }


def parse_message_key(params: dict[str, list[str]]) -> tuple[str, str, int, int]:
    key = first(params, "key")
    if key is not None:
        return decode_cursor(key, kinds=RAW_KEY)  # type: ignore[return-value]

    cluster_id = first(params, "cluster_id")
    topic = first(params, "topic")
    partition = whole_number(params, "partition", high=2**31 - 1)
    offset = whole_number(params, "offset")
    if cluster_id is None or topic is None or partition is None or offset is None:
        raise BadRequest("give key, or cluster_id, topic, partition and offset")
    return cluster_id, topic, partition, offset


def fetch_raw_message(
    connection: psycopg.Connection,
    params: dict[str, list[str]],
    *,
    transform_version: int,
) -> dict[str, Any] | None:
    """One raw message, its processing receipt and the clean rows it produced."""
    key = parse_message_key(params)
    where = _row_compare(RAW_ORDER, "=")

    with connection.cursor(row_factory=dict_row) as cur:
        message = cur.execute(
            sql.SQL(
                """
                SELECT r.kafka_cluster_id, r.topic, r.kafka_partition,
                       r.kafka_offset, r.received_at, r.stored_at,
                       r.kafka_timestamp_ms, r.run_id,
                       octet_length(r.payload) AS size_bytes,
                       substring(r.payload FROM 1 FOR {head_bytes}) AS head,
                       substring(r.payload FROM 1 FOR {max_bytes}) AS payload
                FROM {raw} AS r
                WHERE {where}
                """
            ).format(
                head_bytes=sql.Literal(PREVIEW_CHARS * 4),
                max_bytes=sql.Literal(MAX_PAYLOAD_BYTES),
                raw=RAW_MESSAGES,
                where=where,
            ),
            key,
        ).fetchone()

        if message is None:
            return None

        receipt = cur.execute(
            sql.SQL(
                """
                SELECT transform_version, status, event_count, error_message,
                       processed_at
                FROM {processing}
                WHERE (kafka_cluster_id, topic, kafka_partition, kafka_offset)
                      = (%s, %s, %s, %s)
                  AND transform_version = %s
                """
            ).format(processing=PROCESSING),
            (*key, transform_version),
        ).fetchone()

        events = cur.execute(
            sql.SQL(
                """
                SELECT {columns}
                FROM {events} AS e
                WHERE (e.kafka_cluster_id, e.topic, e.kafka_partition,
                       e.kafka_offset) = (%s, %s, %s, %s)
                  AND e.transform_version = %s
                ORDER BY e.action_index
                LIMIT {limit}
                """
            ).format(
                columns=CLEAN_COLUMNS,
                events=PREFIX_EVENTS,
                limit=sql.Literal(MAX_EVENTS_PER_MESSAGE),
            ),
            (*key, transform_version),
        ).fetchall()

    summary = _raw_row(
        {**message, "status": None, "event_count": None, "error_message": None}
    )
    payload = bytes(message["payload"])
    return {
        "message": {
            **summary,
            "payload": describe_payload(payload, size=message["size_bytes"]),
        },
        "receipt": receipt,
        "events": [_clean_row(row) for row in events],
        "events_truncated": len(events) >= MAX_EVENTS_PER_MESSAGE,
    }


# --- Clean prefix events --------------------------------------------------

CLEAN_KEY = (str, str, int, int, int)
CLEAN_ORDER = (
    "e.kafka_cluster_id",
    "e.topic",
    "e.kafka_partition",
    "e.kafka_offset",
    "e.transform_version",
    "e.action_index",
)
CLEAN_COLUMNS = sql.SQL(
    """
    e.kafka_cluster_id, e.topic, e.kafka_partition, e.kafka_offset,
    e.transform_version, e.action_index, e.source_id, e.event_ts,
    e.collector_host, e.peer_ip, e.peer_asn, e.action_type, e.prefix,
    e.ip_version, e.prefix_length, e.next_hops, e.prefix_was_normalized,
    e.as_path, e.as_path_length, e.unique_as_count, e.prepending_count,
    e.origin_asn, e.has_as_set, e.has_private_asn,
    e.has_nonconsecutive_repeat, e.origin, e.communities, e.community_count,
    e.duplicate_community_count, e.has_blackhole_community
    """
)


def _clean_row(row: dict[str, Any]) -> dict[str, Any]:
    message_key = (
        row["kafka_cluster_id"],
        row["topic"],
        row["kafka_partition"],
        row["kafka_offset"],
    )
    return {
        "key": encode_cursor((*message_key, row["action_index"])),
        "message_key": encode_cursor(message_key),
        "cluster_id": row["kafka_cluster_id"],
        "topic": row["topic"],
        "partition": row["kafka_partition"],
        "offset": row["kafka_offset"],
        "transform_version": row["transform_version"],
        "action_index": row["action_index"],
        "source_id": row["source_id"],
        "event_ts": row["event_ts"],
        "collector_host": row["collector_host"],
        "peer_ip": str(row["peer_ip"]),
        "peer_asn": row["peer_asn"],
        "action_type": row["action_type"],
        "prefix": str(row["prefix"]),
        "ip_version": row["ip_version"],
        "prefix_length": row["prefix_length"],
        "next_hops": [str(hop) for hop in row["next_hops"] or ()],
        "prefix_was_normalized": row["prefix_was_normalized"],
        "as_path": row["as_path"],
        "as_path_length": row["as_path_length"],
        "unique_as_count": row["unique_as_count"],
        "prepending_count": row["prepending_count"],
        "origin_asn": row["origin_asn"],
        "has_as_set": row["has_as_set"],
        "has_private_asn": row["has_private_asn"],
        "has_nonconsecutive_repeat": row["has_nonconsecutive_repeat"],
        "origin": row["origin"],
        "communities": row["communities"],
        "community_count": row["community_count"],
        "duplicate_community_count": row["duplicate_community_count"],
        "has_blackhole_community": row["has_blackhole_community"],
    }


def clean_filters(params: dict[str, list[str]]) -> tuple[list[sql.Composable], list]:
    """Validated filter conditions for the clean table."""
    conditions: list[sql.Composable] = []
    values: list[Any] = []

    action = first(params, "action")
    if action is not None:
        action = action.upper()
        if action not in ("ANNOUNCE", "WITHDRAW"):
            raise BadRequest("action must be ANNOUNCE or WITHDRAW")
        conditions.append(sql.SQL("e.action_type = %s"))
        values.append(action)

    ip_version = whole_number(params, "ip_version", low=4, high=6)
    if ip_version is not None:
        if ip_version not in (4, 6):
            raise BadRequest("ip_version must be 4 or 6")
        conditions.append(sql.SQL("e.ip_version = %s"))
        values.append(ip_version)

    for name in ("origin_asn", "peer_asn"):
        asn = whole_number(params, name, high=4_294_967_295)
        if asn is not None:
            conditions.append(sql.SQL("{} = %s").format(sql.Identifier("e", name)))
            values.append(asn)

    prefix = first(params, "prefix")
    if prefix is not None:
        try:
            network = ip_network(prefix, strict=False)
        except ValueError:
            raise BadRequest(
                "prefix must be an IP network such as 192.0.2.0/24"
            ) from None
        conditions.append(sql.SQL("e.prefix <<= %s::cidr"))
        values.append(str(network))

    return conditions, values


def fetch_clean_page(
    connection: psycopg.Connection,
    params: dict[str, list[str]],
    *,
    topic: str,
    transform_version: int,
) -> dict[str, Any]:
    limit = page_size(params)
    direction, cursor = page_direction(params, kinds=CLEAN_KEY)
    filters, filter_values = clean_filters(params)

    conditions = [sql.SQL("e.topic = %s"), sql.SQL("e.transform_version = %s")]
    values: list[Any] = [topic, transform_version]
    conditions.extend(filters)
    values.extend(filter_values)

    order = "DESC"
    if direction is not None and cursor is not None:
        cluster_id, raw_topic, partition, offset, action_index = cursor
        operator = "<" if direction == "older" else ">"
        conditions.append(_row_compare(CLEAN_ORDER, operator))
        values.extend(
            (cluster_id, raw_topic, partition, offset, transform_version, action_index)
        )
        if direction == "newer":
            order = "ASC"

    query = sql.SQL(
        """
        SELECT {columns}
        FROM {events} AS e
        WHERE {conditions}
        ORDER BY {order}
        LIMIT {limit}
        """
    ).format(
        columns=CLEAN_COLUMNS,
        events=PREFIX_EVENTS,
        conditions=sql.SQL(" AND ").join(conditions),
        order=_order_by(CLEAN_ORDER, order),
        limit=sql.Literal(limit + 1),
    )

    with connection.cursor(row_factory=dict_row) as cur:
        rows = cur.execute(query, values).fetchall()

    return _page(rows, limit=limit, direction=direction, convert=_clean_row)


# --- Summary --------------------------------------------------------------

STATUSES = ("cleaned", "empty", "ignored", "rejected")

# Counting takes seconds once a table holds millions of rows, and the UI's
# role stops any statement after five. Past this many rows, by the planner's
# estimate, the summary reports estimates instead of counts.
EXACT_COUNT_LIMIT = 1_000_000

PROCESSING_ORDER = (
    "p.kafka_cluster_id",
    "p.topic",
    "p.kafka_partition",
    "p.kafka_offset",
    "p.transform_version",
)


def _estimated_rows(
    connection: psycopg.Connection,
    table: sql.Composable,
    condition: str,
    params: tuple[Any, ...],
) -> int:
    """The planner's estimate of the rows that match a condition.

    It costs one plan whatever the table's size, and it follows growth
    between ANALYZE runs, because the planner scales the table's statistics
    to its current size.
    """
    with connection.cursor() as cur:
        plan = cur.execute(
            sql.SQL("EXPLAIN (FORMAT JSON) SELECT 1 FROM {} WHERE {}").format(
                table, sql.SQL(condition)
            ),
            params,
        ).fetchone()[0]
    return int(plan[0]["Plan"]["Plan Rows"])


def _offset_gap(
    latest_raw: dict[str, Any] | None, latest_receipt: dict[str, Any] | None
) -> int | None:
    """Messages stored but not yet processed, from the newest offset of each.

    Exact within one partition, where offsets have no gaps; None when the two
    newest rows come from different partitions or clusters.
    """
    if latest_raw is None:
        return 0
    if latest_receipt is None:
        return None
    same_partition = all(
        latest_raw[key] == latest_receipt[key]
        for key in ("kafka_cluster_id", "kafka_partition")
    )
    gap = latest_raw["kafka_offset"] - latest_receipt["kafka_offset"]
    return gap if same_partition and gap >= 0 else None


def fetch_summary(
    connection: psycopg.Connection,
    *,
    topic: str,
    transform_version: int,
) -> dict[str, Any]:
    """Counts for the pipeline line at the top of the data page.

    Tables up to EXACT_COUNT_LIMIT rows are counted exactly. Past that, every
    count is the planner's estimate and "approximate" is true; the backlog
    comes from the newest raw and processed offsets, and the check that the
    receipts promise as many rows as the clean table holds is skipped
    ("receipts_match_rows" is None).
    """
    clean_rows = "topic = %s AND transform_version = %s"
    estimates = {
        "raw": _estimated_rows(connection, RAW_MESSAGES, "topic = %s", (topic,)),
        "receipts": _estimated_rows(
            connection, PROCESSING, clean_rows, (topic, transform_version)
        ),
        "events": _estimated_rows(
            connection, PREFIX_EVENTS, clean_rows, (topic, transform_version)
        ),
    }

    with connection.cursor(row_factory=dict_row) as cur:
        latest_raw = cur.execute(
            sql.SQL(
                """
                SELECT r.kafka_cluster_id, r.kafka_partition, r.kafka_offset,
                       r.received_at, r.stored_at
                FROM {raw} AS r
                WHERE r.topic = %s
                ORDER BY {order}
                LIMIT 1
                """
            ).format(raw=RAW_MESSAGES, order=_order_by(RAW_ORDER, "DESC")),
            (topic,),
        ).fetchone()

        latest_event = cur.execute(
            sql.SQL(
                """
                SELECT e.event_ts, e.collector_host
                FROM {events} AS e
                WHERE e.topic = %s AND e.transform_version = %s
                ORDER BY {order}
                LIMIT 1
                """
            ).format(events=PREFIX_EVENTS, order=_order_by(CLEAN_ORDER, "DESC")),
            (topic, transform_version),
        ).fetchone()

        if max(estimates.values()) > EXACT_COUNT_LIMIT:
            latest_receipt = cur.execute(
                sql.SQL(
                    """
                    SELECT p.kafka_cluster_id, p.kafka_partition, p.kafka_offset
                    FROM {processing} AS p
                    WHERE p.topic = %s AND p.transform_version = %s
                    ORDER BY {order}
                    LIMIT 1
                    """
                ).format(
                    processing=PROCESSING, order=_order_by(PROCESSING_ORDER, "DESC")
                ),
                (topic, transform_version),
            ).fetchone()
            by_status = {
                status: {
                    "messages": _estimated_rows(
                        connection,
                        PROCESSING,
                        f"{clean_rows} AND status = %s",
                        (topic, transform_version, status),
                    ),
                    # Only cleaned messages give rows.
                    "events": estimates["events"] if status == "cleaned" else 0,
                }
                for status in STATUSES
            }
            return {
                "topic": topic,
                "transform_version": transform_version,
                "approximate": True,
                "raw": {"messages": estimates["raw"], "latest": latest_raw},
                "processing": {
                    "receipts": estimates["receipts"],
                    "pending": _offset_gap(latest_raw, latest_receipt),
                    "by_status": by_status,
                    "promised_events": None,
                },
                "clean": {"events": estimates["events"], "latest": latest_event},
                "receipts_match_rows": None,
            }

        raw_count = cur.execute(
            sql.SQL("SELECT count(*) AS n FROM {} WHERE topic = %s").format(
                RAW_MESSAGES
            ),
            (topic,),
        ).fetchone()["n"]

        by_status = {status: {"messages": 0, "events": 0} for status in STATUSES}
        for row in cur.execute(
            sql.SQL(
                """
                SELECT status, count(*) AS messages,
                       coalesce(sum(event_count), 0) AS events
                FROM {}
                WHERE topic = %s AND transform_version = %s
                GROUP BY status
                """
            ).format(PROCESSING),
            (topic, transform_version),
        ):
            by_status[row["status"]] = {
                "messages": row["messages"],
                "events": int(row["events"]),
            }

        clean_count = cur.execute(
            sql.SQL(
                """
                SELECT count(*) AS n FROM {}
                WHERE topic = %s AND transform_version = %s
                """
            ).format(PREFIX_EVENTS),
            (topic, transform_version),
        ).fetchone()["n"]

    receipts = sum(part["messages"] for part in by_status.values())
    promised = sum(part["events"] for part in by_status.values())
    return {
        "topic": topic,
        "transform_version": transform_version,
        "approximate": False,
        "raw": {"messages": raw_count, "latest": latest_raw},
        "processing": {
            "receipts": receipts,
            "pending": max(raw_count - receipts, 0),
            "by_status": by_status,
            "promised_events": promised,
        },
        "clean": {"events": clean_count, "latest": latest_event},
        "receipts_match_rows": promised == clean_count,
    }
