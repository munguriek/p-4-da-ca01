"""Read-only Kafka views for the web UI: topics, offsets, the consumer group
and the most recent records of a partition.

Nothing here writes to Kafka. The peek consumer is assigned partitions by
hand and never commits, so it does not join or change any consumer group.
"""

from datetime import UTC, datetime
from time import monotonic
from typing import Any

from confluent_kafka import (
    TIMESTAMP_LOG_APPEND_TIME,
    TIMESTAMP_NOT_AVAILABLE,
    Consumer,
    ConsumerGroupTopicPartitions,
    KafkaError,
    KafkaException,
    TopicPartition,
)
from confluent_kafka.admin import (
    AdminClient,
    ConfigResource,
    OffsetSpec,
    ResourceType,
)

SHOWN_CONFIGS = (
    "cleanup.policy",
    "retention.ms",
    "retention.bytes",
    "segment.bytes",
    "segment.ms",
    "max.message.bytes",
)
PEEK_GROUP_ID = "routewatch.ui.peek"
MAX_PEEK = 50
PREVIEW_CHARS = 400
VALUE_CHARS = 65_536


class KafkaUnavailable(RuntimeError):
    """Kafka did not answer within the timeout."""


class PeekRequestError(ValueError):
    """The topic or partition to peek at does not exist."""


def _admin(bootstrap_servers: str, timeout: float) -> AdminClient:
    return AdminClient(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "routewatch-ui",
            "socket.timeout.ms": int(timeout * 1000),
        }
    )


def _metadata(admin: AdminClient, timeout: float):
    try:
        return admin.list_topics(timeout=timeout)
    except KafkaException as error:
        raise KafkaUnavailable(f"Kafka did not answer: {_error_text(error)}") from error


def _is_internal(name: str) -> bool:
    return name.startswith("__")


def _error_text(error: BaseException) -> str:
    if isinstance(error, KafkaException) and error.args:
        inner = error.args[0]
        if isinstance(inner, KafkaError):
            return inner.str()
    return str(error) or type(error).__name__


def _offsets(admin, partitions, spec, timeout) -> dict[tuple[str, int], int | None]:
    if not partitions:
        return {}
    futures = admin.list_offsets(
        {TopicPartition(topic, partition): spec for topic, partition in partitions},
        request_timeout=timeout,
    )
    result: dict[tuple[str, int], int | None] = {}
    for tp, future in futures.items():
        try:
            result[(tp.topic, tp.partition)] = future.result(timeout=timeout).offset
        except KafkaException:
            result[(tp.topic, tp.partition)] = None
    return result


def _configs(admin, names, timeout) -> dict[str, dict[str, Any]]:
    if not names:
        return {}
    resources = [ConfigResource(ResourceType.TOPIC, name) for name in names]
    futures = admin.describe_configs(resources, request_timeout=timeout)
    configs: dict[str, dict[str, Any]] = {}
    for resource, future in futures.items():
        try:
            entries = future.result(timeout=timeout)
        except KafkaException:
            configs[resource.name] = {}
            continue
        configs[resource.name] = {
            key: {"value": entries[key].value, "is_default": entries[key].is_default}
            for key in SHOWN_CONFIGS
            if key in entries
        }
    return configs


def _group(admin, group_id: str, timeout: float) -> dict[str, Any]:
    group: dict[str, Any] = {
        "group_id": group_id,
        "state": None,
        "members": None,
        "committed": {},
        "error": None,
    }
    try:
        description = admin.describe_consumer_groups(
            [group_id], request_timeout=timeout
        )[group_id].result(timeout=timeout)
        group["state"] = description.state.name.lower()
        group["members"] = len(description.members)
    except KafkaException as error:
        group["error"] = _error_text(error)

    try:
        offsets = admin.list_consumer_group_offsets(
            [ConsumerGroupTopicPartitions(group_id)], request_timeout=timeout
        )[group_id].result(timeout=timeout)
        for tp in offsets.topic_partitions or []:
            if tp.offset >= 0:
                group["committed"][(tp.topic, tp.partition)] = tp.offset
    except KafkaException as error:
        group["error"] = group["error"] or _error_text(error)

    return group


def describe_kafka(
    bootstrap_servers: str,
    *,
    group_id: str,
    include_internal: bool = False,
    timeout: float = 5.0,
) -> dict[str, Any]:
    """Topics, partitions, offsets, settings and one consumer group's progress."""
    admin = _admin(bootstrap_servers, timeout)
    metadata = _metadata(admin, timeout)

    names = sorted(
        name for name in metadata.topics if include_internal or not _is_internal(name)
    )
    partitions = [
        (name, partition)
        for name in names
        for partition in sorted(metadata.topics[name].partitions)
    ]
    earliest = _offsets(admin, partitions, OffsetSpec.earliest(), timeout)
    latest = _offsets(admin, partitions, OffsetSpec.latest(), timeout)
    configs = _configs(admin, names, timeout)
    group = _group(admin, group_id, timeout)
    committed = group.pop("committed")

    topics = []
    for name in names:
        topic = metadata.topics[name]
        rows = []
        for partition_id in sorted(topic.partitions):
            partition = topic.partitions[partition_id]
            low = earliest.get((name, partition_id))
            high = latest.get((name, partition_id))
            done = committed.get((name, partition_id))
            rows.append(
                {
                    "partition": partition_id,
                    "leader": partition.leader,
                    "replicas": list(partition.replicas),
                    "in_sync": list(partition.isrs),
                    "earliest_offset": low,
                    "next_offset": high,
                    "kept": (
                        None if low is None or high is None else max(high - low, 0)
                    ),
                    "committed_offset": done,
                    "lag": (
                        None if done is None or high is None else max(high - done, 0)
                    ),
                }
            )
        kept = [row["kept"] for row in rows]
        lags = [row["lag"] for row in rows if row["lag"] is not None]
        topics.append(
            {
                "name": name,
                "internal": _is_internal(name),
                "partitions": rows,
                "kept": None if None in kept else sum(kept),
                "group_lag": sum(lags) if lags else None,
                "config": configs.get(name, {}),
                "error": None if topic.error is None else topic.error.str(),
            }
        )

    return {
        "cluster_id": metadata.cluster_id,
        "controller_id": metadata.controller_id,
        "brokers": [
            {"id": broker.id, "host": broker.host, "port": broker.port}
            for broker in sorted(metadata.brokers.values(), key=lambda b: b.id)
        ],
        "topics": topics,
        "group": group,
    }


def _timestamp(message) -> dict[str, Any]:
    kind, millis = message.timestamp()
    if kind == TIMESTAMP_NOT_AVAILABLE:
        return {"at": None, "type": "none"}
    at = datetime.fromtimestamp(millis / 1000, tz=UTC)
    return {
        "at": at.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "type": "log_append" if kind == TIMESTAMP_LOG_APPEND_TIME else "create",
    }


def _text(value: bytes | None, limit: int) -> str | None:
    if value is None:
        return None
    text = value[: limit * 4].decode("utf-8", errors="replace")
    return text if len(text) <= limit else text[: limit - 1] + "…"


def peek_records(
    bootstrap_servers: str,
    *,
    topic: str,
    partition: int,
    limit: int,
    timeout: float = 5.0,
) -> dict[str, Any]:
    """The newest records of one partition, newest first."""
    if not 1 <= limit <= MAX_PEEK:
        raise PeekRequestError(f"limit must be from 1 to {MAX_PEEK}")

    admin = _admin(bootstrap_servers, timeout)
    metadata = _metadata(admin, timeout)
    found = metadata.topics.get(topic)
    if found is None or _is_internal(topic):
        raise PeekRequestError(f"There is no topic named {topic!r} to read.")
    if partition not in found.partitions:
        raise PeekRequestError(f"Topic {topic!r} has no partition {partition}.")

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": PEEK_GROUP_ID,
            "client.id": "routewatch-ui-peek",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "enable.partition.eof": True,
            "socket.timeout.ms": int(timeout * 1000),
        }
    )
    try:
        try:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=timeout
            )
        except KafkaException as error:
            raise KafkaUnavailable(
                f"Kafka did not answer: {_error_text(error)}"
            ) from error

        start = max(low, high - limit)
        records = []
        if high > start:
            consumer.assign([TopicPartition(topic, partition, start)])
            wanted = high - start
            deadline = monotonic() + timeout
            at_end = False
            while not at_end and len(records) < wanted and monotonic() < deadline:
                for message in consumer.consume(num_messages=wanted, timeout=0.5):
                    error = message.error()
                    if error is not None:
                        if error.code() == KafkaError._PARTITION_EOF:
                            at_end = True
                            break
                        raise KafkaUnavailable(f"Kafka read failed: {error.str()}")
                    value = message.value()
                    records.append(
                        {
                            "partition": message.partition(),
                            "offset": message.offset(),
                            "timestamp": _timestamp(message),
                            "key": _text(message.key(), 200),
                            "headers": [
                                [name, _text(raw, 200)]
                                for name, raw in (message.headers() or [])
                            ],
                            "size_bytes": 0 if value is None else len(value),
                            "preview": _text(value, PREVIEW_CHARS),
                            "value": _text(value, VALUE_CHARS),
                        }
                    )
    finally:
        consumer.close()

    records.sort(key=lambda record: record["offset"], reverse=True)
    return {
        "cluster_id": metadata.cluster_id,
        "topic": topic,
        "partition": partition,
        "earliest_offset": low,
        "next_offset": high,
        "records": records[:limit],
    }
