#!/usr/bin/env bash
# Creates RouteWatch topics. Existing topics are left unchanged.

set -euo pipefail

cd "$(dirname "$0")/.."

kafka_topics() {
  docker compose --env-file .env \
    -p airdeall-routewatch -f compose.routewatch.yaml \
    exec -T -e KAFKA_HEAP_OPTS=-Xmx256m routewatch-kafka \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 "$@"
}

create_topic() {
  local topic="$1"
  local partitions="$2"
  shift 2

  kafka_topics --create --if-not-exists \
    --topic "$topic" --partitions "$partitions" \
    --replication-factor 1 "$@"
}

routewatch_topic="$(python -c '
from airdeall.routewatch.config import load_kafka_settings
print(load_kafka_settings().topic)
')"

create_topic "$routewatch_topic" 1 \
  --config cleanup.policy=delete \
  --config retention.ms=172800000 \
  --config retention.bytes=2147483648 \
  --config segment.bytes=67108864 \
  --config segment.ms=3600000 \
  --config max.message.bytes=8388608

kafka_topics --describe --topic "$routewatch_topic"
kafka_topics --list --exclude-internal