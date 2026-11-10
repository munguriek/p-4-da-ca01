#!/usr/bin/env bash
# Creates the pipeline's Kafka topics. Safe to rerun: existing topics are left as they are.
set -euo pipefail

cd "$(dirname "$0")/.."

kafka_topics() {
  docker compose exec -T -e KAFKA_HEAP_OPTS=-Xmx256m pipeline-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 "$@"
}

create_topic() {
  kafka_topics --create --if-not-exists --topic "$1" --partitions "$2" --replication-factor 1
}

create_topic otel.spans 3
create_topic otel.metrics 3
create_topic otel.logs 3
create_topic demo.flag-changes 1

kafka_topics --list --exclude-internal
