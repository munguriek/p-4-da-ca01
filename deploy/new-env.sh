#!/usr/bin/env bash
# Writes a new routewatch .env with fresh secrets, readable by its owner only.
#   bash deploy/new-env.sh PATH
# It never replaces an existing file: a running stack's volumes keep the
# cluster ID and passwords they were created with. Keys match .env.example.
# Needs Docker (for Kafka's own cluster-ID tool) and python3.

set -euo pipefail

target="${1:?usage: new-env.sh PATH}"
if [[ -e "$target" ]]; then
  echo "new-env.sh: $target already exists; it is left unchanged" >&2
  exit 1
fi

secret() {
  python3 -c 'import secrets; print(secrets.token_urlsafe(24))'
}

cluster_id="$(docker run --rm apache/kafka:4.4.0 /opt/kafka/bin/kafka-storage.sh random-uuid)"
owner_password="$(secret)"
ui_password="$(secret)"

umask 077
set -o noclobber
cat > "$target" <<EOF
# routewatch settings, written by deploy/new-env.sh. Never commit this file.
ROUTEWATCH_KAFKA_CLUSTER_ID=$cluster_id
ROUTEWATCH_KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:39092
ROUTEWATCH_KAFKA_TOPIC=routewatch.ris.raw
ROUTEWATCH_KAFKA_GROUP_ID=routewatch.raw.v1
ROUTEWATCH_POSTGRES_PASSWORD=$owner_password
ROUTEWATCH_POSTGRES_HOST=127.0.0.1
ROUTEWATCH_POSTGRES_PORT=5434
ROUTEWATCH_POSTGRES_DB=routewatch
ROUTEWATCH_POSTGRES_USER=routewatch
ROUTEWATCH_UI_POSTGRES_USER=routewatch_ui
ROUTEWATCH_UI_POSTGRES_PASSWORD=$ui_password
EOF
echo "new-env.sh: wrote $target (mode 600)"
