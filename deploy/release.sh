#!/usr/bin/env bash
# routewatch's release tool on the VM. The pipeline copies a release bundle
# (both compose files, this script and release.env) to the VM and runs:
#   sudo bash BUNDLE/release.sh deploy BUNDLE
# Each release keeps a copy of this script, and vm-bootstrap.sh links the
# current one as /usr/local/bin/routewatch:
#   sudo routewatch status              releases, containers, disk and database size
#   sudo routewatch rollback            return to the previous release
#   sudo routewatch compose ARGS...     docker compose for the current release
#   sudo routewatch disk-guard          stop the collector when the disk is nearly full
#
# /opt/routewatch holds .env (secrets, mode 600), releases/<commit>-<image>/,
# the symlinks current and previous, and deploy.log. Kafka's log and
# PostgreSQL's data are volumes of the Compose project "routewatch": deploys
# and rollbacks keep them.

set -euo pipefail

ROOT="${ROUTEWATCH_ROOT:-/opt/routewatch}"
ENV_FILE="$ROOT/.env"
PROJECT=routewatch
UI=http://127.0.0.1:8765
DISK_PATH="${ROUTEWATCH_DISK_PATH:-/var/lib/docker}"
DISK_LIMIT="${ROUTEWATCH_DISK_LIMIT:-85}"          # percent used
SETTLE_SECONDS="${ROUTEWATCH_SETTLE_SECONDS:-20}"
FLOW_SECONDS="${ROUTEWATCH_FLOW_SECONDS:-180}"
FLOW_POLL_SECONDS="${ROUTEWATCH_FLOW_POLL_SECONDS:-10}"
SERVICES=(routewatch-kafka routewatch-db routewatch-collector routewatch-consumer
  routewatch-cleaner routewatch-ui)

say() { printf '%s\n' "$*"; }
die() {
  printf 'routewatch: %s\n' "$*" >&2
  exit 1
}
log() {
  printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" | tee -a "$ROOT/deploy.log"
}

require_root() { [[ $EUID -eq 0 ]] || die "run this with sudo"; }

take_lock() {
  exec 9>"$ROOT/.lock"
  flock -n 9 || die "another release command is running"
}

# KEY=value lines only; the files are never sourced.
value_of() { sed -n "s/^$2=//p" "$1" | tail -n 1; }

# The Cloudflare tunnel runs when .env holds its token (docs/deploy.md).
tunnel_enabled() { [[ -n "$(value_of "$ENV_FILE" ROUTEWATCH_TUNNEL_TOKEN)" ]]; }

compose() {
  local dir="$1"
  local -a profiles=()
  shift
  if tunnel_enabled; then profiles=(--profile tunnel); fi
  ROUTEWATCH_IMAGE="$(value_of "$dir/release.env" ROUTEWATCH_IMAGE)" \
    docker compose --project-name "$PROJECT" --project-directory "$ROOT" \
    --env-file "$ENV_FILE" "${profiles[@]}" \
    -f "$dir/compose.routewatch.yaml" -f "$dir/compose.app.yaml" "$@"
}

# Replace a symlink in one step, so it never points nowhere.
point() {
  ln -sfn "$2" "$ROOT/.$1.new"
  mv -T "$ROOT/.$1.new" "$ROOT/$1"
}

current_release() {
  if [[ -L $ROOT/current ]]; then readlink -f "$ROOT/current"; fi
}

disk_used() { df -P "$DISK_PATH" | awk 'NR == 2 { sub("%", "", $5); print $5 }'; }

psql_owner() {
  local dir="$1" sql="$2"
  compose "$dir" exec -T routewatch-db psql -XAtq -F ' ' \
    -U "$(value_of "$ENV_FILE" ROUTEWATCH_POSTGRES_USER)" \
    -d "$(value_of "$ENV_FILE" ROUTEWATCH_POSTGRES_DB)" -c "$sql"
}

show_state() {
  compose "$1" ps -a --format 'table {{.Service}}\t{{.State}}\t{{.Status}}' || true
  compose "$1" logs --no-color --tail 20 routewatch-collector routewatch-consumer \
    routewatch-cleaner routewatch-ui || true
}

# Every long-running service is still up, and has not restarted, after a pause.
check_stable() {
  local dir="$1" service id status count
  local -A restarts=()
  local -a services=("${SERVICES[@]}")
  if tunnel_enabled; then services+=(routewatch-tunnel); fi
  for service in "${services[@]}"; do
    id="$(compose "$dir" ps -a -q "$service")"
    [[ -n $id ]] || {
      say "check: $service has no container"
      return 1
    }
    restarts[$service]="$(docker inspect -f '{{.RestartCount}}' "$id")"
  done
  sleep "$SETTLE_SECONDS"
  for service in "${services[@]}"; do
    id="$(compose "$dir" ps -a -q "$service")"
    read -r status count < <(docker inspect -f '{{.State.Status}} {{.RestartCount}}' "$id")
    if [[ $status != running || $count != "${restarts[$service]}" ]]; then
      say "check: $service is $status and restarted $((count - restarts[$service])) time(s)"
      return 1
    fi
  done
  say "check: every service stayed up for $SETTLE_SECONDS s"
}

check_ui() {
  local path
  for path in /favicon.svg '/api/raw?limit=1' /api/kafka; do
    curl -fsS --max-time 15 -o /dev/null "$UI$path" || {
      say "check: GET $path failed"
      return 1
    }
  done
  say "check: the UI answers, reads PostgreSQL and reaches Kafka"
}

# Rows inserted since statistics began: cheap at any table size.
inserted_rows() {
  psql_owner "$1" "SELECT
    coalesce(sum(n_tup_ins) FILTER (WHERE schemaname = 'routewatch_raw' AND relname = 'messages'), 0),
    coalesce(sum(n_tup_ins) FILTER (WHERE schemaname = 'routewatch_clean' AND relname = 'processing'), 0)
    FROM pg_stat_user_tables"
}

# Live updates reach the raw table and the cleaner, through the new containers.
check_flow() {
  local dir="$1" raw0 clean0 raw clean waited=0
  read -r raw0 clean0 < <(inserted_rows "$dir") || {
    say "check: cannot read PostgreSQL's statistics"
    return 1
  }
  while ((waited < FLOW_SECONDS)); do
    sleep "$FLOW_POLL_SECONDS"
    waited=$((waited + FLOW_POLL_SECONDS))
    read -r raw clean < <(inserted_rows "$dir") || continue
    if ((raw > raw0 && clean > clean0)); then
      say "check: in $waited s, $((raw - raw0)) raw messages stored and $((clean - clean0)) cleaned"
      return 0
    fi
  done
  say "check: no new raw and clean rows in $FLOW_SECONDS s; is RIS Live reachable?" \
    "See: sudo routewatch compose logs routewatch-collector"
  return 1
}

activate() {
  local dir="$1" used
  say "== starting $(basename "$dir")"
  if ! compose "$dir" up -d --pull missing --wait --wait-timeout 300 --remove-orphans; then
    show_state "$dir"
    return 1
  fi
  if ! check_stable "$dir" || ! check_ui || ! check_flow "$dir"; then
    show_state "$dir"
    return 1
  fi
  used="$(disk_used)"
  if ((used >= DISK_LIMIT - 10)); then
    say "warning: the disk is $used% full; the disk guard stops the collector at $DISK_LIMIT%"
  fi
}

cmd_deploy() {
  local bundle="${1:?usage: release.sh deploy BUNDLE_DIR}" file release image name target current
  require_root
  [[ -f $ENV_FILE ]] || die "$ENV_FILE is missing; run vm-bootstrap.sh first"
  take_lock
  for file in compose.routewatch.yaml compose.app.yaml release.sh release.env; do
    [[ -f $bundle/$file ]] || die "$bundle/$file is missing"
  done
  release="$(value_of "$bundle/release.env" ROUTEWATCH_RELEASE)"
  image="$(value_of "$bundle/release.env" ROUTEWATCH_IMAGE)"
  [[ $release =~ ^[0-9a-f]{40}$ ]] || die "ROUTEWATCH_RELEASE must be a full commit SHA"
  [[ $image =~ ^[a-z0-9.-]+(/[a-z0-9._-]+)+@sha256:[0-9a-f]{64}$ ]] ||
    die "ROUTEWATCH_IMAGE must name an image by digest"

  # Named by commit and image, so a rebuilt image never overwrites a release.
  name="${release:0:12}-${image: -64:12}"
  target="$ROOT/releases/$name"
  install -d -m 0755 "$target"
  install -m 0644 "$bundle/compose.routewatch.yaml" "$bundle/compose.app.yaml" \
    "$bundle/release.env" "$target/"
  install -m 0755 "$bundle/release.sh" "$target/release.sh"
  current="$(current_release)"

  log "deploy $name ($image): started"
  if activate "$target"; then
    if [[ -n $current && $current != "$target" ]]; then
      point previous "$current"
    fi
    point current "$target"
    log "deploy $name: succeeded"
    return 0
  fi

  log "deploy $name: failed its checks"
  if [[ -z $current ]]; then
    point current "$target" # the first release: keep it, so the commands above work
    return 1
  fi
  if [[ $current != "$target" ]]; then
    log "rollback to $(basename "$current"): started"
    if activate "$current"; then
      log "rollback: succeeded"
    else
      log "rollback: failed its checks"
    fi
  fi
  return 1
}

cmd_rollback() {
  local current previous
  require_root
  take_lock
  [[ -L $ROOT/previous ]] || die "there is no previous release"
  current="$(current_release)"
  previous="$(readlink -f "$ROOT/previous")"
  log "rollback to $(basename "$previous"): started"
  if activate "$previous"; then
    point previous "$current"
    point current "$previous"
    log "rollback: succeeded"
    return 0
  fi
  point previous "$current"
  point current "$previous" # it is what runs now, checks or not
  log "rollback: failed its checks"
  return 1
}

cmd_status() {
  local current previous
  require_root
  current="$(current_release)"
  [[ -n $current ]] || die "nothing is deployed yet"
  say "current:  $(basename "$current")  $(value_of "$current/release.env" ROUTEWATCH_IMAGE)"
  if [[ -L $ROOT/previous ]]; then
    previous="$(readlink -f "$ROOT/previous")"
    say "previous: $(basename "$previous")  $(value_of "$previous/release.env" ROUTEWATCH_IMAGE)"
  fi
  say ""
  compose "$current" ps -a --format 'table {{.Service}}\t{{.State}}\t{{.Status}}'
  say ""
  say "disk: $(disk_used)% of $DISK_PATH used (the guard stops the collector at $DISK_LIMIT%)"
  say "database: $(psql_owner "$current" "SELECT pg_size_pretty(pg_database_size(current_database()))" || echo unknown)"
  say ""
  tail -n 5 "$ROOT/deploy.log" 2>/dev/null || true
}

cmd_compose() {
  local current
  require_root
  current="$(current_release)"
  [[ -n $current ]] || die "nothing is deployed yet"
  compose "$current" "$@"
}

cmd_disk_guard() {
  local current used
  require_root
  exec 9>"$ROOT/.lock"
  flock -n 9 || exit 0 # a deploy is running; check again next time
  current="$(current_release)"
  [[ -n $current ]] || exit 0
  used="$(disk_used)"
  ((used >= DISK_LIMIT)) || exit 0
  [[ -n "$(compose "$current" ps -q routewatch-collector)" ]] || exit 0
  compose "$current" stop routewatch-collector
  log "disk guard: $used% used, at or over $DISK_LIMIT%: collector stopped;" \
    "the consumer and cleaner carry on. Free space, then: sudo routewatch compose up -d"
}

case "${1:-}" in
deploy)
  shift
  cmd_deploy "$@"
  ;;
rollback) cmd_rollback ;;
status) cmd_status ;;
compose)
  shift
  cmd_compose "$@"
  ;;
disk-guard) cmd_disk_guard ;;
*)
  awk 'NR > 1 && /^#/ { sub(/^# ?/, ""); print; next } NR > 1 { exit }' "$0"
  exit 2
  ;;
esac
