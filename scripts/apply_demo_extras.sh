#!/usr/bin/env bash
# Copies the pipeline's changes into the demo checkout. Safe to rerun: each copy replaces the last.
set -euo pipefail

cd "$(dirname "$0")/.."

demo_dir="${DEMO_DIR:-../opentelemetry-demo}"
expected_tag="3.1.0"

found_tag="$(git -C "$demo_dir" describe --tags --exact-match 2>/dev/null || true)"
if [[ "$found_tag" != "$expected_tag" ]]; then
  echo "error: $demo_dir is not a demo checkout at tag $expected_tag (found: ${found_tag:-none})" >&2
  exit 1
fi

copy() {
  cp "$1" "$demo_dir/$2"
  echo "copied $1 -> $demo_dir/$2"
}

copy demo/compose.extras.yaml compose.extras.yaml
copy demo/otelcol-config-extras.yml src/otel-collector/otelcol-config-extras.yml
copy demo/env.override .env.override