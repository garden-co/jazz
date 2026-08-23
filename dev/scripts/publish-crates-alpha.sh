#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-dry-run}"

if [[ "$MODE" != "dry-run" && "$MODE" != "publish" ]]; then
  echo "Usage: $0 [dry-run|publish]"
  exit 1
fi

if [[ "$MODE" == "publish" && -z "${CARGO_REGISTRY_TOKEN:-}" ]]; then
  echo "CARGO_REGISTRY_TOKEN must be set for publish mode"
  exit 1
fi

# Workspace publish order based on dependency graph.
crates=(
  "jazz-wasm-tracing:3.0.0-alpha.0"
  "jazz-tools:2.0.0-alpha.0"
)

for crate_spec in "${crates[@]}"; do
  name="${crate_spec%%:*}"
  version="${crate_spec##*:}"

  if [[ "$MODE" == "dry-run" ]]; then
    echo "==> cargo check -p ${name} (registry dry-run not possible until dependencies are published)"
    cargo check -p "$name"
    continue
  fi

  echo "==> cargo publish -p ${name} (publish)"

  publish_log="$(mktemp)"
  if ! cargo publish -p "$name" --allow-dirty 2>&1 | tee "$publish_log"; then
    if grep -q "already exists on crates.io index" "$publish_log"; then
      echo "==> ${name}@${version} already published, skipping"
      rm -f "$publish_log"
      continue
    fi
    rm -f "$publish_log"
    echo "==> failed to publish ${name}@${version}"
    exit 1
  fi
  rm -f "$publish_log"
done

echo "Crate publish flow complete (${MODE})."
