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
# opfs-btree -> jazz-tools
# jazz-wasm-tracing is independent and can be published in either position.
crates=(
  "opfs-btree:0.1.0"
  "jazz-wasm-tracing:3.0.0-alpha.0"
  "jazz-tools:2.0.0-alpha.0"
)

wait_for_index() {
  local name="$1"
  local expected_version="$2"

  for _ in {1..60}; do
    local published
    published="$(curl -s "https://crates.io/api/v1/crates/${name}" | jq -r '.crate.max_version // empty')"
    if [[ "$published" == "$expected_version" ]]; then
      echo "crates.io index now has ${name}@${published}"
      return 0
    fi
    sleep 5
  done

  echo "Timed out waiting for ${name}@${expected_version} to appear in crates.io index"
  return 1
}

for crate_spec in "${crates[@]}"; do
  name="${crate_spec%%:*}"
  version="${crate_spec##*:}"

  if [[ "$MODE" == "dry-run" ]]; then
    if [[ "$name" == "opfs-btree" ]]; then
      echo "==> cargo publish -p ${name} --dry-run"
      cargo publish -p "$name" --allow-dirty --dry-run
    else
      echo "==> cargo check -p ${name} (registry dry-run not possible until dependencies are published)"
      cargo check -p "$name"
    fi
    continue
  fi

  echo "==> cargo publish -p ${name} (publish)"
  cargo publish -p "$name" --allow-dirty

  wait_for_index "$name" "$version"
done

echo "Crate publish flow complete (${MODE})."
