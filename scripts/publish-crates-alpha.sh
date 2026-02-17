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

crate_version_exists() {
  local name="$1"
  local expected_version="$2"
  local response

  response="$(curl -fsS "https://crates.io/api/v1/crates/${name}/${expected_version}" 2>/dev/null || true)"
  if [[ -z "$response" ]]; then
    return 1
  fi

  jq -e --arg version "$expected_version" '.version.num == $version' >/dev/null <<<"$response"
}

wait_for_index() {
  local name="$1"
  local expected_version="$2"

  for _ in {1..60}; do
    if crate_version_exists "$name" "$expected_version"; then
      echo "crates.io index now has ${name}@${expected_version}"
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

  if crate_version_exists "$name" "$version"; then
    echo "==> ${name}@${version} already published, skipping"
    continue
  fi

  echo "==> cargo publish -p ${name} (publish)"
  if ! cargo publish -p "$name" --allow-dirty; then
    if crate_version_exists "$name" "$version"; then
      echo "==> ${name}@${version} is now published (likely by another run), continuing"
      continue
    fi
    echo "==> failed to publish ${name}@${version}"
    exit 1
  fi

  wait_for_index "$name" "$version"
done

echo "Crate publish flow complete (${MODE})."
