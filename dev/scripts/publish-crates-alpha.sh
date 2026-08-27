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

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to validate the crate publish allowlist" >&2
  exit 1
fi

# Crates approved for publication, with exact workspace versions.
crates=(
  "jazz-wasm-tracing:3.0.0-alpha.0"
)

metadata="$(cargo metadata --no-deps --format-version 1)"

for crate_spec in "${crates[@]}"; do
  name="${crate_spec%%:*}"
  version="${crate_spec##*:}"

  # `cargo publish -p` accepts any package in the workspace.  Resolve the
  # exact metadata object before running it so the checked-in list cannot
  # accidentally publish a package which opted out of crates.io.  Cargo emits
  # `null` for unrestricted packages, `[]` for `publish = false`, and an array
  # of registry names for an explicit allowlist.
  if ! jq -e \
    --arg name "$name" \
    --arg version "$version" \
    '
      .workspace_members as $workspace_members |
      any(
        .packages[];
        .id as $id |
        .name == $name and
        .version == $version and
        ($workspace_members | index($id)) and
        (
          .publish == null or
          ((.publish | type) == "array" and (.publish | index("crates-io")))
        )
      )
    ' \
    <<<"$metadata" >/dev/null; then
    echo "Expected publishable workspace crate ${name}@${version} for crates.io" >&2
    exit 1
  fi
done

for crate_spec in "${crates[@]}"; do
  name="${crate_spec%%:*}"
  version="${crate_spec##*:}"

  if [[ "$MODE" == "dry-run" ]]; then
    echo "==> cargo publish --allow-dirty --dry-run -p ${name}"
    cargo publish --allow-dirty --dry-run -p "$name"
    continue
  fi

  echo "==> cargo publish --allow-dirty -p ${name}"
  publish_log="$(mktemp)"
  if ! cargo publish --allow-dirty -p "$name" 2>&1 | tee "$publish_log"; then
    if grep -Fq "already exists on crates.io index" "$publish_log"; then
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
