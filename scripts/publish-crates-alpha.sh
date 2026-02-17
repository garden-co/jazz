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

JAZZ_TOOLS_CRATE_DIR="crates/jazz-cli"
JAZZ_TOOLS_LIB_RS="${JAZZ_TOOLS_CRATE_DIR}/src/lib.rs"
JAZZ_TOOLS_LIB_RS_BACKUP="${JAZZ_TOOLS_CRATE_DIR}/src/lib.rs.publish-backup"
JAZZ_TOOLS_VENDOR_GROOVE_DIR="${JAZZ_TOOLS_CRATE_DIR}/src/_published_groove"
JAZZ_TOOLS_VENDOR_RUNTIME_DIR="${JAZZ_TOOLS_CRATE_DIR}/src/_published_runtime_tokio"

prepare_jazz_tools_publish_sources() {
  cp "$JAZZ_TOOLS_LIB_RS" "$JAZZ_TOOLS_LIB_RS_BACKUP"
  rm -rf "$JAZZ_TOOLS_VENDOR_GROOVE_DIR" "$JAZZ_TOOLS_VENDOR_RUNTIME_DIR"

  cp -R "${JAZZ_TOOLS_CRATE_DIR}/../groove/src" "$JAZZ_TOOLS_VENDOR_GROOVE_DIR"
  mkdir -p "$JAZZ_TOOLS_VENDOR_RUNTIME_DIR"
  cp "${JAZZ_TOOLS_CRATE_DIR}/../groove-tokio/src/lib.rs" "${JAZZ_TOOLS_VENDOR_RUNTIME_DIR}/lib.rs"

  sed \
    -e 's#../../groove/src/#_published_groove/#g' \
    -e 's#../../groove-tokio/src/lib.rs#_published_runtime_tokio/lib.rs#g' \
    "$JAZZ_TOOLS_LIB_RS_BACKUP" > "$JAZZ_TOOLS_LIB_RS"
}

cleanup_jazz_tools_publish_sources() {
  if [[ -f "$JAZZ_TOOLS_LIB_RS_BACKUP" ]]; then
    mv "$JAZZ_TOOLS_LIB_RS_BACKUP" "$JAZZ_TOOLS_LIB_RS"
  fi
  rm -rf "$JAZZ_TOOLS_VENDOR_GROOVE_DIR" "$JAZZ_TOOLS_VENDOR_RUNTIME_DIR"
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
  if [[ "$name" == "jazz-tools" ]]; then
    prepare_jazz_tools_publish_sources
  fi

  publish_log="$(mktemp)"
  if ! cargo publish -p "$name" --allow-dirty 2>&1 | tee "$publish_log"; then
    if grep -q "already exists on crates.io index" "$publish_log"; then
      echo "==> ${name}@${version} already published, skipping"
      rm -f "$publish_log"
      if [[ "$name" == "jazz-tools" ]]; then
        cleanup_jazz_tools_publish_sources
      fi
      continue
    fi
    rm -f "$publish_log"
    if [[ "$name" == "jazz-tools" ]]; then
      cleanup_jazz_tools_publish_sources
    fi
    echo "==> failed to publish ${name}@${version}"
    exit 1
  fi
  rm -f "$publish_log"
  if [[ "$name" == "jazz-tools" ]]; then
    cleanup_jazz_tools_publish_sources
  fi
done

echo "Crate publish flow complete (${MODE})."
