#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

TARGET_TRIPLE="${TARGET_TRIPLE:-${1:-}}"
ROCKSDB_ARCHIVE_PATH="${ROCKSDB_ARCHIVE_PATH:-${2:-}}"
ROCKSDB_BINDINGS_PATH="${ROCKSDB_BINDINGS_PATH:-${3:-}}"

if [[ -z "${TARGET_TRIPLE}" || -z "${ROCKSDB_ARCHIVE_PATH}" ]]; then
  echo "usage: $(basename "$0") <target-triple> <path-to-librocksdb.a> [path-to-bindings.rs]" >&2
  exit 1
fi

if [[ ! -f "${ROCKSDB_ARCHIVE_PATH}" ]]; then
  echo "missing archive: ${ROCKSDB_ARCHIVE_PATH}" >&2
  exit 1
fi

DEST_DIR="${REPO_ROOT}/vendor/librocksdb-sys/prebuilt/${TARGET_TRIPLE}/lib"
mkdir -p "${DEST_DIR}"
cp "${ROCKSDB_ARCHIVE_PATH}" "${DEST_DIR}/librocksdb.a"

if [[ -n "${ROCKSDB_BINDINGS_PATH}" ]]; then
  if [[ ! -f "${ROCKSDB_BINDINGS_PATH}" ]]; then
    echo "missing bindings: ${ROCKSDB_BINDINGS_PATH}" >&2
    exit 1
  fi
  cp "${ROCKSDB_BINDINGS_PATH}" "${REPO_ROOT}/vendor/librocksdb-sys/bindings/bindings.rs"
fi

echo "staged vendored RocksDB archive for ${TARGET_TRIPLE}: ${DEST_DIR}/librocksdb.a"
