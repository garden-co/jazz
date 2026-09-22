#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

# rust-librocksdb-sys and zstd-sys use bindgen, which dlopens libclang.so at
# build time. Vercel's Amazon Linux image ships without it.
if ! ldconfig -p 2>/dev/null | grep -q "libclang"; then
  if command -v dnf >/dev/null 2>&1; then
    dnf install -y clang
  elif command -v yum >/dev/null 2>&1; then
    yum install -y clang
  elif command -v apt-get >/dev/null 2>&1; then
    apt-get update && apt-get install -y libclang-dev
  fi
fi

JAZZ_SKIP_RN_DEPS=1 bash "${SCRIPT_DIR}/install-jazz-rn-deps.sh"
