#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

run_privileged() {
  if command -v sudo >/dev/null 2>&1; then
    sudo "$@"
  else
    "$@"
  fi
}

install_libclang() {
  if command -v yum >/dev/null 2>&1; then
    run_privileged yum install -y clang-devel
    return
  fi

  if command -v dnf >/dev/null 2>&1; then
    run_privileged dnf install -y clang-devel
    return
  fi

  if command -v microdnf >/dev/null 2>&1; then
    run_privileged microdnf install -y clang-devel
    return
  fi

  if command -v apt-get >/dev/null 2>&1; then
    run_privileged apt-get update
    run_privileged apt-get install -y libclang-dev
    return
  fi

  echo "Unable to install libclang automatically. Install clang-devel or libclang-dev before building on Vercel." >&2
  exit 1
}

if [[ "$(uname -s)" == "Linux" ]]; then
  install_libclang
fi

JAZZ_SKIP_RN_DEPS=1 bash "${SCRIPT_DIR}/install-jazz-rn-deps.sh"
