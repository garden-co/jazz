#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "with-macos-libclang-env.sh only supports macOS runners" >&2
  exit 1
fi

if [[ "$#" -eq 0 ]]; then
  echo "usage: with-macos-libclang-env.sh <command> [args...]" >&2
  exit 1
fi

libclang_dir="/Library/Developer/CommandLineTools/usr/lib"

if [[ ! -f "${libclang_dir}/libclang.dylib" ]]; then
  echo "libclang.dylib not found at ${libclang_dir}" >&2
  exit 1
fi

dyld_library_path="${libclang_dir}"
if [[ -n "${DYLD_LIBRARY_PATH:-}" ]]; then
  dyld_library_path="${libclang_dir}:${DYLD_LIBRARY_PATH}"
fi

exec env \
  LIBCLANG_PATH="${libclang_dir}" \
  DYLD_LIBRARY_PATH="${dyld_library_path}" \
  "$@"
