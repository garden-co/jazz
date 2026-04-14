#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

OUTPUT_DIR="${OUTPUT_DIR:-${REPO_ROOT}/dist/rocksdb}"
LLVM_AR="${LLVM_AR:-/opt/homebrew/opt/llvm/bin/llvm-ar}"
LLVM_RANLIB="${LLVM_RANLIB:-/opt/homebrew/opt/llvm/bin/llvm-ranlib}"

ALL_TARGETS=(
  aarch64-apple-darwin
  x86_64-apple-darwin
  aarch64-unknown-linux-gnu
  x86_64-unknown-linux-gnu
)

if [[ "$#" -eq 0 ]]; then
  TARGETS=("${ALL_TARGETS[@]}")
else
  TARGETS=("$@")
fi

if ! command -v cargo >/dev/null 2>&1 || ! command -v rustup >/dev/null 2>&1; then
  echo "cargo and rustup are required" >&2
  exit 1
fi

if ! command -v gzip >/dev/null 2>&1; then
  echo "gzip is required" >&2
  exit 1
fi

(
  cd "${REPO_ROOT}"
  cargo fetch --manifest-path Cargo.toml
)

require_executable() {
  local path="$1"
  local label="$2"

  if [[ ! -x "${path}" ]]; then
    echo "set ${label} to a working executable path" >&2
    exit 1
  fi
}

require_command() {
  local command_name="$1"

  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "required command not found: ${command_name}" >&2
    exit 1
  fi
}

stage_output() {
  local target="$1"
  local archive_path="$2"
  local target_output_dir="${OUTPUT_DIR}/${target}"

  mkdir -p "${target_output_dir}"
  gzip -n -c "${archive_path}" > "${target_output_dir}/librocksdb.a.gz"
}

build_and_store() {
  local target="$1"
  local target_dir="/tmp/jazz-rocksdb-${target}"
  shift

  rustup target add "${target}" >/dev/null
  rm -rf "${target_dir}"

  (
    cd "${REPO_ROOT}"
    env JAZZ_ROCKSDB_OFFLINE=1 CARGO_TARGET_DIR="${target_dir}" "$@" \
      cargo build --manifest-path vendor/librocksdb-sys/Cargo.toml \
      --release \
      --target "${target}" \
      --features lz4,zstd
  )

  local archive_path
  archive_path="$(find "${target_dir}" -path "*/release/build/librocksdb-sys-*/out/librocksdb.a" -print | sed -n '1p')"
  if [[ -z "${archive_path}" ]]; then
    echo "failed to locate librocksdb.a for ${target}" >&2
    exit 1
  fi

  stage_output "${target}" "${archive_path}"
  rm -rf "${target_dir}"
}

rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}"

for target in "${TARGETS[@]}"; do
  case "${target}" in
    aarch64-apple-darwin|x86_64-apple-darwin)
      require_executable "${LLVM_AR}" "LLVM_AR"
      require_executable "${LLVM_RANLIB}" "LLVM_RANLIB"
      build_and_store "${target}" \
        AR="${LLVM_AR}" \
        RANLIB="${LLVM_RANLIB}"
      ;;
    aarch64-unknown-linux-gnu)
      # Linux consumers link these archives with libstdc++, so publish them with GNU toolchains.
      linux_cc="${CC_aarch64_unknown_linux_gnu:-aarch64-linux-gnu-gcc}"
      linux_cxx="${CXX_aarch64_unknown_linux_gnu:-aarch64-linux-gnu-g++}"
      linux_ar="${AR_aarch64_unknown_linux_gnu:-aarch64-linux-gnu-ar}"
      linux_ranlib="${RANLIB_aarch64_unknown_linux_gnu:-aarch64-linux-gnu-ranlib}"
      require_command "${linux_cc}"
      require_command "${linux_cxx}"
      require_command "${linux_ar}"
      require_command "${linux_ranlib}"
      build_and_store "${target}" \
        CC_aarch64_unknown_linux_gnu="${linux_cc}" \
        CXX_aarch64_unknown_linux_gnu="${linux_cxx}" \
        AR_aarch64_unknown_linux_gnu="${linux_ar}" \
        RANLIB_aarch64_unknown_linux_gnu="${linux_ranlib}"
      ;;
    x86_64-unknown-linux-gnu)
      # Linux consumers link these archives with libstdc++, so publish them with GNU toolchains.
      linux_cc="${CC_x86_64_unknown_linux_gnu:-gcc}"
      linux_cxx="${CXX_x86_64_unknown_linux_gnu:-g++}"
      linux_ar="${AR_x86_64_unknown_linux_gnu:-ar}"
      linux_ranlib="${RANLIB_x86_64_unknown_linux_gnu:-ranlib}"
      require_command "${linux_cc}"
      require_command "${linux_cxx}"
      require_command "${linux_ar}"
      require_command "${linux_ranlib}"
      build_and_store "${target}" \
        CC_x86_64_unknown_linux_gnu="${linux_cc}" \
        CXX_x86_64_unknown_linux_gnu="${linux_cxx}" \
        AR_x86_64_unknown_linux_gnu="${linux_ar}" \
        RANLIB_x86_64_unknown_linux_gnu="${linux_ranlib}"
      ;;
    *)
      echo "unsupported target: ${target}" >&2
      exit 1
      ;;
  esac
done

echo "built RocksDB archives under ${OUTPUT_DIR} for: ${TARGETS[*]}"
