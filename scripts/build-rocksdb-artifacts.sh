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

if [[ ! -x "${LLVM_AR}" || ! -x "${LLVM_RANLIB}" ]]; then
  echo "set LLVM_AR and LLVM_RANLIB to working llvm-ar/llvm-ranlib paths" >&2
  exit 1
fi

(
  cd "${REPO_ROOT}"
  cargo fetch --manifest-path Cargo.toml
)

WRAPPER_DIR="$(mktemp -d /tmp/jazz-rocksdb-zig.XXXXXX)"
cleanup() {
  rm -rf "${WRAPPER_DIR}"
}
trap cleanup EXIT

write_zig_wrapper() {
  local path="$1"
  local tool="$2"
  local zig_target="$3"
  local cargo_target="$4"

  cat > "${path}" <<EOF
#!/usr/bin/env bash
set -euo pipefail
args=()
while [[ \$# -gt 0 ]]; do
  case "\$1" in
    --target=${cargo_target})
      shift
      ;;
    --target)
      if [[ \${2:-} == ${cargo_target} ]]; then
        shift 2
      else
        args+=("\$1")
        shift
        if [[ \$# -gt 0 ]]; then
          args+=("\$1")
          shift
        fi
      fi
      ;;
    *)
      args+=("\$1")
      shift
      ;;
  esac
done
exec zig ${tool} -target ${zig_target} "\${args[@]}"
EOF
  chmod +x "${path}"
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
      build_and_store "${target}" \
        AR="${LLVM_AR}" \
        RANLIB="${LLVM_RANLIB}"
      ;;
    aarch64-unknown-linux-gnu)
      write_zig_wrapper "${WRAPPER_DIR}/cc-${target}" cc aarch64-linux-gnu "${target}"
      write_zig_wrapper "${WRAPPER_DIR}/cxx-${target}" c++ aarch64-linux-gnu "${target}"
      build_and_store "${target}" \
        CC_aarch64_unknown_linux_gnu="${WRAPPER_DIR}/cc-${target}" \
        CXX_aarch64_unknown_linux_gnu="${WRAPPER_DIR}/cxx-${target}" \
        AR_aarch64_unknown_linux_gnu="${LLVM_AR}" \
        RANLIB_aarch64_unknown_linux_gnu="${LLVM_RANLIB}"
      ;;
    x86_64-unknown-linux-gnu)
      write_zig_wrapper "${WRAPPER_DIR}/cc-${target}" cc x86_64-linux-gnu "${target}"
      write_zig_wrapper "${WRAPPER_DIR}/cxx-${target}" c++ x86_64-linux-gnu "${target}"
      build_and_store "${target}" \
        CC_x86_64_unknown_linux_gnu="${WRAPPER_DIR}/cc-${target}" \
        CXX_x86_64_unknown_linux_gnu="${WRAPPER_DIR}/cxx-${target}" \
        AR_x86_64_unknown_linux_gnu="${LLVM_AR}" \
        RANLIB_x86_64_unknown_linux_gnu="${LLVM_RANLIB}"
      ;;
    *)
      echo "unsupported target: ${target}" >&2
      exit 1
      ;;
  esac
done

echo "built RocksDB archives under ${OUTPUT_DIR} for: ${TARGETS[*]}"
