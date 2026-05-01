#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "prepare_libclang_env.sh only supports Linux runners" >&2
  exit 1
fi

if [[ -z "${GITHUB_ENV:-}" ]]; then
  echo "GITHUB_ENV must be set" >&2
  exit 1
fi

bundle_root="${RUNNER_TEMP:-${TMPDIR:-/tmp}}/jazz-libclang"
packages_dir="${bundle_root}/packages"
extract_dir="${bundle_root}/root"

rm -rf "${bundle_root}"
mkdir -p "${packages_dir}" "${extract_dir}"

cd "${packages_dir}"

apt download \
  libclang-18-dev \
  libclang-common-18-dev \
  libclang-cpp18 \
  libclang1-18 \
  libllvm18

for deb in ./*.deb; do
  dpkg-deb -x "${deb}" "${extract_dir}"
done

libclang_file="$(
  find "${extract_dir}" -type f \
    \( -name 'libclang.so' -o -name 'libclang.so.*' -o -name 'libclang-*.so' -o -name 'libclang-*.so.*' \) \
    | sort \
    | head -n 1
)"

if [[ -z "${libclang_file}" ]]; then
  echo "failed to locate libclang in extracted packages" >&2
  exit 1
fi

libclang_dir="$(dirname "${libclang_file}")"

ld_path=""
for dir in \
  "${libclang_dir}" \
  "${extract_dir}/usr/lib/x86_64-linux-gnu" \
  "${extract_dir}/usr/lib/llvm-18/lib"
do
  if [[ -d "${dir}" ]]; then
    if [[ -z "${ld_path}" ]]; then
      ld_path="${dir}"
    else
      ld_path="${ld_path}:${dir}"
    fi
  fi
done

if [[ -z "${ld_path}" ]]; then
  echo "failed to construct LD_LIBRARY_PATH for local libclang bundle" >&2
  exit 1
fi

if ! command -v gcc >/dev/null 2>&1; then
  echo "gcc is required so bindgen can locate the system C headers" >&2
  exit 1
fi

gcc_include_dir="$(gcc -print-file-name=include)"
gcc_target="$(gcc -dumpmachine)"
arch_include_dir="/usr/include/${gcc_target}"

bindgen_args="--sysroot=/ -isystem ${gcc_include_dir}"
include_path="${gcc_include_dir}"

if [[ -d "${arch_include_dir}" ]]; then
  bindgen_args="${bindgen_args} -isystem ${arch_include_dir}"
  include_path="${include_path}:${arch_include_dir}"
fi

bindgen_args="${bindgen_args} -isystem /usr/include"
include_path="${include_path}:/usr/include"

{
  echo "LIBCLANG_PATH=${libclang_dir}"
  if [[ -n "${LD_LIBRARY_PATH:-}" ]]; then
    echo "LD_LIBRARY_PATH=${ld_path}:${LD_LIBRARY_PATH}"
  else
    echo "LD_LIBRARY_PATH=${ld_path}"
  fi
  echo "BINDGEN_EXTRA_CLANG_ARGS=${bindgen_args}"
  if [[ -n "${C_INCLUDE_PATH:-}" ]]; then
    echo "C_INCLUDE_PATH=${include_path}:${C_INCLUDE_PATH}"
  else
    echo "C_INCLUDE_PATH=${include_path}"
  fi
} >> "${GITHUB_ENV}"
