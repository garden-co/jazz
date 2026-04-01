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

{
  echo "LIBCLANG_PATH=${libclang_dir}"
  if [[ -n "${LD_LIBRARY_PATH:-}" ]]; then
    echo "LD_LIBRARY_PATH=${ld_path}:${LD_LIBRARY_PATH}"
  else
    echo "LD_LIBRARY_PATH=${ld_path}"
  fi
} >> "${GITHUB_ENV}"
