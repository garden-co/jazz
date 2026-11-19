#!/usr/bin/env bash

# Install the small, explicitly allowlisted set of Cargo tools used by package
# builds. Keeping this in-repository avoids making every Linux build depend on
# downloading another action before its first build step can start.
set -euo pipefail

case "${JAZZ_RUST_TOOL:?JAZZ_RUST_TOOL must name a pinned tool}" in
  sccache@0.15.0)
    crate=sccache
    version=0.15.0
    binary=sccache
    ;;
  cargo-nextest@0.9.143)
    crate=cargo-nextest
    version=0.9.143
    binary=cargo-nextest
    ;;
  cargo-zigbuild@0.20.1)
    crate=cargo-zigbuild
    version=0.20.1
    binary=cargo-zigbuild
    ;;
  wasm-pack@0.13.1)
    crate=wasm-pack
    version=0.13.1
    binary=wasm-pack
    ;;
  *)
    echo "unsupported pinned Rust tool: ${JAZZ_RUST_TOOL}" >&2
    exit 64
    ;;
esac

install_root="${RUNNER_TEMP:?RUNNER_TEMP must be set}/jazz-rust-tool"

verify_installed_tool() {
  [[ -x "${install_root}/bin/${binary}" ]] &&
    "${install_root}/bin/${binary}" --version | grep -F "${binary} ${version}" >/dev/null
}

if [[ "${JAZZ_RUST_TOOL_CACHE_HIT:-}" == "true" ]]; then
  if verify_installed_tool; then
    echo "using validated cached ${JAZZ_RUST_TOOL}"
    echo "${install_root}/bin" >> "${GITHUB_PATH:?GITHUB_PATH must be set}"
    exit 0
  fi
  echo "::error::cached ${JAZZ_RUST_TOOL} failed version validation for ${JAZZ_RUST_TOOL_CACHE_KEY:-unknown cache key}; expected ${binary} ${version}. Bump installer-revision before retrying." >&2
  exit 65
fi

mkdir -p "${install_root}"

# sccache publishes static musl binaries: on Linux, take the pinned one
# (checksum below) instead of compiling it for 2-3 minutes on a cache miss.
prebuilt_sha256=""
if [[ "${crate}" == sccache && "$(uname -s)" == Linux ]]; then
  case "$(uname -m)" in
    x86_64) prebuilt_target=x86_64-unknown-linux-musl prebuilt_sha256=782d2b5dd7ae0a55ebe368ab258114d0928d019ac2d949ab85d5d02f3926709e ;;
    aarch64) prebuilt_target=aarch64-unknown-linux-musl prebuilt_sha256=3a6a3712b49da3d263bf2d30d702de4302793016019e800bfb81c0c69401d8f8 ;;
  esac
fi
if [[ -n "${prebuilt_sha256}" ]]; then
  archive="sccache-v${version}-${prebuilt_target}"
  # Job-local scratch; RUNNER_TEMP is emptied after the job.
  download="$(mktemp -d "${RUNNER_TEMP}/sccache-download.XXXXXX")"
  curl --proto '=https' --tlsv1.2 -sSfL --retry 3 -o "${download}/${archive}.tar.gz" \
    "https://github.com/mozilla/sccache/releases/download/v${version}/${archive}.tar.gz"
  echo "${prebuilt_sha256}  ${download}/${archive}.tar.gz" | sha256sum -c -
  tar -xzf "${download}/${archive}.tar.gz" -C "${download}"
  mkdir -p "${install_root}/bin"
  install -m 0755 "${download}/${archive}/sccache" "${install_root}/bin/sccache"
else
  cargo install "${crate}" --version "${version}" --locked --root "${install_root}"
fi
verify_installed_tool
echo "${install_root}/bin" >> "${GITHUB_PATH:?GITHUB_PATH must be set}"
