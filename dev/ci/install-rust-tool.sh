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
cargo install "${crate}" --version "${version}" --locked --root "${install_root}"
verify_installed_tool
echo "${install_root}/bin" >> "${GITHUB_PATH:?GITHUB_PATH must be set}"
