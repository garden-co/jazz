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
    # The same pinned release and SHA-256 the CI toolchain image installs
    # (dev/ci/toolchain/Dockerfile): building it from source took ~2 minutes
    # whenever the tool cache missed, which is every merge-group branch.
    release_url=https://github.com/mozilla/sccache/releases/download/v0.15.0/sccache-v0.15.0-x86_64-unknown-linux-musl.tar.gz
    release_sha256=782d2b5dd7ae0a55ebe368ab258114d0928d019ac2d949ab85d5d02f3926709e
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
if [[ -n "${release_url:-}" && "$(uname -s)-$(uname -m)" == "Linux-x86_64" ]]; then
  release_dir="$(mktemp -d)"
  curl -sSfL --retry 3 -o "${release_dir}/release.tar.gz" "${release_url}"
  echo "${release_sha256}  ${release_dir}/release.tar.gz" | sha256sum --check --strict
  tar -xzf "${release_dir}/release.tar.gz" -C "${release_dir}"
  found="$(find "${release_dir}" -type f -name "${binary}" -perm -u+x | head -n 1)"
  test -n "${found}"
  install -D -m 0755 "${found}" "${install_root}/bin/${binary}"
  verify_installed_tool
  echo "${install_root}/bin" >> "${GITHUB_PATH:?GITHUB_PATH must be set}"
  exit 0
fi
cargo install "${crate}" --version "${version}" --locked --root "${install_root}"
verify_installed_tool
echo "${install_root}/bin" >> "${GITHUB_PATH:?GITHUB_PATH must be set}"
