#!/usr/bin/env bash
# Sourced by dev/ci/wasm-pack-test.sh and dev/ci/wasm-cargo-test.sh: put a
# wasm-bindgen-test-runner beside the wasm-bindgen on PATH. wasm-pack uses the
# wasm-bindgen on PATH when its version matches Cargo.lock and expects the
# test runner in the same directory. The CI tool image ships only
# wasm-bindgen, so without this the run stops at "binary does not exist".
# When the runner is missing, fetch the release the image pins (same version
# and checksum as dev/ci/toolchain/Dockerfile) and put both of its binaries
# first on PATH.

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
dockerfile="${root}/dev/ci/toolchain/Dockerfile"

bindgen="$(command -v wasm-bindgen || true)"
if [[ -n "${bindgen}" && ! -x "$(dirname "${bindgen}")/wasm-bindgen-test-runner" ]]; then
  version="$(sed -n 's/^ARG WASM_BINDGEN_VERSION=//p' "${dockerfile}")"
  sha256="$(sed -n 's/^ARG WASM_BINDGEN_SHA256=//p' "${dockerfile}")"
  test -n "${version}" && test -n "${sha256}"
  name="wasm-bindgen-${version}-x86_64-unknown-linux-musl"
  dir="${RUNNER_TEMP:-${TMPDIR:-/tmp}}/jazz-${name}"
  if [[ ! -x "${dir}/wasm-bindgen-test-runner" ]]; then
    work="$(mktemp -d)"
    curl --fail --location --silent --show-error --retry 3 --output "${work}/release.tar.gz" \
      "https://github.com/wasm-bindgen/wasm-bindgen/releases/download/${version}/${name}.tar.gz"
    echo "${sha256}  ${work}/release.tar.gz" | sha256sum --check --strict >/dev/null
    tar -xzf "${work}/release.tar.gz" -C "${work}"
    mkdir -p "${dir}"
    install -m 0755 "${work}/${name}/wasm-bindgen" "${work}/${name}/wasm-bindgen-test-runner" "${dir}/"
    rm -rf "${work}"
  fi
  export PATH="${dir}:${PATH}"
fi

