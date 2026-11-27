#!/usr/bin/env bash

# Run `wasm-pack test` with a wasm-bindgen-test-runner beside the wasm-bindgen
# it picks (dev/ci/wasm-bindgen-test-runner.sh).
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/wasm-bindgen-test-runner.sh"

exec wasm-pack test "$@"
