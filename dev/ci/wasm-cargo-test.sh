#!/usr/bin/env bash

# Run `cargo test` for wasm32-unknown-unknown under wasm-bindgen-test-runner
# (Node), with the runner dev/ci/wasm-bindgen-test-runner.sh provides. Unlike
# `wasm-pack test`, which builds every test target of a crate, this builds
# only the targets named in "$@" (for example `-p jazz-e2ee --lib --test
# slh_dsa`), so a crate whose other tests link native C code can still run
# its wasm32 tests.
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/wasm-bindgen-test-runner.sh"

export CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner
exec cargo test --target wasm32-unknown-unknown "$@"
