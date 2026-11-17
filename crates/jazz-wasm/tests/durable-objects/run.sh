#!/bin/sh
# Build the Durable Object fixture (jazz-wasm with `durable-object-tests`)
# and run the workerd smoke test. Needs the wasm32-unknown-unknown target,
# wasm-pack and the workspace's node_modules (miniflare).
set -eu
here=$(cd "$(dirname "$0")" && pwd)
wasm-pack build "$here/../.." --target web --dev --out-dir "$here/pkg" \
  --out-name jazz_wasm --no-pack -- --features durable-object-tests
# The packaged Durable Object entry (`jazz-wasm/durable-object`), as
# `build:durable-object` builds it, but unoptimised.
wasm-pack build "$here/../.." --target web --dev --out-dir durable-object/pkg \
  --out-name jazz_wasm_do --no-pack -- --features durable-object
cd "$here/../.."
node --test "$here/smoke.test.mjs" "$here/host.test.mjs"
