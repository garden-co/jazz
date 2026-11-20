#!/bin/sh
# Build the Durable Object fixture (jazz-wasm with `durable-object-tests`)
# and run the workerd tests. Needs the wasm32-unknown-unknown target,
# wasm-pack and the workspace's node_modules (miniflare).
set -eu
here=$(cd "$(dirname "$0")" && pwd)
wasm-pack build "$here/../.." --target web --dev --out-dir "$here/pkg" \
  --out-name jazz_wasm --no-pack -- --features durable-object-tests
# The packaged Durable Object entry (`jazz-wasm/durable-object`) as
# `build:durable-object` builds it, but unoptimised and with its test-only
# routes (a panic, held limits): into `do/`, a copy of the entry beside its
# own build, never into the published `durable-object/pkg`.
rm -rf "$here/do"
mkdir -p "$here/do"
cp "$here/../../durable-object/index.js" "$here/../../durable-object/host.js" "$here/do/"
wasm-pack build "$here/../.." --target web --dev --out-dir "$here/do/pkg" \
  --out-name jazz_wasm_do --no-pack -- --features durable-object-tests
node "$here/../../durable-object/reinstantiable.mjs" "$here/do/pkg/jazz_wasm_do.js"
cd "$here/../.."
node --test "$here/smoke.test.mjs" "$here/host.test.mjs" "$here/r2.test.mjs"
