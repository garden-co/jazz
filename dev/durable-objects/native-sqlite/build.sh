#!/bin/sh
# Build the Durable Object package into harness/pkg. FAST=1 skips LTO and
# wasm-opt (for iterating); SPEED=1 optimizes for speed (opt-level 3,
# wasm-opt -O3) instead of size; the default matches jazz-wasm's release
# build.
set -e
cd "$(dirname "$0")"
if [ -n "$FAST" ]; then
  wasm-pack build jazz-do --target web --release --no-opt --out-dir ../harness/pkg -- "$@" \
    --config 'profile.release.lto="off"' --config 'profile.release.codegen-units=16'
elif [ -n "$SPEED" ]; then
  wasm-pack build jazz-do --target web --release --no-opt --out-dir ../harness/pkg -- "$@" \
    --config 'profile.release.opt-level=3' --config 'profile.release.package.jazz-do.opt-level=3'
  wasm-opt -O3 -g --low-memory-unused --enable-bulk-memory --enable-nontrapping-float-to-int \
    --enable-sign-ext --enable-mutable-globals --enable-reference-types --enable-multivalue \
    harness/pkg/jazz_do_bg.wasm -o harness/pkg/jazz_do_bg.wasm
else
  wasm-pack build jazz-do --target web --release --out-dir ../harness/pkg -- "$@"
fi
rm -f harness/pkg/.gitignore harness/pkg/package.json
