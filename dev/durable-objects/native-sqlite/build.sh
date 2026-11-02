#!/bin/sh
# Build the Durable Object package into harness/pkg. FAST=1 skips LTO and
# wasm-opt (for iterating); the default matches jazz-wasm's release build.
set -e
cd "$(dirname "$0")"
if [ -n "$FAST" ]; then
  wasm-pack build jazz-do --target web --release --no-opt --out-dir ../harness/pkg -- \
    --config 'profile.release.lto="off"' --config 'profile.release.codegen-units=16'
else
  wasm-pack build jazz-do --target web --release --out-dir ../harness/pkg
fi
rm -f harness/pkg/.gitignore harness/pkg/package.json
