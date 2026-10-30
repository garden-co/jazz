#!/usr/bin/env bash
# Build the browser WASM exactly as releases do (fat LTO, one codegen unit,
# jazz-wasm at opt-level "s", wasm-opt -O -g --low-memory-unused, then dev/artifacts/wasm-names.mjs drops
# symbol hashes from function names) and print its raw, gzip and Brotli sizes.
#
# Usage: dev/benchmarks/wasm-size/build.sh OUT_DIR [--type-names]
#
# --type-names builds with v0 symbol mangling so attribute.py can see generic
# arguments. Its name section is larger, so compare sizes only between builds
# made the same way.
set -euo pipefail

out=$1
mode=${2:-}
root=$(git rev-parse --show-toplevel)
mkdir -p "$out"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$root/target/wasm-size${mode:+-v0}}"
if [[ $mode == --type-names ]]; then
  export RUSTFLAGS="${RUSTFLAGS:-} -Csymbol-mangling-version=v0"
fi

cargo build -p jazz-wasm --lib --target wasm32-unknown-unknown --release \
  --config 'profile.release.lto="fat"' --config profile.release.codegen-units=1 \
  --config 'profile.release.package.jazz-wasm.opt-level="s"'
wasm-bindgen --target web --out-dir "$out" \
  "$CARGO_TARGET_DIR/wasm32-unknown-unknown/release/jazz_wasm.wasm"
wasm-opt "$out/jazz_wasm_bg.wasm" -O -g --low-memory-unused -o "$out/jazz_wasm_bg.wasm"
node "$root/dev/artifacts/wasm-names.mjs" "$out/jazz_wasm_bg.wasm"

wasm=$out/jazz_wasm_bg.wasm
echo "raw=$(wc -c <"$wasm") gzip9=$(gzip -9c "$wasm" | wc -c) brotli11=$(python3 -c '
import brotli, sys
print(len(brotli.compress(open(sys.argv[1], "rb").read(), quality=11)))' "$wasm" 2>/dev/null || echo n/a)"
