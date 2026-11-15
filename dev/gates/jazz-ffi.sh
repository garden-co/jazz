#!/usr/bin/env bash
# jazz-ffi's C ABI from C, and the Swift and Kotlin sources UniFFI generates
# from it (written to target/jazz-ffi-bindings for the language packages).
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
output_dir=$(mktemp -d)
trap 'rm -rf "$output_dir"' EXIT

cargo build --manifest-path "$root/Cargo.toml" -p jazz-ffi --lib
cargo build --manifest-path "$root/Cargo.toml" -p jazz-ffi --features bindgen --bin uniffi-bindgen

cc -std=c11 -D_POSIX_C_SOURCE=200809L -Wall -Werror \
  -I"$root/crates/jazz-ffi/include" \
  "$root/crates/jazz-ffi/tests/c/jazz_ffi_test.c" \
  "$root/target/debug/libjazz_ffi.a" \
  -ldl -lm -lpthread \
  -o "$output_dir/jazz-ffi-c-test"
"$output_dir/jazz-ffi-c-test"

bindings="$root/target/jazz-ffi-bindings"
rm -rf "$bindings"
for language in swift kotlin; do
  "$root/target/debug/uniffi-bindgen" generate \
    --library "$root/target/debug/libjazz_ffi.so" \
    --language "$language" \
    --no-format \
    --out-dir "$bindings/$language"
done
test -s "$bindings/swift/jazz_ffi.swift"
test -s "$bindings/kotlin/uniffi/jazz_ffi/jazz_ffi.kt"
echo "jazz-ffi bindings: $bindings"
