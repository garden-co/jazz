#!/usr/bin/env bash
# Builds libjazz_ffi and generates the UniFFI Swift sources the Jazz Swift
# package compiles against:
#   Sources/JazzFFI/jazz_ffi.swift
#   Sources/jazz_ffiFFI/include/jazz_ffiFFI.h
# Run it from anywhere; pass --debug for a debug build. The library lands in
# <repo>/target/release (or target/debug, then set JAZZ_FFI_LIB_DIR to it).
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repo="$(cd "$here/../.." && pwd)"
profile=release
flag=--release
if [[ "${1:-}" == "--debug" ]]; then profile=debug; flag=; fi

cd "$repo"
cargo build -p jazz-ffi $flag --lib
lib="$repo/target/$profile/libjazz_ffi.so"
[[ -f "$lib" ]] || lib="$repo/target/$profile/libjazz_ffi.dylib"
out="$(mktemp -d)"
cargo run -q -p jazz-ffi --features bindgen --bin uniffi-bindgen -- \
  generate --library "$lib" --language swift --out-dir "$out"

mkdir -p "$here/Sources/JazzFFI" "$here/Sources/jazz_ffiFFI/include"
cp "$out/jazz_ffi.swift" "$here/Sources/JazzFFI/jazz_ffi.swift"
cp "$out/jazz_ffiFFI.h" "$here/Sources/jazz_ffiFFI/include/jazz_ffiFFI.h"
rm -rf "$out"
# Link the static library: with a .so beside it, the linker would pick the
# shared one and the binaries would need it at run time.
mkdir -p "$repo/target/$profile/jazz-ffi-static"
cp "$repo/target/$profile/libjazz_ffi.a" "$repo/target/$profile/jazz-ffi-static/"
echo "Generated the UniFFI Swift sources; libjazz_ffi.a is in target/$profile/jazz-ffi-static"
