#!/usr/bin/env bash
# Builds libjazz_ffi for the Jazz Swift package and keeps its UniFFI sources
# current. The package (the repository's root Package.swift) finds the
# library through pkg-config: this writes jazz_ffi.pc beside libjazz_ffi.a.
#
#   build-ffi.sh             build libjazz_ffi.a and jazz_ffi.pc, regenerate the
#                            committed UniFFI sources
#   build-ffi.sh --lib-only  only the library and jazz_ffi.pc (what an app needs:
#                            the sources are committed)
#   build-ffi.sh --check     fail if the committed sources differ from what
#                            uniffi-bindgen generates (CI)
#   --debug                  a debug build (default: release)
#
# Then: export PKG_CONFIG_PATH=<target dir>/<profile>/jazz-ffi-static (the
# target dir is $CARGO_TARGET_DIR, else <repo>/target).
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repo="$(cd "$here/../.." && pwd)"
profile=release
flag=--release
mode=build
for arg in "$@"; do
  case "$arg" in
    --debug) profile=debug; flag= ;;
    --lib-only) mode=lib ;;
    --check) mode=check ;;
    *) echo "build-ffi.sh: unknown argument $arg" >&2; exit 2 ;;
  esac
done
target="${CARGO_TARGET_DIR:-$repo/target}"
swift_out="$here/Sources/JazzFFI/jazz_ffi.swift"
header_out="$here/Sources/jazz_ffiFFI/jazz_ffiFFI.h"

cd "$repo"
# The static library, and the native libraries it needs on this platform.
# Uncolored: with CARGO_TERM_COLOR=always (dtolnay/rust-toolchain sets it) the
# note ends in an escape code, which turns the last flag into "-lc<ESC>[0m".
native=$(cargo rustc -q --color never -p jazz-ffi $flag --lib --crate-type staticlib -- --print native-static-libs 2>&1 \
  | sed -n 's/.*native-static-libs: //p' | tail -1)

if [[ "$mode" != lib ]]; then
  cargo build -q -p jazz-ffi $flag --lib
  lib="$target/$profile/libjazz_ffi.so"
  [[ -f "$lib" ]] || lib="$target/$profile/libjazz_ffi.dylib"
  out="$(mktemp -d)"
  trap 'rm -rf "$out"' EXIT
  cargo run -q -p jazz-ffi --features bindgen --bin uniffi-bindgen -- \
    generate --library "$lib" --language swift --out-dir "$out" --no-format
  if [[ "$mode" == check ]]; then
    if ! cmp -s "$out/jazz_ffi.swift" "$swift_out" || ! cmp -s "$out/jazz_ffiFFI.h" "$header_out"; then
      echo "packages/jazz-swift: the committed UniFFI sources are stale; run packages/jazz-swift/scripts/build-ffi.sh and commit them" >&2
      exit 1
    fi
    echo "packages/jazz-swift: the committed UniFFI sources are current"
    exit 0
  fi
  cp "$out/jazz_ffi.swift" "$swift_out"
  cp "$out/jazz_ffiFFI.h" "$header_out"
fi

# Only the static library in its directory: with a .so beside it, the
# linker would pick the shared one and binaries would need it at run time.
dir="$target/$profile/jazz-ffi-static"
mkdir -p "$dir"
cp "$target/$profile/libjazz_ffi.a" "$dir/"
version=$(sed -n 's/^version = "\(.*\)"/\1/p' "$repo/crates/jazz-ffi/Cargo.toml" | head -1)
cat > "$dir/jazz_ffi.pc" <<PC
libdir=$dir

Name: jazz_ffi
Description: Jazz's core for the Jazz Swift package (crates/jazz-ffi)
Version: $version
Libs: -L\${libdir} -ljazz_ffi $native
Cflags:
PC
echo "libjazz_ffi.a and jazz_ffi.pc are in $dir"
echo "export PKG_CONFIG_PATH=$dir"
