#!/usr/bin/env bash
# Builds JazzFFI.xcframework (libjazz_ffi for macOS arm64+x86_64, iOS, and
# the iOS simulator arm64+x86_64, with the UniFFI header and module map)
# and zips it for a SwiftPM binaryTarget. Prints the zip's checksum.
# Needs macOS with Xcode and the Rust targets below. Output: $1 (default
# target/swift-xcframework). With JAZZ_FFI_SLICES set, builds nothing and
# assembles prebuilt staticlibs instead, $JAZZ_FFI_SLICES/<target>/libjazz_ffi.a
# (each built as below, e.g. by a parallel CI job per target). With
# JAZZ_FFI_TARGET=<target>, builds that one slice as below into
# $1/<target>/libjazz_ffi.a and stops: a from-scratch build split by target.
set -euo pipefail
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
out=${1:-$root/target/swift-xcframework}
export MACOSX_DEPLOYMENT_TARGET=13.0
export IPHONEOS_DEPLOYMENT_TARGET=16.0
targets=(aarch64-apple-darwin x86_64-apple-darwin aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios)
cd "$root"
if [[ -n "${JAZZ_FFI_TARGET:-}" ]]; then
  [[ " ${targets[*]} " == *" $JAZZ_FFI_TARGET "* ]] || { echo "not a slice target: $JAZZ_FFI_TARGET" >&2; exit 1; }
  targets=("$JAZZ_FFI_TARGET")
fi
for target in "${targets[@]}"; do
  [[ -z "${JAZZ_FFI_SLICES:-}" ]] || break
  cargo rustc -p jazz-ffi --release --lib --target "$target" --crate-type staticlib -- --print native-static-libs 2>&1 \
    | grep -E "native-static-libs|error" || true
done
if [[ -n "${JAZZ_FFI_TARGET:-}" ]]; then
  mkdir -p "$out/$JAZZ_FFI_TARGET"
  cp "$root/target/$JAZZ_FFI_TARGET/release/libjazz_ffi.a" "$out/$JAZZ_FFI_TARGET/"
  exit 0
fi
if [[ -n "${JAZZ_FFI_SLICES:-}" ]]; then
  lib() { echo "$JAZZ_FFI_SLICES/$1/libjazz_ffi.a"; }
  for target in "${targets[@]}"; do [[ -f "$(lib "$target")" ]] || { echo "no $(lib "$target")" >&2; exit 1; }; done
else
  lib() { echo "$root/target/$1/release/libjazz_ffi.a"; }
fi
rm -rf "$out" && mkdir -p "$out/macos" "$out/sim" "$out/headers"
lipo -create "$(lib aarch64-apple-darwin)" "$(lib x86_64-apple-darwin)" -output "$out/macos/libjazz_ffi.a"
lipo -create "$(lib aarch64-apple-ios-sim)" "$(lib x86_64-apple-ios)" -output "$out/sim/libjazz_ffi.a"
cp "$root/packages/jazz-swift/Sources/jazz_ffiFFI/jazz_ffiFFI.h" "$out/headers/"
# SwiftPM links a binary target's library itself: no `link` line.
cat > "$out/headers/module.modulemap" <<'MAP'
module jazz_ffiFFI {
    header "jazz_ffiFFI.h"
    export *
}
MAP
xcodebuild -create-xcframework \
  -library "$out/macos/libjazz_ffi.a" -headers "$out/headers" \
  -library "$(lib aarch64-apple-ios)" -headers "$out/headers" \
  -library "$out/sim/libjazz_ffi.a" -headers "$out/headers" \
  -output "$out/JazzFFI.xcframework"
# xcodebuild lists the libraries (and their architectures) in any order:
# sort them, so the same slices give the same Info.plist.
python3 - "$out/JazzFFI.xcframework/Info.plist" <<'PY'
import plistlib, sys
path = sys.argv[1]
with open(path, "rb") as f:
    info = plistlib.load(f)
for library in info["AvailableLibraries"]:
    library["SupportedArchitectures"].sort()
info["AvailableLibraries"].sort(key=lambda library: library["LibraryIdentifier"])
with open(path, "wb") as f:
    plistlib.dump(info, f, sort_keys=True)
PY
# One fixed mtime for everything zipped: the same slices give the same zip,
# so the checksum SwiftPM pins can be reproduced from the source.
find "$out/JazzFFI.xcframework" -exec touch -h -t 200001010000 {} +
(cd "$out" && ditto -c -k --sequesterRsrc --keepParent JazzFFI.xcframework JazzFFI.xcframework.zip)
checksum=$(swift package compute-checksum "$out/JazzFFI.xcframework.zip")
echo "$checksum" > "$out/JazzFFI.xcframework.zip.checksum"
echo "JazzFFI.xcframework.zip checksum: $checksum"
