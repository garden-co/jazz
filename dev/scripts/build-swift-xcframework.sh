#!/usr/bin/env bash
# Builds JazzFFI.xcframework (libjazz_ffi for macOS arm64+x86_64, iOS, and
# the iOS simulator arm64+x86_64, with the UniFFI header and module map)
# and zips it for a SwiftPM binaryTarget. Prints the zip's checksum.
# Needs macOS with Xcode and the Rust targets below. Output: $1 (default
# target/swift-xcframework).
set -euo pipefail
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
out=${1:-$root/target/swift-xcframework}
export MACOSX_DEPLOYMENT_TARGET=13.0
export IPHONEOS_DEPLOYMENT_TARGET=16.0
targets=(aarch64-apple-darwin x86_64-apple-darwin aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios)
cd "$root"
for target in "${targets[@]}"; do
  cargo rustc -p jazz-ffi --release --lib --target "$target" --crate-type staticlib -- --print native-static-libs 2>&1 \
    | grep -E "native-static-libs|error" || true
done
lib() { echo "$root/target/$1/release/libjazz_ffi.a"; }
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
(cd "$out" && ditto -c -k --sequesterRsrc --keepParent JazzFFI.xcframework JazzFFI.xcframework.zip)
checksum=$(swift package compute-checksum "$out/JazzFFI.xcframework.zip")
echo "$checksum" > "$out/JazzFFI.xcframework.zip.checksum"
echo "JazzFFI.xcframework.zip checksum: $checksum"
