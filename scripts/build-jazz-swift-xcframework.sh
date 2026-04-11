#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
headers_dir="${JAZZ_SWIFT_GENERATED_DIR:-$repo_root/crates/jazz-swift/generated}"
ios_root="${JAZZ_SWIFT_IOS_ARTIFACTS_DIR:-$repo_root/crates/jazz-swift/artifacts/ios}"
output_path="${JAZZ_SWIFT_XCFRAMEWORK_PATH:-$repo_root/crates/jazz-swift/artifacts/JazzSwiftFFI.xcframework}"
targets=(${JAZZ_SWIFT_IOS_TARGETS:-aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios})

if ! command -v xcodebuild >/dev/null 2>&1; then
  echo "xcodebuild is required to assemble the JazzSwift xcframework." >&2
  exit 1
fi

if [[ ! -f "$headers_dir/jazz_swiftFFI.h" || ! -f "$headers_dir/jazz_swiftFFI.modulemap" ]]; then
  echo "Generated FFI header/modulemap missing in $headers_dir" >&2
  echo "Run bash scripts/generate-jazz-swift-bindings.sh first." >&2
  exit 1
fi

rm -rf "$output_path"

create_cmd=(xcodebuild -create-xcframework)
for target in "${targets[@]}"; do
  library_path="$ios_root/$target/libjazz_swift.a"
  if [[ ! -f "$library_path" ]]; then
    echo "Missing library slice: $library_path" >&2
    echo "Run bash scripts/build-jazz-swift-ios.sh first." >&2
    exit 1
  fi

  create_cmd+=(-library "$library_path" -headers "$headers_dir")
done

create_cmd+=(-output "$output_path")
"${create_cmd[@]}"

echo "xcframework assembled at $output_path"
