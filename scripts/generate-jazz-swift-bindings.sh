#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
target_dir="${JAZZ_SWIFT_TARGET_DIR:-$repo_root/target}"
output_dir="${JAZZ_SWIFT_GENERATED_DIR:-$repo_root/crates/jazz-swift/generated}"
package_sources_dir="${JAZZ_SWIFT_PACKAGE_SOURCES_DIR:-$repo_root/crates/jazz-swift/Sources/JazzSwiftBindings}"
package_generated_file="$package_sources_dir/jazz_swift.swift"
profile="${JAZZ_SWIFT_PROFILE:-debug}"
local_uniffi_bindgen="$repo_root/target/tools/bin/uniffi-bindgen"

if command -v uniffi-bindgen >/dev/null 2>&1; then
  uniffi_bindgen="$(command -v uniffi-bindgen)"
elif [[ -x "$local_uniffi_bindgen" ]]; then
  uniffi_bindgen="$local_uniffi_bindgen"
else
  echo "Missing uniffi-bindgen." >&2
  echo "Install it with: cargo install uniffi --version 0.30.0 --features cli --root $repo_root/target/tools" >&2
  exit 1
fi

mkdir -p "$output_dir"
mkdir -p "$package_sources_dir"

build_cmd=(cargo --config 'net.git-fetch-with-cli=true' build -p jazz-swift --lib)
if [[ "$profile" == "release" ]]; then
  build_cmd+=(--release)
elif [[ "$profile" != "debug" ]]; then
  echo "Unsupported JAZZ_SWIFT_PROFILE: $profile" >&2
  exit 1
fi

"${build_cmd[@]}"

case "$OSTYPE" in
  darwin*)
    library_path="$target_dir/$profile/libjazz_swift.dylib"
    ;;
  linux*)
    library_path="$target_dir/$profile/libjazz_swift.so"
    ;;
  *)
    echo "Unsupported host OS for UniFFI generation: $OSTYPE" >&2
    exit 1
    ;;
esac

if [[ ! -f "$library_path" ]]; then
  echo "Expected host library not found at $library_path" >&2
  exit 1
fi

"$uniffi_bindgen" generate \
  --library "$library_path" \
  --language swift \
  --out-dir "$output_dir"

echo "Swift bindings generated in $output_dir"

{
  printf '%s\n' '#if canImport(jazz_swiftFFI)'
  cat "$output_dir/jazz_swift.swift"
  printf '\n%s\n' '#endif'
} > "$package_generated_file"

echo "Swift package source synced to $package_generated_file"
