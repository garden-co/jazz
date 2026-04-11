#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output_root="${JAZZ_SWIFT_IOS_ARTIFACTS_DIR:-$repo_root/crates/jazz-swift/artifacts/ios}"
profile="${JAZZ_SWIFT_PROFILE:-release}"
targets=(${JAZZ_SWIFT_IOS_TARGETS:-aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios})

mkdir -p "$output_root"

build_args=(cargo --config 'net.git-fetch-with-cli=true' build -p jazz-swift --lib)
if [[ "$profile" == "release" ]]; then
  build_args+=(--release)
elif [[ "$profile" != "debug" ]]; then
  echo "Unsupported JAZZ_SWIFT_PROFILE: $profile" >&2
  exit 1
fi

for target in "${targets[@]}"; do
  "${build_args[@]}" --target "$target"

  source_lib="$repo_root/target/$target/$profile/libjazz_swift.a"
  target_dir="$output_root/$target"
  mkdir -p "$target_dir"

  if [[ ! -f "$source_lib" ]]; then
    echo "Expected iOS static library not found at $source_lib" >&2
    exit 1
  fi

  cp "$source_lib" "$target_dir/"
done

echo "iOS static libraries copied to $output_root"
