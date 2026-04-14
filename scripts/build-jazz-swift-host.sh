#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output_root="${JAZZ_SWIFT_HOST_ARTIFACTS_DIR:-$repo_root/crates/jazz-swift/artifacts/macos}"
profile="${JAZZ_SWIFT_PROFILE:-release}"

mkdir -p "$output_root"

build_args=(cargo --config 'net.git-fetch-with-cli=true' build -p jazz-swift --lib)
if [[ "$profile" == "release" ]]; then
  build_args+=(--release)
elif [[ "$profile" != "debug" ]]; then
  echo "Unsupported JAZZ_SWIFT_PROFILE: $profile" >&2
  exit 1
fi

"${build_args[@]}"

source_lib="$repo_root/target/$profile/libjazz_swift.a"
if [[ ! -f "$source_lib" ]]; then
  echo "Expected host static library not found at $source_lib" >&2
  exit 1
fi

cp "$source_lib" "$output_root/"

echo "Host static library copied to $output_root"
