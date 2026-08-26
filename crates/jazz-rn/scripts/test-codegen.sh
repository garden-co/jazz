#!/usr/bin/env bash
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
output_dir=$(mktemp -d)
trap 'rm -rf "$output_dir"' EXIT

for platform in android ios; do
  if ! output=$(node "$root/node_modules/react-native/scripts/generate-codegen-artifacts.js" \
    --path "$root" \
    --outputPath "$output_dir/$platform" \
    --targetPlatform "$platform" 2>&1); then
    printf '%s\n' "$output" >&2
    echo "React Native Codegen exited unsuccessfully for $platform" >&2
    exit 1
  fi
  printf '%s\n' "$output"
  if [[ "$output" == *"Error:"* ]] || [[ "$output" == *"Unsupported"* ]]; then
    echo "React Native Codegen reported an error for $platform" >&2
    exit 1
  fi
  if ! rg -q 'NativeJazzRelay' "$output_dir/$platform"; then
    echo "React Native Codegen did not generate the JazzRelay module for $platform" >&2
    exit 1
  fi
  if [[ "$platform" == android ]] && ! rg -q 'getAbiVersion|execute' "$output_dir/$platform"; then
    echo "React Native Codegen did not generate the JazzRelay command methods for Android" >&2
    exit 1
  fi
done
