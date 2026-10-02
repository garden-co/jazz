#!/usr/bin/env bash
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
output_dir=$(mktemp -d)
trap 'rm -rf "$output_dir"' EXIT

# Exercise the profile the relay actually ships under (fat LTO + panic=abort),
# mirroring crates/jazz-rn/scripts/build-relay-artifacts.sh.
cargo rustc --manifest-path "$root/crates/jazz-native-relay/Cargo.toml" --crate-type staticlib --profile release-mobile

cc \
  -I"$root/crates/jazz-native-relay/include" \
  "$root/crates/jazz-native-relay/tests/c_abi_probe.c" \
  "$root/target/release-mobile/libjazz_native_relay.a" \
  -ldl -lm -lpthread \
  -o "$output_dir/jazz-native-relay-c-abi"

"$output_dir/jazz-native-relay-c-abi"

c++ \
  -I"$root/crates/jazz-native-relay/include" \
  "$root/crates/jazz-native-relay/tests/cpp_abi_probe.cpp" \
  "$root/target/release-mobile/libjazz_native_relay.a" \
  -ldl -lm -lpthread \
  -o "$output_dir/jazz-native-relay-cpp-abi"

"$output_dir/jazz-native-relay-cpp-abi"
