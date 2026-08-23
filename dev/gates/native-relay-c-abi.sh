#!/usr/bin/env bash
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
output_dir=$(mktemp -d)
trap 'rm -rf "$output_dir"' EXIT

cargo build --manifest-path "$root/crates/jazz-native-relay/Cargo.toml" --release

cc \
  -I"$root/crates/jazz-native-relay/include" \
  "$root/crates/jazz-native-relay/tests/c_abi_probe.c" \
  "$root/target/release/libjazz_native_relay.a" \
  -ldl -lm -lpthread \
  -o "$output_dir/jazz-native-relay-c-abi"

"$output_dir/jazz-native-relay-c-abi"

c++ \
  -I"$root/crates/jazz-native-relay/include" \
  "$root/crates/jazz-native-relay/tests/cpp_abi_probe.cpp" \
  "$root/target/release/libjazz_native_relay.a" \
  -ldl -lm -lpthread \
  -o "$output_dir/jazz-native-relay-cpp-abi"

"$output_dir/jazz-native-relay-cpp-abi"
