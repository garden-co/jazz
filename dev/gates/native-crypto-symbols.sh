#!/usr/bin/env bash
# No lattice or SHA-3 code in the native libraries (see
# dev/gates/check-no-lattice-sha3-symbols.mjs):
# - libjazz_ffi.so, the library jazz-kotlin (JVM) and the Swift package load;
# - the React Native relay (jazz-native-relay's staticlib), linked the way an
#   app links it: into an executable with --gc-sections, since a staticlib
#   alone keeps every object of its dependencies.
# Debug builds, as the bindings partition already has them; the release
# builds (and Android/iOS targets) are built only by the package workflows.
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
output_dir=$(mktemp -d)
trap 'rm -rf "$output_dir"' EXIT

cargo build --manifest-path "$root/Cargo.toml" -p jazz-ffi --lib
cargo build --manifest-path "$root/Cargo.toml" -p jazz-native-relay --lib

cc \
  -I"$root/crates/jazz-native-relay/include" \
  "$root/crates/jazz-native-relay/tests/c_abi_probe.c" \
  "$root/target/debug/libjazz_native_relay.a" \
  -Wl,--gc-sections -ldl -lm -lpthread \
  -o "$output_dir/jazz-native-relay"

node "$root/dev/gates/check-no-lattice-sha3-symbols.mjs" \
  "$root/target/debug/libjazz_ffi.so" \
  "$output_dir/jazz-native-relay"
