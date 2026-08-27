#!/usr/bin/env bash
# Build the shared relay C ABI for the platform package. This script is used by
# trusted platform-artifact jobs; it deliberately does not download toolchains
# or write outside the package staging directories.
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
package="$root/crates/jazz-rn"
relay_manifest="$root/crates/jazz-native-relay/Cargo.toml"
platform=${1:?usage: build-relay-artifacts.sh <android|ios>}
abi=$(sed -nE 's/^pub const NATIVE_RELAY_ABI_VERSION: u16 = ([0-9]+);/\1/p' \
  "$root/crates/jazz-native-relay/src/lib.rs")
if [[ -z "$abi" ]]; then
  echo "could not determine native relay ABI version" >&2
  exit 1
fi

write_manifest() {
  local destination=$1
  shift
  local source_revision
  source_revision=${JAZZ_NATIVE_RELAY_SOURCE_REVISION:-$(git -C "$root" rev-parse HEAD)}
  local cargo_ndk_version=${JAZZ_NATIVE_RELAY_CARGO_NDK_VERSION:-}
  node - "$destination" "$abi" "$source_revision" "$cargo_ndk_version" "$@" <<'NODE'
const { createHash } = require("node:crypto");
const { readdirSync, readFileSync, statSync, writeFileSync } = require("node:fs");
const { join, relative } = require("node:path");
const [destination, abi, sourceRevision, cargoNdkVersion, ...roots] = process.argv.slice(2);
const files = [];
const visit = (root, directory = root) => {
  for (const name of readdirSync(directory).sort()) {
    const path = join(directory, name), stat = statSync(path);
    if (stat.isDirectory()) visit(root, path);
    else if (stat.isFile()) {
      files.push({
        path: relative(root, path).split("\\").join("/"),
        sha256: createHash("sha256").update(readFileSync(path)).digest("hex"),
      });
    } else throw new Error(`relay artifact is not a regular file: ${path}`);
  }
};
for (const root of roots) visit(root);
if (!files.length) throw new Error("relay artifact build produced no static libraries");
const toolchain = cargoNdkVersion ? { cargoNdk: cargoNdkVersion } : undefined;
writeFileSync(destination, JSON.stringify({ format: 1, nativeRelayAbi: Number(abi), sourceRevision, toolchain, files }, null, 2) + "\n");
NODE
}

stage_header() {
  mkdir -p "$package/native/include"
  cp "$root/crates/jazz-native-relay/include/jazz_native_relay.h" "$package/native/include/"
}

case "$platform" in
  android)
    command -v cargo-ndk >/dev/null || {
      echo "Android relay artifacts require cargo-ndk; run dev/scripts/install-jazz-rn-deps.sh" >&2
      exit 1
    }
    stage="$package/android/src/main/jniLibs"
    detected_cargo_ndk_version=$(cargo ndk --version | sed -nE 's/.* ([0-9]+\.[0-9]+\.[0-9]+)$/\1/p')
    if [[ -z "$detected_cargo_ndk_version" ]]; then
      echo "could not determine cargo-ndk version" >&2
      exit 1
    fi
    if [[ -n "${JAZZ_NATIVE_RELAY_CARGO_NDK_VERSION:-}" && "$JAZZ_NATIVE_RELAY_CARGO_NDK_VERSION" != "$detected_cargo_ndk_version" ]]; then
      echo "cargo-ndk version $detected_cargo_ndk_version does not match requested $JAZZ_NATIVE_RELAY_CARGO_NDK_VERSION" >&2
      exit 1
    fi
    export JAZZ_NATIVE_RELAY_CARGO_NDK_VERSION="$detected_cargo_ndk_version"
    stage_header
    rm -rf "$stage"
    mkdir -p "$stage"
    declare -A rust_targets=(
      [arm64-v8a]=aarch64-linux-android
      [armeabi-v7a]=armv7-linux-androideabi
      [x86]=i686-linux-android
      [x86_64]=x86_64-linux-android
    )
    for android_abi in "${!rust_targets[@]}"; do
      rust_target=${rust_targets[$android_abi]}
      cargo ndk -t "$android_abi" build --manifest-path "$relay_manifest" --release
      mkdir -p "$stage/$android_abi"
      cp "$root/target/$rust_target/release/libjazz_native_relay.a" "$stage/$android_abi/"
    done
    write_manifest "$package/android/jazz-native-relay.manifest.json" "$stage"
    ;;
  ios)
    command -v xcodebuild >/dev/null || {
      echo "iOS relay artifacts require macOS Xcode and xcodebuild" >&2
      exit 1
    }
    device_target=aarch64-apple-ios
    stage_header
    simulator_targets=(aarch64-apple-ios-sim x86_64-apple-ios)
    for target in "$device_target" "${simulator_targets[@]}"; do
      cargo build --manifest-path "$relay_manifest" --target "$target" --release
    done
    staging=$(mktemp -d)
    trap 'rm -rf "$staging"' EXIT
    lipo -create \
      "$root/target/aarch64-apple-ios-sim/release/libjazz_native_relay.a" \
      "$root/target/x86_64-apple-ios/release/libjazz_native_relay.a" \
      -output "$staging/libjazz_native_relay_simulator.a"
    framework="$package/JazzNativeRelay.xcframework"
    rm -rf "$framework"
    xcodebuild -create-xcframework \
      -library "$root/target/$device_target/release/libjazz_native_relay.a" -headers "$root/crates/jazz-native-relay/include" \
      -library "$staging/libjazz_native_relay_simulator.a" -headers "$root/crates/jazz-native-relay/include" \
      -output "$framework"
    write_manifest "$package/ios/jazz-native-relay.manifest.json" "$framework"
    ;;
  *)
    echo "unsupported relay artifact platform: $platform" >&2
    exit 1
    ;;
esac
