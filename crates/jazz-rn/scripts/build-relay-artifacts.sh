#!/usr/bin/env bash
# Build the shared relay C ABI for the platform package. This script is used by
# trusted platform-artifact jobs; it deliberately does not download toolchains
# or write outside the package staging directories.
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
package="$root/crates/jazz-rn"
relay_manifest="$root/crates/jazz-native-relay/Cargo.toml"
platform=${1:?usage: build-relay-artifacts.sh <android|android-slice <abi>|android-manifest|ios>}
abi=$(sed -nE 's/^pub const NATIVE_RELAY_ABI_V1: u16 = ([0-9]+);/\1/p' \
  "$root/crates/jazz-native-relay/src/lib.rs")
if [[ -z "$abi" ]]; then
  echo "could not determine native relay ABI version" >&2
  exit 1
fi

# This is intentionally a content fingerprint rather than only a commit ID:
# release assembly can compare it after extracting an npm tarball, and cache
# keys can avoid rebuilding when unrelated JavaScript changes land.
native_source_fingerprint=$(node - "$root" <<'NODE'
const { createHash } = require("node:crypto");
const { execFileSync } = require("node:child_process");
const root = process.argv[2];
const source = execFileSync(
  "git",
  [
    "-C", root, "ls-tree", "-r", "--full-tree", "HEAD", "--",
    "Cargo.lock", "Cargo.toml",
    "crates/jazz", "crates/jazz-compression", "crates/jazz-e2ee", "crates/jazz-e2ee-client",
    "crates/jazz-native-relay", "crates/jazz-native-transport",
    "crates/jazz-rn/scripts/build-relay-artifacts.sh",
  ],
);
process.stdout.write(createHash("sha256").update(source).digest("hex"));
NODE
)
if [[ ! "$native_source_fingerprint" =~ ^[0-9a-f]{64}$ ]]; then
  echo "could not determine native relay source fingerprint" >&2
  exit 1
fi

write_manifest() {
  local destination=$1
  shift
  local source_revision
  source_revision=${JAZZ_NATIVE_RELAY_SOURCE_REVISION:-$(git -C "$root" rev-parse HEAD)}
  local cargo_ndk_version=${JAZZ_NATIVE_RELAY_CARGO_NDK_VERSION:-}
  node - "$destination" "$abi" "$source_revision" "$native_source_fingerprint" "$cargo_ndk_version" "$@" <<'NODE'
const { createHash } = require("node:crypto");
const { readdirSync, readFileSync, statSync, writeFileSync } = require("node:fs");
const { join, relative } = require("node:path");
const [destination, abi, sourceRevision, nativeSourceFingerprint, cargoNdkVersion, ...roots] = process.argv.slice(2);
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
writeFileSync(destination, JSON.stringify({ format: 2, nativeRelayAbi: Number(abi), sourceRevision, nativeSourceFingerprint, toolchain, files }, null, 2) + "\n");
NODE
}

stage_header() {
  mkdir -p "$package/native/include"
  cp "$root/crates/jazz-native-relay/include/jazz_native_relay.h" "$package/native/include/"
}

case "$platform" in
  android|android-slice|android-manifest)
    # `android` builds and seals every ABI. Release CI splits that into one
    # `android-slice <abi>` job per ABI and an `android-manifest` job that
    # seals the downloaded slices with the same manifest.
    stage="$package/android/src/main/jniLibs"
    declare -A rust_targets=(
      [arm64-v8a]=aarch64-linux-android
      [armeabi-v7a]=armv7-linux-androideabi
      [x86_64]=x86_64-linux-android
    )
    if [[ "$platform" == android-manifest ]]; then
      if [[ ! "${JAZZ_NATIVE_RELAY_CARGO_NDK_VERSION:-}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo "android-manifest requires JAZZ_NATIVE_RELAY_CARGO_NDK_VERSION of the slice builds" >&2
        exit 1
      fi
      for android_abi in "${!rust_targets[@]}"; do
        [[ -f "$stage/$android_abi/libjazz_native_relay.a" ]] || {
          echo "missing Android relay slice: $android_abi" >&2
          exit 1
        }
      done
      if [[ $(find "$stage" -type f | wc -l) -ne ${#rust_targets[@]} ]]; then
        echo "unexpected files among the Android relay slices" >&2
        exit 1
      fi
      stage_header
      write_manifest "$package/android/jazz-native-relay.manifest.json" "$stage"
      exit 0
    fi
    command -v cargo-ndk >/dev/null || {
      echo "Android relay artifacts require cargo-ndk; run dev/scripts/install-jazz-rn-deps.sh" >&2
      exit 1
    }
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
    android_abis=("${!rust_targets[@]}")
    if [[ "$platform" == android-slice ]]; then
      android_abis=("${2:?usage: build-relay-artifacts.sh android-slice <abi>}")
      [[ -n "${rust_targets[${android_abis[0]}]:-}" ]] || {
        echo "unknown Android ABI: ${android_abis[0]}" >&2
        exit 1
      }
      rm -rf "$stage/${android_abis[0]}"
    else
      stage_header
      rm -rf "$stage"
    fi
    # The staticlib carries DWARF for the relay and the Rust std it links:
    # strip it with the NDK's own llvm-strip (found the way cargo-ndk finds
    # the NDK) so the npm package stays smaller (#6181). Symbols stay, so
    # consumer apps link as before and backtraces keep function names.
    ndk=${ANDROID_NDK_HOME:-${ANDROID_NDK_ROOT:-}}
    sdk=${ANDROID_SDK_ROOT:-${ANDROID_HOME:-}}
    if [[ -z "$ndk" && -n "$sdk" && -d "$sdk/ndk" ]]; then
      ndk=$(find "$sdk/ndk" -mindepth 1 -maxdepth 1 -type d | sort -V | tail -1)
    fi
    llvm_strip=
    if [[ -n "$ndk" && -d "$ndk/toolchains/llvm/prebuilt" ]]; then
      llvm_strip=$(find "$ndk/toolchains/llvm/prebuilt" -mindepth 3 -maxdepth 3 -path '*/bin/llvm-strip' | head -1)
    fi
    [[ -n "$llvm_strip" && -x "$llvm_strip" ]] || {
      echo "could not find the Android NDK's llvm-strip; set ANDROID_NDK_HOME" >&2
      exit 1
    }
    mkdir -p "$stage"
    for android_abi in "${android_abis[@]}"; do
      rust_target=${rust_targets[$android_abi]}
      cargo ndk -t "$android_abi" rustc --manifest-path "$relay_manifest" --features e2ee-client --crate-type staticlib --profile release-mobile
      mkdir -p "$stage/$android_abi"
      cp "$root/target/$rust_target/release-mobile/libjazz_native_relay.a" "$stage/$android_abi/"
      "$llvm_strip" --strip-debug "$stage/$android_abi/libjazz_native_relay.a"
    done
    if [[ "$platform" == android ]]; then
      write_manifest "$package/android/jazz-native-relay.manifest.json" "$stage"
    fi
    ;;
  ios)
    command -v xcodebuild >/dev/null || {
      echo "iOS relay artifacts require macOS Xcode and xcodebuild" >&2
      exit 1
    }
    # The oldest iOS an app using jazz-rn may target (React Native 0.81 /
    # Expo SDK 54: 15.1). rustc and cc-rs both read it; unset, cc-rs compiles
    # ring and SQLite for the Xcode SDK version and apps get "built for newer
    # iOS version" link warnings. verify-relay-artifacts.mjs checks every object.
    export IPHONEOS_DEPLOYMENT_TARGET=15.1
    device_target=aarch64-apple-ios
    stage_header
    simulator_targets=(aarch64-apple-ios-sim x86_64-apple-ios)
    # JAZZ_NATIVE_RELAY_SLICES: build nothing and assemble prebuilt
    # staticlibs, $JAZZ_NATIVE_RELAY_SLICES/<target>/libjazz_native_relay.a,
    # each built by the cargo command below (CI builds one job per target).
    lib() { echo "$root/target/$1/release-mobile/libjazz_native_relay.a"; }
    if [[ -n "${JAZZ_NATIVE_RELAY_SLICES:-}" ]]; then
      lib() { echo "$JAZZ_NATIVE_RELAY_SLICES/$1/libjazz_native_relay.a"; }
      for target in "$device_target" "${simulator_targets[@]}"; do
        [[ -f "$(lib "$target")" ]] || { echo "no prebuilt $(lib "$target")" >&2; exit 1; }
      done
    else
      for target in "$device_target" "${simulator_targets[@]}"; do
        cargo rustc --manifest-path "$relay_manifest" --features e2ee-client --target "$target" --crate-type staticlib --profile release-mobile
      done
    fi
    staging=$(mktemp -d)
    trap 'rm -rf "$staging"' EXIT
    simulator_stage="$staging/simulator"
    mkdir -p "$simulator_stage"
    lipo -create \
      "$(lib aarch64-apple-ios-sim)" \
      "$(lib x86_64-apple-ios)" \
      -output "$simulator_stage/libjazz_native_relay.a"
    framework="$package/JazzNativeRelay.xcframework"
    rm -rf "$framework"
    xcodebuild -create-xcframework \
      -library "$(lib "$device_target")" -headers "$root/crates/jazz-native-relay/include" \
      -library "$simulator_stage/libjazz_native_relay.a" -headers "$root/crates/jazz-native-relay/include" \
      -output "$framework"
    # xcodebuild lists the libraries (and their architectures) in any order:
    # sort them, so the same slices give the same Info.plist and manifest.
    python3 - "$framework/Info.plist" <<'PY'
import plistlib, sys
path = sys.argv[1]
with open(path, "rb") as f:
    info = plistlib.load(f)
for library in info["AvailableLibraries"]:
    library["SupportedArchitectures"].sort()
info["AvailableLibraries"].sort(key=lambda library: library["LibraryIdentifier"])
with open(path, "wb") as f:
    plistlib.dump(info, f, sort_keys=True)
PY
    write_manifest "$package/ios/jazz-native-relay.manifest.json" "$framework"
    ;;
  *)
    echo "unsupported relay artifact platform: $platform" >&2
    exit 1
    ;;
esac
