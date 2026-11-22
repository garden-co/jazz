#!/usr/bin/env bash
# The Kotlin distribution, attached to a GitHub release (until jazz-kotlin is
# on Maven Central):
#   jazz-kotlin-<version>.jar          the JVM library with libjazz_ffi for every
#                                      platform in <natives> (JNA's layout:
#                                      linux-x86-64/libjazz_ffi.so, darwin-aarch64/
#                                      libjazz_ffi.dylib, win32-x86-64/jazz_ffi.dll, …)
#   jazz-kotlin-android-<version>.aar  with --android: the Android library
#   SHA256SUMS
#
# usage: dev/scripts/build-kotlin-dist.sh <natives> <out> [--android]
#
# <natives> is what attach-language-artifacts.yml's per-platform jobs build
# (`cargo build -p jazz-ffi --profile release`, the library renamed into its
# JNA directory). Locally, `mkdir -p n/linux-x86-64 && cp target/release/
# libjazz_ffi.so n/linux-x86-64/` makes a Linux-only one.
#
# Prebuilt Rust, so this runs no cargo (attach-language-artifacts.yml):
# KOTLIN_JVM_BINDINGS=<dir>, the Kotlin bindings uniffi-bindgen generated from
# the linux-x86_64 release library (the jar's -PuniffiBindings), and
# KOTLIN_ANDROID_JNI_LIBS and KOTLIN_ANDROID_BINDINGS for the AAR
# (dev/scripts/test-kotlin-android.sh).
set -euo pipefail
cd "$(dirname "$0")/../.."

natives="$(cd "$1" && pwd)"
out="$2"
android="${3:-}"
mkdir -p "$out"
out="$(cd "$out" && pwd)"

# Lockstep: jazz-tools' version, as the Gradle builds read it.
version="$(sed -n 's/^  "version": "\(.*\)",$/\1/p' packages/jazz-tools/package.json)"
found=0
for dir in "$natives"/*/; do
  ls "$dir" | grep -Eq '^(lib)?jazz_ffi\.(so|dylib|dll)$' || {
    echo "$dir holds no libjazz_ffi" >&2
    exit 1
  }
  found=$((found + 1))
done
[ "$found" -gt 0 ] || { echo "$natives holds no platform directories" >&2; exit 1; }

# The pinned, checksummed JDK and Gradle 8.14.3, whatever PATH has: a runner's
# Gradle 9 breaks AGP 8.13. test-kotlin-android.sh (--android) inherits it.
export KOTLIN_TOOLCHAIN=pinned
source dev/scripts/kotlin-toolchain.sh
gradle --version | grep -q '^Gradle 8\.14\.3$' || { gradle --version >&2; echo "not the pinned Gradle 8.14.3" >&2; exit 1; }

# The bindings are generated from this platform's build of the same sources.
jvm=(-PcargoProfile=release -PnativeLibs="$natives")
[ -n "${KOTLIN_JVM_BINDINGS:-}" ] && jvm+=(-PuniffiBindings="$(cd "$KOTLIN_JVM_BINDINGS" && pwd)")
gradle --no-daemon --console=plain -p packages/jazz-kotlin "${jvm[@]}" clean jar
cp "packages/jazz-kotlin/build/libs/jazz-kotlin-$version.jar" "$out/"

# The jar must hold every platform's library.
for dir in "$natives"/*/; do
  platform="$(basename "$dir")"
  unzip -l "$out/jazz-kotlin-$version.jar" | grep -q " $platform/" || {
    echo "jazz-kotlin-$version.jar lacks $platform" >&2
    exit 1
  }
done

if [ "$android" = "--android" ]; then
  CARGO_PROFILE=release-mobile bash dev/scripts/test-kotlin-android.sh
  cp packages/jazz-kotlin-android/build/outputs/aar/jazz-kotlin-android-release.aar \
    "$out/jazz-kotlin-android-$version.aar"
fi

(cd "$out" && sha256sum jazz-kotlin* > SHA256SUMS && cat SHA256SUMS)
