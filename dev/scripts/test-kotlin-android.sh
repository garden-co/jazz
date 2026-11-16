#!/usr/bin/env bash
# The Kotlin Android partition: a build check of jazz-kotlin-android, the AAR
# with jazz-ffi cross-compiled for arm64-v8a, armeabi-v7a and x86_64. No
# emulator or device tests. Fetches a JDK, Gradle, the Android SDK packages
# and the NDK into ~/.cache/jazz-kotlin, each pinned and checked against its
# published sha256, and installs cargo-ndk and the Rust Android targets.
#
# CARGO_PROFILE picks the Cargo profile the .so files are built with (default
# dev: the check is that everything cross-compiles, links and packages).
set -euo pipefail
cd "$(dirname "$0")/../.."

cache="${HOME}/.cache/jazz-kotlin"
# Pinned, and checked against the checksums Adoptium, Gradle and Google publish.
jdk_version=21.0.13+9
jdk_url="https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.13%2B9/OpenJDK21U-jdk_x64_linux_hotspot_21.0.13_9.tar.gz"
jdk_sha256=d7f5d5cf865a2902b70ff549b97586a7d3f234e016f041ab447ea740c7d66d1c
gradle_version=8.14.3
gradle_url="https://services.gradle.org/distributions/gradle-${gradle_version}-bin.zip"
gradle_sha256=bd71102213493060956ec229d946beee57158dbd89d0e62b91bca0fa2c5f3531
# The same SDK packages and NDK as jazz-rn (dev/rn-device-acceptance); the
# sha256s are of the zips whose sha1s Google lists in repository2-3.xml.
platform_url=https://dl.google.com/android/repository/platform-36_r02.zip
platform_sha256=37607369a28c5b640b3a7998868d45898ebcb777565a0e85f9acf36f29631d2e
build_tools_url=https://dl.google.com/android/repository/build-tools_r36_linux.zip
build_tools_sha256=5d9ac77fb6ff43d9da518a337b4fcf8f9097113df531d99ccefe80ef7ce8250b
ndk_revision=27.1.12297006
ndk_url=https://dl.google.com/android/repository/android-ndk-r27b-linux.zip
ndk_sha256=33e16af1a6bbabe12cad54b2117085c07eab7e4fa67cdd831805f0e94fd826c1
cargo_ndk_version=4.1.2
rust_targets=(aarch64-linux-android armv7-linux-androideabi x86_64-linux-android)

# Download $1 to $2 and check it has sha256 $3.
fetch() {
  curl -fsSL --retry 3 -o "$2.part" "$1"
  echo "$3  $2.part" | sha256sum -c --quiet -
  mv "$2.part" "$2"
}

# Unpack the pinned zip $1 (sha256 $2) into $3, whose single top-level
# directory becomes $3. A marker records the checksum it came from.
install_zip() {
  local url=$1 sha256=$2 dest=$3
  [ -f "$dest/.jazz-sha256" ] && [ "$(cat "$dest/.jazz-sha256")" = "$sha256" ] && return
  rm -rf "$dest" "$dest.unpack"
  mkdir -p "$dest.unpack"
  fetch "$url" "$dest.unpack.zip" "$sha256"
  unzip -q "$dest.unpack.zip" -d "$dest.unpack"
  rm "$dest.unpack.zip"
  mv "$dest.unpack"/* "$dest"
  rmdir "$dest.unpack"
  echo "$sha256" >"$dest/.jazz-sha256"
}

if ! command -v java >/dev/null; then
  if [ ! -x "$cache/jdk-$jdk_version/bin/java" ]; then
    mkdir -p "$cache/jdk-$jdk_version"
    fetch "$jdk_url" "$cache/jdk.tar.gz" "$jdk_sha256"
    tar xzf "$cache/jdk.tar.gz" -C "$cache/jdk-$jdk_version" --strip-components=1
    rm "$cache/jdk.tar.gz"
  fi
  export JAVA_HOME="$cache/jdk-$jdk_version"
  export PATH="$JAVA_HOME/bin:$PATH"
fi
if ! command -v gradle >/dev/null; then
  if [ ! -x "$cache/gradle-$gradle_version/bin/gradle" ]; then
    mkdir -p "$cache"
    fetch "$gradle_url" "$cache/gradle.zip" "$gradle_sha256"
    (cd "$cache" && unzip -q -o gradle.zip && rm gradle.zip)
  fi
  export PATH="$cache/gradle-$gradle_version/bin:$PATH"
fi

# An SDK of exactly the pinned packages, laid out as sdkmanager would.
sdk="$cache/android-sdk"
install_zip "$platform_url" "$platform_sha256" "$sdk/platforms/android-36"
install_zip "$build_tools_url" "$build_tools_sha256" "$sdk/build-tools/36.0.0"
install_zip "$ndk_url" "$ndk_sha256" "$sdk/ndk/$ndk_revision"
grep -qx "Pkg.Revision = $ndk_revision" "$sdk/ndk/$ndk_revision/source.properties"
export ANDROID_HOME="$sdk" ANDROID_NDK_HOME="$sdk/ndk/$ndk_revision"

rustup target add "${rust_targets[@]}"
if ! "$cache/cargo/bin/cargo-ndk" --version 2>/dev/null | grep -q " $cargo_ndk_version\$"; then
  CARGO_INSTALL_ROOT="$cache/cargo" cargo install "cargo-ndk@$cargo_ndk_version" --locked
fi
export PATH="$cache/cargo/bin:$PATH"

# KOTLIN_ANDROID_TASKS: more tasks to run with the build (dev/scripts/publish-kotlin.sh's
# publishToMavenLocal, publishToMavenCentral).
read -ra extra_tasks <<<"${KOTLIN_ANDROID_TASKS:-}"
gradle --no-daemon --console=plain -p packages/jazz-kotlin-android \
  -PcargoProfile="${CARGO_PROFILE:-dev}" assembleRelease "${extra_tasks[@]}"

# The AAR must carry jazz-ffi for every ABI, and the library's classes.
aar=packages/jazz-kotlin-android/build/outputs/aar/jazz-kotlin-android-release.aar
listing=$(unzip -l "$aar")
for abi in arm64-v8a armeabi-v7a x86_64; do
  grep -q "jni/$abi/libjazz_ffi.so" <<<"$listing" || { echo "$aar lacks jni/$abi/libjazz_ffi.so" >&2; exit 1; }
done
classes=$(mktemp -d)
unzip -q "$aar" classes.jar -d "$classes"
class_listing=$(unzip -l "$classes/classes.jar")
for class in tools/jazz/Jazz.class tools/jazz/UniffiNative.class uniffi/jazz_ffi/JazzHandle.class; do
  grep -q " $class\$" <<<"$class_listing" || { echo "$aar lacks $class" >&2; exit 1; }
done
rm -rf "$classes"
echo "built $aar"
