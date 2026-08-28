#!/usr/bin/env bash
# Lane-local, pinned Android prerequisites for the real device acceptance app.
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cache=${JAZZ_DEVICE_TOOLCACHE:-"$root/.cache/android-device-acceptance"}
downloads="$cache/downloads"
jdk="$cache/jdk"
sdk="$cache/sdk"
verify_archive="$root/dev/rn-device-acceptance/scripts/verify-pinned-archive.sh"
want_emulator=false
[[ ${1:-} == --emulator ]] && want_emulator=true

mkdir -p "$downloads" "$jdk" "$sdk"
fetch() {
  local url=$1 destination=$2 expected_sha256=$3
  if [[ -f "$destination" ]]; then
    "$verify_archive" "$destination" "$expected_sha256" || {
      echo "refusing corrupt cached Android bootstrap archive: $destination" >&2
      exit 1
    }
    return
  fi
  curl --fail --location --retry 3 --output "$destination" "$url"
  "$verify_archive" "$destination" "$expected_sha256" || {
    echo "downloaded Android bootstrap archive failed its pinned checksum: $destination" >&2
    exit 1
  }
}

fetch \
  "https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.16%2B8/OpenJDK17U-jdk_x64_linux_hotspot_17.0.16_8.tar.gz" \
  "$downloads/OpenJDK17U-jdk_x64_linux_hotspot_17.0.16_8.tar.gz" \
  "166774efcf0f722f2ee18eba0039de2d685b350ee14d7b69e6f83437dafd2af1"
if [[ ! -x "$jdk/bin/java" ]]; then
  tar -xzf "$downloads/OpenJDK17U-jdk_x64_linux_hotspot_17.0.16_8.tar.gz" -C "$jdk" --strip-components=1
fi
fetch \
  "https://dl.google.com/android/repository/commandlinetools-linux-13114758_latest.zip" \
  "$downloads/commandlinetools-linux-13114758_latest.zip" \
  "7ec965280a073311c339e571cd5de778b9975026cfcbe79f2b1cdcb1e15317ee"
if [[ ! -x "$sdk/cmdline-tools/latest/bin/sdkmanager" ]]; then
  mkdir -p "$sdk/cmdline-tools"
  unzip -q "$downloads/commandlinetools-linux-13114758_latest.zip" -d "$sdk/cmdline-tools"
  mv "$sdk/cmdline-tools/cmdline-tools" "$sdk/cmdline-tools/latest"
fi

export JAVA_HOME="$jdk" ANDROID_SDK_ROOT="$sdk"
manager="$sdk/cmdline-tools/latest/bin/sdkmanager"
yes | "$manager" --licenses >/dev/null
"$manager" "platform-tools" "platforms;android-36" "build-tools;36.0.0"

# sdkmanager's package metadata is not an archive pin we control, so retain and
# verify the exact vendor NDK archive before expanding it into this lane cache.
ndk_revision=27.1.12297006
ndk_archive="$downloads/android-ndk-r27b-linux.zip"
fetch \
  "https://dl.google.com/android/repository/android-ndk-r27b-linux.zip" \
  "$ndk_archive" \
  "33e16af1a6bbabe12cad54b2117085c07eab7e4fa67cdd831805f0e94fd826c1"
if [[ ! -d "$sdk/ndk/$ndk_revision" ]]; then
  unpack="$cache/ndk-unpack-$ndk_revision"
  mkdir -p "$unpack" "$sdk/ndk"
  unzip -q "$ndk_archive" -d "$unpack"
  mv "$unpack/android-ndk-r27b" "$sdk/ndk/$ndk_revision"
fi
grep -qx "Pkg.Revision = $ndk_revision" "$sdk/ndk/$ndk_revision/source.properties" || {
  echo "refusing unexpected cached Android NDK revision" >&2
  exit 1
}
if "$want_emulator"; then
  "$manager" "emulator" "system-images;android-35;google_apis;x86_64"
fi

if [[ ! -x "$cache/cargo/bin/cargo-ndk" ]]; then
  CARGO_INSTALL_ROOT="$cache/cargo" cargo install cargo-ndk@4.1.2 --locked
fi
printf 'JAZZ_DEVICE_TOOLCACHE=%s\n' "$cache"
