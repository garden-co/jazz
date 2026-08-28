#!/usr/bin/env bash
# Lane-local, pinned Android prerequisites for the real device acceptance app.
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cache=${JAZZ_DEVICE_TOOLCACHE:-"$root/.cache/android-device-acceptance"}
downloads="$cache/downloads"
jdk="$cache/jdk"
sdk="$cache/sdk"
want_emulator=false
[[ ${1:-} == --emulator ]] && want_emulator=true

mkdir -p "$downloads" "$jdk" "$sdk"
fetch() {
  local url=$1 destination=$2
  [[ -f "$destination" ]] || curl --fail --location --retry 3 --output "$destination" "$url"
}

fetch \
  "https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.16%2B8/OpenJDK17U-jdk_x64_linux_hotspot_17.0.16_8.tar.gz" \
  "$downloads/OpenJDK17U-jdk_x64_linux_hotspot_17.0.16_8.tar.gz"
if [[ ! -x "$jdk/bin/java" ]]; then
  tar -xzf "$downloads/OpenJDK17U-jdk_x64_linux_hotspot_17.0.16_8.tar.gz" -C "$jdk" --strip-components=1
fi
fetch \
  "https://dl.google.com/android/repository/commandlinetools-linux-13114758_latest.zip" \
  "$downloads/commandlinetools-linux-13114758_latest.zip"
if [[ ! -x "$sdk/cmdline-tools/latest/bin/sdkmanager" ]]; then
  mkdir -p "$sdk/cmdline-tools"
  unzip -q "$downloads/commandlinetools-linux-13114758_latest.zip" -d "$sdk/cmdline-tools"
  mv "$sdk/cmdline-tools/cmdline-tools" "$sdk/cmdline-tools/latest"
fi

export JAVA_HOME="$jdk" ANDROID_SDK_ROOT="$sdk"
manager="$sdk/cmdline-tools/latest/bin/sdkmanager"
yes | "$manager" --licenses >/dev/null
"$manager" "platform-tools" "platforms;android-36" "build-tools;36.0.0" "ndk;27.1.12297006"
if "$want_emulator"; then
  "$manager" "emulator" "system-images;android-35;google_apis;x86_64"
fi

if [[ ! -x "$cache/cargo/bin/cargo-ndk" ]]; then
  CARGO_INSTALL_ROOT="$cache/cargo" cargo install cargo-ndk@4.1.2 --locked
fi
printf 'JAZZ_DEVICE_TOOLCACHE=%s\n' "$cache"
