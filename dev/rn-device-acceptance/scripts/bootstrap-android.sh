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
reset_cache_path() {
  # These are exact children of the caller-selected lane cache, never a broad path.
  rm -rf -- "$1"
}
mark_verified() { printf '%s\n' "$2" >"$1/.jazz-pinned-sha256"; }
has_marker() { [[ -f "$1/.jazz-pinned-sha256" && $(<"$1/.jazz-pinned-sha256") == "$2" ]]; }
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
if ! has_marker "$jdk" "166774efcf0f722f2ee18eba0039de2d685b350ee14d7b69e6f83437dafd2af1" ||
  ! "$jdk/bin/java" -version 2>&1 | grep -q '17\.0\.16'; then
  reset_cache_path "$jdk"
  mkdir -p "$jdk"
  tar -xzf "$downloads/OpenJDK17U-jdk_x64_linux_hotspot_17.0.16_8.tar.gz" -C "$jdk" --strip-components=1
  mark_verified "$jdk" "166774efcf0f722f2ee18eba0039de2d685b350ee14d7b69e6f83437dafd2af1"
fi
export JAVA_HOME="$jdk" ANDROID_SDK_ROOT="$sdk"
fetch \
  "https://dl.google.com/android/repository/commandlinetools-linux-13114758_latest.zip" \
  "$downloads/commandlinetools-linux-13114758_latest.zip" \
  "7ec965280a073311c339e571cd5de778b9975026cfcbe79f2b1cdcb1e15317ee"
tools="$sdk/cmdline-tools/latest"
if ! has_marker "$tools" "7ec965280a073311c339e571cd5de778b9975026cfcbe79f2b1cdcb1e15317ee" ||
  ! "$tools/bin/sdkmanager" --version 2>/dev/null | grep -qx '19\.0'; then
  reset_cache_path "$sdk/cmdline-tools"
  mkdir -p "$sdk/cmdline-tools"
  unzip -q "$downloads/commandlinetools-linux-13114758_latest.zip" -d "$sdk/cmdline-tools"
  mv "$sdk/cmdline-tools/cmdline-tools" "$sdk/cmdline-tools/latest"
  mark_verified "$tools" "7ec965280a073311c339e571cd5de778b9975026cfcbe79f2b1cdcb1e15317ee"
fi

manager="$sdk/cmdline-tools/latest/bin/sdkmanager"
set +o pipefail
yes | "$manager" --licenses >/dev/null
license_status=${PIPESTATUS[1]}
set -o pipefail
if (( license_status != 0 )); then
  echo "Android SDK license acceptance failed" >&2
  exit "$license_status"
fi
"$manager" "platform-tools" "platforms;android-36" "build-tools;36.0.0"

# sdkmanager's package metadata is not an archive pin we control, so retain and
# verify the exact vendor NDK archive before expanding it into this lane cache.
ndk_revision=27.1.12297006
ndk_archive="$downloads/android-ndk-r27b-linux.zip"
fetch \
  "https://dl.google.com/android/repository/android-ndk-r27b-linux.zip" \
  "$ndk_archive" \
  "33e16af1a6bbabe12cad54b2117085c07eab7e4fa67cdd831805f0e94fd826c1"
if ! has_marker "$sdk/ndk/$ndk_revision" "33e16af1a6bbabe12cad54b2117085c07eab7e4fa67cdd831805f0e94fd826c1" ||
  ! grep -qx "Pkg.Revision = $ndk_revision" "$sdk/ndk/$ndk_revision/source.properties"; then
  reset_cache_path "$sdk/ndk/$ndk_revision"
  reset_cache_path "$cache/ndk-unpack-$ndk_revision"
  unpack="$cache/ndk-unpack-$ndk_revision"
  mkdir -p "$unpack" "$sdk/ndk"
  unzip -q "$ndk_archive" -d "$unpack"
  mv "$unpack/android-ndk-r27b" "$sdk/ndk/$ndk_revision"
  mark_verified "$sdk/ndk/$ndk_revision" "33e16af1a6bbabe12cad54b2117085c07eab7e4fa67cdd831805f0e94fd826c1"
fi
grep -qx "Pkg.Revision = $ndk_revision" "$sdk/ndk/$ndk_revision/source.properties" || {
  echo "refusing unexpected cached Android NDK revision" >&2
  exit 1
}
if "$want_emulator"; then
  "$manager" "emulator" "system-images;android-35;google_apis;x86_64"
fi

if ! "$cache/cargo/bin/cargo-ndk" --version 2>/dev/null | grep -q '4\.1\.2$'; then
  reset_cache_path "$cache/cargo"
  CARGO_INSTALL_ROOT="$cache/cargo" cargo install cargo-ndk@4.1.2 --locked
fi
printf 'JAZZ_DEVICE_TOOLCACHE=%s\n' "$cache"
