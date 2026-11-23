# An Android SDK of exactly the pinned packages, laid out as sdkmanager would,
# in ~/.cache/jazz-kotlin, for the Kotlin Android scripts that source it after
# defining `cache` and `fetch` (fetch <url> <file> <sha256>).
# The same SDK packages as jazz-rn (dev/rn-device-acceptance); the sha256s are
# of the zips whose sha1s Google lists in repository2-3.xml.
platform_url=https://dl.google.com/android/repository/platform-36_r02.zip
platform_sha256=37607369a28c5b640b3a7998868d45898ebcb777565a0e85f9acf36f29631d2e
build_tools_url=https://dl.google.com/android/repository/build-tools_r36_linux.zip
build_tools_sha256=5d9ac77fb6ff43d9da518a337b4fcf8f9097113df531d99ccefe80ef7ce8250b

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

sdk="$cache/android-sdk"
install_zip "$platform_url" "$platform_sha256" "$sdk/platforms/android-36"
install_zip "$build_tools_url" "$build_tools_sha256" "$sdk/build-tools/36.0.0"
# ANDROID_SDK_ROOT too: a runner image may set it to its own SDK, and AGP refuses
# two different SDK paths.
export ANDROID_HOME="$sdk" ANDROID_SDK_ROOT="$sdk"
