#!/usr/bin/env bash
# Generates jazz-ffi's Kotlin and Swift sources with uniffi-bindgen (via
# dev/gates/jazz-ffi.sh), then compiles each with a small smoke program and
# runs it against libjazz_ffi: a name the generated code can't compile with
# (such as `close` or an error field `message` in Kotlin) fails here.
# Needs a JDK, kotlinc and swiftc (GitHub's ubuntu-24.04 image has them);
# JNA is fetched from Maven Central and checked against its hash.
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

bash "$root/dev/gates/jazz-ffi.sh"
bindings="$root/target/jazz-ffi-bindings"
lib_dir="$root/target/debug"

jna_version=5.17.0
jna_sha256=b3a9408e7c51e08ef0e3bfcc08f443f6ec0f6191ba8cd7c18d53d2b22e5bdbc0
jna="$work/jna-$jna_version.jar"
curl -sSfL -o "$jna" "https://repo1.maven.org/maven2/net/java/dev/jna/jna/$jna_version/jna-$jna_version.jar"
echo "$jna_sha256  $jna" | sha256sum -c -

if ! command -v kotlinc >/dev/null; then
  kotlin_version=2.2.21
  kotlin_sha256=a623871f1cd9c938946948b70ef9170879f0758043885bbd30c32f024e511714
  curl -sSfL -o "$work/kotlinc.zip" \
    "https://github.com/JetBrains/kotlin/releases/download/v$kotlin_version/kotlin-compiler-$kotlin_version.zip"
  echo "$kotlin_sha256  $work/kotlinc.zip" | sha256sum -c -
  unzip -q "$work/kotlinc.zip" -d "$work"
  PATH="$work/kotlinc/bin:$PATH"
fi

kotlinc "$bindings/kotlin/uniffi/jazz_ffi/jazz_ffi.kt" "$root/crates/jazz-ffi/tests/kotlin/Smoke.kt" \
  -cp "$jna" -include-runtime -d "$work/smoke.jar"
java -Djna.library.path="$lib_dir" -cp "$work/smoke.jar:$jna" SmokeKt

swiftc -module-name JazzFfiSmoke \
  -I "$bindings/swift" -Xcc -fmodule-map-file="$bindings/swift/jazz_ffiFFI.modulemap" \
  "$bindings/swift/jazz_ffi.swift" "$root/crates/jazz-ffi/tests/swift/main.swift" \
  -L "$lib_dir" -ljazz_ffi -o "$work/swift-smoke"
LD_LIBRARY_PATH="$lib_dir" "$work/swift-smoke"
