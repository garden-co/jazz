#!/usr/bin/env bash
# Publishes jazz-kotlin (the JVM jar) and jazz-kotlin-android (the AAR) to
# Maven Central as tools.jazz:<artifact>:<version>, at jazz-tools' version.
#
# usage: dev/scripts/publish-kotlin.sh <natives> <mode>
#   <natives>  JNA's layout, as dev/scripts/build-kotlin-dist.sh takes it
#              (linux-x86-64/libjazz_ffi.so, darwin-aarch64/libjazz_ffi.dylib,
#              win32-x86-64/jazz_ffi.dll, …): the libraries the jar bundles.
#   <mode>     local: build, sign and publish both to the local Maven
#                     repository (~/.m2), and check what's there. Nothing
#                     leaves the machine.
#              central: the same build, in the same Gradle run, also
#                     uploaded to the Central Portal, where both deployments
#                     wait, validated, for Publish (never released from
#                     here): what's checked is what was uploaded, and a
#                     failed check leaves them to drop.
#
# Signing, in both modes: ORG_GRADLE_PROJECT_signingInMemoryKey (an
# ASCII-armored private key) and ORG_GRADLE_PROJECT_signingInMemoryKeyPassword.
# central also needs ORG_GRADLE_PROJECT_mavenCentralUsername and
# ORG_GRADLE_PROJECT_mavenCentralPassword (a Central Portal user token).
#
# KOTLIN_JVM_BINDINGS, KOTLIN_ANDROID_JNI_LIBS and KOTLIN_ANDROID_BINDINGS take
# prebuilt Rust, as in dev/scripts/build-kotlin-dist.sh.
set -euo pipefail
cd "$(dirname "$0")/../.."

natives="$(cd "$1" && pwd)"
mode="$2"
case "$mode" in local | central) ;; *) echo "mode must be local or central, got '$mode'" >&2; exit 1 ;; esac
: "${ORG_GRADLE_PROJECT_signingInMemoryKey:?a signing key is needed, even locally}"
if [ "$mode" = central ]; then
  : "${ORG_GRADLE_PROJECT_mavenCentralUsername:?}" "${ORG_GRADLE_PROJECT_mavenCentralPassword:?}"
fi

version="$(sed -n 's/^  "version": "\(.*\)",$/\1/p' packages/jazz-tools/package.json)"
[ -n "$version" ] || { echo "no version in packages/jazz-tools/package.json" >&2; exit 1; }
platforms=()
for dir in "$natives"/*/; do
  ls "$dir" | grep -Eq '^(lib)?jazz_ffi\.(so|dylib|dll)$' || { echo "$dir holds no libjazz_ffi" >&2; exit 1; }
  platforms+=("$(basename "$dir")")
done
[ "${#platforms[@]}" -gt 0 ] || { echo "$natives holds no platform directories" >&2; exit 1; }
echo "jazz-kotlin $version with ${platforms[*]} ($mode)"

# The pinned JDK and Gradle (8.14.3: AGP 8.13 and maven-publish 0.35 need
# Gradle 8), whatever PATH has.
export KOTLIN_TOOLCHAIN=pinned
source dev/scripts/kotlin-toolchain.sh
gradle --version | grep -q '^Gradle 8\.14\.3$' || { gradle --version >&2; echo "not the pinned Gradle 8.14.3" >&2; exit 1; }

m2="$HOME/.m2/repository/tools/jazz"
rm -rf "$m2/jazz-kotlin/$version" "$m2/jazz-kotlin-android/$version"

jvm=(gradle --no-daemon --console=plain -p packages/jazz-kotlin -PcargoProfile=release -PnativeLibs="$natives")
[ -n "${KOTLIN_JVM_BINDINGS:-}" ] && jvm+=(-PuniffiBindings="$(cd "$KOTLIN_JVM_BINDINGS" && pwd)")
tasks=(publishToMavenLocal)
[ "$mode" = central ] && tasks+=(publishToMavenCentral)
"${jvm[@]}" clean "${tasks[@]}"
KOTLIN_ANDROID_TASKS="${tasks[*]}" CARGO_PROFILE=release-mobile bash dev/scripts/test-kotlin-android.sh

# What Central will get: every file, signed by the key; the jar with every
# platform's library, the AAR with every ABI's; POMs Central accepts.
gnupg="$(mktemp -d)"
trap 'rm -rf "$gnupg"' EXIT
printf '%s\n' "$ORG_GRADLE_PROJECT_signingInMemoryKey" | gpg --homedir "$gnupg" --batch --quiet --import 2>/dev/null ||
  printf '%s\n' "$ORG_GRADLE_PROJECT_signingInMemoryKey" | gpg --homedir "$gnupg" --batch --quiet --pinentry-mode loopback \
    --passphrase "${ORG_GRADLE_PROJECT_signingInMemoryKeyPassword:-}" --import
fail() { echo "$*" >&2; exit 1; }
for artifact in jazz-kotlin jazz-kotlin-android; do
  dir="$m2/$artifact/$version"
  main=jar
  [ "$artifact" = jazz-kotlin-android ] && main=aar
  for file in "$artifact-$version.$main" "$artifact-$version-sources.jar" "$artifact-$version-javadoc.jar" \
    "$artifact-$version.pom" "$artifact-$version.module"; do
    [ -s "$dir/$file" ] || fail "$artifact lacks $file"
    gpg --homedir "$gnupg" --batch --quiet --verify "$dir/$file.asc" "$dir/$file" 2>/dev/null ||
      fail "$file's signature ($file.asc) doesn't verify"
  done
  pom="$dir/$artifact-$version.pom"
  for field in "<name>$artifact</name>" "<description>" "<url>https://jazz.tools</url>" "<name>MIT License</name>" \
    "<developer>" "<url>https://github.com/garden-co/jazz</url>" "<connection>scm:git:"; do
    grep -qF "$field" "$pom" || fail "$artifact's POM lacks $field"
  done
  # JNA is `implementation`: a runtime dependency, which consumers still get.
  tr -d ' \n' <"$pom" | grep -q '<artifactId>jna</artifactId><version>5.17.0</version>\(<type>aar</type>\)\?<scope>runtime</scope>' ||
    fail "$artifact's POM lacks JNA 5.17.0 at runtime scope"
  bash dev/scripts/check-kotlin-sources-jar.sh "$dir/$artifact-$version-sources.jar"
done
listing="$(unzip -l "$m2/jazz-kotlin/$version/jazz-kotlin-$version.jar")"
for platform in "${platforms[@]}"; do
  # The library itself: a directory entry alone would match "$platform/".
  grep -Eq " $platform/(lib)?jazz_ffi\.(so|dylib|dll)\$" <<<"$listing" ||
    fail "jazz-kotlin-$version.jar lacks $platform's libjazz_ffi"
done
grep -q "uniffi/jazz_ffi/" <<<"$listing" || fail "jazz-kotlin-$version.jar lacks the UniFFI bindings"
listing="$(unzip -l "$m2/jazz-kotlin-android/$version/jazz-kotlin-android-$version.aar")"
for abi in arm64-v8a armeabi-v7a x86_64; do
  grep -q "jni/$abi/libjazz_ffi.so" <<<"$listing" || fail "jazz-kotlin-android-$version.aar lacks jni/$abi/libjazz_ffi.so"
done
grep -q '<type>aar</type>' "$m2/jazz-kotlin-android/$version/jazz-kotlin-android-$version.pom" ||
  fail "jazz-kotlin-android's POM must take JNA's AAR"
echo "Checked tools.jazz:jazz-kotlin:$version and tools.jazz:jazz-kotlin-android:$version in $m2"

if [ "$mode" = central ]; then
  echo "Uploaded both to the Central Portal: check the two deployments there and press Publish."
fi
