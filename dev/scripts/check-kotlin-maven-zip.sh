#!/usr/bin/env bash
# Fails unless jazz-kotlin-maven-<version>.zip is the Kotlin Multiplatform repository a
# Gradle build can resolve tools.jazz:jazz-kotlin:<version> from on the JVM and on Android:
# the root jazz-kotlin with its Gradle module metadata pointing at both variants, the
# jazz-kotlin-jvm jar and the jazz-kotlin-android AAR (each with POM, module and sources jar),
# nothing outside tools/jazz/<artifact>/<version>/, and every sources jar sources only
# (dev/scripts/check-kotlin-sources-jar.sh).
#
# usage: dev/scripts/check-kotlin-maven-zip.sh <zip> <version>
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"

zip="$1"
version="$2"
fail() { echo "$(basename "$zip"): $*" >&2; exit 1; }
entries="$(unzip -Z1 "$zip" | grep -v '/$')"

if stray="$(grep -Ev "^tools/jazz/[a-z-]+/${version//./\\.}/[^/]+\$" <<<"$entries")"; then
  fail "entries outside tools/jazz/<artifact>/$version/:"$'\n'"$stray"
fi
has() { grep -qxF "$1" <<<"$entries" || fail "lacks $1"; }
root="tools/jazz/jazz-kotlin/$version"
has "$root/jazz-kotlin-$version.module"
has "$root/jazz-kotlin-$version.pom"
for artifact in jazz-kotlin-jvm jazz-kotlin-android; do
  dir="tools/jazz/$artifact/$version"
  main=jar
  [ "$artifact" = jazz-kotlin-android ] && main=aar
  for file in "$artifact-$version.$main" "$artifact-$version.pom" "$artifact-$version.module" \
    "$artifact-$version-sources.jar"; do
    has "$dir/$file"
  done
done

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
unzip -q "$zip" -d "$work"
# The root's module metadata must point Gradle at both platform variants.
for artifact in jazz-kotlin-jvm jazz-kotlin-android; do
  grep -q "\"module\": \"$artifact\"" "$work/$root/jazz-kotlin-$version.module" ||
    fail "jazz-kotlin-$version.module doesn't point at $artifact"
done
mapfile -t sources < <(find "$work/tools" -name '*-sources.jar' | LC_ALL=C sort)
[ "${#sources[@]}" -gt 0 ] || fail "has no sources jars"
for jar in "${sources[@]}"; do
  case "$(basename "$jar")" in
    # The root's sources are commonMain only: no generated bindings to require.
    "jazz-kotlin-$version-sources.jar")
      if natives="$(unzip -Z1 "$jar" | grep -iE '\.(so(\.[0-9]+)*|dylib|jnilib|dll|a|lib|o)$')"; then
        fail "$(basename "$jar") holds native libraries:"$'\n'"$natives"
      fi ;;
    *) bash "$here/check-kotlin-sources-jar.sh" "$jar" ;;
  esac
done
echo "$(basename "$zip"): jazz-kotlin, jazz-kotlin-jvm and jazz-kotlin-android $version, sources jars clean"
