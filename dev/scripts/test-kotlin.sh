#!/usr/bin/env bash
# The Kotlin partition: jazz-kotlin's tests and the StagePlan Kotlin CLI
# end to end against `jazz-tools server`, on the JVM. Fetches a JDK and
# Gradle into ~/.cache/jazz-kotlin when none is on PATH.
set -euo pipefail
cd "$(dirname "$0")/../.."

canonical=examples/stage-plan/apps/react-localfirst
example=examples/stage-plan/apps/kotlin-cli
for file in jazz.app.json jazz.permissions.json; do
  if ! cmp -s "$canonical/$file" "$example/$file"; then
    echo "$example/$file differs from $canonical/$file: copy it over (never edit StagePlan's files for Kotlin)" >&2
    exit 1
  fi
done

cache="${HOME}/.cache/jazz-kotlin"
gradle_version=8.14.3
if ! command -v java >/dev/null; then
  if [ ! -x "$cache/jdk/bin/java" ]; then
    mkdir -p "$cache/jdk"
    curl -fsSL "https://api.adoptium.net/v3/binary/latest/21/ga/linux/x64/jdk/hotspot/normal/eclipse" |
      tar xz -C "$cache/jdk" --strip-components=1
  fi
  export JAVA_HOME="$cache/jdk"
  export PATH="$JAVA_HOME/bin:$PATH"
fi
if ! command -v gradle >/dev/null; then
  if [ ! -x "$cache/gradle-$gradle_version/bin/gradle" ]; then
    mkdir -p "$cache"
    curl -fsSL -o "$cache/gradle.zip" "https://services.gradle.org/distributions/gradle-$gradle_version-bin.zip"
    (cd "$cache" && unzip -q -o gradle.zip && rm gradle.zip)
  fi
  export PATH="$cache/gradle-$gradle_version/bin:$PATH"
fi

cargo build -p jazz-ffi -p jazz-cli --bin jazz-tools
./target/debug/jazz-tools generate --dir "$example" --check

gradle --no-daemon --console=plain -p packages/jazz-kotlin test
gradle --no-daemon --console=plain -p "$example" test
