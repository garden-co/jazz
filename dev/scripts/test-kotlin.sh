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
# Pinned, and checked against the checksums Adoptium and Gradle publish.
jdk_version=21.0.13+9
jdk_url="https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.13%2B9/OpenJDK21U-jdk_x64_linux_hotspot_21.0.13_9.tar.gz"
jdk_sha256=d7f5d5cf865a2902b70ff549b97586a7d3f234e016f041ab447ea740c7d66d1c
gradle_version=8.14.3
gradle_url="https://services.gradle.org/distributions/gradle-${gradle_version}-bin.zip"
gradle_sha256=bd71102213493060956ec229d946beee57158dbd89d0e62b91bca0fa2c5f3531

# Download $1 to $2 and check it has sha256 $3.
fetch() {
  curl -fsSL -o "$2.part" "$1"
  echo "$3  $2.part" | sha256sum -c --quiet -
  mv "$2.part" "$2"
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

cargo build -p jazz-ffi -p jazz-cli --bin jazz-tools
./target/debug/jazz-tools generate --dir "$example" --check

gradle --no-daemon --console=plain -p packages/jazz-kotlin test
gradle --no-daemon --console=plain -p "$example" test
