# A JDK and Gradle for the Kotlin scripts, sourced by them: those on PATH, or
# pinned ones fetched into ~/.cache/jazz-kotlin and checked against their
# published sha256s.
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
  # Retried: a transient HTTP/2 reset (curl 92) failed a release build.
  curl -fsSL --retry 5 --retry-all-errors --retry-delay 2 -o "$2.part" "$1"
  echo "$3  $2.part" | sha256sum -c --quiet -
  mv "$2.part" "$2"
}

# KOTLIN_TOOLCHAIN=pinned uses the pinned ones even when PATH has others
# (dev/scripts/publish-kotlin.sh: a runner's Gradle 9 breaks AGP 8).
pinned="${KOTLIN_TOOLCHAIN:-}"
if [ "$pinned" = pinned ] || ! command -v java >/dev/null; then
  if [ ! -x "$cache/jdk-$jdk_version/bin/java" ]; then
    mkdir -p "$cache/jdk-$jdk_version"
    fetch "$jdk_url" "$cache/jdk.tar.gz" "$jdk_sha256"
    tar xzf "$cache/jdk.tar.gz" -C "$cache/jdk-$jdk_version" --strip-components=1
    rm "$cache/jdk.tar.gz"
  fi
  export JAVA_HOME="$cache/jdk-$jdk_version"
  export PATH="$JAVA_HOME/bin:$PATH"
fi
if [ "$pinned" = pinned ] || ! command -v gradle >/dev/null; then
  if [ ! -x "$cache/gradle-$gradle_version/bin/gradle" ]; then
    mkdir -p "$cache"
    fetch "$gradle_url" "$cache/gradle.zip" "$gradle_sha256"
    (cd "$cache" && unzip -q -o gradle.zip && rm gradle.zip)
  fi
  export PATH="$cache/gradle-$gradle_version/bin:$PATH"
fi

