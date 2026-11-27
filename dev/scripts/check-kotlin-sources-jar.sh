#!/usr/bin/env bash
# Fails unless each given Kotlin sources jar holds sources only: no native
# library, no bigger than sources can be, and with the bindings uniffi-bindgen
# generated (at the root, or under a Kotlin Multiplatform source set: jvmMain/, androidMain/). `-PnativeLibs` once made the JVM one 54 MB (jazz#6020).
#
# usage: dev/scripts/check-kotlin-sources-jar.sh <sources jar>...
set -euo pipefail

# Today's are ~60 kB.
max_bytes=$((2 * 1024 * 1024))

[ "$#" -gt 0 ] || { echo "usage: $0 <sources jar>..." >&2; exit 2; }
for jar in "$@"; do
  entries="$(unzip -Z1 "$jar")"
  if natives="$(grep -iE '\.(so(\.[0-9]+)*|dylib|jnilib|dll|a|lib|o)$' <<<"$entries")"; then
    printf '%s holds native libraries:\n%s\n' "$jar" "$natives" >&2
    exit 1
  fi
  bytes="$(wc -c <"$jar")"
  if [ "$bytes" -gt "$max_bytes" ]; then
    echo "$jar is $bytes bytes: too big to be sources only (limit $max_bytes)" >&2
    exit 1
  fi
  grep -Eqx '([A-Za-z]+Main/)?uniffi/jazz_ffi/jazz_ffi.kt' <<<"$entries" || {
    echo "$jar lacks the generated bindings, uniffi/jazz_ffi/jazz_ffi.kt" >&2
    exit 1
  }
  echo "$jar holds sources only ($bytes bytes)"
done
