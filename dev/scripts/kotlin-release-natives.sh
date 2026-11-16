#!/usr/bin/env bash
# Lays a GitHub release's libjazz_ffi out as JNA loads it from the Kotlin jar
# (<out>/linux-x86-64/libjazz_ffi.so, …), for dev/scripts/publish-kotlin.sh:
# downloads every libjazz_ffi-<platform>.tar.gz "Attach language artifacts"
# attached, checks them against the release's SHA256SUMS, and takes each
# one's shared library.
#
# usage: dev/scripts/kotlin-release-natives.sh <tag> <out>   (GH_TOKEN for gh)
set -euo pipefail

tag="$1"
out="$2"
repo="${GITHUB_REPOSITORY:-garden-co/jazz}"
download="$(mktemp -d)"
trap 'rm -rf "$download"' EXIT

gh release download "$tag" -R "$repo" -D "$download" -p 'libjazz_ffi-*.tar.gz' -p SHA256SUMS
(cd "$download" && sha256sum --check --ignore-missing --strict SHA256SUMS)

# The release's platform names, and JNA's.
declare -A jna=(
  [linux-x86_64]=linux-x86-64
  [linux-aarch64]=linux-aarch64
  [darwin-aarch64]=darwin-aarch64
  [darwin-x86_64]=darwin-x86-64
  [windows-x86_64]=win32-x86-64
)
for platform in "${!jna[@]}"; do
  archive="$download/libjazz_ffi-$platform.tar.gz"
  [ -f "$archive" ] || { echo "$tag has no libjazz_ffi-$platform.tar.gz" >&2; exit 1; }
  grep -q " libjazz_ffi-$platform.tar.gz\$" "$download/SHA256SUMS" ||
    { echo "$tag's SHA256SUMS doesn't list libjazz_ffi-$platform.tar.gz" >&2; exit 1; }
  mkdir -p "$download/$platform" "$out/${jna[$platform]}"
  tar -xzf "$archive" -C "$download/$platform"
  shared=("$download/$platform"/libjazz_ffi.so "$download/$platform"/libjazz_ffi.dylib "$download/$platform"/jazz_ffi.dll)
  found=0
  for lib in "${shared[@]}"; do
    if [ -f "$lib" ]; then
      cp "$lib" "$out/${jna[$platform]}/"
      found=1
    fi
  done
  [ "$found" = 1 ] || { echo "libjazz_ffi-$platform.tar.gz holds no shared library at its root" >&2; exit 1; }
done
ls -lR "$out"
