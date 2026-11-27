#!/usr/bin/env bash
# Packs jazz-kotlin's local Maven repository into the GitHub release file
# jazz-kotlin-maven-<version>.zip, reproducibly: entries are tools/jazz/<artifact>/<version>/…
# (the Kotlin Multiplatform root jazz-kotlin with its Gradle module metadata, jazz-kotlin-jvm,
# jazz-kotlin-android), sorted, with one fixed mtime and no extra attributes.
#
# usage: dev/scripts/pack-kotlin-maven-zip.sh <repo> <version> <out.zip> [<more repo>...]
#   <repo>  a local Maven repository root: <repo>/tools/jazz/<artifact>/<version>/
#   <more repo>  optional, laid over it in order (e.g. a macOS run's iOS
#           klibs): their files replace <repo>'s of the same path, so one that
#           holds jazz-kotlin brings its root .module too.
#
# Only <version>'s directories go in, without maven-metadata* and _remote.repositories
# (Gradle reads neither from a local file repository) or .asc files (and their checksums): the tree is signed with
# a throwaway key at build time, and the zip's integrity is its line in SHA256SUMS-languages
# and its build provenance attestation, like the jar and AAR beside it.
set -euo pipefail

repo="$(cd "$1" && pwd)"
version="$2"
out="$3"
shift 3
repos=("$repo")
for more in "$@"; do repos+=("$(cd "$more" && pwd)"); done
mkdir -p "$(dirname "$out")"
out="$(cd "$(dirname "$out")" && pwd)/$(basename "$out")"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
found=0
for r in "${repos[@]}"; do
  for dir in "$r"/tools/jazz/*/"$version"/; do
    [ -d "$dir" ] || continue
    artifact="$(basename "$(dirname "$dir")")"
    mkdir -p "$work/tools/jazz/$artifact/$version"
    cp -R "$dir." "$work/tools/jazz/$artifact/$version/"
    found=$((found + 1))
  done
done
[ "$found" -gt 0 ] || { echo "${repos[*]}: no artifact at $version under tools/jazz" >&2; exit 1; }
find "$work/tools" -type f \( -name '*.asc' -o -name '*.asc.*' -o -name 'maven-metadata*' -o -name _remote.repositories \) -delete

# 1980-01-01, zip's earliest; TZ=UTC since zip stores local time.
find "$work/tools" -exec touch -h -d '1980-01-01T00:00:00Z' {} +
rm -f "$out"
(cd "$work" && find tools -print | LC_ALL=C sort | TZ=UTC zip -X -q -@ "$out")
echo "$(basename "$out"): $(unzip -Z1 "$out" | grep -vc '/$') files"
