#!/usr/bin/env bash
# Lays a GitHub release's libjazz_ffi out as JNA loads it from the Kotlin jar
# (<out>/linux-x86-64/libjazz_ffi.so, …), for dev/scripts/publish-kotlin.sh.
# From the release "Attach language artifacts" filled, which must be
# published (a draft's assets can still change), it downloads every
# libjazz_ffi-<platform>.tar.gz and checks that:
# - each is listed in SHA256SUMS-languages and matches it;
# - each (and the sums) carries a build-provenance attestation by
#   attach-language-artifacts.yml in this repository (releases aren't
#   immutable: an asset could be replaced after publishing);
# - libjazz_ffi-source.txt, the commit they were built from, is <sha>, the
#   commit being published.
# Then it takes each one's shared library.
#
# usage: dev/scripts/kotlin-release-natives.sh <tag> <sha> <out>   (GH_TOKEN for gh)
set -euo pipefail

tag="$1"
sha="$2"
out="$3"
repo="${GITHUB_REPOSITORY:-garden-co/jazz}"
download="$(mktemp -d)"
trap 'rm -rf "$download"' EXIT
fail() { echo "$*" >&2; exit 1; }

draft="$(gh release view "$tag" -R "$repo" --json isDraft --jq .isDraft)"
[ "$draft" = false ] || fail "$tag is still a draft: publish the GitHub release first."

gh release download "$tag" -R "$repo" -D "$download" \
  -p 'libjazz_ffi-*.tar.gz' -p SHA256SUMS-languages -p libjazz_ffi-source.txt
for file in SHA256SUMS-languages libjazz_ffi-source.txt; do
  [ -f "$download/$file" ] || fail "$tag has no $file"
done
(cd "$download" && sha256sum --check --ignore-missing --strict SHA256SUMS-languages)
grep -q ' libjazz_ffi-source.txt$' "$download/SHA256SUMS-languages" ||
  fail "$tag's SHA256SUMS-languages doesn't list libjazz_ffi-source.txt"
built="$(tr -d '[:space:]' <"$download/libjazz_ffi-source.txt")"
[ "$built" = "$sha" ] || fail "$tag's libjazz_ffi was built from $built, not $sha"

attested() {
  gh attestation verify "$1" -R "$repo" \
    --signer-workflow "$repo/.github/workflows/attach-language-artifacts.yml" >/dev/null ||
    fail "$(basename "$1") has no attestation from attach-language-artifacts.yml"
}
attested "$download/SHA256SUMS-languages"
attested "$download/libjazz_ffi-source.txt"

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
  [ -f "$archive" ] || fail "$tag has no libjazz_ffi-$platform.tar.gz"
  grep -q " libjazz_ffi-$platform.tar.gz\$" "$download/SHA256SUMS-languages" ||
    fail "$tag's SHA256SUMS-languages doesn't list libjazz_ffi-$platform.tar.gz"
  attested "$archive"
  mkdir -p "$download/$platform" "$out/${jna[$platform]}"
  tar -xzf "$archive" -C "$download/$platform"
  found=0
  for lib in "$download/$platform"/libjazz_ffi.so "$download/$platform"/libjazz_ffi.dylib "$download/$platform"/jazz_ffi.dll; do
    if [ -f "$lib" ]; then
      cp "$lib" "$out/${jna[$platform]}/"
      found=1
    fi
  done
  [ "$found" = 1 ] || fail "libjazz_ffi-$platform.tar.gz holds no shared library at its root"
done
ls -lR "$out"
