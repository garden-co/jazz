#!/usr/bin/env bash
# Backs the fetch-release-artifacts action; also usable locally:
#   FETCH_TAG=v… FETCH_NAMES=$'a*.tgz\nSHA256SUMS' FETCH_DEST=dir [FETCH_REPO=garden-co/jazz] fetch.sh
set -euo pipefail
repo=${FETCH_REPO:-garden-co/jazz}; dest=${FETCH_DEST:?}; mkdir -p "$dest"
if [ -n "${FETCH_SHA:-}" ] && [ -z "${FETCH_TAG:-}" ]; then
  echo "::error::fetch-release-artifacts: 'sha' (release-artifacts.yml runs) is not implemented yet; pass 'tag'"; exit 1
fi
tag=${FETCH_TAG:?tag or sha required}
assets=$(gh release view "$tag" -R "$repo" --json assets -q '.assets[].name')
missing=0; want=()
while IFS= read -r glob; do
  glob=$(echo "$glob" | xargs); [ -z "$glob" ] && continue
  hits=$(printf '%s\n' "$assets" | while IFS= read -r a; do if [[ "$a" == $glob ]]; then echo "$a"; fi; done || true)
  if [ -z "$hits" ]; then echo "::error::no release asset of $tag matches '$glob'"; missing=1; else while IFS= read -r h; do want+=("$h"); done <<< "$hits"; fi
done <<< "$FETCH_NAMES"
[ "$missing" = 0 ] || exit 1
# Always fetch the checksum files that exist, to verify what we download.
for s in NPM-SHA256SUMS SHA256SUMS SHA256SUMS-languages; do if printf '%s\n' "$assets" | grep -qx "$s"; then want+=("$s"); fi; done
args=(); for w in $(printf '%s\n' "${want[@]}" | sort -u); do args+=(-p "$w"); done
# Retried: one flaky CDN answer must not fail a release gate.
for i in 1 2 3 4 5; do gh release download "$tag" -R "$repo" --clobber -D "$dest" "${args[@]}" && break; [ "$i" = 5 ] && exit 1; sleep $((i * 5)); done
cd "$dest"; unverified=0
for f in $(printf '%s\n' "${want[@]}" | sort -u); do
  case "$f" in NPM-SHA256SUMS|SHA256SUMS|SHA256SUMS-languages) continue;; esac
  line=$( { cat NPM-SHA256SUMS SHA256SUMS SHA256SUMS-languages 2>/dev/null || true; } | awk -v f="$f" '$2==f || $2=="*"f' | head -1)
  if [ -z "$line" ]; then echo "::warning::$f is not listed in any checksum file"; unverified=1; continue; fi
  echo "$line" | sha256sum -c --quiet && echo "verified $f"
done
for b in jazz-tools-linux-x64 jazz-tools-linux-arm64; do [ -f "$b" ] && chmod +x "$b"; done
exit 0
