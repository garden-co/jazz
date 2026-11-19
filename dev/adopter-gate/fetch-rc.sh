#!/usr/bin/env bash
# Download an RC's gate inputs into $AG_TGZ (default dev/adopter-gate/.rc), verified, via the
# fetch-release-artifacts action's script (the same code CI runs).
# Usage: fetch-rc.sh <rc tag, e.g. v2.0.0-alpha.68-rc.1> [attach-language-artifacts run id]
#        fetch-rc.sh --run <release-artifacts run id> [--npm-run <RC run id holding rc-npm>]
# libjazz_ffi and the Kotlin jar come from the release if attached, else from that dry-run's workflow artifacts.
. "$(dirname "$0")/lib.sh"; set -e
# AG_NEED: what this job uses, comma-separated: npm (rc tarballs), server (jazz-tools-linux-x64), languages
# (libjazz_ffi + Kotlin jar). Default all. Each CI job fetches only its own, so no job waits on a shared download.
AG_NEED=${AG_NEED:-npm,server,languages}; need() { case ",$AG_NEED," in *",$1,"*) return 0;; esac; return 1; }
R=${GITHUB_REPOSITORY:-garden-co/jazz}
if [ "$1" = --run ]; then   # no release: workflow artifacts of a release-artifacts run (server, languages) and an RC run (rc-npm)
  RUNID=$2; NPMRUN=$RUNID; [ "${3:-}" = --npm-run ] && [ -n "${4:-}" ] && NPMRUN=$4
  mkdir -p "$AG_TGZ"; cd "$AG_TGZ"
  get() { rm -rf "_$2"; gh run download "$1" -R "$R" -n "$2" -D "_$2" >/dev/null 2>&1 && cp -r "_$2"/. . && rm -rf "_$2"; }
  get "$NPMRUN" rc-npm || { echo "missing artifact rc-npm in run $NPMRUN"; exit 1; }
  # Server binary: release-server (with SHA256SUMS) at ~+9, else the bare jazz-tools-linux-x64 at ~+3.5 (no sums: record ours).
  if need server && ! get "$RUNID" release-server; then get "$RUNID" jazz-tools-linux-x64 || { echo "missing release-server and jazz-tools-linux-x64 in run $RUNID"; exit 1; }
    sha256sum jazz-tools-linux-x64 | tee -a SHA256SUMS; echo "note: jazz-tools-linux-x64 has no published sum yet; recorded above"; fi
  # Languages: release-languages (at the end) or language-artifacts (~+7.5): the same files, with SHA256SUMS-languages.
  # If neither is there yet, wait up to AG_LANG_WAIT_S (120 s) for language-artifacts, then fall back to the early
  # jazz-ffi-linux-x86_64 (~+4.5, unpacked: repacked like the attach step, sum recorded) and jazz-kotlin-dist (~+7).
  langs=; end=$(( $(date +%s) + ${AG_LANG_WAIT_S:-120} ))
  while need languages; do
    get "$RUNID" release-languages && { langs=release-languages; break; }
    get "$RUNID" language-artifacts && { langs=language-artifacts; break; }
    [ "$(date +%s)" -ge $end ] && break; sleep 15
  done
  if need languages && [ -z "$langs" ]; then
    if gh run download "$RUNID" -R "$R" -n jazz-ffi-linux-x86_64 -D _ffi >/dev/null 2>&1 && [ -d _ffi/libjazz_ffi-linux-x86_64 ]; then
      tar czf libjazz_ffi-linux-x86_64.tar.gz -C _ffi/libjazz_ffi-linux-x86_64 . && rm -rf _ffi
      sha256sum libjazz_ffi-linux-x86_64.tar.gz | tee -a SHA256SUMS-languages.local; langs=jazz-ffi-linux-x86_64
      echo "note: language artifacts not ready; using jazz-ffi-linux-x86_64 (no published sum yet; recorded above)"
    fi
    get "$RUNID" jazz-kotlin-dist && echo "note: Kotlin jar from jazz-kotlin-dist" || echo "note: no Kotlin jar yet (t9 kotlin builds from source)"
  fi
  need languages && { echo "languages from: ${langs:-none}"; [ -n "$langs" ] || echo "note: no language artifacts in run $RUNID (t9 go/swift will fail)"; }
  V=$(ls jazz-tools-*.tgz | sed -E 's/^jazz-tools-(.*)\.tgz$/\1/' | head -1); echo "$V" > VERSION
  sha256sum -c NPM-SHA256SUMS --ignore-missing
  mkdir -p /tmp/jazz-rc-npm && cp ./*.tgz /tmp/jazz-rc-npm/   # dry-run tarballs link each other by file:/tmp/jazz-rc-npm/<tgz>
  if need server; then grep " jazz-tools-linux-x64$" SHA256SUMS | sha256sum -c; chmod +x jazz-tools-linux-x64; fi
  if [ -f SHA256SUMS-languages ]; then sha256sum -c SHA256SUMS-languages --ignore-missing; fi
  ls -la; exit 0
fi
TAG=$1; RUN=${2:-}; V=${TAG#v}
FETCH=$(cd "$AG/../.." && pwd)/.github/actions/fetch-release-artifacts/fetch.sh
mkdir -p "$AG_TGZ"
names=; need npm && names="jazz-tools-$V.tgz
jazz-wasm-$V.tgz
jazz-napi-$V.tgz
garden-co-jazz-napi-linux-x64-gnu-$V.tgz"
need server && names="$names
jazz-tools-linux-x64"
[ -z "$names" ] || FETCH_REPO=$R FETCH_TAG=$TAG FETCH_DEST=$AG_TGZ FETCH_NAMES="$names" bash "$FETCH"
echo "$V" > "$AG_TGZ/VERSION"; cd "$AG_TGZ"
if ! need languages; then :
elif gh release view "$TAG" -R "$R" --json assets -q '.assets[].name' | grep -qx "libjazz_ffi-linux-x86_64.tar.gz"; then
  FETCH_REPO=$R FETCH_TAG=$TAG FETCH_DEST=$AG_TGZ FETCH_NAMES="libjazz_ffi-linux-x86_64.tar.gz
jazz-kotlin-*.jar" bash "$FETCH"
elif [ -n "$RUN" ]; then
  rm -rf ffi && gh run download "$RUN" -R "$R" -n jazz-ffi-linux-x86_64 -D ffi
  tar czf libjazz_ffi-linux-x86_64.tar.gz -C ffi/libjazz_ffi-linux-x86_64 . && rm -rf ffi
  gh run download "$RUN" -R "$R" -n jazz-kotlin-dist -D kt 2>/dev/null && mv kt/jazz-kotlin-*.jar . 2>/dev/null || true; rm -rf kt
else echo "note: no libjazz_ffi for $TAG (no release asset, no run id): t9 go/swift/kotlin will build from source"; fi
ls -la "$AG_TGZ"
