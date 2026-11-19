#!/usr/bin/env bash
# Point an app at a release's npm tarballs: download into <app>/tgz, verify NPM-SHA256SUMS,
# run the shipped newerVersion cases, set package.json + pnpm overrides, install, check the lockfile.
# usage: rc-deps.sh <app-dir> <version>     (env: T4_RELEASE_BASE, default the GitHub release)
source "$(dirname "$0")/env.sh"
APP=$(cd "$1" && pwd); V=$2
BASE=${T4_RELEASE_BASE:-https://github.com/garden-co/jazz/releases/download/v$V}
DL="$T4_WORK/dl-$V"; rm -rf "$DL"; mkdir -p "$DL"
for n in $T4_PACKAGES; do curl -sSfL -o "$DL/$n-$V.tgz" "$BASE/$n-$V.tgz" || { echo "FAIL download $n"; exit 1; }; done
curl -sSfL -o "$DL/NPM-SHA256SUMS" "$BASE/NPM-SHA256SUMS" || { echo "FAIL download NPM-SHA256SUMS"; exit 1; }
(cd "$DL" && grep -E "^[0-9a-f]{64}  ($(echo $T4_PACKAGES | sed 's/ /|/g'))-$V\.tgz$" NPM-SHA256SUMS | sha256sum -c) || { echo "FAIL checksums"; exit 1; }
echo "PASS checksums"
mkdir -p "$DL/x" && tar -xzf "$DL/jazz-tools-$V.tgz" -C "$DL/x"
node "$(dirname "$0")/check-newer.mjs" "$DL/x/package" && echo "PASS newerVersion" || echo "FAIL newerVersion"
mkdir -p "$APP/tgz" && cp "$DL"/*.tgz "$APP/tgz/"
python3 - "$APP" "$V" <<'PY'
import json, sys, re
app, V = sys.argv[1], sys.argv[2]
p = json.load(open(f"{app}/package.json")); p["dependencies"]["jazz-tools"] = f"file:./tgz/jazz-tools-{V}.tgz"
json.dump(p, open(f"{app}/package.json", "w"), indent=2); open(f"{app}/package.json", "a").write("\n")
w = open(f"{app}/pnpm-workspace.yaml").read()
w = re.sub(r"(?ms)^blockExoticSubdeps:.*?(?=^\S|\Z)", "", w); w = re.sub(r"(?ms)^overrides:\n(  .*\n)*", "", w)
w += f"""blockExoticSubdeps: false
overrides:
  jazz-tools: file:./tgz/jazz-tools-{V}.tgz
  jazz-wasm: file:./tgz/jazz-wasm-{V}.tgz
  jazz-napi: file:./tgz/jazz-napi-{V}.tgz
  '@garden-co/jazz-napi-linux-x64-gnu': file:./tgz/garden-co-jazz-napi-linux-x64-gnu-{V}.tgz
"""
open(f"{app}/pnpm-workspace.yaml", "w").write(w)
PY
(cd "$APP" && pnpm install --no-frozen-lockfile > "$T4_WORK/install.log" 2>&1) || { echo "FAIL pnpm install (see $T4_WORK/install.log)"; exit 1; }
other=$(grep -E "^  '?(@garden-co/jazz-napi-linux-x64-gnu|jazz-(tools|wasm|napi))@" "$APP/pnpm-lock.yaml" | grep -c -v -- "-$V.tgz")
[ "$other" = 0 ] && echo "PASS lockfile: every jazz package on $V" || { echo "FAIL lockfile: $other jazz entries not on $V"; exit 1; }
