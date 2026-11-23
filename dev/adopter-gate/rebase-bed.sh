#!/usr/bin/env bash
# After a release ships, move the bed to it so the next RC is checked as an upgrade from it.
# Usage: rebase-bed.sh <published version, e.g. 2.0.0-alpha.68>
# For each app: deps -> that version (plus `npx jazz-tools@<v> upgrade` first if its upgrade notes need a codemod; do that by
# hand and commit), `generate`, `generate --check`, smoke. t1's smoke data becomes fixture-data-<minor> (older stores stay).
# Then commit the changed files. t4's app lives in t4/ and is rebased by its own README.
. "$(dirname "$0")/lib.sh"
V=$1; case $V in *-alpha.*) MINOR=${V##*alpha.};; *-beta.*) MINOR=beta-${V##*beta.};; *) MINOR=$V;; esac; cd "$AG/bed"
bump() { node -e '
  const fs = require("fs"), v = process.argv[1], p = JSON.parse(fs.readFileSync("package.json"));
  for (const n of ["jazz-tools", "jazz-wasm", "jazz-napi"]) if (p.dependencies?.[n]) p.dependencies[n] = v;
  fs.writeFileSync("package.json", JSON.stringify(p, null, 2) + "\n");' "$V"
  [ ! -f pnpm-workspace.yaml ] || sed -i -E "s/2\.0\.0-(alpha|beta)\.[0-9]+(-rc\.[0-9]+)?/$V/g" pnpm-workspace.yaml; }
# A smoke passes only if it exits 0 and says so (a pipe to grep would lose its exit status).
smoke() { local out rc; out=$(./_common/run-smoke.sh "$1" "$2"); rc=$?
  echo "$out" | grep -E "^(FAIL|SMOKE|SERVER)"
  [ $rc = 0 ] && echo "$out" | grep -q "^SMOKE PASS" || { echo "FAIL $1 smoke on $V (exit $rc)"; exit 1; }; }
for a in t1-migrations:8601 t3-permissions:8603 t6-import-style:8606; do d=${a%%:*}; p=${a##*:}
  ( cd "$d" && bump && rm -rf node_modules pnpm-lock.yaml && pnpm install > /dev/null 2>&1 && npx jazz-tools generate && npx jazz-tools generate --check ) || { echo "FAIL $d on $V"; exit 1; }
  smoke "$d" "$p"; done
FX=t1-migrations/fixture-data-$MINOR; SRC=t1-migrations/data
# The copy is checked in .work/ (ignored) and only then becomes $FX: a refused copy is never where `git add` finds
# it, and never replaces a fixture that's already committed.
refuse() { echo "FAIL $FX not written: $*"; exit 1; }
# run-smoke.sh only signals the server. Copy once it has let go of the store: port closed, WAL checkpointed and gone.
released() { ! (exec 3<>/dev/tcp/127.0.0.1/8601) 2>/dev/null && [ ! -e "$SRC/jazz/jazz.sqlite-wal" ]; }
for i in $(seq 30); do released && break; sleep 0.5; done
released || refuse "t1's server still holds the store, or didn't checkpoint it"
STAGE=$(mktemp -d "$AG/.work/fixture.XXXXXX") && cp -r "$SRC" "$STAGE/store" || refuse "copy failed"
# Fixtures are committed, so no secrets: the Inspector's token secret is a file of its own (a store makes a new one
# when it's missing), and a generated backend signing key would be a table in the store (jazz#6169). Fail closed:
# search every file, and only grep's "no match" (1) passes, not a read error (2).
find "$STAGE/store" -name inspect-token-secret -delete || refuse "can't drop inspect-token-secret"
[ -f "$STAGE/store/jazz/jazz.sqlite" ] || refuse "no jazz.sqlite"
grep -rqa _jazz_host_backend_signing_key "$STAGE/store"; [ $? = 1 ] || refuse "holds a backend signing key or can't be read"
rm -rf "$FX" && mv "$STAGE/store" "$FX" || refuse "can't move the checked copy into place"
echo "wrote $FX"
( cd t5-expo/server && bump && rm -rf node_modules pnpm-lock.yaml && pnpm install > /dev/null 2>&1 && npx jazz-tools generate --check --schema-dir ../jazz ) || { echo "FAIL t5 server on $V"; exit 1; }
( cd t5-expo && bump && rm -rf node_modules package-lock.json && npm install > /dev/null 2>&1 && npx expo export -p web --clear > /dev/null ) || { echo "FAIL t5 expo on $V"; exit 1; }
smoke t5-expo/server 8605
( cd "$AG/t10" && bump && rm -rf node_modules pnpm-lock.yaml && pnpm install > /dev/null 2>&1 && npx jazz-tools generate ) || { echo "FAIL t10 on $V"; exit 1; }
echo "bed is on $V: review and commit dev/adopter-gate"
