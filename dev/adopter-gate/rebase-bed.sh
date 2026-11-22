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
for a in t1-migrations:8601 t3-permissions:8603 t6-import-style:8606; do d=${a%%:*}; p=${a##*:}
  ( cd "$d" && bump && rm -rf node_modules pnpm-lock.yaml && pnpm install > /dev/null 2>&1 && npx jazz-tools generate && npx jazz-tools generate --check ) || { echo "FAIL $d on $V"; exit 1; }
  ./_common/run-smoke.sh "$d" "$p" | grep -E "^(FAIL|SMOKE|SERVER)"; done
FX=t1-migrations/fixture-data-$MINOR; rm -rf "$FX" && cp -r t1-migrations/data "$FX" || { echo "FAIL copy to $FX"; exit 1; }
# Fixtures are committed, so no secrets: the Inspector's token secret is a file of its own (a store makes a new one
# when it's missing), and a generated backend signing key would be a table in jazz.sqlite (jazz#6169).
find "$FX" -name inspect-token-secret -delete
! grep -qa _jazz_host_backend_signing_key "$FX/jazz/jazz.sqlite" || { echo "FAIL $FX holds a backend signing key: don't commit it"; exit 1; }
echo "wrote $FX"
( cd t5-expo/server && bump && rm -rf node_modules pnpm-lock.yaml && pnpm install > /dev/null 2>&1 && npx jazz-tools generate --check --schema-dir ../jazz ) || { echo "FAIL t5 server on $V"; exit 1; }
( cd t5-expo && bump && rm -rf node_modules package-lock.json && npm install > /dev/null 2>&1 && npx expo export -p web --clear > /dev/null ) || { echo "FAIL t5 expo on $V"; exit 1; }
./_common/run-smoke.sh t5-expo/server 8605 | grep -E "^(FAIL|SMOKE|SERVER)"
( cd "$AG/t10" && bump && rm -rf node_modules pnpm-lock.yaml && pnpm install > /dev/null 2>&1 && npx jazz-tools generate )
echo "bed is on $V: review and commit dev/adopter-gate"
