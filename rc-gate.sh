#!/usr/bin/env bash
# .66 rc gate: for each app, on a fresh copy: install published .65, run the RC's `jazz-tools upgrade`, switch deps to the RC tarballs,
# then `generate --check` and the unedited smoke. Usage: ./rc-gate.sh <rc-version e.g. 2.0.0-alpha.65-rc.1> <work dir>
set -u; V=$1; W=$2; B=https://github.com/garden-co/jazz/releases/download/v$V
HERE=$(cd "$(dirname "$0")" && pwd); T=$W/tgz; mkdir -p $T $W/cli
for n in jazz-tools jazz-wasm jazz-napi garden-co-jazz-napi-linux-x64-gnu; do [ -f $T/$n-$V.tgz ] || curl -sSLf -o $T/$n-$V.tgz $B/$n-$V.tgz || echo "missing tarball $n (may not exist in this release)"; done
OV="blockExoticSubdeps: false\nstrictDepBuilds: false\noverrides:\n  jazz-wasm: file:$T/jazz-wasm-$V.tgz\n  jazz-napi: file:$T/jazz-napi-$V.tgz"
[ -f $T/garden-co-jazz-napi-linux-x64-gnu-$V.tgz ] && OV="$OV\n  \"@garden-co/jazz-napi-linux-x64-gnu\": file:$T/garden-co-jazz-napi-linux-x64-gnu-$V.tgz"
# RC CLI in its own dir
( cd $W/cli && echo '{"name":"rc-cli","private":true,"type":"module","dependencies":{"jazz-tools":"file:'$T'/jazz-tools-'$V'.tgz"}}' > package.json && printf "$OV\n" > pnpm-workspace.yaml && pnpm install >/dev/null 2>&1 )
CLI=${CLI:-$W/cli/node_modules/jazz-tools/bin/jazz-tools.js}; [ -f $CLI ] || CLI=$W/cli/node_modules/jazz-tools/dist/cli.js; echo "CLI=$CLI"
rm -rf $W/bed && git clone -q "$HERE" $W/bed && cd $W/bed
swap() { # switch an app dir's jazz deps to the RC tarballs
  node -e 'const f="package.json",p=JSON.parse(require("fs").readFileSync(f));for(const n of ["jazz-tools","jazz-wasm","jazz-napi"]) if(p.dependencies&&p.dependencies[n]) p.dependencies[n]="file:'$T'/"+n+"-'$V'.tgz";require("fs").writeFileSync(f,JSON.stringify(p,null,2))'
  printf "$OV\n" > pnpm-workspace.yaml; rm -rf node_modules pnpm-lock.yaml; pnpm install > install-rc.log 2>&1 || { echo "RC INSTALL FAILED"; tail -5 install-rc.log; }
  grep -oE "alpha\.6[0-9][^ '\"]*" pnpm-lock.yaml | sort -u | tr '\n' ' '; echo; }
for a in t1-migrations:8601 t2-computed:8602 t3-permissions:8603 t6-import-style:8606 t7-enum-cases:8607; do
  d=${a%%:*}; p=${a##*:}; echo "===== $d"
  ( cd $d && pnpm install >/dev/null 2>&1 && node $CLI upgrade > upgrade.log 2>&1; echo "upgrade exit=$?"; grep -E "^(Typecheck|Refus|Error)" upgrade.log | head -3; git status --short . | grep -v "upgrade.log" | head -40 > ../$d.changes; [ $d = t7-enum-cases ] && exit 0; swap
    npx jazz-tools generate > gen.log 2>&1; echo "generate exit=$?"
    npx jazz-tools generate --check > gc.log 2>&1; echo "generate --check exit=$?"; tail -1 gc.log )
  if [ $d = t7-enum-cases ]; then grep -q 'tickets.kind' $d/upgrade.log && echo "PASS t7 refusal names tickets.kind" || echo "FAIL t7 refusal text"; [ -z "$(cat $d.changes)" ] && echo "PASS t7 untouched" || { echo "FAIL t7 files changed:"; cat $d.changes; }; continue; fi
  ./_common/run-smoke.sh $d $p | grep -E "^(PASS|FAIL|SMOKE|SERVER)"
done
echo "===== t1 fixture store (.65-written data)"; ./t1-migrations/fixture-check.sh | grep -E "^(PASS|FAIL|SMOKE|SERVER)"
echo "===== t5-expo"
( cd t5-expo && npm install >/dev/null 2>&1 && (cd server && pnpm install >/dev/null 2>&1) && node $CLI upgrade > upgrade.log 2>&1; echo "upgrade exit=$?"
  (cd server && swap && npx jazz-tools generate --check --schema-dir ../jazz > gc.log 2>&1; echo "generate --check exit=$?")
  node -e 'const f="package.json",p=JSON.parse(require("fs").readFileSync(f));for(const n of ["jazz-tools","jazz-wasm"]) if(p.dependencies[n]) p.dependencies[n]="file:'$T'/"+n+"-'$V'.tgz";p.overrides={"jazz-wasm":"file:'$T'/jazz-wasm-'$V'.tgz","jazz-napi":"file:'$T'/jazz-napi-'$V'.tgz"};require("fs").writeFileSync(f,JSON.stringify(p,null,2))'
  rm -rf node_modules package-lock.json; npm install > install-rc.log 2>&1; npx expo export -p web --clear > export.log 2>&1; echo "expo export exit=$?" )
./_common/run-smoke.sh t5-expo/server 8605 | grep -E "^(PASS|FAIL|SMOKE|SERVER)"
