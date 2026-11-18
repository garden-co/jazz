#!/usr/bin/env bash
# .68 rc gate on the .67 bed (eval/testbed-a68): each app on a fresh copy switches deps to the rc tarballs,
# then `generate`, `generate --check`, unedited smoke. t1's fixture stores (.65- and .67-written) must open, and their
# identity must move into jazz.sqlite (_jazz_host_identity, jazz#5570) matching the old files. Then t10 (revokeUser across
# instances on one core, jazz#5634; Db lifecycle, jazz#5592) with the rc's core binary.
# Usage: ./rc-gate68.sh <version e.g. 2.0.0-alpha.68-rc.1> <work dir>
set -u; V=$1; W=$2; B=https://github.com/garden-co/jazz/releases/download/v$V; HERE=$(cd "$(dirname "$0")" && pwd); T=$W/tgz; mkdir -p $T
( cd $T && for n in jazz-tools jazz-wasm jazz-napi garden-co-jazz-napi-linux-x64-gnu; do [ -f $n-$V.tgz ] || curl -sSLf -o $n-$V.tgz $B/$n-$V.tgz; done
  [ -f NPM-SHA256SUMS ] || curl -sSLf -o NPM-SHA256SUMS $B/NPM-SHA256SUMS; sha256sum -c NPM-SHA256SUMS --ignore-missing
  [ -f jazz-tools-linux-x64 ] || curl -sSLf -o jazz-tools-linux-x64 $B/jazz-tools-linux-x64; curl -sSLf $B/SHA256SUMS | grep " jazz-tools-linux-x64$" | sha256sum -c; chmod +x jazz-tools-linux-x64 )
OV="blockExoticSubdeps: false\nstrictDepBuilds: false\noverrides:\n  jazz-wasm: file:$T/jazz-wasm-$V.tgz\n  jazz-napi: file:$T/jazz-napi-$V.tgz\n  \"@garden-co/jazz-napi-linux-x64-gnu\": file:$T/garden-co-jazz-napi-linux-x64-gnu-$V.tgz"
rm -rf $W/bed && git clone -q -b eval/testbed-a68 "$HERE" $W/bed && cd $W/bed
swap() { node -e 'const f="package.json",p=JSON.parse(require("fs").readFileSync(f));for(const n of ["jazz-tools","jazz-wasm","jazz-napi"]) if(p.dependencies&&p.dependencies[n]) p.dependencies[n]="file:'$T'/"+n+"-'$V'.tgz";require("fs").writeFileSync(f,JSON.stringify(p,null,2))'
  printf "$OV\n" > pnpm-workspace.yaml; rm -rf node_modules pnpm-lock.yaml; pnpm install > install-rc.log 2>&1 || { echo "RC INSTALL FAILED"; tail -5 install-rc.log; }
  echo "lockfile: $(grep -oE "(jazz-tools|jazz-wasm|jazz-napi[a-z0-9-]*)@[^:'( ]*" pnpm-lock.yaml | grep -v "$V" | grep -cE "@2\.0") non-rc jazz entries"; }
for a in t1-migrations:8601 t3-permissions:8603 t6-import-style:8606; do d=${a%%:*}; p=${a##*:}; echo "===== $d"
  ( cd $d && swap; npx jazz-tools generate > gen.log 2>&1; echo "generate exit=$?"; npx jazz-tools generate --check > gc.log 2>&1; echo "generate --check exit=$?"; tail -1 gc.log )
  ./_common/run-smoke.sh $d $p | grep -E "^(PASS|FAIL|SMOKE|SERVER)"; done
for fx in fixture-data fixture-data-67; do echo "===== t1 $fx opens on rc"
  ( cd t1-migrations && FX=$fx ./fixture-check.sh | grep -E "^(PASS|FAIL|SMOKE|SERVER)"
    db=/tmp/t1fx/jazz/jazz.sqlite; sqlite3 $db "select cast(k as text), length(v) from _jazz_host_identity" 2>&1 | tr '\n' ' '; echo
    sid=$(cat $fx/jazz/store-id); got=$(sqlite3 $db "select cast(v as text) from _jazz_host_identity where cast(k as text)='store_id'" 2>/dev/null | tr -d '\n')
    [ -n "$got" ] && echo "$got" | grep -q "$sid" && echo "PASS store_id moved into jazz.sqlite ($sid)" || echo "FAIL store_id in jazz.sqlite: '$got' vs file '$sid'"
    [ "$(cat /tmp/t1fx/jazz/store-id)" = "$sid" ] && echo "PASS store-id mirror file unchanged" || echo "FAIL store-id mirror changed" ) ; done
echo "===== t5-expo"
( cd t5-expo && (cd server && swap && npx jazz-tools generate --check --schema-dir ../jazz > gc.log 2>&1; echo "server generate --check exit=$?")
  node -e 'const f="package.json",p=JSON.parse(require("fs").readFileSync(f));for(const n of ["jazz-tools","jazz-wasm"]) if(p.dependencies[n]) p.dependencies[n]="file:'$T'/"+n+"-'$V'.tgz";p.overrides={"jazz-wasm":"file:'$T'/jazz-wasm-'$V'.tgz","jazz-napi":"file:'$T'/jazz-napi-'$V'.tgz"};require("fs").writeFileSync(f,JSON.stringify(p,null,2))'
  rm -rf node_modules package-lock.json; npm install > install-rc.log 2>&1; npx expo export -p web --clear > export.log 2>&1; echo "expo export exit=$?" )
./_common/run-smoke.sh t5-expo/server 8605 | grep -E "^(PASS|FAIL|SMOKE|SERVER)"
echo "===== t10 revokeUser across instances + Db lifecycle (rc core binary)"
( cd t10-revoke-instances && swap >/dev/null && npx jazz-tools generate > gen.log 2>&1 && ./run.sh $T/jazz-tools-linux-x64 2>&1 | grep -E "^(PASS|FAIL|REVOKE|LIFECYCLE|==|INSTANCES)" )
echo GATE68_END
