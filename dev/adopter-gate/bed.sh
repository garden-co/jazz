#!/usr/bin/env bash
# Upgrade bed, one app: the app (on the previous release, in bed/) moves to the RC tarballs, then
# `generate`, `generate --check` and its unedited smoke. Usage: bed.sh t1|t3|t5|t6
. "$(dirname "$0")/lib.sh"
body() {
  local W=$AG/.work/bed-$1; rm -rf "$W"; mkdir -p "$W"; cp -r "$AG/bed/_common" "$W/"
  case $1 in
    t1|t3|t6) local d p; d=$(cd "$AG/bed" && ls -d "$1"-*); p=$(case $1 in t1) echo 8601;; t3) echo 8603;; t6) echo 8606;; esac)
      cp -r "$AG/bed/$d" "$W/"; ( cd "$W/$d" && use_rc_pnpm
        npx jazz-tools generate > gen.log 2>&1 && echo "PASS $1 generate" || { echo "FAIL $1 generate"; tail -5 gen.log; }
        npx jazz-tools generate --check > gc.log 2>&1 && echo "PASS $1 generate --check" || { echo "FAIL $1 generate --check"; tail -5 gc.log; } )
      ( cd "$W" && ./_common/run-smoke.sh "$d" "$p" | grep -E "^(PASS|FAIL|SMOKE|SERVER)" | sed 's/^SMOKE FAIL/FAIL smoke/; s/^SERVER FAILED/FAIL server start/; s/^SMOKE PASS/PASS '"$1"' smoke/' ) ;;
    t5) cp -r "$AG/bed/t5-expo" "$W/"; ( cd "$W/t5-expo/server" && use_rc_pnpm
        npx jazz-tools generate --check --schema-dir ../jazz > gc.log 2>&1 && echo "PASS t5 generate --check" || { echo "FAIL t5 generate --check"; tail -5 gc.log; } )
      ( cd "$W/t5-expo" && V=$(ag_version) && node -e '
          const fs = require("fs"), [d, v] = process.argv.slice(1), p = JSON.parse(fs.readFileSync("package.json"));
          for (const n of ["jazz-tools", "jazz-wasm"]) if (p.dependencies[n]) p.dependencies[n] = `file:${d}/${n}-${v}.tgz`;
          p.overrides = { "jazz-wasm": `file:${d}/jazz-wasm-${v}.tgz`, "jazz-napi": `file:${d}/jazz-napi-${v}.tgz` };
          fs.writeFileSync("package.json", JSON.stringify(p, null, 2));' "$AG_TGZ" "$V"
        rm -rf node_modules package-lock.json; npm install > install-rc.log 2>&1 || echo "FAIL t5 npm install"
        npx expo export -p web --clear > export.log 2>&1 && echo "PASS t5 expo export" || { echo "FAIL t5 expo export"; tail -5 export.log; } )
      ( cd "$W" && ./_common/run-smoke.sh t5-expo/server 8605 | grep -E "^(PASS|FAIL|SMOKE|SERVER)" | sed 's/^SMOKE FAIL/FAIL smoke/; s/^SERVER FAILED/FAIL server start/; s/^SMOKE PASS/PASS t5 smoke/' ) ;;
    *) echo "FAIL unknown app $1";;
  esac
}
ag_job "bed-$1" body "$1"
