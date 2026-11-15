#!/usr/bin/env bash
# Re-run every app's baseline: install, validate --check, smoke test. Usage: ./run-all.sh [jazz-tools version or tarball dir]
cd "$(dirname "$0")"
for a in t1-migrations:8601 t2-computed:8602 t3-permissions:8603 t6-import-style:8606 t7-enum-cases:8607; do
  d=${a%%:*}; p=${a##*:}
  echo "===== $d"
  (cd $d && pnpm install >/dev/null 2>&1 && npx jazz-tools validate --check >/tmp/vc-$d.log 2>&1; echo "validate --check exit=$?")
  ./_common/run-smoke.sh $d $p | grep -E "^(PASS|FAIL|SMOKE|SERVER)"
done
echo "===== t1 fixture store (.64 data)"; ./t1-migrations/fixture-check.sh | grep -E "^(PASS|FAIL|SMOKE|SERVER)"
echo "===== t5-expo (backend smoke + web export)"
(cd t5-expo/server && pnpm install >/dev/null 2>&1 && npx jazz-tools validate --check --schema-dir ../jazz >/tmp/vc-t5.log 2>&1; echo "validate --check exit=$?")
./_common/run-smoke.sh t5-expo/server 8605 | grep -E "^(PASS|FAIL|SMOKE|SERVER)"
(cd t5-expo && npm install >/dev/null 2>&1 && npx expo export -p web >/tmp/expo-export.log 2>&1; echo "expo export -p web exit=$?")
