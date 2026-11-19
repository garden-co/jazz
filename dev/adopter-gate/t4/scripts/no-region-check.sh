#!/usr/bin/env bash
# With vercel() lacking `region`: `next build` must fail, and `next dev` must answer /jazz with the
# missing-region error. Prints the exact texts. usage: no-region-check.sh <app-dir> [port]
source "$(dirname "$0")/env.sh"
APP=$(cd "$1" && pwd); PORT=${2:-8303}; cd "$APP"; rm -rf .next .jazz
BETTER_AUTH_SECRET=t4-local-build-only BETTER_AUTH_URL=http://localhost pnpm build > "$T4_WORK/build-noregion.log" 2>&1
rc=$?; echo "next build (no region) exit code: $rc"; grep -E -A1 "error TS|Error: vercel\(\)" "$T4_WORK/build-noregion.log" | head -6
[ $rc -ne 0 ] && echo "PASS next build refuses" || echo "FAIL next build succeeded without region"
rm -rf .next .jazz
BETTER_AUTH_SECRET=t4-local-dev-only BETTER_AUTH_URL=http://localhost:$PORT npx next dev -p "$PORT" > "$T4_WORK/dev-noregion.log" 2>&1 &
for _ in $(seq 1 60); do curl -s -o /dev/null "localhost:$PORT" && break; sleep 1; done
curl -s -o /dev/null -X POST "localhost:$PORT/jazz/apps/x/accounts/backend-user"; sleep 2
pid=$(ss -ltnp 2>/dev/null | grep ":$PORT " | grep -o "pid=[0-9]*" | head -1 | cut -d= -f2); [ -n "$pid" ] && kill "$pid"
grep -A3 "Error: vercel()" "$T4_WORK/dev-noregion.log" | head -4
if grep -q 'vercel({ namespace, region: "iad1" })' "$T4_WORK/dev-noregion.log" && grep -q "vercel.com/docs/sandbox/concepts/regions" "$T4_WORK/dev-noregion.log"; then
  echo "PASS next dev: missing-region error names the fix and the regions page"; else echo "FAIL next dev: expected missing-region error not found"; fi
