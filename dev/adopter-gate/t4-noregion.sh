#!/usr/bin/env bash
# t4's missing-region check, off t4's critical path and without secrets: with the RC, vercel() lacking `region`
# must fail `next build` and make `next dev` answer /jazz with the error that names the fix (jazz#5615).
. "$(dirname "$0")/lib.sh"
body() {
  export T4_WORK=${RUNNER_TEMP:-/tmp}/t4-noregion T4_RELEASE_BASE=${T4_RELEASE_BASE:-file://$AG_TGZ}; rm -rf "$T4_WORK"; mkdir -p "$T4_WORK"
  local APP=$T4_WORK/app; cp -r "$AG/t4/app67" "$APP"
  "$AG/t4/scripts/rc-deps.sh" "$APP" "$(ag_version)" | grep -E "^(PASS|FAIL) " | sed 's/^\(PASS\|FAIL\) /\1 t4-noregion /'
  grep -q 'region:' "$APP/jazz.server.ts" && { echo "SKIP t4-noregion: the base app already sets region"; return; }
  "$AG/t4/scripts/no-region-check.sh" "$APP" | grep -E "^(PASS|FAIL) "
}
ag_job t4-noregion body
