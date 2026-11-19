#!/usr/bin/env bash
# t4: Next.js + Better Auth on vercel(), deployed to the long-lived eval project (see t4/README.md). Serialise it.
# Without VERCEL_TOKEN it reports SKIP (secrets pending), which the verdict shows as not green.
. "$(dirname "$0")/lib.sh"
body() {
  if [ -z "${VERCEL_TOKEN:-}" ]; then echo "SKIP t4 vercel: secrets pending (VERCEL_TOKEN, BASELINE_RED_PASSWORD; see t4/README.md)"; return; fi
  export T4_RELEASE_BASE=${T4_RELEASE_BASE:-file://$AG_TGZ}
  "$AG/t4/scripts/run-t4.sh" "$(ag_version)" > "$AG_RESULTS/t4-full.log" 2>&1; local rc=$?
  grep -E "^(PASS|FAIL|FINDING|SEEDED|smoke run 1 failed)" "$AG_RESULTS/t4-full.log" \
    | sed 's/^FINDING/REVIEW t4 finding:/; s/^SEEDED/REVIEW t4 first run: seeded/; s/^smoke run 1 failed; retrying once/REVIEW t4 smoke run 1 failed, retried once (known flake jazz#5553; remove the retry once it is fixed)/'
  # The core: its version after the deploy, and whether it existed before (the old-data checks need it to).
  local L=$AG_RESULTS/t4-full.log
  local before; before=$(sed -n '/^=== core and inventory before/,/^=== deps/p' "$L" | grep -cE '^  jazz-.*-production-data ')
  [ "$before" -ge 1 ] && echo "PASS t4 the existing core's Drive was there before the deploy" \
    || echo "REVIEW t4 no core existed before the deploy: vercel() created a new, empty one, so old data couldn't be checked against it"
  local ran; ran=$(sed -n '/^=== core and inventory after/,/^=== wrong-region/p' "$L" | grep -oE 'core running=[^ ]+' | cut -d= -f2)
  [ "$ran" = "$(ag_version)" ] && echo "PASS t4 production core runs $ran" || echo "REVIEW t4 couldn't confirm the core's version (read: '${ran:-nothing}')"
  # run-t4.sh stops at a failed required step (build, deploy, playwright) without always printing a FAIL line.
  if [ $rc -ne 0 ]; then echo "FAIL t4 stopped at step: $(grep '^=== ' "$AG_RESULTS/t4-full.log" | tail -1 | sed 's/^=== //') (exit $rc)"; grep -E '^(deploy|next build|jazz-tools generate --check) exit code: [1-9]' "$AG_RESULTS/t4-full.log" | sed 's/^/FAIL t4 /'; fi
}
ag_job t4 body; rc=$?
# On failure, show the step logs run-t4.sh keeps in $T4_WORK (they're also uploaded as the t4-logs artifact).
if [ $rc -ne 0 ]; then for f in "${T4_WORK:-${RUNNER_TEMP:-/tmp}/t4-work}"/*.log; do [ -f "$f" ] && { echo "--- tail $f"; tail -25 "$f"; }; done; fi
exit $rc
