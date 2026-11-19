#!/usr/bin/env bash
# t4: Next.js + Better Auth on vercel(), deployed to the long-lived eval project (see t4/README.md). Serialise it.
# Without VERCEL_TOKEN it reports SKIP (secrets pending), which the verdict shows as not green.
. "$(dirname "$0")/lib.sh"
body() {
  if [ -z "${VERCEL_TOKEN:-}" ]; then echo "SKIP t4 vercel: secrets pending (VERCEL_TOKEN, BASELINE_RED_PASSWORD; see t4/README.md)"; return; fi
  export T4_RELEASE_BASE=${T4_RELEASE_BASE:-file://$AG_TGZ}
  "$AG/t4/scripts/run-t4.sh" "$(ag_version)" 2>&1 | tee "$AG_RESULTS/t4-full.log" \
    | grep -E "^(PASS|FAIL|FINDING|SEEDED|smoke run 1 failed)" \
    | sed 's/^FINDING/REVIEW t4 finding:/; s/^SEEDED/REVIEW t4 first run: seeded/; s/^smoke run 1 failed; retrying once/REVIEW t4 smoke run 1 failed, retried once (known flake jazz#5553; remove the retry once it is fixed)/'
}
ag_job t4 body
