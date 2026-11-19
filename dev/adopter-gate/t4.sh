#!/usr/bin/env bash
# t4: Next.js + Better Auth on vercel(), deployed to the long-lived eval project (see t4/README.md). Serialise it.
# Without VERCEL_TOKEN it reports SKIP (secrets pending), which the verdict shows as not green.
. "$(dirname "$0")/lib.sh"
body() {
  # Modes: production (a real RC), preview (a dry run: a Vercel preview with its own core), skip (not run).
  # "Not run" prints only NOTE lines: the verdict lists them and leaves t4 out of the status, so a dry run can still PASS.
  case "${T4_MODE:=production}" in
    skip) echo "NOTE t4 not run: dry run (t4_mode skip)"; return;;
    # The eval project's previews are public (Anselm, 2026-11-19); a T4_VERCEL_BYPASS_SECRET, if one is ever set, is sent.
    preview) ;;
    production)
      # Fail closed: production deploys to the long-lived eval core, so only for a real RC (its release may still be a draft).
      local tag=${T4_RC_TAG:-}
      [ -n "$tag" ] && [ "${tag#dryrun-}" = "$tag" ] || { echo "FAIL t4 production mode needs a real rc_tag (got '${tag:-none}'); not deploying"; return; }
      # An existing release of this repo (the RC's draft counts: the gate runs before publish) named by a real tag.
      gh release view "$tag" -R "${GITHUB_REPOSITORY:-garden-co/jazz}" --json tagName >/dev/null 2>&1 \
        || { echo "FAIL t4 production mode: no release $tag in ${GITHUB_REPOSITORY:-garden-co/jazz}; not deploying"; return; };;
    *) echo "FAIL t4 unknown t4_mode '$T4_MODE'"; return;;
  esac
  export T4_MODE
  if [ -z "${VERCEL_TOKEN:-}" ]; then echo "SKIP t4 vercel: secrets pending (VERCEL_TOKEN, BASELINE_RED_PASSWORD; see t4/README.md)"; return; fi
  export T4_RELEASE_BASE=${T4_RELEASE_BASE:-file://$AG_TGZ}
  # The eval core only moves to newer versions (newerVersion). A version that sorts below what it runs (an RC dry run's
  # rc.0, an old RC) would deploy a backend onto a newer core: not an upgrade test, and it leaves the eval app skewed.
  local cur; cur=$("$AG/t4/scripts/core-state.sh" 2>/dev/null | grep -oE 'core running=[^ ]+' | head -1 | cut -d= -f2)
  newer() { [ "$1" != "$2" ] && [ "$(printf '%s\n%s\n' "${1//-/\~}" "${2//-/\~}" | sort -V | tail -1)" = "${1//-/\~}" ]; }
  if [ -n "$cur" ] && newer "$cur" "$(ag_version)"; then
    echo "SKIP t4 vercel: $(ag_version) sorts below the eval core's $cur (a dry-run or older version); t4 deploys only versions that move the core"; return; fi
  "$AG/t4/scripts/run-t4.sh" "$(ag_version)" > "$AG_RESULTS/t4-full.log" 2>&1; local rc=$?
  cat "$AG_RESULTS/t4-full.log" \
    | grep -E "^(PASS|FAIL|REVIEW|NOTE|FINDING|SEEDED)" \
    | sed 's/^FINDING:\{0,1\} */NOTE t4 known finding (jazz#5615, wrong-region fall-through): /; s/^SEEDED/REVIEW t4 first run: seeded/'
  local links; links=$(grep -c "invalid_link_challenge" "$AG_RESULTS/t4-full.log")
  [ "$links" -gt 0 ] && echo "FAIL t4 smoke hit invalid_link_challenge $links× (jazz#5553: vercel() signs guest-link challenges per function instance)"
  # Per-step wall time (run-t4.sh stamps each "=== step @epoch"), for the release-gate budget.
  awk '/^=== .* @[0-9]+$/ { t=$NF; sub(/^@/,"",t); if (p) printf "TIME t4 %ss %s\n", t-pt, p; p=$0; sub(/^=== /,"",p); sub(/ @[0-9]+$/,"",p); pt=t }' "$AG_RESULTS/t4-full.log"
  # The core: its version after the deploy, and whether it existed before (the old-data checks need it to).
  local L=$AG_RESULTS/t4-full.log
  local before; before=$(sed -n '/^=== core and inventory before/,/^=== deps/p' "$L" | grep -cE '^  jazz-.*-data ')
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
