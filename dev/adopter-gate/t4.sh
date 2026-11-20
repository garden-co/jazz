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
    preview)
      # In CI, previews run side by side (adopter-gate.yml's t4 concurrency group is per run), so each needs its own
      # core: without T4_TEARDOWN=1 it would share cli-previews with every other preview run.
      [ -n "${GITHUB_RUN_ID:-}" ] && [ "${T4_TEARDOWN:-}" != 1 ] && { echo "FAIL t4 preview can't run: in CI it needs T4_TEARDOWN=1 (its own per-run core), else it would share cli-previews with concurrent runs"; return; }
      ;;
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
  # t4 that should run but can't is FAIL (Anselm, 2026-11-19); only an explicit t4_mode skip is NOT_RUN.
  if [ -z "${VERCEL_TOKEN:-}" ]; then echo "FAIL t4 vercel: can't run, VERCEL_TOKEN is missing (environment adopter-gate-t4; see t4/README.md)"; return; fi
  export T4_RELEASE_BASE=${T4_RELEASE_BASE:-file://$AG_TGZ}
  # The eval core only moves to newer versions (newerVersion). A version that sorts below what it runs (an RC dry run's
  # rc.0, an old RC) would deploy a backend onto a newer core: not an upgrade test, and it leaves the eval app skewed.
  local cur; cur=$("$AG/t4/scripts/core-state.sh" 2>/dev/null | grep -oE 'core running=[^ ]+' | head -1 | cut -d= -f2)
  newer() { [ "$1" != "$2" ] && [ "$(printf '%s\n%s\n' "${1//-/\~}" "${2//-/\~}" | sort -V | tail -1)" = "${1//-/\~}" ]; }
  if [ -n "$cur" ] && newer "$cur" "$(ag_version)"; then
    echo "FAIL t4 vercel: can't run, $(ag_version) sorts below the eval core's $cur (a dry-run or older version; use t4_mode preview); not deploying"; return; fi
  "$AG/t4/scripts/run-t4.sh" "$(ag_version)" > "$AG_RESULTS/t4-full.log" 2>&1; local rc=$?
  # A seeded baseline means the core had no data before. A preview's per-run core is empty by design (NOTE); the
  # production core should carry over, so there it's REVIEW (#ci-release-speed's call). A failed seeding FAILs.
  local seeded=REVIEW; [ "$T4_MODE" = preview ] && seeded=NOTE
  cat "$AG_RESULTS/t4-full.log" \
    | grep -E "^(PASS|FAIL|REVIEW|NOTE|FINDING|SEEDED)" \
    | sed 's/^FINDING:\{0,1\} */FAIL t4 known bug jazz#5615 (wrong-region fall-through, fails until fixed): /' \
    | sed "/^SEEDED.*FAILED$/s/^SEEDED/FAIL t4 first run: seeding/; s/^SEEDED/$seeded t4 first run: seeded/"
  local links; links=$(grep -c "invalid_link_challenge" "$AG_RESULTS/t4-full.log")
  [ "$links" -gt 0 ] && echo "FAIL t4 smoke hit invalid_link_challenge $links× (jazz#5553: vercel() signs guest-link challenges per function instance)"
  # Per-step wall time (run-t4.sh stamps each "=== step @epoch"), for the release-gate budget.
  awk '/^=== .* @[0-9]+$/ { t=$NF; sub(/^@/,"",t); if (p) printf "TIME t4 %ss %s\n", t-pt, p; p=$0; sub(/^=== /,"",p); sub(/ @[0-9]+$/,"",p); pt=t }' "$AG_RESULTS/t4-full.log"
  # The core: its version after the deploy, and whether it existed before (the old-data checks need it to).
  local L=$AG_RESULTS/t4-full.log
  if [ "$T4_MODE" = production ]; then   # the long-lived production core must carry over (its old data is checked)
    local before; before=$(sed -n '/^=== core and inventory before/,/^=== deps/p' "$L" | grep -cE '^  jazz-.*-production-data ')
    [ "$before" -ge 1 ] && echo "PASS t4 the existing core's Drive was there before the deploy" \
      || echo "REVIEW t4 no core existed before the deploy: vercel() created a new, empty one, so old data couldn't be checked against it"
  fi
  local ran; ran=$(sed -n '/^=== core and inventory after/,/^=== wrong-region/p' "$L" | grep -oE 'core running=[^ ]+' | cut -d= -f2)
  # The core this run used: the per-run ci-<run id> core in preview mode, the long-lived production core otherwise.
  local core; core=$(source "$AG/t4/scripts/env.sh" && t4_core_name)
  [ "$ran" = "$(ag_version)" ] && echo "PASS t4 core $core runs $ran" || echo "REVIEW t4 couldn't confirm the core's version (read: '${ran:-nothing}')"
  # run-t4.sh stops at a failed required step (build, deploy, playwright) without always printing a FAIL line.
  # A run that reached "=== end" didn't stop: its failures are the FAIL lines above (a failed smoke, say).
  if [ $rc -ne 0 ] && ! grep -q '^=== end' "$AG_RESULTS/t4-full.log"; then echo "FAIL t4 stopped at step: $(grep '^=== ' "$AG_RESULTS/t4-full.log" | grep -vE '^=== (teardown|end)' | tail -1 | sed 's/^=== //') (exit $rc)"; grep -E '^(deploy|next build|jazz-tools generate --check) exit code: [1-9]' "$AG_RESULTS/t4-full.log" | sed 's/^/FAIL t4 /'; fi
}
ag_job t4 body; rc=$?
# On failure, show the step logs run-t4.sh keeps in $T4_WORK (they're also uploaded as the t4-logs artifact).
if [ $rc -ne 0 ]; then for f in "${T4_WORK:-${RUNNER_TEMP:-/tmp}/t4-work}"/*.log; do [ -f "$f" ] && { echo "--- tail $f"; tail -25 "$f"; }; done; fi
exit $rc
