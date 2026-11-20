#!/usr/bin/env bash
# t4: the Next.js + Better Auth app on vercel() against a release's npm tarballs.
# usage: run-t4.sh <version> [base-app: app67]   e.g. run-t4.sh 2.0.0-alpha.68-rc.1
# env: see ../README.md. Prints PASS/FAIL/FINDING lines; exits 1 if a required step failed.
source "$(dirname "$0")/env.sh"
V=$1; BASEAPP=${2:-app67}; S="$(dirname "$0")"; FAIL=0
APP="$T4_WORK/app"; rm -rf "$APP"; cp -r "$T4_DIR/$BASEAPP" "$APP"
step() { echo; echo "=== $* @$(date -u +%s)"; }
step "core and inventory before"; "$S/core-state.sh" || true
step "deps -> $V"; "$S/rc-deps.sh" "$APP" "$V" || exit 1
# Playwright for the smoke, in its own dir and in the background: it doesn't touch the app being built and deployed.
( rm -rf "$T4_WORK/pw" && mkdir -p "$T4_WORK/pw" && cd "$T4_WORK/pw" && echo '{"private":true}' > package.json \
  && { for i in 1 2 3; do npm install --no-audit --no-fund playwright@1.65.1 && break; [ $i = 3 ] && exit 1; sleep $((i * 5)); done; } \
  && { for i in 1 2 3; do npx playwright install --with-deps chromium && break; [ $i = 3 ] && exit 1; sleep $((i * 5)); done; } ) > "$T4_WORK/playwright.log" 2>&1 &
PW_PID=$!
if ! grep -q 'region:' "$APP/jazz.server.ts"; then
  # T4_SKIP_NOREGION=1: the no-region check runs elsewhere (CI: its own job, t4-noregion.sh), off this critical path.
  if [ -z "${T4_SKIP_NOREGION:-}" ]; then step "vercel() without region"; "$S/no-region-check.sh" "$APP"; fi
  step "region iad1"; "$S/set-region.sh" "$APP" iad1
fi
step "build"; "$S/build.sh" "$APP" || exit 1
# T4_TEARDOWN=1 (preview): the teardown runs on every exit after the deploy step starts, a failed one too.
teardown() {
  # Delete exactly what this run created: its preview deployment (first: while it is live, any request starts the core
  # again), then the Sandbox and Drive named for ci-<this run id>. Nothing else.
  step "teardown (this run's preview deployment and core only)"; core=$(t4_core_name); url=$(cat "$T4_WORK/deploy-url" 2>/dev/null)
  case "$core" in *-ci-"${GITHUB_RUN_ID:-local}") ;; *) echo "FAIL teardown: refusing to delete '$core' (not this run's)"; FAIL=1; core=;; esac
  : > "$T4_WORK/teardown.log"
  # Never production: only a preview URL this run's deploy printed, never the production address.
  [ -n "$url" ] && [ "$url" = "${T4_BASE_URL:-}" ] && { echo "FAIL teardown: refusing to delete the production URL"; FAIL=1; url=; }
  case "$url" in https://jazz-eval-*.vercel.app) ;; "") ;; *) echo "FAIL teardown: refusing to delete deployment '$url'"; FAIL=1; url=;; esac
  if [ -n "$url" ]; then
    t4_vercel rm --yes "$url" >> "$T4_WORK/teardown.log" 2>&1 && echo "PASS teardown: deployment $url removed" || { echo "FAIL teardown: deployment $url not removed (see teardown.log)"; FAIL=1; }
  fi
  if [ -n "$core" ]; then
    # --all: plain `sandbox ls` hides stopped sandboxes, which still have to go.
    if t4_vercel sandbox ls --all --project "$VERCEL_PROJECT_ID" 2>/dev/null | grep -q "^$core "; then
      t4_vercel sandbox rm --project "$VERCEL_PROJECT_ID" "$core" >> "$T4_WORK/teardown.log" 2>&1 && echo "PASS teardown: sandbox $core removed" || { echo "FAIL teardown: sandbox $core not removed (see teardown.log)"; FAIL=1; }
    fi
    # A Drive can't be deleted while a Sandbox holds it: wait (up to 3 min) until it shows no attached sandbox.
    for i in $(seq 36); do
      row=$(t4_vercel sandbox drives ls --project "$VERCEL_PROJECT_ID" --name-prefix "$core-data" 2>/dev/null | grep "^$core-data ")
      [ -z "$row" ] || [ "$(echo "$row" | awk '{print $(NF-1)}')" = - ] && break; sleep 5
    done
    if [ -z "$row" ]; then echo "PASS teardown: no drive $core-data left"
    else t4_vercel sandbox drives rm --project "$VERCEL_PROJECT_ID" "$core-data" >> "$T4_WORK/teardown.log" 2>&1 && echo "PASS teardown: drive $core-data removed" || { echo "FAIL teardown: drive $core-data not removed (see teardown.log)"; FAIL=1; }; fi
  fi
  step "end"; [ "$FAIL" = 0 ] || exit 1
}
[ "$T4_MODE" = preview ] && [ "${T4_TEARDOWN:-}" = 1 ] && trap teardown EXIT
step "deploy"; "$S/deploy.sh" "$APP" || exit 1
if [ "$T4_MODE" = preview ]; then export T4_TARGET_URL=$(cat "$T4_WORK/deploy-url"); [ -n "$T4_TARGET_URL" ] || { echo "FAIL preview url not found in the deploy output"; exit 1; }
  code=$(t4_curl -s -o /dev/null -w "%{http_code}" "$T4_TARGET_URL/"); [ "$code" = 200 ] || { echo "FAIL preview answers $code (protected? set T4_VERCEL_BYPASS_SECRET)"; exit 1; }
else export T4_TARGET_URL=${T4_BASE_URL:?}; fi
step "playwright (installed in the background since the deps step)"
wait "$PW_PID" || { echo "FAIL playwright install"; tail -5 "$T4_WORK/playwright.log"; exit 1; }
mkdir -p "$APP/node_modules" && ln -sfn "$T4_WORK/pw/node_modules/playwright" "$APP/node_modules/playwright" && ln -sfn "$T4_WORK/pw/node_modules/playwright-core" "$APP/node_modules/playwright-core"
step "smoke (one run, no retry)"
(cd "$APP" && BASE="$T4_TARGET_URL" node smoke/smoke.mjs) || FAIL=1
step "core and inventory after"; "$S/core-state.sh" || true
step "wrong-region probe (all creating calls stubbed)"
for n in 0 1 2; do echo "-- FAIL_GET_AT=$n"; FAIL_GET_AT=$n node "$S/wrong-region-probe.mjs" "$APP" sfo1 2>&1 | grep -E "^(result|PASS|FAIL|FINDING)"; done
step "end"
exit $FAIL
