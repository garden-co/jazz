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
step "deploy"; "$S/deploy.sh" "$APP" || exit 1
if [ "$T4_MODE" = preview ]; then export T4_TARGET_URL=$(cat "$T4_WORK/deploy-url"); [ -n "$T4_TARGET_URL" ] || { echo "FAIL preview url not found in the deploy output"; exit 1; }
  code=$(t4_curl -s -o /dev/null -w "%{http_code}" "$T4_TARGET_URL/"); [ "$code" = 200 ] || { echo "FAIL preview answers $code (protected? set T4_VERCEL_BYPASS_SECRET)"; exit 1; }
else export T4_TARGET_URL=${T4_BASE_URL:?}; fi
step "playwright (installed in the background since the deps step)"
wait "$PW_PID" || { echo "FAIL playwright install"; tail -5 "$T4_WORK/playwright.log"; exit 1; }
mkdir -p "$APP/node_modules" && ln -sfn "$T4_WORK/pw/node_modules/playwright" "$APP/node_modules/playwright" && ln -sfn "$T4_WORK/pw/node_modules/playwright-core" "$APP/node_modules/playwright-core"
step "smoke (one retry for jazz#5553)"
(cd "$APP" && BASE="$T4_TARGET_URL" node smoke/smoke.mjs) || { echo "smoke run 1 failed; retrying once"; (cd "$APP" && BASE="$T4_TARGET_URL" node smoke/smoke.mjs) || FAIL=1; }
step "core and inventory after"; "$S/core-state.sh" || true
step "wrong-region probe (all creating calls stubbed)"
for n in 0 1 2; do echo "-- FAIL_GET_AT=$n"; FAIL_GET_AT=$n node "$S/wrong-region-probe.mjs" "$APP" sfo1 2>&1 | grep -E "^(result|PASS|FAIL|FINDING)"; done
if [ "$T4_MODE" = preview ] && [ "${T4_TEARDOWN:-}" = 1 ]; then
  # Delete exactly this run's preview core: the Sandbox and Drive named for ci-<this run id>, nothing else.
  step "teardown (this run's preview core only)"; core=$(t4_core_name)
  case "$core" in *-ci-"${GITHUB_RUN_ID:-local}"*) ;; *) echo "FAIL teardown: refusing to delete '$core' (not this run's)"; FAIL=1; core=;; esac
  if [ -n "$core" ]; then
    t4_vercel sandbox rm --project "$VERCEL_PROJECT_ID" "$core" > "$T4_WORK/teardown.log" 2>&1 && echo "PASS teardown: sandbox $core removed" || echo "REVIEW teardown: sandbox $core not removed (see teardown.log)"
    t4_vercel sandbox drives rm --project "$VERCEL_PROJECT_ID" "$core-data" >> "$T4_WORK/teardown.log" 2>&1 && echo "PASS teardown: drive $core-data removed" || echo "REVIEW teardown: drive $core-data not removed (see teardown.log)"
  fi
fi
step "end"
exit $FAIL
