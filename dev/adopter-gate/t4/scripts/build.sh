#!/usr/bin/env bash
# App-file check (generate --check on .66+, validate --check before) and next build, then sizes.
# usage: build.sh <app-dir>
source "$(dirname "$0")/env.sh"
APP=$(cd "$1" && pwd); cd "$APP"; rm -rf .next .jazz
if npx jazz-tools --help 2>&1 | grep -q "^  generate"; then CHECK="generate --check"; else CHECK="validate --check"; fi
npx jazz-tools $CHECK > "$T4_WORK/check.log" 2>&1; rc=$?; echo "jazz-tools $CHECK exit code: $rc"; [ $rc = 0 ] || { tail -20 "$T4_WORK/check.log"; exit 1; }
if [ -n "${VERCEL_TOKEN:-}" ]; then
  # Build once, here: the deploy uploads this output (deploy.sh --prebuilt) instead of Vercel rebuilding remotely.
  t4_vercel pull --yes --environment=production > "$T4_WORK/pull.log" 2>&1 || { echo "vercel pull failed"; tail -10 "$T4_WORK/pull.log"; exit 1; }
  # Sensitive production vars come down redacted, which breaks the prerender (BETTER_AUTH_URL isn't a URL). The build
  # only needs placeholders: the deployed functions get the project's real values at runtime.
  sed -i '/^BETTER_AUTH_SECRET=/d; /^BETTER_AUTH_URL=/d' .vercel/.env.production.local 2>/dev/null
  BETTER_AUTH_SECRET=t4-local-build-only BETTER_AUTH_URL=${T4_BASE_URL:-http://localhost} t4_vercel build --prod > "$T4_WORK/build.log" 2>&1; rc=$?
else BETTER_AUTH_SECRET=t4-local-build-only pnpm build > "$T4_WORK/build.log" 2>&1; rc=$?; fi
echo "next build exit code: $rc"; [ $rc = 0 ] || { tail -30 "$T4_WORK/build.log"; exit 1; }
python3 -I "$(dirname "$0")/sizes.py" "$APP"
