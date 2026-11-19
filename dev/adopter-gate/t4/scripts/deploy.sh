#!/usr/bin/env bash
# Production deploy of the app to the eval project. usage: deploy.sh <app-dir>
# env: VERCEL_TOKEN, VERCEL_ORG_ID, VERCEL_PROJECT_ID (read by the Vercel CLI), VERCEL_SCOPE
source "$(dirname "$0")/env.sh"
APP=$(cd "$1" && pwd); cd "$APP"
: "${VERCEL_ORG_ID:?}" "${VERCEL_PROJECT_ID:?}"
if [ -d .vercel/output ]; then t4_vercel deploy --prebuilt --prod --yes > "$T4_WORK/deploy.log" 2>&1; rc=$?   # build.sh built it
else t4_vercel deploy --prod --yes > "$T4_WORK/deploy.log" 2>&1; rc=$?; fi
echo "deploy exit code: $rc"; grep -E '"message"|^Error' "$T4_WORK/deploy.log" | tail -2; exit $rc
