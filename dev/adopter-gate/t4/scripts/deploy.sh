#!/usr/bin/env bash
# Production deploy of the app to the eval project. usage: deploy.sh <app-dir>
# env: VERCEL_TOKEN, VERCEL_ORG_ID, VERCEL_PROJECT_ID (read by the Vercel CLI), VERCEL_SCOPE
source "$(dirname "$0")/env.sh"
APP=$(cd "$1" && pwd); cd "$APP"
: "${VERCEL_ORG_ID:?}" "${VERCEL_PROJECT_ID:?}"
t4_vercel deploy --prod --yes > "$T4_WORK/deploy.log" 2>&1; rc=$?
echo "deploy exit code: $rc"; grep -E '"message"' "$T4_WORK/deploy.log" | tail -1; exit $rc
