#!/usr/bin/env bash
# Production deploy of the app to the eval project. usage: deploy.sh <app-dir>
# env: VERCEL_TOKEN, VERCEL_ORG_ID, VERCEL_PROJECT_ID (read by the Vercel CLI), VERCEL_SCOPE
source "$(dirname "$0")/env.sh"
APP=$(cd "$1" && pwd); cd "$APP"
: "${VERCEL_ORG_ID:?}" "${VERCEL_PROJECT_ID:?}"
if [ "$T4_MODE" = preview ]; then
  # A preview: the project's auth vars are production-only, so this deployment gets its own secret; BETTER_AUTH_URL
  # stays unset and Better Auth takes the request's origin.
  CORE_TGZ=$(cd public && ls t4-core/garden-co-jazz-napi-linux-x64-gnu-*.tgz) || { echo "no core tarball in public/t4-core (build.sh)"; exit 1; }
  t4_vercel deploy --prebuilt --yes -e "T4_NAMESPACE=$(t4_namespace)" -e "T4_CORE_TARBALL=$CORE_TGZ" -e "BETTER_AUTH_SECRET=t4-preview-$(head -c 16 /dev/urandom | od -An -tx1 | tr -d ' \n')" > "$T4_WORK/deploy.log" 2>&1; rc=$?
  grep -oE 'https://[a-z0-9.-]+\.vercel\.app' "$T4_WORK/deploy.log" | tail -1 > "$T4_WORK/deploy-url"; echo "preview url: $(cat "$T4_WORK/deploy-url")"
elif [ -d .vercel/output ]; then t4_vercel deploy --prebuilt --prod --yes > "$T4_WORK/deploy.log" 2>&1; rc=$?   # build.sh built it
else t4_vercel deploy --prod --yes > "$T4_WORK/deploy.log" 2>&1; rc=$?; fi
echo "deploy exit code: $rc"; grep -E '"message"|^Error' "$T4_WORK/deploy.log" | tail -2; exit $rc
