#!/usr/bin/env bash
# Wake the core through the app, then print the version it runs (Sandbox /tmp/jazz.version, Drive
# /data/version, core binaries) and its /health, and the project's Sandboxes and Drives.
# usage: core-state.sh   (env: T4_BASE_URL, VERCEL_TOKEN, VERCEL_SCOPE, VERCEL_PROJECT_ID)
source "$(dirname "$0")/env.sh"
S=$(t4_core_name); P="--project ${VERCEL_PROJECT_ID}"
U=${T4_TARGET_URL:-${T4_BASE_URL:-}}; [ -n "$U" ] && t4_curl -s -o /dev/null -w "app /: %{http_code}\n" "$U/"
t4_vercel sandbox exec $P "$S" sh -c 'printf "core running=%s drive=%s bins=%s\n" "$(cat /tmp/jazz.version)" "$(cat /data/version)" "$(ls /data/bin | tr "\n" " ")"; printf "core health=%s\n" "$(curl -s localhost:1625/health)"' 2>&1 | grep -E "^core |[Ee]rror" || echo "core: no answer (is the Sandbox stopped? wake it through the app first)"
echo "sandboxes:"; t4_vercel sandbox ls --all $P 2>&1 | grep -E "^jazz-" | awk '{print "  " $1, $2}'
echo "drives:"; t4_vercel sandbox drives ls $P 2>&1 | grep -E "^jazz-" | awk '{print "  " $1, $2}'
