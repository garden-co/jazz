# Shared configuration for the t4 scripts. Source it; every value comes from the environment.
#   Required for deploy/core checks: VERCEL_TOKEN, VERCEL_ORG_ID, VERCEL_PROJECT_ID, VERCEL_SCOPE (team slug)
#   Optional: T4_RELEASE_BASE (tarball base URL), T4_BASE_URL (deployed URL), T4_WORK (scratch dir)
set -u
T4_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
T4_WORK=${T4_WORK:-${RUNNER_TEMP:-/tmp}/t4-work}
T4_PACKAGES="jazz-tools jazz-wasm jazz-napi garden-co-jazz-napi-linux-x64-gnu"
# T4_MODE=production (a real RC: the long-lived production core, upgraded in place) or preview (a dry run: a Vercel
# preview with the app's preview core, namespace "cli-previews"; production and its core are never touched).
T4_MODE=${T4_MODE:-production}
# Preview core: per run (ci-<run id>) when T4_TEARDOWN=1, so no dry run inherits a newer core from an earlier one, and
# t4 deletes exactly that Sandbox and Drive afterwards; otherwise the shared preview core (cli-previews), never deleted.
T4_PREVIEW_NS=$([ "${T4_TEARDOWN:-}" = 1 ] && echo "ci-${GITHUB_RUN_ID:-local$$}" || echo cli-previews)
export T4_PREVIEW_NS
t4_namespace() { [ "$T4_MODE" = preview ] && echo "$T4_PREVIEW_NS" || echo production; }
# Preview URLs sit behind Vercel Authentication: T4_VERCEL_BYPASS_SECRET (Protection Bypass for Automation) opens them.
t4_curl() { curl ${T4_VERCEL_BYPASS_SECRET:+-H "x-vercel-protection-bypass: $T4_VERCEL_BYPASS_SECRET"} "$@"; }
mkdir -p "$T4_WORK"
# Name of the production core vercel() uses: jazz-<project id, lowercased, non-alnum runs -> "-">-production.
t4_core_name() {
  local p; p=$(printf '%s' "${VERCEL_PROJECT_ID:?}" | tr 'A-Z' 'a-z' | sed -E 's/[^a-z0-9]+/-/g')
  echo "jazz-$p-$(t4_namespace)"
}
t4_vercel() { vercel --token "${VERCEL_TOKEN:?}" --scope "${VERCEL_SCOPE:?}" "$@"; }
