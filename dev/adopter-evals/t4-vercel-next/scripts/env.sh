# Shared configuration for the t4 scripts. Source it; every value comes from the environment.
#   Required for deploy/core checks: VERCEL_TOKEN, VERCEL_ORG_ID, VERCEL_PROJECT_ID, VERCEL_SCOPE (team slug)
#   Optional: T4_RELEASE_BASE (tarball base URL), T4_BASE_URL (deployed URL), T4_WORK (scratch dir)
set -u
T4_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
T4_WORK=${T4_WORK:-${RUNNER_TEMP:-/tmp}/t4-work}
T4_PACKAGES="jazz-tools jazz-wasm jazz-napi garden-co-jazz-napi-linux-x64-gnu"
mkdir -p "$T4_WORK"
# Name of the production core vercel() uses: jazz-<project id, lowercased, non-alnum runs -> "-">-production.
t4_core_name() {
  local p; p=$(printf '%s' "${VERCEL_PROJECT_ID:?}" | tr 'A-Z' 'a-z' | sed -E 's/[^a-z0-9]+/-/g')
  echo "jazz-$p-production"
}
t4_vercel() { vercel --token "${VERCEL_TOKEN:?}" --scope "${VERCEL_SCOPE:?}" "$@"; }
