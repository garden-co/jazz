#!/usr/bin/env bash
# Finds the prebuilt artifacts for a release, for release-publish.yml.
#
#   SHA=<commit> VERSION=<version> MODE=<publish|dry-run> [ARTIFACTS_RUN_ID=<id>] [LEGACY_RUN_IDS="<id> <id> …"] \
#     dev/release-publish/resolve-artifacts.sh
#
# Takes the newest successful release-artifacts.yml run that built <sha>
# itself (head_sha), whose release-manifest names the same commit, tree and
# version (dev/release-publish/producer-run.mjs). ARTIFACTS_RUN_ID names one
# run instead of searching; it is held to the same rule.
#
# Dry runs only: LEGACY_RUN_IDS names the runs of the
# existing workflows that hold the same files under their old names (a
# preview-jazz-tools-alpha-release run, attach-language-artifacts dry run,
# swift-mirror dry run, publish-jazz-kotlin dry run): the workflow repacks
# them under the new names in its own run.
#
# Writes to GITHUB_OUTPUT: mode (release-artifacts|legacy), run_id (where
# the release-* artifacts are; empty for legacy: this run), source_sha (the
# commit the artifacts were built from), tree, inspector_run_id, legacy (a
# JSON map of old artifact name -> run id).
set -euo pipefail
: "${SHA:?}" "${VERSION:?}" "${GH_REPO:?}"
out="${GITHUB_OUTPUT:-/dev/stdout}"
tree="$(gh api "repos/${GH_REPO}/git/commits/${SHA}" --jq .tree.sha)"
echo "tree=${tree}" >> "${out}"
echo "release ${VERSION} at ${SHA}, tree ${tree}"

# Supply-chain rule: only a successful release-artifacts.yml run of this
# repository that built exactly ${SHA} (head_sha), with a release-manifest
# naming the same commit, tree and version (dev/release-publish/producer-run.mjs).
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
producer_ok() { # <run id>
  local dir
  dir="$(mktemp -d)"
  gh api "repos/${GH_REPO}/actions/runs/$1" > "${dir}/run.json" || return 1
  gh run download "$1" -R "${GH_REPO}" -n release-manifest -D "${dir}" >/dev/null 2>&1 ||
    { echo "run $1: no release-manifest"; return 1; }
  SHA="${SHA}" TREE="${tree}" VERSION="${VERSION}" GH_REPO="${GH_REPO}" \
    node "${here}/producer-run.mjs" "${dir}/run.json" "${dir}/manifest.json"
}

candidates=()
if [ -n "${ARTIFACTS_RUN_ID:-}" ]; then
  candidates=("${ARTIFACTS_RUN_ID}")
elif runs="$(gh api "repos/${GH_REPO}/actions/workflows/release-artifacts.yml/runs?head_sha=${SHA}&status=success&per_page=50" 2>/dev/null)"; then
  mapfile -t candidates < <(jq -r --arg sha "${SHA}" '.workflow_runs | sort_by(.created_at) | reverse | .[]
    | select(.head_sha == $sha) | .id' <<<"${runs}")
fi
for run in "${candidates[@]}"; do
  if producer_ok "${run}"; then
    echo "release-artifacts run ${run} built ${SHA}: its artifacts, and its inspector-prebuilt"
    {
      echo "mode=release-artifacts"
      echo "run_id=${run}"
      echo "source_sha=${SHA}"
      echo "inspector_run_id=${run}"
      echo "legacy={}"
    } >> "${out}"
    exit 0
  fi
done
if [ -n "${ARTIFACTS_RUN_ID:-}" ]; then
  echo "::error::release-artifacts run ${ARTIFACTS_RUN_ID} may not be published for ${SHA} (see above)"
  exit 1
fi

# The legacy runs were dispatched from main with a `ref` input, so their
# head_sha is never the commit they built: they can't meet the rule, and only
# a dry run takes them.
if [ -n "${LEGACY_RUN_IDS:-}" ] && [ "${MODE:-}" = publish ]; then
  echo "::error::legacy-run-ids is for dry runs only: a publish takes a release-artifacts.yml run that built ${SHA}."
  exit 1
fi

if [ -z "${LEGACY_RUN_IDS:-}" ]; then
  echo "::error::No successful release-artifacts.yml run built tree ${tree} at ${VERSION}, and no legacy-run-ids were given."
  exit 1
fi

# Legacy: map each old artifact name to the run holding it.
declare -A where=()
for run in ${LEGACY_RUN_IDS//,/ }; do
  info="$(gh api "repos/${GH_REPO}/actions/runs/${run}")"
  [ "$(jq -r .conclusion <<<"${info}")" = success ] || { echo "::error::run ${run} did not succeed"; exit 1; }
  while read -r name; do
    [ -n "${name}" ] && [ -z "${where[${name}]:-}" ] && where["${name}"]="${run}"
  done < <(gh api "repos/${GH_REPO}/actions/runs/${run}/artifacts?per_page=100" --jq '.artifacts[] | select(.expired | not) | .name')
done
need=(pkg-jazz-tools pkg-jazz-wasm pkg-jazz-napi pkg-jazz-rn pkg-create-jazz inspector-prebuilt
  jazz-tools-linux-x64 jazz-tools-linux-arm64 language-artifacts jazz-kotlin-maven-dry-run
  JazzFFI.xcframework jazz-swift-mirror)
missing=()
legacy="{}"
for name in "${need[@]}"; do
  if [ -z "${where[${name}]:-}" ]; then missing+=("${name}"); continue; fi
  legacy="$(jq -c --arg k "${name}" --arg v "${where[${name}]}" '. + {($k): $v}' <<<"${legacy}")"
done
if [ "${#missing[@]}" -gt 0 ]; then
  echo "::error::The legacy runs lack: ${missing[*]}"
  exit 1
fi
# The npm packages were built from the preview run's head, which must have
# this tree (as publish-jazz-tools-alpha.yml checks today).
npm_run="${where[pkg-jazz-tools]}"
source_sha="$(gh api "repos/${GH_REPO}/actions/runs/${npm_run}" --jq .head_sha)"
source_tree="$(gh api "repos/${GH_REPO}/git/commits/${source_sha}" --jq .tree.sha)"
if [ "${source_tree}" != "${tree}" ]; then
  echo "::error::run ${npm_run} built ${source_sha} (tree ${source_tree}), not tree ${tree}"
  exit 1
fi
echo "legacy artifacts: ${legacy}"
{
  echo "mode=legacy"
  echo "run_id="
  echo "source_sha=${source_sha}"
  echo "inspector_run_id=${where[inspector-prebuilt]}"
  echo "legacy=${legacy}"
} >> "${out}"
