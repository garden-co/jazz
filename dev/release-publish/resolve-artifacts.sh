#!/usr/bin/env bash
# Finds the prebuilt artifacts for a release, for release-publish.yml.
#
#   SHA=<commit> VERSION=<version> [ARTIFACTS_RUN_ID=<id>] [LEGACY_RUN_IDS="<id> <id> …"] \
#     dev/release-publish/resolve-artifacts.sh
#
# Prefers a successful release-artifacts.yml run whose release-manifest has
# this commit's tree and version: a run on <sha> itself, or the run on a
# `release` push that predicted the Version Packages tree (its manifest's
# `tree`). ARTIFACTS_RUN_ID names one run instead of searching.
#
# Until release-artifacts.yml exists, LEGACY_RUN_IDS names the runs of the
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

manifest_matches() { # <run id> -> prints source_sha when its manifest matches
  local dir
  dir="$(mktemp -d)"
  gh run download "$1" -R "${GH_REPO}" -n release-manifest -D "${dir}" >/dev/null 2>&1 || return 1
  local m="${dir}/manifest.json"
  [ -f "${m}" ] || return 1
  [ "$(jq -r .tree "${m}")" = "${tree}" ] || return 1
  [ "$(jq -r .version "${m}")" = "${VERSION}" ] || return 1
  jq -r '.source_sha // .sha' "${m}"
}

candidates=()
if [ -n "${ARTIFACTS_RUN_ID:-}" ]; then
  candidates=("${ARTIFACTS_RUN_ID}")
elif runs="$(gh api "repos/${GH_REPO}/actions/workflows/release-artifacts.yml/runs?status=success&per_page=50" 2>/dev/null)"; then
  mapfile -t candidates < <(jq -r --arg sha "${SHA}" '.workflow_runs[]
    | select(.head_sha == $sha or .head_branch == "release" or (.head_branch | startswith("changeset-release/")))
    | .id' <<<"${runs}")
fi
for run in "${candidates[@]}"; do
  if source_sha="$(manifest_matches "${run}")"; then
    echo "release-artifacts run ${run} (built from ${source_sha}) matches"
    {
      echo "mode=release-artifacts"
      echo "run_id=${run}"
      echo "source_sha=${source_sha}"
      echo "inspector_run_id=${run}"
      echo "legacy={}"
    } >> "${out}"
    exit 0
  fi
done
if [ -n "${ARTIFACTS_RUN_ID:-}" ]; then
  echo "::error::release-artifacts run ${ARTIFACTS_RUN_ID} has no release-manifest for tree ${tree} at ${VERSION}"
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
