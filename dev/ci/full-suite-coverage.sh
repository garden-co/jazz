#!/usr/bin/env bash
# Usage: dev/ci/full-suite-coverage.sh <base-sha> <head-sha>
#
# Prints, one per line, the commits a green full CI run on <head-sha> covers
# (#4091). <head-sha> itself is always covered. When <head-sha> is a
# merge-group/* branch, the PR heads it merged are covered too, but only if the
# branch's first-parent chain back to <base-sha> consists solely of clean
# merges: each merge's tree must equal git's own merge of its two parents. A
# plain commit or a hand-resolved merge on the chain changes the tested tree
# away from what merging each PR alone would produce, so then only <head-sha>
# is covered.
set -euo pipefail

base="$1"
head="$2"

echo "${head}"

merge_base="$(git merge-base "${base}" "${head}")"
heads=()
while read -r commit first second rest; do
  if [ -z "${second}" ] || [ -n "${rest}" ]; then
    echo "::notice::${commit} is not a two-parent merge; covering only ${head}" >&2
    exit 0
  fi
  if ! merged_tree="$(git merge-tree --write-tree "${first}" "${second}" | head -n 1)" ||
    [ "${merged_tree}" != "$(git rev-parse "${commit}^{tree}")" ]; then
    echo "::notice::${commit} is not a clean merge; covering only ${head}" >&2
    exit 0
  fi
  heads+=("${second}")
done < <(git rev-list --first-parent --parents "${merge_base}..${head}")

if [ "${#heads[@]}" -gt 0 ]; then
  printf '%s\n' "${heads[@]}"
fi
