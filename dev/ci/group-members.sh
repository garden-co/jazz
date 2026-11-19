#!/usr/bin/env bash
# Lists the PRs a Review Loop merge group carries, oldest first.
#
# usage: group-members.sh <final-sha> <label>
#   label: the group's commit-message label, e.g. g733 for merge-group/733.
#
# The Review Loop cuts a group as one merge commit per PR onto the previous
# group's FINAL, each with the message "Merge #<pr> (<sha8>) into <label>".
# Walking the first-parent chain from FINAL down to the first commit that is
# not such a merge gives the base and, per PR, its head sha and the tree main
# must have once that PR has landed.
#
# Output: "base <sha> <tree>", then "pr <number> <head-sha> <tree>" per PR.
# Exits non-zero (printing why) if any group commit is not a clean two-parent
# merge whose second parent matches its message.
set -euo pipefail

final="$1"
label="$2"
pattern="^Merge #([0-9]+) \(([0-9a-f]{8})\) into ${label}\$"

members=()
commit="${final}"
while :; do
  subject="$(git log -1 --format=%s "${commit}")"
  [[ "${subject}" =~ ${pattern} ]] || break
  pr="${BASH_REMATCH[1]}"
  short="${BASH_REMATCH[2]}"
  read -r _ first second rest <<<"$(git rev-list --parents -n 1 "${commit}")"
  if [ -z "${second:-}" ] || [ -n "${rest:-}" ]; then
    echo "group-members: ${commit} (#${pr}) is not a two-parent merge" >&2
    exit 1
  fi
  if [ "${second:0:8}" != "${short}" ]; then
    echo "group-members: ${commit} names ${short} but merges ${second}" >&2
    exit 1
  fi
  tree="$(git rev-parse "${commit}^{tree}")"
  if ! merged="$(git merge-tree --write-tree "${first}" "${second}" | head -n 1)" ||
    [ "${merged}" != "${tree}" ]; then
    echo "group-members: ${commit} (#${pr}) is not git's clean merge of its parents" >&2
    exit 1
  fi
  members=("pr ${pr} ${second} ${tree}" "${members[@]}")
  commit="${first}"
done

if [ "${#members[@]}" -eq 0 ]; then
  echo "group-members: ${final} carries no '${label}' merges" >&2
  exit 1
fi

echo "base ${commit} $(git rev-parse "${commit}^{tree}")"
printf '%s\n' "${members[@]}"
