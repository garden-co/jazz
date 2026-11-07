#!/usr/bin/env bash
# Usage: dev/ci/main-push-coverage.sh <main-sha>
#
# Decides whether a push to main repeats a full suite a merge group already ran.
# The Review Loop merges a green merge-group/* branch's PRs into main one by
# one, in the branch's order, each with its own merge commit. While main does
# not move in between, each of those main commits has the same tree as one
# commit on the branch's first-parent chain. Only the tip's tree was tested;
# the earlier commits are prefixes of it that the queue is about to complete,
# so their main pushes skip the suite too. If the queue stops before merging
# the whole group, it must dispatch CI on main (AGENTS.md).
#
# Prints "<branch> <tip-sha>" and exits 0 when <main-sha>'s tree equals the tree
# of a commit on a merge-group branch whose tip carries a successful
# `full-suite` status posted by GitHub Actions, the only creator the main
# ruleset accepts for it. Exits 1 otherwise, and on any error, so the caller then
# runs the full suite. Needs the merge-group branches fetched as
# refs/remotes/origin/merge-group/*, plus GH_TOKEN and REPO for `gh api`.
set -euo pipefail

head="$1"
# Bounds the walk; a merge group holds a handful of PRs.
max_group_length=200

tree="$(git rev-parse "${head}^{tree}")"

while read -r ref; do
  [ -n "${ref}" ] || continue
  tip="$(git rev-parse "${ref}^{commit}")"
  # Only commits main does not contain: the group's own merges, back to where it
  # was cut from main.
  trees="$(git log --first-parent --max-count="${max_group_length}" --format=%T "${tip}" --not "${head}")"
  grep -qx "${tree}" <<<"${trees}" || continue
  # Statuses are listed newest first; the latest one from Actions decides.
  state="$(gh api "repos/${REPO}/commits/${tip}/statuses?per_page=100" \
    --jq '[.[] | select(.context == "full-suite" and .creator.login == "github-actions[bot]")][0].state // ""' </dev/null)"
  if [ "${state}" = success ]; then
    echo "${ref#refs/remotes/origin/} ${tip}"
    exit 0
  fi
done < <(git for-each-ref --format='%(refname)' refs/remotes/origin/merge-group/)

exit 1
