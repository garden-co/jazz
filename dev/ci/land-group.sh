#!/usr/bin/env bash
# Lands a green Review Loop merge group on main, PR by PR, in order.
#
# usage: land-group.sh <final-sha> <label>      (env: GH_TOKEN, REPO)
#
# This is the Review Loop's own landing sequence (merge_one.sh/merge_any.sh),
# run by CI as soon as the group's suite and full-suite statuses are posted,
# instead of by an agent polling for the result. Per PR, in group order:
#   - main's tree must equal the tree before this PR (the group's base, or the
#     previous PR's): if main moved any other way, stop and merge nothing more;
#   - the PR's head must still be the sha the group merged;
#   - it must carry the Review Loop's approval comment at that exact sha and a
#     github-actions full-suite success status (the ruleset requires it). The
#     approval is any OWNER/MEMBER/COLLABORATOR comment whose first line is
#     "Approved by <who> ... at <full head sha>": the same trust the Review
#     Loop's own landing applies today;
#   - merge it with a merge commit pinned to that sha (merge-async with
#     direct_merge for stacked-base PRs), then require main's tree to equal the
#     group's tree after that PR.
# The first failure stops the run; nothing after it is merged. A rerun resumes
# after the PRs that already landed. It never posts a status or a comment.
set -euo pipefail

final="$1"
label="$2"
: "${REPO:?}" "${GH_TOKEN:?}"
wait_seconds="${LAND_WAIT_SECONDS:-2700}"
# Pause between the 15 reads of main's tree after a merge (30 s in total by
# default). Tests set 0 so a merge that never shows its tree fails at once.
tree_poll_seconds="${LAND_TREE_POLL_SECONDS:-2}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

members="$("${here}/group-members.sh" "${final}" "${label}")"
base_tree="$(awk '$1 == "base" { print $3 }' <<<"${members}")"
mapfile -t prs < <(awk '$1 == "pr" { print $2, $3, $4 }' <<<"${members}")

main_tree() { gh api "repos/${REPO}/branches/main" --jq .commit.commit.tree.sha; }

# Groups are cut on the previous group's FINAL, so this one can go green before
# the previous one has landed: wait until main reaches this group's base, or a
# point inside it when a previous attempt landed some of its PRs. This wait is
# what serialises stacked groups: a later group's base only appears on main
# once every PR before it has landed, so two jobs never merge interleaved.
start=-1
deadline=$((SECONDS + wait_seconds))
while :; do
  tree="$(main_tree)"
  if [ "${tree}" = "${base_tree}" ]; then start=0; break; fi
  for i in "${!prs[@]}"; do
    read -r _ _ t <<<"${prs[$i]}"
    if [ "${tree}" = "${t}" ]; then start=$((i + 1)); break 2; fi
  done
  if [ "${SECONDS}" -ge "${deadline}" ]; then
    echo "::error::main's tree ${tree} never reached ${label}'s base ${base_tree}; not landing"
    exit 1
  fi
  echo "waiting for main to reach ${label}'s base (main tree ${tree:0:8}, base ${base_tree:0:8})"
  sleep 10
done
if [ "${start}" -ge "${#prs[@]}" ]; then
  echo "${label} is already on main"
  exit 0
fi

expect_main_tree() {
  local want="$1" have=""
  for _ in $(seq 1 15); do
    have="$(main_tree)"
    [ "${have}" = "${want}" ] && return 0
    sleep "${tree_poll_seconds}"
  done
  echo "::error::main's tree is ${have}, expected ${want}"
  return 1
}

# Index of the last PR whose post-merge tree main is on now, or -1. A PR that
# contains later PRs' heads (a stack's top) lands them all at once: GitHub then
# marks those PRs merged itself, and main is already past them.
landed_through() {
  local tree t k found=-1
  tree="$(main_tree)"
  for k in "${!prs[@]}"; do
    read -r _ _ t <<<"${prs[$k]}"
    [ "${tree}" = "${t}" ] && found="${k}"
  done
  echo "${found}"
}

for ((i = start; i < ${#prs[@]}; i++)); do
  reached="$(landed_through)"
  if [ "${reached}" -ge "${i}" ]; then
    echo "main already includes the group through PR $((reached + 1)) of ${#prs[@]}; skipping ahead"
    i="${reached}"
    continue
  fi
  read -r pr sha tree <<<"${prs[$i]}"
  echo "::group::#${pr} at ${sha:0:8}"
  pull="$(gh api "repos/${REPO}/pulls/${pr}")"
  if [ "$(jq -r .merged <<<"${pull}")" = true ]; then
    echo "::error::#${pr} is already merged but main's tree does not show it; stopping"
    exit 1
  fi
  head="$(jq -r .head.sha <<<"${pull}")"
  if [ "${head}" != "${sha}" ]; then
    echo "::error::#${pr}'s head moved to ${head} after ${label} was cut at ${sha}; stopping"
    exit 1
  fi
  approvals="$(gh api --paginate "repos/${REPO}/issues/${pr}/comments" --jq \
    "[.[] | select((.author_association == \"OWNER\" or .author_association == \"MEMBER\" or .author_association == \"COLLABORATOR\") and ((.body | split(\"\n\")[0]) | test(\"^Approved by .+ at ${sha}\$\")))] | length" |
    awk '{ n += $1 } END { print n + 0 }')"
  if [ "${approvals}" -eq 0 ]; then
    echo "::error::#${pr} has no Review Loop approval comment at ${sha}; stopping"
    exit 1
  fi
  suite="$(gh api "repos/${REPO}/commits/${sha}/statuses?per_page=100" --jq \
    '[.[] | select(.context == "full-suite" and .creator.login == "github-actions[bot]")][0].state // ""')"
  if [ "${suite}" != success ]; then
    echo "::error::#${pr} at ${sha} has no full-suite success status; stopping"
    exit 1
  fi
  if ! out="$(gh api -X PUT "repos/${REPO}/pulls/${pr}/merge" -f merge_method=merge -f sha="${sha}" 2>&1)"; then
    if ! grep -q "stacked PRs" <<<"${out}"; then
      echo "::error::#${pr} did not merge: ${out}"
      exit 1
    fi
    uuid="$(gh api -X PUT "repos/${REPO}/pulls/${pr}/merge-async" -f sha="${sha}" \
      -f merge_action=direct_merge -f merge_method=merge --jq .details.uuid)"
    status=""
    for _ in $(seq 1 45); do
      sleep 4
      status="$(gh api "repos/${REPO}/pulls/${pr}/merge-async/${uuid}" --jq .status)"
      [ "${status}" = merged ] && break
      if [ "${status}" = failed ]; then echo "::error::#${pr} async merge failed"; exit 1; fi
    done
    if [ "${status}" != merged ]; then echo "::error::#${pr} async merge did not finish ($status)"; exit 1; fi
  fi
  expect_main_tree "${tree}"
  echo "#${pr} landed; main's tree is ${tree:0:8}"
  echo "::endgroup::"
done
echo "${label} landed: ${#prs[@]} PR(s)"
