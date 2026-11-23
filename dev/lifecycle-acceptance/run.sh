#!/usr/bin/env bash
# Runs the lifecycle acceptance on a candidate, until the workflow is on main: commits the candidate
# to eval/lifecycle-acceptance (which is all it pushes to), waits, and prints the verdict.
# usage: dev/lifecycle-acceptance/run.sh <candidate sha> <release-artifacts run id> [name]
#        (once on main: gh workflow run lifecycle-acceptance.yml -f sha=<sha> -f run=<run id>)
set -euo pipefail
repo=garden-co/jazz
branch=eval/lifecycle-acceptance
file=dev/lifecycle-acceptance/candidates.json
sha=${1:?usage: run.sh <candidate sha> <release-artifacts run id> [name]}
run=${2:?usage: run.sh <candidate sha> <release-artifacts run id> [name]}
name=${3:-candidate-${sha:0:8}}
[[ $sha =~ ^[0-9a-f]{40}$ ]] || { echo "the sha must be a full commit sha" >&2; exit 2; }
[[ $run =~ ^[0-9]+$ ]] || { echo "the run must be a run id" >&2; exit 2; }
[[ $name =~ ^[a-z0-9][a-z0-9-]{0,30}$ ]] || { echo "the name must be lowercase letters, digits and -" >&2; exit 2; }
# The run has to have built the sha. CI checks the same again, fail closed.
built=$(gh api "repos/$repo/actions/runs/$run" --jq .head_sha)
[ "$built" = "$sha" ] || { echo "run $run built $built, not $sha" >&2; exit 2; }

# beta.0 stays beside the candidate as the negative control.
list=$(gh api "repos/$repo/contents/$file?ref=$branch" --jq .content | base64 -d |
  node -e '
    const [name, sha, run] = process.argv.slice(1);
    let text = "";
    process.stdin.on("data", (d) => (text += d)).on("end", () => {
      const controls = JSON.parse(text).filter((c) => c.mustFail?.length && c.name !== name);
      const all = [...controls, { name, sha, run }];
      process.stdout.write(`[\n${all.map((c) => `  ${JSON.stringify(c)}`).join(",\n")}\n]\n`);
    });' "$name" "$sha" "$run")
blob=$(gh api "repos/$repo/contents/$file?ref=$branch" --jq .sha)
# The trailer makes the commit differ when the same candidate runs again.
commit=$(gh api -X PUT "repos/$repo/contents/$file" \
  -f branch="$branch" -f sha="$blob" \
  -f message="ci(lifecycle): run on $sha (release-artifacts $run) at $(date -u +%FT%TZ)" \
  -f content="$(printf '%s\n' "$list" | base64 -w0)" --jq .commit.sha)
echo "pushed $commit to $branch"

id=
for _ in $(seq 30); do
  id=$(gh run list -R "$repo" --workflow lifecycle-acceptance.yml --branch "$branch" --commit "$commit" --json databaseId -q '.[0].databaseId' || true)
  [ -n "$id" ] && break
  sleep 5
done
[ -n "$id" ] || { echo "no run started for $commit" >&2; exit 1; }
echo "https://github.com/$repo/actions/runs/$id"
gh run watch "$id" -R "$repo" --interval 30 >/dev/null 2>&1 || true
[ "$(gh run view "$id" -R "$repo" --json status -q .status)" = completed ] || { echo "run $id hasn't finished" >&2; exit 1; }
job=$(gh api "repos/$repo/actions/runs/$id/jobs?per_page=100" --jq '.jobs[]|select(.name=="verdict").id')
[ -n "$job" ] || { echo "run $id has no verdict job" >&2; exit 1; }
lines=$(gh api --allow-escape-sequences "repos/$repo/actions/jobs/$job/logs" | sed -E 's/^[^ ]+ //' |
  grep -E '^(PASS|FAIL|KNOWN_ISSUE|NOT_COVERED|CONTROL_OK|CONTROL_BROKEN|BLOCKING|KNOWN_ISSUES|NOTE|VERSION_OK|VERSION_WRONG|VERDICT) ')
# Blocking failures and broken controls first.
printf '%s\n' "$lines" | grep -E '^(CONTROL_BROKEN|VERSION_WRONG|BLOCKING|VERDICT) ' || true
echo ---
printf '%s\n' "$lines"
gh api "repos/$repo/actions/runs/$id" --jq '"wall time \((.updated_at|fromdate) - (.run_started_at|fromdate)) s"'
# The control is red on purpose: the exit status is the candidate's, if every control held.
if printf '%s\n' "$lines" | grep -q '^CONTROL_BROKEN'; then exit 1; fi
printf '%s\n' "$lines" | grep -q "^VERDICT $name PASS:"
