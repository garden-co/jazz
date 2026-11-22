#!/usr/bin/env bash
# Tests fetch-rc.sh's producer-run check (#supply-chain's rule) against a stubbed gh: no network.
# usage: dev/adopter-gate/provenance.test.sh   (exits 1 if any case is wrong)
AG=$(cd "$(dirname "$0")" && pwd); T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
GOOD=a2b23927b2a2c6cac4a550ffe293fff0eb7ed396; OTHER=5deb65d281caeb87a5f22a94d0de957695ffd108
# The stub answers `gh api repos/<repo>/actions/runs/<id> --jq …` with $T/run-<id> (head_sha event repo path) and
# fails everything else, so a run that passes the check stops at the first download.
mkdir -p "$T/bin"; cat > "$T/bin/gh" <<'STUB'
#!/usr/bin/env bash
[ "$1" = api ] && [[ "$2" =~ /actions/runs/([0-9]+)$ ]] && [ -f "$STUB_DIR/run-${BASH_REMATCH[1]}" ] && exec cat "$STUB_DIR/run-${BASH_REMATCH[1]}"
exit 1
STUB
chmod +x "$T/bin/gh"
run() { echo "$2" > "$T/run-1"; echo "$GOOD push garden-co/jazz .github/workflows/release-candidate.yml" > "$T/run-2"
  out=$(PATH="$T/bin:$PATH" STUB_DIR=$T GITHUB_REPOSITORY=garden-co/jazz AG_TGZ=$T/tgz AG_NEED=npm AG_EXPECT_SHA=${3-$GOOD} \
    "$AG/fetch-rc.sh" --run 1 --npm-run 2 2>&1 | grep -m1 "provenance")
  case "$out" in $4*) echo "ok   $1";; *) echo "FAIL $1: got '$out'"; bad=1;; esac; }
bad=
run "matching push run passes"            "$GOOD push garden-co/jazz .github/workflows/release-artifacts.yml" "$GOOD" "provenance: run 1 built"
run "matching dispatch run passes"        "$GOOD workflow_dispatch garden-co/jazz .github/workflows/release-candidate.yml" "$GOOD" "provenance: run 1 built"
run "other commit refused"                "$OTHER push garden-co/jazz .github/workflows/release-artifacts.yml" "$GOOD" "FAIL provenance: run 1 built $OTHER"
run "PR-event run with matching sha refused" "$GOOD pull_request garden-co/jazz .github/workflows/release-artifacts.yml" "$GOOD" "FAIL provenance: run 1 is a 'pull_request' run"
run "pull_request_target refused"         "$GOOD pull_request_target garden-co/jazz .github/workflows/release-artifacts.yml" "$GOOD" "FAIL provenance: run 1 is a 'pull_request_target' run"
run "fork run refused"                    "$GOOD push someone/jazz .github/workflows/release-artifacts.yml" "$GOOD" "FAIL provenance: run 1 built from 'someone/jazz'"
run "other workflow refused"              "$GOOD push garden-co/jazz .github/workflows/ci.yml" "$GOOD" "FAIL provenance: run 1 is from '.github/workflows/ci.yml'"
run "no expected sha refused"             "$GOOD push garden-co/jazz .github/workflows/release-artifacts.yml" "" "FAIL provenance: AG_EXPECT_SHA must be"
run "short expected sha refused"          "$GOOD push garden-co/jazz .github/workflows/release-artifacts.yml" "a2b23927" "FAIL provenance: AG_EXPECT_SHA must be"
run "empty API answer refused"           "" "$GOOD" "FAIL provenance: run 1 built"
[ -z "$bad" ]
