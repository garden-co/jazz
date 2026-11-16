#!/usr/bin/env bash
# Runs the language clients against the t9 backend (oidc.ts on 8790/8791 + server.ts on 8780 already up).
# Usage: RUST_BIN=<target/debug/stage-plan> KT_CLI=<.../stage-plan-kotlin-cli> SW_BIN=<.build/debug/stage-plan-swift> APP_ID=<served app id> ./run-lang-clients.sh
set -u; EP=http://127.0.0.1:8780/jazz; B=http://127.0.0.1:8780; W=http://127.0.0.1:8790; U=http://127.0.0.1:8791
APP_ID=${APP_ID:?app id the backend serves}; fails=0
wtok() { curl -s "$W/token?sub=$1"; }            # workload token (aud jazz:stageplan-gate)
utok() { curl -s "$U/token?sub=$1&aud=jazz:stageplan-users"; }  # user token
curl -s -X POST $B/account/user-a >/dev/null; curl -s -X POST $B/account/user-b >/dev/null
judge() { # $1 label, stdin JSON lines; pass iff every line ok:true and exit 0
  local out rc; out=$(cat); rc=${PIPESTATUS[0]}; echo "$out" | sed "s/^/  [$1] /"
  if echo "$out" | grep -q '"ok":false' || [ -z "$out" ]; then echo "FAIL $1"; fails=$((fails+1)); else echo "PASS $1"; fi; }
if [ -n "${RUST_BIN:-}" ]; then
  echo "== Rust backend client: scoped"
  A_SHOW=${A_SHOW:-}
  JAZZ_SERVER_URL=$EP JAZZ_APP_ID=$APP_ID JAZZ_TOKEN_COMMAND="curl -s '$W/token?sub=gate-scoped-agent'" STAGE_PLAN_MODE=scoped ${A_SHOW:+STAGE_PLAN_FOREIGN_SHOW=$A_SHOW} timeout 120 $RUST_BIN gate 2>/tmp/rust-scoped.err | judge rust-scoped
  echo "== Rust backend client: trusted"
  JAZZ_SERVER_URL=$EP JAZZ_APP_ID=$APP_ID JAZZ_TOKEN_COMMAND="curl -s '$W/token?sub=gate-trusted-job'" STAGE_PLAN_MODE=trusted STAGE_PLAN_USER_A=user-a STAGE_PLAN_USER_B=user-b timeout 120 $RUST_BIN gate 2>/tmp/rust-trusted.err | judge rust-trusted
fi
if [ -n "${KT_CLI:-}" ]; then
  echo "== Kotlin CLI (bearer users)"; TA=$(utok kt-alice); TB=$(utok kt-bob); D=$(mktemp -d)
  o=$(JAZZ_ENDPOINT=$EP JAZZ_APP_ID=$APP_ID JAZZ_TOKEN=$TA JAZZ_DATA_DIR=$D/a timeout 120 $KT_CLI "Alice's show" add Secret setlist 2>&1); echo "$o" | sed 's/^/  [kt A add] /'
  SID=$(echo "$o" | grep -o "Alice's show ([0-9a-f-]*)" | grep -o "[0-9a-f-]\{36\}"); TID=$(echo "$o" | grep -o "added [0-9a-f-]*" | awk '{print $2}')
  [ -n "$SID" ] && [ -n "$TID" ] && echo "PASS kotlin A writes show+task" || { echo "FAIL kotlin A writes"; fails=$((fails+1)); }
  JAZZ_ENDPOINT=$EP JAZZ_APP_ID=$APP_ID JAZZ_TOKEN=$TB JAZZ_DATA_DIR=$D/b timeout 120 $KT_CLI --show "$SID" tasks > /tmp/kt-b.out 2>&1; rc=$?; sed 's/^/  [kt B read] /' /tmp/kt-b.out
  [ $rc = 3 ] && echo "PASS kotlin B can't see A's show (exit 3)" || { echo "FAIL kotlin B read exit $rc"; fails=$((fails+1)); }
  JAZZ_ENDPOINT=$EP JAZZ_APP_ID=$APP_ID JAZZ_TOKEN=$TB JAZZ_DATA_DIR=$D/b timeout 120 $KT_CLI "B's show" move "$TID" done > /tmp/kt-bw.out 2>&1; rc=$?; sed 's/^/  [kt B move] /' /tmp/kt-bw.out
  [ $rc = 4 ] && echo "PASS kotlin B can't move A's task (exit 4)" || { echo "FAIL kotlin B move exit $rc"; fails=$((fails+1)); }
fi
if [ -n "${SW_BIN:-}" ]; then
  echo "== Swift CLI (bearer users)"; TA=$(utok sw-alice); TB=$(utok sw-bob)
  TID=$(JAZZ_ENDPOINT=$EP JAZZ_APP_ID=$APP_ID JAZZ_BEARER=$TA timeout 120 $SW_BIN --show Private add "Load in" 2>&1 | tail -1); echo "  [sw A add] $TID"
  JAZZ_ENDPOINT=$EP JAZZ_APP_ID=$APP_ID JAZZ_BEARER=$TA timeout 120 $SW_BIN --show Private list 2>&1 | sed 's/^/  [sw A list] /'
  SID=${SW_SHOW_ID:-}   # set if the CLI prints the show id; else read via the TS reference client
  if [ -n "$SID" ]; then
    n=$(JAZZ_ENDPOINT=$EP JAZZ_APP_ID=$APP_ID JAZZ_BEARER=$TB timeout 120 $SW_BIN --show-id "$SID" list 2>&1 | grep -c "Load in"); [ "$n" = 0 ] && echo "PASS swift B reads none of A's tasks" || { echo "FAIL swift B read"; fails=$((fails+1)); }
    JAZZ_ENDPOINT=$EP JAZZ_APP_ID=$APP_ID JAZZ_BEARER=$TB timeout 120 $SW_BIN --show-id "$SID" add "intrude" > /tmp/sw-bw.out 2>&1; rc=$?; sed 's/^/  [sw B add] /' /tmp/sw-bw.out
    [ $rc = 1 ] && grep -q "permission denied" /tmp/sw-bw.out && echo "PASS swift B write refused" || { echo "FAIL swift B write exit $rc"; fails=$((fails+1)); }
  else echo "SKIP swift by-id checks: set SW_SHOW_ID"; fi
fi
echo; [ $fails = 0 ] && echo "LANG CLIENTS PASS" || echo "LANG CLIENTS FAIL ($fails)"
exit $fails
