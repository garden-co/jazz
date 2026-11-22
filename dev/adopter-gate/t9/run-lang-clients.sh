#!/usr/bin/env bash
# Runs the language clients against the t9 backend (oidc.ts on 8790/8791 + server.ts on 8780 already up).
# Usage: RUST_BIN=<target/debug/stage-plan> KT_CLI=<.../stage-plan-kotlin-cli> SW_BIN=<.build/debug/stage-plan-swift> APP_ID=<served app id> ./run-lang-clients.sh
set -u; EP=http://127.0.0.1:8780/jazz; B=http://127.0.0.1:8780; W=http://127.0.0.1:8790; U=http://127.0.0.1:8791
APP_ID=${APP_ID:?app id the backend serves}; fails=0
wtok() { curl -s "$W/token?sub=$1"; }            # workload token (aud jazz:stageplan-gate)
utok() { curl -s "$U/token?sub=$1&aud=jazz:stageplan-users"; }  # user token
curl -s -X POST $B/account/user-a >/dev/null; curl -s -X POST $B/account/user-b >/dev/null
judge() { # $1 label, $2 exit code, $3 JSON lines; pass iff exit 0, output non-empty, no ok:false
  echo "$3" | sed "s/^/  [$1] /"
  if [ "$2" -ne 0 ] || [ -z "$3" ] || echo "$3" | grep -q '"ok":false'; then echo "FAIL $1 (exit $2)"; fails=$((fails+1)); else echo "PASS $1"; fi; }
if [ -n "${RUST_BIN:-}" ]; then
  echo "== Rust backend client: trusted (system write/read, forUser A writes+reads, forUser B can't read or write)"
  o=$(JAZZ_SERVER_URL=$EP JAZZ_APP_ID=$APP_ID JAZZ_TOKEN_COMMAND="curl -s '$W/token?sub=gate-trusted-job'" STAGE_PLAN_MODE=trusted STAGE_PLAN_USER_A=user-a STAGE_PLAN_USER_B=user-b timeout 120 $RUST_BIN gate 2>/tmp/rust-trusted.err); rc=$?
  judge rust-trusted $rc "$o"
  echo "$o" | grep '"step":"for_user_read"' | grep '"user":"user-b"' | grep -q '"visible":false' && echo "PASS rust-trusted B can't see A" || { echo "FAIL rust-trusted B visibility line missing"; fails=$((fails+1)); }
  A_SHOW=$(echo "$o" | grep '"step":"for_user_write"' | grep '"user":"user-a"' | grep -o '"show":"[^"]*"' | head -1 | cut -d'"' -f4)
  echo "== Rust backend client: scoped (own show write/read; A's show $A_SHOW invisible and unwritable)"
  [ -n "$A_SHOW" ] || { echo "FAIL no A show id from trusted run"; fails=$((fails+1)); }
  o=$(env JAZZ_SERVER_URL=$EP JAZZ_APP_ID=$APP_ID JAZZ_TOKEN_COMMAND="curl -s '$W/token?sub=gate-scoped-agent'" STAGE_PLAN_MODE=scoped ${A_SHOW:+STAGE_PLAN_FOREIGN_SHOW=$A_SHOW} timeout 120 $RUST_BIN gate 2>/tmp/rust-scoped.err); rc=$?
  judge rust-scoped $rc "$o"
  for st in foreign_read foreign_write; do echo "$o" | grep "\"step\":\"$st\"" | grep -q '"ok":true' && echo "PASS rust-scoped $st" || { echo "FAIL rust-scoped $st missing"; fails=$((fails+1)); }; done
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
  echo "== Swift CLI (bearer users)"; TA=$(utok sw-alice-$RANDOM); TB=$(utok sw-bob-$RANDOM)
  o=$(JAZZ_ENDPOINT=$EP JAZZ_APP_ID=$APP_ID JAZZ_BEARER=$TA timeout 120 $SW_BIN --show Private add "Secret" 2>&1); echo "$o" | sed 's/^/  [sw A add] /'
  TID=$(echo "$o" | grep -oE "^[0-9a-f-]{36}" | head -1); SID=$(echo "$o" | grep -oE "^show [0-9a-f-]{36}" | awk '{print $2}')
  [ -n "$TID" ] && [ -n "$SID" ] && echo "PASS swift A writes show+task" || { echo "FAIL swift A add"; fails=$((fails+1)); }
  n=$(JAZZ_ENDPOINT=$EP JAZZ_APP_ID=$APP_ID JAZZ_BEARER=$TA timeout 120 $SW_BIN --show-id "$SID" list 2>&1 | tee /tmp/sw-a.out | grep -c "Secret"); sed 's/^/  [sw A list] /' /tmp/sw-a.out
  [ "$n" -ge 1 ] && echo "PASS swift A reads her task (new process, remote)" || { echo "FAIL swift A read"; fails=$((fails+1)); }
  JAZZ_ENDPOINT=$EP JAZZ_APP_ID=$APP_ID JAZZ_BEARER=$TB timeout 120 $SW_BIN --show-id "$SID" list > /tmp/sw-b.out 2>&1; brc=$?; n=$(grep -c "Secret" /tmp/sw-b.out); sed 's/^/  [sw B list] /' /tmp/sw-b.out
  [ "$brc" = 0 ] && [ "$n" = 0 ] && [ -n "$SID" ] && echo "PASS swift B reads none of A's tasks" || { echo "FAIL swift B read (exit $brc, matches $n)"; fails=$((fails+1)); }
  JAZZ_ENDPOINT=$EP JAZZ_APP_ID=$APP_ID JAZZ_BEARER=$TB timeout 120 $SW_BIN --show-id "$SID" add "Sneaky" > /tmp/sw-bw.out 2>&1; rc=$?; sed 's/^/  [sw B add] /' /tmp/sw-bw.out
  [ $rc = 1 ] && grep -q "permission denied" /tmp/sw-bw.out && echo "PASS swift B write refused" || { echo "FAIL swift B write exit $rc"; fails=$((fails+1)); }
fi
echo; [ $fails = 0 ] && echo "LANG CLIENTS PASS" || echo "LANG CLIENTS FAIL ($fails)"
exit $fails
