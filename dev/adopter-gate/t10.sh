#!/usr/bin/env bash
# t10: revokeUser across backend instances on one core (jazz#5634) + Db lifecycle (jazz#5592), with the RC's core binary.
. "$(dirname "$0")/lib.sh"
body() { local W=$AG/.work/t10; rm -rf "$W"; cp -r "$AG/t10" "$W"; cd "$W" && use_rc_pnpm
  npx jazz-tools generate > gen.log 2>&1 || { echo "FAIL t10 generate"; tail -5 gen.log; }
  ./run.sh "$AG_TGZ/jazz-tools-linux-x64" 2>&1 | grep -E "^(PASS|FAIL|INSTANCES)" | sed 's/^INSTANCES FAILED/FAIL instances start/'; }
ag_job t10 body
