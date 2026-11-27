#!/usr/bin/env bash
# Marks-only timing runs of every variant (run from the repo root).
# Usage: results/2026-11-14/thin-startup/measure-all.sh [runs]
set -euo pipefail
RUNS=${1:-11}
D=results/2026-11-14/thin-startup
for v in marks fix-w4 fix-mint fix-socket fix-pipe fix-all; do
  node $D/patch/apply.mjs $v >/dev/null
  node $D/thin-reload.mjs --runs "$RUNS" --label "$v" --jazz-tools "jazz-tools-$v" --no-seed > $D/runs/$v.txt 2> $D/runs/$v.log
  echo "$v: $(grep dom:cards-100 $D/runs/$v.txt)"
done
