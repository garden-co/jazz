#!/usr/bin/env bash
# Marks-only runs at an added round-trip latency (run from the repo root).
# Usage: results/2026-11-14/thin-startup/measure-rtt.sh [rtt-ms] [runs]
set -euo pipefail
RTT=${1:-40}; RUNS=${2:-7}
D=results/2026-11-14/thin-startup
for v in marks fix-mint fix-socket fix-pipe fix-all; do
  node $D/patch/apply.mjs $v >/dev/null
  node $D/thin-reload.mjs --runs "$RUNS" --rtt-ms "$RTT" --label "rtt$RTT-$v" --jazz-tools "jazz-tools-$v" --no-seed > $D/runs/rtt$RTT-$v.txt 2> $D/runs/rtt$RTT-$v.log
  echo "$v: $(grep dom:cards-100 $D/runs/rtt$RTT-$v.txt)"
done
