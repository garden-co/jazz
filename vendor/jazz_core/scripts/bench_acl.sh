#!/usr/bin/env bash
set -euo pipefail

# Emits one JSON line per ACL scenario run. Defaults to groove_prepared only so
# normal engine recaptures can reuse frozen SQLite comparison rows.
series_list="${GROOVE_BENCH_ACL_SERIES:-insert delete}"
engines="${GROOVE_BENCH_ENGINES:-groove_prepared}"
repetitions="${GROOVE_BENCH_REPETITIONS:-1}"

if [[ "${1:-}" == "--with-sqlite" ]]; then
  engines="${GROOVE_BENCH_ENGINES:-groove_prepared sqlite_indexed}"
elif [[ "${1:-}" == "--full-v0" ]]; then
  engines="${GROOVE_BENCH_ENGINES:-groove sqlite_indexed}"
elif [[ $# -gt 0 ]]; then
  echo "usage: scripts/bench_acl.sh [--with-sqlite|--full-v0]" >&2
  exit 2
fi

for repetition in $(seq 1 "$repetitions"); do
  for series in $series_list; do
    for engine in $engines; do
      GROOVE_SCENARIO=acl \
        GROOVE_ENGINE="$engine" \
        GROOVE_ACL_SERIES="$series" \
        GROOVE_BENCH_REPETITION="$repetition" \
        scripts/bench_run.py
    done
  done
done
