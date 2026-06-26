#!/usr/bin/env bash
set -euo pipefail

# Emits one JSON line per run for the social-feed headline curve:
# commit latency vs active subscription count.
#
# By default this reruns only groove_prepared_sql, which is the public SQL
# prepare/bind path and the normal day-to-day engine-development path. SQLite
# numbers are retained and only need a refresh when fixture generation, SQLite
# SQL/indexes, durability, or workload knobs change. Pass --with-sqlite for a
# full recapture.
counts="${GROOVE_BENCH_SUBSCRIPTIONS:-10 100 1000 10000}"
engines="${GROOVE_BENCH_ENGINES:-groove_prepared_sql}"
repetitions="${GROOVE_BENCH_REPETITIONS:-1}"

if [[ "${1:-}" == "--with-sqlite" ]]; then
  engines="${GROOVE_BENCH_ENGINES:-groove_prepared_sql sqlite_indexed}"
elif [[ "${1:-}" == "--full-v0" ]]; then
  engines="${GROOVE_BENCH_ENGINES:-groove sqlite_indexed}"
elif [[ $# -gt 0 ]]; then
  echo "usage: scripts/bench_headline.sh [--with-sqlite|--full-v0]" >&2
  exit 2
fi

for repetition in $(seq 1 "$repetitions"); do
  for subscriptions in $counts; do
    for engine in $engines; do
      GROOVE_SCENARIO=social_feed \
        GROOVE_ENGINE="$engine" \
        GROOVE_SUBSCRIPTIONS="$subscriptions" \
        GROOVE_BENCH_REPETITION="$repetition" \
        scripts/bench_run.py
    done
  done
done
