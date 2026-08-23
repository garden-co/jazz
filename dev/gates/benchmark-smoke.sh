#!/usr/bin/env bash
# Compile only the benchmark target being edited.  This is deliberately not a
# performance command: CodSpeed owns regression comparisons and CI owns the
# deterministic scenario assertions.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  dev/gates/benchmark-smoke.sh <jazz|jazz-sim> <bench>
  dev/gates/benchmark-smoke.sh --ci
  dev/gates/benchmark-smoke.sh --compile-ci

The local form performs one debug-profile `cargo check` only. It never runs
`cargo bench`, never writes a timing ledger, and does not request release
RocksDB artifacts. `--ci` executes deterministic correctness assertions on
ordinary PR CI. `--compile-ci` checks all maintained benchmark APIs on the
realistic benchmark workflow, where compile/performance coverage already lives.
EOF
}

run_phase() {
  local phase="$1"
  shift
  local started_seconds=$SECONDS
  local status=0
  if "$@"; then
    status=0
  else
    status=$?
  fi
  printf 'benchmark-smoke phase=%s duration_seconds=%s status=%s\n' \
    "$phase" "$((SECONDS - started_seconds))" "$status"
  return "$status"
}

if [[ "${1:-}" == "--ci" && $# == 1 ]]; then
  run_phase legacy-benchmark-correctness \
    cargo test -p jazz --features testing --test legacy_benchmark_smoke
  run_phase jazz-sim-scenario-correctness cargo test -p jazz-sim --test scenario_smoke
  exit 0
fi

if [[ "${1:-}" == "--compile-ci" && $# == 1 ]]; then
  run_phase jazz-benchmark-api cargo check -p jazz --benches --features testing
  run_phase jazz-sim-benchmark-api cargo check -p jazz-sim --benches
  exit 0
fi

if [[ $# != 2 || ( "$1" != "jazz" && "$1" != "jazz-sim" ) ]]; then
  usage >&2
  exit 2
fi

required_features="$(
  cargo metadata --no-deps --format-version 1 |
    jq -r --arg package "$1" --arg bench "$2" \
      '.packages[] | select(.name == $package) | .targets[] | select(.name == $bench and (.kind | index("bench"))) | .["required-features"][]?'
)"
if [[ -n "$required_features" ]]; then
  feature_csv="$(paste -sd, <<<"$required_features")"
  cargo check -p "$1" --features "$feature_csv" --bench "$2"
else
  cargo check -p "$1" --bench "$2"
fi
