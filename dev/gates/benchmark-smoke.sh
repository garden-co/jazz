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

The local form performs one debug-profile `cargo check` only. It never runs
`cargo bench`, never writes a timing ledger, and does not request release
RocksDB artifacts. `--ci` checks all maintained benchmark APIs and executes
the deterministic jazz-sim scenario assertions.
EOF
}

if [[ "${1:-}" == "--ci" && $# == 1 ]]; then
  cargo check -p jazz --benches
  cargo check -p jazz-sim --benches
  cargo check -p jazz --features testing --bench route_subscription_curve
  cargo test -p jazz --features testing --test legacy_benchmark_smoke
  cargo test -p jazz-sim --test scenario_smoke
  exit 0
fi

if [[ $# != 2 || ( "$1" != "jazz" && "$1" != "jazz-sim" ) ]]; then
  usage >&2
  exit 2
fi

cargo check -p "$1" --bench "$2"
