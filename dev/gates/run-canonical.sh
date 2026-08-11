#!/usr/bin/env bash
# Run the canonical core gates and preserve their direct statuses in TSV output.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

usage() {
  echo "Usage: dev/gates/run-canonical.sh [--only gate-id] [--output-dir directory]" >&2
}

only=""
output_dir=""
while (($# > 0)); do
  case "$1" in
    --only)
      (($# >= 2)) || { usage; exit 2; }
      only="$2"
      shift 2
      ;;
    --output-dir)
      (($# >= 2)) || { usage; exit 2; }
      output_dir="$2"
      shift 2
      ;;
    -h|--help) usage; exit 0 ;;
    *) usage; exit 2 ;;
  esac
done

for required in git cargo pnpm mktemp date env; do
  command -v "$required" >/dev/null 2>&1 || {
    echo "run-canonical: required command not found: $required" >&2
    exit 127
  }
done

sha="$(git rev-parse HEAD)"
if [[ -n "$(git status --porcelain)" ]]; then
  tree_clean=false
else
  tree_clean=true
fi

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
if [[ -z "$output_dir" ]]; then
  output_dir="target/gate-reports"
fi
mkdir -p "$output_dir"
tsv="$output_dir/canonical-$timestamp-$sha.tsv"

printf 'sha\t%s\n' "$sha" >"$tsv"
printf 'tree_clean\t%s\n' "$tree_clean" >>"$tsv"
printf 'gate\texit_code\tsummary\n' >>"$tsv"

printf 'Canonical gates for %s (tree_clean=%s)\n' "$sha" "$tree_clean"
printf '%-48s %9s  %s\n' 'gate' 'exit code' 'summary'

overall=0
ran_count=0
run_gate() {
  local id="$1"
  shift
  local log status summary

  if [[ -n "$only" && "$only" != "$id" ]]; then
    return
  fi
  ((ran_count += 1))

  log="$(mktemp "${TMPDIR:-/tmp}/jazz-gate-${id}.XXXXXX")"
  printf '\n==> %s\n    ' "$id"
  printf '%q ' "$@"
  printf '\n'

  # This deliberately avoids a conditional or pipeline around the gate. The
  # status captured here is the command's direct status, including 127.
  set +e
  "$@" >"$log" 2>&1
  status=$?
  set -e
  cat "$log"
  summary=""
  while IFS= read -r line || [[ -n "$line" ]]; do
    if [[ -n "${line//[[:space:]]/}" ]]; then
      summary="${line//$'\t'/ }"
    fi
  done <"$log"
  [[ -n "$summary" ]] || summary="no output"
  printf '%-48s %9d  %s\n' "$id" "$status" "$summary"
  printf '%s\t%d\t%s\n' "$id" "$status" "$summary" >>"$tsv"
  rm -f "$log"

  if ((status != 0)); then
    overall=1
  fi
}

run_gate cargo-test-jazz cargo test -p jazz
run_gate cargo-test-groove cargo test -p groove
run_gate cargo-test-jazz-no-default-features cargo test -p jazz --no-default-features --features test
run_gate cargo-check-jazz-sim-benches cargo check -p jazz-sim --benches
run_gate ts-wire-codec dev/gates/ts-wire-codec.sh
run_gate m3-maintained-one-shot env JAZZ_SEED_COUNT=300 cargo test -p jazz m3_maintained_one_shot_differential_oracle
run_gate incremental-delivery-canary cargo test -p jazz --test incremental_delivery_canary maintained_relation_include_single_row_changes_are_scale_independent -- --exact

if [[ -n "$only" ]] && ((ran_count == 0)); then
  echo "run-canonical: unknown gate id: $only" >&2
  exit 2
fi

echo "TSV report: $tsv"
exit "$overall"
