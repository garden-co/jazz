#!/usr/bin/env bash
set -u -o pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PROFILE_ROOT="${JAZZ_PROFILE_ROOT:-$ROOT/dev/benchmarks/profiles}"
LEDGER="${SMOKE_LEDGER:-$ROOT/dev/benchmarks/SMOKE_LEDGER.md}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="$PROFILE_ROOT/$RUN_ID"
LOG_DIR="$ROOT/target/benchmark-profiles"
RUN_LABEL="${PROFILE_LABEL:-profile-smoke}"

mkdir -p "$OUT_DIR" "$LOG_DIR"

failures=()

run_profile() {
  local name="$1"
  local bench="$2"
  local log="$LOG_DIR/${name//[^A-Za-z0-9_.-]/_}.log"

  printf '==> profile %s\n' "$name"
  (
    cd "$ROOT"
    env JAZZ_SMOKE=1 JAZZ_PROFILE_OUT="$OUT_DIR" \
      cargo bench -p jazz-sim --features profiling --bench "$bench"
  ) >"$log" 2>&1
  local status=$?
  if [[ $status -ne 0 ]]; then
    failures+=("$name")
  fi
}

append_profile_ledger() {
  local run_at sha dirty result
  run_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  sha="$(git -C "$ROOT" rev-parse --short HEAD)"
  if git -C "$ROOT" diff --quiet && git -C "$ROOT" diff --cached --quiet; then
    dirty=false
  else
    dirty=true
  fi
  if ((${#failures[@]})); then
    result=fail
  else
    result=pass
  fi

  {
    printf '\n---\n\n'
    printf '## Profile Run %s - %s\n\n' "$run_at" "$RUN_LABEL"
    printf '%s\n' "- result: \`$result\`"
    printf '%s\n' "- git: \`$sha\`"
    printf '%s\n' "- dirty: \`$dirty\`"
    printf '%s\n' "- profile_dir: \`${OUT_DIR#$ROOT/}\`"
    printf '%s\n' "- command: \`dev/benchmarks/profile.sh\`"
    printf '\n### Top-10 Self-Time Tables\n'

    local top
    shopt -s nullglob
    for top in "$OUT_DIR"/*.top.txt; do
      printf '\n#### %s\n\n' "$(basename "$top" .top.txt)"
      cat "$top"
      printf '\n'
    done
    shopt -u nullglob
  } >>"$LEDGER"
}

(
  cd "$ROOT"
  for bench in s1_saas s3_permissions s4_order_processing
  do
    cargo bench -p jazz-sim --bench "$bench" --no-run -j 2 --features profiling
  done
) >"$LOG_DIR/prebuild.log" 2>&1
prebuild_status=$?
if [[ $prebuild_status -ne 0 ]]; then
  failures+=("prebuild")
else
  run_profile "s1_saas" "s1_saas"
  run_profile "s3_permissions" "s3_permissions"
  run_profile "s4_order_processing" "s4_order_processing"
fi

append_profile_ledger

if ((${#failures[@]})); then
  printf '\nProfile failures:\n'
  for failure in "${failures[@]}"; do
    printf '  - %s\n' "$failure"
  done
  printf '\nProfiles written to %s\n' "$OUT_DIR"
  exit 1
fi

printf '\nProfiles written to %s\n' "$OUT_DIR"
printf 'Ledger appended at %s\n' "$LEDGER"
