#!/usr/bin/env bash
set -u -o pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PROFILE_ROOT="${JAZZ_PROFILE_ROOT:-$ROOT/dev/benchmarks/profiles}"
LEDGER="${SMOKE_LEDGER:-$ROOT/dev/benchmarks/SMOKE_LEDGER.md}"
RESULT_ROOT="${SMOKE_RESULT_ROOT:-$ROOT/dev/benchmarks/results}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="$PROFILE_ROOT/$RUN_ID"
RESULT_DIR="$RESULT_ROOT/$RUN_ID"
MANIFEST="$RESULT_DIR/profile-manifest.tsv"
LOG_DIR="$ROOT/target/benchmark-profiles"
PROFILE_SIZE="${JAZZ_PROFILE_SIZE:-smoke}"
RUN_LABEL="${PROFILE_LABEL:-profile-$PROFILE_SIZE}"

mkdir -p "$OUT_DIR" "$RESULT_DIR" "$LOG_DIR"
: >"$MANIFEST"

failures=()

run_profile() {
  local name="$1"
  local bench="$2"
  local safe_name log jsonl start end elapsed status emitted
  safe_name="${name//[^A-Za-z0-9_.-]/_}"
  log="$LOG_DIR/$safe_name.log"
  jsonl="$RESULT_DIR/$safe_name.jsonl"

  printf '==> profile %s\n' "$name"
  start="$(perl -MTime::HiRes=time -e 'print time')"
  (
    cd "$ROOT"
    if [[ "$PROFILE_SIZE" == "smoke" ]]; then
      env JAZZ_SMOKE=1 JAZZ_PROFILE_OUT="$OUT_DIR" \
        cargo bench -p jazz-sim --features profiling --bench "$bench"
    elif [[ "$PROFILE_SIZE" == "default" ]]; then
      env JAZZ_PROFILE_OUT="$OUT_DIR" \
        cargo bench -p jazz-sim --features profiling --bench "$bench"
    else
      printf 'unknown JAZZ_PROFILE_SIZE=%s\n' "$PROFILE_SIZE" >&2
      exit 2
    fi
  ) >"$log" 2>&1
  status=$?
  end="$(perl -MTime::HiRes=time -e 'print time')"
  elapsed="$(awk -v start="$start" -v end="$end" 'BEGIN { printf "%.6f", end - start }')"
  emitted="$(extract_json_lines "$log" "$jsonl")"
  append_harness_json "$jsonl" "$name" "$status" "$elapsed" "$log" "$emitted"
  printf '%s\t%s\t%s\t%s\t%s\n' \
    "$name" "$status" "$elapsed" "${log#$ROOT/}" "${jsonl#$ROOT/}" >>"$MANIFEST"
  if [[ $status -ne 0 ]]; then
    failures+=("$name")
  fi
}

extract_json_lines() {
  local log="$1"
  local jsonl="$2"
  python3 - "$log" "$jsonl" <<'PY'
import json
import sys
from pathlib import Path

log = Path(sys.argv[1])
jsonl = Path(sys.argv[2])
count = 0
with log.open("r", encoding="utf-8", errors="replace") as src, jsonl.open(
    "w", encoding="utf-8"
) as dest:
    for line in src:
        text = line.strip()
        if not text.startswith("{"):
            continue
        try:
            obj = json.loads(text)
        except json.JSONDecodeError:
            continue
        dest.write(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n")
        count += 1
print(count)
PY
}

append_harness_json() {
  local jsonl="$1"
  local name="$2"
  local status="$3"
  local elapsed="$4"
  local log="$5"
  local emitted="$6"
  python3 - "$jsonl" "$name" "$status" "$elapsed" "${log#$ROOT/}" "$emitted" <<'PY'
import json
import sys
from pathlib import Path

jsonl = Path(sys.argv[1])
status_code = int(sys.argv[3])
status = "pass" if status_code == 0 else "fail"
row = {
    "scenario": sys.argv[2],
    "phase": "profile_harness",
    "status": status,
    "exit_code": status_code,
    "wall_s": float(sys.argv[4]),
    "wall_us": int(float(sys.argv[4]) * 1_000_000),
    "log": sys.argv[5],
    "emitted_json_lines": int(sys.argv[6]),
}
with jsonl.open("a", encoding="utf-8") as file:
    file.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
PY
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
    printf '%s\n' "- result_dir: \`${RESULT_DIR#$ROOT/}\`"
    printf '%s\n' "- command: \`JAZZ_PROFILE_SIZE=$PROFILE_SIZE dev/benchmarks/profile.sh\`"
    printf '\n### Profile Runs\n\n'
    printf '| Scenario | Status | Wall Time | JSONL |\n'
    printf '| --- | --- | ---: | --- |\n'
    if [[ -s "$MANIFEST" ]]; then
      while IFS=$'\t' read -r scenario status elapsed _log jsonl; do
        if [[ "$status" == "0" ]]; then
          status_text=pass
        else
          status_text=fail
        fi
        printf '| %s | `%s` | %.3fs | `%s` |\n' "$scenario" "$status_text" "$elapsed" "$jsonl"
      done <"$MANIFEST"
    fi
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
  cargo bench -p jazz-sim --no-run -j 2 --features profiling
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
