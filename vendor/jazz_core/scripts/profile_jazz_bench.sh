#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: scripts/profile_jazz_bench.sh <bench-name> [bench-args...]" >&2
  echo "example: scripts/profile_jazz_bench.sh s2_canvas" >&2
  exit 2
fi

bench_name="$1"
shift

if ! command -v samply >/dev/null 2>&1; then
  echo "samply is required for capture; install it with: cargo install samply" >&2
  exit 127
fi

mkdir -p target/profiles

build_log="$(mktemp -t jazz-bench-build.XXXXXX.jsonl)"
trap 'rm -f "$build_log"' EXIT

CARGO_PROFILE_BENCH_DEBUG="${CARGO_PROFILE_BENCH_DEBUG:-true}" \
  cargo bench -p jazz-sim --bench "$bench_name" --no-run --message-format=json \
  >"$build_log"

bench_exe="$(
  python3 - "$build_log" <<'PY'
import json
import sys

for line in open(sys.argv[1], encoding="utf-8"):
    try:
        msg = json.loads(line)
    except json.JSONDecodeError:
        continue
    if msg.get("reason") != "compiler-artifact":
        continue
    target = msg.get("target", {})
    if "bench" not in target.get("kind", []):
        continue
    executable = msg.get("executable")
    if executable:
        print(executable)
        break
PY
)"

if [[ -z "$bench_exe" ]]; then
  echo "could not find bench executable in cargo output" >&2
  exit 1
fi

output="${JAZZ_PROFILE_OUTPUT:-target/profiles/${bench_name}-profile.json.gz}"

echo "profiling $bench_exe"
echo "writing $output"
JAZZ_BENCH_PROFILE="${JAZZ_BENCH_PROFILE:-profile}" \
  samply record --save-only -o "$output" -- "$bench_exe" "$@"
