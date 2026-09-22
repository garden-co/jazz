#!/usr/bin/env bash
# Ad-hoc CPU capture helper. Profiles are local investigation artifacts, not a
# correctness or performance-regression gate.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PROFILE_ROOT="${JAZZ_PROFILE_ROOT:-$ROOT/dev/benchmarks/profiles}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="$PROFILE_ROOT/$RUN_ID"
PROFILE_SIZE="${JAZZ_PROFILE_SIZE:-smoke}"

if [[ "$PROFILE_SIZE" != "smoke" && "$PROFILE_SIZE" != "default" ]]; then
  printf 'unknown JAZZ_PROFILE_SIZE=%s\n' "$PROFILE_SIZE" >&2
  exit 2
fi

mkdir -p "$OUT_DIR"
for bench in s1_saas s3_permissions s4_order_processing; do
  printf '==> profile %s\n' "$bench"
  if [[ "$PROFILE_SIZE" == "smoke" ]]; then
    (
      cd "$ROOT"
      env JAZZ_SMOKE=1 JAZZ_PROFILE_OUT="$OUT_DIR" \
        cargo bench -p jazz-sim --features profiling --bench "$bench"
    )
  else
    (
      cd "$ROOT"
      env JAZZ_PROFILE_OUT="$OUT_DIR" \
        cargo bench -p jazz-sim --features profiling --bench "$bench"
    )
  fi
done

printf '\nProfiles written to %s\n' "$OUT_DIR"
