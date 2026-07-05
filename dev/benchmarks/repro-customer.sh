#!/usr/bin/env bash
# One-command repro of the customer-shape cold/warm numbers (2026-07-04 state).
# Runs the anonymized the pilot customer-shaped benchmark at full scale (20.5k-row shape,
# full permission fidelity, real seeded-membership + inherited child policies)
# across the identity matrix, and prints a summary table.
#
# Usage:            dev/benchmarks/repro-customer.sh
# Quick variant:    JAZZ_REPRO_SCALE=0.10 dev/benchmarks/repro-customer.sh
# Fresh seed:       JAZZ_CUSTOMER_FRESH_SEED=1 dev/benchmarks/repro-customer.sh
#
# Each run emits full JSON on stdout (per-subscription timelines, allocation
# and memory-amplification metrics); this script extracts the headline row.
# Expect ~1-2 min per cell warm-cached; first run pays a one-time release build.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

SCALE="${JAZZ_REPRO_SCALE:-1.0}"
OUT_DIR="${JAZZ_REPRO_OUT:-target/customer-repro}"
mkdir -p "$OUT_DIR"

run_cell() {
  local identity="$1" phase="$2"
  local log="$OUT_DIR/${identity}-${phase}-scale${SCALE}.json"
  echo "==> $identity / $phase / scale $SCALE"
  JAZZ_CUSTOMER_IDENTITY="$identity" \
  JAZZ_CUSTOMER_PHASES="$phase" \
  JAZZ_CUSTOMER_SCALE="$SCALE" \
  JAZZ_CUSTOMER_MAX_TICKS="${JAZZ_CUSTOMER_MAX_TICKS:-200000}" \
    cargo bench -p jazz-sim --bench customer_cold_start -- --nocapture \
    | tee /dev/stderr | grep -o '{"a.*}' | tail -1 > "$log" || true
  python3 - "$log" "$identity" "$phase" << 'EOF'
import json, sys
try:
    j = json.load(open(sys.argv[1]))
except Exception:
    print(f"  {sys.argv[2]}/{sys.argv[3]}: NO JSON CAPTURED (see stderr above)"); sys.exit(0)
print(f"  -> wall {j['wall_ms']}ms | settle {j['settle_ms']}ms | "
      f"rows {j['rows_materialized']}/{j['expected_rows']} | "
      f"amp {j.get('memory_amplification', 0):.1f}x | "
      f"allocs/row {j.get('allocs_per_row', 0):.0f}")
EOF
}

echo "Customer-shape repro — scale $SCALE — $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "(full JSON per cell in $OUT_DIR)"
run_cell member cold
run_cell member warm
run_cell admin  cold
run_cell spy    cold
echo "Done. Cells above must show rows == expected; spy must hold zero protected rows (enforced in-bench)."
