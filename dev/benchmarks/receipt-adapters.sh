#!/usr/bin/env bash
# Thin entry-point adapters. They intentionally delegate to the existing
# harness commands; run-receipt.mjs only adds the smoke receipt envelope.
set -u -o pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RECEIPT=(node "$ROOT/dev/benchmarks/run-receipt.mjs")

usage() {
  cat <<'EOF'
Usage: dev/benchmarks/receipt-adapters.sh <adapter> [adapter args]

Adapters:
  groove-scenario [scenario] [engine]   (defaults: oneshot groove_query)
  groove-micro
  jazz-tools-criterion [bench]          (default: observer_write_path)
  realistic-native [scenario-json]      (default: w4_cold_start)
  realistic-browser [W1|W4|B1..B6]     (default: W1)
  storage-native
  opfs-btree
  opfs-worker
  wasm-probes
  wasm-ingest-native                    (requires JAZZ_WASM_INGEST_FIXTURE)
  wasm-ingest-wasm                      (requires JAZZ_WASM_INGEST_FIXTURE)
  wasm-ingest-capture                   (capture's wall time/provenance only)
EOF
}

adapter="${1:-}"
shift || true
case "$adapter" in
  groove-scenario)
    scenario="${1:-oneshot}"; engine="${2:-groove_query}"
    exec "${RECEIPT[@]}" --scenario "groove/scenario/${scenario}/${engine}" \
      --invocation "GROOVE_SCENARIO=$scenario GROOVE_ENGINE=$engine cargo bench -p groove --bench scenario --quiet" -- \
      env "GROOVE_SCENARIO=$scenario" "GROOVE_ENGINE=$engine" cargo bench -p groove --bench scenario --quiet
    ;;
  groove-micro)
    exec "${RECEIPT[@]}" --scenario groove/micro \
      --invocation 'GROOVE_MICRO_ITERS=1 cargo bench -p groove --bench micro --quiet' -- \
      env GROOVE_MICRO_ITERS=1 cargo bench -p groove --bench micro --quiet
    ;;
  jazz-tools-criterion)
    bench="${1:-observer_write_path}"
    exec "${RECEIPT[@]}" --scenario "jazz-tools/criterion/$bench" \
      --invocation "cargo bench -p jazz-tools --bench $bench -- --sample-size 10" --criterion -- \
      cargo bench -p jazz-tools --bench "$bench" -- --sample-size 10
    ;;
  realistic-native)
    scenario="${1:-w4_cold_start}"
    scenario_path="dev/benchmarks/realistic/scenarios/$scenario.json"
    exec "${RECEIPT[@]}" --scenario "realistic/native/$scenario" \
      --invocation "cargo run -p jazz-tools --features client,rocksdb --example realistic_bench -- --scenario $scenario_path --profile dev/benchmarks/realistic/profiles/s.json" -- \
      cargo run -p jazz-tools --features client,rocksdb --example realistic_bench -- --scenario "$scenario_path" --profile dev/benchmarks/realistic/profiles/s.json
    ;;
  realistic-browser)
    browser_scenario="${1:-W1}"
    browser_id="receipt-${browser_scenario,,}"
    exec "${RECEIPT[@]}" --scenario "realistic/browser/${browser_scenario,,}" \
      --invocation "JAZZ_REALISTIC_BROWSER_SCENARIOS=$browser_scenario pnpm --dir packages/jazz-tools run bench:realistic:browser" \
      --json-file "packages/jazz-tools/.vitest-browser-bench/$browser_id.json" -- \
      env "JAZZ_REALISTIC_BROWSER_SCENARIOS=$browser_scenario" "JAZZ_REALISTIC_BROWSER_RUN_ID=$browser_id" pnpm --dir packages/jazz-tools run bench:realistic:browser
    ;;
  storage-native)
    exec "${RECEIPT[@]}" --scenario storage/native-engines \
      --invocation 'cargo bench -p jazz-storage-native-bench --bench native_storage_engines' --criterion -- \
      cargo bench -p jazz-storage-native-bench --bench native_storage_engines
    ;;
  opfs-btree)
    exec "${RECEIPT[@]}" --scenario opfs-btree/hot-paths \
      --invocation 'cargo bench -p opfs-btree --bench hot_paths' --criterion -- \
      cargo bench -p opfs-btree --bench hot_paths
    ;;
  opfs-worker)
    exec "${RECEIPT[@]}" --scenario opfs-btree/wasm-worker \
      --invocation 'pnpm --dir crates/opfs-btree run bench:wasm:opfs -- --count 1 --value-sizes 32 --json' -- \
      pnpm --dir crates/opfs-btree run bench:wasm:opfs -- --count 1 --value-sizes 32 --json
    ;;
  wasm-probes)
    exec "${RECEIPT[@]}" --scenario wasm-ingest/runtime-probes \
      --invocation 'node dev/benchmarks/wasm-ingest/run-wasm-probes.mjs' -- \
      node dev/benchmarks/wasm-ingest/run-wasm-probes.mjs
    ;;
  wasm-ingest-native)
    exec "${RECEIPT[@]}" --scenario wasm-ingest/native-replay \
      --invocation 'node dev/benchmarks/wasm-ingest/replay-native-ingest.mjs' -- \
      node dev/benchmarks/wasm-ingest/replay-native-ingest.mjs
    ;;
  wasm-ingest-wasm)
    exec "${RECEIPT[@]}" --scenario wasm-ingest/wasm-replay \
      --invocation 'node dev/benchmarks/wasm-ingest/replay-wasm-ingest.mjs' -- \
      node dev/benchmarks/wasm-ingest/replay-wasm-ingest.mjs
    ;;
  wasm-ingest-capture)
    exec "${RECEIPT[@]}" --scenario wasm-ingest/capture \
      --invocation 'node dev/benchmarks/wasm-ingest/capture-real-app-fixture.mjs' -- \
      node dev/benchmarks/wasm-ingest/capture-real-app-fixture.mjs
    ;;
  *) usage; exit 2 ;;
esac
