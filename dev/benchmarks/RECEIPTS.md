# Benchmark receipts

`dev/benchmarks/receipt-adapters.sh` contains the per-harness entry-point
adapters. It delegates to `run-receipt.mjs`, which does not change or combine
their runners: it captures native output, adds the `phase: "harness"` row used
by `smoke.sh`, and appends the same status/wall-time/previous/delta summary to
`SMOKE_LEDGER.md`. The stable scenario name is the ledger key, so rerunning an
adapter preserves the previous/delta comparison.

Run these from the workspace root. Each command is deliberately a single cheap
target for verifying an adapter; use the harness's normal command when taking a
deliberate full baseline.

```sh
# Groove custom JSON (one scenario and one micro sample)
dev/benchmarks/receipt-adapters.sh groove-scenario oneshot groove_query
dev/benchmarks/receipt-adapters.sh groove-micro

# jazz-tools Criterion: retain p50 and p95 per-iteration samples plus sample
# count (not a misleading single mean), alongside the harness wall time.
dev/benchmarks/receipt-adapters.sh jazz-tools-criterion observer_write_path

# Native realistic JSON report.
dev/benchmarks/receipt-adapters.sh realistic-native w4_cold_start

# Browser realistic remains the browser runner. Select one scenario and pass
# its native report back to the adapter rather than trying to execute it under
# cargo bench.
dev/benchmarks/receipt-adapters.sh realistic-browser W1

# Raw native storage and in-memory B-tree Criterion targets.
dev/benchmarks/receipt-adapters.sh storage-native
dev/benchmarks/receipt-adapters.sh opfs-btree

# Real OPFS worker and separate WASM runtime probes.
dev/benchmarks/receipt-adapters.sh opfs-worker

# This adapter expects the documented probe build artifact. Build it first:
wasm-pack build crates/jazz-wasm --target web --release -- --features bench-probes
dev/benchmarks/receipt-adapters.sh wasm-probes
```

For ingest capture/replay, use the same wrapper with the existing capture,
`replay-native-ingest.mjs`, or `replay-wasm-ingest.mjs` command. Point
`--json-file` at the replay receipt selected by `JAZZ_NATIVE_INGEST_RECEIPT` or
`JAZZ_WASM_INGEST_RECEIPT`. Capture itself has no performance metric: its
receipt records provenance plus harness wall time, while replay retains the
open/subscribe/decode/callback/frame-pump phase timings it already emits.
