# jazz-lsm Benchmark Overview

Generated on 2026-02-13.

All values below are throughput shown as `K/s` (1 K = 1,000 ops/s) from the runs executed in this session. Native numbers come from Criterion `--quick` runs; WASM numbers come from the OPFS worker harness.

| Scenario | Value Size (bytes) | jazz-lsm native (compare_native) | jazz-lsm wasm/opfs | bf-tree native | rocksdb native | fjall native | bf-tree wasm/opfs | Notes |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| seq_write | 32 | 36.3 K/s | 3.4 K/s | 866.3 K/s | 1430.1 K/s | 21.2 K/s | N/A | Native `count=5000`; WASM `count=500` |
| random_write | 32 | 34.0 K/s | 3.3 K/s | 1160.1 K/s | 1062.5 K/s | 21.4 K/s | N/A | Native `count=5000`; WASM `count=500` |
| seq_read | 32 | 3.1 K/s | 4.8 K/s | 2767.3 K/s | 1069.9 K/s | 23.1 K/s | N/A | Native `count=5000`; WASM `count=500` |
| random_read | 32 | 3.1 K/s | 4.9 K/s | 2401.0 K/s | 1015.2 K/s | 23.1 K/s | N/A | Native `count=5000`; WASM `count=500` |
| seq_write | 256 | 26.8 K/s | 3.1 K/s | 365.7 K/s | 1063.8 K/s | 21.4 K/s | N/A | Native `count=5000`; WASM `count=500` |
| random_write | 256 | 24.6 K/s | 3.1 K/s | 622.3 K/s | 824.4 K/s | 21.5 K/s | N/A | Native `count=5000`; WASM `count=500` |
| seq_read | 256 | 5.9 K/s | 4.3 K/s | 2162.7 K/s | 988.1 K/s | 24.2 K/s | N/A | Native `count=5000`; WASM `count=500` |
| random_read | 256 | 5.9 K/s | 4.4 K/s | 1783.8 K/s | 908.0 K/s | 24.3 K/s | N/A | Native `count=5000`; WASM `count=500` |
| seq_write | 4096 | 3.0 K/s | 3.7 K/s | 33.6 K/s | 152.1 K/s | 10.8 K/s | N/A | Native `count=5000`; WASM `count=500` |
| random_write | 4096 | 3.1 K/s | 3.7 K/s | 41.4 K/s | 161.5 K/s | 10.1 K/s | N/A | Native `count=5000`; WASM `count=500` |
| seq_read | 4096 | 0.6 K/s | 0.7 K/s | 322.1 K/s | 352.9 K/s | 23.1 K/s | N/A | Native `count=5000`; WASM `count=500` |
| random_read | 4096 | 0.5 K/s | 0.7 K/s | 314.6 K/s | 324.5 K/s | 21.2 K/s | N/A | Native `count=5000`; WASM `count=500` |
| seq_write | 1,048,576 | <0.1 K/s | 0.1 K/s | N/A | 0.8 K/s | 0.3 K/s | N/A | Native compare `count=64`; WASM `count=32`; `bf-tree` unsupported at 1MB. Standalone `lsm_native`: <0.1 K/s |
| random_write | 1,048,576 | <0.1 K/s | 0.1 K/s | N/A | 0.8 K/s | 0.2 K/s | N/A | Native compare `count=64`; WASM `count=32`; `bf-tree` unsupported at 1MB. Standalone `lsm_native`: <0.1 K/s |
| seq_read | 1,048,576 | 0.3 K/s | 0.2 K/s | N/A | 4.6 K/s | 1.3 K/s | N/A | Native compare `count=64`; WASM `count=32`; `bf-tree` unsupported at 1MB. Standalone `lsm_native`: 0.2 K/s |
| random_read | 1,048,576 | 0.2 K/s | 0.2 K/s | N/A | 3.7 K/s | 0.2 K/s | N/A | Native compare `count=64`; WASM `count=32`; `bf-tree` unsupported at 1MB. Standalone `lsm_native`: 0.2 K/s |

Rounded to one decimal place; values below `0.1 K/s` are shown as `<0.1 K/s`.

## Phase 1 Mixed Baseline (single-threaded worker)

All throughput values below are shown as `K/s` (1 K = 1,000 ops/s). This section is the baseline for the new mixed workload scenarios from Phase 1.

| Scenario | Value Size (bytes) | jazz-lsm native ops/s | native p95 op (ms) | jazz-lsm wasm/opfs ops/s | wasm p95 op (ms) | Notes |
|---|---:|---:|---:|---:|---:|---|
| mixed_random_70r_30w | 32 | 2.7 K/s | 2.38 | 7.4 K/s | 1.00 | Native `count=500`; WASM `count=200` |
| mixed_random_50r_50w_with_updates | 32 | 2.4 K/s | 3.35 | 7.4 K/s | 1.00 | Native `count=500`; WASM `count=200` |
| mixed_random_60r_20w_20d | 32 | 2.6 K/s | 3.05 | 6.1 K/s | 1.00 | Native `count=500`; WASM `count=200` |
| mixed_random_70r_30w | 256 | 2.5 K/s | 2.77 | 5.4 K/s | 1.00 | Native `count=500`; WASM `count=200` |
| mixed_random_50r_50w_with_updates | 256 | 2.2 K/s | 3.22 | 5.4 K/s | 1.00 | Native `count=500`; WASM `count=200` |
| mixed_random_60r_20w_20d | 256 | 2.3 K/s | 2.63 | 5.3 K/s | 1.00 | Native `count=500`; WASM `count=200` |
| mixed_random_70r_30w | 4096 | 1.1 K/s | 2.45 | 2.4 K/s | 1.00 | Native `count=500`; WASM `count=200` |
| mixed_random_50r_50w_with_updates | 4096 | 1.6 K/s | 2.19 | 2.6 K/s | 1.00 | Native `count=500`; WASM `count=200` |
| mixed_random_60r_20w_20d | 4096 | 2.3 K/s | 0.63 | 2.6 K/s | 1.00 | Native `count=500`; WASM `count=200` |
| mixed_random_70r_30w | 1,048,576 | <0.1 K/s | 78.04 | 0.3 K/s | 7.00 | Native `count=64`; WASM `count=4` |
| mixed_random_50r_50w_with_updates | 1,048,576 | <0.1 K/s | 46.97 | 0.3 K/s | 7.00 | Native `count=64`; WASM `count=4` |
| mixed_random_60r_20w_20d | 1,048,576 | <0.1 K/s | 44.13 | 0.3 K/s | 6.00 | Native `count=64`; WASM `count=4` |

WASM latency here is quantized by `Date.now()` millisecond resolution, so `p95` is coarse and should be treated as directional.

## Progress Tracking

- Use this mixed baseline table as the source of truth for Phase 1+ changes.
- Re-run with the same commands and counts before/after each optimization.
- Track per-row deltas:
- Throughput delta = `(new_ops_per_sec - baseline_ops_per_sec) / baseline_ops_per_sec`
- Latency delta = `(new_p95_ms - baseline_p95_ms) / baseline_p95_ms`
- Gate rule:
- No regression worse than `-10%` throughput on any `32/256/4096` row
- Any deliberate tradeoff (for example writes up / reads down) must be called out explicitly in notes

## Run Details

- `jazz-lsm` WASM rerun (standard sizes): `pnpm --dir /Users/anselm/jazz2-clean/crates/jazz-lsm run bench:wasm:opfs -- --count 500 --value-sizes 32,256,4096 --json`
- `jazz-lsm` native 1MB standalone: `JAZZ_LSM_BENCH_KEY_COUNT=64 JAZZ_LSM_BENCH_VALUE_SIZES=1048576 cargo bench -p jazz-lsm --bench lsm_native -- --quick`
- Native comparative 1MB: `DYLD_LIBRARY_PATH=/opt/homebrew/opt/llvm/lib DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/opt/llvm/lib LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib JAZZ_LSM_BENCH_KEY_COUNT=64 JAZZ_LSM_BENCH_VALUE_SIZES=1048576 cargo bench -p jazz-lsm --features compare-native --bench compare_native -- --quick`
- `jazz-lsm` WASM 1MB: `pnpm --dir /Users/anselm/jazz2-clean/crates/jazz-lsm run bench:wasm:opfs -- --count 32 --value-sizes 1048576 --json`
- Mixed native baseline (32/256/4096): `cargo run -p jazz-lsm --release --bin mixed_bench_native -- --count 500 --value-sizes 32,256,4096 --json`
- Mixed native baseline (1MB): `cargo run -p jazz-lsm --release --bin mixed_bench_native -- --count 64 --value-sizes 1048576 --json`
- Mixed wasm/opfs baseline (32/256/4096): `pnpm --dir /Users/anselm/jazz2-clean/crates/jazz-lsm run bench:wasm:opfs -- --profile mixed --count 200 --value-sizes 32,256,4096 --json`
- Mixed wasm/opfs baseline (1MB reduced): `pnpm --dir /Users/anselm/jazz2-clean/crates/jazz-lsm run bench:wasm:opfs -- --profile mixed --count 4 --value-sizes 1048576 --json`
