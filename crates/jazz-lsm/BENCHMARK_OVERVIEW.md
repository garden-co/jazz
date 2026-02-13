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

Throughput values below are shown as exact `ops/s` from the mixed benchmark runners.

| Scenario | Value Size (bytes) | jazz-lsm native ops/s | native p95 op (ms) | jazz-lsm wasm/opfs ops/s | wasm p95 op (ms) | Notes |
|---|---:|---:|---:|---:|---:|---|
| mixed_random_70r_30w | 32 | 2673.363 | 2.380 | 5882.353 | 0.200 | Native `count=500`; WASM `count=100` |
| mixed_random_50r_50w_with_updates | 32 | 2359.634 | 3.354 | 5555.556 | 0.300 | Native `count=500`; WASM `count=100` |
| mixed_random_60r_20w_20d | 32 | 2578.329 | 3.052 | 6024.096 | 0.300 | Native `count=500`; WASM `count=100` |
| mixed_random_70r_30w | 256 | 2471.936 | 2.766 | 2941.176 | 0.800 | Native `count=500`; WASM `count=100` |
| mixed_random_50r_50w_with_updates | 256 | 2204.954 | 3.219 | 5347.594 | 0.300 | Native `count=500`; WASM `count=100` |
| mixed_random_60r_20w_20d | 256 | 2250.680 | 2.631 | 5882.353 | 0.300 | Native `count=500`; WASM `count=100` |
| mixed_random_70r_30w | 4096 | 1140.796 | 2.448 | 3003.003 | 0.400 | Native `count=500`; WASM `count=100` |
| mixed_random_50r_50w_with_updates | 4096 | 1590.260 | 2.190 | 3225.806 | 0.400 | Native `count=500`; WASM `count=100` |
| mixed_random_60r_20w_20d | 4096 | 2294.221 | 0.633 | 3205.128 | 0.400 | Native `count=500`; WASM `count=100` |
| mixed_random_70r_30w | 1,048,576 | 35.669 | 78.036 | 277.778 | 6.800 | Native `count=64`; WASM `count=4` |
| mixed_random_50r_50w_with_updates | 1,048,576 | 39.794 | 46.966 | 283.688 | 6.400 | Native `count=64`; WASM `count=4` |
| mixed_random_60r_20w_20d | 1,048,576 | 46.140 | 44.129 | 289.855 | 6.400 | Native `count=64`; WASM `count=4` |

WASM benchmark timing now uses `performance.now()` (with `Date.now()` fallback), so p95 values include sub-millisecond precision.

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
- Mixed wasm/opfs baseline (32/256/4096): `pnpm --dir /Users/anselm/jazz2-clean/crates/jazz-lsm run bench:wasm:opfs -- --profile mixed --count 100 --value-sizes 32,256,4096 --json`
- Mixed wasm/opfs baseline (1MB reduced): `pnpm --dir /Users/anselm/jazz2-clean/crates/jazz-lsm run bench:wasm:opfs -- --profile mixed --count 4 --value-sizes 1048576 --json`
