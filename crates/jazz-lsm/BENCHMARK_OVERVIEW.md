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

## Phase 2 Results (internal WAL batching + in-memory `wal_bytes`)

Phase 2 values below use the same mixed scenarios and count settings as the Phase 1 section. `Δ%` is computed versus Phase 1.

| Scenario | Value Size (bytes) | Native ops/s (P1 -> P2) | Native p95 ms (P1 -> P2) | WASM ops/s (P1 -> P2) | WASM p95 ms (P1 -> P2) | Notes |
|---|---:|---|---|---|---|---|
| mixed_random_70r_30w | 32 | `2673.363 -> 6534.520` (`+144.4%`) | `2.380 -> 0.124` (`-94.8%`) | `5882.353 -> 9009.009` (`+53.2%`) | `0.200 -> 0.200` (`~0.0%`) | Native `count=500`; WASM `count=100` |
| mixed_random_50r_50w_with_updates | 32 | `2359.634 -> 7840.841` (`+232.3%`) | `3.354 -> 0.101` (`-97.0%`) | `5555.556 -> 14925.373` (`+168.7%`) | `0.300 -> 0.100` (`-66.7%`) | Native `count=500`; WASM `count=100` |
| mixed_random_60r_20w_20d | 32 | `2578.329 -> 7361.530` (`+185.5%`) | `3.052 -> 0.101` (`-96.7%`) | `6024.096 -> 12345.679` (`+104.9%`) | `0.300 -> 0.200` (`-33.3%`) | Native `count=500`; WASM `count=100` |
| mixed_random_70r_30w | 256 | `2471.936 -> 6436.643` (`+160.4%`) | `2.766 -> 0.119` (`-95.7%`) | `2941.176 -> 10638.298` (`+261.7%`) | `0.800 -> 0.200` (`-75.0%`) | Native `count=500`; WASM `count=100` |
| mixed_random_50r_50w_with_updates | 256 | `2204.954 -> 7984.770` (`+262.1%`) | `3.219 -> 0.106` (`-96.7%`) | `5347.594 -> 11764.706` (`+120.0%`) | `0.300 -> 0.200` (`-33.3%`) | Native `count=500`; WASM `count=100` |
| mixed_random_60r_20w_20d | 256 | `2250.680 -> 7086.641` (`+214.9%`) | `2.631 -> 0.100` (`-96.2%`) | `5882.353 -> 8403.361` (`+42.9%`) | `0.300 -> 0.200` (`-33.3%`) | Native `count=500`; WASM `count=100` |
| mixed_random_70r_30w | 4096 | `1140.796 -> 2037.671` (`+78.6%`) | `2.448 -> 0.738` (`-69.9%`) | `3003.003 -> 3676.471` (`+22.4%`) | `0.400 -> 0.400` (`~0.0%`) | Native `count=500`; WASM `count=100` |
| mixed_random_50r_50w_with_updates | 4096 | `1590.260 -> 1675.335` (`+5.4%`) | `2.190 -> 0.938` (`-57.2%`) | `3225.806 -> 4716.981` (`+46.2%`) | `0.400 -> 0.400` (`~0.0%`) | Native `count=500`; WASM `count=100` |
| mixed_random_60r_20w_20d | 4096 | `2294.221 -> 2178.766` (`-5.0%`) | `0.633 -> 0.789` (`+24.6%`) | `3205.128 -> 3952.569` (`+23.3%`) | `0.400 -> 0.400` (`~0.0%`) | Native `count=500`; WASM `count=100` |
| mixed_random_70r_30w | 1,048,576 | `35.669 -> 43.998` (`+23.4%`) | `78.036 -> 42.167` (`-46.0%`) | `277.778 -> 272.109` (`-2.0%`) | `6.800 -> 6.600` (`-2.9%`) | Native `count=64`; WASM `count=4` |
| mixed_random_50r_50w_with_updates | 1,048,576 | `39.794 -> 38.335` (`-3.7%`) | `46.966 -> 58.213` (`+24.0%`) | `283.688 -> 294.118` (`+3.7%`) | `6.400 -> 6.000` (`-6.3%`) | Native `count=64`; WASM `count=4` |
| mixed_random_60r_20w_20d | 1,048,576 | `46.140 -> 48.729` (`+5.6%`) | `44.129 -> 44.055` (`-0.2%`) | `289.855 -> 307.692` (`+6.2%`) | `6.400 -> 5.600` (`-12.5%`) | Native `count=64`; WASM `count=4` |

## Phase 3 Results (encode/decode buffer reuse)

Phase 3 values below compare against Phase 2 using the same scenario/count matrix.

| Scenario | Value Size (bytes) | Native ops/s (P2 -> P3) | Native p95 ms (P2 -> P3) | WASM ops/s (P2 -> P3) | WASM p95 ms (P2 -> P3) | Notes |
|---|---:|---|---|---|---|---|
| mixed_random_70r_30w | 32 | `6534.520 -> 8145.130` (`+24.7%`) | `0.124 -> 0.133` (`+7.0%`) | `9009.009 -> 7874.016` (`-12.6%`) | `0.200 -> 0.200` (`~0.0%`) | Native `count=500`; WASM `count=100` |
| mixed_random_50r_50w_with_updates | 32 | `7840.841 -> 11789.614` (`+50.4%`) | `0.101 -> 0.078` (`-23.2%`) | `14925.373 -> 11111.111` (`-25.6%`) | `0.100 -> 0.200` (`+100.0%`) | Native `count=500`; WASM `count=100` |
| mixed_random_60r_20w_20d | 32 | `7361.530 -> 11494.407` (`+56.1%`) | `0.101 -> 0.072` (`-29.2%`) | `12345.679 -> 9803.922` (`-20.6%`) | `0.200 -> 0.200` (`~0.0%`) | Native `count=500`; WASM `count=100` |
| mixed_random_70r_30w | 256 | `6436.643 -> 9536.775` (`+48.2%`) | `0.119 -> 0.091` (`-23.6%`) | `10638.298 -> 8695.652` (`-18.3%`) | `0.200 -> 0.200` (`~0.0%`) | Native `count=500`; WASM `count=100` |
| mixed_random_50r_50w_with_updates | 256 | `7984.770 -> 10985.178` (`+37.6%`) | `0.106 -> 0.082` (`-22.8%`) | `11764.706 -> 10869.565` (`-7.6%`) | `0.200 -> 0.200` (`~0.0%`) | Native `count=500`; WASM `count=100` |
| mixed_random_60r_20w_20d | 256 | `7086.641 -> 10337.867` (`+45.9%`) | `0.100 -> 0.084` (`-15.7%`) | `8403.361 -> 8333.333` (`-0.8%`) | `0.200 -> 0.200` (`~0.0%`) | Native `count=500`; WASM `count=100` |
| mixed_random_70r_30w | 4096 | `2037.671 -> 2662.683` (`+30.7%`) | `0.738 -> 0.520` (`-29.6%`) | `3676.471 -> 3267.974` (`-11.1%`) | `0.400 -> 0.400` (`~0.0%`) | Native `count=500`; WASM `count=100` |
| mixed_random_50r_50w_with_updates | 4096 | `1675.335 -> 2610.600` (`+55.8%`) | `0.938 -> 0.592` (`-36.9%`) | `4716.981 -> 4016.064` (`-14.9%`) | `0.400 -> 0.400` (`~0.0%`) | Native `count=500`; WASM `count=100` |
| mixed_random_60r_20w_20d | 4096 | `2178.766 -> 2713.318` (`+24.5%`) | `0.789 -> 0.633` (`-19.7%`) | `3952.569 -> 3676.471` (`-7.0%`) | `0.400 -> 0.400` (`~0.0%`) | Native `count=500`; WASM `count=100` |
| mixed_random_70r_30w | 1,048,576 | `43.998 -> 44.601` (`+1.4%`) | `42.167 -> 43.665` (`+3.6%`) | `272.109 -> 254.777` (`-6.4%`) | `6.600 -> 7.400` (`+12.1%`) | Native `count=64`; WASM `count=4` |
| mixed_random_50r_50w_with_updates | 1,048,576 | `38.335 -> 40.661` (`+6.1%`) | `58.213 -> 47.854` (`-17.8%`) | `294.118 -> 275.862` (`-6.2%`) | `6.000 -> 6.500` (`+8.3%`) | Native `count=64`; WASM `count=4` |
| mixed_random_60r_20w_20d | 1,048,576 | `48.729 -> 48.667` (`-0.1%`) | `44.055 -> 44.308` (`+0.6%`) | `307.692 -> 268.456` (`-12.8%`) | `5.600 -> 6.900` (`+23.2%`) | Native `count=64`; WASM `count=4` |

## Phase 4 Results (SST v2 block format + point-read index)

Phase 4 values below compare against Phase 3 using the same scenario/count matrix where runs completed.

| Scenario | Value Size (bytes) | Native ops/s (P3 -> P4) | Native p95 ms (P3 -> P4) | WASM ops/s (P3 -> P4) | WASM p95 ms (P3 -> P4) | Notes |
|---|---:|---|---|---|---|---|
| mixed_random_70r_30w | 32 | `8145.130 -> 8826.677` (`+8.4%`) | `0.133 -> 0.119` (`-10.1%`) | `7874.016 -> 3802.281` (`-51.7%`) | `0.200 -> 0.400` (`+100.0%`) | Native `count=500`; WASM `count=100` |
| mixed_random_50r_50w_with_updates | 32 | `11789.614 -> 11004.389` (`-6.7%`) | `0.078 -> 0.080` (`+2.9%`) | `11111.111 -> 6024.096` (`-45.8%`) | `0.200 -> 0.300` (`+50.0%`) | Native `count=500`; WASM `count=100` |
| mixed_random_60r_20w_20d | 32 | `11494.407 -> 10538.879` (`-8.3%`) | `0.072 -> 0.078` (`+9.5%`) | `9803.922 -> 4901.961` (`-50.0%`) | `0.200 -> 0.300` (`+50.0%`) | Native `count=500`; WASM `count=100` |
| mixed_random_70r_30w | 256 | `9536.775 -> 11279.574` (`+18.3%`) | `0.091 -> 0.071` (`-22.7%`) | `8695.652 -> 4651.163` (`-46.5%`) | `0.200 -> 0.400` (`+100.0%`) | Native `count=500`; WASM `count=100` |
| mixed_random_50r_50w_with_updates | 256 | `10985.178 -> 13009.695` (`+18.4%`) | `0.082 -> 0.058` (`-28.9%`) | `10869.565 -> 5780.347` (`-46.8%`) | `0.200 -> 0.300` (`+50.0%`) | Native `count=500`; WASM `count=100` |
| mixed_random_60r_20w_20d | 256 | `10337.867 -> 11787.750` (`+14.0%`) | `0.084 -> 0.065` (`-23.4%`) | `8333.333 -> 4608.295` (`-44.7%`) | `0.200 -> 0.400` (`+100.0%`) | Native `count=500`; WASM `count=100` |
| mixed_random_70r_30w | 4096 | `2662.683 -> 10998.347` (`+313.0%`) | `0.520 -> 0.071` (`-86.4%`) | `3267.974 -> 4237.288` (`+29.7%`) | `0.400 -> 0.300` (`-25.0%`) | Native `count=500`; WASM `count=100` |
| mixed_random_50r_50w_with_updates | 4096 | `2610.600 -> 7500.994` (`+187.3%`) | `0.592 -> 0.095` (`-84.0%`) | `4016.064 -> 5263.158` (`+31.1%`) | `0.400 -> 0.300` (`-25.0%`) | Native `count=500`; WASM `count=100` |
| mixed_random_60r_20w_20d | 4096 | `2713.318 -> 12372.052` (`+355.9%`) | `0.633 -> 0.058` (`-90.8%`) | `3676.471 -> 4901.961` (`+33.3%`) | `0.400 -> 0.300` (`-25.0%`) | Native `count=500`; WASM `count=100` |
| mixed_random_70r_30w | 1,048,576 | `44.601 -> 89.716` (`+101.2%`) | `43.665 -> 51.758` (`+18.5%`) | N/A | N/A | Native `count=64`; WASM 1MB run did not complete in this session |
| mixed_random_50r_50w_with_updates | 1,048,576 | `40.661 -> 62.312` (`+53.2%`) | `47.854 -> 65.344` (`+36.5%`) | N/A | N/A | Native `count=64`; WASM 1MB run did not complete in this session |
| mixed_random_60r_20w_20d | 1,048,576 | `48.667 -> 96.424` (`+98.1%`) | `44.308 -> 50.606` (`+14.2%`) | N/A | N/A | Native `count=64`; WASM 1MB run did not complete in this session |

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
- Phase 2 mixed native rerun (32/256/4096): `cargo run -p jazz-lsm --release --bin mixed_bench_native -- --count 500 --value-sizes 32,256,4096 --json`
- Phase 2 mixed native rerun (1MB): `cargo run -p jazz-lsm --release --bin mixed_bench_native -- --count 64 --value-sizes 1048576 --json`
- Phase 2 mixed wasm/opfs rerun (32/256/4096): `pnpm --dir /Users/anselm/jazz2-clean/crates/jazz-lsm run bench:wasm:opfs -- --profile mixed --count 100 --value-sizes 32,256,4096 --json`
- Phase 2 mixed wasm/opfs rerun (1MB reduced): `pnpm --dir /Users/anselm/jazz2-clean/crates/jazz-lsm run bench:wasm:opfs -- --profile mixed --count 4 --value-sizes 1048576 --json`
- Phase 3 mixed native rerun (32/256/4096): `cargo run -p jazz-lsm --release --bin mixed_bench_native -- --count 500 --value-sizes 32,256,4096 --json`
- Phase 3 mixed native rerun (1MB): `cargo run -p jazz-lsm --release --bin mixed_bench_native -- --count 64 --value-sizes 1048576 --json`
- Phase 3 mixed wasm/opfs rerun (32/256/4096): `pnpm --dir /Users/anselm/jazz2-clean/crates/jazz-lsm run bench:wasm:opfs -- --profile mixed --count 100 --value-sizes 32,256,4096 --json`
- Phase 3 mixed wasm/opfs rerun (1MB reduced): `pnpm --dir /Users/anselm/jazz2-clean/crates/jazz-lsm run bench:wasm:opfs -- --profile mixed --count 4 --value-sizes 1048576 --json`
- Phase 4 mixed native rerun (32/256/4096): `cargo run -p jazz-lsm --release --bin mixed_bench_native -- --count 500 --value-sizes 32,256,4096 --json`
- Phase 4 mixed native rerun (1MB): `cargo run -p jazz-lsm --release --bin mixed_bench_native -- --count 64 --value-sizes 1048576 --json`
- Phase 4 mixed wasm/opfs rerun (32/256/4096): `pnpm --dir /Users/anselm/jazz2-clean/crates/jazz-lsm run bench:wasm:opfs -- --profile mixed --count 100 --value-sizes 32,256,4096 --json`
- Phase 4 mixed wasm/opfs rerun (1MB reduced): attempted with `count=4` and `count=1`, but did not complete before manual termination
