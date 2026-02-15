# opfs-btree Benchmark Overview

Generated on 2026-02-14.

Single baseline table for further `opfs-btree` work. All values are throughput in `K/s` (1 K = 1,000 ops/s), rounded to one decimal.
Current `opfs-btree` default cache is `32MB`.

| Scenario                          | Value Size (bytes) | opfs-btree native | opfs-btree wasm/opfs | bf-tree native | rocksdb native | surrealkv native | fjall native | Notes      |
| --------------------------------- | -----------------: | ----------------: | -------------------: | -------------: | -------------: | ---------------: | -----------: | ---------- |
| mixed_random_70r_30w              |                 32 |              40.7 |                142.9 |           81.9 |          214.0 |             75.5 |          2.2 | [1][5][9]  |
| mixed_random_70r_30w              |                256 |              48.0 |                222.2 |           89.8 |          256.5 |             81.8 |          2.2 | [1][5][9]  |
| mixed_random_70r_30w              |              4,096 |              32.6 |                 42.7 |           66.2 |          153.1 |             57.6 |          2.2 | [1][5][9]  |
| mixed_random_70r_30w              |          1,048,576 |               0.9 |                  0.1 |            N/A |            1.8 |              1.4 |          0.2 | [2][5][9]  |
| mixed_random_50r_50w_with_updates |                 32 |              40.3 |                 99.7 |           98.1 |          265.2 |             77.3 |          2.2 | [1][5][9]  |
| mixed_random_50r_50w_with_updates |                256 |              47.6 |                176.5 |           77.4 |          263.0 |             84.1 |          2.2 | [1][5][9]  |
| mixed_random_50r_50w_with_updates |              4,096 |              32.3 |                 35.2 |           56.4 |          143.2 |             49.3 |          2.3 | [1][5][9]  |
| mixed_random_50r_50w_with_updates |          1,048,576 |               0.8 |                  0.1 |            N/A |            1.8 |              1.3 |          0.2 | [2][5][9]  |
| mixed_random_60r_20w_20d          |                 32 |              40.4 |                154.6 |           95.2 |          298.1 |             78.7 |          2.2 | [1][5][9]  |
| mixed_random_60r_20w_20d          |                256 |              48.1 |                217.4 |           90.3 |          256.3 |             85.5 |          2.2 | [1][5][9]  |
| mixed_random_60r_20w_20d          |              4,096 |              36.8 |                 35.8 |           71.7 |          176.4 |             63.3 |          2.2 | [1][5][9]  |
| mixed_random_60r_20w_20d          |          1,048,576 |               0.9 |                  0.1 |            N/A |            3.4 |              3.0 |          0.2 | [2][5][9]  |
| range_seq_window_64               |                 32 |             151.9 |                123.5 |          302.4 |           89.6 |            110.3 |          9.0 | [8][11]    |
| range_seq_window_64               |                256 |             148.9 |                114.5 |          268.2 |           75.7 |            109.2 |          9.0 | [8][11]    |
| range_seq_window_64               |              4,096 |               3.0 |                  2.1 |           35.9 |           27.2 |             66.4 |         11.3 | [8][11]    |
| range_seq_window_64               |          1,048,576 |               1.0 |                  0.3 |            N/A |            0.5 |              0.4 |          0.0 | [8][12]    |
| range_random_window_64            |                 32 |             149.3 |                135.7 |          312.9 |           89.8 |            110.8 |          8.9 | [8][11]    |
| range_random_window_64            |                256 |             144.3 |                100.3 |          276.7 |           78.0 |            106.9 |          9.0 | [8][11]    |
| range_random_window_64            |              4,096 |               2.0 |                  0.3 |           25.5 |           26.9 |             67.6 |         11.4 | [8][11]    |
| range_random_window_64            |          1,048,576 |               1.1 |                  0.4 |            N/A |            0.5 |              0.5 |          0.0 | [8][12]    |
| cold_seq_read                     |                 32 |            2491.9 |                882.4 |          890.8 |          716.1 |            268.0 |         18.5 | [3][10]    |
| cold_seq_read                     |                256 |            1658.4 |                500.0 |          863.3 |          626.8 |            220.1 |         18.4 | [3][10]    |
| cold_seq_read                     |              4,096 |             279.4 |                 49.8 |          297.6 |          297.4 |            181.7 |         18.9 | [3][10]    |
| cold_seq_read                     |          1,048,576 |               2.4 |                  0.1 |            N/A |            4.4 |              0.9 |          0.2 | [4][5][10] |
| cold_random_read                  |                 32 |            2154.1 |                937.5 |          694.2 |          664.3 |            261.4 |         18.6 | [3][10]    |
| cold_random_read                  |                256 |            1560.0 |                379.7 |          654.2 |          641.7 |            262.7 |         18.5 | [3][10]    |
| cold_random_read                  |              4,096 |             265.4 |                 33.7 |          280.2 |          294.6 |            166.4 |         18.8 | [3][10]    |
| cold_random_read                  |          1,048,576 |               2.4 |                  0.1 |            N/A |            4.6 |              1.1 |          0.2 | [4][5][10] |

**Notes**

[1] Mixed scenario native comparator rows come from the comparative mixed harness runs.
WASM mixed rows come from OPFS runners.

[2] Mixed `1,048,576` rows use reduced `count=64`.
`bf-tree` does not support 1MB values in this harness and is therefore `N/A` for those rows.

[3] Cold native rows come from Criterion cross-engine groups (`--quick`) with `count=5000` for `32/256/4096`:
`compare_native_cold_seq_read` and `compare_native_cold_random_read`.
`bf-tree` is included for supported sizes (`<= 30KB` in this harness).
WASM cold rows come from OPFS runners with `--include-cold-read`.

[4] Cold `1,048,576` rows use reduced `count=64` for both native and wasm.

[5] `N/A` means the scenario/runtime combination is currently not benchmarked or unsupported.

[6] Historical `opfs-btree wasm/opfs` mixed rows for the incremental-checkpoint phase A/B used median of 3 runs with seed `0xA5A5A5A501234567`:
`node wasm-bench/run-opfs-bench.cjs --profile mixed --count 5000 --value-sizes 32,256,4096 --seed 0xA5A5A5A501234567 --json`
and
`node wasm-bench/run-opfs-bench.cjs --profile mixed --count 64 --value-sizes 1048576 --seed 0xA5A5A5A501234567 --json`.

[7] Current `opfs-btree wasm/opfs` mixed rows reflect the lazy-page-loading phase A/B medians over 3 runs (same seed/commands as [6]), with baseline at commit `b3fe6ae`.

[8] `opfs-btree wasm/opfs` range rows (`32/256/4096`) reflect the latest read-coalescing run (`--profile all --include-cold-read`, `count=3000`, seed `0xA5A5A5A501234567`, `cache=32MB`, `pin_internal_pages=true`, `read_coalesce_pages=4`).
`1,048,576` range rows are still carried forward from the prior baseline (`count=128`).

[9] Current top-table `opfs-btree wasm/opfs` mixed rows for `32/256/4096`
reflect the latest read-coalescing run with `pin_internal_pages=true`,
seed `0xA5A5A5A501234567`, `count=3000`, `cache=32MB`, `read_coalesce_pages=4` (single run).
`1,048,576` rows are still carried forward from the prior baseline (`count=64`).

[10] Current top-table `opfs-btree wasm/opfs` cold rows for `32/256/4096` reflect the same read-coalescing run as [8]/[9].
`1,048,576` cold rows are still carried forward from the prior baseline (`count=64`).

[11] Native range rows come from restored cross-engine Criterion groups
`compare_native_range_seq_window` and `compare_native_range_random_window`
in quick mode, with `key_count=5000` and `query_count=2000` for `32/256/4096`.

[12] Native `1,048,576` range rows use reduced counts (`key_count=16`, `query_count=4`).
`bf-tree` remains `N/A` at `1,048,576` due value-size constraints in this harness.

## Range Query Benchmarks (opfs-btree wasm/opfs)

Workload details:

- `range_seq_window_64`: deterministic sequential start positions.
- `range_random_window_64`: random start positions from deterministic RNG.
- Both use a prefilled keyspace and execute bounded range queries with `limit=64`.

| operation              | value_size | count | median K/s | median p95 ms |
| ---------------------- | ---------: | ----: | ---------: | ------------: |
| range_seq_window_64    |         32 |  3000 |      123.5 |         0.100 |
| range_seq_window_64    |        256 |  3000 |      114.5 |         0.100 |
| range_seq_window_64    |       4096 |  3000 |        2.1 |         0.600 |
| range_seq_window_64    |    1048576 |   128 |        0.3 |         3.100 |
| range_random_window_64 |         32 |  3000 |      135.7 |         0.100 |
| range_random_window_64 |        256 |  3000 |      100.3 |         0.100 |
| range_random_window_64 |       4096 |  3000 |        0.3 |         4.700 |
| range_random_window_64 |    1048576 |   128 |        0.4 |         3.000 |

`1,048,576` range rows above are currently retained from the prior baseline run.

## Phase: Incremental Checkpoint (Before/After)

Before = `HEAD` at `71e16af`.
After = dirty-page incremental checkpoint changes.
Values are medians across 3 runs, in `K/s`.

| scenario                          | value_size | before median (K/s) | after median (K/s) |   delta |
| --------------------------------- | ---------: | ------------------: | -----------------: | ------: |
| mixed_random_70r_30w              |         32 |               141.2 |              138.5 |   -1.9% |
| mixed_random_70r_30w              |        256 |               112.6 |              114.4 |   +1.6% |
| mixed_random_70r_30w              |       4096 |                12.8 |               31.1 | +144.0% |
| mixed_random_70r_30w              |    1048576 |                 0.1 |                0.3 | +213.1% |
| mixed_random_50r_50w_with_updates |         32 |                98.4 |               98.8 |   +0.4% |
| mixed_random_50r_50w_with_updates |        256 |                94.9 |               98.8 |   +4.2% |
| mixed_random_50r_50w_with_updates |       4096 |                12.4 |               20.8 |  +68.2% |
| mixed_random_50r_50w_with_updates |    1048576 |                 0.1 |                0.3 | +180.5% |
| mixed_random_60r_20w_20d          |         32 |               142.5 |              140.8 |   -1.1% |
| mixed_random_60r_20w_20d          |        256 |               113.6 |              115.5 |   +1.6% |
| mixed_random_60r_20w_20d          |       4096 |                12.8 |               25.5 |  +99.6% |
| mixed_random_60r_20w_20d          |    1048576 |                 0.1 |                0.9 | +578.3% |

## Phase: Lazy Page Loading (Before/After)

Before = `b3fe6ae` (incremental checkpoint only).
After = lazy page loading changes.
Values are medians across 3 runs, in `K/s`.

| scenario                          | value_size | before median (K/s) | after median (K/s) | delta |
| --------------------------------- | ---------: | ------------------: | -----------------: | ----: |
| mixed_random_70r_30w              |         32 |               137.4 |              138.1 | +0.6% |
| mixed_random_70r_30w              |        256 |               113.6 |              116.6 | +2.6% |
| mixed_random_70r_30w              |       4096 |                31.9 |               32.0 | +0.3% |
| mixed_random_70r_30w              |    1048576 |                 0.3 |                0.3 | -0.6% |
| mixed_random_50r_50w_with_updates |         32 |                99.0 |               97.3 | -1.8% |
| mixed_random_50r_50w_with_updates |        256 |                99.8 |              101.6 | +1.8% |
| mixed_random_50r_50w_with_updates |       4096 |                21.5 |               20.9 | -2.9% |
| mixed_random_50r_50w_with_updates |    1048576 |                 0.3 |                0.3 | +4.4% |
| mixed_random_60r_20w_20d          |         32 |               139.7 |              144.1 | +3.2% |
| mixed_random_60r_20w_20d          |        256 |               115.5 |              116.8 | +1.2% |
| mixed_random_60r_20w_20d          |       4096 |                26.1 |               25.8 | -1.1% |
| mixed_random_60r_20w_20d          |    1048576 |                 0.9 |                0.9 | +0.3% |

## Cache Sizing Sweep (WASM/OPFS)

Method:

- 3 runs per cache size, deterministic seed `0xA5A5A5A501234567`.
- Mixed profile: `count=3000`, sizes `32/256/4096`.
- Range profile: `count=2000`, sizes `32/256/4096`.
- Values below are mean-of-row-medians in `K/s`.

| cache_mb | mixed aggregate K/s | range aggregate K/s | combined aggregate K/s | delta vs 32MB |
| -------: | ------------------: | ------------------: | ---------------------: | ------------: |
|        2 |                81.4 |                48.3 |                   68.2 |        -18.7% |
|        8 |                80.9 |                80.0 |                   80.5 |         -4.1% |
|       16 |                80.9 |                77.9 |                   79.7 |         -5.1% |
|       24 |                85.5 |                77.8 |                   82.4 |         -1.8% |
|       32 |                87.2 |                79.0 |                   83.9 |         +0.0% |
|       50 |                85.0 |                79.3 |                   82.7 |         -1.4% |

Decision: default cache is set to `32MB`.

## Phase: Lazy Page Loading -> 32MB Default Cache (Before/After)

Before = Lazy Page Loading phase medians (`after` column in the section above).
After = current top-table medians with `32MB` cache default.
Values are in `K/s`.

Aggregate deltas:

| metric                        | before (Lazy phase) | after (32MB default) | delta |
| ----------------------------- | ------------------: | -------------------: | ----: |
| mixed aggregate (all sizes)   |                66.2 |                 65.4 | -1.2% |
| mixed aggregate (32/256/4096) |                88.1 |                 87.2 | -1.0% |

Mixed scenarios:

| scenario                          | value_size | before (Lazy phase) | after (32MB default) |  delta |
| --------------------------------- | ---------: | ------------------: | -------------------: | -----: |
| mixed_random_70r_30w              |         32 |               138.1 |                125.5 |  -9.1% |
| mixed_random_70r_30w              |        256 |               116.6 |                117.6 |  +0.9% |
| mixed_random_70r_30w              |       4096 |                32.0 |                 31.8 |  -0.6% |
| mixed_random_70r_30w              |    1048576 |                 0.3 |                  0.1 | -66.7% |
| mixed_random_50r_50w_with_updates |         32 |                97.3 |                 92.9 |  -4.5% |
| mixed_random_50r_50w_with_updates |        256 |               101.6 |                106.4 |  +4.7% |
| mixed_random_50r_50w_with_updates |       4096 |                20.9 |                 21.4 |  +2.4% |
| mixed_random_50r_50w_with_updates |    1048576 |                 0.3 |                  0.1 | -66.7% |
| mixed_random_60r_20w_20d          |         32 |               144.1 |                145.6 |  +1.0% |
| mixed_random_60r_20w_20d          |        256 |               116.8 |                118.6 |  +1.5% |
| mixed_random_60r_20w_20d          |       4096 |                25.8 |                 25.2 |  -2.3% |
| mixed_random_60r_20w_20d          |    1048576 |                 0.9 |                  0.1 | -88.9% |

## Phase: Internal Page Pinning + OPFS IO Counters

Changes in this phase:

- Added `BTreeOptions.pin_internal_pages` and enabled it for WASM OPFS runtime/bench options.
- Added OPFS IO counters (`read/write calls + bytes, len/truncate/flush calls`) to wasm benchmark result payloads.
- Reduced per-call JS object churn in `OpfsFile` by reusing `FileSystemReadWriteOptions` objects.

### A/B: Mixed Profile (32MB cache, single run, fixed seed)

Command shape (before/after): `--profile mixed --count 3000 --value-sizes 32,256,4096 --seed 0xA5A5A5A501234567 --cache-mb 32 --pin-internal-pages false|true --json`

| scenario                          | value_size | before K/s | after K/s |  delta |
| --------------------------------- | ---------: | ---------: | --------: | -----: |
| mixed_random_70r_30w              |         32 |      122.0 |     131.6 |  +7.9% |
| mixed_random_70r_30w              |        256 |      113.2 |     114.9 |  +1.5% |
| mixed_random_70r_30w              |       4096 |       29.2 |      32.2 | +10.2% |
| mixed_random_50r_50w_with_updates |         32 |       94.3 |      97.7 |  +3.6% |
| mixed_random_50r_50w_with_updates |        256 |      104.2 |     105.3 |  +1.1% |
| mixed_random_50r_50w_with_updates |       4096 |       20.0 |      20.3 |  +1.9% |
| mixed_random_60r_20w_20d          |         32 |      154.6 |     147.1 |  -4.9% |
| mixed_random_60r_20w_20d          |        256 |      117.2 |     117.6 |  +0.4% |
| mixed_random_60r_20w_20d          |       4096 |       24.7 |      25.3 |  +2.4% |

Aggregate (sum of rows): `+1.6%` throughput.
OPFS call counts in this profile were unchanged because this workload remained mostly memory-resident during the measured operation window.

### A/B: Cold-Read Stress (16KB cache, 4096-byte values)

Command shape (before/after): `--profile basic --include-cold-read --count 3000 --value-sizes 4096 --seed 0xA5A5A5A501234567 --cache-mb 0.015625 --pin-internal-pages false|true --json`

| scenario         | before K/s | after K/s |   delta | read calls before | read calls after | read call delta |
| ---------------- | ---------: | --------: | ------: | ----------------: | ---------------: | --------------: |
| cold_seq_read    |        4.6 |      16.9 | +265.5% |              6003 |             1506 |          -74.9% |
| cold_random_read |        4.8 |       8.3 |  +74.3% |              6003 |             3004 |          -50.0% |

This stress run confirms the intended mechanism: pinning internal pages can materially reduce OPFS read call volume under heavy eviction pressure.

## Phase: Checkpoint Write Coalescing

Changes in this phase:

- Checkpoint now groups contiguous dirty/freelist pages into page runs and writes each run with one `write_all_at` call.
- Total bytes written are unchanged; OPFS write call count is reduced.

### A/B: Mixed Profile (32MB cache, single run, fixed seed)

Before was captured on previous code at the same settings.
After uses coalesced checkpoint writes.

Command shape: `--profile mixed --count 3000 --value-sizes 32,256,4096 --seed 0xA5A5A5A501234567 --cache-mb 32 --pin-internal-pages true --json`

| scenario                          | value_size | before K/s | after K/s |  delta |
| --------------------------------- | ---------: | ---------: | --------: | -----: |
| mixed_random_70r_30w              |         32 |      121.0 |     125.5 |  +3.8% |
| mixed_random_70r_30w              |        256 |      110.3 |     209.8 | +90.2% |
| mixed_random_70r_30w              |       4096 |       31.8 |      39.1 | +22.9% |
| mixed_random_50r_50w_with_updates |         32 |       95.2 |      97.7 |  +2.6% |
| mixed_random_50r_50w_with_updates |        256 |      103.4 |     175.4 | +69.6% |
| mixed_random_50r_50w_with_updates |       4096 |       21.0 |      29.4 | +40.3% |
| mixed_random_60r_20w_20d          |         32 |      147.1 |     160.4 |  +9.1% |
| mixed_random_60r_20w_20d          |        256 |      114.5 |     214.3 | +87.1% |
| mixed_random_60r_20w_20d          |       4096 |       24.5 |      32.2 | +31.1% |

Aggregate (sum of rows): `768.8 K/s -> 1083.9 K/s` (`+41.0%`).

### OPFS IO Counter Delta (same mixed run)

| metric                 | before | after |  delta |
| ---------------------- | -----: | ----: | -----: |
| opfs write calls       |   2481 |  1090 | -56.1% |
| opfs write bytes (MiB) |   38.8 |  38.8 |  +0.0% |

Interpretation: coalescing reduced call overhead substantially while writing the same amount of data.

## Phase: Read Coalescing

Changes in this phase:

- Added `BTreeOptions.read_coalesce_pages` (default `1`), and enabled `read_coalesce_pages=4` in groove OPFS storage + wasm bench defaults.
- `ensure_page_loaded` now reads a contiguous on-disk page run (up to configured run length) and caches decoded pages.
- Added unit coverage for read-call reduction and eviction safety around prefetched pages.

### A/B: WASM OPFS profile all + cold (single run, fixed seed)

- Before: `read_coalesce_pages=1`
- After: `read_coalesce_pages=4`
- Command shape: `--profile all --include-cold-read --count 3000 --value-sizes 32,256,4096 --seed 0xA5A5A5A501234567 --cache-mb 32 --pin-internal-pages true --read-coalesce-pages {1|4} --json`

Aggregate deltas:

| metric            | before K/s | after K/s |  delta |
| ----------------- | ---------: | --------: | -----: |
| mixed aggregate   |     1120.1 |    1126.9 |  +0.6% |
| range aggregate   |      484.2 |     476.5 |  -1.6% |
| cold aggregate    |     1831.1 |    2783.0 | +52.0% |
| overall aggregate |     3435.5 |    4386.4 | +27.7% |

Selected per-scenario deltas:

| scenario                          | value_size | before K/s | after K/s |   delta | read calls before | read calls after |
| --------------------------------- | ---------: | ---------: | --------: | ------: | ----------------: | ---------------: |
| mixed_random_70r_30w              |         32 |      130.4 |     142.9 |   +9.5% |                 0 |                0 |
| mixed_random_70r_30w              |        256 |      222.2 |     222.2 |   +0.0% |                 0 |                0 |
| mixed_random_70r_30w              |       4096 |       43.4 |      42.7 |   -1.6% |                 0 |                0 |
| mixed_random_50r_50w_with_updates |         32 |       96.2 |      99.7 |   +3.7% |                 0 |                0 |
| mixed_random_50r_50w_with_updates |        256 |      178.6 |     176.5 |   -1.2% |                 0 |                0 |
| mixed_random_50r_50w_with_updates |       4096 |       35.1 |      35.2 |   +0.4% |                 0 |                0 |
| mixed_random_60r_20w_20d          |         32 |      157.9 |     154.6 |   -2.1% |                 0 |                0 |
| mixed_random_60r_20w_20d          |        256 |      220.6 |     217.4 |   -1.4% |                 0 |                0 |
| mixed_random_60r_20w_20d          |       4096 |       35.8 |      35.8 |   -0.2% |                 0 |                0 |
| range_seq_window_64               |         32 |      125.5 |     123.5 |   -1.6% |                 0 |                0 |
| range_seq_window_64               |        256 |      114.9 |     114.5 |   -0.4% |                 0 |                0 |
| range_seq_window_64               |       4096 |        1.4 |       2.1 |  +54.7% |             10558 |             2646 |
| range_random_window_64            |         32 |      141.5 |     135.7 |   -4.1% |                 0 |                0 |
| range_random_window_64            |        256 |      100.7 |     100.3 |   -0.3% |                 0 |                0 |
| range_random_window_64            |       4096 |        0.2 |       0.3 |  +47.8% |             64768 |            17316 |
| cold_seq_read                     |         32 |      625.0 |     882.4 |  +41.2% |                21 |                8 |
| cold_seq_read                     |        256 |      234.4 |     500.0 | +113.3% |               103 |               28 |
| cold_seq_read                     |       4096 |       19.0 |      49.8 | +162.0% |              1506 |              380 |
| cold_random_read                  |         32 |      697.7 |     937.5 |  +34.4% |                21 |                8 |
| cold_random_read                  |        256 |      236.2 |     379.7 |  +60.8% |               103 |               42 |
| cold_random_read                  |       4096 |       18.9 |      33.7 |  +78.5% |              1506 |              603 |

## Phase: On-Page Slot Directory (Regression Note)

Changes in this phase:

- Internal and leaf page formats moved to an on-page slot-directory layout (cell-pointer-array style).
- Raw read helpers now consume slot directory offsets directly from page buffers.
- Removed transient raw leaf index map from `db.rs`; lookup metadata now lives in the page itself.

Benchmark method:

- Compared against the read-coalescing baseline (`read_coalesce_pages=4`).
- Command shape: `--profile all --include-cold-read --count 3000 --value-sizes 32,256,4096 --seed 0xA5A5A5A501234567 --cache-mb 32 --pin-internal-pages true --read-coalesce-pages 4 --json`
- 3 runs due browser/worker variance.

| run    | mixed delta | range delta | cold delta | overall delta |
| ------ | ----------: | ----------: | ---------: | ------------: |
| 1      |       -4.1% |      +18.5% |     -36.3% |        -22.1% |
| 2      |       +3.6% |      +23.2% |      -9.1% |         -2.3% |
| 3      |       -0.9% |      +27.0% |      -8.7% |         -2.8% |
| median |       -0.9% |      +23.2% |      -9.1% |         -2.8% |

Interpretation: the slot-directory format consistently improves range throughput, but cold point-read throughput regressed versus the prior read-coalescing baseline.

## Phase: Raw-Only Page Cache (Decode-On-Demand)

Changes in this phase:

- Removed the mixed raw/decoded cache model in `db.rs`; cache entries are now always raw page bytes.
- Mutation paths (`put`/`delete`) decode page bytes only for the active edit and re-encode immediately on dirty-set.
- Read paths (`get`/`range`/tree descent/overflow) stay fully on the raw slot-directory helpers.

Benchmark method:

- Before = slot-directory baseline runs:
  - `/tmp/opfs_slotdir_current.json`
  - `/tmp/opfs_slotdir_current2.json`
  - `/tmp/opfs_slotdir_current3.json`
- After = raw-only cache runs:
  - `/tmp/opfs_raw_only_run1.json`
  - `/tmp/opfs_raw_only_run2.json`
  - `/tmp/opfs_raw_only_run3.json`
- Command shape: `--profile all --include-cold-read --count 3000 --value-sizes 32,256,4096 --seed 0xA5A5A5A501234567 --cache-mb 32 --pin-internal-pages true --read-coalesce-pages 4 --json`

Run deltas (before -> after):

| run    | mixed delta | range delta | cold delta | overall delta |
| ------ | ----------: | ----------: | ---------: | ------------: |
| 1      |       +9.7% |      +10.1% |     +45.6% |        +28.4% |
| 2      |       +3.1% |       +9.2% |      +5.4% |         +5.3% |
| 3      |       -1.0% |       +0.5% |      +0.5% |         +0.1% |
| median |       +3.1% |       +9.2% |      +5.4% |         +5.3% |

Aggregate medians:

| metric            | before K/s | after K/s | delta |
| ----------------- | ---------: | --------: | ----: |
| mixed aggregate   |     1116.3 |    1185.5 | +6.2% |
| range aggregate   |      586.8 |     621.9 | +6.0% |
| cold aggregate    |     2529.4 |    2582.9 | +2.1% |
| overall aggregate |     4263.3 |    4390.3 | +3.0% |

## Full Phase Comparison: Read Coalescing -> Current

Method:

- Baseline = read-coalescing phase result (`/tmp/opfs_read_coalesce_after.json`).
- Current = median of:
  - `/tmp/opfs_raw_only_run1.json`
  - `/tmp/opfs_raw_only_run2.json`
  - `/tmp/opfs_raw_only_run3.json`
- Profile/settings matched: `--profile all --include-cold-read --count 3000 --value-sizes 32,256,4096 --seed 0xA5A5A5A501234567 --cache-mb 32 --pin-internal-pages true --read-coalesce-pages 4 --json`

Aggregate:

| metric            | read-coalescing K/s | current K/s |  delta |
| ----------------- | ------------------: | ----------: | -----: |
| mixed aggregate   |              1126.9 |      1182.8 |  +5.0% |
| range aggregate   |               476.5 |       622.6 | +30.7% |
| cold aggregate    |              2783.0 |      2600.0 |  -6.6% |
| overall aggregate |              4386.4 |      4405.4 |  +0.4% |

Per-scenario and size:

| scenario                          | value_size | read-coalescing K/s | current K/s |  delta |
| --------------------------------- | ---------: | ------------------: | ----------: | -----: |
| cold_random_read                  |         32 |               937.5 |       857.1 |  -8.6% |
| cold_random_read                  |        256 |               379.7 |       405.4 |  +6.8% |
| cold_random_read                  |       4096 |                33.7 |        35.6 |  +5.7% |
| cold_seq_read                     |         32 |               882.4 |       731.7 | -17.1% |
| cold_seq_read                     |        256 |               500.0 |       517.2 |  +3.4% |
| cold_seq_read                     |       4096 |                49.8 |        52.9 |  +6.3% |
| mixed_random_50r_50w_with_updates |         32 |                99.7 |       104.9 |  +5.2% |
| mixed_random_50r_50w_with_updates |        256 |               176.5 |       184.0 |  +4.3% |
| mixed_random_50r_50w_with_updates |       4096 |                35.2 |        37.6 |  +6.8% |
| mixed_random_60r_20w_20d          |         32 |               154.6 |       170.5 | +10.2% |
| mixed_random_60r_20w_20d          |        256 |               217.4 |       234.4 |  +7.8% |
| mixed_random_60r_20w_20d          |       4096 |                35.8 |        40.7 | +13.7% |
| mixed_random_70r_30w              |         32 |               142.9 |       137.6 |  -3.7% |
| mixed_random_70r_30w              |        256 |               222.2 |       227.3 |  +2.3% |
| mixed_random_70r_30w              |       4096 |                42.7 |        45.9 |  +7.5% |
| range_random_window_64            |         32 |               135.7 |       176.5 | +30.0% |
| range_random_window_64            |        256 |               100.3 |       141.5 | +41.0% |
| range_random_window_64            |       4096 |                 0.3 |         0.3 |  -6.0% |
| range_seq_window_64               |         32 |               123.5 |       149.3 | +20.9% |
| range_seq_window_64               |        256 |               114.5 |       153.1 | +33.7% |
| range_seq_window_64               |       4096 |                 2.1 |         2.0 |  -4.2% |

## Phase: In-Place Leaf Mutation (Raw Slotted Pages)

Changes in this phase:

- Added raw in-place leaf helpers in `page.rs`:
  - `raw_leaf_upsert_in_place`
  - `raw_leaf_delete_in_place`
- Wired leaf fast paths in `db.rs` for `insert_recursive` and `delete_recursive`.
- Kept decode/split fallback for cases where in-page upsert does not fit (`NeedSplit`).

Benchmark method:

- Before = raw-only cache phase medians from:
  - `/tmp/opfs_raw_only_run1.json`
  - `/tmp/opfs_raw_only_run2.json`
  - `/tmp/opfs_raw_only_run3.json`
- After = this phase medians from:
  - `/tmp/opfs_inplace_run1.json`
  - `/tmp/opfs_inplace_run2.json`
  - `/tmp/opfs_inplace_run3.json`
- Command shape: `--profile all --include-cold-read --count 3000 --value-sizes 32,256,4096 --seed 0xA5A5A5A501234567 --cache-mb 32 --pin-internal-pages true --read-coalesce-pages 4 --json`

Run deltas (before -> after):

| run    | mixed delta | range delta | cold delta | overall delta |
| ------ | ----------: | ----------: | ---------: | ------------: |
| 1      |       -4.7% |       -0.6% |     -13.4% |         -9.2% |
| 2      |       -3.3% |       -6.1% |      -0.6% |         -2.1% |
| 3      |       +1.5% |       -0.2% |      -6.0% |         -3.2% |
| median |       -3.3% |       -0.6% |      -6.0% |         -3.2% |

Aggregate medians:

| metric            | before K/s | after K/s | delta |
| ----------------- | ---------: | --------: | ----: |
| mixed aggregate   |     1182.8 |    1142.2 | -3.4% |
| range aggregate   |      622.6 |     607.3 | -2.5% |
| cold aggregate    |     2600.0 |    2543.5 | -2.2% |
| overall aggregate |     4405.4 |    4293.0 | -2.6% |

Interpretation: this first in-place implementation regressed overall throughput versus the raw-only baseline.
