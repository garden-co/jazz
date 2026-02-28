# opfs-btree Benchmark Overview

Generated on 2026-02-16.

Single baseline table for further `opfs-btree` work. All values are throughput in `K/s` (1 K = 1,000 ops/s), rounded to one decimal.
Current `opfs-btree` default cache is `32MB`.

| Scenario                          | Value Size (bytes) | opfs-btree native | opfs-btree wasm/opfs | bf-tree native | rocksdb native | surrealkv native | fjall native | Notes          |
| --------------------------------- | -----------------: | ----------------: | -------------------: | -------------: | -------------: | ---------------: | -----------: | -------------- |
| mixed_random_70r_30w              |                 32 |              40.7 |                152.3 |           81.9 |          214.0 |            407.1 |        364.5 | [1][5][9][13]  |
| mixed_random_70r_30w              |                256 |              48.0 |                225.6 |           89.8 |          256.5 |            368.9 |        339.9 | [1][5][9][13]  |
| mixed_random_70r_30w              |              4,096 |              32.6 |                 46.2 |           66.2 |          153.1 |            202.9 |        277.0 | [1][5][9][13]  |
| mixed_random_70r_30w              |          1,048,576 |               0.9 |                  0.5 |            N/A |            1.8 |              2.6 |          0.9 | [2][5][9][13]  |
| mixed_random_50r_50w_with_updates |                 32 |              40.3 |                107.5 |           98.1 |          265.2 |            363.4 |        330.9 | [1][5][9][13]  |
| mixed_random_50r_50w_with_updates |                256 |              47.6 |                184.0 |           77.4 |          263.0 |            328.5 |        346.6 | [1][5][9][13]  |
| mixed_random_50r_50w_with_updates |              4,096 |              32.3 |                 37.5 |           56.4 |          143.2 |            165.3 |        223.5 | [1][5][9][13]  |
| mixed_random_50r_50w_with_updates |          1,048,576 |               0.8 |                  0.5 |            N/A |            1.8 |              1.5 |          0.8 | [2][5][9][13]  |
| mixed_random_60r_20w_20d          |                 32 |              40.4 |                166.7 |           95.2 |          298.1 |            374.6 |        349.2 | [1][5][9][13]  |
| mixed_random_60r_20w_20d          |                256 |              48.1 |                232.6 |           90.3 |          256.3 |            378.3 |        354.4 | [1][5][9][13]  |
| mixed_random_60r_20w_20d          |              4,096 |              36.8 |                 40.7 |           71.7 |          176.4 |            227.1 |        302.1 | [1][5][9][13]  |
| mixed_random_60r_20w_20d          |          1,048,576 |               0.9 |                  1.3 |            N/A |            3.4 |              2.3 |          1.0 | [2][5][9][13]  |
| range_seq_window_64               |                 32 |             151.9 |                151.5 |          302.4 |           89.6 |            116.0 |         83.8 | [8][11][13]    |
| range_seq_window_64               |                256 |             148.9 |                154.6 |          268.2 |           75.7 |            109.3 |         90.9 | [8][11][13]    |
| range_seq_window_64               |              4,096 |               3.0 |                  2.0 |           35.9 |           27.2 |             72.3 |        104.6 | [8][11][13]    |
| range_seq_window_64               |          1,048,576 |               1.0 |                  0.1 |            N/A |            0.5 |              0.4 |          1.6 | [8][12][13]    |
| range_random_window_64            |                 32 |             149.3 |                172.4 |          312.9 |           89.8 |            108.5 |         89.3 | [8][11][13]    |
| range_random_window_64            |                256 |             144.3 |                141.5 |          276.7 |           78.0 |            107.3 |         87.2 | [8][11][13]    |
| range_random_window_64            |              4,096 |               2.0 |                  0.3 |           25.5 |           26.9 |             71.6 |         81.9 | [8][11][13]    |
| range_random_window_64            |          1,048,576 |               1.1 |                  0.1 |            N/A |            0.5 |              0.4 |          1.6 | [8][12][13]    |
| cold_seq_read                     |                 32 |            2491.9 |                750.0 |          890.8 |          716.1 |            260.9 |        218.4 | [3][10][13]    |
| cold_seq_read                     |                256 |            1658.4 |                483.9 |          863.3 |          626.8 |            243.6 |        215.8 | [3][10][13]    |
| cold_seq_read                     |              4,096 |             279.4 |                 52.4 |          297.6 |          297.4 |            148.5 |        187.3 | [3][10][13]    |
| cold_seq_read                     |          1,048,576 |               2.4 |                  2.9 |            N/A |            4.4 |              1.2 |          3.7 | [4][5][10][13] |
| cold_random_read                  |                 32 |            2154.1 |                882.4 |          694.2 |          664.3 |            242.7 |        222.0 | [3][10][13]    |
| cold_random_read                  |                256 |            1560.0 |                400.0 |          654.2 |          641.7 |            257.9 |        188.5 | [3][10][13]    |
| cold_random_read                  |              4,096 |             265.4 |                 35.3 |          280.2 |          294.6 |            172.9 |        188.0 | [3][10][13]    |
| cold_random_read                  |          1,048,576 |               2.4 |                  3.1 |            N/A |            4.6 |              1.0 |          3.8 | [4][5][10][13] |

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

[8] `opfs-btree wasm/opfs` range rows (`32/256/4096`) reflect the latest direct-slot-walk phase medians over 3 runs (`--profile all --include-cold-read`, `count=3000`, seed `0xA5A5A5A501234567`, `cache=32MB`, `pin_internal_pages=true`, `read_coalesce_pages=4`).
`1,048,576` range rows are now from the overflow-threshold tuning medians over 3 runs (`--profile all --include-cold-read`, `count=64`, `overflow-threshold-bytes=4096`, same seed/cache/options).

[9] Current top-table `opfs-btree wasm/opfs` mixed rows for `32/256/4096`
reflect the latest direct-slot-walk phase medians over 3 runs with `pin_internal_pages=true`,
seed `0xA5A5A5A501234567`, `count=3000`, `cache=32MB`, `read_coalesce_pages=4`.
`1,048,576` mixed rows are now from overflow-threshold tuning medians over 3 runs
(`--profile all --include-cold-read --count 64 --value-sizes 1048576 --overflow-threshold-bytes=4096`, same seed/cache/options).

[10] Current top-table `opfs-btree wasm/opfs` cold rows for `32/256/4096` reflect the same direct-slot-walk phase medians as [8]/[9].
`1,048,576` cold rows are now from overflow-threshold tuning medians over 3 runs
(`--profile all --include-cold-read --count 64 --value-sizes 1048576 --overflow-threshold-bytes=4096`, same seed/cache/options).

[11] Native range rows come from restored cross-engine Criterion groups
`compare_native_range_seq_window` and `compare_native_range_random_window`
in quick mode, with `key_count=5000` and `query_count=2000` for `32/256/4096`.

[12] Native `1,048,576` range rows use reduced counts (`key_count=16`, `query_count=4`).
`bf-tree` remains `N/A` at `1,048,576` due value-size constraints in this harness.

[13] `surrealkv`/`fjall` native columns were refreshed on 2026-02-16 after dependency updates
to `surrealkv=0.20.1` and `fjall=3.0.2` by rerunning native Criterion harnesses with
`cargo +1.91.0 bench` and `JAZZ_COMPARE_ENGINES=surrealkv,fjall`.
Other native columns (`opfs-btree`, `bf-tree`, `rocksdb`) are carried forward from prior runs.

## Range Query Benchmarks (opfs-btree wasm/opfs)

Workload details:

- `range_seq_window_64`: deterministic sequential start positions.
- `range_random_window_64`: random start positions from deterministic RNG.
- Both use a prefilled keyspace and execute bounded range queries with `limit=64`.

| operation              | value_size | count | median K/s | median p95 ms |
| ---------------------- | ---------: | ----: | ---------: | ------------: |
| range_seq_window_64    |         32 |  3000 |      151.5 |         0.100 |
| range_seq_window_64    |        256 |  3000 |      154.6 |         0.100 |
| range_seq_window_64    |       4096 |  3000 |        2.0 |         0.700 |
| range_seq_window_64    |    1048576 |   128 |        0.3 |         3.100 |
| range_random_window_64 |         32 |  3000 |      172.4 |         0.100 |
| range_random_window_64 |        256 |  3000 |      141.5 |         0.100 |
| range_random_window_64 |       4096 |  3000 |        0.3 |         5.000 |
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

- Added `BTreeOptions.read_coalesce_pages` (default `1`), and enabled `read_coalesce_pages=4` in jazz OPFS storage + wasm bench defaults.
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

## Phase: In-Place Leaf Mutation Follow-Up (Tail Allocator + Slot Hint)

Changes in this phase:

- Kept the raw in-place leaf fast path, but switched from full payload rewrite to:
  - slot-only shifts for insert/delete,
  - tail allocation for new key/value cells,
  - in-place overwrite for same-key updates when the new value cell fits.
- Added leaf `data_start` hint in the page header reserved bytes (u24 in bytes `5..7`) to avoid per-op slot scans on hot paths.
- Added on-demand leaf compaction fallback when contiguous free space is insufficient.

Benchmark method:

- Before = previous in-place phase runs:
  - `/tmp/opfs_inplace_run1.json`
  - `/tmp/opfs_inplace_run2.json`
  - `/tmp/opfs_inplace_run3.json`
- After = this follow-up phase runs:
  - `/tmp/opfs_inplace2_run1.json`
  - `/tmp/opfs_inplace2_run2.json`
  - `/tmp/opfs_inplace2_run3.json`
- Command shape: `--profile all --include-cold-read --count 3000 --value-sizes 32,256,4096 --seed 0xA5A5A5A501234567 --cache-mb 32 --pin-internal-pages true --read-coalesce-pages 4 --json`

Run deltas (before -> after):

| run    | mixed delta | range delta | cold delta | overall delta |
| ------ | ----------: | ----------: | ---------: | ------------: |
| 1      |       -1.5% |       -0.6% |     +11.3% |         +5.9% |
| 2      |       -3.0% |       -0.1% |     -11.6% |         -7.7% |
| 3      |       -0.6% |       -1.9% |      -2.9% |         -2.1% |
| median |       -1.5% |       -0.6% |      -2.9% |         -2.1% |

Aggregate medians:

| metric            | before K/s | after K/s | delta |
| ----------------- | ---------: | --------: | ----: |
| mixed aggregate   |     1129.6 |    1115.9 | -1.2% |
| range aggregate   |      607.3 |     601.2 | -1.0% |
| cold aggregate    |     2401.2 |    2342.4 | -2.4% |
| overall aggregate |     4130.7 |    4072.3 | -1.4% |

Interpretation: this follow-up reduced the size of the regression versus the first in-place attempt, but it is still slower than the previous in-place baseline.

## Phase: Locality-Aware Page Allocation

Changes in this phase:

- Added `alloc_page_near(preferred)` allocator in `db.rs`:
  - prefers nearby free pages within a bounded window,
  - otherwise appends contiguously when splitting at the file tail,
  - falls back to normal allocation when no nearby option exists.
- Wired near-allocation into:
  - leaf splits,
  - internal splits,
  - overflow chain page allocation.
- Added allocator tests for adjacent-page preference and tail-append behavior.

Benchmark method:

- Before = in-place follow-up runs:
  - `/tmp/opfs_inplace2_run1.json`
  - `/tmp/opfs_inplace2_run2.json`
  - `/tmp/opfs_inplace2_run3.json`
- After = locality-allocation runs:
  - `/tmp/opfs_locality_run1.json`
  - `/tmp/opfs_locality_run2.json`
  - `/tmp/opfs_locality_run3.json`
- Command shape: `--profile all --include-cold-read --count 3000 --value-sizes 32,256,4096 --seed 0xA5A5A5A501234567 --cache-mb 32 --pin-internal-pages true --read-coalesce-pages 4 --json`

Run deltas (before -> after):

| run    | mixed delta | range delta | cold delta | overall delta |
| ------ | ----------: | ----------: | ---------: | ------------: |
| 1      |       +0.4% |       -2.1% |      -3.3% |         -2.1% |
| 2      |       -0.3% |       +1.3% |      +5.5% |         +3.3% |
| 3      |       -0.3% |       +0.8% |      +6.7% |         +3.9% |
| median |       -0.3% |       +0.8% |      +5.5% |         +3.3% |

Aggregate medians:

| metric            | before K/s | after K/s | delta |
| ----------------- | ---------: | --------: | ----: |
| mixed aggregate   |     1115.9 |    1116.8 | +0.1% |
| range aggregate   |      601.2 |     601.7 | +0.1% |
| cold aggregate    |     2342.4 |    2472.2 | +5.5% |
| overall aggregate |     4072.3 |    4200.1 | +3.1% |

Interpretation: locality-aware allocation recovers and improves cold-read throughput, with a net positive overall delta.

## Phase: Key-Only Leaf Binary Search

Changes in this phase:

- Removed value-cell parsing from leaf binary-search comparisons in `page.rs`.
- `leaf_search_position` now reads only key slices (`leaf_key`) while searching.
- Value parsing is only performed once the target slot is found (or when explicitly needed).

Benchmark method:

- Before = locality-allocation runs:
  - `/tmp/opfs_locality_run1.json`
  - `/tmp/opfs_locality_run2.json`
  - `/tmp/opfs_locality_run3.json`
- After = key-only-search runs:
  - `/tmp/opfs_keyonly_run1.json`
  - `/tmp/opfs_keyonly_run2.json`
  - `/tmp/opfs_keyonly_run3.json`
- Command shape: `--profile all --include-cold-read --count 3000 --value-sizes 32,256,4096 --seed 0xA5A5A5A501234567 --cache-mb 32 --pin-internal-pages true --read-coalesce-pages 4 --json`

Run deltas (before -> after):

| run    | mixed delta | range delta | cold delta | overall delta |
| ------ | ----------: | ----------: | ---------: | ------------: |
| 1      |       -3.6% |       -3.0% |      +5.7% |         +1.9% |
| 2      |       +0.7% |       -2.8% |      -0.5% |         -0.5% |
| 3      |       +1.5% |       -1.0% |      +2.3% |         +1.6% |
| median |       +0.7% |       -2.8% |      +2.3% |         +1.6% |

Aggregate medians:

| metric            | before K/s | after K/s | delta |
| ----------------- | ---------: | --------: | ----: |
| mixed aggregate   |     1116.8 |    1128.9 | +1.1% |
| range aggregate   |      601.7 |     591.5 | -1.7% |
| cold aggregate    |     2472.2 |    2544.2 | +2.9% |
| overall aggregate |     4200.1 |    4207.0 | +0.2% |

Interpretation: key-only search reduces compare-path CPU and slightly improves mixed/cold throughput, but range regressed in this sample set.

## Phase: Direct Slot Walk for Range Queries

Changes in this phase:

- Reworked `raw_leaf_scan` to walk the leaf slot directory directly with a slot-base cursor.
- Inlined slot decode in the binary-search and range-scan loops (no per-entry `leaf_slot(...)` helper calls).
- Kept value-cell parsing lazy: parse only after key is inside `[start, end)`.

Benchmark method:

- Before = locality-allocation runs:
  - `/tmp/opfs_locality_run1.json`
  - `/tmp/opfs_locality_run2.json`
  - `/tmp/opfs_locality_run3.json`
- After = direct-slot-walk runs:
  - `/tmp/opfs_slotwalk2_run1.json`
  - `/tmp/opfs_slotwalk2_run2.json`
  - `/tmp/opfs_slotwalk2_run3.json`
- Command shape: `--profile all --include-cold-read --count 3000 --value-sizes 32,256,4096 --seed 0xA5A5A5A501234567 --cache-mb 32 --pin-internal-pages true --read-coalesce-pages 4 --json`

Run deltas (before -> after):

| run    | mixed delta | range delta | cold delta | overall delta |
| ------ | ----------: | ----------: | ---------: | ------------: |
| 1      |       +2.1% |       +3.4% |      +0.2% |         +1.2% |
| 2      |       +7.0% |       +0.5% |      +8.1% |         +6.7% |
| 3      |       +7.1% |       +5.3% |      +4.7% |         +5.4% |
| median |       +7.0% |       +3.4% |      +4.7% |         +5.4% |

Aggregate medians:

| metric            | before K/s | after K/s | delta |
| ----------------- | ---------: | --------: | ----: |
| mixed aggregate   |     1116.8 |    1191.7 | +6.7% |
| range aggregate   |      601.7 |     622.4 | +3.4% |
| cold aggregate    |     2472.2 |    2603.1 | +5.3% |
| overall aggregate |     4200.1 |    4427.3 | +5.4% |

Range medians by scenario:

| scenario               | value_size | before K/s | after K/s | delta |
| ---------------------- | ---------: | ---------: | --------: | ----: |
| range_seq_window_64    |         32 |      153.1 |     151.5 | -1.0% |
| range_seq_window_64    |        256 |      148.5 |     154.6 | +4.1% |
| range_seq_window_64    |       4096 |        1.9 |       2.0 | +5.7% |
| range_random_window_64 |         32 |      169.5 |     172.4 | +1.7% |
| range_random_window_64 |        256 |      130.4 |     141.5 | +8.5% |
| range_random_window_64 |       4096 |        0.3 |       0.3 | +5.2% |

Interpretation: direct slot walking improves overall throughput in this setup, with strongest gains on mixed/cold and moderate positive effect on range aggregate.

## Phase: Large-Value Extent Storage (WIP)

Changes in this phase:

- Switched overflow values from pointer-chained pages to contiguous extents (start page + length).
- Added fast raw overflow-page encoding path for large-value writes.
- Added in-place extent rewrite + tail-page reclaim for large updates when page count does not grow.
- Added large-value reuse guard (`>= 128KB`) to avoid extra update-lookup overhead on smaller overflow values.

Benchmark method:

- Baseline mixed-size sweep:
  - `/tmp/opfs_largefocus_before_4096.json`
  - `/tmp/opfs_largefocus_before_8192.json`
  - `/tmp/opfs_largefocus_before_16384.json`
  - `/tmp/opfs_largefocus_before_32768.json`
  - `/tmp/opfs_largefocus_before_65536.json`
  - `/tmp/opfs_largefocus_before_1048576.json`
- After (extent WIP):
  - `/tmp/opfs_largefocus_after7_4096.json`
  - `/tmp/opfs_largefocus_after7_8192.json`
  - `/tmp/opfs_largefocus_after7_16384.json`
  - `/tmp/opfs_largefocus_after7_32768.json`
  - `/tmp/opfs_largefocus_after7_65536.json`
  - `/tmp/opfs_largefocus_after7_1048576.json`
- Command shape (mixed sweep):
  - `--profile mixed --count 400 --value-sizes 4096|8192|16384|32768|65536`
  - `--profile mixed --count 64 --value-sizes 1048576`
  - `--seed 0xA5A5A5A501234567 --cache-mb 32 --pin-internal-pages true --read-coalesce-pages 4 --json`

Mixed aggregate deltas by value size (sum of 3 mixed scenarios):

| value_size | before K/s | after K/s | delta |
| ---------: | ---------: | --------: | ----: |
|      4,096 |    110.913 |   117.245 | +5.7% |
|      8,192 |     79.072 |    75.866 | -4.1% |
|     16,384 |     71.094 |    71.920 | +1.2% |
|     32,768 |     58.904 |    56.402 | -4.2% |
|     65,536 |     46.848 |    45.444 | -3.0% |
|  1,048,576 |      0.612 |     0.596 | -2.6% |

Overall mixed aggregate across these sizes: `367.443 -> 367.474 K/s` (`+0.0%`).

Focused 1MB all-profile delta:

- Before: `/tmp/opfs_large_baseline_run1.json`
- After: `/tmp/opfs_large_after7_all_1m.json`
- Command shape: `--profile all --include-cold-read --count 64 --value-sizes 1048576 --seed 0xA5A5A5A501234567 --cache-mb 32 --pin-internal-pages true --read-coalesce-pages 4 --json`

| operation                         | before K/s | after K/s | delta |
| --------------------------------- | ---------: | --------: | ----: |
| seq_write                         |      0.201 |     0.212 | +5.5% |
| random_write                      |      0.271 |     0.277 | +2.2% |
| seq_read                          |      0.113 |     0.113 | -0.2% |
| random_read                       |      0.120 |     0.122 | +1.8% |
| mixed_random_70r_30w              |      0.184 |     0.184 | +0.0% |
| mixed_random_50r_50w_with_updates |      0.189 |     0.206 | +8.8% |
| mixed_random_60r_20w_20d          |      0.207 |     0.212 | +2.5% |
| cold_seq_read                     |      0.182 |     0.179 | -1.3% |
| cold_random_read                  |      0.177 |     0.170 | -3.5% |

Interpretation: this WIP extent model improves large write-heavy/update-heavy paths, but does not yet improve large cold reads and is near-flat in mixed aggregate across 4KB..1MB. It should be treated as an exploratory phase, not promoted to top-table baseline yet.

## Phase: Large-Value Direct Extent Read (WIP)

Changes in this phase:

- Added direct contiguous extent-read path for large values (`>= 128KB`) that bypasses page-cache materialization when extent pages are not dirty.
- Kept read-your-writes correctness by falling back to in-memory path when any extent page is dirty.
- Kept extent-update reuse path gated to large values (`>= 128KB`) to avoid extra lookup overhead on smaller overflow values.

Benchmark method:

- Before (extent baseline): `/tmp/opfs_largefocus_after7_*.json`, `/tmp/opfs_large_after7_all_1m.json`
- After (direct extent-read): `/tmp/opfs_largefocus_after8_*.json`, `/tmp/opfs_large_after8_all_1m.json`
- Command shape remains the same as the previous large-value phase.

Mixed aggregate deltas by value size (extent baseline -> direct-read):

| value_size | before K/s | after K/s | delta |
| ---------: | ---------: | --------: | ----: |
|      4,096 |    117.245 |   111.078 | -5.3% |
|      8,192 |     75.866 |    77.880 | +2.7% |
|     16,384 |     71.920 |    76.496 | +6.4% |
|     32,768 |     56.402 |    57.071 | +1.2% |
|     65,536 |     45.444 |    46.202 | +1.7% |
|  1,048,576 |      0.596 |     0.595 | -0.2% |

Overall mixed aggregate across these sizes: `367.474 -> 369.322 K/s` (`+0.5%`).

Focused 1MB all-profile delta (extent baseline -> direct-read):

| operation                         | before K/s | after K/s | delta |
| --------------------------------- | ---------: | --------: | ----: |
| seq_write                         |      0.212 |     0.214 | +1.0% |
| random_write                      |      0.277 |     0.277 | +0.3% |
| seq_read                          |      0.113 |     0.113 | -0.0% |
| random_read                       |      0.122 |     0.122 | +0.3% |
| mixed_random_70r_30w              |      0.184 |     0.178 | -3.4% |
| mixed_random_50r_50w_with_updates |      0.206 |     0.208 | +1.1% |
| mixed_random_60r_20w_20d          |      0.212 |     0.207 | -2.6% |
| cold_seq_read                     |      0.179 |     0.180 | +0.6% |
| cold_random_read                  |      0.170 |     0.177 | +3.9% |

Interpretation: direct extent-read yields a modest net uplift over the previous extent baseline and improves cold random reads at 1MB, but mixed 1MB scenarios remain near-flat to slightly negative.

## Phase: Overflow Threshold Tuning (Default = 4KB)

Changes in this phase:

- Added a runtime-tunable overflow threshold to the wasm benchmark harness:
  - Rust export: `bench_set_overflow_threshold_bytes(...)`
  - JS runner flag: `--overflow-threshold-bytes`
- Set the default overflow threshold from `8KB` to `4KB`:
  - `DEFAULT_OVERFLOW_THRESHOLD` in `db.rs`
  - `DEFAULT_BENCH_OVERFLOW_THRESHOLD` in `wasm_bench.rs`
  - `DEFAULT_OVERFLOW_THRESHOLD_BYTES` in `run-opfs-bench.cjs`

Benchmark method:

- Threshold candidates: `4096`, `6144`, `8192` bytes.
- For each candidate, medians over 3 runs were collected for:
  - `--profile mixed --count 400 --value-sizes 4096,8192,16384,32768,65536`
  - `--profile mixed --count 64 --value-sizes 1048576`
- Additional 3-run medians for top-table 1MB rows:
  - `--profile all --include-cold-read --count 64 --value-sizes 1048576`
- Common flags:
  - `--seed 0xA5A5A5A501234567 --cache-mb 32 --pin-internal-pages true --read-coalesce-pages 4`

Candidate summary (3-run medians):

| threshold (bytes) | medium mixed agg K/s (4KB..64KB) | delta vs 8192 | mixed 1MB agg K/s | delta vs 8192 | combined K/s | delta vs 8192 |
| ----------------: | -------------------------------: | ------------: | ----------------: | ------------: | -----------: | ------------: |
|             4,096 |                          437.966 |         +7.7% |             2.205 |         -5.8% |      440.232 |         +7.7% |
|             6,144 |                          409.252 |         +0.7% |             2.261 |         -3.4% |      411.512 |         +0.6% |
|             8,192 |                          406.573 |      baseline |             2.341 |      baseline |      408.861 |      baseline |

Mixed aggregate by value size (`4096` vs `8192`, 3-run medians):

| value_size | 8192 K/s | 4096 K/s |  delta |
| ---------: | -------: | -------: | -----: |
|      4,096 |   93.975 |   92.998 |  -1.0% |
|      8,192 |   70.541 |  104.743 | +48.5% |
|     16,384 |   95.026 |   94.741 |  -0.3% |
|     32,768 |   80.959 |   78.486 |  -3.1% |
|     65,536 |   64.488 |   66.496 |  +3.1% |
|  1,048,576 |    2.341 |    2.205 |  -5.8% |

1MB all-profile operation medians (`8192` -> `4096`, 3-run medians):

| operation                         | 8192 K/s | 4096 K/s | delta |
| --------------------------------- | -------: | -------: | ----: |
| seq_write                         |    0.241 |    0.235 | -2.4% |
| random_write                      |    0.314 |    0.315 | +0.3% |
| seq_read                          |    2.991 |    2.991 | +0.0% |
| random_read                       |    3.019 |    2.896 | -4.1% |
| mixed_random_70r_30w              |    0.482 |    0.458 | -4.9% |
| mixed_random_50r_50w_with_updates |    0.535 |    0.537 | +0.4% |
| mixed_random_60r_20w_20d          |    1.333 |    1.311 | -1.6% |
| range_seq_window_64               |    0.051 |    0.053 | +3.7% |
| range_random_window_64            |    0.053 |    0.053 | -0.6% |
| cold_seq_read                     |    3.184 |    2.896 | -9.0% |
| cold_random_read                  |    3.107 |    3.137 | +1.0% |

Extra sanity point:

- `3072` threshold (single-run spot check) produced `430.763 K/s` combined mixed aggregate, below `4096`.

Decision:

- Default threshold is now `4096` as the best simple single-threshold balance for this workload set.
