# opfs-btree Benchmark Overview

Generated on 2026-02-14.

Single baseline table for further `opfs-btree` work. All values are throughput in `K/s` (1 K = 1,000 ops/s), rounded to one decimal.
Current `opfs-btree` default cache is `32MB`.

| Scenario                          | Value Size (bytes) | opfs-btree native | opfs-btree wasm/opfs | bf-tree native | rocksdb native | surrealkv native | fjall native | Notes      |
| --------------------------------- | -----------------: | ----------------: | -------------------: | -------------: | -------------: | ---------------: | -----------: | ---------- |
| mixed_random_70r_30w              |                 32 |              40.7 |                125.5 |           81.9 |          214.0 |             75.5 |          2.2 | [1][5][9]  |
| mixed_random_70r_30w              |                256 |              48.0 |                117.6 |           89.8 |          256.5 |             81.8 |          2.2 | [1][5][9]  |
| mixed_random_70r_30w              |              4,096 |              32.6 |                 31.8 |           66.2 |          153.1 |             57.6 |          2.2 | [1][5][9]  |
| mixed_random_70r_30w              |          1,048,576 |               0.9 |                  0.1 |            N/A |            1.8 |              1.4 |          0.2 | [2][5][9]  |
| mixed_random_50r_50w_with_updates |                 32 |              40.3 |                 92.9 |           98.1 |          265.2 |             77.3 |          2.2 | [1][5][9]  |
| mixed_random_50r_50w_with_updates |                256 |              47.6 |                106.4 |           77.4 |          263.0 |             84.1 |          2.2 | [1][5][9]  |
| mixed_random_50r_50w_with_updates |              4,096 |              32.3 |                 21.4 |           56.4 |          143.2 |             49.3 |          2.3 | [1][5][9]  |
| mixed_random_50r_50w_with_updates |          1,048,576 |               0.8 |                  0.1 |            N/A |            1.8 |              1.3 |          0.2 | [2][5][9]  |
| mixed_random_60r_20w_20d          |                 32 |              40.4 |                145.6 |           95.2 |          298.1 |             78.7 |          2.2 | [1][5][9]  |
| mixed_random_60r_20w_20d          |                256 |              48.1 |                118.6 |           90.3 |          256.3 |             85.5 |          2.2 | [1][5][9]  |
| mixed_random_60r_20w_20d          |              4,096 |              36.8 |                 25.2 |           71.7 |          176.4 |             63.3 |          2.2 | [1][5][9]  |
| mixed_random_60r_20w_20d          |          1,048,576 |               0.9 |                  0.1 |            N/A |            3.4 |              3.0 |          0.2 | [2][5][9]  |
| cold_seq_read                     |                 32 |            2491.9 |                 83.3 |            N/A |          716.1 |            268.0 |         18.5 | [3][5][10] |
| cold_seq_read                     |                256 |            1658.4 |                 83.3 |            N/A |          626.8 |            220.1 |         18.4 | [3][5][10] |
| cold_seq_read                     |              4,096 |             279.4 |                 14.9 |            N/A |          297.4 |            181.7 |         18.9 | [3][5][10] |
| cold_seq_read                     |          1,048,576 |               2.4 |                  0.1 |            N/A |            4.4 |              0.9 |          0.2 | [4][5][10] |
| cold_random_read                  |                 32 |            2154.1 |                111.1 |            N/A |          664.3 |            261.4 |         18.6 | [3][5][10] |
| cold_random_read                  |                256 |            1560.0 |                 76.9 |            N/A |          641.7 |            262.7 |         18.5 | [3][5][10] |
| cold_random_read                  |              4,096 |             265.4 |                 14.5 |            N/A |          294.6 |            166.4 |         18.8 | [3][5][10] |
| cold_random_read                  |          1,048,576 |               2.4 |                  0.1 |            N/A |            4.6 |              1.1 |          0.2 | [4][5][10] |

**Notes**

[1] Mixed scenario native comparator rows come from the comparative mixed harness runs.
WASM mixed rows come from OPFS runners.

[2] Mixed `1,048,576` rows use reduced `count=64`.
`bf-tree` does not support 1MB values in this harness and is therefore `N/A` for those rows.

[3] Cold native rows come from Criterion cross-engine groups (`--quick`) with `count=5000` for `32/256/4096`:
`compare_native_cold_seq_read` and `compare_native_cold_random_read`.
WASM cold rows come from OPFS runners with `--include-cold-read`.

[4] Cold `1,048,576` rows use reduced `count=64` for both native and wasm.

[5] `N/A` means the scenario/runtime combination is currently not benchmarked or unsupported.

[6] Historical `opfs-btree wasm/opfs` mixed rows for the incremental-checkpoint phase A/B used median of 3 runs with seed `0xA5A5A5A501234567`:
`node wasm-bench/run-opfs-bench.cjs --profile mixed --count 5000 --value-sizes 32,256,4096 --seed 0xA5A5A5A501234567 --json`
and
`node wasm-bench/run-opfs-bench.cjs --profile mixed --count 64 --value-sizes 1048576 --seed 0xA5A5A5A501234567 --json`.

[7] Current `opfs-btree wasm/opfs` mixed rows reflect the lazy-page-loading phase A/B medians over 3 runs (same seed/commands as [6]), with baseline at commit `b3fe6ae`.

[8] Range benchmarks are currently measured for `opfs-btree wasm/opfs` only via `--profile range`, with medians across 3 runs.

[9] Current top-table `opfs-btree wasm/opfs` mixed rows reflect cache tuning results at `32MB` cache:
3-run medians with seed `0xA5A5A5A501234567`,
`count=3000` for `32/256/4096` and `count=64` for `1,048,576`.

[10] Top-table wasm cold rows are currently carried forward from the prior baseline run (before cache-sizing phase re-measurement).

## Range Query Benchmarks (opfs-btree wasm/opfs)

Workload details:

- `range_seq_window_64`: deterministic sequential start positions.
- `range_random_window_64`: random start positions from deterministic RNG.
- Both use a prefilled keyspace and execute bounded range queries with `limit=64`.

| operation              | value_size | count | median K/s | median p95 ms |
| ---------------------- | ---------: | ----: | ---------: | ------------: |
| range_seq_window_64    |         32 |  2000 |      111.1 |         0.100 |
| range_seq_window_64    |        256 |  2000 |      113.0 |         0.100 |
| range_seq_window_64    |       4096 |  2000 |        1.5 |         0.800 |
| range_seq_window_64    |    1048576 |   128 |        0.3 |         3.100 |
| range_random_window_64 |         32 |  2000 |      143.9 |         0.100 |
| range_random_window_64 |        256 |  2000 |      104.2 |         0.100 |
| range_random_window_64 |       4096 |  2000 |        0.3 |         6.200 |
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

## Phase: Cache Default 8MB -> 32MB (Before/After)

Before = `8MB` cache.
After = `32MB` cache.
Values are 3-run medians in `K/s`, deterministic seed `0xA5A5A5A501234567`.

Aggregate deltas:

| metric                             | before (8MB) | after (32MB) | delta |
| ---------------------------------- | -----------: | -----------: | ----: |
| mixed aggregate (32/256/4096)      |         80.9 |         87.2 | +7.9% |
| range aggregate (32/256/4096)      |         80.0 |         79.0 | -1.2% |
| combined aggregate (mixed + range) |         80.5 |         83.9 | +4.3% |

Mixed scenarios:

| scenario                          | value_size | before (8MB) | after (32MB) |   delta |
| --------------------------------- | ---------: | -----------: | -----------: | ------: |
| mixed_random_70r_30w              |         32 |        126.6 |        125.5 |   -0.8% |
| mixed_random_70r_30w              |        256 |        120.0 |        117.6 |   -2.0% |
| mixed_random_70r_30w              |       4096 |          8.9 |         31.8 | +258.3% |
| mixed_random_70r_30w              |    1048576 |          0.1 |          0.1 |   +8.9% |
| mixed_random_50r_50w_with_updates |         32 |         93.8 |         92.9 |   -0.9% |
| mixed_random_50r_50w_with_updates |        256 |        104.5 |        106.4 |   +1.8% |
| mixed_random_50r_50w_with_updates |       4096 |          7.1 |         21.4 | +201.2% |
| mixed_random_50r_50w_with_updates |    1048576 |          0.1 |          0.1 |  +12.1% |
| mixed_random_60r_20w_20d          |         32 |        142.9 |        145.6 |   +1.9% |
| mixed_random_60r_20w_20d          |        256 |        116.7 |        118.6 |   +1.6% |
| mixed_random_60r_20w_20d          |       4096 |          7.4 |         25.2 | +242.3% |
| mixed_random_60r_20w_20d          |    1048576 |          0.1 |          0.1 |   +4.7% |

Range scenarios:

| operation              | value_size | before (8MB) | after (32MB) |  delta |
| ---------------------- | ---------: | -----------: | -----------: | -----: |
| range_seq_window_64    |         32 |        114.9 |        111.1 |  -3.3% |
| range_seq_window_64    |        256 |        112.4 |        113.0 |  +0.6% |
| range_seq_window_64    |       4096 |          2.2 |          1.5 | -32.7% |
| range_random_window_64 |         32 |        140.8 |        143.9 |  +2.2% |
| range_random_window_64 |        256 |        109.3 |        104.2 |  -4.7% |
| range_random_window_64 |       4096 |          0.3 |          0.3 | +17.0% |
