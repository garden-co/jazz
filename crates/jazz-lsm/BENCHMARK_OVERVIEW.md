# jazz-lsm Benchmark Overview

Generated on 2026-02-14.

Single baseline table for further `opfs-btree` work. All values are throughput in `K/s` (1 K = 1,000 ops/s), rounded to one decimal.

| Scenario | Value Size (bytes) | jazz-lsm native | jazz-lsm wasm/opfs | opfs-btree native | opfs-btree wasm/opfs | bf-tree native | bf-tree wasm/opfs | rocksdb native | surrealkv native | fjall native | Notes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| mixed_random_70r_30w | 32 | 15.3 | 13.3 | 40.5 | 62.5 | N/A | N/A | N/A | N/A | N/A | [1][5] |
| mixed_random_70r_30w | 256 | 28.6 | 20.4 | 53.1 | 71.4 | N/A | N/A | N/A | N/A | N/A | [1][5] |
| mixed_random_70r_30w | 4,096 | 23.0 | 19.2 | 34.0 | 11.6 | N/A | N/A | N/A | N/A | N/A | [1][5] |
| mixed_random_70r_30w | 1,048,576 | <0.1 | 0.1 | 0.9 | <0.1 | N/A | N/A | N/A | N/A | N/A | [2][5] |
| mixed_random_50r_50w_with_updates | 32 | 19.4 | 20.0 | 37.4 | 83.3 | N/A | N/A | N/A | N/A | N/A | [1][5] |
| mixed_random_50r_50w_with_updates | 256 | 28.5 | 23.3 | 47.4 | 71.4 | N/A | N/A | N/A | N/A | N/A | [1][5] |
| mixed_random_50r_50w_with_updates | 4,096 | 14.3 | 15.2 | 31.4 | 11.9 | N/A | N/A | N/A | N/A | N/A | [1][5] |
| mixed_random_50r_50w_with_updates | 1,048,576 | <0.1 | 0.1 | 0.8 | 0.1 | N/A | N/A | N/A | N/A | N/A | [2][5] |
| mixed_random_60r_20w_20d | 32 | 19.6 | 21.3 | 44.1 | 76.9 | N/A | N/A | N/A | N/A | N/A | [1][5] |
| mixed_random_60r_20w_20d | 256 | 28.5 | 23.3 | 48.0 | 71.4 | N/A | N/A | N/A | N/A | N/A | [1][5] |
| mixed_random_60r_20w_20d | 4,096 | 24.7 | 18.9 | 33.5 | 10.8 | N/A | N/A | N/A | N/A | N/A | [1][5] |
| mixed_random_60r_20w_20d | 1,048,576 | 0.2 | 0.2 | 0.9 | 0.1 | N/A | N/A | N/A | N/A | N/A | [2][5] |
| cold_seq_read | 32 | 35.0 | 34.5 | 2491.9 | 83.3 | N/A | N/A | 716.1 | 268.0 | 18.5 | [3][5] |
| cold_seq_read | 256 | 97.0 | 37.0 | 1658.4 | 83.3 | N/A | N/A | 626.8 | 220.1 | 18.4 | [3][5] |
| cold_seq_read | 4,096 | 99.1 | 30.3 | 279.4 | 14.9 | N/A | N/A | 297.4 | 181.7 | 18.9 | [3][5] |
| cold_seq_read | 1,048,576 | 2.0 | 1.2 | 2.4 | 0.1 | N/A | N/A | 4.4 | 0.9 | 0.2 | [4][5] |
| cold_random_read | 32 | 34.9 | 40.0 | 2154.1 | 111.1 | N/A | N/A | 664.3 | 261.4 | 18.6 | [3][5] |
| cold_random_read | 256 | 94.1 | 37.0 | 1560.0 | 76.9 | N/A | N/A | 641.7 | 262.7 | 18.5 | [3][5] |
| cold_random_read | 4,096 | 72.6 | 31.2 | 265.4 | 14.5 | N/A | N/A | 294.6 | 166.4 | 18.8 | [3][5] |
| cold_random_read | 1,048,576 | 2.0 | 1.2 | 2.4 | 0.1 | N/A | N/A | 4.6 | 1.1 | 0.2 | [4][5] |

**Notes**

[1] Mixed scenario rows come from the custom mixed harness (`mixed_bench_native` and wasm OPFS runners) with `count=500` native and `count=100` wasm for `32/256/4096`.

[2] Mixed `1,048,576` rows use reduced `count=64` for both native and wasm.

[3] Cold native rows come from Criterion `compare_native_cold_seq_read` and `compare_native_cold_random_read` (`--quick`) with `count=5000` for `32/256/4096`; cold wasm rows come from wasm OPFS mixed runners with `--include-cold-read` and `count=100`.

[4] Cold `1,048,576` rows use reduced `count=64` for both native and wasm.

[5] `N/A` means the scenario was not benchmarked for that engine/runtime in this baseline. `bf-tree` was excluded from native cold compare due to reopen instability and has no wasm/opfs run in this matrix.
