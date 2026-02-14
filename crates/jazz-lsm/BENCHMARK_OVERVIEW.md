# jazz-lsm Benchmark Overview

Generated on 2026-02-14.

Single baseline table for further `opfs-btree` work. All values are throughput in `K/s` (1 K = 1,000 ops/s), rounded to one decimal.

| Scenario                          | Value Size (bytes) | jazz-lsm native | jazz-lsm wasm/opfs | opfs-btree native | opfs-btree wasm/opfs | bf-tree native | rocksdb native | surrealkv native | fjall native | Notes  |
| --------------------------------- | -----------------: | --------------: | -----------------: | ----------------: | -------------------: | -------------: | -------------: | ---------------: | -----------: | ------ |
| mixed_random_70r_30w              |                 32 |            17.0 |               13.3 |              40.7 |                 62.5 |           81.9 |          214.0 |             75.5 |          2.2 | [1][5] |
| mixed_random_70r_30w              |                256 |            28.7 |               20.4 |              48.0 |                 71.4 |           89.8 |          256.5 |             81.8 |          2.2 | [1][5] |
| mixed_random_70r_30w              |              4,096 |            24.2 |               19.2 |              32.6 |                 11.6 |           66.2 |          153.1 |             57.6 |          2.2 | [1][5] |
| mixed_random_70r_30w              |          1,048,576 |             0.1 |                0.1 |               0.9 |                 <0.1 |            N/A |            1.8 |              1.4 |          0.2 | [2][5] |
| mixed_random_50r_50w_with_updates |                 32 |            23.2 |               20.0 |              40.3 |                 83.3 |           98.1 |          265.2 |             77.3 |          2.2 | [1][5] |
| mixed_random_50r_50w_with_updates |                256 |            28.9 |               23.3 |              47.6 |                 71.4 |           77.4 |          263.0 |             84.1 |          2.2 | [1][5] |
| mixed_random_50r_50w_with_updates |              4,096 |            13.6 |               15.2 |              32.3 |                 11.9 |           56.4 |          143.2 |             49.3 |          2.3 | [1][5] |
| mixed_random_50r_50w_with_updates |          1,048,576 |            <0.1 |                0.1 |               0.8 |                  0.1 |            N/A |            1.8 |              1.3 |          0.2 | [2][5] |
| mixed_random_60r_20w_20d          |                 32 |            22.1 |               21.3 |              40.4 |                 76.9 |           95.2 |          298.1 |             78.7 |          2.2 | [1][5] |
| mixed_random_60r_20w_20d          |                256 |            30.8 |               23.3 |              48.1 |                 71.4 |           90.3 |          256.3 |             85.5 |          2.2 | [1][5] |
| mixed_random_60r_20w_20d          |              4,096 |            28.4 |               18.9 |              36.8 |                 10.8 |           71.7 |          176.4 |             63.3 |          2.2 | [1][5] |
| mixed_random_60r_20w_20d          |          1,048,576 |             0.2 |                0.2 |               0.9 |                  0.1 |            N/A |            3.4 |              3.0 |          0.2 | [2][5] |
| cold_seq_read                     |                 32 |            35.0 |               34.5 |            2491.9 |                 83.3 |            N/A |          716.1 |            268.0 |         18.5 | [3][5] |
| cold_seq_read                     |                256 |            97.0 |               37.0 |            1658.4 |                 83.3 |            N/A |          626.8 |            220.1 |         18.4 | [3][5] |
| cold_seq_read                     |              4,096 |            99.1 |               30.3 |             279.4 |                 14.9 |            N/A |          297.4 |            181.7 |         18.9 | [3][5] |
| cold_seq_read                     |          1,048,576 |             2.0 |                1.2 |               2.4 |                  0.1 |            N/A |            4.4 |              0.9 |          0.2 | [4][5] |
| cold_random_read                  |                 32 |            34.9 |               40.0 |            2154.1 |                111.1 |            N/A |          664.3 |            261.4 |         18.6 | [3][5] |
| cold_random_read                  |                256 |            94.1 |               37.0 |            1560.0 |                 76.9 |            N/A |          641.7 |            262.7 |         18.5 | [3][5] |
| cold_random_read                  |              4,096 |            72.6 |               31.2 |             265.4 |                 14.5 |            N/A |          294.6 |            166.4 |         18.8 | [3][5] |
| cold_random_read                  |          1,048,576 |             2.0 |                1.2 |               2.4 |                  0.1 |            N/A |            4.6 |              1.1 |          0.2 | [4][5] |

**Notes**

[1] Mixed scenario native rows come from the comparative-enabled mixed harness:
`cargo run -p jazz-lsm --release --features compare-native --bin mixed_bench_native -- --engines all --count 500 --value-sizes 32,256,4096 --json`.
WASM mixed rows come from OPFS runners with `count=100`.

[2] Mixed `1,048,576` rows use reduced `count=64`:
`cargo run -p jazz-lsm --release --features compare-native --bin mixed_bench_native -- --engines all --count 64 --value-sizes 1048576 --json`.
`bf-tree` does not support 1MB values in this harness and is therefore `N/A` for those rows.

[3] Cold native rows come from Criterion cross-engine groups (`--quick`) with `count=5000` for `32/256/4096`:
`compare_native_cold_seq_read` and `compare_native_cold_random_read`.
WASM cold rows come from OPFS mixed runners with `--include-cold-read` and `count=100`.

[4] Cold `1,048,576` rows use reduced `count=64` for both native and wasm.

[5] `N/A` means the scenario/runtime combination is currently not benchmarked or unsupported.
