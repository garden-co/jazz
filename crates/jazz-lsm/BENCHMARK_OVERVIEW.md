# jazz-lsm Benchmark Overview

Generated on 2026-02-13.

All values below are throughput shown as `K/s` (1 K = 1,000 ops/s) from the runs executed in this session. Native numbers come from Criterion `--quick` runs; WASM numbers come from the OPFS worker harness.

| Scenario     | Value Size (bytes) | jazz-lsm native (compare_native) | jazz-lsm wasm/opfs | bf-tree native | rocksdb native | surrealkv native | fjall native | bf-tree wasm/opfs | Notes                                                                                                       |
| ------------ | -----------------: | -------------------------------: | -----------------: | -------------: | -------------: | ---------------: | -----------: | ----------------: | ----------------------------------------------------------------------------------------------------------- |
| seq_write    |                 32 |                        125.5 K/s |            3.4 K/s |     1110.7 K/s |     1265.8 K/s |        487.4 K/s |     21.4 K/s |               N/A | Native `count=5000`; WASM `count=500`                                                                       |
| random_write |                 32 |                        182.1 K/s |            3.3 K/s |     1148.6 K/s |      983.5 K/s |        482.1 K/s |     21.4 K/s |               N/A | Native `count=5000`; WASM `count=500`                                                                       |
| seq_read     |                 32 |                         66.0 K/s |            4.8 K/s |     2764.1 K/s |     1017.0 K/s |        704.8 K/s |     29.6 K/s |               N/A | Native `count=5000`; WASM `count=500`                                                                       |
| random_read  |                 32 |                         66.4 K/s |            4.9 K/s |     2371.1 K/s |      950.4 K/s |        563.9 K/s |     22.1 K/s |               N/A | Native `count=5000`; WASM `count=500`                                                                       |
| seq_write    |                256 |                         60.6 K/s |            3.1 K/s |      876.2 K/s |      947.7 K/s |        411.1 K/s |     21.8 K/s |               N/A | Native `count=5000`; WASM `count=500`                                                                       |
| random_write |                256 |                         62.6 K/s |            3.1 K/s |      891.6 K/s |      819.9 K/s |        386.9 K/s |     21.2 K/s |               N/A | Native `count=5000`; WASM `count=500`                                                                       |
| seq_read     |                256 |                        145.3 K/s |            4.3 K/s |     2813.7 K/s |      890.4 K/s |        666.3 K/s |     22.5 K/s |               N/A | Native `count=5000`; WASM `count=500`                                                                       |
| random_read  |                256 |                        147.5 K/s |            4.4 K/s |     2217.4 K/s |      877.7 K/s |        621.1 K/s |     22.8 K/s |               N/A | Native `count=5000`; WASM `count=500`                                                                       |
| seq_write    |               4096 |                          2.7 K/s |            3.7 K/s |       76.7 K/s |      150.9 K/s |        129.4 K/s |     21.3 K/s |               N/A | Native `count=5000`; WASM `count=500`                                                                       |
| random_write |               4096 |                          3.6 K/s |            3.7 K/s |       81.1 K/s |      201.1 K/s |        116.1 K/s |     20.9 K/s |               N/A | Native `count=5000`; WASM `count=500`                                                                       |
| seq_read     |               4096 |                         86.7 K/s |            0.7 K/s |      515.6 K/s |      353.4 K/s |        541.0 K/s |     29.1 K/s |               N/A | Native `count=5000`; WASM `count=500`                                                                       |
| random_read  |               4096 |                         55.0 K/s |            0.7 K/s |      432.7 K/s |      327.5 K/s |        455.5 K/s |     28.8 K/s |               N/A | Native `count=5000`; WASM `count=500`                                                                       |
| seq_write    |          1,048,576 |                         <0.1 K/s |            0.1 K/s |            N/A |        0.7 K/s |          0.7 K/s |      0.3 K/s |               N/A | Native compare `count=64`; WASM `count=32`; `bf-tree` unsupported at 1MB. Standalone `lsm_native`: <0.1 K/s |
| random_write |          1,048,576 |                         <0.1 K/s |            0.1 K/s |            N/A |        0.6 K/s |          0.7 K/s |      0.3 K/s |               N/A | Native compare `count=64`; WASM `count=32`; `bf-tree` unsupported at 1MB. Standalone `lsm_native`: <0.1 K/s |
| seq_read     |          1,048,576 |                          1.6 K/s |            0.2 K/s |            N/A |        5.9 K/s |          1.9 K/s |      0.2 K/s |               N/A | Native compare `count=64`; WASM `count=32`; `bf-tree` unsupported at 1MB. Standalone `lsm_native`: 0.2 K/s  |
| random_read  |          1,048,576 |                          1.8 K/s |            0.2 K/s |            N/A |        6.4 K/s |          6.5 K/s |      0.2 K/s |               N/A | Native compare `count=64`; WASM `count=32`; `bf-tree` unsupported at 1MB. Standalone `lsm_native`: 0.2 K/s  |

Rounded to one decimal place; values below `0.1 K/s` are shown as `<0.1 K/s`.

## Phase 1 Mixed Baseline (single-threaded worker)

Throughput values below are shown as exact `ops/s` from the mixed benchmark runners.

| Scenario                          | Value Size (bytes) | jazz-lsm native ops/s | native p95 op (ms) | jazz-lsm wasm/opfs ops/s | wasm p95 op (ms) | Notes                                |
| --------------------------------- | -----------------: | --------------------: | -----------------: | -----------------------: | ---------------: | ------------------------------------ |
| mixed_random_70r_30w              |                 32 |              2673.363 |              2.380 |                 5882.353 |            0.200 | Native `count=500`; WASM `count=100` |
| mixed_random_50r_50w_with_updates |                 32 |              2359.634 |              3.354 |                 5555.556 |            0.300 | Native `count=500`; WASM `count=100` |
| mixed_random_60r_20w_20d          |                 32 |              2578.329 |              3.052 |                 6024.096 |            0.300 | Native `count=500`; WASM `count=100` |
| mixed_random_70r_30w              |                256 |              2471.936 |              2.766 |                 2941.176 |            0.800 | Native `count=500`; WASM `count=100` |
| mixed_random_50r_50w_with_updates |                256 |              2204.954 |              3.219 |                 5347.594 |            0.300 | Native `count=500`; WASM `count=100` |
| mixed_random_60r_20w_20d          |                256 |              2250.680 |              2.631 |                 5882.353 |            0.300 | Native `count=500`; WASM `count=100` |
| mixed_random_70r_30w              |               4096 |              1140.796 |              2.448 |                 3003.003 |            0.400 | Native `count=500`; WASM `count=100` |
| mixed_random_50r_50w_with_updates |               4096 |              1590.260 |              2.190 |                 3225.806 |            0.400 | Native `count=500`; WASM `count=100` |
| mixed_random_60r_20w_20d          |               4096 |              2294.221 |              0.633 |                 3205.128 |            0.400 | Native `count=500`; WASM `count=100` |
| mixed_random_70r_30w              |          1,048,576 |                35.669 |             78.036 |                  277.778 |            6.800 | Native `count=64`; WASM `count=4`    |
| mixed_random_50r_50w_with_updates |          1,048,576 |                39.794 |             46.966 |                  283.688 |            6.400 | Native `count=64`; WASM `count=4`    |
| mixed_random_60r_20w_20d          |          1,048,576 |                46.140 |             44.129 |                  289.855 |            6.400 | Native `count=64`; WASM `count=4`    |

WASM benchmark timing now uses `performance.now()` (with `Date.now()` fallback), so p95 values include sub-millisecond precision.

## Phase 2 Results (internal WAL batching + in-memory `wal_bytes`)

Phase 2 values below use the same mixed scenarios and count settings as the Phase 1 section. `Δ%` is computed versus Phase 1.

| Scenario                          | Value Size (bytes) | Native ops/s (P1 -> P2)            | Native p95 ms (P1 -> P2)      | WASM ops/s (P1 -> P2)               | WASM p95 ms (P1 -> P2)      | Notes                                |
| --------------------------------- | -----------------: | ---------------------------------- | ----------------------------- | ----------------------------------- | --------------------------- | ------------------------------------ |
| mixed_random_70r_30w              |                 32 | `2673.363 -> 6534.520` (`+144.4%`) | `2.380 -> 0.124` (`-94.8%`)   | `5882.353 -> 9009.009` (`+53.2%`)   | `0.200 -> 0.200` (`~0.0%`)  | Native `count=500`; WASM `count=100` |
| mixed_random_50r_50w_with_updates |                 32 | `2359.634 -> 7840.841` (`+232.3%`) | `3.354 -> 0.101` (`-97.0%`)   | `5555.556 -> 14925.373` (`+168.7%`) | `0.300 -> 0.100` (`-66.7%`) | Native `count=500`; WASM `count=100` |
| mixed_random_60r_20w_20d          |                 32 | `2578.329 -> 7361.530` (`+185.5%`) | `3.052 -> 0.101` (`-96.7%`)   | `6024.096 -> 12345.679` (`+104.9%`) | `0.300 -> 0.200` (`-33.3%`) | Native `count=500`; WASM `count=100` |
| mixed_random_70r_30w              |                256 | `2471.936 -> 6436.643` (`+160.4%`) | `2.766 -> 0.119` (`-95.7%`)   | `2941.176 -> 10638.298` (`+261.7%`) | `0.800 -> 0.200` (`-75.0%`) | Native `count=500`; WASM `count=100` |
| mixed_random_50r_50w_with_updates |                256 | `2204.954 -> 7984.770` (`+262.1%`) | `3.219 -> 0.106` (`-96.7%`)   | `5347.594 -> 11764.706` (`+120.0%`) | `0.300 -> 0.200` (`-33.3%`) | Native `count=500`; WASM `count=100` |
| mixed_random_60r_20w_20d          |                256 | `2250.680 -> 7086.641` (`+214.9%`) | `2.631 -> 0.100` (`-96.2%`)   | `5882.353 -> 8403.361` (`+42.9%`)   | `0.300 -> 0.200` (`-33.3%`) | Native `count=500`; WASM `count=100` |
| mixed_random_70r_30w              |               4096 | `1140.796 -> 2037.671` (`+78.6%`)  | `2.448 -> 0.738` (`-69.9%`)   | `3003.003 -> 3676.471` (`+22.4%`)   | `0.400 -> 0.400` (`~0.0%`)  | Native `count=500`; WASM `count=100` |
| mixed_random_50r_50w_with_updates |               4096 | `1590.260 -> 1675.335` (`+5.4%`)   | `2.190 -> 0.938` (`-57.2%`)   | `3225.806 -> 4716.981` (`+46.2%`)   | `0.400 -> 0.400` (`~0.0%`)  | Native `count=500`; WASM `count=100` |
| mixed_random_60r_20w_20d          |               4096 | `2294.221 -> 2178.766` (`-5.0%`)   | `0.633 -> 0.789` (`+24.6%`)   | `3205.128 -> 3952.569` (`+23.3%`)   | `0.400 -> 0.400` (`~0.0%`)  | Native `count=500`; WASM `count=100` |
| mixed_random_70r_30w              |          1,048,576 | `35.669 -> 43.998` (`+23.4%`)      | `78.036 -> 42.167` (`-46.0%`) | `277.778 -> 272.109` (`-2.0%`)      | `6.800 -> 6.600` (`-2.9%`)  | Native `count=64`; WASM `count=4`    |
| mixed_random_50r_50w_with_updates |          1,048,576 | `39.794 -> 38.335` (`-3.7%`)       | `46.966 -> 58.213` (`+24.0%`) | `283.688 -> 294.118` (`+3.7%`)      | `6.400 -> 6.000` (`-6.3%`)  | Native `count=64`; WASM `count=4`    |
| mixed_random_60r_20w_20d          |          1,048,576 | `46.140 -> 48.729` (`+5.6%`)       | `44.129 -> 44.055` (`-0.2%`)  | `289.855 -> 307.692` (`+6.2%`)      | `6.400 -> 5.600` (`-12.5%`) | Native `count=64`; WASM `count=4`    |

## Phase 3 Results (encode/decode buffer reuse)

Phase 3 values below compare against Phase 2 using the same scenario/count matrix.

| Scenario                          | Value Size (bytes) | Native ops/s (P2 -> P3)            | Native p95 ms (P2 -> P3)      | WASM ops/s (P2 -> P3)               | WASM p95 ms (P2 -> P3)       | Notes                                |
| --------------------------------- | -----------------: | ---------------------------------- | ----------------------------- | ----------------------------------- | ---------------------------- | ------------------------------------ |
| mixed_random_70r_30w              |                 32 | `6534.520 -> 8145.130` (`+24.7%`)  | `0.124 -> 0.133` (`+7.0%`)    | `9009.009 -> 7874.016` (`-12.6%`)   | `0.200 -> 0.200` (`~0.0%`)   | Native `count=500`; WASM `count=100` |
| mixed_random_50r_50w_with_updates |                 32 | `7840.841 -> 11789.614` (`+50.4%`) | `0.101 -> 0.078` (`-23.2%`)   | `14925.373 -> 11111.111` (`-25.6%`) | `0.100 -> 0.200` (`+100.0%`) | Native `count=500`; WASM `count=100` |
| mixed_random_60r_20w_20d          |                 32 | `7361.530 -> 11494.407` (`+56.1%`) | `0.101 -> 0.072` (`-29.2%`)   | `12345.679 -> 9803.922` (`-20.6%`)  | `0.200 -> 0.200` (`~0.0%`)   | Native `count=500`; WASM `count=100` |
| mixed_random_70r_30w              |                256 | `6436.643 -> 9536.775` (`+48.2%`)  | `0.119 -> 0.091` (`-23.6%`)   | `10638.298 -> 8695.652` (`-18.3%`)  | `0.200 -> 0.200` (`~0.0%`)   | Native `count=500`; WASM `count=100` |
| mixed_random_50r_50w_with_updates |                256 | `7984.770 -> 10985.178` (`+37.6%`) | `0.106 -> 0.082` (`-22.8%`)   | `11764.706 -> 10869.565` (`-7.6%`)  | `0.200 -> 0.200` (`~0.0%`)   | Native `count=500`; WASM `count=100` |
| mixed_random_60r_20w_20d          |                256 | `7086.641 -> 10337.867` (`+45.9%`) | `0.100 -> 0.084` (`-15.7%`)   | `8403.361 -> 8333.333` (`-0.8%`)    | `0.200 -> 0.200` (`~0.0%`)   | Native `count=500`; WASM `count=100` |
| mixed_random_70r_30w              |               4096 | `2037.671 -> 2662.683` (`+30.7%`)  | `0.738 -> 0.520` (`-29.6%`)   | `3676.471 -> 3267.974` (`-11.1%`)   | `0.400 -> 0.400` (`~0.0%`)   | Native `count=500`; WASM `count=100` |
| mixed_random_50r_50w_with_updates |               4096 | `1675.335 -> 2610.600` (`+55.8%`)  | `0.938 -> 0.592` (`-36.9%`)   | `4716.981 -> 4016.064` (`-14.9%`)   | `0.400 -> 0.400` (`~0.0%`)   | Native `count=500`; WASM `count=100` |
| mixed_random_60r_20w_20d          |               4096 | `2178.766 -> 2713.318` (`+24.5%`)  | `0.789 -> 0.633` (`-19.7%`)   | `3952.569 -> 3676.471` (`-7.0%`)    | `0.400 -> 0.400` (`~0.0%`)   | Native `count=500`; WASM `count=100` |
| mixed_random_70r_30w              |          1,048,576 | `43.998 -> 44.601` (`+1.4%`)       | `42.167 -> 43.665` (`+3.6%`)  | `272.109 -> 254.777` (`-6.4%`)      | `6.600 -> 7.400` (`+12.1%`)  | Native `count=64`; WASM `count=4`    |
| mixed_random_50r_50w_with_updates |          1,048,576 | `38.335 -> 40.661` (`+6.1%`)       | `58.213 -> 47.854` (`-17.8%`) | `294.118 -> 275.862` (`-6.2%`)      | `6.000 -> 6.500` (`+8.3%`)   | Native `count=64`; WASM `count=4`    |
| mixed_random_60r_20w_20d          |          1,048,576 | `48.729 -> 48.667` (`-0.1%`)       | `44.055 -> 44.308` (`+0.6%`)  | `307.692 -> 268.456` (`-12.8%`)     | `5.600 -> 6.900` (`+23.2%`)  | Native `count=64`; WASM `count=4`    |

## Phase 4 Results (SST v2 block format + point-read index)

Phase 4 values below compare against Phase 3 using the same scenario/count matrix where runs completed.

| Scenario                          | Value Size (bytes) | Native ops/s (P3 -> P4)             | Native p95 ms (P3 -> P4)      | WASM ops/s (P3 -> P4)              | WASM p95 ms (P3 -> P4)       | Notes                                                            |
| --------------------------------- | -----------------: | ----------------------------------- | ----------------------------- | ---------------------------------- | ---------------------------- | ---------------------------------------------------------------- |
| mixed_random_70r_30w              |                 32 | `8145.130 -> 8826.677` (`+8.4%`)    | `0.133 -> 0.119` (`-10.1%`)   | `7874.016 -> 3802.281` (`-51.7%`)  | `0.200 -> 0.400` (`+100.0%`) | Native `count=500`; WASM `count=100`                             |
| mixed_random_50r_50w_with_updates |                 32 | `11789.614 -> 11004.389` (`-6.7%`)  | `0.078 -> 0.080` (`+2.9%`)    | `11111.111 -> 6024.096` (`-45.8%`) | `0.200 -> 0.300` (`+50.0%`)  | Native `count=500`; WASM `count=100`                             |
| mixed_random_60r_20w_20d          |                 32 | `11494.407 -> 10538.879` (`-8.3%`)  | `0.072 -> 0.078` (`+9.5%`)    | `9803.922 -> 4901.961` (`-50.0%`)  | `0.200 -> 0.300` (`+50.0%`)  | Native `count=500`; WASM `count=100`                             |
| mixed_random_70r_30w              |                256 | `9536.775 -> 11279.574` (`+18.3%`)  | `0.091 -> 0.071` (`-22.7%`)   | `8695.652 -> 4651.163` (`-46.5%`)  | `0.200 -> 0.400` (`+100.0%`) | Native `count=500`; WASM `count=100`                             |
| mixed_random_50r_50w_with_updates |                256 | `10985.178 -> 13009.695` (`+18.4%`) | `0.082 -> 0.058` (`-28.9%`)   | `10869.565 -> 5780.347` (`-46.8%`) | `0.200 -> 0.300` (`+50.0%`)  | Native `count=500`; WASM `count=100`                             |
| mixed_random_60r_20w_20d          |                256 | `10337.867 -> 11787.750` (`+14.0%`) | `0.084 -> 0.065` (`-23.4%`)   | `8333.333 -> 4608.295` (`-44.7%`)  | `0.200 -> 0.400` (`+100.0%`) | Native `count=500`; WASM `count=100`                             |
| mixed_random_70r_30w              |               4096 | `2662.683 -> 10998.347` (`+313.0%`) | `0.520 -> 0.071` (`-86.4%`)   | `3267.974 -> 4237.288` (`+29.7%`)  | `0.400 -> 0.300` (`-25.0%`)  | Native `count=500`; WASM `count=100`                             |
| mixed_random_50r_50w_with_updates |               4096 | `2610.600 -> 7500.994` (`+187.3%`)  | `0.592 -> 0.095` (`-84.0%`)   | `4016.064 -> 5263.158` (`+31.1%`)  | `0.400 -> 0.300` (`-25.0%`)  | Native `count=500`; WASM `count=100`                             |
| mixed_random_60r_20w_20d          |               4096 | `2713.318 -> 12372.052` (`+355.9%`) | `0.633 -> 0.058` (`-90.8%`)   | `3676.471 -> 4901.961` (`+33.3%`)  | `0.400 -> 0.300` (`-25.0%`)  | Native `count=500`; WASM `count=100`                             |
| mixed_random_70r_30w              |          1,048,576 | `44.601 -> 89.716` (`+101.2%`)      | `43.665 -> 51.758` (`+18.5%`) | N/A                                | N/A                          | Native `count=64`; WASM 1MB run did not complete in this session |
| mixed_random_50r_50w_with_updates |          1,048,576 | `40.661 -> 62.312` (`+53.2%`)       | `47.854 -> 65.344` (`+36.5%`) | N/A                                | N/A                          | Native `count=64`; WASM 1MB run did not complete in this session |
| mixed_random_60r_20w_20d          |          1,048,576 | `48.667 -> 96.424` (`+98.1%`)       | `44.308 -> 50.606` (`+14.2%`) | N/A                                | N/A                          | Native `count=64`; WASM 1MB run did not complete in this session |

## Phase 5 Results (per-SST bloom filters)

Phase 5 values below compare against Phase 4 using the same scenario/count matrix where runs completed.

| Scenario                          | Value Size (bytes) | Native ops/s (P4 -> P5)             | Native p95 ms (P4 -> P5)       | WASM ops/s (P4 -> P5)             | WASM p95 ms (P4 -> P5)      | Notes                                                                                     |
| --------------------------------- | -----------------: | ----------------------------------- | ------------------------------ | --------------------------------- | --------------------------- | ----------------------------------------------------------------------------------------- |
| mixed_random_70r_30w              |                 32 | `8826.677 -> 7514.155` (`-14.9%`)   | `0.119 -> 0.179` (`+50.3%`)    | `3802.281 -> 3012.048` (`-20.8%`) | `0.400 -> 0.500` (`+25.0%`) | Native `count=500`; WASM `count=100`                                                      |
| mixed_random_50r_50w_with_updates |                 32 | `11004.389 -> 9926.823` (`-9.8%`)   | `0.080 -> 0.114` (`+43.0%`)    | `6024.096 -> 4694.836` (`-22.1%`) | `0.300 -> 0.400` (`+33.3%`) | Native `count=500`; WASM `count=100`                                                      |
| mixed_random_60r_20w_20d          |                 32 | `10538.879 -> 10131.806` (`-3.9%`)  | `0.078 -> 0.093` (`+19.3%`)    | `4901.961 -> 3968.254` (`-19.0%`) | `0.300 -> 0.400` (`+33.3%`) | Native `count=500`; WASM `count=100`                                                      |
| mixed_random_70r_30w              |                256 | `11279.574 -> 10555.176` (`-6.4%`)  | `0.071 -> 0.081` (`+15.1%`)    | N/A                               | N/A                         | Native `count=500`; WASM `count=100` run did not complete (`count=1` smoke run completed) |
| mixed_random_50r_50w_with_updates |                256 | `13009.695 -> 11196.380` (`-13.9%`) | `0.058 -> 0.074` (`+27.9%`)    | N/A                               | N/A                         | Native `count=500`; WASM `count=100` run did not complete (`count=1` smoke run completed) |
| mixed_random_60r_20w_20d          |                256 | `11787.750 -> 10336.692` (`-12.3%`) | `0.065 -> 0.083` (`+29.1%`)    | N/A                               | N/A                         | Native `count=500`; WASM `count=100` run did not complete (`count=1` smoke run completed) |
| mixed_random_70r_30w              |               4096 | `10998.347 -> 9672.419` (`-12.1%`)  | `0.071 -> 0.073` (`+3.2%`)     | N/A                               | N/A                         | Native `count=500`; WASM `count=100` run did not complete (`count=1` smoke run completed) |
| mixed_random_50r_50w_with_updates |               4096 | `7500.994 -> 5926.754` (`-21.0%`)   | `0.095 -> 0.116` (`+21.8%`)    | N/A                               | N/A                         | Native `count=500`; WASM `count=100` run did not complete (`count=1` smoke run completed) |
| mixed_random_60r_20w_20d          |               4096 | `12372.052 -> 11477.487` (`-7.2%`)  | `0.058 -> 0.069` (`+18.5%`)    | N/A                               | N/A                         | Native `count=500`; WASM `count=100` run did not complete (`count=1` smoke run completed) |
| mixed_random_70r_30w              |          1,048,576 | `89.716 -> 41.586` (`-53.6%`)       | `51.758 -> 75.860` (`+46.6%`)  | N/A                               | N/A                         | Native `count=64`; WASM 1MB mixed run not completed in Phase 5                            |
| mixed_random_50r_50w_with_updates |          1,048,576 | `62.312 -> 38.544` (`-38.1%`)       | `65.344 -> 122.174` (`+87.0%`) | N/A                               | N/A                         | Native `count=64`; WASM 1MB mixed run not completed in Phase 5                            |
| mixed_random_60r_20w_20d          |          1,048,576 | `96.424 -> 61.520` (`-36.2%`)       | `50.606 -> 78.449` (`+55.0%`)  | N/A                               | N/A                         | Native `count=64`; WASM 1MB mixed run not completed in Phase 5                            |

## Phase 6 Results (SST metadata/index cache + block cache)

Phase 6 values below compare against Phase 5 where a Phase 5 baseline exists.

| Scenario                          | Value Size (bytes) | Native ops/s (P5 -> P6)             | Native p95 ms (P5 -> P6)       | WASM ops/s (P5 -> P6)               | WASM p95 ms (P5 -> P6)      | Notes                                                                           |
| --------------------------------- | -----------------: | ----------------------------------- | ------------------------------ | ----------------------------------- | --------------------------- | ------------------------------------------------------------------------------- |
| mixed_random_70r_30w              |                 32 | `7514.155 -> 13546.587` (`+80.3%`)  | `0.179 -> 0.042` (`-76.6%`)    | `3012.048 -> 14285.714` (`+374.2%`) | `0.500 -> 0.100` (`-80.0%`) | Native `count=500`; WASM `count=100`                                            |
| mixed_random_50r_50w_with_updates |                 32 | `9926.823 -> 16396.108` (`+65.2%`)  | `0.114 -> 0.029` (`-74.8%`)    | `4694.836 -> 22222.222` (`+373.3%`) | `0.400 -> 0.100` (`-75.0%`) | Native `count=500`; WASM `count=100`                                            |
| mixed_random_60r_20w_20d          |                 32 | `10131.806 -> 16452.623` (`+62.4%`) | `0.093 -> 0.029` (`-68.8%`)    | `3968.254 -> 20408.163` (`+414.3%`) | `0.400 -> 0.100` (`-75.0%`) | Native `count=500`; WASM `count=100`                                            |
| mixed_random_70r_30w              |                256 | `10555.176 -> 18244.596` (`+72.9%`) | `0.081 -> 0.010` (`-88.2%`)    | N/A -> `24390.244`                  | N/A -> `0.100`              | Native `count=500`; WASM `count=100` (Phase 5 `count=100` baseline unavailable) |
| mixed_random_50r_50w_with_updates |                256 | `11196.380 -> 18303.872` (`+63.5%`) | `0.074 -> 0.009` (`-87.4%`)    | N/A -> `23255.814`                  | N/A -> `0.100`              | Native `count=500`; WASM `count=100` (Phase 5 `count=100` baseline unavailable) |
| mixed_random_60r_20w_20d          |                256 | `10336.692 -> 18319.464` (`+77.2%`) | `0.083 -> 0.010` (`-87.6%`)    | N/A -> `25641.026`                  | N/A -> `0.100`              | Native `count=500`; WASM `count=100` (Phase 5 `count=100` baseline unavailable) |
| mixed_random_70r_30w              |               4096 | `9672.419 -> 17609.018` (`+82.1%`)  | `0.073 -> 0.024` (`-67.5%`)    | N/A -> `14285.714`                  | N/A -> `0.200`              | Native `count=500`; WASM `count=100` (Phase 5 `count=100` baseline unavailable) |
| mixed_random_50r_50w_with_updates |               4096 | `5926.754 -> 10286.937` (`+73.6%`)  | `0.116 -> 0.035` (`-69.9%`)    | N/A -> `14705.882`                  | N/A -> `0.200`              | Native `count=500`; WASM `count=100` (Phase 5 `count=100` baseline unavailable) |
| mixed_random_60r_20w_20d          |               4096 | `11477.487 -> 19627.471` (`+71.0%`) | `0.069 -> 0.020` (`-71.0%`)    | N/A -> `16129.032`                  | N/A -> `0.100`              | Native `count=500`; WASM `count=100` (Phase 5 `count=100` baseline unavailable) |
| mixed_random_70r_30w              |          1,048,576 | `41.586 -> 112.378` (`+170.2%`)     | `75.860 -> 46.181` (`-39.1%`)  | N/A -> `249.501`                    | N/A -> `21.100`             | Native `count=64`; WASM `count=100` via instrumented/progress-enabled harness   |
| mixed_random_50r_50w_with_updates |          1,048,576 | `38.544 -> 74.406` (`+93.0%`)       | `122.174 -> 50.933` (`-58.3%`) | N/A -> `138.562`                    | N/A -> `23.200`             | Native `count=64`; WASM `count=100` via instrumented/progress-enabled harness   |
| mixed_random_60r_20w_20d          |          1,048,576 | `61.520 -> 122.286` (`+98.8%`)      | `78.449 -> 44.676` (`-43.1%`)  | N/A -> `286.123`                    | N/A -> `19.000`             | Native `count=64`; WASM `count=100` via instrumented/progress-enabled harness   |

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
- Native comparative standard sizes (`jazz-lsm` / `bf-tree` / `rocksdb` / `surrealkv` / `fjall`): `DYLD_LIBRARY_PATH=/opt/homebrew/opt/llvm/lib DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/opt/llvm/lib LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib cargo bench -p jazz-lsm --features compare-native --bench compare_native -- --quick`
- Native comparative 1MB (`jazz-lsm` / `rocksdb` / `surrealkv` / `fjall`): `DYLD_LIBRARY_PATH=/opt/homebrew/opt/llvm/lib DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/opt/llvm/lib LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib JAZZ_LSM_BENCH_KEY_COUNT=64 JAZZ_LSM_BENCH_VALUE_SIZES=1048576 cargo bench -p jazz-lsm --features compare-native --bench compare_native -- --quick`
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
- Phase 5 mixed native rerun (32/256/4096): `cargo run -p jazz-lsm --release --bin mixed_bench_native -- --count 500 --value-sizes 32,256,4096 --json`
- Phase 5 mixed native rerun (1MB): `cargo run -p jazz-lsm --release --bin mixed_bench_native -- --count 64 --value-sizes 1048576 --json`
- Phase 5 mixed wasm/opfs rerun (32 only): `pnpm --dir /Users/anselm/jazz2-clean/crates/jazz-lsm run bench:wasm:opfs -- --profile mixed --count 100 --value-sizes 32 --json`
- Phase 5 mixed wasm/opfs reruns (`256/4096` at `count=100`): did not complete before manual termination in this session (`count=1` smoke runs completed)
- Phase 5 mixed wasm/opfs reruns (`1MB`): not completed in this session
- Phase 6 mixed native rerun (32/256/4096): `cargo run -p jazz-lsm --release --bin mixed_bench_native -- --count 500 --value-sizes 32,256,4096 --json`
- Phase 6 mixed native rerun (1MB): `cargo run -p jazz-lsm --release --bin mixed_bench_native -- --count 64 --value-sizes 1048576 --json`
- Phase 6 mixed wasm/opfs rerun (32/256/4096): `pnpm --dir /Users/anselm/jazz2-clean/crates/jazz-lsm run bench:wasm:opfs -- --profile mixed --count 100 --value-sizes 32,256,4096 --json`
- Phase 6 mixed wasm/opfs rerun (1MB): `pnpm --dir /Users/anselm/jazz2-clean/crates/jazz-lsm run bench:wasm:opfs -- --profile mixed --count 100 --value-sizes 1048576 --json --progress`
- WASM harness progress + bootstrap-fix validation: `pnpm --dir /Users/anselm/jazz2-clean/crates/jazz-lsm run bench:wasm:opfs -- --profile basic --count 1 --value-sizes 32 --progress`
- WASM 1MB instrumented sanity (`basic`, `count=1`): `pnpm --dir /Users/anselm/jazz2-clean/crates/jazz-lsm run bench:wasm:opfs -- --profile basic --count 1 --value-sizes 1048576 --json --progress`
- WASM 1MB instrumented sanity (`basic`, `count=2`): `pnpm --dir /Users/anselm/jazz2-clean/crates/jazz-lsm run bench:wasm:opfs -- --profile basic --count 2 --value-sizes 1048576 --json --progress`
- WASM 1MB instrumented mixed (`count=4`): `pnpm --dir /Users/anselm/jazz2-clean/crates/jazz-lsm run bench:wasm:opfs -- --profile mixed --count 4 --value-sizes 1048576 --json --progress`
- WASM 1MB instrumented mixed (`count=32`): `pnpm --dir /Users/anselm/jazz2-clean/crates/jazz-lsm run bench:wasm:opfs -- --profile mixed --count 32 --value-sizes 1048576 --json --progress`
- WASM 1MB instrumented mixed (`count=100`): `pnpm --dir /Users/anselm/jazz2-clean/crates/jazz-lsm run bench:wasm:opfs -- --profile mixed --count 100 --value-sizes 1048576 --json --progress`
