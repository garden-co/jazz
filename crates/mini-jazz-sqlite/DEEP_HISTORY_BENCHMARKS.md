# Deep History Benchmarks

Status: comparison tracker for deep row-history experiments.

These probes measure rows with very deep edit histories. The goal is to keep
canonical runs around 10 seconds while exploring, then drive all three below 1
second as storage and sync paths improve.

Run from the workspace root:

```bash
cargo build -q -p mini-jazz-sqlite --example perf_scenarios
```

## Canonical Inputs

Use a hard 10s wall-clock guard when searching for the largest full-result
input size. A full result includes:

- normal row writes
- sampled live receive path: export, apply, poll listener
- final full table-history export
- cold-load apply and read
- storage stats

Current canonical inputs:

```bash
MINI_JAZZ_PERF_ONLY_DEEP_HISTORY=append \
MINI_JAZZ_DEEP_HISTORY_APPEND_TOKENS=2225 \
MINI_JAZZ_DEEP_HISTORY_SAMPLE_EVERY=445 \
target/debug/examples/perf_scenarios

MINI_JAZZ_PERF_ONLY_DEEP_HISTORY=automerge-paper \
MINI_JAZZ_DEEP_HISTORY_AUTOMERGE_UPDATES=2900 \
MINI_JAZZ_DEEP_HISTORY_SAMPLE_EVERY=580 \
target/debug/examples/perf_scenarios

MINI_JAZZ_PERF_ONLY_DEEP_HISTORY=canvas \
MINI_JAZZ_DEEP_HISTORY_CANVAS_FRAMES=3900 \
MINI_JAZZ_DEEP_HISTORY_SAMPLE_EVERY=780 \
target/debug/examples/perf_scenarios
```

Canvas `3900` frames is about 65 simulated seconds at 60 FPS.

## Experiment Columns

| Short | Meaning                                                                        |
| ----: | ------------------------------------------------------------------------------ |
|  Base | Naive full-row history in vanilla SQLite, debug build                          |
| Base1 | Base plus compact tx metadata: derived public tx ids and NULL empty metadata   |
| Base2 | Base1 plus packed tx writes/reads JSON on `jazz_tx`                            |
| Base3 | Base2 plus compact bundle wire dictionaries and positional record arrays       |
| Block | Base3 plus sealed lz4 history blocks after the write loop                      |
|  Incr | Same sequence sidecar, but sampled sync copies only new sidecar nodes/segments |

The earlier full-sidecar-snapshot sequence experiment was replaced by `Incr` and
is no longer tracked as a canonical column.

## Timing Fields

- `write only`: edit generation plus durable write/version insert work only
- `sampled receive`: sum of sampled live receive/listener checks
- `total loop`: write loop wall time, including sampled receive checks
- `avg loop/update`: `total loop / completed updates`
- `avg write/update`: `write only / completed updates`
- `current read`: current projection lookup

For append and document edits, final-payload ratios compare storage to the text
content produced by the run. For canvas positions, final-payload ratios are
intentionally `N/A`; compare to the gzipped position trace instead.

## Comparison Table

| Scenario  | Metric                     |       Base |      Base1 |      Base2 |      Base3 |      Block |      Incr |
| --------- | -------------------------- | ---------: | ---------: | ---------: | ---------: | ---------: | --------: |
| Append    | completed updates          |       2225 |       2225 |       2225 |       2225 |       2225 |      2225 |
| Append    | total loop                 |    7360 ms |    7406 ms |    7687 ms |    7609 ms |    7752 ms |   7653 ms |
| Append    | write only                 |     812 ms |     623 ms |     725 ms |     767 ms |     775 ms |   3406 ms |
| Append    | avg loop/update            |    3.31 ms |    3.33 ms |    3.45 ms |    3.42 ms |    3.48 ms |   3.44 ms |
| Append    | avg write/update           |    0.37 ms |    0.28 ms |    0.33 ms |    0.34 ms |    0.35 ms |   1.53 ms |
| Append    | sampled receive            |    6544 ms |    6780 ms |    6959 ms |    6839 ms |    6972 ms |   4245 ms |
| Append    | live receive avg           |    1091 ms |    1130 ms |    1160 ms |    1140 ms |    1162 ms |    707 ms |
| Append    | live receive p50           |     886 ms |     924 ms |     974 ms |     969 ms |     986 ms |    819 ms |
| Append    | live receive p95           |    3303 ms |    3350 ms |    3382 ms |    3369 ms |    3456 ms |   1390 ms |
| Append    | cold load                  |    1898 ms |    1967 ms |    2081 ms |    1961 ms |    1989 ms |   2137 ms |
| Append    | current read               |    0.15 ms |    0.15 ms |    0.24 ms |    0.15 ms |    0.18 ms |  51.60 ms |
| Append    | history rows               |       2226 |       2226 |       2226 |       2226 |          0 |      2225 |
| Append    | final payload bytes        |     13,350 |     13,350 |     13,350 |     13,350 |     13,350 |    13,350 |
| Append    | bundle bytes               | 16,115,414 | 16,115,414 | 16,115,414 | 15,235,071 | 15,235,071 | 1,411,141 |
| Append    | database bytes             | 18,796,544 | 18,677,760 | 18,657,280 | 18,657,280 | 18,788,352 |   573,440 |
| Append    | live database bytes        |        N/A |        N/A |        N/A |        N/A |    258,048 |       N/A |
| Append    | freelist bytes             |        N/A |        N/A |        N/A |        N/A | 18,530,304 |       N/A |
| Append    | total file bytes           | 22,319,040 | 22,773,576 | 22,388,624 | 22,388,624 | 22,400,912 | 4,738,720 |
| Append    | database / final payload   |   1407.98x |   1399.08x |   1397.55x |   1397.55x |   1407.37x |    42.95x |
| Append    | total file / final payload |   1671.84x |   1705.89x |   1677.05x |   1677.05x |   1677.97x |   354.96x |
| Append    | sidecar nodes              |        N/A |        N/A |        N/A |        N/A |        N/A |      4449 |
| Append    | sidecar leaves             |        N/A |        N/A |        N/A |        N/A |        N/A |      2225 |
| Append    | sidecar concat nodes       |        N/A |        N/A |        N/A |        N/A |        N/A |      2224 |
| Append    | sidecar segment bytes      |        N/A |        N/A |        N/A |        N/A |        N/A |    13,350 |
| Automerge | completed updates          |       2900 |       2900 |       2900 |       2900 |       2900 |      2900 |
| Automerge | total loop                 |    7589 ms |    7528 ms |    8000 ms |    7999 ms |    8040 ms | 18,519 ms |
| Automerge | write only                 |     811 ms |     817 ms |     958 ms |     969 ms |     780 ms |   9019 ms |
| Automerge | avg loop/update            |    2.62 ms |    2.60 ms |    2.76 ms |    2.76 ms |    2.77 ms |   6.39 ms |
| Automerge | avg write/update           |    0.28 ms |    0.28 ms |    0.33 ms |    0.33 ms |    0.27 ms |   3.11 ms |
| Automerge | sampled receive            |    6774 ms |    6707 ms |    7038 ms |    7025 ms |    7155 ms |   9497 ms |
| Automerge | live receive avg           |    1129 ms |    1118 ms |    1173 ms |    1171 ms |    1192 ms |   1583 ms |
| Automerge | live receive p50           |     977 ms |     884 ms |     940 ms |     938 ms |     947 ms |   2090 ms |
| Automerge | live receive p95           |    3511 ms |    3429 ms |    3547 ms |    3532 ms |    3623 ms |   3271 ms |
| Automerge | cold load                  |    1895 ms |    1902 ms |    2024 ms |    2031 ms |    2039 ms |   7311 ms |
| Automerge | current read               |    0.14 ms |    0.14 ms |    0.14 ms |    0.13 ms |    0.15 ms |  41.52 ms |
| Automerge | history rows               |       2901 |       2901 |       2901 |       2901 |          0 |      2900 |
| Automerge | final payload bytes        |       1750 |       1750 |       1750 |       1750 |       1750 |      1750 |
| Automerge | source trace gzip bytes    |    904,360 |    904,360 |    904,360 |    904,360 |    904,360 |   904,360 |
| Automerge | bundle bytes               |  5,351,258 |  5,351,258 |  5,351,258 |  4,152,081 |  4,152,081 | 2,209,941 |
| Automerge | database bytes             |  9,859,072 |  9,707,520 |  9,687,040 |  9,687,040 |  9,846,784 |   892,928 |
| Automerge | live database bytes        |        N/A |        N/A |        N/A |        N/A |    262,144 |       N/A |
| Automerge | freelist bytes             |        N/A |        N/A |        N/A |        N/A |  9,584,640 |       N/A |
| Automerge | total file bytes           | 13,729,560 | 13,807,432 | 13,778,664 | 13,778,664 | 13,790,952 | 5,082,808 |
| Automerge | database / final payload   |   5633.76x |   5547.15x |   5535.45x |   5535.45x |   5626.73x |   510.24x |
| Automerge | total file / final payload |   7845.46x |   7889.96x |   7873.52x |   7873.52x |   7880.54x |  2904.46x |
| Automerge | database / source gzip     |     10.90x |     10.73x |     10.71x |     10.71x |     10.89x |     0.99x |
| Automerge | bundle / source gzip       |      5.92x |      5.92x |      5.92x |      4.59x |      4.59x |     2.44x |
| Automerge | sidecar nodes              |        N/A |        N/A |        N/A |        N/A |        N/A |    17,042 |
| Automerge | sidecar leaves             |        N/A |        N/A |        N/A |        N/A |        N/A |      2325 |
| Automerge | sidecar concat nodes       |        N/A |        N/A |        N/A |        N/A |        N/A |    14,717 |
| Automerge | sidecar segment bytes      |        N/A |        N/A |        N/A |        N/A |        N/A |      2325 |
| Canvas    | completed updates          |       3900 |       3900 |       3900 |       3900 |       3900 |      3900 |
| Canvas    | total loop                 |    7801 ms |    7896 ms |    8459 ms |    8455 ms |    8526 ms | 15,234 ms |
| Canvas    | write only                 |    1043 ms |     766 ms |     912 ms |     903 ms |     883 ms |   5217 ms |
| Canvas    | avg loop/update            |    2.00 ms |    2.02 ms |    2.17 ms |    2.17 ms |    2.19 ms |   3.91 ms |
| Canvas    | avg write/update           |    0.27 ms |    0.20 ms |    0.23 ms |    0.23 ms |    0.23 ms |   1.34 ms |
| Canvas    | sampled receive            |    6753 ms |    7126 ms |    7543 ms |    7548 ms |    7638 ms | 10,012 ms |
| Canvas    | live receive avg           |    1125 ms |    1188 ms |    1257 ms |    1258 ms |    1273 ms |   1669 ms |
| Canvas    | live receive p50           |    1119 ms |    1176 ms |    1261 ms |    1263 ms |    1284 ms |   1578 ms |
| Canvas    | live receive p95           |    3026 ms |    3159 ms |    3292 ms |    3274 ms |    3307 ms |   4394 ms |
| Canvas    | cold load                  |    2007 ms |    2072 ms |    2217 ms |    2236 ms |    2250 ms |   5479 ms |
| Canvas    | current read               |    0.11 ms |    0.14 ms |    0.13 ms |    0.13 ms |    0.13 ms |   0.18 ms |
| Canvas    | history rows               |       3901 |       3901 |       3901 |       3901 |          0 |      3900 |
| Canvas    | final payload bytes        |         46 |         46 |         46 |         46 |         46 |        46 |
| Canvas    | position trace gzip bytes  |     78,526 |     78,526 |     78,526 |     78,526 |     78,526 |    77,211 |
| Canvas    | position trace JSON bytes  |    205,609 |    205,609 |    205,609 |    205,609 |    205,609 |   182,209 |
| Canvas    | bundle bytes               |  2,455,136 |  2,455,136 |  2,455,136 |    858,561 |    858,561 | 2,591,442 |
| Canvas    | database bytes             |    884,736 |    679,936 |    659,456 |    659,456 |    946,176 |   913,408 |
| Canvas    | live database bytes        |        N/A |        N/A |        N/A |        N/A |    380,928 |       N/A |
| Canvas    | freelist bytes             |        N/A |        N/A |        N/A |        N/A |    565,248 |       N/A |
| Canvas    | total file bytes           |  5,070,520 |  4,857,480 |  4,828,760 |  4,828,760 |  4,845,168 | 5,090,976 |
| Canvas    | database / final payload   |        N/A |        N/A |        N/A |        N/A |        N/A |       N/A |
| Canvas    | total file / final payload |        N/A |        N/A |        N/A |        N/A |        N/A |       N/A |
| Canvas    | database / position gzip   |     11.27x |      8.66x |      8.40x |      8.40x |     12.05x |    11.83x |
| Canvas    | bundle / position gzip     |     31.27x |     31.27x |     31.27x |     10.93x |     10.93x |    33.56x |
| Canvas    | sidecar nodes              |        N/A |        N/A |        N/A |        N/A |        N/A |      7799 |
| Canvas    | sidecar leaves             |        N/A |        N/A |        N/A |        N/A |        N/A |      3900 |
| Canvas    | sidecar concat nodes       |        N/A |        N/A |        N/A |        N/A |        N/A |      3899 |
| Canvas    | sidecar segment bytes      |        N/A |        N/A |        N/A |        N/A |        N/A |    31,392 |

## Notes

- Current reads remain fast in the naive baseline because the current projection
  is doing its job.
- Incremental sidecar copying helped sampled live receive, but full Jazz
  row-history export still dominated.
- Current text materialization is expensive in the incremental sequence probes;
  that is a useful pressure point for the next storage shape.
- With block compaction hot tail set to `0`, the Block experiment removes all
  open history rows and leaves only the current projection plus one sealed block
  per scenario. `live database bytes` shows the real page footprint after
  compaction; `total file bytes` still includes freed pages unless the explicit
  reclaim step is run.

## Reclaim Probe

Quick non-canonical probe after adding `Runtime::reclaim_storage()`. Input was
the canonical append Block run with
`MINI_JAZZ_DEEP_HISTORY_COMPACT_HOT_TAIL=0` and
`MINI_JAZZ_DEEP_HISTORY_RECLAIM_AFTER_COMPACT=1`.

| Scenario | reclaim time | database bytes | live database bytes | freelist bytes | total file bytes | total file / final payload |
| -------- | -----------: | -------------: | ------------------: | -------------: | ---------------: | -------------------------: |
| Append   |      47.6 ms |        258,048 |             258,048 |              0 |          290,816 |                     21.78x |

## Batched Write Probe

Quick non-canonical probe after adding `MINI_JAZZ_DEEP_HISTORY_WRITE_BATCH_SIZE`.
Inputs were 2,000 updates per scenario, sample every 500 updates, max 10s,
write batch size 64, no history block compaction. The point was only to
separate SQLite commit cost from export/apply/cold-load cost.

| Scenario  | total loop | write only | avg write/update | sampled receive | cold load | database bytes | bundle bytes |
| --------- | ---------: | ---------: | ---------------: | --------------: | --------: | -------------: | -----------: |
| Append    |  5089.6 ms |   499.4 ms |          0.25 ms |       4585.3 ms | 1604.2 ms |     15,663,104 |   12,343,821 |
| Automerge |  3103.4 ms |   415.8 ms |          0.21 ms |       2633.5 ms |  964.0 ms |      5,431,296 |    2,324,985 |
| Canvas    |  2543.3 ms |   356.6 ms |          0.18 ms |       2183.7 ms |  786.5 ms |        393,216 |      437,270 |

Interpretation: grouped SQLite commits do help the pure write side, but the
large remaining time is still history export/apply/cold load. This argues for
tracking batched writes as an orthogonal benchmark dimension, not as a
replacement for history blocks.
