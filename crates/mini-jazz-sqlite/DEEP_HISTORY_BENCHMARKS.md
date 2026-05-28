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

| Scenario  | Metric                     |       Base |      Incr |
| --------- | -------------------------- | ---------: | --------: |
| Append    | completed updates          |       2225 |      2225 |
| Append    | total loop                 |    7360 ms |   7653 ms |
| Append    | write only                 |     812 ms |   3406 ms |
| Append    | avg loop/update            |    3.31 ms |   3.44 ms |
| Append    | avg write/update           |    0.37 ms |   1.53 ms |
| Append    | sampled receive            |    6544 ms |   4245 ms |
| Append    | live receive avg           |    1091 ms |    707 ms |
| Append    | live receive p50           |     886 ms |    819 ms |
| Append    | live receive p95           |    3303 ms |   1390 ms |
| Append    | cold load                  |    1898 ms |   2137 ms |
| Append    | current read               |    0.15 ms |  51.60 ms |
| Append    | history rows               |       2226 |      2225 |
| Append    | final payload bytes        |     13,350 |    13,350 |
| Append    | bundle bytes               | 16,115,414 | 1,411,141 |
| Append    | database bytes             | 18,796,544 |   573,440 |
| Append    | total file bytes           | 22,319,040 | 4,738,720 |
| Append    | database / final payload   |   1407.98x |    42.95x |
| Append    | total file / final payload |   1671.84x |   354.96x |
| Append    | sidecar nodes              |        N/A |      4449 |
| Append    | sidecar leaves             |        N/A |      2225 |
| Append    | sidecar concat nodes       |        N/A |      2224 |
| Append    | sidecar segment bytes      |        N/A |    13,350 |
| Automerge | completed updates          |       2900 |      2900 |
| Automerge | total loop                 |    7589 ms | 18,519 ms |
| Automerge | write only                 |     811 ms |   9019 ms |
| Automerge | avg loop/update            |    2.62 ms |   6.39 ms |
| Automerge | avg write/update           |    0.28 ms |   3.11 ms |
| Automerge | sampled receive            |    6774 ms |   9497 ms |
| Automerge | live receive avg           |    1129 ms |   1583 ms |
| Automerge | live receive p50           |     977 ms |   2090 ms |
| Automerge | live receive p95           |    3511 ms |   3271 ms |
| Automerge | cold load                  |    1895 ms |   7311 ms |
| Automerge | current read               |    0.14 ms |  41.52 ms |
| Automerge | history rows               |       2901 |      2900 |
| Automerge | final payload bytes        |       1750 |      1750 |
| Automerge | source trace gzip bytes    |    904,360 |   904,360 |
| Automerge | bundle bytes               |  5,351,258 | 2,209,941 |
| Automerge | database bytes             |  9,859,072 |   892,928 |
| Automerge | total file bytes           | 13,729,560 | 5,082,808 |
| Automerge | database / final payload   |   5633.76x |   510.24x |
| Automerge | total file / final payload |   7845.46x |  2904.46x |
| Automerge | database / source gzip     |     10.90x |     0.99x |
| Automerge | bundle / source gzip       |      5.92x |     2.44x |
| Automerge | sidecar nodes              |        N/A |    17,042 |
| Automerge | sidecar leaves             |        N/A |      2325 |
| Automerge | sidecar concat nodes       |        N/A |    14,717 |
| Automerge | sidecar segment bytes      |        N/A |      2325 |
| Canvas    | completed updates          |       3900 |      3900 |
| Canvas    | total loop                 |    7801 ms | 15,234 ms |
| Canvas    | write only                 |    1043 ms |   5217 ms |
| Canvas    | avg loop/update            |    2.00 ms |   3.91 ms |
| Canvas    | avg write/update           |    0.27 ms |   1.34 ms |
| Canvas    | sampled receive            |    6753 ms | 10,012 ms |
| Canvas    | live receive avg           |    1125 ms |   1669 ms |
| Canvas    | live receive p50           |    1119 ms |   1578 ms |
| Canvas    | live receive p95           |    3026 ms |   4394 ms |
| Canvas    | cold load                  |    2007 ms |   5479 ms |
| Canvas    | current read               |    0.11 ms |   0.18 ms |
| Canvas    | history rows               |       3901 |      3900 |
| Canvas    | final payload bytes        |         46 |        46 |
| Canvas    | position trace gzip bytes  |     78,526 |    77,211 |
| Canvas    | position trace JSON bytes  |    205,609 |   182,209 |
| Canvas    | bundle bytes               |  2,455,136 | 2,591,442 |
| Canvas    | database bytes             |    884,736 |   913,408 |
| Canvas    | total file bytes           |  5,070,520 | 5,090,976 |
| Canvas    | database / final payload   |        N/A |       N/A |
| Canvas    | total file / final payload |        N/A |       N/A |
| Canvas    | database / position gzip   |     11.27x |    11.83x |
| Canvas    | bundle / position gzip     |     31.27x |    33.56x |
| Canvas    | sidecar nodes              |        N/A |      7799 |
| Canvas    | sidecar leaves             |        N/A |      3900 |
| Canvas    | sidecar concat nodes       |        N/A |      3899 |
| Canvas    | sidecar segment bytes      |        N/A |    31,392 |

## Notes

- Current reads remain fast in the naive baseline because the current projection
  is doing its job.
- Incremental sidecar copying helped sampled live receive, but full Jazz
  row-history export still dominated.
- Current text materialization is expensive in the incremental sequence probes;
  that is a useful pressure point for the next storage shape.
