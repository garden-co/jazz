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
| Append    | total loop                 |    7443 ms |   6438 ms |
| Append    | write only                 |        N/A |   2516 ms |
| Append    | avg loop/update            |    3.35 ms |   2.89 ms |
| Append    | avg write/update           |        N/A |   1.13 ms |
| Append    | sampled receive            |        N/A |   3920 ms |
| Append    | live receive avg           |    1135 ms |    653 ms |
| Append    | live receive p50           |     924 ms |       N/A |
| Append    | live receive p95           |    3470 ms |   1377 ms |
| Append    | cold load                  |    1913 ms |   1801 ms |
| Append    | current read               |    0.13 ms |  50.82 ms |
| Append    | history rows               |       2226 |       N/A |
| Append    | final payload bytes        |     13,350 |       N/A |
| Append    | bundle bytes               | 16,115,414 |       N/A |
| Append    | database bytes             | 18,796,544 |       N/A |
| Append    | total file bytes           | 22,319,040 |       N/A |
| Append    | database / final payload   |   1407.98x |       N/A |
| Append    | total file / final payload |   1671.84x |       N/A |
| Automerge | completed updates          |       2900 |      2900 |
| Automerge | total loop                 |    7118 ms | 23,861 ms |
| Automerge | write only                 |        N/A | 13,016 ms |
| Automerge | avg loop/update            |    2.45 ms |   8.23 ms |
| Automerge | avg write/update           |        N/A |   4.49 ms |
| Automerge | sampled receive            |        N/A | 10,842 ms |
| Automerge | live receive avg           |    1062 ms |   1807 ms |
| Automerge | live receive p50           |     804 ms |       N/A |
| Automerge | live receive p95           |    3322 ms |   3437 ms |
| Automerge | cold load                  |    1946 ms | 12,276 ms |
| Automerge | current read               |    0.13 ms |  40.24 ms |
| Automerge | history rows               |       2901 |       N/A |
| Automerge | final payload bytes        |       1750 |       N/A |
| Automerge | source trace gzip bytes    |    904,360 |       N/A |
| Automerge | bundle bytes               |  5,351,258 |       N/A |
| Automerge | database bytes             |  9,859,072 |       N/A |
| Automerge | total file bytes           | 13,729,560 |       N/A |
| Automerge | database / final payload   |   5633.76x |       N/A |
| Automerge | total file / final payload |   7845.46x |       N/A |
| Automerge | database / source gzip     |     10.90x |       N/A |
| Automerge | bundle / source gzip       |      5.92x |       N/A |
| Canvas    | completed updates          |       3900 |      3900 |
| Canvas    | total loop                 |    7602 ms | 17,354 ms |
| Canvas    | write only                 |        N/A |   6687 ms |
| Canvas    | avg loop/update            |    1.95 ms |   4.45 ms |
| Canvas    | avg write/update           |        N/A |   1.71 ms |
| Canvas    | sampled receive            |        N/A | 10,663 ms |
| Canvas    | live receive avg           |    1141 ms |   1777 ms |
| Canvas    | live receive p50           |    1123 ms |       N/A |
| Canvas    | live receive p95           |    3065 ms |   3342 ms |
| Canvas    | cold load                  |    2048 ms |   3745 ms |
| Canvas    | current read               |    0.12 ms |   0.17 ms |
| Canvas    | history rows               |       3901 |       N/A |
| Canvas    | final payload bytes        |         46 |       N/A |
| Canvas    | position trace gzip bytes  |     78,526 |       N/A |
| Canvas    | position trace JSON bytes  |    205,609 |       N/A |
| Canvas    | bundle bytes               |  2,455,136 |       N/A |
| Canvas    | database bytes             |    884,736 |       N/A |
| Canvas    | total file bytes           |  5,070,520 |       N/A |
| Canvas    | database / final payload   |        N/A |       N/A |
| Canvas    | total file / final payload |        N/A |       N/A |
| Canvas    | database / position gzip   |     11.27x |       N/A |
| Canvas    | bundle / position gzip     |     31.27x |       N/A |

## Notes

- Current reads remain fast in the naive baseline because the current projection
  is doing its job.
- Incremental sidecar copying helped sampled live receive, but full Jazz
  row-history export still dominated.
- Current text materialization is expensive in the incremental sequence probes;
  that is a useful pressure point for the next storage shape.
