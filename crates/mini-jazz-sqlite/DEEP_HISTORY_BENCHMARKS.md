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

`MINI_JAZZ_PERF_ONLY_DEEP_HISTORY=all` runs append, Automerge, and canvas
baseline probes without running the broader perf suite. `all-history-blocks`
runs the Block probes, and `all-block-incr` runs the current Block+S append and
Automerge probes. Use these for smoke checks or when grouped scenarios should
run together.
Scenario-specific sample intervals override the shared
`MINI_JAZZ_DEEP_HISTORY_SAMPLE_EVERY`.

Current canonical inputs use scenario-specific sample intervals:

```bash
MINI_JAZZ_PERF_ONLY_DEEP_HISTORY=all \
MINI_JAZZ_DEEP_HISTORY_APPEND_TOKENS=2225 \
MINI_JAZZ_DEEP_HISTORY_APPEND_SAMPLE_EVERY=445 \
MINI_JAZZ_DEEP_HISTORY_AUTOMERGE_UPDATES=2900 \
MINI_JAZZ_DEEP_HISTORY_AUTOMERGE_SAMPLE_EVERY=580 \
MINI_JAZZ_DEEP_HISTORY_CANVAS_FRAMES=3900 \
MINI_JAZZ_DEEP_HISTORY_CANVAS_SAMPLE_EVERY=780 \
target/debug/examples/perf_scenarios
```

Or run one scenario at a time:

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

Block experiments also accept:

```bash
MINI_JAZZ_DEEP_HISTORY_MAX_ROWS_PER_BLOCK=500
```

Leave it unset for the current canonical Block column, which compacts each
row's cold accepted history into one lz4 block.

## Experiment Columns

|   Short | Meaning                                                                  |
| ------: | ------------------------------------------------------------------------ |
|   Base3 | Base2 plus compact bundle wire dictionaries and positional record arrays |
|   Block | Base3 plus sealed lz4 history blocks after the write loop                |
| Block+S | Block plus immutable sequence sidecar roots and incremental sidecar sync |

`Block+S` is the latest immutable sidecar implementation: text/position sidecar
segments are immutable, old Jazz root history is sealed into lz4 blocks, and
the current text root is rebuilt into large immutable leaves after compaction so
recent/current reads stay shallow.

## Timing Fields

- `write only`: edit generation plus durable write/version insert work only
- `sampled receive`: sum of sampled live receive/listener checks
- `total loop`: write loop wall time, including sampled receive checks
- `avg loop/update`: `total loop / completed updates`
- `avg write/update`: `write only / completed updates`
- `current read`: current projection lookup and any sidecar materialization
- `historical read`: average local point-in-time row lookup over sampled early,
  middle, and latest epochs; for Block and Block+S this crosses sealed blocks
  plus hot tail
- `tx info`: average `transaction_info(tx-id)` over sampled early, middle, and
  latest transaction ids

For append and document edits, final-payload ratios compare storage to the text
content produced by the run. For canvas positions, final-payload ratios are
intentionally `N/A`; compare to the gzipped position trace instead.

## Comparison Table

| Scenario  | Metric                     |      Base3 |      Block |   Block+S |
| --------- | -------------------------- | ---------: | ---------: | --------: |
| Append    | completed updates          |       2225 |       2225 |      2225 |
| Append    | total loop                 |    7609 ms |    7733 ms |   8331 ms |
| Append    | write only                 |     767 ms |     680 ms |   2934 ms |
| Append    | avg loop/update            |    3.42 ms |    3.48 ms |   3.74 ms |
| Append    | avg write/update           |    0.34 ms |    0.31 ms |   1.32 ms |
| Append    | sampled receive            |    6839 ms |    7048 ms |   5395 ms |
| Append    | live receive avg           |    1140 ms |    1175 ms |    899 ms |
| Append    | live receive p50           |     969 ms |    1000 ms |    974 ms |
| Append    | live receive p95           |    3369 ms |    3484 ms |   1762 ms |
| Append    | cold load                  |    1961 ms |    2012 ms |   2589 ms |
| Append    | current read               |    0.15 ms |    0.14 ms |   0.32 ms |
| Append    | historical read avg        |        N/A |   41.79 ms |  49.98 ms |
| Append    | tx info avg                |        N/A |    0.27 ms |   0.26 ms |
| Append    | history rows               |       2226 |        445 |       446 |
| Append    | final payload bytes        |     13,350 |     13,350 |    13,350 |
| Append    | bundle bytes               | 15,235,071 | 15,235,071 |   567,966 |
| Append    | block-native export        |        N/A |   21.72 ms |  18.35 ms |
| Append    | block-native import        |        N/A |  302.87 ms |    137 ms |
| Append    | block-native blocks        |        N/A |          1 |         1 |
| Append    | block-native payload bytes |        N/A |     67,005 |    31,452 |
| Append    | current-root sidecar bytes |        N/A |        N/A |    13,742 |
| Append    | database bytes             | 18,657,280 | 18,722,816 |   503,808 |
| Append    | live database bytes        |        N/A |  6,053,888 |   339,968 |
| Append    | freelist bytes             |        N/A | 12,668,928 |   163,840 |
| Append    | total file bytes           | 22,388,624 | 22,405,008 | 4,640,368 |
| Append    | database / final payload   |   1397.55x |   1402.46x |    37.74x |
| Append    | total file / final payload |   1677.05x |   1678.28x |   347.59x |
| Append    | sidecar nodes              |        N/A |        N/A |      4456 |
| Append    | sidecar leaves             |        N/A |        N/A |      2229 |
| Append    | sidecar concat nodes       |        N/A |        N/A |      2227 |
| Append    | sidecar segment bytes      |        N/A |        N/A |    26,700 |
| Automerge | completed updates          |       2900 |       2900 |      2900 |
| Automerge | total loop                 |    7999 ms |    8176 ms |  18423 ms |
| Automerge | write only                 |     969 ms |     764 ms |   6840 ms |
| Automerge | avg loop/update            |    2.76 ms |    2.82 ms |   6.35 ms |
| Automerge | avg write/update           |    0.33 ms |    0.26 ms |   2.36 ms |
| Automerge | sampled receive            |    7025 ms |    7303 ms |  11580 ms |
| Automerge | live receive avg           |    1171 ms |    1217 ms |   1930 ms |
| Automerge | live receive p50           |     938 ms |     980 ms |   1948 ms |
| Automerge | live receive p95           |    3532 ms |    3677 ms |   5017 ms |
| Automerge | cold load                  |    2031 ms |    2100 ms |   7055 ms |
| Automerge | current read               |    0.13 ms |    0.14 ms |   0.19 ms |
| Automerge | historical read avg        |        N/A |   61.47 ms |  72.89 ms |
| Automerge | tx info avg                |        N/A |    0.36 ms |   0.33 ms |
| Automerge | history rows               |       2901 |        580 |       581 |
| Automerge | final payload bytes        |       1750 |       1750 |      1750 |
| Automerge | source trace gzip bytes    |    904,360 |    904,360 |   904,360 |
| Automerge | bundle bytes               |  4,152,081 |  4,152,081 | 1,090,481 |
| Automerge | block-native export        |        N/A |   25.94 ms |  23.71 ms |
| Automerge | block-native import        |        N/A |  272.37 ms |    188 ms |
| Automerge | block-native blocks        |        N/A |          1 |         1 |
| Automerge | block-native payload bytes |        N/A |     87,526 |    41,677 |
| Automerge | current-root sidecar bytes |        N/A |        N/A |     1,806 |
| Automerge | database bytes             |  9,687,040 |  9,785,344 |   802,816 |
| Automerge | live database bytes        |        N/A |  2,965,504 |   585,728 |
| Automerge | freelist bytes             |        N/A |  6,819,840 |   217,088 |
| Automerge | total file bytes           | 13,778,664 | 13,807,408 | 4,931,184 |
| Automerge | database / final payload   |   5535.45x |   5591.63x |   458.75x |
| Automerge | total file / final payload |   7873.52x |   7889.95x |  2817.82x |
| Automerge | database / source gzip     |     10.71x |     10.83x |     0.89x |
| Automerge | bundle / source gzip       |      4.59x |      4.59x |     1.21x |
| Automerge | sidecar nodes              |        N/A |        N/A |    17,043 |
| Automerge | sidecar leaves             |        N/A |        N/A |      2326 |
| Automerge | sidecar concat nodes       |        N/A |        N/A |    14,717 |
| Automerge | sidecar segment bytes      |        N/A |        N/A |     4,075 |
| Canvas    | completed updates          |       3900 |       3900 |       N/A |
| Canvas    | total loop                 |    8455 ms |    8513 ms |       N/A |
| Canvas    | write only                 |     903 ms |     817 ms |       N/A |
| Canvas    | avg loop/update            |    2.17 ms |    2.18 ms |       N/A |
| Canvas    | avg write/update           |    0.23 ms |    0.21 ms |       N/A |
| Canvas    | sampled receive            |    7548 ms |    7691 ms |       N/A |
| Canvas    | live receive avg           |    1258 ms |    1282 ms |       N/A |
| Canvas    | live receive p50           |    1263 ms |    1275 ms |       N/A |
| Canvas    | live receive p95           |    3274 ms |    3345 ms |       N/A |
| Canvas    | cold load                  |    2236 ms |    2269 ms |       N/A |
| Canvas    | current read               |    0.13 ms |    0.13 ms |       N/A |
| Canvas    | historical read avg        |        N/A |   98.89 ms |       N/A |
| Canvas    | tx info avg                |        N/A |    0.44 ms |       N/A |
| Canvas    | history rows               |       3901 |        780 |       N/A |
| Canvas    | final payload bytes        |         46 |         46 |       N/A |
| Canvas    | position trace gzip bytes  |     78,526 |     78,526 |       N/A |
| Canvas    | position trace JSON bytes  |    205,609 |    205,609 |       N/A |
| Canvas    | bundle bytes               |    858,561 |    858,108 |       N/A |
| Canvas    | block-native export        |        N/A |   32.56 ms |       N/A |
| Canvas    | block-native import        |        N/A |  295.79 ms |       N/A |
| Canvas    | block-native blocks        |        N/A |          1 |       N/A |
| Canvas    | block-native payload bytes |        N/A |    173,244 |       N/A |
| Canvas    | database bytes             |    659,456 |    847,872 |       N/A |
| Canvas    | live database bytes        |        N/A |    401,408 |       N/A |
| Canvas    | freelist bytes             |        N/A |    446,464 |       N/A |
| Canvas    | total file bytes           |  4,828,760 |  4,849,264 |       N/A |
| Canvas    | database / final payload   |        N/A |        N/A |       N/A |
| Canvas    | total file / final payload |        N/A |        N/A |       N/A |
| Canvas    | database / position gzip   |      8.40x |     10.80x |       N/A |
| Canvas    | bundle / position gzip     |     10.93x |     10.93x |       N/A |

## Notes

- Current reads remain fast in the naive baseline because the current projection
  is doing its job.
- `Block+S` reduces compatibility bundle size and block-native current sync
  payloads by moving large text values into immutable sidecar segments. Its
  current write path is intentionally unbatched, so write cost is worse than
  Base3/Block for now.
- Current-root compaction in `Block+S` makes text current reads shallow again,
  while sealed historical roots still preserve older versions.
- The canonical Block experiment keeps one sample interval as the hot tail
  (`445` append, `580` Automerge, `780` canvas) and seals the older accepted
  history into one block per scenario. `live database bytes` shows the real page
  footprint after compaction; `total file bytes` still includes freed pages
  unless the explicit reclaim step is run.
- Block-native payload bytes are the compressed sealed block bytes that a
  block-aware peer could request/store without expanding archived history back
  into ordinary row history. `block-native export` now measures creating an open
  history bundle plus missing block payloads; `block-native import` measures
  importing those blocks and applying the open bundle. The ordinary `bundle bytes`
  rows still show the compatibility
  path that decodes blocks back into logical Jazz history.
- Historical local point reads currently decode and scan a whole selected block.
  The first measured Block numbers are intentionally rough but show this path is
  a real optimization target.
- The current Block payload uses v9 columnar JSON compressed with lz4. It is
  not the final binary/delta-varint block format, but it already avoids repeated
  per-record JSON object keys and stores user values as per-column arrays inside
  sealed history blocks. v9 dictionary-codes repeated string metadata and
  repeated user values, run/delta-codes integer metadata, run-codes nullable
  integer/JSON/vector metadata, and keeps the v4 packing for text values shaped
  as JSON `{x, y}` objects into numeric `x[]` / `y[]` streams.

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

## Block+S Probe

Canonical append and Automerge run combining immutable sequence sidecar roots with history blocks. Jazz row history stores only rope root refs, then cold root
history is sealed into lz4 blocks. Sidecar segments are immutable; after sealing
cold Jazz history, the benchmark rewrites the current text root into a compact
immutable root so recent/current reads are shallow. Cold load still imports a
full sidecar snapshot for compatibility-bundle import. Block-native import
applies the history delta first, then imports only the sidecar nodes/segments
reachable from the received current root.

Run:

```bash
MINI_JAZZ_PERF_ONLY_DEEP_HISTORY=all-block-incr \
MINI_JAZZ_DEEP_HISTORY_APPEND_TOKENS=2225 \
MINI_JAZZ_DEEP_HISTORY_APPEND_SAMPLE_EVERY=445 \
MINI_JAZZ_DEEP_HISTORY_AUTOMERGE_UPDATES=2900 \
MINI_JAZZ_DEEP_HISTORY_AUTOMERGE_SAMPLE_EVERY=580 \
target/debug/examples/perf_scenarios
```

Output: `/tmp/deep_history_block_incr_immutable_compacted.json`.

| Scenario  | total loop | write only | sampled receive | cold load | current read | historical read avg | tx info avg | block import | block payload bytes | current-root sidecar bytes | database bytes | live database bytes | bundle bytes |
| --------- | ---------: | ---------: | --------------: | --------: | -----------: | ------------------: | ----------: | -----------: | ------------------: | -------------------------: | -------------: | ------------------: | -----------: |
| Append    |    8331 ms |    2934 ms |         5395 ms |   2589 ms |      0.32 ms |            49.98 ms |     0.26 ms |       137 ms |              31,452 |                     13,742 |        503,808 |             339,968 |      567,966 |
| Automerge |   18423 ms |    6840 ms |        11580 ms |   7055 ms |      0.19 ms |            72.89 ms |     0.33 ms |       188 ms |              41,677 |                      1,806 |        802,816 |             585,728 |    1,090,481 |

Interpretation: `Block+S` is the first combined text result where sealed Jazz
history and sidecar value sharing are both active. Immutable segments make
lifecycle reasoning simpler and current-root compaction turns the latest value
into a shallow root.
That changes the time profile substantially: current reads are sub-millisecond,
and block-native import falls to `137 ms` for append and `188 ms` for Automerge.
The short-term cost is extra sidecar storage until we add segment-object GC:
append database/final payload rises from `35.90x` to `37.74x`, and Automerge
database/source gzip rises from `0.86x` to `0.89x`.

This is not the final sidecar protocol yet: historical roots sealed inside
blocks still need explicit sidecar root manifests/deltas if a receiver should be
able to answer historical reads from block-native sync without asking for a
compatibility sidecar snapshot.

## Block Size Probe

Quick non-canonical append Block sweep after adding
`MINI_JAZZ_DEEP_HISTORY_MAX_ROWS_PER_BLOCK`. Inputs were the canonical append
Block workload (`2225` updates, sample every `445`) with v9 columnar lz4 blocks.

| max rows/block | blocks | historical read avg | block-native import | block payload bytes | database bytes |
| -------------: | -----: | ------------------: | ------------------: | ------------------: | -------------: |
|          unset |      1 |            79.22 ms |           182.35 ms |              70,162 |     18,726,912 |
|           1000 |      2 |            77.89 ms |           188.70 ms |              56,837 |     18,677,760 |
|            500 |      4 |            57.41 ms |           190.59 ms |              58,388 |     18,673,664 |
|            250 |      8 |            47.75 ms |           189.39 ms |              62,046 |     18,673,664 |
|            100 |     18 |            43.72 ms |           188.90 ms |              71,037 |     18,673,664 |

Interpretation: one huge per-row block is not automatically best. Smaller
blocks reduce point-read decode units and the compressed payload stayed close
enough that this should remain a tunable compaction policy. The cap-100 row was
remeasured after limiting node-local point reads to one candidate sealed block.
