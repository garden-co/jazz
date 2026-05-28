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

|    Short | Meaning                                                                         |
| -------: | ------------------------------------------------------------------------------- |
|    Base3 | Base2 plus compact bundle wire dictionaries and positional record arrays        |
|    Block | Base3 plus sealed lz4 history blocks after the write loop                       |
|  Block+S | Block plus immutable sequence sidecar roots and incremental sidecar sync        |
| Block+S2 | Block+S plus SQLite transaction batching and current-derived pending visibility |

`Block+S` is the immutable sidecar implementation for large text
columns: text sidecar segments are immutable, old Jazz root history is sealed
into lz4 blocks, and the current text root is rebuilt into large immutable
leaves after compaction so recent/current reads stay shallow. Canvas stays inline
in this column; its Block+S numbers are a no-sidecar control run through the same
current code. `Block+S2` is the current write-path iteration: logical Jazz
history remains one row per update, but root writes/sidecar writes are grouped
into short SQLite transactions, sidecar receive uses one SQLite transaction, and
remote-pending visibility is derived from the current visible tx before falling
back to a durable-history lookup.

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
intentionally `N/A`; compare to the gzipped position trace instead. Storage rows
use measured `live_database_bytes`; because these canonical runs completed their
target update counts, no extrapolation was needed. Aggregate write-loop and native sync timings are normalized by completed update
count; point reads and `transaction_info` stay as absolute per-call latencies.

## Comparison Tables

### Append

| Metric                        |      Base3 |     Block |  Block+S | Block+S2 |
| ----------------------------- | ---------: | --------: | -------: | -------: |
| completed updates             |       2225 |      2225 |     2225 |     2225 |
| total loop / update           |    3.45 ms |   3.57 ms |  3.66 ms |  0.89 ms |
| write only / update           |    0.31 ms |   0.36 ms |  1.27 ms |  0.21 ms |
| sampled receive / update      |    3.14 ms |   3.20 ms |  2.38 ms |  0.68 ms |
| current read                  |    0.14 ms |   0.15 ms |  0.30 ms |  0.32 ms |
| historical read avg           |  693.96 ms |  41.18 ms | 50.07 ms | 48.97 ms |
| tx info avg                   |    1.36 ms |   0.28 ms |  0.25 ms |  0.25 ms |
| native export / update        |    0.05 ms |  0.010 ms | 0.008 ms | 0.008 ms |
| native import / update        |    0.90 ms |   0.14 ms |  0.06 ms |  0.05 ms |
| native sync bytes             | 15,235,071 | 5,486,681 |  118,681 |  114,583 |
| live database / final payload |   1397.55x |   453.47x |   25.47x |   25.16x |

### Automerge

| Metric                      |      Base3 |     Block |  Block+S | Block+S2 |
| --------------------------- | ---------: | --------: | -------: | -------: |
| completed updates           |       2900 |      2900 |     2900 |     2900 |
| total loop / update         |    2.80 ms |   2.77 ms |  6.72 ms |  1.07 ms |
| write only / update         |    0.29 ms |   0.26 ms |  2.98 ms |  0.39 ms |
| sampled receive / update    |    2.46 ms |   2.47 ms |  3.74 ms |  0.67 ms |
| current read                |    0.14 ms |   0.13 ms |  0.18 ms |  0.17 ms |
| historical read avg         | 1148.49 ms |  60.26 ms | 75.65 ms | 72.74 ms |
| tx info avg                 |    1.84 ms |   0.32 ms |  0.33 ms |  0.30 ms |
| native export / update      |    0.05 ms |  0.009 ms | 0.008 ms | 0.008 ms |
| native import / update      |    0.71 ms |   0.09 ms |  0.07 ms |  0.05 ms |
| native sync bytes           |  4,152,081 | 1,229,154 |  139,884 |  138,177 |
| live database / source gzip |     10.73x |     3.28x |    0.65x |    0.64x |

### Canvas

| Metric                        |      Base3 |    Block |   Block+S | Block+S2 |
| ----------------------------- | ---------: | -------: | --------: | -------: |
| completed updates             |       3900 |     3900 |      3900 |     3900 |
| total loop / update           |    2.18 ms |  2.16 ms |   2.18 ms |  0.78 ms |
| write only / update           |    0.21 ms |  0.23 ms |   0.23 ms |  0.22 ms |
| sampled receive / update      |    1.97 ms |  1.93 ms |   1.95 ms |  0.55 ms |
| current read                  |    0.16 ms |  0.13 ms |   0.14 ms |  0.13 ms |
| historical read avg           | 2080.19 ms | 98.32 ms | 100.07 ms | 97.88 ms |
| tx info avg                   |    2.35 ms |  0.39 ms |   0.43 ms |  0.39 ms |
| native export / update        |    0.04 ms | 0.008 ms |  0.008 ms | 0.008 ms |
| native import / update        |    0.58 ms |  0.08 ms |   0.08 ms |  0.06 ms |
| native sync bytes             |    858,561 |  337,476 |   337,675 |  337,694 |
| live database / position gzip |      8.61x |    5.11x |     5.11x |    5.11x |

## Notes

- Current reads remain fast in the naive baseline because the current projection
  is doing its job.
- `Block+S` reduces native sync payloads by moving large text values into
  immutable sidecar segments. `Block+S2` keeps that storage shape but batches
  SQLite write/receive work and avoids the per-history-row durable-history scan
  for the common remote-pending-over-current case. Canvas does not use the
  sidecar in either `Block+S` column; it is the inline history-block control.
- Current-root compaction in `Block+S` makes text current reads shallow again,
  while sealed historical roots still preserve older versions.
- The canonical Block experiment keeps one sample interval as the hot tail
  (`445` append, `580` Automerge, `780` canvas) and seals the older accepted
  history into one block per scenario. `live database bytes` shows the real page
  footprint after compaction; `total file bytes` still includes freed pages
  unless the explicit reclaim step is run.
- `native sync bytes` is each setup's intended sync payload shape: Base3 compact
  bundle bytes; Block open-hot-tail bundle plus compressed history block bytes;
  Block+S/Block+S2 open-hot-tail root bundle plus compressed history block bytes
  plus the current-root sidecar delta. Native export/import timings use the same
  path.
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
