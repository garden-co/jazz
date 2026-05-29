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
runs the Block probes, and `all-block-ops` runs the current Block+Ops text
probes plus the canvas Block probe. Use these for smoke checks or when grouped
scenarios should run together.
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

|      Short | Meaning                                                                                      |
| ---------: | -------------------------------------------------------------------------------------------- |
|      Base3 | Base2 plus compact bundle wire dictionaries and positional record arrays                     |
|      Block | Base3 plus sealed lz4 history blocks after the write loop                                    |
|  Block+Ops | Block plus text op-log sidecar roots and binary lz4 sidecar delta sync                       |
| Block+Ops2 | Block+Ops plus row-level current repair and candidate current fast path                      |
| Block+Ops3 | Block+Ops2 plus 10ms SQLite write batching for row writes and write-path metadata/row caches |
| Block+Ops4 | Block+Ops3 plus incremental live export/apply and direct local/received tx tuple writes      |
| Block+Ops5 | Block+Ops4 plus batched receive tx upserts, history inserts, and tuple updates               |

`Block+Ops` is the current text-sidecar experiment for large text columns:
Jazz row history stores text op ids, text changes append to an op log, and
occasional content-addressed chunk snapshots bound replay cost. Native sync
sends sealed Jazz history blocks plus a binary lz4 sidecar delta. Canvas stays
inline in this column; its Block+Ops numbers use the plain Block path.
`Block+Ops2` keeps the same storage and sync format, but applies imported
history in two phases: store all history first, then repair current projection
once per touched row, using an in-memory best-candidate fast path when the
bundle itself contains the winning version.
`Block+Ops3` keeps the same storage/sync shape and optimizes the local write
path: generic row writes use the same 10ms SQLite transaction batching policy as
text op roots, and batched writes reuse user/table metadata, row numbers,
creation metadata, and current visible-tx observations.
`Block+Ops4` keeps the same storage/sync shape and treats the experimental
single-writer incremental live path as the measured realtime path: live export
uses a node/local-epoch watermark, receive caches exact already-applied txs,
received read/write tuple columns are written together, and batched local tx
rows are inserted with tuple JSONB already populated.
`Block+Ops5` keeps the same format and further reduces receive-side SQLite
statement count by batching tx upserts, open-history inserts, and tx tuple
updates inside `apply_bundle`.

## Timing Fields

- `write only`: edit generation plus durable write/version insert work only
- `sampled receive`: sum of sampled live receive/listener checks
- `total loop`: write loop wall time, including sampled receive checks
- `avg loop/update`: `total loop / completed updates`
- `avg write/update`: `write only / completed updates`
- `current read`: current projection lookup and any sidecar materialization
- `historical read`: average local point-in-time row lookup over sampled early,
  middle, and latest epochs; for Block and Block+Ops this crosses sealed blocks
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

| Metric                        |      Base3 |     Block | Block+Ops | Block+Ops2 | Block+Ops3 | Block+Ops4 | Block+Ops5 |
| ----------------------------- | ---------: | --------: | --------: | ---------: | ---------: | ---------: | ---------: |
| completed updates             |       2225 |      2225 |      2225 |       2225 |       2225 |       2225 |       2225 |
| total loop / update           |    3.45 ms |   3.57 ms |   0.51 ms |    0.40 ms |    0.35 ms |    0.15 ms |    0.15 ms |
| write only / update           |    0.31 ms |   0.36 ms |   0.16 ms |    0.16 ms |    0.06 ms |    0.05 ms |    0.05 ms |
| sampled receive / update      |    3.14 ms |   3.20 ms |   0.35 ms |    0.24 ms |    0.29 ms |    0.10 ms |    0.09 ms |
| current read                  |    0.14 ms |   0.15 ms |   0.22 ms |    0.21 ms |    0.28 ms |    0.29 ms |    0.25 ms |
| historical read avg           |  693.96 ms |  41.18 ms |  37.03 ms |   36.42 ms |    1.11 ms |    1.15 ms |    1.12 ms |
| tx info avg                   |    1.36 ms |   0.28 ms |   0.25 ms |    0.26 ms |    0.24 ms |    0.25 ms |    0.24 ms |
| native export / update        |    0.05 ms |  0.010 ms |  0.005 ms |   0.005 ms |   0.007 ms |   0.007 ms |   0.007 ms |
| native import / update        |    0.90 ms |   0.14 ms |   0.04 ms |    0.03 ms |    0.04 ms |    0.04 ms |    0.04 ms |
| native sync bytes             | 15,235,071 | 5,486,681 |   104,635 |    104,669 |     60,707 |     60,449 |     60,541 |
| live database / final payload |   1397.55x |   453.47x |    22.70x |     22.70x |     16.57x |     16.57x |     16.57x |

### Automerge

| Metric                      |      Base3 |     Block | Block+Ops | Block+Ops2 | Block+Ops3 | Block+Ops4 | Block+Ops5 |
| --------------------------- | ---------: | --------: | --------: | ---------: | ---------: | ---------: | ---------: |
| completed updates           |       2900 |      2900 |      2900 |       2900 |       2900 |       2900 |       2900 |
| total loop / update         |    2.80 ms |   2.77 ms |   0.54 ms |    0.42 ms |    0.37 ms |    0.18 ms |    0.18 ms |
| write only / update         |    0.29 ms |   0.26 ms |   0.19 ms |    0.18 ms |    0.09 ms |    0.08 ms |    0.09 ms |
| sampled receive / update    |    2.46 ms |   2.47 ms |   0.35 ms |    0.24 ms |    0.28 ms |    0.10 ms |    0.09 ms |
| current read                |    0.14 ms |   0.13 ms |   0.19 ms |    0.18 ms |    0.29 ms |    0.24 ms |    0.26 ms |
| historical read avg         | 1148.49 ms |  60.26 ms |  57.28 ms |   56.72 ms |    1.40 ms |    1.40 ms |    1.46 ms |
| tx info avg                 |    1.84 ms |   0.32 ms |   0.30 ms |    0.30 ms |    0.29 ms |    0.29 ms |    0.30 ms |
| native export / update      |    0.05 ms |  0.009 ms |  0.005 ms |   0.005 ms |   0.007 ms |   0.007 ms |   0.007 ms |
| native import / update      |    0.71 ms |   0.09 ms |   0.04 ms |    0.03 ms |    0.04 ms |    0.04 ms |    0.04 ms |
| native sync bytes           |  4,152,081 | 1,229,154 |   143,135 |    143,917 |     80,124 |     79,749 |     79,988 |
| live database / source gzip |     10.73x |     3.28x |     0.34x |      0.34x |      0.27x |      0.27x |      0.27x |

### Canvas

| Metric                        |      Base3 |    Block | Block+Ops | Block+Ops2 | Block+Ops3 | Block+Ops4 | Block+Ops5 |
| ----------------------------- | ---------: | -------: | --------: | ---------: | ---------: | ---------: | ---------: |
| completed updates             |       3900 |     3900 |      3900 |       3900 |       3900 |       3900 |       3900 |
| total loop / update           |    2.18 ms |  2.16 ms |   2.16 ms |    0.43 ms |    0.25 ms |    0.10 ms |    0.09 ms |
| write only / update           |    0.21 ms |  0.23 ms |   0.23 ms |    0.19 ms |    0.03 ms |    0.02 ms |    0.03 ms |
| sampled receive / update      |    1.97 ms |  1.93 ms |   1.93 ms |    0.24 ms |    0.22 ms |    0.07 ms |    0.07 ms |
| current read                  |    0.16 ms |  0.13 ms |   0.13 ms |    0.13 ms |    0.14 ms |    0.13 ms |    0.14 ms |
| historical read avg           | 2080.19 ms | 98.32 ms |  98.32 ms |   95.77 ms |    1.91 ms |    1.77 ms |    1.95 ms |
| tx info avg                   |    2.35 ms |  0.39 ms |   0.39 ms |    0.39 ms |    0.41 ms |    0.38 ms |    0.40 ms |
| native export / update        |    0.04 ms | 0.008 ms |  0.008 ms |   0.004 ms |   0.004 ms |   0.004 ms |   0.004 ms |
| native import / update        |    0.58 ms |  0.08 ms |   0.08 ms |    0.04 ms |    0.02 ms |    0.01 ms |    0.01 ms |
| native sync bytes             |    858,561 |  337,476 |   337,476 |    337,111 |    199,193 |    198,883 |    198,905 |
| live database / position gzip |      8.61x |    5.11x |     5.11x |      5.11x |      4.96x |      4.96x |      4.96x |

## Notes

- Current reads remain fast in the naive baseline because the current projection
  is doing its job.
- `Block+Ops` reduces repeated large text values by moving text history into an
  op-log sidecar with content-addressed chunk snapshots. The current binary
  sidecar delta is lz4-compressed and watermark-based.
- Canvas does not use the text op sidecar yet; its `Block+Ops` numbers are the
  inline history-block control.
- The canonical Block experiment keeps one sample interval as the hot tail
  (`445` append, `580` Automerge, `780` canvas) and seals the older accepted
  history into one block per scenario. `live database bytes` shows the real page
  footprint after compaction; `total file bytes` still includes freed pages
  unless the explicit reclaim step is run.
- `native sync bytes` is each setup's intended sync payload shape: Base3 compact
  bundle bytes; Block open-hot-tail bundle plus compressed history block bytes;
  Block+Ops open-hot-tail root bundle plus compressed history block bytes plus
  the binary text-op sidecar delta. Native export/import timings use the same
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
