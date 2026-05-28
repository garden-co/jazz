# Deep History Benchmarks

Status: baseline tracker for naive full-row history.

These probes measure rows with deep edit histories before adding specialized
column storage or sync handling. The goal is to keep each canonical run around
10 seconds today, then drive all three below 1 second as optimizations land.

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

Timing fields:

- `write_only_ms`: edit generation plus durable write/version insert work only.
- `sampled_receive_total_ms`: sum of sampled live receive/listener checks.
- `total_loop_ms`: write loop wall time, including sampled receive checks.
- `elapsed_write_ms`/`average_write_ms`: legacy names for `total_loop_ms`
  and per-update total loop time.

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

## Ratio Rules

For append and document edits, final-payload ratios compare storage to the text
content produced by the run. If a future canonical run stops early, use
`*_to_extrapolated_final_payload_ratio`, which scales the produced content by
`target_updates / completed_updates`.

For canvas positions, final-payload ratios are intentionally `null`. Comparing a
deep coordinate history to the final `{x,y}` payload is not meaningful. Use the
gzipped JSON position trace ratios instead.

## Baseline: Naive Full-Row History

Date: 2026-05-27

Branch: `codex/sqlite-core-deep-history-efficiency`

Build: debug example binary

### Append Stream

Input: `2225` token-like appends

Wall time under hard guard: `9.86s`

| Metric                     |      Value |
| -------------------------- | ---------: |
| completed updates          |       2225 |
| elapsed write loop         | 7442.96 ms |
| average loop/update        |    3.35 ms |
| live receive average       | 1135.01 ms |
| live receive p50           |  923.73 ms |
| live receive p95           | 3470.40 ms |
| cold load                  | 1913.16 ms |
| current read               |    0.13 ms |
| history rows               |       2226 |
| final payload bytes        |     13,350 |
| bundle bytes               | 16,115,414 |
| database bytes             | 18,796,544 |
| total SQLite file bytes    | 22,319,040 |
| database / final payload   |   1407.98x |
| total file / final payload |   1671.84x |

### Automerge Paper Trace

Input: first `2900` edits from `automerge-paper.json.gz`

Wall time under hard guard: `9.73s`

| Metric                       |      Value |
| ---------------------------- | ---------: |
| completed updates            |       2900 |
| elapsed write loop           | 7118.23 ms |
| average loop/update          |    2.45 ms |
| live receive average         | 1061.55 ms |
| live receive p50             |  803.58 ms |
| live receive p95             | 3321.75 ms |
| cold load                    | 1945.50 ms |
| current read                 |    0.13 ms |
| history rows                 |       2901 |
| final payload bytes          |      1,750 |
| source trace gzip bytes      |    904,360 |
| bundle bytes                 |  5,351,258 |
| database bytes               |  9,859,072 |
| total SQLite file bytes      | 13,729,560 |
| database / final payload     |   5633.76x |
| total file / final payload   |   7845.46x |
| database / source trace gzip |     10.90x |
| bundle / source trace gzip   |      5.92x |

### Canvas Positions

Input: `3900` 2D positions at 60 FPS

Wall time under hard guard: `9.84s`

| Metric                         |      Value |
| ------------------------------ | ---------: |
| completed updates              |       3900 |
| elapsed write loop             | 7602.00 ms |
| average loop/update            |    1.95 ms |
| live receive average           | 1140.96 ms |
| live receive p50               | 1122.62 ms |
| live receive p95               | 3065.49 ms |
| cold load                      | 2047.94 ms |
| current read                   |    0.12 ms |
| history rows                   |       3901 |
| final payload bytes            |         46 |
| position trace gzip bytes      |     78,526 |
| position trace JSON bytes      |    205,609 |
| bundle bytes                   |  2,455,136 |
| database bytes                 |    884,736 |
| total SQLite file bytes        |  5,070,520 |
| database / final payload       |        N/A |
| total file / final payload     |        N/A |
| database / position trace gzip |     11.27x |
| bundle / position trace gzip   |     31.27x |

## Notes

- Current reads remain fast in all three probes because the current projection is
  doing its job.
- The live receive and cold-load paths are already multi-second at these small
  canonical sizes.
- These numbers are debug-build baselines and should be treated as directional,
  not launch targets.

## Experiment: Jazz + Persisted Sequence Sidecar

Date: 2026-05-27

Branch: `codex/sqlite-core-deep-history-efficiency`

Shape:

- Jazz row history stores sequence root ids in ordinary text fields.
- Text and position payloads live in the persisted sequence sidecar.
- Sync in this experiment sends a normal Jazz bundle plus a full sidecar
  snapshot. This is intentionally not incremental yet.

### Append Stream, Jazz + Text Sequence

Input: `2225` token-like appends

| Metric                     |      Value |
| -------------------------- | ---------: |
| total loop                 | 7986.40 ms |
| write only                 | 2050.71 ms |
| average write only         |    0.92 ms |
| sampled receive total      | 5933.25 ms |
| live receive average       |  988.87 ms |
| live receive p95           | 2191.56 ms |
| cold load                  | 1853.95 ms |
| current read               |   52.78 ms |
| history rows               |       2225 |
| final payload bytes        |     13,350 |
| bundle bytes               |  1,411,141 |
| database bytes             |    573,440 |
| total SQLite file bytes    |  4,738,720 |
| database / final payload   |     42.95x |
| total file / final payload |    354.96x |

Sidecar notes: `4449` sequence nodes, `2225` leaves, `2224` concat nodes,
`13,350` segment bytes. Bundle bytes are `1,262,422` Jazz plus `148,719`
sidecar snapshot bytes.

### Automerge Paper, Jazz + Text Sequence

Input: first `2900` edits from `automerge-paper.json.gz`

| Metric                       |        Value |
| ---------------------------- | -----------: |
| total loop                   | 29,510.70 ms |
| write only                   |   6562.95 ms |
| average write only           |      2.26 ms |
| sampled receive total        | 22,944.71 ms |
| live receive average         |   3824.12 ms |
| live receive p95             |   9865.29 ms |
| cold load                    | 10,879.55 ms |
| current read                 |     40.48 ms |
| history rows                 |         2900 |
| final payload bytes          |        1,750 |
| source trace gzip bytes      |      904,360 |
| bundle bytes                 |    2,209,941 |
| database bytes               |      892,928 |
| total SQLite file bytes      |    5,082,808 |
| database / final payload     |      510.24x |
| total file / final payload   |     2904.46x |
| database / source trace gzip |        0.99x |
| bundle / source trace gzip   |        2.44x |

Sidecar notes: `17,042` sequence nodes, `2325` leaves, `14,717` concat nodes,
`2325` segment bytes. Bundle bytes are `1,647,929` Jazz plus `562,012`
sidecar snapshot bytes.

### Canvas Positions, Jazz + Delta Sequence

Input: `3900` frames

| Metric                         |        Value |
| ------------------------------ | -----------: |
| total loop                     | 16,495.76 ms |
| write only                     |   3681.13 ms |
| average write only             |      0.94 ms |
| sampled receive total          | 12,810.41 ms |
| live receive average           |   2135.07 ms |
| live receive p95               |   5595.58 ms |
| cold load                      |   8541.08 ms |
| current read                   |      0.21 ms |
| history rows                   |         3900 |
| position trace gzip bytes      |       77,211 |
| position trace JSON bytes      |      182,209 |
| bundle bytes                   |    2,591,442 |
| database bytes                 |      913,408 |
| total SQLite file bytes        |    5,090,976 |
| database / final payload       |          N/A |
| total file / final payload     |          N/A |
| database / position trace gzip |       11.83x |
| bundle / position trace gzip   |       33.56x |

Sidecar notes: `7799` sequence nodes, `3900` position leaves, `3899` concat
nodes, `31,392` position segment bytes. Bundle bytes are `2,295,186` Jazz plus
`296,256` sidecar snapshot bytes.

### Immediate Takeaway

The persisted sequence sidecar cuts payload storage dramatically and keeps hot
current reads tiny for presence-like positions. The remaining obvious problem is
sync/cold-load: every sampled receive and cold load currently copies the full
sidecar snapshot and exports full Jazz row history. The next experiment should
make sidecar sync incremental across text and position codecs.

## Experiment: Incremental Sidecar Sync

Date: 2026-05-27

Build: `cargo build --release -p mini-jazz-sqlite --example perf_scenarios`

Shape:

- Sampled live receive copies only new sidecar nodes and new or still-active
  segments, tracked by sidecar id watermarks.
- Cold load and final bundle-size accounting still use full sidecar snapshots.
- Jazz row-history export is still full table history in these probes.

### Append Stream, Incremental Sidecar

Input: `2225` token-like appends

| Metric                | Full sidecar snapshot | Incremental sidecar |
| --------------------- | --------------------: | ------------------: |
| total loop            |            7986.40 ms |          6438.34 ms |
| write only            |            2050.71 ms |          2516.18 ms |
| sampled receive total |            5933.25 ms |          3919.78 ms |
| live receive average  |             988.87 ms |           653.30 ms |
| live receive p95      |            2191.56 ms |          1377.11 ms |
| cold load             |            1853.95 ms |          1800.99 ms |
| current read          |              52.78 ms |            50.82 ms |

### Automerge Paper, Incremental Sidecar

Input: first `2900` edits from `automerge-paper.json.gz`

| Metric                | Full sidecar snapshot | Incremental sidecar |
| --------------------- | --------------------: | ------------------: |
| total loop            |          29,510.70 ms |        23,861.41 ms |
| write only            |            6562.95 ms |        13,016.09 ms |
| sampled receive total |          22,944.71 ms |        10,842.13 ms |
| live receive average  |            3824.12 ms |          1807.02 ms |
| live receive p95      |            9865.29 ms |          3436.79 ms |
| cold load             |          10,879.55 ms |        12,275.93 ms |
| current read          |              40.48 ms |            40.24 ms |

### Canvas Positions, Incremental Sidecar

Input: `3900` frames

| Metric                | Full sidecar snapshot | Incremental sidecar |
| --------------------- | --------------------: | ------------------: |
| total loop            |          16,495.76 ms |        17,353.74 ms |
| write only            |            3681.13 ms |          6686.90 ms |
| sampled receive total |          12,810.41 ms |        10,662.75 ms |
| live receive average  |            2135.07 ms |          1777.12 ms |
| live receive p95      |            5595.58 ms |          3341.81 ms |
| cold load             |            8541.08 ms |          3745.26 ms |
| current read          |               0.21 ms |             0.17 ms |

### Takeaway

Incremental sidecar copying helps sampled live receive across all three shapes,
but the remaining cost is still large because every sampled receive also exports
and applies full Jazz row history. The next likely target is incremental Jazz
history export for root-ref rows, or a sequence-aware subscription path that can
avoid re-sending every historical root ref.

## Experiment: Offline Page Compression Upper Bound

Date: 2026-05-27

Shape:

- Checkpoint and truncate WAL before measuring.
- Read the main SQLite file page-by-page using the current SQLite page size.
- Compress each page independently with lz4.
- Sum compressed page bytes and measure page encode/decode CPU time.
- This is not a VFS yet; it is an upper-bound probe for whether page-level
  compression is worth implementing.

### Naive Full-Row History

| Scenario        | Pages |   DB bytes | LZ4 bytes | Ratio | Compress | Compress/page | Decompress | Decompress/page |
| --------------- | ----: | ---------: | --------: | ----: | -------: | ------------: | ---------: | --------------: |
| Append stream   |  4589 | 18,796,544 |   390,623 | 0.02x |   2.6 ms |       0.57 us |     8.2 ms |         1.80 us |
| Automerge paper |  2407 |  9,859,072 | 2,214,659 | 0.22x |   4.2 ms |       1.73 us |     7.1 ms |         2.94 us |
| Canvas          |   216 |    884,736 |   422,311 | 0.48x |   1.4 ms |       6.39 us |     0.4 ms |         1.69 us |

### Jazz + Persisted Sequence Sidecar

These numbers sum independent page-compression probes for the Jazz SQLite file
and the sidecar SQLite file.

| Scenario        | Pages | DB bytes | LZ4 bytes | Ratio | Compress | Compress/page | Decompress | Decompress/page |
| --------------- | ----: | -------: | --------: | ----: | -------: | ------------: | ---------: | --------------: |
| Append stream   |   140 |  573,440 |   224,296 | 0.39x |   0.5 ms |       3.38 us |     0.2 ms |         1.30 us |
| Automerge paper |   218 |  892,928 |   442,180 | 0.50x |   0.9 ms |       4.03 us |     0.3 ms |         1.19 us |
| Canvas          |   223 |  913,408 |   428,859 | 0.47x |   0.8 ms |       3.54 us |     0.3 ms |         1.15 us |

### Takeaway

Page compression is extremely promising for naive append history because the
full-row SQLite pages contain huge repeated prefixes. It is less dramatic after
the persisted sequence sidecar removes content redundancy, but still meaningful
for metadata-heavy pages.

In release mode, lz4 page compression is cheap enough to be plausible in the
hot path. The storage win is largest for naive append history, where repeated
full-row prefixes dominate the SQLite pages.
