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
