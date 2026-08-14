# Ordinary-row text production receipt

This receipt exercises the public `TextStore` implementation, not a synthetic
row-count model. Every edit waits for local RocksDB durability. The bounded
frontier is configured for 32 patches / 4 KiB encoded patches / 4 KiB UTF-8
leaves. End and deterministic middle-heavy one-character inserts are measured
separately.

The structure uses ordinary document, explicit immutable version, and immutable
rope-node rows. Consolidation path-copies localized edits and shares untouched
subtrees. It instead emits a complete balanced root when scattered path copies
would create more rows than rebuilding the full leaf tree.

Environment: one Node 24 process using the freshly built release NAPI/RocksDB
artifact. These are single-machine receipts and not stable release thresholds.

## 100 KiB initial text, 300 edits

| Workload | Representation   | durable p50 / p95 | row mutations/edit | logical bytes | RocksDB dir | consolidations p50 / p95 | exact-version materialization p50 |
| -------- | ---------------- | ----------------: | -----------------: | ------------: | ----------: | -----------------------: | --------------------------------: |
| end      | whole string     |  15.50 / 27.21 ms |               1.00 |      30.87 MB |    62.43 MB |                       -- |                       not exposed |
| end      | bounded frontier |   6.82 / 13.24 ms |               2.21 |       ≥248 KB |     2.67 MB |         26.29 / 28.51 ms |                          85.37 ms |
| middle   | whole string     |  16.43 / 26.26 ms |               1.00 |      30.87 MB |    62.41 MB |                       -- |                       not exposed |
| middle   | bounded frontier |   6.52 / 11.40 ms |               3.53 |      ≥1.17 MB |     4.89 MB |         66.36 / 74.83 ms |                         237.01 ms |

`logical bytes` for the frontier is deliberately marked as a lower bound: it
counts encoded patch frontiers, ids, leaf text, and child references, but not
the engine's ordinary transaction framing. Row mutations include the immutable
version insert and document-head update plus amortized new rope nodes.

## 4 KiB initial text, 1,000 edits

| Workload | Representation   | durable p50 / p95 | row mutations/edit | logical bytes | RocksDB dir | consolidations p50 / p95 | exact-version materialization p50 |
| -------- | ---------------- | ----------------: | -----------------: | ------------: | ----------: | -----------------------: | --------------------------------: |
| end      | whole string     |    3.32 / 6.34 ms |               1.00 |       4.60 MB |    11.06 MB |                       -- |                       not exposed |
| end      | bounded frontier |   9.39 / 19.58 ms |               2.06 |       ≥441 KB |    16.62 MB |         17.94 / 36.24 ms |                          13.70 ms |
| middle   | whole string     |    3.21 / 6.57 ms |               1.00 |       4.60 MB |    11.04 MB |                       -- |                       not exposed |
| middle   | bounded frontier |  10.02 / 17.43 ms |               2.09 |       ≥556 KB |    16.86 MB |         55.45 / 85.76 ms |                          12.70 ms |

## Interpretation

- At 100 KiB, avoiding the full-string cell rewrite wins clearly on ordinary
  durable latency, logical payload, and disk footprint even while retaining an
  explicit independently readable version row for every keystroke.
- At 4 KiB, whole-string rewrite wins: the frontier pays two ordinary row
  mutations per edit and explicit-version metadata dominates. A production API
  should keep the representation choice visible or use a size crossover rather
  than claiming the rope is universally cheaper.
- Localized end edits share well (2.21 rows/edit at 100 KiB). Scattered edits
  choose bounded full-tree rebuilding and cost 3.53 rows/edit, still avoiding a
  100 KiB string rewrite on every keystroke.
- Exact historical reads perform no Jazz-history replay, but the current public
  implementation issues one ordinary point query per reachable rope node. The
  100 KiB result makes batched/reachable child hydration the clearest next
  general-purpose Jazz optimization.
- Consolidation spikes, especially scattered edits, remain visible. The
  frontier bounds their frequency rather than hiding their cost.

## Reproduce

```sh
pnpm build:test-artifacts
JAZZ_TEXT_BENCH=1 JAZZ_TEXT_BENCH_INITIAL_BYTES=102400 \
JAZZ_TEXT_BENCH_EDITS=300 pnpm --dir packages/jazz-tools exec vitest run \
src/text/text.bench.test.ts --reporter=verbose

JAZZ_TEXT_BENCH=1 JAZZ_TEXT_BENCH_INITIAL_BYTES=4096 \
JAZZ_TEXT_BENCH_EDITS=1000 pnpm --dir packages/jazz-tools exec vitest run \
src/text/text.bench.test.ts --reporter=verbose
```
