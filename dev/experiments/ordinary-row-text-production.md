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
artifact. The production RocksDB profile uses Zstd for append/history column
families, LZ4 for current/index/meta, and Zstd for bottommost compression. These
are single-machine receipts and not stable release thresholds.

## 100 KiB initial text, 300 edits

| Workload | Representation   | durable p50 / p95 | row mutations/edit | logical bytes | closed apparent / allocated | reopened apparent / allocated | consolidations p50 / p95 | exact-version materialization p50 |
| -------- | ---------------- | ----------------: | -----------------: | ------------: | --------------------------: | ----------------------------: | -----------------------: | --------------------------------: |
| end      | whole string     |  15.46 / 25.94 ms |               1.00 |      30.87 MB |            62.43 / 62.46 MB |                0.62 / 0.66 MB |                       -- |                       not exposed |
| end      | bounded frontier |   6.45 / 13.18 ms |               2.21 |       ≥248 KB |             2.67 / 78.18 MB |                0.50 / 0.53 MB |         25.21 / 27.72 ms |                          83.12 ms |
| middle   | whole string     |  16.29 / 26.59 ms |               1.00 |      30.87 MB |            62.41 / 62.43 MB |                0.96 / 1.00 MB |                       -- |                       not exposed |
| middle   | bounded frontier |   6.62 / 12.05 ms |               3.53 |      ≥1.17 MB |             4.89 / 78.18 MB |                0.64 / 0.68 MB |         65.45 / 83.93 ms |                         230.76 ms |

`logical bytes` for the frontier is deliberately marked as a lower bound: it
counts encoded patch frontiers, ids, leaf text, and child references, but not
the engine's ordinary transaction framing. Row mutations include the immutable
version insert and document-head update plus amortized new rope nodes.

## 4 KiB initial text, 1,000 edits

| Workload | Representation   | durable p50 / p95 | row mutations/edit | logical bytes | closed apparent / allocated | reopened apparent / allocated | consolidations p50 / p95 | exact-version materialization p50 |
| -------- | ---------------- | ----------------: | -----------------: | ------------: | --------------------------: | ----------------------------: | -----------------------: | --------------------------------: |
| end      | whole string     |    2.51 / 4.35 ms |               1.00 |       4.60 MB |            11.06 / 11.08 MB |                0.57 / 0.61 MB |                       -- |                       not exposed |
| end      | bounded frontier |   8.21 / 15.05 ms |               2.06 |       ≥441 KB |            16.60 / 78.18 MB |                0.88 / 0.91 MB |         12.91 / 22.96 ms |                          10.14 ms |
| middle   | whole string     |    2.41 / 4.93 ms |               1.00 |       4.60 MB |            11.04 / 11.06 MB |                1.51 / 1.54 MB |                       -- |                       not exposed |
| middle   | bounded frontier |   8.44 / 15.56 ms |               2.09 |       ≥556 KB |            16.86 / 78.18 MB |                0.96 / 1.00 MB |         39.05 / 46.04 ms |                          10.92 ms |

## Storage measurement method

Each representation/workload uses a fresh isolated database and the same
deterministic edit offsets. `apparent` recursively sums file lengths;
`allocated` sums `stat.blocks * 512`, equivalent to the data portion of `du`.
The live and just-closed stores are WAL-dominated. RocksDB can preallocate WAL
blocks, which explains the 78.18 MB allocated figure even when apparent bytes
are much lower.

To prove recovery without relying on the original process releasing every DB
wrapper, the benchmark copies the exact closed directory, reopens that copy,
reads and verifies the final value, and shuts it down before measuring again.
Normal recovery turns almost all of the WAL into compressed SSTs: reopened WAL
is below 1 KB in every receipt, while SST apparent bytes range from 0.19 MB to
1.20 MB. The reopened columns above therefore show a useful compressed
post-recovery footprint, not merely the logical payload or a live WAL size.
They are not a claim about fully compacted bottommost-level size.

No public production API currently exposes RocksDB memtable flush or manual
compaction. `JazzContext.flush()` flushes the query runtime, not RocksDB. The
receipt records live-after-runtime-flush, close, and reopen in its JSON output,
but deliberately does not label any number as a forced-compaction result.

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
