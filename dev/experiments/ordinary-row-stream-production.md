# Ordinary-row stream production helper: early receipt

The opt-in benchmark exercises `createConventionalStreamStorage` through the
public `Db` API against a real local Jazz authority. Every append is one
exclusive transaction and waits for authority acceptance. It records append
latency, consolidation spikes, ordinary immutable node/part rows, a full read,
and independently reads the first, middle, and last saved snapshot roots.

## Initial receipt

Environment: Node 24 client using the in-memory WASM runtime, local
`jazz-server`, one process. Workload: 40 authority-accepted 32-byte appends,
256-byte tail, fanout 4. The deliberately small tail forces four consolidations.

| metric                   |                result |
| ------------------------ | --------------------: |
| append p50 / p95 / max   | 25.3 / 35.3 / 81.7 ms |
| consolidation p50 / p95  |        28.3 / 35.3 ms |
| immutable part rows      |                     4 |
| immutable tree-node rows |                     4 |
| full 1,280-byte read     |                254 ms |

The receipt is directional, not a cross-machine benchmark. Its important early
signal is structural: 40 small appends produced four parts and four tree nodes,
instead of a part and copied tree path on every append. Consolidation was
visible but remained inside the ordinary accepted-transaction latency band.

The 254 ms full read exposes the current userland cost: the helper resolves
ordinary referenced rows through individual public point queries. General
reachable-row hydration/batching would improve documents, files, and other
ordinary graph-shaped application data too.

Node `createDb` currently uses the in-memory WASM runtime even when a native
filesystem path is supplied, so this receipt deliberately does not claim a
client disk delta. The earlier structural experiment measured a persistent
backend, but backend trusted-serving exclusive transaction reads require the
newer browser/runtime stack. A durable client disk receipt belongs after that
parent is integrated rather than as a synthetic zero.

## Reproduction

```sh
JAZZ_STREAM_BENCH=1 \
JAZZ_STREAM_BENCH_APPENDS=40 \
JAZZ_STREAM_BENCH_APPEND_BYTES=32 \
JAZZ_STREAM_BENCH_TAIL_BYTES=256 \
JAZZ_STREAM_BENCH_FANOUT=4 \
pnpm --dir packages/jazz-tools exec vitest run --config vitest.config.ts \
  src/runtime/stream-storage.benchmark.test.ts --reporter=verbose
```

The append count, append size, tail threshold, and fanout are configurable. The
benchmark is skipped in ordinary test runs.
