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

## Compressed RocksDB layout receipt

`stream_layout_storage` writes the three candidate physical layouts to isolated
fresh RocksDB databases. Each append is one atomic batch and writes a complete
root history version. Immutable parts and persistent-tree nodes use ordinary
separate column families. The receipt uses the production compression split:
Zstd for append/history data, LZ4 for ordinary part/node data, and Zstd at the
bottommost level. Payloads are swept as deterministic incompressible binary and
highly repetitive text; those are useful sensitivity bounds, not claims about a
specific application corpus.

The most comparable number is the filesystem increment after explicit memtable
flush and full compaction. The harness now emits that baseline subtraction
directly for both apparent and allocated bytes. Each configuration uses a fresh
store; the empty baseline was about 85 KiB apparent / 4,192 KiB allocated. The
tables below show `apparent / allocated` incremental KiB, tail consolidations,
immutable part/node rows, total timed RocksDB write milliseconds, and p95 write
microseconds. Every workload writes one history row per append; payload
generation, flush, and compaction are outside the write timing.

### 1,000 × 32-byte appends (1,000 history versions)

| tail cap |      binary KiB |    text KiB | consolidations | parts / nodes | binary total / p95 | text total / p95 |
| -------- | --------------: | ----------: | -------------: | ------------: | -----------------: | ---------------: |
| tree (0) |       180 / 188 |   151 / 156 |              0 | 1,000 / 2,503 |      5.1 ms / 7 µs |    4.4 ms / 5 µs |
| 64 B     |       116 / 120 |     69 / 72 |            333 |     333 / 653 |      2.6 ms / 4 µs |    2.6 ms / 4 µs |
| 128 B    |   **109 / 112** |     58 / 60 |            200 |     200 / 379 |      2.6 ms / 3 µs |    2.6 ms / 3 µs |
| 256 B    |       110 / 116 |     53 / 60 |            111 |     111 / 195 |      2.2 ms / 3 µs |    2.2 ms / 3 µs |
| 512 B    |       126 / 132 | **52 / 60** |             58 |       58 / 86 |      2.7 ms / 3 µs |    3.0 ms / 3 µs |
| 1 KiB    |       184 / 192 |     57 / 64 |             30 |       30 / 30 |      2.8 ms / 3 µs |    2.8 ms / 3 µs |
| 64 KiB   | 15,604 / 15,608 |   446 / 448 |              0 |         0 / 0 |    23.6 ms / 41 µs |  23.1 ms / 41 µs |

### 1,000 × 1-KiB appends (1,000 history versions)

| tail cap |        binary KiB |      text KiB | consolidations | parts / nodes | binary total / p95 | text total / p95 |
| -------- | ----------------: | ------------: | -------------: | ------------: | -----------------: | ---------------: |
| tree (0) | **1,159 / 1,164** |     175 / 180 |              0 | 1,000 / 2,503 |      5.5 ms / 6 µs |    5.6 ms / 6 µs |
| 64 B     |     1,159 / 1,164 |     175 / 180 |          1,000 | 1,000 / 2,503 |      5.9 ms / 7 µs |    5.1 ms / 6 µs |
| 128 B    |     1,159 / 1,164 |     175 / 180 |          1,000 | 1,000 / 2,503 |      5.0 ms / 6 µs |    5.3 ms / 6 µs |
| 256 B    |     1,159 / 1,164 |     175 / 180 |          1,000 | 1,000 / 2,503 |      5.3 ms / 6 µs |    4.9 ms / 5 µs |
| 512 B    |     1,159 / 1,164 |     175 / 180 |          1,000 | 1,000 / 2,503 |      4.9 ms / 6 µs |    5.3 ms / 5 µs |
| 1 KiB    |     1,620 / 1,624 | **126 / 132** |            500 |     500 / 998 |      4.8 ms / 6 µs |    4.5 ms / 6 µs |
| 64 KiB   |   32,543 / 32,556 |     224 / 228 |             15 |       15 / 15 |    46.2 ms / 84 µs |  45.9 ms / 89 µs |

### 256 × 64-KiB appends (256 history versions)

| tail cap |          binary KiB |      text KiB | consolidations | parts / nodes | binary total / p95 | text total / p95 |
| -------- | ------------------: | ------------: | -------------: | ------------: | -----------------: | ---------------: |
| tree (0) | **16,439 / 16,448** |     131 / 136 |              0 |     256 / 494 |    22.7 ms / 92 µs |  23.3 ms / 91 µs |
| 64 B     |     16,439 / 16,448 |     131 / 136 |            256 |     256 / 494 |    22.7 ms / 91 µs |  23.1 ms / 92 µs |
| 128 B    |     16,439 / 16,448 |     131 / 136 |            256 |     256 / 494 |    22.9 ms / 92 µs |  23.4 ms / 94 µs |
| 256 B    |     16,439 / 16,448 |     131 / 136 |            256 |     256 / 494 |    23.8 ms / 93 µs |  23.3 ms / 92 µs |
| 512 B    |     16,439 / 16,448 |     131 / 136 |            256 |     256 / 494 |    23.0 ms / 92 µs |  23.6 ms / 91 µs |
| 1 KiB    |     16,439 / 16,448 |     131 / 136 |            256 |     256 / 494 |    23.2 ms / 93 µs |  23.2 ms / 93 µs |
| 64 KiB   |     24,626 / 24,632 | **123 / 128** |            128 |     128 / 230 |   34.4 ms / 179 µs | 35.0 ms / 181 µs |

Full compaction changed post-flush figures only slightly because each fresh
column family already had a single small SST. Before memtable flush, every
store reserved about 78 MiB of filesystem blocks for RocksDB/WAL machinery, so
absolute allocated bytes at that phase remain honest but unhelpful for these
small isolated stores. The flat-root-array reference remains in the raw JSONL
output but is omitted above because the tail crossover is against the stable
persistent-tree baseline.

The recommendation is now narrower than either universal tree or universal
64-KiB tail. For frequent 32-byte appends, a 128–256 B cap is the best binary
storage point and 256–512 B is the best repetitive-text point. Those caps also
reduce tree churn and total direct write time by grouping several appends per
consolidation. At 1-KiB appends, a cap below the append size is effectively the
tree baseline: it consolidates every append, with the same rows, storage, and
write latency. A 1-KiB cap helps repetitive text by grouping pairs but costs
about 40% more storage for incompressible binary. At 64-KiB appends, all smaller
caps collapse to the tree baseline; the 64-KiB cap saves only 8 KiB of repetitive
text while making p95 writes roughly twice as slow and adds about 8 MiB for
binary.

Therefore do not change the public default from this physical receipt alone.
For unknown or binary content, tree baseline remains safest; if a default tail
is introduced after end-to-end Jazz-row measurement, 128–256 B is the strongest
current candidate for small appends. Content-aware text policy can justify a
somewhat larger cap, but compression alone does not bound historical-tail cost.

This is a physical-layout receipt rather than a full Jazz-row storage receipt:
it includes real keys, values, atomic batches, WAL, memtables, SST compression,
flush, compaction, and filesystem allocation, but intentionally excludes the
common Jazz row envelope and indices. Those shared costs would raise every
layout and should not reverse the large binary-tail result.

### Reproduction

```sh
JAZZ_STREAM_DISK_BENCH=1 \
cargo bench -p groove --bench stream_layout_storage
```

Defaults are 1,000 appends for 32-byte and 1-KiB chunks and 256 appends for
64-KiB chunks. `JAZZ_STREAM_DISK_SHORT_APPENDS` and
`JAZZ_STREAM_DISK_LARGE_APPENDS` override those counts.
