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
flush and full compaction. It subtracts the same store's 87,080 apparent-byte /
4,292,608 allocated-byte empty baseline:

| payload         | workload      |     flat root array |         persistent tree |  tree + 64 KiB tail |
| --------------- | ------------- | ------------------: | ----------------------: | ------------------: |
| binary          | 1,000 × 32 B  |       660 / 648 KiB |       **180 / 188 KiB** | 15,604 / 15,604 KiB |
| binary          | 1,000 × 1 KiB |   1,611 / 1,616 KiB |   **1,159 / 1,164 KiB** | 32,543 / 32,556 KiB |
| binary          | 256 × 64 KiB  | 16,441 / 16,452 KiB | **16,438 / 16,444 KiB** | 24,626 / 24,632 KiB |
| repetitive text | 1,000 × 32 B  |       605 / 608 KiB |       **151 / 156 KiB** |       446 / 448 KiB |
| repetitive text | 1,000 × 1 KiB |       628 / 632 KiB |       **175 / 180 KiB** |       224 / 228 KiB |
| repetitive text | 256 × 64 KiB  |       133 / 140 KiB |           131 / 136 KiB |   **123 / 128 KiB** |

Each cell is `apparent / allocated` incremental storage. Full compaction changed
the post-flush figures only slightly because each fresh column family already
had a single small SST. Before memtable flush, every store reserved about 78 MiB
of filesystem blocks for RocksDB/WAL machinery, so absolute allocated bytes at
that phase are honest but not useful for comparing these small isolated stores.

The result changes the recommendation. A 64 KiB inline tail is excellent only
when repeated historical tails compress strongly. For incompressible data it
stores the growing tail again in every independently readable history row:
1,000 × 1 KiB appends consumed 32.5 MiB after compaction for 1 MiB of logical
payload. The persistent tree consumed 1.16 MiB. Even 32-byte binary appends did
not cross the threshold and therefore retained 15.6 MiB of historical tails for
only 32 KiB of payload.

So bounded inline tails should not be a universal stream representation. The
persistent tree is the stable default for unknown or binary content. An inline
tail remains plausible for compressible text or with a much smaller byte/history
budget, an explicit content policy, or storage-level suffix sharing. Compression
alone does not bound the worst case.

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
