# Async page-store experiment (WIP)

This document defines the honest path to compare the existing B-tree algorithm
on OPFS and IndexedDB. It deliberately supersedes a discarded native
IndexedDB-key/value experiment: IndexedDB may store opaque pages by page id and
metadata only; it must not supply ordering, range scan, or tree behavior.

## Existing synchronous seam

`OpfsBTree<F: SyncFile>` owns a bounded page cache, page codec, WAL, superblock
swap, splits, rebalance, and scan logic. Its `SyncFile` calls occur during
open/WAL replay, cache misses, large-overflow reads, dirty-page checkpoint,
WAL append/truncate, and superblock writes. Browser IndexedDB is asynchronous,
so a blocking adapter would deadlock a worker and a whole-file memory mirror
would not measure backing-store cache misses honestly.

The new `AsyncPageStore` is therefore intentionally page-addressed:

- `metadata` supplies only fixed page size and logical file length;
- `read_pages([page_id...])` retrieves opaque page blobs by identity;
- `commit` atomically records dirty page blobs, removals, and metadata.

Pages 0 and 1 remain the existing superblock slots. WAL and normal pages retain
their current byte encoding. An IndexedDB implementation will use one object
store keyed by numeric page id plus a metadata record and make each commit one
relaxed `readwrite` transaction. OPFS will implement the same three operations
against fixed offsets in the existing file. Neither adapter gains a key-value
API.

## Required `AsyncOpfsBTree` shape

The existing sync API stays untouched. The comparative implementation is a
parallel `AsyncOpfsBTree<S: AsyncPageStore>` that reuses page encode/decode and
the same B-tree/cache policy, but has async cache-miss and commit boundaries.
Methods own page buffers before awaiting; no borrowed page slice crosses an
await. Its public operations are `open`, `get`, `put`, `delete`, `range`, and
`checkpoint` returning futures.

An awaited `put`/`delete` must first mutate the B-tree cache, then commit its
dirty pages before resolving. Consequently the immediately following
program-order `get` observes the mutation, and reopening after success observes
the same state. The comparative browser tests must prove both properties for
both backends.

## Apples-to-apples benchmark

Both adapters use identical page size, cache capacity/eviction, dirty-page
selection, checkpoint cadence, page identities, and workload driver. The
matrix will include cold/open, sequential and random point reads/writes, mixed
read-write/delete, batch sizes, and bounded range scans at small and medium
values. No numbers are reported until this async path exists for both stores.
