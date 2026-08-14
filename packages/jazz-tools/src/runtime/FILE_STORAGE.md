# Ordinary-row mutable files

`createConventionalFileStorage` is a library convention, not a Jazz storage
feature. A `files` row is the mutable logical root; immutable `file_parts`
(at most 256 KiB each) and immutable `file_nodes` form a bounded-fanout extent
tree. A saved root tuple is independently readable and never requires an
ancestor-history scan.

The first implementation uses ordinary `bytes` rows. Replacing part payloads
with immutable blob-store pointers is deliberately a future extension: the
root/tree contract does not change. Writers must have ordinary transaction
permission to insert the referenced rows and update the file root; readers
need permission to every reachable row. The helper does not bypass Jazz
permissions, transactions, branching, or rollback behavior.

## Invariants

1. A file root's `byteLength` equals its immutable-tree bytes plus inline bytes.
2. Node child ids and lengths have equal non-zero cardinality; child lengths
   exactly match the referenced part/subtree.
3. Nodes and parts are append-only immutable objects.
4. Range reads validate the reachable tree (including cycles and heights)
   before yielding bytes.
5. A root change and all newly referenced ordinary rows commit atomically.

Equal-length overwrites path-copy only affected parts and their ancestor nodes.
Insert still uses the conservative immutable splice/rebuild path; persistent
tree concatenation/rebalancing is the next performance slice and must be
benchmarked before claiming efficient arbitrary large-file insertion.

## Native RocksDB receipt

`JAZZ_FILE_DISK_BENCH=1 JAZZ_FILE_DISK_APPENDS=8
JAZZ_FILE_DISK_APPEND_BYTES=4096 cargo bench -p groove --bench
file_layout_storage` on the native lane reported p50 7 µs and p95 29 µs for
32,768 logical bytes (eight ordinary root transactions and eight parts), with
Zstd enabled. Directory bytes were 103,746 apparent / 78,102,528 allocated
before and after flush, and 112,328 apparent / 78,110,720 allocated after full
compaction. This deliberately small receipt is a correctness/lifecycle sample,
not a capacity claim: allocated blocks include RocksDB's preallocation,
metadata and WAL; compaction timing and space reuse are backend-dependent.
