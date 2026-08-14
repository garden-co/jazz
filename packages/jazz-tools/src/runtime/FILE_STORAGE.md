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

The current implementation makes bounded byte parts and immutable roots
available first. Its tree rebuilds on overwrite/insert; right-spine path-copy
optimization is the next performance slice and must be benchmarked before
claiming efficient large-file editing.
