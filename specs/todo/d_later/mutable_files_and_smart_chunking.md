# Built-in File Storage: Mutable Files & Smart Chunking — TODO

Support editing files in-place with efficient diff-sync via content-defined chunking.

See also: [Phase 1: static files](../b_mvp/built_in_file_storage.md), [Phase 2: cascade integration](../c_launch/file_storage_cascade_integration.md).

## FastCDC Chunking

Replace Phase 1's fixed-size chunking with content-defined chunking (FastCDC):

- Variable-size chunks: min ~64KB, target ~256KB, max ~1MB.
- Chunk boundaries determined by content (rolling hash), not position.
- Insert 10 bytes in the middle of a 100MB file → only 1-2 chunks change; the rest keep their content-addressed IDs.
- Makes diff-sync of large binary files practical — only changed chunks transfer.

The `parts` array becomes a flat rope: ordered references to variable-size content-addressed chunks.

## Update Workflow

For Dropbox-like incremental editing:
1. Re-chunk the modified region (FastCDC).
2. New/changed chunks get new UUIDv5 IDs; unchanged chunks keep theirs.
3. Update `parts` + `part_sizes` arrays.
4. Sync only sends the new chunk rows + the updated file row.

**Update helper**: re-chunk modified region, diff parts arrays, create only new chunk rows, update file row.

## Merge Strategy for Concurrent Edits

Concurrent edits to the same file need a custom merge strategy that reconciles both the array ordering and the contents of divergent parts. Since chunks are content-addressed, the merge can diff arrays semantically (same chunk IDs = same content, new IDs = changed regions).

See `../b_mvp/complex_merge_strategies.md` for the general merge strategy framework.

## Open Questions

- FastCDC parameters: should min/target/max chunk sizes be tunable per-app?
- Three-way merge on chunk ID sequences? OT-style? Something else?
- How to expose "file changed" semantics to the app layer — subscription on the `files` row? On individual parts?
