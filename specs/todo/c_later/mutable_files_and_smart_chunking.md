# Binary Large Values: Mutable Files & Smart Chunking — TODO

Support editing blob-backed file values in-place with efficient diff-sync via
content-defined chunking.

See also: [Cascade integration](../b_launch/file_storage_cascade_integration.md).

Current `jazz-tools` file helpers deliberately model files as ordinary rows with
conventional `mime_type` and `data` columns, where `data` is a binary large
value. This TODO is about improving the internal large-value/content-store
representation. It must not reintroduce alpha's historical `file_parts` table,
application-visible chunk rows, or row-level file chunk permissions.

## FastCDC Chunking

Use content-defined chunking (FastCDC) inside the binary large-value layer:

- Variable-size chunks: min ~64KB, target ~256KB, max ~1MB.
- Chunk boundaries determined by content (rolling hash), not position.
- Insert 10 bytes in the middle of a 100MB file → only 1-2 chunks change; the rest keep their content-addressed IDs.
- Makes diff-sync of large binary files practical — only changed chunks transfer.

The application-visible cell remains one `blob` value. Internally, the
large-value store can represent that value as a flat rope of ordered references
to variable-size content-addressed chunks.

## Update Workflow

For Dropbox-like incremental editing:

1. Re-chunk the modified region (FastCDC).
2. New/changed chunks get new UUIDv5 IDs; unchanged chunks keep theirs.
3. Update the internal large-value rope metadata.
4. Sync only sends new content extents plus the updated blob edit metadata.

**Update helper**: re-chunk modified region, diff internal chunk references,
write only new content extents, and commit a blob edit against the existing row.

## Merge Strategy for Concurrent Edits

Concurrent edits to the same blob need a custom merge strategy that reconciles
both the byte ordering and the contents of divergent internal chunks. Since
chunks are content-addressed, the merge can diff chunk references semantically
(same chunk IDs = same content, new IDs = changed regions).

See `../a_mvp/complex_merge_strategies.md` for the general merge strategy framework.

## Open Questions

- FastCDC parameters: should min/target/max chunk sizes be tunable per-app?
- Three-way merge on chunk ID sequences? OT-style? Something else?
- How to expose "file changed" semantics to the app layer — subscription on the
  ordinary file row, blob ranges, or both?
