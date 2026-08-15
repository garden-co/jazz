# jazz — Specification · 21. File content adapter

`file-v1` is declared as a required manifest record with `Bytes` edit-tail
entries. Use `ContentManifestSchema::with_tail_entry_type("file-v1",
`ValueType::Bytes`, entries, bytes)` and `ColumnSchema::content_manifest`.

`FileContentAdapter::store_bytes` creates an immutable extent tree. Encode a
`FileEdit` with `FileContentAdapter::encode_edit`, put the resulting typed
value in the manifest tail, and store the complete cell with `into_value`.
Materialization accepts full and range requests through
`ContentManifestRuntime`; `index_values_for_cell` exposes adapter-defined
metadata.

File edit offsets are rooted in the immutable base. Non-overlapping tails can
merge deterministically; overlapping edits fail. `consolidate` publishes a new
immutable root and an empty tail. Extents are content-addressed, so unaffected
leaf objects remain reusable across a consolidation.
