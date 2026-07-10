# Descriptor persistence

Type: `wayfinder:grilling`
Status: open
Assignee: guido (claimed 2026-07-10)
Blocked by: (none)

## Question

How does a file descriptor (file id, name, mime_type, size — an immutable
composite value) persist as a cell value through the whole stack: schema
declaration (`s.file()`), row-batch encoding (`RowBytes` /
`StoredRowBatch`), the `Storage` KV layer, and the WASM/NAPI/TS bindings?

Decide: a new first-class composite value type in the row encoding, vs
lowering onto existing primitives behind the schema facade (the "facade &
lowering" pattern in `crates/jazz/CONTEXT.md`). Include: how the fate
authority recognizes descriptor writes to enforce in-place-mutation
rejection and grant claiming; whether descriptor fields are queryable or
indexable (PRD says mutable/queryable metadata belongs in sibling columns —
does that mean descriptors are fully opaque to the query layer?); and what
the encoding means for history/branch reads of past descriptors.
