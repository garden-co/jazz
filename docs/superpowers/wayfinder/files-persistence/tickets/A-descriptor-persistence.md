# Descriptor persistence

Type: `wayfinder:grilling`
Status: closed (resolved 2026-07-10)
Assignee: guido
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

## Resolution (2026-07-10)

1. **Type/value shape:** new schema-level `ColumnType::File` lowering onto
   `Value::Text` carrying canonical JSON — the `ColumnType::Json` precedent
   (schema facade over the Text value type). No new `Value` variant, no
   row-format change, no WASM/NAPI/RN value-boundary change; only
   schema-wire (DDL string), encode/decode routing, and a write-path
   validation branch.
2. **Canonical form:** compact sorted-key JSON, required `v: 1` plus `id`,
   `name`, `mime_type`, `size`. Shape strictly validated on write (like
   JSON-schema validation for JSON columns); readers lenient (unknown
   fields/versions tolerated; `url()` needs only the id).
   _Same-day amendments (recorded in the PRD): the id is now
   identity-bound — its segments encode TTL class + uploader identity +
   random — finalized at the first cell write using the destination
   column's schema-declared class (`s.file({ ttl })`). The canonical form
   still stands at exactly the four fields above (a briefly-added `ttl`
   field was dropped again when the class moved into the id string)._
3. **No immutability enforcement.** In-place edits, copies, hand-rolled
   descriptors: all legal, ordinary policy-gated writes. Body immutability
   lives at the bucket only (one grant per id ever + `If-None-Match: *`).
   `size`/`mime_type`/`name` are app-trusted metadata.
4. **No body verification.** Acceptance has no file-specific role. Release
   = edge completes multipart, server-side-copies `pending/{app}/{id}` →
   `{app}/{id}`, marks the grant claimed. Unclaimed cleanup = bucket
   lifecycle TTL on the `pending/` prefix + native incomplete-multipart
   abort (R2-portable, prefix-based). No server sweep machinery.
5. **Deletion is an explicit API** (`jazz.files.delete(fileId)` over sync),
   authorized for the uploader identity (recorded in the ledger) and the
   backend/admin surface. Cell death never deletes objects.
6. **Ledger shrinks** to: file id → uploader + granted/claimed state,
   permanent (id never grantable twice).
7. **Queryability:** file cells are opaque in v1 (text-column semantics);
   queryable metadata in sibling columns; magic columns a future extension.
8. **Outbox hold** is SDK courtesy (hold `fromBlob`-carrying transactions
   until the PUT completes), not a server gate. "Visible descriptor ⇒ bytes"
   is not a protocol guarantee; dangling descriptors are legal and 404.
9. **History/branch reads:** descriptors decode as ordinary text at any
   cut; URLs may 404.

Assets: PRD + explainer amended in the same commit
(`docs/superpowers/specs/2026-07-09-files-spec.md`,
`2026-07-09-files-design-explained.md`).
