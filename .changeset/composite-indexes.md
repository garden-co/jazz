---
"jazz-tools": patch
"jazz-napi": patch
"jazz-wasm": patch
"jazz-rn": patch
---

Declare ordered multi-column indexes with `s.table({...}).compositeIndex(["owner", "rank"])` in TypeScript or `TableSchemaBuilder::composite_index` in Rust. An index needs at least two distinct declared columns, and `bytea` columns cannot be indexed. The index is stored and backfilled when the schema is published through the usual migration. Declaring a composite index changes the schema id and needs a server on this version: `deploy` and `pushSchema` stop with `SchemaHashMismatchError` against an older server before publishing anything, and a store that uses composite indexes cannot go back to an earlier version. Schemas without a composite index keep byte-identical ids and hashes.
