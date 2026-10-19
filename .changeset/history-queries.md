---
"jazz-tools": patch
"jazz-napi": patch
"jazz-wasm": patch
"jazz-rn": patch
---

Query and subscribe to previous row versions through `app.table.history` in TypeScript or `history.tableName` in Rust.

History queries support filtering, projection, ordering, pagination, and local-first and remote reads and subscriptions, fetching missing history from upstream when needed. Historical content is transformed through schema lenses to the requested schema.

Each version exposes `$acceptedAt` as a JavaScript `Date` and `$transactionId` as a `TxId`, alongside the usual opt-in attribution columns (`$createdAt`, `$createdBy`, `$updatedAt`, `$updatedBy`). Server authorization follows access to the current row.

History tables are read-only and require a bounded `id` constraint. They cannot be filtered by `$transactionId` or used on either side of `.include()`, `.hopTo()`, or `.gather()`. Only accepted content versions appear: pending writes and deletion-only events do not add history rows. Deleted rows' content history can be queried with `.includeDeleted()`. History is currently indexed only by row ID.

**Breaking wire change:** `SupportingRow` now includes an `is_current` flag, allowing queries to combine a current table and its history without treating historical versions as current rows. This is part of the upcoming wire v6 break; upgrade clients, relays, and servers together. Storage formats are unchanged.
