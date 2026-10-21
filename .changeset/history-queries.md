---
"jazz-tools": patch
"jazz-napi": patch
"jazz-wasm": patch
"jazz-rn": patch
---

Query and subscribe to previous row versions through `app.$history.table` in TypeScript or `history.tableName` in Rust.

History queries support filtering, projection, ordering, pagination, and local-first and remote reads and subscriptions, fetching missing history from upstream when needed. Historical content is transformed through schema lenses to the requested schema.

Each version exposes `$acceptedAt` as a JavaScript `Date` and `$transactionId` as a `TxId`, alongside the usual opt-in attribution columns (`$createdAt`, `$createdBy`, `$updatedAt`, `$updatedBy`). Server authorization follows access to the current row.

**Upgrade note:** history is available for every table, with no opt-in. Anyone who can currently read a row in a branch can read all retained versions of that row in that branch, including versions written while they had no read access. Review your read policies before upgrading if earlier versions of a row may hold content its current readers should not see.

History tables are read-only and require a bounded `id` constraint. They cannot be filtered by `$transactionId` or used on either side of `.include()`, `.hopTo()`, or `.gather()`. Only accepted content versions appear: pending writes and deletion-only events do not add history rows. Deleted rows' content history can be queried with `.includeDeleted()`. History is currently indexed only by row ID.

**Breaking wire change:** `SupportingRow` now includes an `is_current` flag, allowing queries to combine a current table and its history without treating historical versions as current rows. This is part of the upcoming wire v6 break; upgrade clients, relays, and servers together. Storage formats are unchanged.

**Breaking binding change:** `ResultKey` moves to version 2. A v2 key starts with byte `2` and ends with a `has_transaction` byte, followed by the version's `tx_time` (u64, big-endian) and `tx_node` (16 bytes) when the key names a history version. Version 1 keys are rejected. `OutputOccurrenceId` now serializes as its `ResultKey` bytes instead of the old postcard `(root, joined)` pair, and the typed-row result member variant is removed. These keys are not persisted, so no migration is needed; update the TypeScript and Rust packages together.
