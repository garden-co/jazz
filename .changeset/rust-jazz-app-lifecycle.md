---
"jazz-tools": patch
---

**Rust: mutation errors, disconnect/reconnect, deleting client storage, text anchors.**

- **`jazz.mutation_errors()`:** a `Stream` of the writes the server refused that no durable wait handled, like TS's `onMutationError`. Refusals that arrived before the first listener are delivered to it (the last 1024). Each stream holds up to 1024 unread refusals; drops are logged with `tracing::warn!`.
- **`jazz.disconnect()` / `jazz.reconnect()`:** leave the server and resume sync, as TS's `disconnect`/`reconnect` do. Local reads and writes go on while disconnected. Without a server both do nothing.
- **`jazz.delete_client_storage()`:** stops the client and deletes its local store, like TS's `deleteClientStorage`. While another open app has the same store directory (in this process, or another one through a `jazz.lock` file lock) it fails with `Error::StoreHeld`, as TS refuses with `StoreHeldByAnotherTabError`.
- **`jazz_app_support::text`:** re-exports `jazz-text`'s anchors and ranges, like TS's `db.textAnchors()`.
- **`JazzClient`:** gains `disconnect` and `reconnect`. `MutationError` serializes (camelCase, `transaction` as its string id).
