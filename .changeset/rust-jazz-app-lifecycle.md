---
"jazz-tools": patch
---

**Rust: mutation errors, disconnect/reconnect, deleting client storage, text anchors.**

- **`jazz.mutation_errors()`:** a `Stream` of the writes the server refused that no durable wait handled, like TS's `onMutationError`. Refusals that arrived before the first listener are delivered to it.
- **`jazz.disconnect()` / `jazz.reconnect()`:** leave the server and resume sync, as TS's `disconnect`/`reconnect` do. Local reads and writes go on while disconnected.
- **`jazz.delete_client_storage()`:** stops the client and deletes its local store, like TS's `deleteClientStorage`.
- **`jazz_app::text`:** re-exports `jazz-text`'s anchors and ranges, like TS's `db.textAnchors()`.
- **`JazzClient`:** gains `disconnect`, `reconnect`, `track_mutation_errors` and `take_mutation_errors`.
