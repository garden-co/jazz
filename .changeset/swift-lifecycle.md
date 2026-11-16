---
"jazz-tools": patch
---

Jazz for Swift follows TS's lifecycle: `transaction` is mergeable (its commit merges with concurrent writes and is never a conflict), with `serializableTransaction` for the serializable kind; `disconnect()` and `reconnect()`; `deleteClientStorage()`; and `mutationErrors`, a stream of the writes the server refused that nothing waited on (TS's `onMutationError`).
