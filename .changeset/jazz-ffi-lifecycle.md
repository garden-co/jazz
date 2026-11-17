---
"jazz-tools": patch
---

jazz-ffi gains TS's lifecycle calls. Mergeable transactions (`begin_mergeable`, C `txBegin {"serializable": false}`), `disconnect` / `reconnect`, `deleteClientStorage`, and a `mutationErrors` watch reporting the writes the server refused that nothing waited on (TS's `onMutationError`).
