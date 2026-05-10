---
"jazz-tools": patch
"jazz-rn": patch
---

`jazz-rn`: `query` is now `async` and no longer blocks the React Native JS thread on one-shot reads.

The native uniffi export used `block_on` on the JS thread, so any `db.all(...)` that needed a later `batched_tick` to settle (e.g. queries that wait on server-sourced data or parked sync messages) could deadlock — the JS thread was blocked, so the `batched_tick` callback could never fire to fulfil the query future. The export is now `async fn` and uniffi-bindgen-react-native generates a Promise-returning JSI call, polled off the JS thread. `JazzRnRuntimeAdapter.query` now `await`s the binding before parsing.
