---
"jazz-tools": patch
---

React Native writes no longer block the JS thread on applying the write, subscription (IVM) evaluation or relay pumping: the native relay owner does that work on its own thread and wakes JS with already-computed results. Write errors now surface the same way on React Native, web and Node. Validation errors, a closed Db and an unknown table throw synchronously. Failures that depend on row state are reported through the write handle: updating, upserting or deleting a row that is already deleted no longer throws on React Native; the returned handle's `wait()` rejects with `row already deleted: <id>`, and `onMutationError` fires if nothing is waiting. Direct writes are bounded at 64 queued per client; when that queue cannot make progress, the call throws a synchronous `backpressure` error and admits nothing. Reads and subscriptions opened after a write in the same turn reflect it.
