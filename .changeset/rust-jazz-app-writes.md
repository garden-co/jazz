---
"jazz-tools": patch
---

**Rust (`jazz-app`): durability, upsert, restore and transactions that read.**

- **`jazz.durable(DurabilityTier::GlobalServer)`** gives a handle whose writes (`insert`, `update`, `delete`, `upsert`, `restore`, `transaction`) return only once the server has them, and fail if it rejected them.
- **`jazz.wait_for_sync()`** waits until every write made so far reached the server.
- **`jazz.upsert(id, NewX::new(…))`** writes a row with a chosen id, or replaces its columns. **`jazz.restore::<X>(id)`** brings a deleted row back.
- **Breaking: `jazz.transaction` takes an async closure,** `|tx| async move { … }`, so the body can read with `tx.all`, `tx.one` and `tx.get` as the transaction sees the data. `Transaction` gains `upsert` and `restore`.
