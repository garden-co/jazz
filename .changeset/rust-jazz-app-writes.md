---
"jazz-tools": patch
---

**Rust (`jazz-app-support`): durability, upsert, restore and transactions that read.**

- **`jazz.durable(DurabilityTier::GlobalServer)`** gives a handle whose writes (`insert`, `update`, `delete`, `upsert`, `restore`, `transaction`) return only once the server accepted them, and fail if it refused them. Without a server, they are refused before anything is written.
- **`jazz.wait_for_sync()`** waits until every write committed so far reached the server and fails if the server rejected one, like TS's `waitForSync` (`JazzClient::wait_for_sync`).
- **`jazz.upsert(id, NewX::new(…))`** writes a row with a chosen id, or replaces its columns.
- **`jazz.restore::<X>(id)`** brings a deleted row back, and **`restore_with(id, NewX::new(…))`** brings it back with new values (the full row, like TS's `restore(table, id, data)`). Each returns the row.
- **Breaking: `jazz.transaction` takes an async closure,** `|tx| async move { … }`, so the body can read with `tx.all`, `tx.one` and `tx.get` as the transaction sees the data. `Transaction` gains `upsert`, `restore` and `restore_with`, which return the row. A body that panics rolls back and fails with `Error::Panicked`.
- A write's future runs to its end on the client's thread: dropping it doesn't cancel the write.
