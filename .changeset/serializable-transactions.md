---
"jazz-tools": major
"jazz-napi": major
"jazz-wasm": major
"jazz-rn": major
---

Rename exclusive transactions to serializable transactions. `db.exclusiveTransaction(...)` is now `db.serializableTransaction(...)`, `db.beginExclusiveTransaction()` is now `db.beginSerializableTransaction()`, the Effect `jazz.exclusiveTransaction` is now `jazz.serializableTransaction`, `ExclusiveWriteResult` and `ExclusiveWriteHandle` are now `SerializableWriteResult` and `SerializableWriteHandle`, and the transaction kind `"exclusive"` is now `"serializable"`. A conflict rejection now has the code `"serialization_conflict"` instead of `"exclusive_conflict"`. In Rust, `Db::exclusive_tx` is now `Db::serializable_tx`, and `ExclusiveTx`, `ExclusiveTxOps` and `ExclusiveTxRef` are now `SerializableTx`, `SerializableTxOps` and `SerializableTxRef`. Only the names changed: validation, conflicts, and stored and synced data are the same, and the old names have no alias.
