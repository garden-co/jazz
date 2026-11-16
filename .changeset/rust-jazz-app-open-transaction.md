---
"jazz-tools": patch
---

**Rust: transactions driven call by call.** `jazz.begin_transaction()` and `begin_serializable_transaction()` return an `OpenTransaction`, like TS's `beginTransaction` / `beginSerializableTransaction`. It's a `Send` handle with `insert`, `update`, `upsert`, `delete`, `restore`, `all`, `one`, `get`, `commit` and `rollback`, and dropping it uncommitted rolls it back.
