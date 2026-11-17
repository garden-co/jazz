---
"jazz-tools": patch
---

Serialize a thin client's origin lookup and write under one database operation lease, and clear its attribution if the callback is cancelled. Rust callers of `Db::write_as_origin` must now perform the standalone write through the database handle supplied to their callback (`async |db| { db.insert(...).await }`).
