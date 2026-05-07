---
"jazz-tools": patch
---

Add rollback and scoped read support to explicit batches. `DirectBatch`/`DbDirectBatch` now expose `rollback()`, and batch handles can read their own local writes before commit through `DirectBatch.query(...)` and `DbDirectBatch.all(...)`/`one(...)`.
