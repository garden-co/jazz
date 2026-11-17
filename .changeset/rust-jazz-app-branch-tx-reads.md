---
"jazz-tools": patch
---

**Rust: branch reads in mergeable transactions.** With the core fix, `tx.on_branch(view)` in `Jazz::transaction` reads through the branch view too, staged writes included, so its `get`, `all`, `upsert` and `restore` work. In `serializable_transaction` a branch read is still refused (jazz#5806), and `upsert`/`restore` through a branch fail before staging.
