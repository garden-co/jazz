---
"jazz-tools": patch
---

**Rust: serializable transactions read back their writes through a branch.** In `serializable_transaction`, `tx.on_branch(view)`'s `upsert`, `restore` and `restore_with` now return the row as it reads through the view, as they already do in a mergeable transaction. Before, they were refused before staging.
