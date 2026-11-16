---
"jazz-tools": patch
---

`db.transaction(callback)` whose callback writes nothing, such as a cleanup that finds nothing to clean up, no longer throws "empty mergeable transaction has no committed unit". The transaction is rolled back, as there's nothing to commit, and the result resolves with the callback's value; its `wait()` resolves at once. Committing an empty mergeable transaction by hand (`beginTransaction()` … `commit()`) still refuses, as before.
