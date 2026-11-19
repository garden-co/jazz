---
"jazz-tools": patch
---

**Breaking (Rust): the low-level `Db` callback transaction API is removed.** `Db::transaction(…)`, `Db::transaction_for_identity(…)` and `MergeableTx` are gone. Open a transaction handle with `Db::mergeable_tx()` (or `Db::mergeable_tx_for_identity(author)`), write through it, and finish with `tx.commit().await`; dropping it uncommitted discards its writes. Other `Db` calls made while a handle is open no longer join its commit. `Held::callback_transaction` is removed too. The `jazz-app` `jazz.transaction(…)` API is unchanged.
