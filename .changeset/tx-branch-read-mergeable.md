---
"jazz-tools": patch
---

Transactions read through branch views ([jazz#5806](https://github.com/garden-co/jazz/issues/5806)). Inside a mergeable or serializable transaction, `tx.all` / `tx.one` with `branch` / `base` read the draft over its base, live or frozen (`base: [branch, snapshot]`), including the transaction's own staged writes. Before, such a read silently ignored the branch and returned rows outside the requested view.

A serializable transaction's commit is validated through the views it read, so a change under them is a `serialization_conflict`. It commits locally, offline included. It is only sent over a link that negotiated `FEATURE_SERIALIZABLE_BRANCH_READS` with the server. Against an older server, the client refuses it (`write_rejected`) before sending, instead of having it validated against the wrong rows.
