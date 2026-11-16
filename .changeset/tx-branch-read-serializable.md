---
"jazz-tools": patch
---

Serializable transactions can read through a branch view too: `tx.all` / `tx.one` with `branch` / `base` read the draft over its base, and the commit is validated through that view, so a change under what the transaction read is a `serialization_conflict` ([jazz#5806](https://github.com/garden-co/jazz/issues/5806)). Clients now negotiate this with the server (`FEATURE_SERIALIZABLE_BRANCH_READS`). A server without it refuses to commit a serializable transaction that read through a branch view, rather than validating it against the wrong rows.
