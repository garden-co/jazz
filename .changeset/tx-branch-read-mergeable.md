---
"jazz-tools": patch
---

Mergeable transactions read through a branch view: `tx.all` / `tx.one` with `branch` / `base` read the draft over its base, live or frozen (`base: [branch, snapshot]`), including the transaction's own staged writes ([jazz#5806](https://github.com/garden-co/jazz/issues/5806)). Before, such a read silently ignored the branch and returned rows outside the requested view. Serializable transactions still refuse such reads with a "not supported yet" error.
