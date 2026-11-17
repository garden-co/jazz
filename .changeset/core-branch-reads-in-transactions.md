---
"jazz-tools": patch
---

**Core: reads through a branch view inside a mergeable transaction** ([jazz#5806](https://github.com/garden-co/jazz/issues/5806)). A transaction read with a branch view (`branch`/`base`) used to ignore the view and read every branch-local row as stored. In a mergeable transaction it now composes the head over its live or frozen base, over the transaction's staged writes, as outside one. Serializable transactions read through branch views too, and their commits are validated through the view (see the serializable branch read notes). A snapshot read in any transaction is refused with "not supported yet".
