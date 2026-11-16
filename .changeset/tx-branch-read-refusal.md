---
"jazz-tools": patch
---

Transaction reads (`tx.all` / `tx.one`) now reject `branch` and `base` with a "not supported yet" error instead of silently ignoring the branch and returning rows outside the requested branch view ([jazz#5806](https://github.com/garden-co/jazz/issues/5806)). Branch writes inside transactions are unaffected; read through a branch with `db.all` / `db.one` outside the transaction for now.
