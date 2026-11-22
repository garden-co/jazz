---
"jazz-tools": patch
---

A transaction staged at the server that inserts a row and then updates it is no longer refused by the update's permission check (`update.new` / WITH CHECK) when that check compares the row with its committed version. The row isn't committed yet, so its insert decides it, on the state the transaction leaves it in, as it already did when a local-first client committed and uploaded the same writes.
