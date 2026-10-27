---
"jazz-tools": patch
---

Fix remote reads and subscriptions asked right after a burst of writes sometimes missing those writes (most visibly in nested relations): the read now always reaches the server after the client's earlier commits.
