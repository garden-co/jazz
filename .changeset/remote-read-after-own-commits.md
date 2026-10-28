---
"jazz-tools": patch
---

Fix remote reads and subscriptions asked right after a burst of writes sometimes missing those writes (most visibly in nested relations): the read now always reaches the server after the client's earlier commits. While the server's large-value upload limit holds a client's write back, that client's reads of the tables it writes (or whose permissions depend on them) wait for the upload to be admitted; its other reads are still served right away.
