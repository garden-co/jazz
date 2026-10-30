---
"jazz-tools": patch
---

Faster first sync: the server reads the rows it ships in chunks instead of one statement per row, and the client reuses each table's insert and author codes while it stores a batch. A server no longer ships stale columns after a table gains one.
