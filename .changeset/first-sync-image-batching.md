---
"jazz-tools": patch
---

Faster sync: a server reads the rows it ships or drops, their history versions and flat-join rows in chunks instead of one statement per row. A client reads what it holds of a message's rows at once, and reuses each table's insert and author codes while it stores them. A server no longer ships stale columns after a table gains one.
