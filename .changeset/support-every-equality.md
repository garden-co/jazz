---
"jazz-tools": patch
---

A read policy whose `EXISTS` matches the read row on several columns (`kind = this.kind AND space = this.space`) now ships replicas only the rows equal on all of them, and follows later writes to just those rows. Before, it followed every row sharing the first column, so a client could load and watch every space of the same kind. Each column is matched by value, enum columns included, and the server stops watching these rows once no subscription relies on them.
