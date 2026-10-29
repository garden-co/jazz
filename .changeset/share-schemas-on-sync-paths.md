---
"jazz-tools": patch
---

Share a maintained subscription's output tables instead of copying them on every update, and borrow table schemas during query validation instead of cloning them on every lookup. This removes a large share of the schema copying a client does while it syncs a schema with many tables.
