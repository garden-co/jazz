---
"jazz-tools": patch
---

Breaking change: pass caller-supplied upsert IDs as the second argument with `upsert(table, id, data, options?)`. This replaces `{ id }` in the options object and aligns upsert with other row mutation APIs.
