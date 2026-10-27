---
"jazz-tools": patch
---

Wait for IndexedDB persistence before acknowledging a browser write as locally durable. Keep storage failures visible through later flushes and reopen attempts until an explicit successful reset.
