---
"jazz-tools": patch
---

Keep subscription hydration’s memo capture and replacement scoped to reachable graph nodes instead of scanning unrelated resident memos. Retain only hydration snapshots, maintain cache-byte accounting during removal, and preserve the existing frontier validation and eviction limits.
